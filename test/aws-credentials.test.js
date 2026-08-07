/**
 * Where a credential comes from, and what happens when it expires.
 *
 * All of it against a FAKE metadata service, which is the point: an
 * instance role is an HTTP endpoint and a clock, and both are easier to
 * control here than on an EC2 instance. The failure that matters —
 * credentials rotating underneath a long-lived process — takes about
 * six hours to observe on real hardware and about a millisecond here.
 *
 * `AWS_EC2_METADATA_SERVICE_ENDPOINT` is the AWS SDKs' own variable, so
 * pointing it at a local server is a supported thing to do rather than
 * a hook invented for testing.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import http from 'node:http';
import { credentialProvider } from '../src/aws-credentials.js';

const AWS_ENV = [
  'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN',
  'AWS_CONTAINER_CREDENTIALS_FULL_URI', 'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
  'AWS_CONTAINER_AUTHORIZATION_TOKEN', 'AWS_EC2_METADATA_SERVICE_ENDPOINT',
  'AWS_EC2_METADATA_DISABLED'
];

let saved, server;

beforeEach(() => {
  saved = Object.fromEntries(AWS_ENV.map((k) => [k, process.env[k]]));
  for (const k of AWS_ENV) delete process.env[k];
});

afterEach(async () => {
  for (const [k, v] of Object.entries(saved)) {
    if (v === undefined) delete process.env[k];
    else process.env[k] = v;
  }
  if (server) await new Promise((r) => server.close(r));
  server = null;
});

/** An IMDSv2 that hands out a credential expiring when told to. */
async function fakeIMDS({ expiresInMs = 3_600_000, role = 'nisaba-agent' } = {}) {
  const seen = [];
  let issued = 0;
  server = http.createServer((req, res) => {
    seen.push({ method: req.method, url: req.url, headers: req.headers });
    if (req.method === 'PUT' && req.url === '/latest/api/token') {
      return res.end('imds-token');
    }
    // A hardened instance refuses anything without the token.
    if (req.headers['x-aws-ec2-metadata-token'] !== 'imds-token') {
      res.writeHead(401);
      return res.end('needs a token');
    }
    if (req.url === '/latest/meta-data/iam/security-credentials/') return res.end(role);
    if (req.url === `/latest/meta-data/iam/security-credentials/${role}`) {
      issued++;
      return res.end(JSON.stringify({
        AccessKeyId: `ASIA-issue-${issued}`,
        SecretAccessKey: `secret-${issued}`,
        Token: `token-${issued}`,
        Expiration: new Date(Date.now() + expiresInMs).toISOString()
      }));
    }
    res.writeHead(404);
    res.end();
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  process.env.AWS_EC2_METADATA_SERVICE_ENDPOINT = `http://127.0.0.1:${server.address().port}`;
  return { seen, issues: () => issued };
}

describe('the order things are tried in', () => {
  it('prefers what the caller passed to anything in the environment', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'from-env';
    process.env.AWS_SECRET_ACCESS_KEY = 'env-secret';
    const c = await credentialProvider({ accessKeyId: 'explicit', secretAccessKey: 's' }).get();
    expect(c.accessKeyId).toBe('explicit');
  });

  it('reads the environment when the caller passed nothing', async () => {
    process.env.AWS_ACCESS_KEY_ID = 'from-env';
    process.env.AWS_SECRET_ACCESS_KEY = 'env-secret';
    process.env.AWS_SESSION_TOKEN = 'env-token';
    const c = await credentialProvider().get();
    expect(c).toMatchObject({ accessKeyId: 'from-env', sessionToken: 'env-token' });
  });

  it('takes a container credential when one is offered', async () => {
    const seen = [];
    server = http.createServer((req, res) => {
      seen.push(req.headers.authorization ?? null);
      res.end(JSON.stringify({
        AccessKeyId: 'ASIA-container', SecretAccessKey: 's', Token: 't',
        Expiration: new Date(Date.now() + 3_600_000).toISOString()
      }));
    });
    await new Promise((r) => server.listen(0, '127.0.0.1', r));
    process.env.AWS_CONTAINER_CREDENTIALS_FULL_URI = `http://127.0.0.1:${server.address().port}/creds`;
    process.env.AWS_CONTAINER_AUTHORIZATION_TOKEN = 'task-token';

    const c = await credentialProvider().get();
    expect(c.accessKeyId).toBe('ASIA-container');
    expect(seen[0]).toBe('task-token');       // the header ECS expects
  });

  it('falls through to the instance role, over IMDSv2', async () => {
    const imds = await fakeIMDS();
    const c = await credentialProvider().get();
    expect(c).toMatchObject({ accessKeyId: 'ASIA-issue-1', sessionToken: 'token-1' });
    // v2: a token is taken first, and every read carries it.
    expect(imds.seen[0]).toMatchObject({ method: 'PUT', url: '/latest/api/token' });
    expect(imds.seen[1].headers['x-aws-ec2-metadata-token']).toBe('imds-token');
  });

  it('says so plainly when there is nothing anywhere', async () => {
    process.env.AWS_EC2_METADATA_DISABLED = 'true';
    await expect(credentialProvider().get()).rejects.toThrow(/no AWS credentials/);
  });

  it('does not go looking on a machine that told it not to', async () => {
    // A laptop is not an EC2 instance, and 169.254.169.254 does not
    // refuse quickly there -- it hangs until something gives up.
    process.env.AWS_EC2_METADATA_DISABLED = 'true';
    const started = Date.now();
    await expect(credentialProvider().get()).rejects.toThrow();
    expect(Date.now() - started).toBeLessThan(500);
  });
});

describe('a credential that expires', () => {
  it('is fetched once and reused while it is good', async () => {
    const imds = await fakeIMDS({ expiresInMs: 3_600_000 });
    const provider = credentialProvider();
    for (let i = 0; i < 5; i++) await provider.get();
    expect(imds.issues()).toBe(1);
  });

  it('is refreshed BEFORE it expires, not after it fails', async () => {
    /*
     * The whole reason this file exists. A process that holds one until
     * it is refused signs a request that is going to fail, and finds
     * out by having a backup fail hours after a deploy that looked
     * fine. The margin means the credential in hand is always still
     * good when it is used.
     */
    const imds = await fakeIMDS({ expiresInMs: 60_000 });   // inside the 5-minute margin
    const provider = credentialProvider();

    const first = await provider.get();
    expect(first.accessKeyId).toBe('ASIA-issue-1');
    const second = await provider.get();
    expect(second.accessKeyId).toBe('ASIA-issue-2');        // already refreshed
    expect(imds.issues()).toBe(2);
  });

  it('fetches once when many requests notice the expiry together', async () => {
    // A machine with a dozen tenants backs them up on one clock. Twelve
    // simultaneous refreshes of the same credential is a thundering
    // herd at the metadata service, which rate-limits.
    const imds = await fakeIMDS({ expiresInMs: 60_000 });
    const provider = credentialProvider();
    await Promise.all(Array.from({ length: 12 }, () => provider.get()));
    expect(imds.issues()).toBe(1);
  });

  it('lets a static pair alone: it does not expire and is never fetched', async () => {
    const imds = await fakeIMDS();
    process.env.AWS_ACCESS_KEY_ID = 'AKIA-static';
    process.env.AWS_SECRET_ACCESS_KEY = 'secret';
    const provider = credentialProvider();
    await provider.get();
    await provider.get();
    expect(imds.issues()).toBe(0);
    expect(imds.seen).toHaveLength(0);
  });
});
