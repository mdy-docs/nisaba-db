/**
 * The three things about src/s3.js that only matter against AWS, and so
 * cannot be proven by the MinIO suite next door: temporary credentials,
 * retry with backoff, and a socket that goes quiet.
 *
 * Against a HAND-WRITTEN S3, deliberately. MinIO is the right tool for
 * "is the dialect correct" and the wrong one for "what happens when the
 * store answers 503 twice and then works" — a real store will not
 * produce that on demand, and the code paths that decide whether a
 * backup happens at all are exactly the ones a healthy store never
 * exercises. So these run on every push, with no MinIO anywhere.
 */
import { describe, it, expect, afterEach } from 'vitest';
import http from 'node:http';
import { S3Client, S3Error } from '../src/s3.js';

const CREDS = { accessKeyId: 'AKIAEXAMPLE', secretAccessKey: 'secret' };

let server = null;

/** A fake S3 that answers however the test says, remembering what it
 * was asked. `reply(n, req)` returns {status, headers?, body?} or null
 * to never answer at all. */
async function fakeS3(reply) {
  const seen = [];
  server = http.createServer((req, res) => {
    const n = seen.length;
    seen.push({ method: req.method, url: req.url, headers: req.headers });
    const answer = reply(n, req);
    if (!answer) return;                       // hang, on purpose
    res.writeHead(answer.status, { 'Content-Type': 'application/xml', ...(answer.headers ?? {}) });
    res.end(answer.body ?? '');
  });
  await new Promise((r) => server.listen(0, '127.0.0.1', r));
  return { seen, endpoint: `http://127.0.0.1:${server.address().port}` };
}

afterEach(async () => {
  if (server) await new Promise((r) => server.close(r));
  server = null;
});

const error = (code, message) =>
  `<?xml version="1.0"?><Error><Code>${code}</Code><Message>${message}</Message></Error>`;

describe('a region it was not given', () => {
  it('refuses to guess when the endpoint is AWS', () => {
    // The region picks the host AND signs the request, so a wrong guess
    // comes back as a malformed signature -- which reads as a broken
    // client rather than an unset variable.
    expect(() => new S3Client({ bucket: 'b', ...CREDS }))
      .toThrow(/needs a region when no endpoint is given/);
  });

  it('keeps its harmless default for a store addressed by endpoint', () => {
    const s3 = new S3Client({ bucket: 'b', endpoint: 'http://127.0.0.1:9000', ...CREDS });
    expect(s3.region).toBe('us-east-1');
  });

  it('passes S3\'s own answer through when the bucket is somewhere else', async () => {
    const { endpoint } = await fakeS3(() => ({
      status: 400,
      headers: { 'x-amz-bucket-region': 'eu-west-2' },
      body: error('AuthorizationHeaderMalformed', 'the region us-east-1 is wrong')
    }));
    const s3 = new S3Client({ bucket: 'b', endpoint, region: 'us-east-1', maxAttempts: 1, ...CREDS });
    // Not "malformed signature" -- the name of the region to use.
    await expect(s3.getObject('k')).rejects.toThrow(/is in eu-west-2, this client signed for us-east-1/);
  });
});

describe('temporary credentials', () => {
  it('sends the session token, and SIGNS it', async () => {
    /*
     * The whole point: an instance role, an assumed role and a CI OIDC
     * exchange all issue a triple, and AWS refuses the pair without the
     * third. Signing it matters as much as sending it -- a token
     * outside SignedHeaders is a request whose signature covers
     * something other than what arrived.
     */
    const { seen, endpoint } = await fakeS3(() => ({ status: 200, body: 'ok' }));
    const s3 = new S3Client({
      bucket: 'b', endpoint, ...CREDS, sessionToken: 'FwoGZXIvYXdzEExample//////'
    });
    await s3.getObject('k');

    expect(seen[0].headers['x-amz-security-token']).toBe('FwoGZXIvYXdzEExample//////');
    expect(seen[0].headers.authorization).toMatch(/SignedHeaders=[^,]*x-amz-security-token/);
  });

  it('sends none at all for a static key pair', async () => {
    const { seen, endpoint } = await fakeS3(() => ({ status: 200, body: 'ok' }));
    await new S3Client({ bucket: 'b', endpoint, ...CREDS }).getObject('k');
    expect(seen[0].headers['x-amz-security-token']).toBeUndefined();
    expect(seen[0].headers.authorization).not.toMatch(/x-amz-security-token/);
  });

  it('takes it from the environment, and re-reads it rather than capturing it', async () => {
    /*
     * Re-read per request, not frozen in the constructor: an outside
     * refresher rewriting process.env is how temporary credentials were
     * kept alive before this client could fetch its own, and a client
     * that captured the first value would sign with an expired one
     * forever afterwards.
     */
    const before = process.env.AWS_SESSION_TOKEN;
    const { seen, endpoint } = await fakeS3(() => ({ status: 200, body: 'ok' }));
    const s3 = new S3Client({ bucket: 'b', endpoint, ...CREDS });
    try {
      process.env.AWS_SESSION_TOKEN = 'first';
      await s3.getObject('k');
      expect(seen[0].headers['x-amz-security-token']).toBe('first');

      process.env.AWS_SESSION_TOKEN = 'rotated';
      await s3.getObject('k');
      expect(seen[1].headers['x-amz-security-token']).toBe('rotated');
    } finally {
      if (before === undefined) delete process.env.AWS_SESSION_TOKEN;
      else process.env.AWS_SESSION_TOKEN = before;
    }
  });
});

describe('what S3 asks a client to come back for', () => {
  it('retries a 503 SlowDown and succeeds', async () => {
    // AWS throttles per prefix and answers this under load. A client
    // that treats it as a failure turns a busy minute into a backup
    // that did not happen -- and MinIO on loopback never says it.
    const { seen, endpoint } = await fakeS3((n) => n < 2
      ? { status: 503, body: error('SlowDown', 'Please reduce your request rate.') }
      : { status: 200, body: 'finally' });
    const s3 = new S3Client({ bucket: 'b', endpoint, ...CREDS });

    expect((await s3.getObject('k')).toString()).toBe('finally');
    expect(seen).toHaveLength(3);
  });

  it('retries a dropped connection', async () => {
    const { seen, endpoint } = await fakeS3((n, req) => {
      if (n === 0) { req.destroy(); return null; }
      return { status: 200, body: 'second time' };
    });
    const s3 = new S3Client({ bucket: 'b', endpoint, ...CREDS });
    expect((await s3.getObject('k')).toString()).toBe('second time');
    expect(seen).toHaveLength(2);
  });

  it('gives up after maxAttempts rather than hammering forever', async () => {
    const { seen, endpoint } = await fakeS3(() => ({ status: 503, body: error('SlowDown', 'no') }));
    const s3 = new S3Client({ bucket: 'b', endpoint, maxAttempts: 3, ...CREDS });

    await expect(s3.getObject('k')).rejects.toThrow(S3Error);
    expect(seen).toHaveLength(3);
  });

  it('never retries a 4xx: an answer is not a hiccup', async () => {
    // Retrying AccessDenied is how a misconfigured agent turns one
    // refusal into three, on every routine, forever.
    const { seen, endpoint } = await fakeS3(() => ({
      status: 403, body: error('AccessDenied', 'Access Denied')
    }));
    const s3 = new S3Client({ bucket: 'b', endpoint, ...CREDS });

    await expect(s3.getObject('k')).rejects.toThrow(/Access Denied/);
    expect(seen).toHaveLength(1);
  });

  it('reads a missing object as absence, without retrying it', async () => {
    const { seen, endpoint } = await fakeS3(() => ({ status: 404 }));
    const s3 = new S3Client({ bucket: 'b', endpoint, ...CREDS });
    expect(await s3.headObject('gone')).toBeNull();
    expect(seen).toHaveLength(1);
  });
});

describe('a socket that goes quiet', () => {
  it('gives up on silence instead of hanging the backup that asked', async () => {
    /*
     * INACTIVITY, not a deadline: a generation file may be gigabytes
     * and is allowed to take as long as the wire needs. What is refused
     * is a connection that has said nothing at all. Without this a
     * half-open socket hangs a routine in `running` forever, and the
     * no-overlap guard means it is never due again either.
     */
    const { seen, endpoint } = await fakeS3(() => null);        // accepts, never answers
    const s3 = new S3Client({ bucket: 'b', endpoint, socketTimeoutMs: 120, maxAttempts: 2, ...CREDS });

    await expect(s3.getObject('k')).rejects.toThrow(/no data for 120ms/);
    expect(seen).toHaveLength(2);                               // and it was retried
  });
});
