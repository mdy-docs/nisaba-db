/**
 * aws-credentials.js — where an AWS credential comes from, and when it
 * stops being one.
 *
 * s3.js used to read `AWS_ACCESS_KEY_ID` once, in its constructor, and
 * hold it forever. That is exactly right for the static key pair a
 * developer exports, and wrong for every way AWS actually hands
 * credentials to something it is running:
 *
 *   - an EC2 instance role, over IMDS
 *   - an ECS task role or EKS pod identity, over the container endpoint
 *   - anything assumed through STS
 *
 * All three issue a TRIPLE that EXPIRES — typically in about six hours,
 * rotated well before that. Two failures follow from holding one
 * forever, and the second is the nastier:
 *
 *   1. Nothing populates the environment on an EC2 box, so an agent
 *      configured "with an instance profile" has no credentials at all.
 *   2. A process that captured one at boot keeps signing with it after
 *      it expires. A control plane that has been up a day answers every
 *      "what can I restore" with 403, having worked perfectly at
 *      deploy time. The failure is dated, not immediate, which is the
 *      kind that reaches production.
 *
 * So credentials are RESOLVED PER REQUEST here, cached until shortly
 * before they expire, and refreshed underneath the caller. A static
 * pair resolves to itself and costs nothing.
 *
 * ── THE CHAIN, IN ORDER ──────────────────────────────────────────────
 *
 *   1. what the caller passed explicitly
 *   2. the environment, re-read each time — so an external refresher
 *      that rewrites `process.env` works, which is how this was done
 *      before there was anything else
 *   3. the container endpoint (`AWS_CONTAINER_CREDENTIALS_FULL_URI` or
 *      `…_RELATIVE_URI`) — ECS, EKS
 *   4. IMDSv2 — EC2
 *
 * The same order the AWS SDKs use, minus the profile files and SSO:
 * this runs in servers, and a config file in `~/.aws` is not how a
 * fleet agent should be credentialed.
 *
 * ── WHY IMDS IS TRIED LAST, AND BRIEFLY ──────────────────────────────
 *
 * 169.254.169.254 is a link-local address that exists on EC2 and
 * nowhere else. On a laptop the connection does not fail fast — it can
 * hang until something gives up — so the timeout here is short and one
 * attempt, and `AWS_EC2_METADATA_DISABLED=true` skips it entirely. The
 * cost of guessing wrong is a slow error rather than a wrong answer,
 * but a slow error inside a backup is still a backup that did not
 * happen on time.
 *
 * `AWS_EC2_METADATA_SERVICE_ENDPOINT` (the SDKs' own variable) points
 * it somewhere else, which is what makes every path here testable
 * without an EC2 instance — see test/aws-credentials.test.js.
 */

import http from 'node:http';
import https from 'node:https';

/** Refresh this long before expiry: a credential that dies mid-backup
 * is a failed backup, and the window costs nothing. */
const EXPIRY_MARGIN_MS = 5 * 60_000;

const IMDS_DEFAULT = 'http://169.254.169.254';
const CONTAINER_HOST = 'http://169.254.170.2';

/** One small HTTP request, with a deadline. Metadata services answer in
 * milliseconds or are not there at all. */
function fetchText(url, { method = 'GET', headers = {}, timeoutMs = 1000 } = {}) {
  return new Promise((resolve, reject) => {
    const u = new URL(url);
    const transport = u.protocol === 'https:' ? https : http;
    const req = transport.request({
      host: u.hostname,
      port: u.port || (u.protocol === 'https:' ? 443 : 80),
      path: `${u.pathname}${u.search}`,
      method,
      headers
    }, (res) => {
      const chunks = [];
      res.on('data', (c) => chunks.push(c));
      res.on('end', () => {
        const body = Buffer.concat(chunks).toString('utf8');
        if (res.statusCode >= 200 && res.statusCode < 300) resolve(body);
        else reject(new Error(`${method} ${url} answered ${res.statusCode}`));
      });
      res.on('error', reject);
    });
    req.setTimeout(timeoutMs, () => {
      req.destroy(new Error(`${method} ${url} did not answer within ${timeoutMs}ms`));
    });
    req.on('error', reject);
    req.end();
  });
}

/** The shape all three services answer with. */
function fromResponse(json, source) {
  const parsed = typeof json === 'string' ? JSON.parse(json) : json;
  if (!parsed.AccessKeyId || !parsed.SecretAccessKey) {
    throw new Error(`${source} answered without a credential in it`);
  }
  return {
    accessKeyId: parsed.AccessKeyId,
    secretAccessKey: parsed.SecretAccessKey,
    sessionToken: parsed.Token ?? null,
    // No expiry means "does not expire", which is true of a static pair
    // and never true of these -- but reading it as forever is safer
    // than inventing a deadline.
    expiresAt: parsed.Expiration ? Date.parse(parsed.Expiration) : null,
    source
  };
}

/** ECS / EKS: the endpoint is handed over in the environment. */
async function fromContainer() {
  const full = process.env.AWS_CONTAINER_CREDENTIALS_FULL_URI;
  const relative = process.env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI;
  if (!full && !relative) return null;
  const url = full ?? `${CONTAINER_HOST}${relative}`;
  const headers = {};
  const token = process.env.AWS_CONTAINER_AUTHORIZATION_TOKEN;
  if (token) headers.authorization = token;
  return fromResponse(await fetchText(url, { headers, timeoutMs: 2000 }), 'the container credentials endpoint');
}

/** EC2, IMDSv2: a token first, then the role, then its credential. */
async function fromIMDS() {
  if (process.env.AWS_EC2_METADATA_DISABLED === 'true') return null;
  const base = process.env.AWS_EC2_METADATA_SERVICE_ENDPOINT ?? IMDS_DEFAULT;

  // IMDSv2. v1 (no token) is still accepted by many instances, but a
  // hardened one requires this, and asking costs one round trip.
  const token = await fetchText(`${base}/latest/api/token`, {
    method: 'PUT',
    headers: { 'x-aws-ec2-metadata-token-ttl-seconds': '21600' }
  });
  const headers = { 'x-aws-ec2-metadata-token': token };

  const role = (await fetchText(`${base}/latest/meta-data/iam/security-credentials/`, { headers })).trim();
  if (!role) throw new Error('this instance has no role attached');
  const body = await fetchText(
    `${base}/latest/meta-data/iam/security-credentials/${encodeURIComponent(role.split('\n')[0])}`,
    { headers }
  );
  return fromResponse(body, `the instance role '${role.split('\n')[0]}'`);
}

/**
 * A credential source that refreshes itself.
 *
 * @param {object} [explicit] - {accessKeyId, secretAccessKey, sessionToken}
 *   as passed to S3Client; any part missing falls through to the chain
 * @returns {{ get: () => Promise<object>, describe: () => string }}
 */
export function credentialProvider(explicit = {}) {
  let cached = null;
  let inFlight = null;

  const fixed = () => {
    const accessKeyId = explicit.accessKeyId ?? process.env.AWS_ACCESS_KEY_ID;
    const secretAccessKey = explicit.secretAccessKey ?? process.env.AWS_SECRET_ACCESS_KEY;
    if (!accessKeyId || !secretAccessKey) return null;
    return {
      accessKeyId,
      secretAccessKey,
      sessionToken: explicit.sessionToken ?? process.env.AWS_SESSION_TOKEN ?? null,
      expiresAt: null,
      source: explicit.accessKeyId ? 'the credentials given to S3Client' : 'the environment'
    };
  };

  const fresh = (c) => c && (c.expiresAt === null || c.expiresAt - Date.now() > EXPIRY_MARGIN_MS);

  async function resolve() {
    const container = await fromContainer();
    if (container) return container;
    const imds = await fromIMDS();
    if (imds) return imds;
    throw new Error(
      'no AWS credentials: none given to S3Client, none in AWS_ACCESS_KEY_ID / ' +
      'AWS_SECRET_ACCESS_KEY, no container credentials endpoint, and no instance role'
    );
  }

  return {
    async get() {
      // Static wins and is re-read every time, so an outside refresher
      // rewriting process.env is picked up without restarting anything.
      const still = fixed();
      if (still) return still;
      if (fresh(cached)) return cached;
      // One fetch even if twenty requests notice the expiry together.
      inFlight ??= resolve()
        .then((c) => { cached = c; return c; })
        .finally(() => { inFlight = null; });
      return inFlight;
    },
    describe: () => cached?.source ?? fixed()?.source ?? 'unresolved'
  };
}
