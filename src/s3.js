/**
 * s3.js — the S3 client the backup agent stands on (docs/s3-backup.md
 * step 4): SigV4 over node:http/https, and nothing else. No AWS SDK,
 * for the same reason every transport in this repository is
 * zero-dependency: the surface needed is five verbs, SigV4 is a page of
 * node:crypto, and a dependency is a second implementation of exactly
 * the part that must not drift.
 *
 * PATH-STYLE ADDRESSING, always: `endpoint/bucket/key`. It is MinIO's
 * default and the form that works against any S3-compatible store
 * without DNS games. `endpoint` picks the target — MinIO in
 * development (`http://127.0.0.1:9000`), AWS by omitting it (the
 * regional `https://s3.<region>.amazonaws.com`).
 *
 * BODIES ARE BUFFERS, and every PUT is single-part, signed over the
 * real payload hash — the strongest integrity SigV4 offers. The 5 GB
 * single-PUT ceiling is refused loudly by S3 itself; multipart is the
 * follow-up when a generation file crosses it, and the failure until
 * then names the limit rather than truncating (docs/s3-backup.md).
 *
 * THREE THINGS THAT ONLY MATTER AGAINST AWS, and so are easy to leave
 * out when the only store you ever run against is a MinIO on loopback:
 * temporary credentials (`sessionToken`, without which an instance
 * role's key pair is refused), retry with backoff (AWS answers `503
 * SlowDown` under load and expects it), and a socket timeout (nothing
 * on loopback ever goes quiet). None of them change a byte of what is
 * stored; all of them decide whether a backup happens at all.
 *
 * ListObjectsV2 pages internally (`encoding-type=url`, so keys with
 * any byte S3 allows round-trip), and every non-2xx answer becomes an
 * S3Error carrying the status and S3's own <Code>/<Message> — a
 * refusal is a response here too.
 */
import http from 'node:http';
import https from 'node:https';
import { createHash, createHmac } from 'node:crypto';

/** A non-2xx answer from the store: `status` is HTTP's, `code` is
 * S3's (<Code> in the error body; '' when the body carried none). */
export class S3Error extends Error {
  constructor(status, code, message) {
    super(message || `${code || 'S3 error'} (HTTP ${status})`);
    this.name = 'S3Error';
    this.status = status;
    this.code = code || '';
  }
}

/** What S3 asks a client to come back for rather than give up on:
 * throttling and its own transient faults. Never a 4xx — that is an
 * answer. */
const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);

/** A connection that failed in a way another attempt might not. */
const RETRYABLE_CODES = new Set([
  'ECONNRESET', 'ECONNREFUSED', 'EPIPE', 'ETIMEDOUT', 'EAI_AGAIN', 'ENOTFOUND', 'EHOSTUNREACH'
]);
const isRetryableNetworkError = (err) => RETRYABLE_CODES.has(err?.code);

const sha256 = (data) => createHash('sha256').update(data).digest('hex');
const hmac = (key, data) => createHmac('sha256', key).update(data).digest();

/** RFC 3986, which is stricter than encodeURIComponent about !'()*. */
const rfc3986 = (s) =>
  encodeURIComponent(s).replace(/[!'()*]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);

/** A key is a path: each segment encoded, the '/'s kept. */
const encodeKey = (key) => key.split('/').map(rfc3986).join('/');

const XML_ENTITIES = { '&amp;': '&', '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'" };
const decodeXml = (s) => s
  .replace(/&#x([0-9a-fA-F]+);/g, (_, h) => String.fromCodePoint(parseInt(h, 16)))
  .replace(/&#(\d+);/g, (_, d) => String.fromCodePoint(Number(d)))
  .replace(/&(?:amp|lt|gt|quot|apos);/g, (m) => XML_ENTITIES[m]);

/** Every <tag>...</tag> text in `xml`, in order. Not an XML parser —
 * S3's list and error documents are flat, and a parser dependency for
 * two element names would be the tail wagging the dog. */
const xmlAll = (xml, tag) => {
  const out = [];
  const re = new RegExp(`<${tag}>([^<]*)</${tag}>`, 'g');
  for (let m; (m = re.exec(xml)) !== null; ) out.push(decodeXml(m[1]));
  return out;
};
const xmlOne = (xml, tag) => xmlAll(xml, tag)[0];

export class S3Client {
  /**
   * @param {object} options
   * @param {string} options.bucket
   * @param {string} [options.endpoint] - e.g. 'http://127.0.0.1:9000'
   *   (MinIO); omitted, the AWS regional endpoint for `region`, which
   *   is then REQUIRED -- see below
   * @param {string} [options.region='us-east-1'] - required when
   *   `endpoint` is omitted
   * @param {string} [options.accessKeyId] - default env AWS_ACCESS_KEY_ID
   * @param {string} [options.secretAccessKey] - default env AWS_SECRET_ACCESS_KEY
   * @param {string} [options.sessionToken] - default env AWS_SESSION_TOKEN.
   *   Temporary credentials (an instance role, an assumed role, a CI
   *   OIDC exchange) are a triple, and the third part is not optional:
   *   without it AWS rejects the other two. Absent for a static key
   *   pair and for MinIO, where it is simply never sent.
   * @param {number} [options.maxAttempts=3] - see `_request`
   * @param {number} [options.socketTimeoutMs=60000] - inactivity, not
   *   total: a 4 GB PUT is allowed to take as long as it takes, a
   *   silent socket is not
   */
  constructor({
    bucket, endpoint, region, accessKeyId, secretAccessKey, sessionToken,
    maxAttempts = 3, socketTimeoutMs = 60_000
  } = {}) {
    if (!bucket) throw new Error('S3Client needs a bucket');
    /*
     * REGION IS REQUIRED WHEN THE ENDPOINT IS NOT GIVEN, because then
     * it picks the host AND signs the request, and a wrong guess fails
     * as `AuthorizationHeaderMalformed` or a 301 with no body -- which
     * reads like a broken client rather than an unset variable. An
     * S3-compatible store addressed by endpoint does not care, so it
     * keeps the harmless default.
     */
    if (!endpoint && !region) {
      throw new Error(
        'S3Client needs a region when no endpoint is given: it selects the AWS host and signs ' +
        'every request, and the wrong one is refused as a malformed signature. Set AWS_REGION.'
      );
    }
    this.bucket = bucket;
    this.region = region ?? 'us-east-1';
    this.accessKeyId = accessKeyId ?? process.env.AWS_ACCESS_KEY_ID;
    this.secretAccessKey = secretAccessKey ?? process.env.AWS_SECRET_ACCESS_KEY;
    this.sessionToken = sessionToken ?? process.env.AWS_SESSION_TOKEN ?? null;
    this.maxAttempts = Math.max(1, maxAttempts);
    this.socketTimeoutMs = socketTimeoutMs;
    if (!this.accessKeyId || !this.secretAccessKey) {
      throw new Error('S3Client needs credentials: accessKeyId/secretAccessKey or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY');
    }
    const url = new URL(endpoint ?? `https://s3.${this.region}.amazonaws.com`);
    this._https = url.protocol === 'https:';
    this._host = url.hostname;
    this._port = url.port ? Number(url.port) : (this._https ? 443 : 80);
    /** The host header SigV4 signs: no port when it is the default. */
    this._hostHeader = url.port ? `${url.hostname}:${url.port}` : url.hostname;
  }

  /* ---- one signed request ------------------------------------------- */

  /**
   * One signed attempt, retried when S3 says to.
   *
   * RETRY IS NOT OPTIONAL AGAINST AWS. It answers `503 SlowDown` and
   * bare `500 InternalError` under load and documents that clients back
   * off and try again; the SDKs do it invisibly, which is why code
   * written against MinIO -- where it never happens -- looks finished
   * and is not. A dropped socket is the same story from the other end.
   *
   * Every verb here is idempotent (S3 PUT replaces, DELETE of the gone
   * is success), so a retry cannot half-apply anything. What it can do
   * is turn a stampede into a worse stampede, hence exponential backoff
   * with jitter rather than a tight loop.
   *
   * A 4xx is never retried: it is an answer, not a hiccup.
   */
  async _request(method, key, opts = {}) {
    let lastErr = null;
    for (let attempt = 1; ; attempt++) {
      try {
        const res = await this._send(method, key, opts);
        if (!RETRYABLE_STATUS.has(res.status) || attempt >= this.maxAttempts) return res;
        lastErr = new S3Error(res.status, xmlOne(res.body.toString('utf8'), 'Code'), `HTTP ${res.status}`);
      } catch (err) {
        if (!isRetryableNetworkError(err) || attempt >= this.maxAttempts) throw err;
        lastErr = err;
      }
      // 100ms, 200ms, 400ms ... each with up to its own width of
      // jitter, so a fleet that was throttled together does not come
      // back together.
      const base = 100 * 2 ** (attempt - 1);
      await new Promise((r) => setTimeout(r, base + Math.floor(Math.random() * base)));
    }
    // eslint-disable-next-line no-unreachable
    throw lastErr;
  }

  async _send(method, key, { query = {}, body = null, contentType = null, metadata = null } = {}) {
    const path = `/${rfc3986(this.bucket)}${key ? `/${encodeKey(key)}` : ''}`;
    const qs = Object.keys(query).sort()
      .map((k) => `${rfc3986(k)}=${rfc3986(String(query[k]))}`)
      .join('&');

    const now = new Date().toISOString().replace(/[-:]|\.\d{3}/g, ''); // YYYYMMDDTHHMMSSZ
    const day = now.slice(0, 8);
    const payloadHash = sha256(body ?? '');

    const headers = {
      host: this._hostHeader,
      'x-amz-content-sha256': payloadHash,
      'x-amz-date': now
    };
    // Temporary credentials carry their token as a SIGNED header --
    // it goes in before signedNames is taken, or the signature covers
    // a request that is not the one being sent.
    if (this.sessionToken) headers['x-amz-security-token'] = this.sessionToken;
    if (contentType) headers['content-type'] = contentType;
    if (metadata) {
      for (const [k, v] of Object.entries(metadata)) {
        headers[`x-amz-meta-${k.toLowerCase()}`] = String(v);
      }
    }

    const signedNames = Object.keys(headers).sort();
    const canonical = [
      method, path, qs,
      ...signedNames.map((h) => `${h}:${String(headers[h]).trim()}`),
      '',
      signedNames.join(';'),
      payloadHash
    ].join('\n');

    const scope = `${day}/${this.region}/s3/aws4_request`;
    const toSign = ['AWS4-HMAC-SHA256', now, scope, sha256(canonical)].join('\n');
    const kSigning = hmac(hmac(hmac(hmac(`AWS4${this.secretAccessKey}`, day),
      this.region), 's3'), 'aws4_request');
    const signature = hmac(kSigning, toSign).toString('hex');

    headers.authorization =
      `AWS4-HMAC-SHA256 Credential=${this.accessKeyId}/${scope}, ` +
      `SignedHeaders=${signedNames.join(';')}, Signature=${signature}`;
    if (body !== null) headers['content-length'] = String(body.length);

    const transport = this._https ? https : http;
    return new Promise((resolve, reject) => {
      const req = transport.request({
        host: this._host, port: this._port, method,
        path: qs ? `${path}?${qs}` : path,
        headers
      }, (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () => resolve({
          status: res.statusCode,
          headers: res.headers,
          body: Buffer.concat(chunks)
        }));
        res.on('error', reject);
      });
      /*
       * INACTIVITY, not a deadline. A generation file may be gigabytes
       * and is allowed to take as long as the wire needs; a socket that
       * has said nothing for a minute is not slow, it is gone. Without
       * this a half-open connection hangs a backup until something
       * further out kills it, which on a fleet means a routine stuck
       * `running` and never due again.
       */
      req.setTimeout(this.socketTimeoutMs, () => {
        req.destroy(Object.assign(
          new Error(`no data for ${this.socketTimeoutMs}ms from ${this._hostHeader}`),
          { code: 'ETIMEDOUT' }
        ));
      });
      req.on('error', reject);
      if (body !== null) req.write(body);
      req.end();
    });
  }

  _throw(res) {
    const text = res.body.toString('utf8');
    // S3 names the right region on a mismatch; passing that through
    // turns "malformed signature" into an instruction.
    const elsewhere = res.headers?.['x-amz-bucket-region'];
    const hint = elsewhere && elsewhere !== this.region
      ? ` (bucket '${this.bucket}' is in ${elsewhere}, this client signed for ${this.region})`
      : '';
    throw new S3Error(res.status, xmlOne(text, 'Code'), `${xmlOne(text, 'Message') ?? ''}${hint}`);
  }

  /* ---- the five verbs ------------------------------------------------ */

  /** Make the bucket; one that already exists (and is yours) is success,
   * because "make sure the bucket exists" is what every caller means. */
  async createBucket() {
    const res = await this._request('PUT', '');
    if (res.status === 200) return;
    if (res.status === 409) {
      const code = xmlOne(res.body.toString('utf8'), 'Code');
      if (code === 'BucketAlreadyOwnedByYou' || code === 'BucketAlreadyExists') return;
    }
    this._throw(res);
  }

  /** `metadata` rides as x-amz-meta-* headers (signed like any header)
   * and comes back from headObject -- facts ABOUT an object that must
   * not change its bytes, e.g. which member a manifest came from. */
  async putObject(key, body, { contentType = 'application/octet-stream', metadata = null } = {}) {
    const res = await this._request('PUT', key, { body, contentType, metadata });
    if (res.status !== 200) this._throw(res);
    return { etag: res.headers.etag ?? null };
  }

  async getObject(key) {
    const res = await this._request('GET', key);
    if (res.status !== 200) this._throw(res);
    return res.body;
  }

  /** { size, etag, metadata }, or null when there is no such object --
   * absence is an answer here, not an exception, because callers probe. */
  async headObject(key) {
    const res = await this._request('HEAD', key);
    if (res.status === 404) return null;
    if (res.status !== 200) throw new S3Error(res.status, '', `HEAD ${key} (HTTP ${res.status})`);
    const metadata = {};
    for (const [h, v] of Object.entries(res.headers)) {
      if (h.startsWith('x-amz-meta-')) metadata[h.slice('x-amz-meta-'.length)] = v;
    }
    return { size: Number(res.headers['content-length']), etag: res.headers.etag ?? null, metadata };
  }

  /** Deleting the already-gone is what the caller asked for (204 both ways). */
  async deleteObject(key) {
    const res = await this._request('DELETE', key);
    if (res.status !== 204 && res.status !== 200) this._throw(res);
  }

  /**
   * Every key under `prefix`, paged internally until the listing is
   * complete -- no silent caps. `delimiter: '/'` answers "what
   * directories are here": keys stop at the delimiter and the common
   * prefixes come back separately.
   */
  async list(prefix, { delimiter = null, maxKeysPerPage = 1000 } = {}) {
    const keys = [];
    const prefixes = [];
    let token = null;
    for (;;) {
      const query = {
        'list-type': '2', 'encoding-type': 'url',
        'max-keys': String(maxKeysPerPage)
      };
      if (prefix) query.prefix = prefix;
      if (delimiter) query.delimiter = delimiter;
      if (token) query['continuation-token'] = token;
      const res = await this._request('GET', '', { query });
      if (res.status !== 200) this._throw(res);
      const xml = res.body.toString('utf8');

      // encoding-type=url: keys and prefixes come URL-encoded, in the
      // query-string dialect -- '+' is a space.
      const decodeListed = (s) => decodeURIComponent(s.replace(/\+/g, '%20'));
      for (const m of xml.matchAll(/<Contents>([\s\S]*?)<\/Contents>/g)) {
        keys.push({
          key: decodeListed(xmlOne(m[1], 'Key')),
          size: Number(xmlOne(m[1], 'Size'))
        });
      }
      // (The listing echoes the REQUEST prefix in a bare <Prefix>
      // element; only ones inside <CommonPrefixes> are answers.)
      for (const m of xml.matchAll(/<CommonPrefixes>([\s\S]*?)<\/CommonPrefixes>/g)) {
        prefixes.push(decodeListed(xmlOne(m[1], 'Prefix')));
      }
      if (xmlOne(xml, 'IsTruncated') !== 'true') break;
      token = xmlOne(xml, 'NextContinuationToken');
      if (!token) break;
    }
    return { keys, prefixes };
  }
}
