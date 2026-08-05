/**
 * The S3 client (src/s3.js) against a real store: MinIO in development
 * (docs/s3-backup.md step 4). Skips unless something answers at the
 * endpoint -- the same rule the server suites skip by when a binary is
 * not built -- so `npm test` works on a machine with no MinIO, and a
 * machine WITH one (the documented dev setup) proves signing, paging,
 * key encoding and the error shape against real HTTP.
 *
 *   MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
 *     minio server ~/projects/minio-data --address :9000
 */
import { describe, it, expect } from 'vitest';
import http from 'node:http';
import https from 'node:https';
import { S3Client, S3Error } from '../src/s3.js';

const ENDPOINT = process.env.NISABA_S3_TEST_ENDPOINT ?? 'http://127.0.0.1:9000';
const CREDS = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'minioadmin',
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'minioadmin'
};

const reachable = await (async () => {
  try {
    const u = new URL(ENDPOINT);
    const mod = u.protocol === 'https:' ? https : http;
    await new Promise((resolve, reject) => {
      const req = mod.request(
        { host: u.hostname, port: u.port || undefined, method: 'GET', path: '/', timeout: 700 },
        (res) => { res.resume(); resolve(); });
      req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
      req.on('error', reject);
      req.end();
    });
    return true;
  } catch { return false; }
})();

describe.skipIf(!reachable)(`s3 client (against ${ENDPOINT})`, () => {
  const bucket = `nisaba-s3-test-${Date.now().toString(36)}`;
  const s3 = reachable ? new S3Client({ bucket, endpoint: ENDPOINT, ...CREDS }) : null;
  const made = new Set();
  const put = async (key, body, opts) => {
    made.add(key);
    return s3.putObject(key, body, opts);
  };

  it('createBucket is idempotent', async () => {
    await s3.createBucket();
    await s3.createBucket();   // already-ours is success
  });

  it('put/get/head/delete round-trips binary bytes', async () => {
    const body = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
    const { etag } = await put('roundtrip/all-bytes.bin', body);
    expect(etag).toBeTruthy();

    const back = await s3.getObject('roundtrip/all-bytes.bin');
    expect(back.equals(body)).toBe(true);

    const head = await s3.headObject('roundtrip/all-bytes.bin');
    expect(head.size).toBe(256);

    await s3.deleteObject('roundtrip/all-bytes.bin');
    expect(await s3.headObject('roundtrip/all-bytes.bin')).toBeNull();
    await s3.deleteObject('roundtrip/all-bytes.bin');   // the already-gone is fine
  });

  it('keys with spaces, plus, parens and unicode survive signing and listing', async () => {
    const key = "inst dev/gen 1/f+0 ('α') !.bj";
    await put(key, Buffer.from('odd name'));
    expect((await s3.getObject(key)).toString()).toBe('odd name');
    const { keys } = await s3.list('inst dev/');
    expect(keys.map((k) => k.key)).toContain(key);
  });

  it('list pages internally and honors prefix + delimiter', async () => {
    for (let i = 1; i <= 5; i++) {
      await put(`paged/gen-${i}/data.bin`, Buffer.alloc(i, i));
    }
    // Force real pagination: five objects, two per page.
    const { keys } = await s3.list('paged/', { maxKeysPerPage: 2 });
    expect(keys.length).toBe(5);
    expect(keys.map((k) => k.size).sort()).toEqual([1, 2, 3, 4, 5]);

    const { prefixes, keys: shallow } = await s3.list('paged/', { delimiter: '/' });
    expect(shallow.length).toBe(0);
    expect(prefixes.sort()).toEqual([
      'paged/gen-1/', 'paged/gen-2/', 'paged/gen-3/', 'paged/gen-4/', 'paged/gen-5/'
    ]);
  });

  it('a missing object is an S3Error with the store\'s own code', async () => {
    const err = await s3.getObject('nowhere/nothing.bin').catch((e) => e);
    expect(err).toBeInstanceOf(S3Error);
    expect(err.status).toBe(404);
    expect(err.code).toBe('NoSuchKey');
  });

  it('cleans up after itself', async () => {
    for (const key of made) await s3.deleteObject(key);
    expect((await s3.list('')).keys.length).toBe(0);
    // The bucket itself, through the raw seam -- the agent never
    // deletes buckets, so the client grows no verb for it.
    const res = await s3._request('DELETE', '');
    expect([204, 200]).toContain(res.status);
  });
});
