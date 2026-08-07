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

  /** `total` bytes in `chunk`-sized pieces, generated as they are asked
   * for -- a producer that never holds the whole thing, which is the
   * only kind worth testing a streaming upload with. */
  async function* generated(total, chunk = 1024 * 1024, seed = 7) {
    let sent = 0;
    while (sent < total) {
      const n = Math.min(chunk, total - sent);
      const b = Buffer.allocUnsafe(n);
      b.fill((seed + (sent / chunk)) % 251);
      sent += n;
      yield b;
    }
  }

  const PART = 5 * 1024 * 1024;    // S3's floor, so the test crosses it cheaply

  it('takes the single-PUT path when the content fits in one part', async () => {
    made.add('stream/small.bin');
    const body = Buffer.from('not big enough to bother');
    const r = await s3.putObjectStream(
      'stream/small.bin', (async function* () { yield body; })(), { partSize: PART });

    // No multipart at all: one request, and the bytes were complete
    // before any were sent.
    expect(r.parts).toBe(0);
    expect(r.bytes).toBe(body.length);
    expect((await s3.getObject('stream/small.bin')).equals(body)).toBe(true);
  });

  it('an empty body is an object, not a nothing', async () => {
    made.add('stream/empty.bin');
    const r = await s3.putObjectStream('stream/empty.bin', (async function* () {})(), { partSize: PART });
    expect(r).toMatchObject({ parts: 0, bytes: 0 });
    expect((await s3.headObject('stream/empty.bin')).size).toBe(0);
  });

  it('switches to multipart when it outgrows a part, and the bytes survive', async () => {
    // 12 MiB over a 5 MiB part: two full parts and a short last one,
    // which is the only part allowed to be short.
    made.add('stream/multi.bin');
    const total = 12 * 1024 * 1024;
    const r = await s3.putObjectStream('stream/multi.bin', generated(total), { partSize: PART });
    expect(r.parts).toBe(3);
    expect(r.bytes).toBe(total);

    const back = await s3.getObject('stream/multi.bin');
    expect(back.length).toBe(total);
    // Spot-check across two part boundaries: a mis-ordered or
    // mis-sliced part shows up here and nowhere else.
    const expected = Buffer.concat(await (async () => {
      const out = []; for await (const c of generated(total)) out.push(c); return out;
    })());
    expect(back.equals(expected)).toBe(true);
  });

  it('carries metadata through a multipart upload', async () => {
    // The x-amz-meta-* headers ride on the CREATE, not on the parts and
    // not on the completion -- an easy thing to attach to the wrong
    // request, and the manifest's member/boundary facts depend on it.
    made.add('stream/meta.bin');
    await s3.putObjectStream('stream/meta.bin', generated(11 * 1024 * 1024), {
      partSize: PART, metadata: { member: '10.0.0.1:20050', gen: '7' }
    });
    const head = await s3.headObject('stream/meta.bin');
    expect(head.metadata).toMatchObject({ member: '10.0.0.1:20050', gen: '7' });
  });

  it('chunks that do not line up with the part size still make the right object', async () => {
    made.add('stream/ragged.bin');
    const total = 11 * 1024 * 1024 + 12345;
    const r = await s3.putObjectStream('stream/ragged.bin', generated(total, 777_777), { partSize: PART });
    expect(r.bytes).toBe(total);
    expect((await s3.headObject('stream/ragged.bin')).size).toBe(total);
  });

  it('aborts the upload when the producer fails, leaving no object', async () => {
    /*
     * A CRC that only fails at the end -- exactly how a corrupted
     * generation file surfaces -- must not leave an object behind
     * claiming to be a backup. An abandoned multipart upload is also
     * billed while it exists and invisible in a listing, so it is
     * cleaned up rather than left for a lifecycle rule to find.
     */
    async function* failsLate() {
      for await (const c of generated(11 * 1024 * 1024)) yield c;
      throw new Error('CRC mismatch in transit');
    }
    await expect(s3.putObjectStream('stream/aborted.bin', failsLate(), { partSize: PART }))
      .rejects.toThrow(/CRC mismatch/);
    expect(await s3.headObject('stream/aborted.bin')).toBeNull();
  });

  it('holds a part, not a file: 192 MiB streams without 192 MiB of buffers', async () => {
    /*
     * THE POINT OF ALL OF THIS. The old path read a generation file
     * whole and `Buffer.concat`ed it, so peak memory was twice the
     * largest database file -- on the machine running the tenants,
     * where an OOM takes more than the backup with it.
     *
     * `arrayBuffers` is Buffer memory specifically, which is what used
     * to balloon. The bound is generous (a part is 5 MiB here) because
     * the claim is about the SHAPE of the curve, not a byte count: flat
     * in the size of the content rather than linear in it.
     */
    made.add('stream/big.bin');
    const total = 192 * 1024 * 1024;
    const base = process.memoryUsage().arrayBuffers;
    let peak = 0;
    const watch = setInterval(() => {
      peak = Math.max(peak, process.memoryUsage().arrayBuffers - base);
    }, 20);
    try {
      const r = await s3.putObjectStream('stream/big.bin', generated(total, 4 * 1024 * 1024), { partSize: PART });
      expect(r.bytes).toBe(total);
      expect(r.parts).toBeGreaterThan(30);
    } finally {
      clearInterval(watch);
    }
    expect((await s3.headObject('stream/big.bin')).size).toBe(total);
    expect(peak).toBeLessThan(64 * 1024 * 1024);
  }, 120_000);

  it('cleans up after itself', async () => {
    for (const key of made) await s3.deleteObject(key);
    expect((await s3.list('')).keys.length).toBe(0);
    // The bucket itself, through the raw seam -- the agent never
    // deletes buckets, so the client grows no verb for it.
    const res = await s3._request('DELETE', '');
    expect([204, 200]).toContain(res.status);
  });
});
