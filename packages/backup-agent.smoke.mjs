/**
 * The smallest proof the assembled backup-agent package works. Lives
 * beside the package (see backup-agent.build.mjs's header) and runs
 * against the assembled files inside it, after `npm run build`.
 *
 * Two layers, the same split every package smoke here makes:
 *
 *  - always: the closure is complete — the agent and the S3 client
 *    import from the package alone, the CRC is bjfile's, a manifest
 *    round-trips through its own commit rule, and restore's
 *    empty-directory refusal fires before any network is touched.
 *  - when something answers at MinIO's port (the documented dev
 *    setup, the same probe test/s3.test.js skips by): a LIVE
 *    round-trip through the packaged S3 client — bucket, put, get,
 *    delete. The full ship/restore cycle is the repository's
 *    test/db.backup.test.js's job, not a smoke's.
 */
import fs from 'node:fs';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/

const {
  BackupAgent, restoreFromS3, shipGenerationFromDir,
  crc32, verifyManifestBytes, SNAP_PREFIX
} = await import('./backup-agent/src/db-backup.js');
const { S3Client, S3Error } = await import('./backup-agent/src/s3.js');
const { encode } = await import('./backup-agent/third_party/binjson/js/binjson.js');

/* The closure, without a network. */
if (crc32(new TextEncoder().encode('123456789')) !== 0xCBF43926) {
  throw new Error('crc32 is not the zlib CRC-32');
}
const record = { gen: 3, lastIncludedIndex: 41, files: [] };
const body = encode(record);
const trailer = crc32(body);
const manifestBytes = new Uint8Array(body.length + 4);
manifestBytes.set(body, 0);
new DataView(manifestBytes.buffer).setUint32(body.length, trailer, true);
if (verifyManifestBytes(manifestBytes).gen !== 3) throw new Error('a valid manifest failed to verify');
manifestBytes[0] ^= 0xff;
let torn = false;
try { verifyManifestBytes(manifestBytes); } catch { torn = true; }
if (!torn) throw new Error('a torn manifest verified');

const s3 = new S3Client({
  bucket: 'smoke', endpoint: 'http://127.0.0.1:9000',
  accessKeyId: 'k', secretAccessKey: 's'
});
if (!(new S3Error(404, 'NoSuchKey', 'x') instanceof Error)) throw new Error('S3Error is not an Error');
if (typeof BackupAgent !== 'function' || typeof shipGenerationFromDir !== 'function') {
  throw new Error('the agent surface did not import');
}
if (!SNAP_PREFIX.startsWith('__')) throw new Error('SNAP_PREFIX is not itself');

const occupied = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-backup-pkg-'));
fs.writeFileSync(path.join(occupied, 'occupant'), 'x');
let refused = false;
try { await restoreFromS3({ s3, instance: 'i', into: occupied }); } catch (err) {
  refused = /not empty/.test(err.message);
}
fs.rmSync(occupied, { recursive: true, force: true });
if (!refused) throw new Error('restore did not refuse a non-empty directory');
console.log('smoke ok: the closure imports, the CRC and the commit rule hold');

/* The live half, against a reachable MinIO. */
const ENDPOINT = process.env.NISABA_S3_TEST_ENDPOINT ?? 'http://127.0.0.1:9000';
const reachable = await new Promise((resolve) => {
  const u = new URL(ENDPOINT);
  const req = http.request(
    { host: u.hostname, port: u.port || 80, method: 'GET', path: '/', timeout: 700 },
    (res) => { res.resume(); resolve(true); });
  req.on('timeout', () => { req.destroy(); resolve(false); });
  req.on('error', () => resolve(false));
  req.end();
});
if (!reachable) {
  console.log(`smoke skipped the live round-trip: nothing answers at ${ENDPOINT} ` +
              '(MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin minio server …)');
  process.exit(0);
}

const live = new S3Client({
  bucket: `nisaba-backup-smoke-${Date.now().toString(36)}`,
  endpoint: ENDPOINT,
  accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'minioadmin',
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'minioadmin'
});
await live.createBucket();
await live.putObject('gen-1/manifest.bj', manifestBytes, { metadata: { member: 'smoke:1' } });
const head = await live.headObject('gen-1/manifest.bj');
if (head?.metadata.member !== 'smoke:1') throw new Error('metadata did not ride the object');
await live.deleteObject('gen-1/manifest.bj');
console.log('smoke ok: a live round-trip against the object store');
