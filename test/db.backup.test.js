/**
 * The S3 backup agent (src/db-backup.js): a real nisaba-server on one
 * side, real MinIO on the other — docs/s3-backup.md step 5. Skips
 * unless BOTH are available: the native server binary (built by
 * ./wasm/build-server.sh --native) and something answering at
 * NISABA_S3_TEST_ENDPOINT (default http://127.0.0.1:9000, the
 * documented MinIO dev setup). What these prove: a shipped generation
 * in S3 is byte-identical to the member's on-disk generation, the
 * manifest commits it last, re-shipping is a no-op, retention prunes
 * manifest-first, the per-member prefix guard refuses a mixed prefix,
 * and watch ships on the member's own entries-driven cadence.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { spawn } from 'node:child_process';
import http from 'node:http';
import https from 'node:https';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer } from '../src/db-server-client.js';
import { S3Client } from '../src/s3.js';
import {
  BackupAgent, restoreFromS3, verifyManifestBytes, crc32,
  SNAP_PREFIX as BACKUP_SNAP_PREFIX
} from '../src/db-backup.js';

const NATIVE = 'wasm/lib/nisaba-server';
const haveNative = fs.existsSync(NATIVE);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const ENDPOINT = process.env.NISABA_S3_TEST_ENDPOINT ?? 'http://127.0.0.1:9000';
const CREDS = {
  accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'minioadmin',
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'minioadmin'
};
const haveMinio = await (async () => {
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

describe.skipIf(!haveNative || !haveMinio)('the backup agent: nisaba-server to MinIO', () => {
  const port = 21000 + (process.pid % 500);
  const bucket = `nisaba-backup-test-${Date.now().toString(36)}`;
  let proc = null;
  let dir = null;
  let client = null;
  let s3 = null;
  let agent = null;

  beforeAll(async () => {
    dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-backup-'));
    proc = spawn(path.resolve(NATIVE),
      ['--port', String(port), '--raft', '1', '--snapshot-entries', '8'],
      { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
    for (let i = 0; i < 100 && !client; i++) {
      client = await connectServer(`127.0.0.1:${port}`).catch(() => null);
      if (!client) await sleep(100);
    }
    if (!client) throw new Error('nisaba-server never started');
    s3 = new S3Client({ bucket, endpoint: ENDPOINT, ...CREDS });
    await s3.createBucket();
    agent = new BackupAgent({ client, s3, instance: 'ci', member: `127.0.0.1:${port}` });
    return async () => {
      // Empty the bucket and remove it: dev MinIO should not accumulate
      // a bucket per test run.
      for (const { key } of (await s3.list('')).keys) await s3.deleteObject(key);
      await s3._request('DELETE', '');
      await client.close().catch(() => {});
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
      fs.rmSync(dir, { recursive: true, force: true });
    };
  }, 60000);

  it('ships nothing when the member has no committed generation', async () => {
    expect(await agent.shipLatest()).toEqual({ shipped: false, absent: true });
  });

  it('ships the generation byte-identically, manifest last; re-shipping is a no-op', async () => {
    const users = client.db('appa').collection('users');
    for (let i = 0; i < 5; i++) await users.insertOne({ n: i });
    await client.db('appb').collection('things').insertOne({ n: 42 });

    const r = await agent.once({ keep: null });
    expect(r.shipped).toBe(true);
    expect(r.gen).toBe(1);

    // What landed in S3 is exactly the member's on-disk generation --
    // manifest FILE included, byte-identical, CRC trailer and all.
    const manifestBytes = await s3.getObject('ci/gen-1/manifest.bj');
    expect(manifestBytes.equals(
      fs.readFileSync(path.join(dir, `${BACKUP_SNAP_PREFIX}-1.manifest.bj`)))).toBe(true);
    const manifest = verifyManifestBytes(manifestBytes);
    expect(manifest.lastIncludedIndex).toBe(r.boundary);
    // The agent's own facts ride as metadata, never in the bytes.
    const head = await s3.headObject('ci/gen-1/manifest.bj');
    expect(head.metadata.member).toBe(`127.0.0.1:${port}`);
    expect(head.metadata.gen).toBe('1');
    expect(manifest.config.live.every((f) => f.name.includes('/'))).toBe(true);
    for (const f of manifest.files) {
      const shipped = await s3.getObject(`ci/gen-1/${f.role}.bj`);
      const disk = fs.readFileSync(path.join(dir, f.name));
      expect(shipped.equals(disk)).toBe(true);
      expect(crc32(shipped)).toBe(f.crc);
    }
    // The whole listing is the files plus the manifest -- nothing else.
    const { keys } = await s3.list('ci/gen-1/');
    expect(keys.length).toBe(manifest.files.length + 1);

    // Same generation again: recognized, not re-uploaded.
    expect((await agent.shipLatest()).shipped).toBe(false);
  }, 60000);

  it('a new generation ships and retention prunes the old, manifest first', async () => {
    await client.db('appa').collection('users').insertOne({ n: 99 });
    const r = await agent.once({ keep: 1 });
    expect(r.shipped).toBe(true);
    expect(r.gen).toBe(2);

    expect(await s3.headObject('ci/gen-2/manifest.bj')).not.toBeNull();
    expect(await s3.headObject('ci/gen-1/manifest.bj')).toBeNull();
    expect((await s3.list('ci/gen-1/')).keys.length).toBe(0);
  }, 60000);

  it('a prefix that belongs to another member refuses the run', async () => {
    const impostor = new BackupAgent({
      client, s3, instance: 'ci', member: 'somewhere.else:8097'
    });
    await expect(impostor.shipLatest()).rejects.toThrow(/belongs to member|holds generations from member/);
  });

  it('watch ships on the member\'s own entries-driven cadence', async () => {
    const abort = new AbortController();
    const done = agent.watch({ pollMs: 150, keep: 2, signal: abort.signal });
    try {
      // Push the member past --snapshot-entries 8: it snapshots itself,
      // base moves, and the watcher must notice and ship gen 3 with no
      // explicit snapshot from anyone.
      const users = client.db('appa').collection('users');
      for (let i = 0; i < 12; i++) await users.insertOne({ w: i });
      const until = Date.now() + 20_000;
      let manifest = null;
      while (!manifest && Date.now() < until) {
        manifest = await s3.headObject('ci/gen-3/manifest.bj');
        if (!manifest) await sleep(200);
      }
      expect(manifest).not.toBeNull();
      // Retention rode along: gen 1 (already pruned) and now gen 2 fall
      // outside --keep 2? No: keep 2 holds {3, 2}. Gen 2 survives.
      expect(await s3.headObject('ci/gen-2/manifest.bj')).not.toBeNull();
    } finally {
      abort.abort();
      await done;
    }
  }, 60000);
});

describe.skipIf(!haveNative || !haveMinio)('the restore half: S3 back to a serving member', () => {
  const port = 21600 + (process.pid % 300);
  const bucket = `nisaba-restore-test-${Date.now().toString(36)}`;
  let srcProc = null;
  let srcDir = null;
  let client = null;
  let s3 = null;
  let counts = null;   // the live truth the restore must reproduce

  const startAt = async (dir, p) => {
    const proc = spawn(path.resolve(NATIVE),
      ['--port', String(p), '--raft', '1'],
      { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
    let stderr = '';
    proc.stderr.on('data', (c) => { stderr += c; });
    let c = null;
    for (let i = 0; i < 100 && !c; i++) {
      c = await connectServer(`127.0.0.1:${p}`).catch(() => null);
      if (!c) await sleep(100);
    }
    if (!c) { proc.kill(); throw new Error(`server never started: ${stderr}`); }
    return { proc, client: c, errors: () => stderr };
  };

  beforeAll(async () => {
    srcDir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-restore-src-'));
    ({ proc: srcProc, client } = await startAt(srcDir, port));
    s3 = new S3Client({ bucket, endpoint: ENDPOINT, ...CREDS });
    await s3.createBucket();

    const users = client.db('appa').collection('users');
    for (let i = 0; i < 7; i++) await users.insertOne({ n: i });
    await client.db('appb').collection('things').insertOne({ n: 42 });
    const agent = new BackupAgent({ client, s3, instance: 'dr', member: `127.0.0.1:${port}` });
    await agent.once();
    counts = { appa: 7, appb: 1 };

    return async () => {
      for (const { key } of (await s3.list('')).keys) await s3.deleteObject(key);
      await s3._request('DELETE', '');
      await client.close().catch(() => {});
      srcProc.kill();
      await new Promise((r) => srcProc.once('exit', r));
      fs.rmSync(srcDir, { recursive: true, force: true });
    };
  }, 60000);

  it('the canonical prefix has one spelling across agent, WAL host and C server', async () => {
    expect(BACKUP_SNAP_PREFIX).toBe('__snap__');
    const replica = fs.readFileSync('server/replica.c', 'utf8');
    expect(replica).toMatch(/#define\s+REPLICA_SNAP_PREFIX\s+"__snap__"/);
  });

  it('a restored root boots, adopts, serves the data, and takes writes', async () => {
    const into = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-restore-dst-')) + '/root';
    const r = await restoreFromS3({ s3, instance: 'dr', into });
    expect(r.gen).toBe(1);
    expect(r.lastIncludedIndex).toBeGreaterThan(0);

    // The restored directory is exactly the snapstore shape: the
    // generation's files and the manifest, nothing else.
    const names = fs.readdirSync(into).sort();
    expect(names).toContain(`${BACKUP_SNAP_PREFIX}-1.manifest.bj`);
    expect(names.length).toBe(r.files + 1);

    const dst = await startAt(into, port + 1);
    try {
      expect(dst.errors()).toMatch(/restoring snapshot at index/);
      expect(await dst.client.db('appa').collection('users').countDocuments({})).toBe(counts.appa);
      expect(await dst.client.db('appb').collection('things').countDocuments({})).toBe(counts.appb);
      // A new life: it takes writes of its own.
      await dst.client.db('appa').collection('users').insertOne({ fresh: true });
      expect(await dst.client.db('appa').collection('users').countDocuments({})).toBe(counts.appa + 1);
    } finally {
      await dst.client.close().catch(() => {});
      dst.proc.kill();
      await new Promise((res) => dst.proc.once('exit', res));
    }
    fs.rmSync(path.dirname(into), { recursive: true, force: true });
  }, 60000);

  it('refuses a non-empty directory: restore never merges', async () => {
    const into = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-restore-full-'));
    fs.writeFileSync(path.join(into, 'somebody-elses.txt'), 'data');
    await expect(restoreFromS3({ s3, instance: 'dr', into }))
      .rejects.toThrow(/not empty/);
    fs.rmSync(into, { recursive: true, force: true });
  });

  it('an interrupted restore is the crashed-attempt shape: swept at boot, never adopted', async () => {
    // Everything but the manifest -- exactly what a crash mid-download
    // leaves, because the manifest is written last.
    const into = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-restore-torn-')) + '/root';
    fs.mkdirSync(into);
    const manifest = verifyManifestBytes(await s3.getObject('dr/gen-1/manifest.bj'));
    for (const f of manifest.files) {
      fs.writeFileSync(path.join(into, f.name), await s3.getObject(`dr/gen-1/${f.role}.bj`));
    }

    const dst = await startAt(into, port + 2);
    try {
      // No manifest: the generation never existed. The server boots
      // EMPTY and the store's own sweep removes the orphaned files.
      expect(await dst.client.listDatabases()).toEqual([]);
      expect(fs.readdirSync(into).filter((n) => n.startsWith(`${BACKUP_SNAP_PREFIX}-`)).length).toBe(0);
    } finally {
      await dst.client.close().catch(() => {});
      dst.proc.kill();
      await new Promise((res) => dst.proc.once('exit', res));
    }
    fs.rmSync(path.dirname(into), { recursive: true, force: true });
  }, 60000);
});
