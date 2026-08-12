/**
 * The instance layer over the WAL host (src/db-wal-instance.js): one
 * root, many databases, one log, one snapshot store — the C server's
 * shape (db_instance.h), spoken by JavaScript. docs/s3-backup.md ("One
 * snapshot, two hosts") step 2.
 *
 * The cross-host suite at the bottom is the point of the whole layer:
 * a root written by this host is served by the real nisaba-server, a
 * root written by the server opens here, and an entry FORGED into the
 * log by this host (durable but unapplied — the crashed-before-apply
 * shape) is replayed by the C process. It skips unless the native
 * server has been built (./build/build-server.sh --native), the same
 * rule test/db.server.test.js skips by.
 */
import { describe, it, expect } from 'vitest';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import {
  ready, ObjectId, EntryLog, ENTRYLOG_TYPE, decode, encode, walPlan, WAL_REQ
} from '../src/nisaba-wasm.js';
import { MemoryStorageProvider } from '../src/db.js';
import { NodeFSStorageProvider } from '../src/db-node.js';
import { connectWal, WAL_FILE } from '../src/db-wal.js';
import { connectWalInstance, restoreLatestInstanceSnapshot } from '../src/db-wal-instance.js';
import { connectServer } from '../src/db-server-client.js';

await ready();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function stateOf(idb, name) {
  const col = await idb.collection(name);
  return (await col.find({}, { sort: { _id: 1 } }).toArray())
    .map((d) => ({ ...d, _id: d._id.toHexString() }));
}

/** Every NORMAL entry's decoded payload, in log order. */
function normalEntries(log) {
  const out = [];
  let from = log.baseIndex + 1;
  while (from <= log.lastIndex) {
    const batch = log.getBatch(from, 1 << 20);
    if (batch.length === 0) break;
    for (const e of batch) {
      if (e.type === ENTRYLOG_TYPE.NORMAL) out.push({ index: e.index, env: decode(e.payload) });
    }
    from = batch[batch.length - 1].index + 1;
  }
  return out;
}

/** A durable-but-unapplied entry: the crashed-before-apply shape,
 * forged through the same planner and envelope the live path uses. */
function forgeUnapplied(inst, db, coll, doc) {
  const { commands } = walPlan(null, coll, WAL_REQ.INSERT_ONE, doc);
  inst._propose([encode({ d: db, c: decode(commands[0]) })]);
}

describe('WAL instance: one log, many databases', () => {
  it('routes writes by envelope; the log records { d, c }', async () => {
    const provider = new MemoryStorageProvider();
    const inst = await connectWalInstance(provider);
    const users = await (await inst.db('analytics')).collection('users');
    const bills = await (await inst.db('billing')).collection('invoices');
    await users.insertOne({ _id: oid(1), who: 'ada' });
    await bills.insertOne({ _id: oid(2), amount: 7 });
    await users.insertOne({ _id: oid(3), who: 'grace' });

    expect(await users.countDocuments({})).toBe(2);
    expect(await bills.countDocuments({})).toBe(1);
    expect((await inst.listDatabases()).sort()).toEqual(['analytics', 'billing']);

    const entries = normalEntries(inst.log);
    expect(entries.map((e) => e.env.d)).toEqual(['analytics', 'billing', 'analytics']);
    expect(entries[0].env.c.c).toBe('users');   // the command inside is db_wal.h's, untouched
    await inst.close();
  });

  it('recovery replays a durable-but-unapplied suffix, routed by envelope', async () => {
    const provider = new MemoryStorageProvider();
    let inst = await connectWalInstance(provider);
    const users = await (await inst.db('analytics')).collection('users');
    await users.insertOne({ _id: oid(1), i: 1 });
    forgeUnapplied(inst, 'analytics', 'users', { _id: oid(2), forged: true });
    forgeUnapplied(inst, 'billing', 'invoices', { _id: oid(3), forged: true });
    await inst.close();

    inst = await connectWalInstance(provider);
    const a = await (await inst.db('analytics')).collection('users');
    expect(await a.countDocuments({})).toBe(2);
    expect((await a.findOne({ _id: oid(2) })).forged).toBe(true);
    // The billing database did not exist before replay: created by its
    // entry, as dbi_apply creates on a first insert.
    const b = await (await inst.db('billing')).collection('invoices');
    expect((await b.findOne({ _id: oid(3) })).forged).toBe(true);
    await inst.close();
  });

  it('dropDatabase is a logged act and stays dropped across reopen', async () => {
    const provider = new MemoryStorageProvider();
    let inst = await connectWalInstance(provider);
    await (await (await inst.db('analytics')).collection('users')).insertOne({ _id: oid(1) });
    await (await (await inst.db('billing')).collection('invoices')).insertOne({ _id: oid(2) });

    const { dropped } = await inst.dropDatabase('billing');
    expect(dropped).toBe(true);
    expect(await inst.listDatabases()).toEqual(['analytics']);
    const entries = normalEntries(inst.log);
    expect(entries[entries.length - 1].env).toEqual({ d: 'billing', i: 'drop' });
    // Dropping the already-gone is what the caller asked for.
    expect((await inst.dropDatabase('nowhere')).dropped).toBe(false);
    await inst.close();

    inst = await connectWalInstance(provider);
    expect(await inst.listDatabases()).toEqual(['analytics']);
    // The name is reusable, fresh.
    const b = await (await inst.db('billing')).collection('invoices');
    expect(await b.countDocuments({})).toBe(0);
    await inst.close();
  });

  it('the instance floor survives a dropped collection in one database', async () => {
    /*
     * The instance floor is a max over every database, and each
     * database's is a max over its catalog and its collections. The
     * catalog's term is the one a dropCollection cannot delete -- without
     * it, dropping the collection carrying the highest index makes the
     * INSTANCE floor regress to whatever some other database happens to
     * have recorded, and a floor below the log's base halts the apply
     * pump on every boot (Db.noteApplied).
     */
    const provider = new MemoryStorageProvider();
    let inst = await connectWalInstance(provider);
    await (await (await inst.db('analytics')).collection('users')).insertOne({ _id: oid(1) }); // 1
    const bills = await (await inst.db('billing')).collection('invoices');
    for (let i = 2; i <= 4; i++) await bills.insertOne({ _id: oid(i), amount: i });           // 2..4
    expect(await inst._appliedFloor()).toBe(4);

    // The drop is logged and applied through the envelope, like any write.
    expect(await (await inst.db('billing')).dropCollection('invoices')).toBe(true);           // 5
    expect(inst.log.lastIndex).toBe(5);
    // Not 1 -- analytics' -- which is what surviving collections alone say.
    expect(await inst._appliedFloor()).toBe(5);
    await inst.close();

    inst = await connectWalInstance(provider);
    expect(await inst._appliedFloor()).toBe(5);
    expect(await (await inst.db('billing')).listCollections()).toEqual([]);
    expect(await (await (await inst.db('analytics')).collection('users')).countDocuments({})).toBe(1);
    await inst.close();
  });

  it('refuses a root that is itself a database', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    await (await db.collection('users')).insertOne({ _id: oid(1) });
    await db.close();
    await expect(connectWalInstance(provider)).rejects.toThrow(/is a database, not an instance root/);
  });

  it('refuses a single-database log loudly, never misapplies it', async () => {
    const provider = new MemoryStorageProvider();
    // Forge an instance-shaped root whose log carries a BARE command --
    // what a connectWal host would have written.
    const log = new EntryLog(await provider.openFile(WAL_FILE, { create: true }));
    await log.open();
    log.setHardState(1);
    const { commands } = walPlan(null, 'users', WAL_REQ.INSERT_ONE, { _id: oid(1) });
    log.append(1, commands[0]);
    log.sync();
    await log.close();
    await expect(connectWalInstance(provider)).rejects.toThrow(/no instance envelope/);
  });
});

describe('WAL instance: snapshots span the root', () => {
  it('one generation, "db/file" live names, compacted log', async () => {
    const provider = new MemoryStorageProvider();
    const inst = await connectWalInstance(provider);
    const users = await (await inst.db('analytics')).collection('users');
    await users.createIndex({ team: 1 });
    await users.insertOne({ _id: oid(1), team: 'a' });
    await (await (await inst.db('billing')).collection('invoices')).insertOne({ _id: oid(2), amount: 7 });

    const snap = await inst.snapshot();
    expect(snap.lastIncludedIndex).toBe(3);   // createIndex + 2 inserts
    // catalog+primary+index for analytics, catalog+primary for billing
    expect(snap.config.live.length).toBe(5);
    for (const { name } of snap.config.live) expect(name).toMatch(/^(analytics|billing)\//);
    expect(snap.config.live.map((f) => f.name)).toContain('analytics/__catalog__.bj');
    expect(snap.config.live.map((f) => f.name)).toContain('billing/__catalog__.bj');

    expect(inst.log.baseIndex).toBe(3);
    const files = await provider.listFiles();
    expect(files).toContain('__snap__-log-1.bj');
    expect(files).not.toContain(WAL_FILE);
    await inst.close();
  });

  it('restore + log-suffix replay equals full history, across databases', async () => {
    const provider = new MemoryStorageProvider();
    let inst = await connectWalInstance(provider);
    const users = await (await inst.db('analytics')).collection('users');
    const bills = await (await inst.db('billing')).collection('invoices');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await bills.insertOne({ _id: oid(10), amount: 1 });
    await inst.snapshot();
    // History continues past the boundary in both databases.
    await users.updateOne({ _id: oid(2) }, { $set: { seen: true } });
    await bills.insertOne({ _id: oid(11), amount: 2 });
    const wantUsers = await stateOf(await inst.db('analytics'), 'users');
    const wantBills = await stateOf(await inst.db('billing'), 'invoices');
    await inst.close();

    await restoreLatestInstanceSnapshot(provider);
    inst = await connectWalInstance(provider);
    expect(await stateOf(await inst.db('analytics'), 'users')).toEqual(wantUsers);
    expect(await stateOf(await inst.db('billing'), 'invoices')).toEqual(wantBills);
    await inst.close();
  });

  it('deleting the log first gives a boundary-exact restore', async () => {
    const provider = new MemoryStorageProvider();
    let inst = await connectWalInstance(provider);
    const users = await (await inst.db('analytics')).collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await inst.snapshot();
    await users.insertOne({ _id: oid(4), i: 4 });   // beyond the boundary
    await inst.close();

    await restoreLatestInstanceSnapshot(provider);
    for (const f of await provider.listFiles()) {
      if (/^__snap__-log-\d+\.bj$/.test(f) || f === WAL_FILE) await provider.deleteFile(f);
    }
    inst = await connectWalInstance(provider);
    const a = await (await inst.db('analytics')).collection('users');
    expect(await a.countDocuments({})).toBe(3);
    expect(await a.findOne({ _id: oid(4) })).toBeNull();
    expect(inst.log.baseIndex).toBe(3);   // a fresh log, based at the boundary
    await inst.close();
  });
});

/* ---- the cross-host half: one artifact, two hosts ---------------------- */

const NATIVE = 'build/lib/nisaba-server';
const have = fs.existsSync(NATIVE);

let nextPort = 9310;
const takePort = () => nextPort++;

async function startNative(dir, extra = []) {
  const port = takePort();
  const proc = spawn(path.resolve(NATIVE), ['--port', String(port), '--raft', '1', ...extra],
    { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  proc.stderr.on('data', (c) => { stderr += c; });
  let client = null;
  for (let i = 0; i < 100 && !client; i++) {
    client = await connectServer(`127.0.0.1:${port}`).catch(() => null);
    if (!client) await sleep(100);
  }
  if (!client) {
    proc.kill();
    throw new Error(`nisaba-server never listened on ${port}: ${stderr}`);
  }
  return {
    proc, client, port,
    /* What the server SAID, which is the only place some decisions are
     * visible -- a generation restored, a sweep, a halt. */
    stderr: () => stderr,
    stop: async () => {
      await client.close().catch(() => {});
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
    }
  };
}

/** Retry until the single member has elected itself (reads and writes
 * refuse -63/-66 until then). */
async function eventually(fn, ms = 20000) {
  const until = Date.now() + ms;
  for (;;) {
    try { return await fn(); } catch (err) {
      if (Date.now() > until) throw err;
      await sleep(100);
    }
  }
}

describe.skipIf(!have)('WAL instance ↔ nisaba-server: one artifact', () => {
  it('a JS-written root serves from the C server, which replays the JS suffix', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-inst-jc-'));
    const provider = new NodeFSStorageProvider(dir);
    const inst = await connectWalInstance(provider);
    const users = await (await inst.db('appa')).collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await (await (await inst.db('appb')).collection('things')).insertOne({ _id: oid(10), n: 10 });
    await inst.snapshot();
    for (let i = 4; i <= 5; i++) await users.insertOne({ _id: oid(i), i });
    // The crashed-before-apply shape: durable in the log, not in the
    // files. If the C server sees this document, it parsed a JS-built
    // envelope and applied the db_wal.h command inside it.
    forgeUnapplied(inst, 'appa', 'users', { _id: oid(99), forged: true });
    await inst.close();
    await provider.close();   // locks off: the root changes owners now

    const server = await startNative(dir);
    try {
      const a = server.client.db('appa').collection('users');
      await eventually(async () => {
        expect(await a.countDocuments({})).toBe(6);   // 3 + 2 + the forged one
      });
      expect((await a.findOne({ _id: oid(99) })).forged).toBe(true);
      expect(await server.client.db('appb').collection('things').countDocuments({})).toBe(1);
      // And the server can carry on writing to it.
      await a.insertOne({ _id: oid(100), fromC: true });
      expect(await a.countDocuments({})).toBe(7);
    } finally {
      await server.stop();
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }, 60000);

  it('the applied index a drop leaves in the catalog crosses in both directions',
     async () => {
    /*
     * THE NEWEST THING ON DISK, READ BY THE OTHER SIDE. A dropCollection
     * takes the collection files that recorded what had been applied, so
     * the catalog records it too -- the one structure a drop keeps and
     * writes (Db.noteApplied, db_session.c's catalog_note_applied). Both
     * hosts write it now; neither had ever read the other's.
     *
     * It needed no format bump, and that is the claim under test rather
     * than an assumption: the field is per-tree metadata that
     * binjson-structures has always written for every tree, so no byte
     * changed shape -- a number that was always zero is sometimes not. The
     * break it could hide is not a parse error but a DISAGREEMENT: a floor
     * computed here and a floor computed there differing about which
     * entries are applied, which resumes replay in a place the files are
     * not.
     *
     * THE LOG IS COMPACTED PAST THE DROP ON PURPOSE, because otherwise
     * this test proves nothing: a host that cannot read the field would
     * replay the suffix and arrive at the same floor by accident. With the
     * entries gone, the catalog is the ONLY place that answer exists --
     * and a host that misses it computes a floor BELOW the log's base,
     * which is the unusable state that restores a generation (or halts).
     */
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-inst-floor-'));

    /* ---- JS writes it, C reads it -------------------------------------- */
    const provider = new NodeFSStorageProvider(dir);
    let inst = await connectWalInstance(provider);
    const keep = await (await inst.db('appa')).collection('keep');
    await keep.insertOne({ _id: oid(1), n: 1 });                                 // 1
    const doomed = await (await inst.db('appa')).collection('doomed');
    for (let i = 2; i <= 4; i++) await doomed.insertOne({ _id: oid(i), n: i });   // 2..4
    expect(await (await inst.db('appa')).dropCollection('doomed')).toBe(true);    // 5
    const jsFloor = await inst._appliedFloor();
    expect(jsFloor, 'the JS host did not record the drop on the catalog')
      .toBe(inst.log.lastIndex);
    expect(jsFloor, 'the survivor alone would report 1').toBeGreaterThan(1);
    /* Compact: the entries that made and dropped `doomed` leave the log. */
    const gen = await inst.snapshot();
    expect(gen.lastIncludedIndex).toBe(jsFloor);
    expect(inst.log.baseIndex).toBe(jsFloor);
    await inst.close();
    await provider.close();

    let cFloor = 0;
    const server = await startNative(dir);
    try {
      await eventually(async () => {
        const s = await server.client.ping();
        expect(s.pong).toBe(true);
      });
      const seen = await server.client.ping();
      expect(seen.base, 'the C server did not adopt the JS generation')
        .toBe(jsFloor);
      /* The floor is the JS-written one, out of the catalog: there are no
       * entries left to derive it from. Below the base it would be
       * unusable state, and the server would say so. */
      expect(seen.applied,
        'the C server did not see the applied index JS left in the catalog')
        .toBeGreaterThanOrEqual(jsFloor);
      expect(server.stderr(),
        'the C server treated a perfectly good root as unusable')
        .not.toMatch(/restoring snapshot|halted/);
      expect(await server.client.db('appa').listCollections()).toEqual(['keep']);

      /* ---- and now C writes one, for JS to read ----------------------- */
      const second = server.client.db('appa').collection('second');
      await eventually(async () => { await second.insertOne({ _id: oid(20), n: 20 }); });
      for (let i = 21; i <= 23; i++) await second.insertOne({ _id: oid(i), n: i });
      expect(await server.client.db('appa').dropCollection('second')).toBe(true);
      await server.client.snapshot();          /* the entries leave the log */
      const after = await server.client.ping();
      cFloor = after.applied;
      expect(after.base, 'the C log did not compact past its own drop').toBe(cFloor);
      expect(cFloor).toBeGreaterThan(jsFloor);
      await server.stop();
    } finally {
      server.proc.kill();
    }

    const back = new NodeFSStorageProvider(dir);
    inst = await connectWalInstance(back);
    try {
      /* Same question from the other side: with the log based at cFloor,
       * a JS host that could not read C's catalog term would compute the
       * survivor's index, decide the live files are unreachable, and
       * restore the generation to get there. */
      expect(await inst._appliedFloor(),
        'the JS host did not see the applied index C left in the catalog')
        .toBeGreaterThanOrEqual(cFloor);
      expect(inst.log.baseIndex).toBe(cFloor);
      expect((await (await inst.db('appa')).listCollections()).sort()).toEqual(['keep']);
      expect(await (await (await inst.db('appa')).collection('keep'))
        .countDocuments({})).toBe(1);
      /* And it is a working root, not merely a readable one. */
      await (await (await inst.db('appa')).collection('keep'))
        .insertOne({ _id: oid(30), n: 30 });
      expect(await (await (await inst.db('appa')).collection('keep'))
        .countDocuments({})).toBe(2);
    } finally {
      await inst.close();
      await back.close();
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }, 90000);

  it('a C-written root (log, generation and all) opens in the JS host, and back', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-inst-cj-'));
    fs.mkdirSync(dir, { recursive: true });
    const server = await startNative(dir, ['--snapshot-entries', '8']);
    const x = server.client.db('dbx').collection('users');
    await eventually(async () => { await x.insertOne({ _id: oid(1), i: 1 }); });
    for (let i = 2; i <= 12; i++) await x.insertOne({ _id: oid(i), i });
    await eventually(async () => {
      const s = await server.client.ping();
      expect(s.base).toBeGreaterThan(0);   // the C generation exists
    });
    await server.client.db('dby').collection('things').insertOne({ _id: oid(20), n: 20 });
    await server.stop();

    const provider = new NodeFSStorageProvider(dir);
    const inst = await connectWalInstance(provider);
    expect(await (await (await inst.db('dbx')).collection('users')).countDocuments({})).toBe(12);
    expect(await (await (await inst.db('dby')).collection('things')).countDocuments({})).toBe(1);
    // The store adopted the C server's generation as its own.
    expect(inst.snapshots.latest).not.toBeNull();
    expect(inst.snapshots.latest.lastIncludedIndex).toBeGreaterThan(0);
    // Every command entry the C log still holds parses as the envelope.
    for (const { env } of normalEntries(inst.log)) {
      expect(typeof env.d).toBe('string');
    }
    // Write from the JS side, then hand the root BACK to the C server.
    await (await (await inst.db('dbx')).collection('users')).insertOne({ _id: oid(13), i: 13 });
    await inst.close();
    await provider.close();

    const again = await startNative(dir);
    try {
      await eventually(async () => {
        expect(await again.client.db('dbx').collection('users').countDocuments({})).toBe(13);
      });
    } finally {
      await again.stop();
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }, 90000);
});
