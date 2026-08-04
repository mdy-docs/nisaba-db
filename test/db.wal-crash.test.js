/**
 * db.wal-crash.test.js — crash-point testing across the single-node WAL
 * write path (roadmap step 7; docs/steps brief "crash-point testing").
 *
 * The technique is the wrapping-handle one db.quota.test.js proved: a
 * provider whose mutating calls (write/truncate/flush) all run through
 * one counter, and once the counter reaches the armed crash point, every
 * subsequent mutation THROWS, forever. That is a crash-stop at syscall
 * granularity — the bytes that reached the files before the trip are the
 * durable state, nothing written after it can exist (the in-process
 * unwind code that runs after the throw is blocked from touching disk by
 * the same exhausted trip), and "reboot" is copying those bytes into a
 * fresh provider and opening it. Sweeping the crash point over every
 * mutation of a fixed workload kills the process at every boundary the
 * write path has — after append before sync, after sync before apply,
 * mid-apply between the appliedIndex stage and the mutation's commit,
 * mid-batch, before the commit marker's ride-along sync — without this
 * file having to know where those boundaries fall in the byte stream.
 *
 * The invariants are assertions, not prose:
 *   - recovery always opens (no crash point wedges the database);
 *   - nothing acked is ever lost (an awaited write that resolved before
 *     the crash is visible after it);
 *   - replay is exactly-once: the tally document's counter equals the
 *     number of $inc commands in the recovered log — an $inc applied
 *     twice or skipped moves the counter off that count (db.wal.test.js's
 *     forged windows prove single cases; the sweep proves every window);
 *   - a document and its index entries land together or not at all (the
 *     dc_wal_apply stage/commit pair): an indexed find agrees with a full
 *     scan at every crash point;
 *   - recovery converges: a second reopen replays nothing and changes
 *     nothing.
 *
 * Fault injection below the provider (torn sectors, lying fsync) is out
 * of scope here as everywhere in this repository: flush() is a real
 * fsync (db.durability.test.js proves who calls it) and flushed bytes
 * stay put.
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId, MemoryHandle, decode } from '../wasm/nisaba-wasm.js';
import { MemoryStorageProvider } from '../src/db.js';
import { connectWal } from '../src/db-wal.js';

await ready();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));
const TALLY = oid(1);

/** The crash itself, distinguishable from any database error. */
class CrashError extends Error {
  constructor(op, at) {
    super(`simulated crash at mutation #${at} (${op})`);
    this.name = 'CrashError';
  }
}

/**
 * MemoryStorageProvider that dies at mutation #`crashAt` and stays dead:
 * write/truncate/flush share one counter, the call that reaches the trip
 * throws before touching the file, and so does every mutation after it.
 * Reads keep working — a crashed process stops writing, and what this
 * models is that nothing mutates the durable bytes past the instant.
 */
class CrashPointProvider extends MemoryStorageProvider {
  constructor(crashAt = Infinity) {
    super();
    this.crashAt = crashAt;
    this.mutations = 0;
    this.dead = false;
  }

  _trip(op) {
    if (this.dead || ++this.mutations >= this.crashAt) {
      this.dead = true;
      throw new CrashError(op, this.mutations);
    }
  }

  async openFile(name, opts) {
    const handle = await super.openFile(name, opts);
    if (handle._crashWrapped) return handle;
    const provider = this;
    const inner = {
      write: handle.write.bind(handle),
      truncate: handle.truncate.bind(handle),
      flush: handle.flush ? handle.flush.bind(handle) : null
    };
    handle.write = (buf, o) => { provider._trip('write'); return inner.write(buf, o); };
    handle.truncate = (len) => { provider._trip('truncate'); return inner.truncate(len); };
    handle.flush = () => { provider._trip('flush'); if (inner.flush) inner.flush(); };
    handle._crashWrapped = true;
    return handle;
  }
}

/** Reboot: the durable bytes, and nothing else, in a fresh provider. */
function durableCopy(provider) {
  const copy = new MemoryStorageProvider();
  for (const [name, handle] of provider._files) {
    copy._files.set(name, new MemoryHandle(handle.toBytes()));
  }
  return copy;
}

/**
 * The fixed workload the sweep crashes at every point of. Sequential
 * awaits, and a fact is recorded only after its await RESOLVES — so when
 * the crash throws out of the middle, `facts` holds exactly what was
 * acknowledged, which is exactly what must survive.
 */
async function workload(db) {
  const facts = { incs: 0, docs: [], deletes: [], seen: false };
  const users = await db.collection('users');
  await users.createIndex({ team: 1 });
  await users.insertOne({ _id: TALLY, tally: 0, team: 'meta' });
  facts.docs.push(1);
  for (let i = 0; i < 5; i++) {
    await users.updateOne({ _id: TALLY }, { $inc: { tally: 1 } });
    facts.incs++;
    await users.insertOne({ _id: oid(10 + i), team: i % 2 ? 'blue' : 'red', i });
    facts.docs.push(10 + i);
  }
  await users.insertMany([30, 31, 32, 33, 34].map((n) => ({ _id: oid(n), team: 'red', n })));
  facts.docs.push(30, 31, 32, 33, 34);
  await users.updateMany({ team: 'red' }, { $set: { seen: true } });
  facts.seen = true;
  await users.deleteMany({ _id: { $in: [oid(30), oid(31)] } });
  facts.deletes.push(30, 31);
  for (let i = 0; i < 3; i++) {
    await users.updateOne({ _id: TALLY }, { $inc: { tally: 1 } });
    facts.incs++;
  }
  return facts;
}

/** Every invariant, checked against a rebooted database and the facts
 * acknowledged before the crash. */
async function checkInvariants(provider, facts, label) {
  const db = await connectWal(provider);
  const users = await db.collection('users');

  // The floor never claims more than the log holds. (It may sit below
  // the tip: DDL entries replay idempotently rather than advancing it.)
  expect(await users.appliedIndex(), label).toBeLessThanOrEqual(db.log.lastIndex);

  // Exactly-once: the counter IS the count of $inc commands in the log.
  const entries = db.log.getBatch(db.log.baseIndex + 1, 1 << 22);
  const incsLogged = entries.filter((e) => {
    const cmd = decode(e.payload);
    return cmd.op === 'u' && cmd.update?.$inc?.tally === 1;
  }).length;
  const tallyDoc = await users.findOne({ _id: TALLY });
  if (incsLogged > 0 || facts.docs.includes(1)) {
    expect(tallyDoc?.tally ?? 0, `${label}: tally != $inc count in log`).toBe(incsLogged);
  }
  // Nothing acked is lost: every resolved $inc is in that count.
  expect(incsLogged, `${label}: acked $inc lost`).toBeGreaterThanOrEqual(facts.incs);

  for (const n of facts.docs) {
    if (facts.deletes.includes(n)) continue;
    expect(await users.findOne({ _id: oid(n) }), `${label}: acked doc ${n} lost`).not.toBeNull();
  }
  for (const n of facts.deletes) {
    expect(await users.findOne({ _id: oid(n) }), `${label}: acked delete ${n} undone`).toBeNull();
  }
  if (facts.seen) {
    for (const n of [32, 33, 34]) {
      expect((await users.findOne({ _id: oid(n) }))?.seen, `${label}: acked updateMany lost on ${n}`).toBe(true);
    }
  }

  // The stage/commit pair: documents and their index entries agree at
  // every crash point — an indexed read returns exactly the docs a full
  // scan holds, never a phantom index entry or an unindexed document.
  const all = await users.find({}).toArray();
  for (const team of ['red', 'blue']) {
    const indexed = (await users.find({ team }).toArray()).map((d) => String(d._id)).sort();
    const scanned = all.filter((d) => d.team === team).map((d) => String(d._id)).sort();
    expect(indexed, `${label}: index disagrees with scan for team ${team}`).toEqual(scanned);
  }

  const state = { count: all.length, tally: tallyDoc?.tally ?? 0, applied: await users.appliedIndex() };
  await db.close();

  // Convergence: recovery is a fixpoint — a second reopen replays
  // nothing and moves nothing.
  const db2 = await connectWal(provider);
  const users2 = await db2.collection('users');
  expect(await users2.countDocuments({}), `${label}: second reopen changed the count`).toBe(state.count);
  expect((await users2.findOne({ _id: TALLY }))?.tally ?? 0, `${label}: second reopen moved the tally`).toBe(state.tally);
  expect(await users2.appliedIndex(), `${label}: second reopen replayed something`).toBe(state.applied);
  await db2.close();
}

/** How many mutations the workload alone performs (the sweep's domain;
 * the initial connect's own mutations are the recovery sweep's turf). */
async function measureWorkload() {
  const provider = new CrashPointProvider();
  const db = await connectWal(provider);
  const before = provider.mutations;
  await workload(db);
  const total = provider.mutations - before;
  await db.close();
  return total;
}

describe('WAL: crash-point sweep across the write path', () => {
  it('every crash point recovers: nothing acked lost, replay exactly-once, docs and indexes atomic', async () => {
    const total = await measureWorkload();
    expect(total).toBeGreaterThan(50); // the workload is not trivially small
    // Every mutation index is a distinct crash schedule. Sweep all of
    // them: the workload is in-memory and each recovery is milliseconds.
    for (let at = 1; at <= total; at++) {
      const provider = new CrashPointProvider();
      const db = await connectWal(provider);         // unarmed: the crash is the workload's
      provider.crashAt = provider.mutations + at;
      let facts = { incs: 0, docs: [], deletes: [], seen: false };
      let crashed = false;
      try {
        facts = await workload(db);
      } catch (err) {
        // The trip can surface re-labelled by intermediate layers
        // (bridgeHandle swallows the exception, C reports its own
        // failure) — once the provider is dead, any throw IS the crash.
        if (!provider.dead) throw err;
        crashed = true;
      }
      // Free the wasm contexts; the dead provider blocks every mutation,
      // so close() cannot launder buffered state into the durable bytes.
      try { await db.close(); } catch { /* it crashed; of course close fails */ }
      expect(crashed, `mutation budget ${at} of ${total} did not crash`).toBe(true);
      await checkInvariants(durableCopy(provider), facts, `crash at mutation ${at}`);
    }
  }, 120_000);

  it('a crash during recovery itself recovers: replay is restartable at every point', async () => {
    // First crash mid-workload, then crash again mid-REPLAY, then let the
    // third process finish the recovery. The replay path shares the apply
    // code but not the proposal path, so it gets its own sweep.
    const base = new CrashPointProvider(60);
    const db = await connectWal(base);
    let facts;
    try { facts = await workload(db); } catch { facts = null; }
    try { await db.close(); } catch { /* crashed */ }

    // Measure recovery's own mutation count, then sweep it.
    const probe = new CrashPointProvider();
    for (const [name, handle] of durableCopy(base)._files) probe._files.set(name, handle);
    const rdb = await connectWal(probe);
    await rdb.close();
    const recoveryTotal = probe.mutations;

    for (let at = 1; at <= recoveryTotal; at++) {
      const again = new CrashPointProvider(at);
      for (const [name, handle] of durableCopy(base)._files) again._files.set(name, handle);
      try {
        const mid = await connectWal(again);
        await mid.close(); // crash point fell past recovery: fine
      } catch (err) {
        if (!again.dead) throw err;
      }
      await checkInvariants(durableCopy(again), { incs: 0, docs: [], deletes: [], seen: false },
        `recovery crash at mutation ${at}`);
    }
    expect(facts).toBeNull(); // the first crash really was mid-workload
  }, 60_000);
});
