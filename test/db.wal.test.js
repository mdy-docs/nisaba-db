/**
 * Replication roadmap step 2 (docs/replicaton-roadmap.md): the single-node
 * write-ahead log -- src/db-wal.js. Every write is a command logged and
 * durable (EntryLog append + sync) BEFORE it applies to the collections;
 * recovery replays the committed suffix each collection hasn't applied
 * (its appliedIndex(), step 1). Covers: driver-shape equivalence with the
 * plain Db, proposal-time determinism (_ids, upsert ids, $currentDate,
 * TTL cutoffs), per-document decomposition of multi-doc writes, failed-
 * command retraction, crash recovery, and replay idempotence.
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId, EntryLog, encode, decode } from '../src/nisaba-wasm.js';
import { MemoryStorageProvider } from '../src/db.js';
import { connectWal, WAL_FILE } from '../src/db-wal.js';

await ready();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));

/** Open a raw EntryLog on the provider's WAL file -- how the tests forge
 * "proposed but never applied" entries (the crash window between sync()
 * and apply) and inspect what the WAL layer logged. */
async function rawLog(provider) {
  const log = new EntryLog(await provider.openFile(WAL_FILE, { create: false }));
  await log.open();
  return log;
}

describe('WAL: driver-shaped behavior', () => {
  it('insertOne / findOne round-trips, assigning an _id when none is given', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    const { acknowledged, insertedId } = await users.insertOne({ name: 'Ada' });
    expect(acknowledged).toBe(true);
    expect(insertedId).toBeInstanceOf(ObjectId);
    expect((await users.findOne({ _id: insertedId })).name).toBe('Ada');
    expect(await users.countDocuments({})).toBe(1);
    await db.close();
  });

  it('updateOne: matched, no-match, and upsert (with returned upsertedId)', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), n: 1 });

    expect(await users.updateOne({ _id: oid(1) }, { $set: { n: 2 } }))
      .toEqual({ acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedId: null });
    expect((await users.findOne({ _id: oid(1) })).n).toBe(2);

    expect(await users.updateOne({ _id: oid(9) }, { $set: { n: 9 } }))
      .toEqual({ acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null });

    const r = await users.updateOne({ tag: 'new' }, { $set: { n: 3 } }, { upsert: true });
    expect(r.upsertedId).toBeInstanceOf(ObjectId);
    const upserted = await users.findOne({ _id: r.upsertedId });
    expect(upserted.tag).toBe('new');
    expect(upserted.n).toBe(3);
    await db.close();
  });

  it('replaceOne, deleteOne, findOneAnd{Update,Replace,Delete} keep their contracts', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), n: 1, keep: true });

    expect(await users.replaceOne({ _id: oid(1) }, { n: 10 }))
      .toEqual({ acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedId: null });
    expect((await users.findOne({ _id: oid(1) })).keep).toBeUndefined();

    const before = await users.findOneAndUpdate({ _id: oid(1) }, { $inc: { n: 5 } });
    expect(before.n).toBe(10);
    const after = await users.findOneAndUpdate({ _id: oid(1) }, { $inc: { n: 5 } }, { returnDocument: 'after' });
    expect(after.n).toBe(20);
    expect(await users.findOneAndUpdate({ _id: oid(9) }, { $set: { n: 0 } })).toBeNull();

    const replaced = await users.findOneAndReplace({ _id: oid(1) }, { n: 1 }, { returnDocument: 'after' });
    expect(replaced.n).toBe(1);

    const deleted = await users.findOneAndDelete({ _id: oid(1) });
    expect(deleted.n).toBe(1);
    expect(await users.findOneAndDelete({ _id: oid(1) })).toBeNull();
    expect(await users.deleteOne({ _id: oid(1) })).toEqual({ acknowledged: true, deletedCount: 0 });
    await db.close();
  });

  it('insertMany / updateMany / deleteMany / bulkWrite aggregate like the plain driver', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');

    const { insertedCount, insertedIds } = await users.insertMany([
      { _id: oid(1), team: 'a' }, { _id: oid(2), team: 'a' }, { _id: oid(3), team: 'b' }
    ]);
    expect(insertedCount).toBe(3);
    expect(insertedIds[2].equals(oid(3))).toBe(true);

    expect(await users.updateMany({ team: 'a' }, { $set: { seen: true } }))
      .toEqual({ acknowledged: true, matchedCount: 2, modifiedCount: 2, upsertedId: null });
    expect(await users.countDocuments({ seen: true })).toBe(2);

    const um = await users.updateMany({ team: 'z' }, { $set: { z: 1 } }, { upsert: true });
    expect(um.upsertedId).toBeInstanceOf(ObjectId);

    const bw = await users.bulkWrite([
      { insertOne: { document: { _id: oid(4), team: 'c' } } },
      { updateOne: { filter: { _id: oid(1) }, update: { $set: { bulk: true } } } },
      { deleteOne: { filter: { _id: oid(2) } } }
    ]);
    expect(bw).toMatchObject({ insertedCount: 1, matchedCount: 1, modifiedCount: 1, deletedCount: 1 });

    expect(await users.deleteMany({ team: 'a' })).toEqual({ acknowledged: true, deletedCount: 1 });
    await db.close();
  });

  it('reads, indexes, change streams, and compact work through the WAL', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });

    const stream = users.watch();
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'red' });
    const event = (await stream.next()).value;
    expect(event.operationType).toBe('insert');
    expect(event.documentKey._id.equals(insertedId)).toBe(true);
    await stream.close();

    expect((await users.find({ team: 'red' }).toArray()).length).toBe(1);
    expect((await users.explain({ team: 'red' })).source).toBeDefined();
    await users.compact();
    expect((await users.find({ team: 'red' }).toArray()).length).toBe(1);
    await db.close();
  });

  it('DDL is logged: createIndex/dropIndex/dropCollection replay like any write', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), team: 'red' });
    const before = db.log.lastIndex;
    await users.createIndex({ team: 1 });
    expect(db.log.lastIndex).toBe(before + 1); // DDL is an entry
    expect((await users.find({ team: 'red' }).toArray()).length).toBe(1);
    await users.dropIndex('team_1');
    expect(db.log.lastIndex).toBe(before + 2);
    expect(await db.dropCollection('users')).toBe(true);
    expect(db.log.lastIndex).toBe(before + 3);
    await db.close();

    // Replay reconstructs the same end state: recreated then re-dropped.
    const db2 = await connectWal(provider);
    expect(await db2.listCollections()).toEqual([]);
    await db2.close();
  });

  it('snapshot() is refused without listFiles(), but dropCollection works (it is just a logged command)', async () => {
    const provider = new MemoryStorageProvider();
    const bare = { openFile: provider.openFile.bind(provider), deleteFile: provider.deleteFile.bind(provider) };
    const db = await connectWal(bare);
    await (await db.collection('users')).insertOne({ n: 1 });
    await expect(db.snapshot()).rejects.toThrow(/listFiles/);
    expect(await db.dropCollection('users')).toBe(true);
    expect(await db.listCollections()).toEqual([]);
    await db.close();
  });
});

describe('WAL: what gets logged', () => {
  it('one entry per document; multi-doc writes decompose; no-match writes log nothing', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');

    await users.insertOne({ _id: oid(1), n: 1 });          // 1 entry
    expect(db.log.lastIndex).toBe(1);
    await users.insertMany([{ _id: oid(2) }, { _id: oid(3) }, { _id: oid(4) }]); // 3 entries
    expect(db.log.lastIndex).toBe(4);
    await users.updateMany({}, { $set: { seen: true } });  // 4 entries (one per doc)
    expect(db.log.lastIndex).toBe(8);
    await users.deleteMany({ _id: { $in: [oid(3), oid(4)] } }); // 2 entries
    expect(db.log.lastIndex).toBe(10);

    // No-match, non-upsert writes never reach the log.
    await users.updateOne({ _id: oid(99) }, { $set: { n: 0 } });
    await users.deleteOne({ _id: oid(99) });
    await users.deleteMany({ nope: true });
    expect(db.log.lastIndex).toBe(10);

    // Matched updates are logged resolved to their target _id.
    const entries = db.log.getBatch(5);
    const cmd = decode(entries[0].payload);
    expect(cmd.op).toBe('u');
    expect(cmd.id).toBeInstanceOf(ObjectId);
    await db.close();
  });

  it('no logged command carries a filter: every one names a single _id', async () => {
    // The invariant the C grammar exists to hold (engine/include/db_wal.h).
    // A command carrying a filter would have apply re-run a query against
    // the very state replay is in the middle of reproducing -- which is
    // how the retired `uu`/`ru` opcodes worked, and why they are gone.
    // Checked over every write shape at once, because the failure mode is
    // one method quietly reintroducing it.
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), team: 'core' });
    await users.insertMany([{ _id: oid(2), team: 'core' }, { _id: oid(3), team: 'research' }]);
    await users.updateOne({ team: 'core' }, { $set: { a: 1 } });
    await users.updateMany({ team: 'core' }, { $set: { b: 1 } });
    await users.updateOne({ team: 'ghosts' }, { $set: { c: 1 } }, { upsert: true });
    await users.updateMany({ team: 'spectres' }, { $set: { d: 1 } }, { upsert: true });
    await users.replaceOne({ team: 'research' }, { team: 'research', replaced: true });
    await users.replaceOne({ team: 'nobody' }, { team: 'nobody' }, { upsert: true });
    await users.findOneAndUpdate({ team: 'core' }, { $set: { e: 1 } });
    await users.findOneAndReplace({ team: 'ghosts' }, { team: 'ghosts' });
    await users.findOneAndDelete({ team: 'spectres' });
    await users.deleteOne({ team: 'nobody' });
    await users.deleteMany({ team: 'core' });
    await users.createIndex({ team: 1 });
    await users.dropIndex('team_1');

    const entries = db.log.getBatch(1, 1 << 20);
    expect(entries.length).toBeGreaterThan(14);   // every write above logged
    const documentOps = new Set(['i', 'u', 'r', 'd']);
    for (const entry of entries) {
      const cmd = decode(entry.payload);
      expect(cmd.filter).toBeUndefined();
      if (!documentOps.has(cmd.op)) continue;
      // An insert names its _id inside the document; the rest name an id.
      const id = cmd.op === 'i' ? cmd.doc._id : cmd.id;
      expect(id).toBeInstanceOf(ObjectId);
    }
    await db.close();
  });

  it('$currentDate is resolved to a concrete Date before logging', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1) });
    await users.updateOne({ _id: oid(1) }, { $currentDate: { ts: true }, $set: { n: 1 } });

    const [entry] = db.log.getBatch(db.log.lastIndex);
    const cmd = decode(entry.payload);
    expect(cmd.update.$currentDate).toBeUndefined();
    expect(cmd.update.$set.ts).toBeInstanceOf(Date);
    // The applied document carries the exact logged timestamp.
    const doc = await users.findOne({ _id: oid(1) });
    expect(doc.ts.getTime()).toBe(cmd.update.$set.ts.getTime());
    await db.close();
  });

  it('a command whose apply fails is retracted from the log', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), n: 1 });
    expect(db.log.lastIndex).toBe(1);

    await expect(users.insertOne({ _id: oid(1), n: 2 })).rejects.toMatchObject({ name: 'DuplicateKeyError' });
    expect(db.log.lastIndex).toBe(1); // the failed command left no trace
    expect((await users.findOne({ _id: oid(1) })).n).toBe(1);
    await db.close();
  });

  it('ordered insertMany stops at the first failure and retracts the never-applied suffix', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(2), n: 'existing' });

    let err = null;
    try {
      await users.insertMany([{ _id: oid(1) }, { _id: oid(2) }, { _id: oid(3) }]);
    } catch (e) { err = e; }
    expect(err).not.toBeNull();
    expect(err.result).toMatchObject({ insertedCount: 1 });

    // doc 1 applied; the dup and its suffix retracted.
    expect(db.log.lastIndex).toBe(2); // entry 1 = existing insert, entry 2 = doc 1
    expect(await users.countDocuments({})).toBe(2);
    expect(await users.findOne({ _id: oid(3) })).toBeNull();
    await db.close();
  });

  it('unordered insertMany attempts every document; failed residue is replay-safe', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(2), n: 'existing' });

    await expect(users.insertMany(
      [{ _id: oid(1) }, { _id: oid(2) }, { _id: oid(3) }], { ordered: false }
    )).rejects.toMatchObject({ result: expect.anything() });
    expect(await users.countDocuments({})).toBe(3); // 1 and 3 landed
    await db.close();

    // The dup command is still in the log; recovery re-runs it into the
    // same deterministic error and the state is unchanged.
    const db2 = await connectWal(provider);
    const u2 = await db2.collection('users');
    expect(await u2.countDocuments({})).toBe(3);
    expect((await u2.findOne({ _id: oid(2) })).n).toBe('existing');
    await db2.close();
  });
});

describe('WAL: crash recovery', () => {
  it('replays commands that were durable in the log but never applied', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    await (await db1.collection('users')).insertOne({ _id: oid(1), n: 1 });
    await db1.close();

    // Forge the crash window: commands synced to the log, apply never ran.
    const log = await rawLog(provider);
    log.append(log.currentTerm, encode({ c: 'users', op: 'i', doc: { _id: oid(2), n: 2 } }));
    log.append(log.currentTerm, encode({ c: 'users', op: 'u', id: oid(1), update: { $set: { n: 10 } } }));
    log.append(log.currentTerm, encode({ c: 'users', op: 'd', id: oid(2) }));
    log.append(log.currentTerm, encode({ c: 'other', op: 'i', doc: { _id: oid(3), from: 'log' } }));
    log.sync();
    await log.close();

    const db2 = await connectWal(provider); // recovery runs here
    const users = await db2.collection('users');
    expect((await users.findOne({ _id: oid(1) })).n).toBe(10);
    expect(await users.findOne({ _id: oid(2) })).toBeNull(); // inserted then deleted, in order
    expect(await users.appliedIndex()).toBe(4);
    expect((await (await db2.collection('other')).findOne({ _id: oid(3) })).from).toBe('log');
    await db2.close();
  });

  it('an upsert is logged as a plain insert, and replays to the identical _id', async () => {
    // The upsert opcodes that carried a filter into the log are gone: the
    // planner resolves an upsert the whole way and logs the document it
    // would have inserted (engine/include/db_wal.h). So what survives a
    // crash between sync and apply is not "re-run this filter and see" --
    // it is one document with one _id, which is why replay is exact.
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    const users1 = await db1.collection('users');
    await users1.insertOne({ _id: oid(1) });

    const { upsertedId } = await users1.updateOne({ tag: 'x' }, { $set: { n: 7 } }, { upsert: true });
    const cmd = decode(db1.log.getBatch(db1.log.lastIndex)[0].payload);
    expect(cmd.op).toBe('i');                       // not an upsert command
    expect(cmd.filter).toBeUndefined();             // and it carries no filter
    expect(cmd.doc._id.equals(upsertedId)).toBe(true);
    expect(cmd.doc).toEqual({ _id: upsertedId, tag: 'x', n: 7 });
    await db1.close();

    // Replay that exact entry into a database that has never seen it.
    const fresh = new MemoryStorageProvider();
    const seed = await connectWal(fresh);
    await (await seed.collection('users')).insertOne({ _id: oid(1) });
    await seed.close();
    const log = await rawLog(fresh);
    log.append(log.currentTerm, encode(cmd));
    log.sync();
    await log.close();

    const db2 = await connectWal(fresh);
    const doc = await (await db2.collection('users')).findOne({ tag: 'x' });
    expect(doc._id.equals(upsertedId)).toBe(true);
    expect(doc.n).toBe(7);
    await db2.close();
  });

  it('an upsert pinning its _id logs that _id, on the WAL path too', async () => {
    // The planner builds the upserted document through the same
    // dc_upsert_document the direct path uses, so a filter-pinned _id
    // reaches the log rather than being replaced by a generated one --
    // and a replica replaying the entry stores it under the id the
    // caller named. Were the two to diverge, the WAL and non-WAL
    // databases would disagree about where a document lives.
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    const pinned = oid(0xfeed);

    const { upsertedId } = await users.updateOne(
      { _id: pinned, team: 'core' }, { $set: { seen: true } }, { upsert: true }
    );
    expect(upsertedId.equals(pinned)).toBe(true);

    const cmd = decode(db.log.getBatch(db.log.lastIndex)[0].payload);
    expect(cmd.op).toBe('i');
    expect(cmd.doc._id.equals(pinned)).toBe(true);
    await db.close();

    const db2 = await connectWal(provider);
    expect((await (await db2.collection('users')).findOne({ _id: pinned })).seen).toBe(true);
    await db2.close();
  });

  it('an entry naming an op this build cannot execute is refused, not skipped', async () => {
    // A follower that silently ignores what it does not understand has
    // diverged from one that does. The grammar (db_wal.h) rejects it, and
    // recovery's swallow-deterministic-errors arm leaves the state
    // untouched rather than half-applying a log it cannot read.
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    await (await db1.collection('users')).insertOne({ _id: oid(1), n: 1 });
    await db1.close();

    const log = await rawLog(provider);
    log.append(log.currentTerm, encode({ c: 'users', op: 'teleport', doc: { _id: oid(2) } }));
    // The retired upsert opcode is now exactly such an entry.
    log.append(log.currentTerm, encode({ c: 'users', op: 'uu', filter: { tag: 'x' }, update: {}, did: oid(3) }));
    log.sync();
    await log.close();

    const db2 = await connectWal(provider);
    const users = await db2.collection('users');
    expect(await users.countDocuments({})).toBe(1);   // neither was applied
    expect((await users.findOne({ _id: oid(1) })).n).toBe(1);
    await db2.close();
  });

  it('a crash mid-batch resumes exactly: applied prefix skipped, suffix applied', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    const users1 = await db1.collection('users');
    await users1.insertMany([{ _id: oid(1) }, { _id: oid(2) }, { _id: oid(3) }]);
    await db1.close();

    // Forge: 3 more inserts durable in the log, none applied ("crash"
    // right after the batch's sync).
    const log = await rawLog(provider);
    for (const n of [4, 5, 6]) {
      log.append(log.currentTerm, encode({ c: 'users', op: 'i', doc: { _id: oid(n), n } }));
    }
    log.sync();
    await log.close();

    const db2 = await connectWal(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(6);
    expect(await users2.appliedIndex()).toBe(6);

    // A second reopen replays nothing (idempotence): same state, same index.
    await db2.close();
    const db3 = await connectWal(provider);
    const users3 = await db3.collection('users');
    expect(await users3.countDocuments({})).toBe(6);
    expect(await users3.appliedIndex()).toBe(6);
    await db3.close();
  });

  it('clean shutdown and reopen preserves state with no replay effects', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    const users1 = await db1.collection('users');
    await users1.createIndex({ team: 1 });
    await users1.insertMany([{ _id: oid(1), team: 'a' }, { _id: oid(2), team: 'b' }]);
    await users1.updateOne({ _id: oid(1) }, { $set: { seen: true } });
    await db1.close();

    const db2 = await connectWal(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(2);
    expect((await users2.findOne({ _id: oid(1) })).seen).toBe(true);
    expect((await users2.find({ team: 'a' }).toArray()).length).toBe(1);
    expect(await users2.appliedIndex()).toBe(db2.log.lastIndex);
    await db2.close();
  });
});

describe('WAL: semantics under load', () => {
  it('concurrent writes serialize: log order is apply order, nothing lost', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    await Promise.all(Array.from({ length: 20 }, (_, i) => users.insertOne({ _id: oid(i + 1), i })));
    expect(db.log.lastIndex).toBe(20);
    expect(await users.countDocuments({})).toBe(20);
    expect(await users.appliedIndex()).toBe(20);
    await db.close();
  });

  it('read-your-writes holds for every awaited write', async () => {
    const db = await connectWal(new MemoryStorageProvider());
    const users = await db.collection('users');
    for (let i = 1; i <= 5; i++) {
      await users.insertOne({ _id: oid(i), i });
      expect((await users.findOne({ _id: oid(i) })).i).toBe(i);
    }
    await db.close();
  });

  it('pruneExpired logs concrete-cutoff deletes and survives replay', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const sessions = await db.collection('sessions');
    await sessions.createIndex({ lastSeen: 1 }, { expireAfterSeconds: 10 });
    await sessions.insertOne({ _id: oid(1), lastSeen: new Date(Date.now() - 60_000) }); // expired
    await sessions.insertOne({ _id: oid(2), lastSeen: new Date() });                    // fresh

    expect(await sessions.pruneExpired()).toBe(1);
    expect(await sessions.countDocuments({})).toBe(1);
    const [entry] = db.log.getBatch(db.log.lastIndex);
    expect(decode(entry.payload).op).toBe('d'); // logged as a plain delete
    await db.close();

    const db2 = await connectWal(provider);
    expect(await (await db2.collection('sessions')).countDocuments({})).toBe(1);
    await db2.close();
  });
});

describe('WAL: log lost, applied state survives (the backup-restore shape)', () => {
  it('re-bases the log at the applied floor and keeps serving writes', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    const users = await db1.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await db1.close();

    // Applied files survive, the log does not — a restore of applied
    // state only, or a hand-deleted WAL. Without reconciliation the
    // reopened log restarts at index 1 and the first write's
    // setAppliedIndex(1 < 3) is refused, failing every write forever.
    await provider.deleteFile(WAL_FILE);

    const db2 = await connectWal(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(3);
    expect(db2.log.baseIndex).toBe(3); // re-based at the applied floor
    expect(db2.log.lastIndex).toBe(3);
    await users2.insertOne({ _id: oid(4), i: 4 }); // appends resume at floor + 1
    expect(db2.log.lastIndex).toBe(4);
    expect(await users2.countDocuments({})).toBe(4);
    await db2.close();

    // The re-based log is an ordinary log thereafter: reopen replays
    // nothing (everything applied) and serves normally.
    const db3 = await connectWal(provider);
    expect(await (await db3.collection('users')).countDocuments({})).toBe(4);
    expect(db3.log.baseIndex).toBe(3);
    await db3.close();
  });

  it('leaves a consistent log alone', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connectWal(provider);
    await (await db1.collection('users')).insertOne({ _id: oid(1) });
    await db1.close();

    const db2 = await connectWal(provider); // log intact: no re-base
    expect(db2.log.baseIndex).toBe(0);
    expect(db2.log.lastIndex).toBe(1);
    await db2.close();
  });
});
