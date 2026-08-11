/**
 * Replication roadmap step 1 (docs/replicaton-roadmap.md): the collection-
 * level applied index -- dc_applied_index/dc_set_applied_index in
 * engine/src/db.c, surfaced as Collection.appliedIndex()/setAppliedIndex().
 *
 * The contract under test (db.h): setAppliedIndex stages the entry's index
 * onto the primary tree and every attached index structure; each file's
 * next commit persists it atomically with the mutation itself. The primary
 * tree is the authority for reads (every document mutation commits it);
 * index files a mutation didn't touch lag until their own next commit,
 * which is safe because the cross-file commit journal keeps the collection
 * at one consistent commit through crashes. Replay therefore resumes
 * exactly from appliedIndex() + 1.
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId, EntryLog, encode, decode, getFileHandle, deleteFile } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider, OPFSStorageProvider } from '../src/db.js';
import { bootstrapOPFS } from './binjson.suite.js';

await ready();
const { hasOPFS } = await bootstrapOPFS();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));

describe('collection applied index: basics', () => {
  it('reads 0 on a fresh collection and stays 0 without staging', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    expect(await users.appliedIndex()).toBe(0);
    await users.insertOne({ name: 'Ada' });
    expect(await users.appliedIndex()).toBe(0);
    await db1.close();

    // Not-log-driven collections stay that way across reopen.
    const db2 = await connect(provider);
    expect(await (await db2.collection('users')).appliedIndex()).toBe(0);
    await db2.close();
  });

  it('a staged index is persisted by the next mutation and survives reopen', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    await users.setAppliedIndex(7);
    expect(await users.appliedIndex()).toBe(7); // staged, visible immediately
    await users.insertOne({ _id: oid(1), name: 'Ada' });
    await db1.close();

    const db2 = await connect(provider);
    expect(await (await db2.collection('users')).appliedIndex()).toBe(7);
    await db2.close();
  });

  it('staging alone is NOT durable -- the mutation commit is the persistence point', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    await users.insertOne({ _id: oid(1), name: 'Ada' }); // some prior state
    await users.setAppliedIndex(9);                      // staged, never committed
    await db1.close();

    const db2 = await connect(provider);
    expect(await (await db2.collection('users')).appliedIndex()).toBe(0);
    await db2.close();
  });

  it('is sticky: later commits keep carrying the last staged value', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    await users.setAppliedIndex(3);
    await users.insertOne({ _id: oid(1), n: 1 });
    await users.insertOne({ _id: oid(2), n: 2 }); // no restage -- still carries 3
    await users.deleteOne({ _id: oid(1) });
    await db1.close();

    const db2 = await connect(provider);
    expect(await (await db2.collection('users')).appliedIndex()).toBe(3);
    await db2.close();
  });

  it('advances across a staged sequence and never decreases', async () => {
    const db = await connect(new MemoryStorageProvider());
    const users = await db.collection('users');
    for (let i = 1; i <= 5; i++) {
      await users.setAppliedIndex(i);
      await users.insertOne({ _id: oid(i), n: i });
    }
    expect(await users.appliedIndex()).toBe(5);

    await expect(users.setAppliedIndex(5)).resolves.toBeUndefined(); // equal restage is legal
    await expect(users.setAppliedIndex(4)).rejects.toMatchObject({ code: -2 }); // BJ_ERR_STATE
    expect(await users.appliedIndex()).toBe(5); // refusal left it unchanged

    // Still fully usable after a refused stage.
    await users.setAppliedIndex(6);
    await users.insertOne({ _id: oid(6), n: 6 });
    expect(await users.appliedIndex()).toBe(6);
    await db.close();
  });

  it('round-trips indexes far beyond 32 bits through the double bridge', async () => {
    const provider = new MemoryStorageProvider();
    const big = 2 ** 40 + 12345;
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    await users.setAppliedIndex(big);
    await users.insertOne({ _id: oid(1), n: 1 });
    await db1.close();

    const db2 = await connect(provider);
    expect(await (await db2.collection('users')).appliedIndex()).toBe(big);
    await db2.close();
  });

  it('staging works while a find() cursor is open (cursor snapshots are separate handles)', async () => {
    const db = await connect(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), n: 1 });
    const cursor = users.find({});
    await users.setAppliedIndex(1);
    await users.insertOne({ _id: oid(2), n: 2 });
    expect((await cursor.toArray()).length).toBeGreaterThanOrEqual(1);
    expect(await users.appliedIndex()).toBe(1);
    await db.close();
  });
});

describe('collection applied index: attached index structures', () => {
  it('stages onto equality, text, and geo index files, persisted with their commits', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const places = await db1.collection('places');
    await places.createIndex({ team: 1 });
    await places.createIndex({ body: 'text' });
    await places.createIndex({ location: '2dsphere' });

    await places.setAppliedIndex(4);
    await places.insertOne({
      _id: oid(1), team: 'red', body: 'the quick brown fox',
      location: { type: 'Point', coordinates: [151.2, -33.8] }
    });
    await db1.close();

    const db2 = await connect(provider);
    const p2 = await db2.collection('places');
    expect(await p2.appliedIndex()).toBe(4);
    // Every structure's own file carries the index (recovered on open).
    expect(p2._tree.appliedIndex()).toBe(4);
    expect(p2._indexes.get('team_1').tree.appliedIndex()).toBe(4);
    const tix = p2._indexes.get('body_text').trees;
    expect(tix.index.appliedIndex()).toBe(4);
    expect(tix.docTerms.appliedIndex()).toBe(4);
    expect(tix.docLengths.appliedIndex()).toBe(4);
    expect(p2._indexes.get('location_2dsphere').rt.appliedIndex()).toBe(4);
    // And the indexes still answer queries.
    expect((await p2.find({ team: 'red' }).toArray()).length).toBe(1);
    await db2.close();
  });

  it('an index file a mutation did not touch lags safely; the primary stays the authority', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    await users.createIndex({ age: 1 }, { sparse: true });

    // Entry 1's document has no `age`: the sparse index file is never
    // touched, so only the primary commits the staged value.
    await users.setAppliedIndex(1);
    await users.insertOne({ _id: oid(1), name: 'Ada' });
    await db1.close();

    const db2 = await connect(provider);
    const u2 = await db2.collection('users');
    expect(await u2.appliedIndex()).toBe(1);              // primary: authoritative
    expect(u2._indexes.get('age_1').tree.appliedIndex()).toBe(0); // lagging file

    // The sticky staged value catches the index file up on its next commit.
    await u2.setAppliedIndex(2);
    await u2.insertOne({ _id: oid(2), name: 'Bob', age: 41 });
    await db2.close();

    const db3 = await connect(provider);
    const u3 = await db3.collection('users');
    expect(await u3.appliedIndex()).toBe(2);
    expect(u3._indexes.get('age_1').tree.appliedIndex()).toBe(2);
    await db3.close();
  });

  it('compact() carries the applied index through every rewritten file', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users = await db1.collection('users');
    await users.createIndex({ team: 1 });
    await users.setAppliedIndex(11);
    for (let i = 1; i <= 20; i++) await users.insertOne({ _id: oid(i), team: i % 2 ? 'a' : 'b' });
    for (let i = 1; i <= 10; i++) await users.deleteOne({ _id: oid(i) });

    await users.compact();
    expect(await users.appliedIndex()).toBe(11);
    await db1.close();

    const db2 = await connect(provider);
    const u2 = await db2.collection('users');
    expect(await u2.appliedIndex()).toBe(11);
    expect(u2._indexes.get('team_1').tree.appliedIndex()).toBe(11);
    expect((await u2.find({ team: 'a' }).toArray()).length).toBe(5);
    await db2.close();
  });
});

/**
 * The pattern this feature exists for: an apply loop replaying a command
 * log guards each entry with `entry.index > appliedIndex()`, staging the
 * index immediately before applying. Interrupt it anywhere and replay from
 * the start -- already-applied entries are skipped, the rest apply, and the
 * result is identical to an uninterrupted run.
 */
describe('collection applied index: idempotent replay', () => {
  const COMMANDS = [
    { op: 'insertOne', doc: { _id: oid(1), team: 'red', score: 1 } },
    { op: 'insertOne', doc: { _id: oid(2), team: 'blue', score: 2 } },
    { op: 'updateOne', filter: { _id: oid(1) }, update: { $set: { score: 10 } } },
    { op: 'insertOne', doc: { _id: oid(3), team: 'red', score: 3 } },
    { op: 'deleteOne', filter: { _id: oid(2) } },
    { op: 'updateOne', filter: { _id: oid(3) }, update: { $inc: { score: 5 } } }
  ];

  async function applyEntry(col, index, cmd) {
    await col.setAppliedIndex(index);
    if (cmd.op === 'insertOne') await col.insertOne(cmd.doc);
    else if (cmd.op === 'updateOne') await col.updateOne(cmd.filter, cmd.update);
    else if (cmd.op === 'deleteOne') await col.deleteOne(cmd.filter);
  }

  async function replay(col, commands) {
    const applied = await col.appliedIndex();
    for (let i = 0; i < commands.length; i++) {
      const index = i + 1;
      if (index <= applied) continue; // already applied -- skip
      await applyEntry(col, index, commands[i]);
    }
  }

  async function stateOf(col) {
    const docs = await col.find({}, { sort: { _id: 1 } }).toArray();
    return docs.map((d) => ({ _id: d._id.toHexString(), team: d.team, score: d.score }));
  }

  it('an interrupted apply resumes exactly; final state matches an uninterrupted run', async () => {
    // Reference: uninterrupted.
    const refDb = await connect(new MemoryStorageProvider());
    const refCol = await refDb.collection('games');
    await refCol.createIndex({ team: 1 });
    await replay(refCol, COMMANDS);
    const want = await stateOf(refCol);
    expect(await refCol.appliedIndex()).toBe(COMMANDS.length);
    await refDb.close();

    // Crash after entry 3, then replay everything from entry 1.
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const col1 = await db1.collection('games');
    await col1.createIndex({ team: 1 });
    for (let i = 0; i < 3; i++) await applyEntry(col1, i + 1, COMMANDS[i]);
    await db1.close(); // "crash"

    const db2 = await connect(provider);
    const col2 = await db2.collection('games');
    expect(await col2.appliedIndex()).toBe(3);
    await replay(col2, COMMANDS); // full replay -- 1..3 skipped, 4..6 applied
    expect(await stateOf(col2)).toEqual(want);
    expect(await col2.appliedIndex()).toBe(COMMANDS.length);
    await db2.close();
  });

  it('double replay is a no-op (unique constraints prove nothing re-applied)', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const col = await db1.collection('games');
    await col.createIndex({ team: 1 }, { unique: false });
    await replay(col, COMMANDS);
    const before = await stateOf(col);

    await replay(col, COMMANDS); // everything already applied
    expect(await stateOf(col)).toEqual(before);
    expect(await col.countDocuments({})).toBe(2);
    await db1.close();
  });
});

/**
 * Step-2 preview: EntryLog as the WAL in front of the collection. Commands
 * are proposed (append + sync) before they are applied; recovery replays
 * the committed log suffix the collection hasn't seen. This is the exact
 * loop the replication apply path will run.
 */
describe.skipIf(!hasOPFS)('collection applied index: EntryLog write-ahead integration', () => {
  it('recovers by replaying the log from appliedIndex + 1', async () => {
    const root = await navigator.storage.getDirectory();
    const logName = 'test-applied-wal.bj';

    const openLog = async () => {
      const fh = await getFileHandle(root, logName, { create: true });
      const log = new EntryLog(await fh.createSyncAccessHandle());
      await log.open();
      return log;
    };

    const apply = async (col, entry) => {
      const cmd = decode(entry.payload);
      await col.setAppliedIndex(entry.index);
      if (cmd.op === 'insertOne') await col.insertOne(cmd.doc);
      else if (cmd.op === 'deleteOne') await col.deleteOne(cmd.filter);
    };

    const provider = new MemoryStorageProvider();
    try {
      // Session 1: propose 4 commands to the log (all durable), but
      // "crash" after applying only the first two to the collection.
      let log = await openLog();
      log.setHardState(1);
      const cmds = [
        { op: 'insertOne', doc: { _id: oid(1), v: 'a' } },
        { op: 'insertOne', doc: { _id: oid(2), v: 'b' } },
        { op: 'deleteOne', filter: { _id: oid(1) } },
        { op: 'insertOne', doc: { _id: oid(3), v: 'c' } }
      ];
      for (const c of cmds) log.append(1, encode(c));
      log.sync();

      let db = await connect(provider);
      let col = await db.collection('kv');
      for (const entry of log.getBatch(1).slice(0, 2)) await apply(col, entry);
      expect(await col.appliedIndex()).toBe(2);
      await db.close(); // "crash": log is ahead of the state machine
      await log.close();

      // Session 2 (recovery): replay the committed suffix.
      log = await openLog();
      expect(log.lastIndex).toBe(4);
      db = await connect(provider);
      col = await db.collection('kv');
      const from = (await col.appliedIndex()) + 1;
      expect(from).toBe(3);
      for (const entry of log.getBatch(from)) await apply(col, entry);

      expect(await col.appliedIndex()).toBe(4);
      expect(await col.countDocuments({})).toBe(2); // {2:'b'}, {3:'c'}
      expect(await col.findOne({ _id: oid(1) })).toBeNull();
      expect((await col.findOne({ _id: oid(3) })).v).toBe('c');
      await db.close();
      await log.close();
    } finally {
      await deleteFile(root, logName);
    }
  });
});

/*
 * ---- the database floor, and the term a drop cannot delete ---------------
 *
 * Everything above is per-collection: the record of what has been applied
 * lives in the collection's own files, staged so the mutation's commit
 * persists both atomically. That is exactly right until the mutation IS
 * the deletion of the structure holding the record -- dropping the
 * collection carrying the highest index made the floor go BACKWARDS, by
 * however far that collection was ahead.
 *
 * So the catalog carries one too (Db.noteApplied, the twin of
 * db_session.c's catalog_note_applied): it is the one structure a drop
 * both keeps and writes. db.replicated.test.js has what a regressed floor
 * costs -- a node that halts on its own files, on every boot.
 */
describe('database applied floor', () => {
  it('is the max over the catalog and every collection', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    expect(await db.appliedFloor()).toBe(0);

    const keep = await db.collection('keep');
    await keep.setAppliedIndex(10);
    await keep.insertOne({ _id: oid(1), n: 1 });
    expect(await db.appliedFloor()).toBe(10);

    // The catalog's own term, staged and then committed by a DDL act --
    // here createIndex, which writes the catalog last, having built and
    // attached.
    await db.noteApplied(11);
    await keep.createIndex({ n: 1 });
    expect(await db.appliedFloor()).toBe(11);
    await db.close();

    // Durable, not merely staged: the DDL's catalog commit carried it.
    const again = await connect(provider);
    expect(await again.appliedFloor()).toBe(11);
    await again.close();
  });

  it('does not go backwards when the collection holding the max is dropped', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const keep = await db.collection('keep');
    await keep.setAppliedIndex(3);
    await keep.insertOne({ _id: oid(1), n: 1 });

    const doomed = await db.collection('doomed');
    await doomed.setAppliedIndex(40);
    await doomed.insertOne({ _id: oid(2), n: 2 });
    expect(await db.appliedFloor()).toBe(40);

    // The drop, applied as a logged entry would be: note first, then
    // mutate -- the catalog's commit is the drop's decisive durable act.
    await db.noteApplied(41);
    expect(await db.dropCollection('doomed')).toBe(true);
    // Without the catalog's term this is 3, the survivor's, and the 38
    // entries above it look unapplied.
    expect(await db.appliedFloor()).toBe(41);
    await db.close();

    const again = await connect(provider);
    expect(await again.appliedFloor()).toBe(41);
    await again.close();
  });

  it('is sticky and never decreases: a replay re-offering an index is no error', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const col = await db.collection('c');
    await db.noteApplied(9);
    await col.createIndex({ n: 1 });
    expect(await db.appliedFloor()).toBe(9);

    // Replay offers 9 again, and 4 from an earlier entry. The setter
    // refuses a decrease; noteApplied answers that with a skip rather
    // than an error, because a re-offer is the guard working.
    await db.noteApplied(9);
    await db.noteApplied(4);
    expect(await db.appliedFloor()).toBe(9);
    await db.close();
  });

  it('an implicit collection creation does not note, though it writes the catalog', async () => {
    // ONLY the DDL three note (WalDb._applyCommand). A first insert
    // creates the collection -- a catalog write -- but its index belongs
    // to the collection's own commit; recording it here would let the
    // floor claim an entry whose mutation the catalog's next commit does
    // not carry, which is a lost write rather than a halt.
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const col = await db.collection('made-by-insert');
    await col.setAppliedIndex(6);
    await col.insertOne({ _id: oid(1), n: 1 });
    await db.close();

    const again = await connect(provider);
    expect(await (await again.collection('made-by-insert')).appliedIndex()).toBe(6);
    // The floor is the collection's 6; the catalog recorded nothing.
    expect(await again.appliedFloor()).toBe(6);
    // And this is the regression itself, in the only way left to reach it:
    // a drop that noted nothing takes the record with it. The logged path
    // cannot get here -- WalDb._applyCommand notes every drop -- which is
    // exactly why the note is where it is.
    expect(await again.dropCollection('made-by-insert')).toBe(true);
    expect(await again.appliedFloor()).toBe(0);
    await again.close();
  });
});
