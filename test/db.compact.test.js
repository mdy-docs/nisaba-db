/**
 * Collection/Db compaction (docs/compaction.md): rewriting a collection's
 * whole file set (primary tree + every index + a fresh journal) into a new
 * generation of files, atomically adopted via one catalog commit, with the
 * old generation deleted afterward and any crash leftovers swept by
 * Db.open().
 *
 * Crash windows are simulated the same way test/db.atomic-wasm.test.js
 * simulates them -- by snapshotting the backing files' bytes around the
 * operation and reopening synthesized combinations -- but in memory:
 * MemoryHandle.toBytes() replaces reading OPFS files back, so these run in
 * plain Node.
 */
import { describe, it, expect } from 'vitest';
import { ready, MemoryHandle } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider, ObjectId } from '../src/db.js';

await ready();

/** Byte-level snapshot of every file currently in a MemoryStorageProvider. */
function snapshotFiles(provider) {
  const out = new Map();
  for (const [name, handle] of provider._files) out.set(name, handle.toBytes());
  return out;
}

/** A fresh provider seeded with `maps` merged left to right (later wins) --
 * how the crash tests compose "which writes survived". */
function providerWith(...maps) {
  const p = new MemoryStorageProvider();
  for (const m of maps) {
    for (const [name, bytes] of m) p._files.set(name, new MemoryHandle(bytes));
  }
  return p;
}

function pick(map, predicate) {
  const out = new Map();
  for (const [k, v] of map) if (predicate(k)) out.set(k, v);
  return out;
}

const point = (lng, lat) => ({ type: 'Point', coordinates: [lng, lat] });

/** Open `provider`'s db, create the three index kinds on "users", insert
 * `count` documents, then churn half of them (updates + deletes) so the
 * append-only files carry real garbage for compact() to drop. Returns the
 * documents that remain. */
async function seedUsers(provider, { count = 60, indexes = true } = {}) {
  const db = await connect(provider);
  const users = await db.collection('users');
  if (indexes) {
    await users.createIndex({ team: 1 }, { name: 'teamIdx' });
    await users.createIndex({ bio: 'text' }, { name: 'bioIdx' });
    await users.createIndex({ loc: '2dsphere' }, { name: 'locIdx' });
  }
  const docs = [];
  for (let i = 0; i < count; i++) {
    const doc = {
      _id: new ObjectId(),
      i,
      team: i % 3 === 0 ? 'core' : 'infra',
      bio: `person number${i} enjoys writing tests`,
      loc: point(i * 0.01, i * 0.01)
    };
    await users.insertOne(doc);
    docs.push(doc);
  }
  for (let i = 0; i < count; i += 4) {
    await users.updateOne({ i }, { $set: { team: 'churned' } });
    docs.find(d => d.i === i).team = 'churned';
  }
  const survivors = [];
  for (const doc of docs) {
    if (doc.i % 5 === 0 && doc.i > 0) await users.deleteOne({ _id: doc._id });
    else survivors.push(doc);
  }
  return { db, users, survivors };
}

async function expectSameDocs(coll, expected) {
  const all = await coll.find({}).toArray();
  expect(all.map(d => d._id.toHexString()).sort())
    .toEqual(expected.map(d => d._id.toHexString()).sort());
}

describe('db: collection compaction', () => {
  it('preserves every document, shrinks the file set, and reports stats', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider, { indexes: false });

    const before = users._storageBytes();
    const stats = await users.compact();

    expect(stats.generation).toBe(1);
    expect(stats.bytesBefore).toBe(before);
    expect(stats.bytesAfter).toBeLessThan(stats.bytesBefore);
    expect(stats.bytesFreed).toBe(stats.bytesBefore - stats.bytesAfter);

    expect(await users.countDocuments({})).toBe(survivors.length);
    await expectSameDocs(users, survivors);
    expect(users._tree.verify()).toBe(true);
    await db.close();
  });

  it('renames the file set to the new generation and deletes the old one', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users } = await seedUsers(provider, { indexes: false });

    // Pre-compact layout is the historical (generation-0) one, and the
    // catalog entry carries neither gen nor journal fields yet -- i.e.
    // compacting a legacy database is the default path, not a special case.
    expect(await provider.listFiles()).toContain('coll-users.bj');
    const entryBefore = db._catalog.search('users');
    expect(entryBefore.gen).toBeUndefined();
    expect(entryBefore.journal).toBeUndefined();

    await users.compact();

    const files = await provider.listFiles();
    expect(files).toContain('g1-coll-users.bj');
    expect(files).toContain('g1-coll-users-journal.bj');
    expect(files).not.toContain('coll-users.bj');
    expect(files).not.toContain('coll-users-journal.bj');

    const entry = db._catalog.search('users');
    expect(entry.gen).toBe(1);
    expect(entry.file).toBe('g1-coll-users.bj');
    expect(entry.journal).toBe('g1-coll-users-journal.bj');
    expect(entry.compactedBytes).toBeGreaterThan(0);
    await db.close();
  });

  it('keeps every index kind live and correct across compact()', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider);

    const indexesBefore = await users.listIndexes();
    await users.compact();
    expect(await users.listIndexes()).toEqual(indexesBefore);

    // Equality index: still planned and correct.
    const core = await users.findByIndex('teamIdx', ['core']);
    expect(core.map(d => d.i).sort((a, b) => a - b))
      .toEqual(survivors.filter(d => d.team === 'core').map(d => d.i).sort((a, b) => a - b));
    expect(users._indexes.get('teamIdx').tree.verify()).toBe(true);

    // Text index: $text still matches on stems present only in survivors.
    const hits = await users.find({ $text: { $search: 'number8' } }).toArray();
    expect(hits.map(d => d.i)).toEqual(survivors.some(d => d.i === 8) ? [8] : []);

    // Geo index: $near returns nearest-first from the compacted R-tree.
    const near = await users.find({ loc: { $near: { $geometry: point(0, 0) } } }).toArray();
    expect(near.length).toBe(survivors.length);
    expect(near[0].i).toBe(Math.min(...survivors.map(d => d.i)));

    // Index files moved to the new generation with the collection.
    const files = await provider.listFiles();
    expect(files).toContain('g1-idx-users-teamIdx.bj');
    expect(files).toContain('g1-idx-users-bioIdx-terms.bj');
    expect(files).toContain('g1-idx-users-locIdx.bj');
    expect(files).not.toContain('idx-users-teamIdx.bj');
    expect(files).not.toContain('idx-users-bioIdx-terms.bj');
    expect(files).not.toContain('idx-users-locIdx.bj');
    await db.close();
  });

  it('keeps unique/partial/TTL index options enforced after compact()', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.createIndex({ email: 1 }, { name: 'emailIdx', unique: true });
    await users.createIndex({ lastSeen: 1 }, { name: 'ttlIdx', expireAfterSeconds: 3600 });
    await users.insertOne({ email: 'ada@example.com', lastSeen: new Date(Date.now() - 7200 * 1000) });
    await users.insertOne({ email: 'grace@example.com', lastSeen: new Date() });

    await users.compact();

    await expect(users.insertOne({ email: 'ada@example.com' })).rejects.toThrow();
    const defs = await users.listIndexes();
    expect(defs.find(d => d.name === 'emailIdx').unique).toBe(true);
    expect(defs.find(d => d.name === 'ttlIdx').expireAfterSeconds).toBe(3600);
    expect(await users.pruneExpired()).toBe(1); // the 2h-old doc, via the compacted TTL index
    await db.close();
  });

  it('stays fully usable for writes and survives a reopen afterwards', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider);

    await users.compact();
    const extra = { _id: new ObjectId(), i: 999, team: 'core', bio: 'joined after compaction', loc: point(1, 1) };
    await users.insertOne(extra);
    await users.updateOne({ i: 999 }, { $set: { team: 'infra' } });
    await users.deleteOne({ _id: survivors[0]._id });
    await db.close();

    // Fresh Db over the same provider: catalog-recorded generation names,
    // journal recovery on the new journal, all indexes reattached.
    const db2 = await connect(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(survivors.length); // -1 deleted, +1 inserted
    expect((await users2.findOne({ i: 999 })).team).toBe('infra');
    expect(await users2.findOne({ _id: survivors[0]._id })).toBeNull();
    expect((await users2.find({ $text: { $search: 'joined' } }).toArray()).map(d => d.i)).toEqual([999]);
    await db2.close();
  });

  it('increments the generation on every compact and never leaves stragglers', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider, { indexes: false });

    for (let gen = 1; gen <= 3; gen++) {
      await users.insertOne({ filler: 'x'.repeat(500) });
      await users.deleteOne({ filler: { $exists: true } });
      const stats = await users.compact();
      expect(stats.generation).toBe(gen);
    }
    const files = await provider.listFiles();
    expect(files.filter(f => f.includes('coll-users')))
      .toEqual(expect.arrayContaining(['g3-coll-users.bj', 'g3-coll-users-journal.bj']));
    expect(files.some(f => f.startsWith('g1-') || f.startsWith('g2-'))).toBe(false);
    await expectSameDocs(users, survivors);
    await db.close();
  });

  it('compacts an empty collection (empty B+ trees and R-tree bulk loads)', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const empty = await db.collection('empty');
    await empty.createIndex({ team: 1 }, { name: 'teamIdx' });
    await empty.createIndex({ loc: '2dsphere' }, { name: 'locIdx' });

    const stats = await empty.compact();
    expect(stats.generation).toBe(1);
    expect(await empty.countDocuments({})).toBe(0);
    await empty.insertOne({ team: 'core', loc: point(0, 0) });
    expect((await empty.findByIndex('teamIdx', ['core'])).length).toBe(1);
    await db.close();
  });

  it('refuses to compact while a find() cursor is open, then succeeds once closed', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const users = await db.collection('users');
    // > one streaming batch (100), so the WASM cursor stays open after next().
    await users.insertMany(Array.from({ length: 250 }, (_, i) => ({ i })));

    const cursor = users.find({});
    await cursor.next();
    await expect(users.compact()).rejects.toThrow(/open find\(\) cursors/);

    await cursor.close();
    const stats = await users.compact();
    expect(stats.generation).toBe(1);
    expect(await users.countDocuments({})).toBe(250);
    await db.close();
  });

  it('queues operations issued mid-compact behind it instead of interleaving or failing', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider);
    // The non-sparse teamIdx makes indexed fields mandatory, compact or not.
    const doc = () => ({ i: -1, team: 'late', bio: 'arrived mid-compact', loc: point(9, 9) });

    const compacting = users.compact(); // _compacting is set synchronously
    // All issued while the compact is in flight; each must resolve, in
    // order, against the *new* generation -- never interleave with the
    // build, never surface a "being compacted" error.
    const settled = [];
    compacting.then(() => settled.push('compact'));
    const queued = Promise.all([
      users.insertOne(doc()).then(() => settled.push('insert')),
      users.findOne({ i: -1 }),
      users.find({ team: 'late' }).toArray(),
      users.compact() // back-to-back compacts serialize too
    ]);

    await compacting;
    const [, found, viaCursor, secondStats] = await queued;
    expect(settled).toEqual(['compact', 'insert']); // the gate held the insert until the swap finished
    expect(found.i).toBe(-1);
    expect(viaCursor).toHaveLength(1);
    expect(secondStats.generation).toBe(2); // the queued compact ran as its own generation
    expect(await users.countDocuments({})).toBe(survivors.length + 1);
    // The queued insert's index entries landed in the new generation.
    expect((await users.find({ $text: { $search: 'arrived' } }).toArray())[0].i).toBe(-1);
    await db.close();
  });

  it('compact() waits for an in-flight operation with internal awaits to drain', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider);
    // A watcher gives deleteOne an internal await (documentKey resolution
    // via findOne) between entering the critical section and mutating --
    // exactly the window a compact() must not start inside.
    const stream = users.watch();

    const settled = [];
    const deleting = users.deleteOne({ i: 1 }).then(() => settled.push('delete'));
    const compacting = users.compact().then(() => settled.push('compact'));
    // deleteOne is counted in-flight, so compact() must be parked in its
    // drain wait with the gate still open -- not building.
    expect(users._compacting).toBeNull();
    expect(users._inFlight).toBeGreaterThan(0);

    await Promise.all([deleting, compacting]);
    expect(settled).toEqual(['delete', 'compact']);
    expect(await users.findOne({ i: 1 })).toBeNull();
    expect(await users.countDocuments({})).toBe(survivors.length - 1);
    // The delete landed in the pre-compact generation: its index entries
    // are consistent in the compacted files.
    expect((await users.find({ i: 1 }).toArray())).toEqual([]);
    stream.close();
    await db.close();
  });

  it('re-entrant operations (bulkWrite nesting other ops) never deadlock against a queued compact', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider);
    const doc = (i) => ({ i, team: 'bulk', bio: `bulk number${i}`, loc: point(1, 1) });

    // Issued together in one tick: bulkWrite's inner insertOne/deleteOne
    // calls re-enter the critical section while compact() waits to drain.
    const bulk = users.bulkWrite([
      { insertOne: { document: doc(1001) } },
      { insertOne: { document: doc(1002) } },
      { deleteOne: { filter: { i: 1001 } } }
    ]);
    const compacting = users.compact();
    const [bulkResult, stats] = await Promise.all([bulk, compacting]);

    expect(bulkResult.insertedCount).toBe(2);
    expect(bulkResult.deletedCount).toBe(1);
    expect(stats.generation).toBe(1);
    expect(await users.countDocuments({})).toBe(survivors.length + 1);
    expect((await users.findOne({ i: 1002 })).team).toBe('bulk');
    await db.close();
  });

  it('an abandoned, unexhausted cursor is eventually reclaimed by GC so compact() can proceed', async () => {
    // Exercises the cursorFinalizer safety net (src/nisaba-wasm.js);
    // requires --expose-gc (vitest.config.js passes it) and tolerates
    // FinalizationRegistry's lazy timing by polling.
    if (typeof globalThis.gc !== 'function') return; // no gc hook -- covered only where exposed
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.insertMany(Array.from({ length: 250 }, (_, i) => ({ i })));

    await (async () => {
      const cursor = users.find({});
      await cursor.next(); // opens the WASM-side cursor (250 > one batch)
      // ...and the cursor is abandoned without close() as this scope exits.
    })();
    expect(users._openCursors.size).toBe(1);
    await expect(users.compact()).rejects.toThrow(/open find\(\) cursors/);

    for (let i = 0; i < 100 && users._openCursors.size > 0; i++) {
      globalThis.gc();
      await new Promise((r) => setTimeout(r, 10));
    }
    expect(users._openCursors.size).toBe(0); // finalizer freed the dc_cursor and its token

    const stats = await users.compact(); // no longer blocked
    expect(stats.generation).toBe(1);
    expect(await users.countDocuments({})).toBe(250);
    await db.close();
  });

  it('watch() streams stay attached across a compact', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users } = await seedUsers(provider, { indexes: false });

    const stream = users.watch();
    const firstChange = new Promise((resolve) => stream.on('change', resolve));
    await users.compact();
    await users.insertOne({ i: 12345 });

    const change = await firstChange;
    expect(change.operationType).toBe('insert');
    expect(change.fullDocument.i).toBe(12345);
    stream.close();
    await db.close();
  });

  it('dropCollection after a compact removes the generation files and journal', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users } = await seedUsers(provider);
    await users.compact();
    await db.dropCollection('users');
    const files = await provider.listFiles();
    expect(files.filter(f => f !== '__catalog__.bj')).toEqual([]);
    await db.close();
  });
});

describe('db: compaction crash windows', () => {
  /** Seed + churn, snapshot the bytes, compact, snapshot again -- the raw
   * material every crash-state combination below is composed from. */
  async function compactWithSnapshots() {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider);
    const pre = snapshotFiles(provider);
    await users.compact();
    const post = snapshotFiles(provider);
    await db.close();
    return { pre, post, survivors };
  }

  async function expectHealthyUsers(provider, survivors) {
    const db = await connect(provider);
    const users = await db.collection('users');
    await expectSameDocs(users, survivors);
    expect(users._tree.verify()).toBe(true);
    // Every index answers, agreeing with the primary tree.
    const viaIndex = await users.findByIndex('teamIdx', ['churned']);
    expect(viaIndex.length).toBe(survivors.filter(d => d.team === 'churned').length);
    expect((await users.find({ $text: { $search: 'tests' } }).toArray()).length).toBe(survivors.length);
    await db.close();
  }

  it('crash mid-build (partial new generation, catalog not flipped): old generation stays live', async () => {
    const { pre, post, survivors } = await compactWithSnapshots();
    // Only the primary tree's new file made it to disk before the crash.
    const provider = providerWith(pre, pick(post, f => f === 'g1-coll-users.bj'));

    await expectHealthyUsers(provider, survivors);
    // The half-built file is unreferenced by the catalog -- swept on open.
    expect(await provider.listFiles()).not.toContain('g1-coll-users.bj');
  });

  it('crash after the build but before the flip: old generation stays live, new files swept', async () => {
    const { pre, post, survivors } = await compactWithSnapshots();
    // Every new-generation file landed, but the catalog commit did not.
    const provider = providerWith(pre, pick(post, f => f.startsWith('g1-')));

    await expectHealthyUsers(provider, survivors);
    expect((await provider.listFiles()).filter(f => f.startsWith('g1-'))).toEqual([]);
  });

  it('crash after the flip but before the old files were deleted: new generation is live, old files swept', async () => {
    const { pre, post, survivors } = await compactWithSnapshots();
    // The post-compact state (flipped catalog + new files), with the old
    // generation's files resurrected as if the deletes never ran.
    const provider = providerWith(post, pick(pre, f => f !== '__catalog__.bj'));

    await expectHealthyUsers(provider, survivors);
    const files = await provider.listFiles();
    expect(files).toContain('g1-coll-users.bj');
    expect(files).not.toContain('coll-users.bj');
    expect(files).not.toContain('coll-users-journal.bj');
    expect(files).not.toContain('idx-users-teamIdx.bj');
  });

  it('the sweep never touches foreign files or referenced ones', async () => {
    const provider = new MemoryStorageProvider();
    const { db, users, survivors } = await seedUsers(provider, { indexes: false });
    await users.compact();
    await db.close();

    provider._files.set('notes.txt', new MemoryHandle(new TextEncoder().encode('mine')));
    provider._files.set('coll-orphan.bj', new MemoryHandle(new Uint8Array([1, 2, 3])));

    const db2 = await connect(provider);
    const files = await provider.listFiles();
    expect(files).toContain('notes.txt');          // not DB_FILE_PATTERN: kept
    expect(files).not.toContain('coll-orphan.bj'); // pattern + unreferenced: swept
    expect(files).toContain('g1-coll-users.bj');   // referenced: kept
    const users2 = await db2.collection('users');
    await expectSameDocs(users2, survivors);
    await db2.close();
  });
});

describe('db: Db.compact()', () => {
  it('compacts every collection unconditionally by default', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    for (const name of ['a', 'b']) {
      const coll = await db.collection(name);
      await coll.insertMany(Array.from({ length: 30 }, (_, i) => ({ i, pad: 'x'.repeat(100) })));
      await coll.deleteMany({ i: { $lt: 15 } });
    }

    const results = await db.compact();
    expect(Object.keys(results).sort()).toEqual(['a', 'b']);
    for (const name of ['a', 'b']) {
      expect(results[name].generation).toBe(1);
      expect(results[name].bytesFreed).toBeGreaterThan(0);
      expect(await (await db.collection(name)).countDocuments({})).toBe(15);
    }
    await db.close();
  });

  it('minBytes/factor skip collections not worth compacting', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const big = await db.collection('big');
    const small = await db.collection('small');
    await big.insertMany(Array.from({ length: 200 }, (_, i) => ({ i, pad: 'x'.repeat(200) })));
    await big.deleteMany({ i: { $lt: 100 } });
    await small.insertOne({ i: 0 });

    const threshold = small._storageBytes() + 1;
    const results = await db.compact({ minBytes: threshold });
    expect(results.small).toBeNull();
    expect(results.big.generation).toBe(1);

    // Right after compacting, `factor` finds nothing worth doing: nothing
    // has grown past factor x its fresh compactedBytes baseline.
    const again = await db.compact({ minBytes: 1, factor: 2 });
    expect(again.big).toBeNull();

    // ...until enough churn accumulates to cross the growth factor.
    for (let round = 0; round < 20; round++) {
      await big.updateMany({}, { $set: { pad: `y${round}`.repeat(100) } });
    }
    const third = await db.compact({ minBytes: 1, factor: 2 });
    expect(third.big.generation).toBe(2);
    await db.close();
  });

  it('skipBusy skips a collection with open cursors instead of throwing', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const busy = await db.collection('busy');
    const idle = await db.collection('idle');
    for (const coll of [busy, idle]) {
      await coll.insertMany(Array.from({ length: 250 }, (_, i) => ({ i })));
      await coll.deleteMany({ i: { $lt: 100 } });
    }

    const cursor = busy.find({});
    await cursor.next(); // > one batch left, so the WASM cursor stays open

    const results = await db.compact({ skipBusy: true });
    expect(results.busy).toBeNull();          // skipped, not thrown
    expect(results.idle.generation).toBe(1);  // the rest of the sweep still ran

    await cursor.close();
    const retry = await db.compact({ skipBusy: true });
    expect(retry.busy.generation).toBe(1);    // gets its turn on the next sweep
    await db.close();
  });
});

describe('db: autoCompact on connect()', () => {
  /** Seed + churn through a plain connect, then close -- the reopen-with-
   * autoCompact scenario is the realistic one (next page load / next
   * leadership acquisition), since a brand-new db has nothing to sweep. */
  async function churnedProvider() {
    const provider = new MemoryStorageProvider();
    const { db } = await seedUsers(provider);
    await db.close();
    return provider;
  }

  it('runs one deferred sweep after open, without blocking connect()', async () => {
    const provider = await churnedProvider();
    const db = await connect(provider, { autoCompact: { minBytes: 1 } });
    expect(db.isOpen).toBe(true); // connect() resolved without waiting for the sweep

    const results = await db.autoCompacted;
    expect(results.users.generation).toBe(1);
    expect(results.users.bytesFreed).toBeGreaterThan(0);
    expect(await (await db.collection('users')).countDocuments({})).toBeGreaterThan(0);
    await db.close();
  });

  it('operations issued right after connect() queue behind the sweep, never fail', async () => {
    const provider = await churnedProvider();
    const db = await connect(provider, { autoCompact: { minBytes: 1 } });
    // Issued while the sweep may hold the collection's compaction gate.
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ i: 999, team: 'late', bio: 'post-sweep', loc: point(1, 1) });
    expect((await users.findOne({ _id: insertedId })).i).toBe(999);
    await db.autoCompacted;
    await db.close();
  });

  it('respects the growth heuristic: a freshly compacted db is left alone', async () => {
    const provider = await churnedProvider();
    const first = await connect(provider, { autoCompact: { minBytes: 1, factor: 2 } });
    expect((await first.autoCompacted).users.generation).toBe(1);
    await first.close();

    // Reopen: nothing has grown past factor x the new baseline.
    const second = await connect(provider, { autoCompact: { minBytes: 1, factor: 2 } });
    expect((await second.autoCompacted).users).toBeNull();
    await second.close();
  });

  it('stays null without the option, and a close() mid-sweep is quiet', async () => {
    const plain = await connect(await churnedProvider());
    expect(plain.autoCompacted).toBeNull();
    await plain.close();

    const db = await connect(await churnedProvider(), { autoCompact: { minBytes: 1 } });
    await db.close(); // interrupts the in-flight sweep -- must not warn or reject
    expect(await db.autoCompacted).toBeDefined(); // settles either way
  });
});
