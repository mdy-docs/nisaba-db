/**
 * Cross-file write atomicity tests for the document database (milestone 5,
 * docs/db-plan.md). Every insertOne/deleteOne/replaceOne/updateOne (and each
 * matched document within updateMany) commits its primary-tree write and
 * every attached index's write(s) as one journaled transaction, generalizing
 * textindex.c's fixed-3-tree journal (test/textindex.atomic-wasm.test.js) to
 * a variable number of files: primary + equality index (1) + text index (3)
 * + geo index (1).
 *
 * Crashes are simulated by snapshotting the collection's backing files
 * mid-sequence and restoring subsets, below the Collection abstraction,
 * exactly like the textindex tests.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ready } from '../src/nisaba-wasm.js';
import { ObjectId } from '../src/nisaba-wasm.js';
import { connect, OPFSStorageProvider } from '../src/db.js';
import { deleteFile, getFileHandle } from '../src/nisaba-wasm.js';
import { bootstrapOPFS } from './binjson.suite.js';

await ready();
const { hasOPFS } = await bootstrapOPFS();

describe.skipIf(!hasOPFS)('WASM db: cross-file write atomicity', () => {
  let root = null;
  let counter = 0;
  const files = [];
  const base = () => `test-dbatomic-${Date.now()}-${counter++}`;

  beforeAll(async () => {
    root = await navigator.storage.getDirectory();
  });

  afterAll(async () => {
    for (const f of files) await deleteFile(root, f).catch(() => {});
  });

  function trackFiles(collName, indexNames = ['teamIdx', 'textIdx', 'geoIdx']) {
    const names = [`coll-${collName}.bj`, `coll-${collName}-journal.bj`, '__catalog__.bj'];
    if (indexNames.includes('teamIdx')) names.push(`idx-${collName}-teamIdx.bj`);
    if (indexNames.includes('textIdx')) {
      names.push(`idx-${collName}-textIdx-terms.bj`, `idx-${collName}-textIdx-documents.bj`, `idx-${collName}-textIdx-lengths.bj`);
    }
    if (indexNames.includes('geoIdx')) names.push(`idx-${collName}-geoIdx.bj`);
    for (const f of names) if (!files.includes(f)) files.push(f);
    return names;
  }

  async function openDb() {
    return connect(new OPFSStorageProvider(root));
  }

  async function setupIndexedCollection(collName) {
    const db = await openDb();
    const coll = await db.collection(collName);
    await coll.createIndex({ team: 1 }, { name: 'teamIdx' });
    await coll.createIndex({ body: 'text' }, { name: 'textIdx' });
    await coll.createIndex({ loc: '2dsphere' }, { name: 'geoIdx' });
    trackFiles(collName);
    return { db, coll };
  }

  function seedDoc(i) {
    return {
      _id: new ObjectId(),
      team: i % 2 === 0 ? 'core' : 'infra',
      body: `shared corpus unique${i}x number${i}`,
      loc: { type: 'Point', coordinates: [i * 0.01, i * 0.01] }
    };
  }

  async function seed(collName, k) {
    const { db, coll } = await setupIndexedCollection(collName);
    const docs = [];
    for (let i = 0; i < k; i++) {
      const doc = seedDoc(i);
      await coll.insertOne(doc);
      docs.push(doc);
    }
    await db.close();
    return docs;
  }

  async function readBytes(filename) {
    const fh = await getFileHandle(root, filename, { create: false });
    const h = await fh.createSyncAccessHandle();
    const buf = new Uint8Array(h.getSize());
    h.read(buf, { at: 0 });
    await h.close();
    return buf;
  }
  async function writeBytes(filename, buf) {
    const fh = await getFileHandle(root, filename, { create: false });
    const h = await fh.createSyncAccessHandle();
    h.truncate(0);
    h.write(buf, { at: 0 });
    h.flush();
    await h.close();
  }
  async function snapshot(names) {
    const out = {};
    for (const f of names) out[f] = await readBytes(f);
    return out;
  }
  async function restore(snap, subset) {
    for (const f of subset) await writeBytes(f, snap[f]);
  }

  /** Every document present in the primary tree must also be reachable
   * through every index, and vice versa (findByIndex / $text / $near all
   * agree with a full scan) -- proof the write(s) landed as a unit. */
  async function expectConsistent(collName, expectedDocs) {
    const db = await openDb();
    const coll = await db.collection(collName);
    const all = await coll.find({}).toArray();
    expect(all.map(d => d._id.toHexString()).sort())
      .toEqual(expectedDocs.map(d => d._id.toHexString()).sort());

    for (const doc of all) {
      const byTeam = await coll.findByIndex('teamIdx', [doc.team]);
      expect(byTeam.some(d => d._id.equals(doc._id))).toBe(true);

      const term = doc.body.match(/unique\d+x/)[0];
      const textHits = await coll.find({ $text: { $search: term } }).toArray();
      expect(textHits.some(d => d._id.equals(doc._id))).toBe(true);

      const nearHits = await coll.find({
        loc: { $near: { $geometry: doc.loc, $maxDistance: 1 } }
      }).toArray();
      expect(nearHits.some(d => d._id.equals(doc._id))).toBe(true);
    }
    await db.close();
    return all;
  }

  it('journaled writes work normally and the journal stays bounded', async () => {
    const name = base();
    const docs = await seed(name, 10);
    // n = primary + equality(1) + text(3) + geo(1) = 6 files/slot.
    const j = await readBytes(`coll-${name}-journal.bj`);
    expect(j.byteLength).toBeLessThanOrEqual(2 * (24 + 8 * 6));
    await expectConsistent(name, docs);
  });

  it('a collection with no secondary indexes keeps an empty journal', async () => {
    const name = base();
    const db = await openDb();
    const coll = await db.collection(name);
    trackFiles(name, []);
    await coll.insertOne({ _id: new ObjectId(), x: 1 });
    await coll.insertOne({ _id: new ObjectId(), x: 2 });
    await db.close();
    const j = await readBytes(`coll-${name}-journal.bj`);
    expect(j.byteLength).toBe(0);
  });

  it('rolls back an insertOne whose journal record never landed', async () => {
    const name = base();
    const docs = await seed(name, 8);
    const names = trackFiles(name);
    const snap = await snapshot(names);

    const db = await openDb();
    const coll = await db.collection(name);
    await coll.insertOne(seedDoc(8));
    await db.close();

    // Crash simulation: every tree write persisted, the journal write did
    // not. The reopened collection must show no trace of the 9th document.
    await restore(snap, [`coll-${name}-journal.bj`]);
    await expectConsistent(name, docs);
  });

  it('rolls back a partially persisted insertOne (one index file behind)', async () => {
    const name = base();
    const docs = await seed(name, 8);
    const names = trackFiles(name);
    const snap = await snapshot(names);

    const db = await openDb();
    const coll = await db.collection(name);
    await coll.insertOne(seedDoc(8));
    await db.close();

    // Crash simulation: primary + equality + text landed, the geo index and
    // the journal did not.
    await restore(snap, [`idx-${name}-geoIdx.bj`, `coll-${name}-journal.bj`]);
    await expectConsistent(name, docs);
  });

  it('falls back to the previous journal slot when the newest is unsatisfiable', async () => {
    const name = base();
    const docs = await seed(name, 8);
    const names = trackFiles(name);
    const snap = await snapshot(names);

    const db = await openDb();
    const coll = await db.collection(name);
    await coll.insertOne(seedDoc(8));
    await db.close();

    // Crash simulation: the journal's newest slot persisted, but none of
    // the tree writes did. The previous slot matches the trees exactly.
    await restore(snap, names.filter(f => !f.endsWith('-journal.bj') && !f.includes('__catalog__')));
    await expectConsistent(name, docs);
  });

  it('refuses to open when the files are behind every journal record', async () => {
    const name = base();
    await seed(name, 8);
    const populatedJournal = await readBytes(`coll-${name}-journal.bj`);

    // A structurally identical but empty collection: its own files are far
    // shorter than anything the populated journal records.
    const fresh = base();
    {
      const db = await openDb();
      const coll = await db.collection(fresh);
      await coll.createIndex({ team: 1 }, { name: 'teamIdx' });
      await coll.createIndex({ body: 'text' }, { name: 'textIdx' });
      await coll.createIndex({ loc: '2dsphere' }, { name: 'geoIdx' });
      trackFiles(fresh);
      await db.close();
    }
    await writeBytes(`coll-${fresh}-journal.bj`, populatedJournal);

    const db = await openDb();
    await expect(db.collection(fresh)).rejects.toThrow();
    await db.close();
  });

  it('rolls back deleteOne, replaceOne and updateOne the same way', async () => {
    const name = base();
    const docs = await seed(name, 6);
    const names = trackFiles(name);

    // deleteOne
    let snap = await snapshot(names);
    let db = await openDb();
    let coll = await db.collection(name);
    expect(await coll.deleteOne({ _id: docs[0]._id })).toMatchObject({ deletedCount: 1 });
    await db.close();
    await restore(snap, [`coll-${name}-journal.bj`]);
    await expectConsistent(name, docs);

    // replaceOne
    snap = await snapshot(names);
    db = await openDb();
    coll = await db.collection(name);
    const replacement = { ...seedDoc(100), _id: docs[1]._id };
    await coll.replaceOne({ _id: docs[1]._id }, replacement);
    await db.close();
    await restore(snap, [`coll-${name}-journal.bj`]);
    await expectConsistent(name, docs);

    // updateOne
    snap = await snapshot(names);
    db = await openDb();
    coll = await db.collection(name);
    await coll.updateOne({ _id: docs[2]._id }, { $set: { team: 'changed' } });
    await db.close();
    await restore(snap, [`coll-${name}-journal.bj`]);
    const finalDocs = await expectConsistent(name, docs);
    expect(finalDocs.find(d => d._id.equals(docs[2]._id)).team).toBe(docs[2].team);
  });

  it('rolls back an entire updateMany when its journal writes are lost', async () => {
    const name = base();
    const docs = await seed(name, 6);
    const names = trackFiles(name);
    const snap = await snapshot(names);

    const db = await openDb();
    const coll = await db.collection(name);
    const result = await coll.updateMany({ team: 'core' }, { $set: { team: 'core-v2' } });
    expect(result.matchedCount).toBeGreaterThan(0);
    await db.close();

    await restore(snap, [`coll-${name}-journal.bj`]);
    const finalDocs = await expectConsistent(name, docs);
    for (const d of finalDocs) expect(d.team === 'core' || d.team === 'infra').toBe(true);
  });

  it('createIndex resets the journal, and recovery still works after N changes', async () => {
    const name = base();
    const db0 = await openDb();
    const coll0 = await db0.collection(name);
    await coll0.createIndex({ team: 1 }, { name: 'teamIdx' });
    trackFiles(name, ['teamIdx']);
    const docs = [];
    for (let i = 0; i < 5; i++) {
      const doc = seedDoc(i);
      await coll0.insertOne(doc);
      docs.push(doc);
    }
    // Read the size through coll0's own already-open journal handle rather
    // than readBytes' fresh one -- coll0 hasn't closed it yet at this point,
    // and node-opfs now enforces OPFS's real single-writer-per-file
    // constraint (previously unenforced, silently allowing a second handle
    // on the same still-open file).
    expect(coll0._journal.getSize()).toBeGreaterThan(0);

    // Structural change: journal must reset (index count is about to change).
    await coll0.createIndex({ body: 'text' }, { name: 'textIdx' });
    trackFiles(name, ['teamIdx', 'textIdx']);
    expect(coll0._journal.getSize()).toBe(0);
    await db0.close();

    // An empty journal imposes no constraint (rolling back the very first
    // write after a reset is indistinguishable from journaling being off,
    // same as tix_clear's own documented limitation) -- so establish one
    // settled post-reset commit before snapshotting, giving recovery a real
    // baseline to fall back to.
    const db1 = await openDb();
    const coll1 = await db1.collection(name);
    const settled = seedDoc(5);
    await coll1.insertOne(settled);
    docs.push(settled);
    await db1.close();

    const names = trackFiles(name, ['teamIdx', 'textIdx']);
    const snap = await snapshot(names);
    const db2 = await openDb();
    const coll2 = await db2.collection(name);
    const extra = seedDoc(6);
    await coll2.insertOne(extra);
    await db2.close();
    await restore(snap, [`coll-${name}-journal.bj`]);

    const db3 = await openDb();
    const coll3 = await db3.collection(name);
    const all = await coll3.find({}).toArray();
    expect(all.map(d => d._id.toHexString()).sort())
      .toEqual(docs.map(d => d._id.toHexString()).sort());
    for (const doc of all) {
      const byTeam = await coll3.findByIndex('teamIdx', [doc.team]);
      expect(byTeam.some(d => d._id.equals(doc._id))).toBe(true);
    }
    await db3.close();
  });
});
