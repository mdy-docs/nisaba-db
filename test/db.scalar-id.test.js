/**
 * Format v2's `_id` domain, end to end through the in-process host.
 *
 * An `_id` may be an ObjectId, a string without U+0000, a finite number,
 * or a Date — the values the ordered key encoding can order — and the
 * primary tree, every secondary index's back-pointer, the WAL's `id`
 * rows and the change events all carry them. docs/format-compatibility.md
 * has the format's side of that story.
 *
 * What this file is FOR is the paths where an id is not merely stored but
 * *interpreted*: index back-pointers deref'd to fetch a document, a
 * staged build's cursor recording where the backfill got to, a text
 * index's refs, a compaction re-keying rows it copies, and the geo
 * index's fixed-width refs, which are the one place the domain is
 * deliberately narrower. Each of those spelled an id its own way in
 * version 1, and an id it can no longer spell is not a failed write —
 * it is a document that is silently unreachable.
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider } from '../src/db.js';

await ready();

const openDb = () => connect(new MemoryStorageProvider());

/** One id of every admissible type, and a label for failure messages. */
const OID = new ObjectId('0123456789abcdef01234567');
const IDS = [
  ['string', 'user-42'],
  ['number', 7],
  ['float', -0.5],
  ['ObjectId', OID],
  ['Date', new Date(1700000000000)]
];

describe('scalar _id: the primary tree', () => {
  it('stores, finds, updates, replaces and deletes every admissible type', async () => {
    const db = await openDb();
    const c = await db.collection('things');

    for (const [label, _id] of IDS) {
      expect((await c.insertOne({ _id, n: 1 })).insertedId, label).toEqual(_id);
      expect((await c.findOne({ _id })).n, label).toBe(1);
      expect((await c.updateOne({ _id }, { $set: { n: 2 } })).matchedCount, label).toBe(1);
      expect((await c.findOne({ _id })).n, label).toBe(2);
      expect((await c.replaceOne({ _id }, { n: 3 })).matchedCount, label).toBe(1);
      expect((await c.findOne({ _id })).n, label).toBe(3);
    }
    expect(await c.countDocuments({})).toBe(IDS.length);

    for (const [label, _id] of IDS) {
      expect((await c.deleteOne({ _id })).deletedCount, label).toBe(1);
      expect(await c.findOne({ _id }), label).toBeNull();
    }
    expect(await c.countDocuments({})).toBe(0);
    await db.close();
  });

  it('treats an integer and its float spelling as one id', async () => {
    // The key form is canonical over numbers, so 5 and 5.0 name the same
    // document — inserting the second is a duplicate, not a sibling.
    const db = await openDb();
    const c = await db.collection('things');
    await c.insertOne({ _id: 5, n: 1 });
    await expect(c.insertOne({ _id: 5.0, n: 2 })).rejects.toThrow();
    expect((await c.findOne({ _id: 5.0 })).n).toBe(1);
    expect(await c.countDocuments({})).toBe(1);
    await db.close();
  });

  it('caps how long an id may be, rather than truncating one', async () => {
    // A primary key is written into every index entry that points at the
    // document, so an unbounded id is an unbounded cost per entry. The
    // limit is on the id's VALUE form -- a type byte, a 4-byte length,
    // then the UTF-8 -- and it is a refusal, because the alternative is
    // storing a document under a key that is not its id.
    const db = await openDb();
    const c = await db.collection('things');
    const at = (chars) => 'x'.repeat(chars);

    await c.insertOne({ _id: at(251), n: 1 });          // 1 + 4 + 251 = 256
    expect((await c.findOne({ _id: at(251) })).n).toBe(1);
    await expect(c.insertOne({ _id: at(252), n: 2 })).rejects.toThrow(/_id/);
    expect(await c.countDocuments({})).toBe(1);
    await db.close();
  });

  it('orders ids across types by the key encoding, not by insertion', async () => {
    // Cross-type order is the tag order the encoding assigns: numbers,
    // then strings, then ObjectIds, then dates. It only has to be TOTAL
    // and stable — a scan must never depend on which type it starts in.
    const db = await openDb();
    const c = await db.collection('things');
    for (const [, _id] of [...IDS].reverse()) await c.insertOne({ _id });

    const scanned = (await c.find({}).toArray()).map((d) => d._id);
    expect(scanned).toEqual([-0.5, 7, 'user-42', OID, new Date(1700000000000)]);

    // The same order a paged scan produces, one document at a time, so
    // the cursor's resume key round-trips through every type boundary.
    const paged = [];
    const cursor = c.find({}, { batchSize: 1 });
    for await (const doc of cursor) paged.push(doc._id);
    expect(paged).toEqual(scanned);
    await db.close();
  });
});

describe('scalar _id: secondary indexes', () => {
  it('an index deref fetches the document its back-pointer names', async () => {
    // The back-pointer is the half of an index that has to spell an id:
    // the composite key ends in one and the row value holds one. A lookup
    // that finds the entry but cannot resolve it reads as "no document".
    const db = await openDb();
    const c = await db.collection('things');
    await c.createIndex({ email: 1 }, { unique: true });
    for (const [label, _id] of IDS) await c.insertOne({ _id, email: `${label}@x.test` });

    for (const [label, _id] of IDS) {
      const hits = await c.findByIndex('email_1', [`${label}@x.test`]);
      expect(hits.map((d) => d._id), label).toEqual([_id]);
      // The indexed read path and the scan agree about the document.
      expect((await c.findOne({ email: `${label}@x.test` }))._id, label).toEqual(_id);
    }

    // Uniqueness holds across id types, and the violated write leaves
    // nothing behind.
    await expect(c.insertOne({ _id: 'other', email: 'string@x.test' })).rejects.toThrow();
    expect(await c.countDocuments({})).toBe(IDS.length);

    // Deleting through the index removes both halves: the entry stops
    // answering and the id it named is gone.
    await c.deleteOne({ _id: 7 });
    expect(await c.findByIndex('email_1', ['number@x.test'])).toEqual([]);
    await db.close();
  });

  it('a staged build backfills scalar-keyed documents, cursor and all', async () => {
    /*
     * The backfill cursor is a stored id: each chunk records the last
     * document it reached so the next resumes after it. In version 1 that
     * was twelve raw bytes in the catalog entry; it is a value form now,
     * and a chunk that cannot record where it got to either loops or
     * skips the rest of the collection.
     *
     * k is deliberately small so the cursor is written and re-read many
     * times, crossing every type boundary on the way.
     */
    const db = await openDb();
    const c = await db.collection('things');
    for (let i = 0; i < 12; i++) {
      const _id = i % 3 === 0 ? `s-${i}` : (i % 3 === 1 ? i : new Date(1700000000000 + i));
      await c.insertOne({ _id, team: i % 2 ? 'core' : 'ops' });
    }

    const name = await c.indexBegin({ team: 1 });
    let rounds = 0, advanced = 0;
    for (;;) {
      const step = await c.indexChunk(name, 2);
      advanced += step.advanced;
      if (step.done) break;
      expect(++rounds, 'the build never finished').toBeLessThan(50);
    }
    expect(advanced).toBe(12);       // every document, exactly once
    expect(rounds).toBeGreaterThan(1); // and the cursor really was resumed

    const core = await c.findByIndex('team_1', ['core']);
    expect(core).toHaveLength(6);
    expect(core.every((d) => d.team === 'core')).toBe(true);
    await db.close();
  });

  it('a text index resolves its refs back to scalar-keyed documents', async () => {
    // A text index names documents by a ref string of its own rather than
    // by a composite key, so it has its own spelling of an id to get
    // right (db.c's id_ref_hex/hex_to_key).
    const db = await openDb();
    const c = await db.collection('posts');
    await c.createIndex({ body: 'text' });
    await c.insertOne({ _id: 'post-str', body: 'a fox in the forest' });
    await c.insertOne({ _id: 11, body: 'a fox downtown' });
    await c.insertOne({ _id: new Date(1700000000000), body: 'a badger, elsewhere' });

    const foxes = await c.find({ $text: { $search: 'fox' } }).toArray();
    expect(foxes.map((d) => d._id).sort((a, b) => String(a).localeCompare(String(b))))
      .toEqual([11, 'post-str']);

    // Updating and deleting go back through the same refs.
    await c.updateOne({ _id: 11 }, { $set: { body: 'a badger now' } });
    expect((await c.find({ $text: { $search: 'fox' } }).toArray()).map((d) => d._id))
      .toEqual(['post-str']);
    await c.deleteOne({ _id: 'post-str' });
    expect(await c.find({ $text: { $search: 'fox' } }).toArray()).toEqual([]);
    expect((await c.find({ $text: { $search: 'badger' } }).toArray())).toHaveLength(2);
    await db.close();
  });

  it('a geo index refuses a non-ObjectId id rather than truncating it', async () => {
    /*
     * The one place the domain is narrower, stated rather than hidden:
     * an r-tree ref is twelve fixed bytes, so a geo-indexed write of a
     * document keyed by anything else has nowhere to put the id. It is
     * refused at the write, before the document exists — never written
     * and left out of the index, which would be a document the geo query
     * cannot see and nothing to say so.
     */
    const db = await openDb();
    const c = await db.collection('places');
    await c.createIndex({ location: '2dsphere' });
    const point = { type: 'Point', coordinates: [-0.1, 51.5] };

    await expect(c.insertOne({ _id: 'london', location: point })).rejects.toThrow();
    expect(await c.countDocuments({})).toBe(0);

    // An ObjectId id is ordinary, and a scalar-keyed document with no
    // indexed field at all is fine: the refusal is about the index entry,
    // not about the collection.
    const { insertedId } = await c.insertOne({ name: 'London', location: point });
    expect(insertedId).toBeInstanceOf(ObjectId);
    await c.insertOne({ _id: 'no-location', name: 'nowhere' });
    expect(await c.countDocuments({})).toBe(2);

    const near = await c.find({ location: { $near: { $geometry: point } } }).toArray();
    expect(near.map((d) => d.name)).toEqual(['London']);
    await db.close();
  });
});

describe('scalar _id: rewriting the files underneath', () => {
  it('compaction re-keys every row it copies', async () => {
    // compact() rebuilds the primary tree and every index into fresh
    // files. It is the same machinery the format migration rides, so a
    // key it got wrong would show up here first.
    const db = await openDb();
    const c = await db.collection('things');
    await c.createIndex({ team: 1 });
    for (const [label, _id] of IDS) await c.insertOne({ _id, team: label });
    await c.deleteOne({ _id: 7 });          // leave history to reclaim

    const { generation } = await c.compact();
    expect(generation).toBeGreaterThan(0);

    for (const [label, _id] of IDS.filter(([, v]) => v !== 7)) {
      expect((await c.findOne({ _id })).team, label).toBe(label);
      expect((await c.findByIndex('team_1', [label])).map((d) => d._id), label).toEqual([_id]);
    }
    expect(await c.findOne({ _id: 7 })).toBeNull();
    await db.close();
  });

  it('survives a close and reopen, which is the only test the files take', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const c = await db.collection('things');
    await c.createIndex({ team: 1 });
    for (const [label, _id] of IDS) await c.insertOne({ _id, team: label });
    await db.close();

    const reopened = await connect(provider);
    const back = await reopened.collection('things');
    for (const [label, _id] of IDS) {
      expect((await back.findOne({ _id })).team, label).toBe(label);
      expect((await back.findByIndex('team_1', [label])).map((d) => d._id), label).toEqual([_id]);
    }
    await reopened.close();
  });
});
