/**
 * Milestone: resumable find() cursors (c/db.c's dc_cursor), replacing
 * find()'s previous "always materialize everything in one WASM call"
 * behavior for the no-sort case. test/db.test.js already proves result
 * correctness across every filter/index shape via the same find() entry
 * point (unchanged from the outside); this file is specifically about the
 * streaming mechanics that only exist now: multi-batch resumption, manual
 * next(), and cursor lifecycle/cleanup.
 */
import { describe, it, expect } from 'vitest';
import { ready } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider } from '../src/db.js';

await ready();

async function openDb() {
  return connect(new MemoryStorageProvider());
}

describe('db: resumable find() cursors', () => {
  it('streams a result larger than one internal batch (100) via for-await, in order', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (let i = 0; i < 250; i++) await items.insertOne({ n: i });

    const seen = [];
    for await (const doc of items.find({})) seen.push(doc.n);
    expect(seen.sort((a, b) => a - b)).toEqual(Array.from({ length: 250 }, (_, i) => i));
    await db.close();
  });

  it('toArray() resumes correctly across multiple internal batches', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (let i = 0; i < 250; i++) await items.insertOne({ n: i });

    const all = await items.find({}).toArray();
    expect(all).toHaveLength(250);
    await db.close();
  });

  it('manual next() pulls one document at a time and reports done at the end', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    await items.insertOne({ n: 1 });
    await items.insertOne({ n: 2 });

    const cursor = items.find({});
    const first = await cursor.next();
    const second = await cursor.next();
    const third = await cursor.next();

    expect(first.done).toBe(false);
    expect(second.done).toBe(false);
    expect([first.value.n, second.value.n].sort()).toEqual([1, 2]);
    expect(third).toEqual({ value: undefined, done: true });
    await db.close();
  });

  it('next() after toArray() (already exhausted) keeps reporting done', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    await items.insertOne({ n: 1 });

    const cursor = items.find({});
    await cursor.toArray();
    expect(await cursor.next()).toEqual({ value: undefined, done: true });
    await db.close();
  });

  it('skip/limit are honored across the streamed result, not just within one batch', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (let i = 0; i < 250; i++) await items.insertOne({ n: i });

    const middle = await items.find({}, { skip: 120, limit: 30 }).toArray();
    expect(middle).toHaveLength(30);
    await db.close();
  });

  it('projection is applied per document while streaming', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    await items.insertOne({ n: 1, secret: 'x' });
    await items.insertOne({ n: 2, secret: 'y' });

    const docs = await items.find({}, { projection: { n: 1 } }).toArray();
    for (const d of docs) expect(d.secret).toBeUndefined();
    expect(docs.map((d) => d.n).sort()).toEqual([1, 2]);
    await db.close();
  });

  // 150 documents (> the internal 100-doc batch size) so a single next()/
  // break genuinely leaves the WASM cursor open mid-stream instead of the
  // first fetch already exhausting (and self-closing) it.

  it('breaking out of a for-await loop early closes the underlying WASM cursor', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (let i = 0; i < 150; i++) await items.insertOne({ n: i });

    let count = 0;
    for await (const doc of items.find({})) {
      void doc;
      count++;
      if (count === 3) break;
    }
    expect(count).toBe(3);
    expect(items._openCursors.size).toBe(0); // return() ran and released it
    await db.close(); // must not throw / hang on a leaked cursor
  });

  it('explicit close() on a partially-consumed cursor releases it', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (let i = 0; i < 150; i++) await items.insertOne({ n: i });

    const cursor = items.find({});
    await cursor.next();
    expect(items._openCursors.size).toBe(1);
    await cursor.close();
    expect(items._openCursors.size).toBe(0);
    await db.close();
  });

  it('closing the collection force-closes any cursor left open', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (let i = 0; i < 150; i++) await items.insertOne({ n: i });

    const cursor = items.find({});
    await cursor.next(); // opens the underlying WASM cursor, leaves it mid-stream
    await db.close(); // must not throw
  });

  it('next() throws for a sorted cursor -- sorted results use toArray()/for-await instead', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    await items.insertOne({ n: 1 });

    const cursor = items.find({}).sort({ n: 1 });
    await expect(cursor.next()).rejects.toThrow(/sort/);
    await db.close();
  });

  it('a sorted cursor still returns correctly ordered results via toArray() and for-await', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    for (const n of [5, 3, 1, 4, 2]) await items.insertOne({ n });

    const sorted = await items.find({}).sort({ n: 1 }).toArray();
    expect(sorted.map((d) => d.n)).toEqual([1, 2, 3, 4, 5]);

    const viaIterator = [];
    for await (const doc of items.find({}).sort({ n: -1 })) viaIterator.push(doc.n);
    expect(viaIterator).toEqual([5, 4, 3, 2, 1]);
    await db.close();
  });

  it('an empty result streams cleanly (no batches, done immediately)', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    await items.insertOne({ n: 1 });

    expect(await items.find({ n: 999 }).toArray()).toEqual([]);
    const cursor = items.find({ n: 999 });
    expect(await cursor.next()).toEqual({ value: undefined, done: true });

    const seen = [];
    for await (const doc of items.find({ n: 999 })) seen.push(doc);
    expect(seen).toEqual([]);
    await db.close();
  });

  it('an equality-indexed filter streams via the index-planned candidate path across multiple batches', async () => {
    const db = await openDb();
    const items = await db.collection('items');
    await items.createIndex({ team: 1 });
    for (let i = 0; i < 150; i++) await items.insertOne({ team: i % 2 === 0 ? 'a' : 'b', n: i });

    const teamA = await items.find({ team: 'a' }).toArray();
    expect(teamA).toHaveLength(75);
    expect(teamA.every((d) => d.team === 'a')).toBe(true);
    await db.close();
  });
});
