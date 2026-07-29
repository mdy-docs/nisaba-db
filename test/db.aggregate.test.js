/**
 * aggregate() subset + explain() (docs/roadmap.md P2). aggregate: a
 * leading $match runs in the engine (indexes + full operator grammar);
 * later stages run in JS with documented subset semantics. explain:
 * dcw_explain consults the same C planners the queries run, so the
 * report is the dispatch's truth, not a JS guess.
 */
import { describe, it, expect } from 'vitest';
import { ready, MemoryStorageProvider, ObjectId } from '../wasm/nisaba-wasm.js';
import { connect } from '../src/db.js';

await ready();

const point = (lng, lat) => ({ type: 'Point', coordinates: [lng, lat] });

async function salesDb() {
  const db = await connect(new MemoryStorageProvider());
  const sales = await db.collection('sales');
  await sales.createIndex({ region: 1 }, { name: 'regionIdx' });
  const rows = [
    { region: 'eu', product: 'a', qty: 2, price: 10, day: new Date('2026-01-01') },
    { region: 'eu', product: 'b', qty: 1, price: 40, day: new Date('2026-01-02') },
    { region: 'us', product: 'a', qty: 5, price: 10, day: new Date('2026-01-01') },
    { region: 'us', product: 'b', qty: 3, price: 40, day: new Date('2026-01-03') },
    { region: 'us', product: 'a', qty: 1, price: 12, day: new Date('2026-01-04') },
    { region: 'ap', product: 'c', qty: 7, price: 5, day: new Date('2026-01-01') }
  ];
  await sales.insertMany(rows);
  return { db, sales };
}

describe('db: aggregate()', () => {
  it('$group with accumulators over a pushed-down $match', async () => {
    const { db, sales } = await salesDb();
    const out = await sales.aggregate([
      { $match: { region: { $in: ['eu', 'us'] } } },
      { $group: {
        _id: '$region',
        n: { $count: {} },
        units: { $sum: '$qty' },
        revenue: { $sum: '$price' },
        avgQty: { $avg: '$qty' },
        maxPrice: { $max: '$price' },
        firstProduct: { $first: '$product' },
        products: { $addToSet: '$product' }
      } },
      { $sort: { _id: 1 } }
    ]).toArray();

    expect(out).toHaveLength(2);
    const [eu, us] = out;
    expect(eu._id).toBe('eu');
    expect(eu.n).toBe(2);
    expect(eu.units).toBe(3);
    expect(eu.avgQty).toBe(1.5);
    expect(eu.products.sort()).toEqual(['a', 'b']);
    expect(us.units).toBe(9);
    expect(us.maxPrice).toBe(40);
    await db.close();
  });

  it('composite group ids, $push, and $sum with a literal', async () => {
    const { db, sales } = await salesDb();
    const out = await sales.aggregate([
      { $group: { _id: { r: '$region', p: '$product' }, count: { $sum: 1 }, qtys: { $push: '$qty' } } },
      { $sort: { 'count': -1, '_id.r': 1, '_id.p': 1 } }
    ]).toArray();
    const usA = out.find((g) => g._id.r === 'us' && g._id.p === 'a');
    expect(usA.count).toBe(2);
    expect(usA.qtys.sort()).toEqual([1, 5]);
    expect(out[0].count).toBe(2); // sorted by count desc
    await db.close();
  });

  it('$project inclusion, exclusion, computed fields, and _id rules', async () => {
    const { db, sales } = await salesDb();
    const inc = await sales.aggregate([
      { $match: { region: 'ap' } },
      { $project: { product: 1, total: '$qty', _id: 0 } }
    ]).toArray();
    expect(inc).toEqual([{ product: 'c', total: 7 }]);

    const exc = await sales.aggregate([
      { $match: { region: 'ap' } },
      { $project: { day: 0, _id: 0 } }
    ]).toArray();
    expect(exc).toEqual([{ region: 'ap', product: 'c', qty: 7, price: 5 }]);

    await expect(sales.aggregate([{ $project: { a: 1, b: 0 } }]).toArray())
      .rejects.toThrow(/cannot mix inclusion and exclusion/);
    await db.close();
  });

  it('later $match (post-$group), $skip/$limit, and $count', async () => {
    const { db, sales } = await salesDb();
    const busy = await sales.aggregate([
      { $group: { _id: '$region', units: { $sum: '$qty' } } },
      { $match: { units: { $gte: 3 } } },
      { $sort: { units: -1 } },
      { $skip: 1 },
      { $limit: 1 }
    ]).toArray();
    expect(busy).toHaveLength(1);
    expect(busy[0].units).toBe(7); // us=9 skipped, ap=7 next, eu=3 cut by limit

    const counted = await sales.aggregate([
      { $match: { product: 'a' } },
      { $count: 'aSales' }
    ]).toArray();
    expect(counted).toEqual([{ aSales: 3 }]);
    await db.close();
  });

  it('async iteration and next() work on the aggregation cursor', async () => {
    const { db, sales } = await salesDb();
    const cursor = sales.aggregate([{ $group: { _id: '$product', n: { $sum: 1 } } }, { $sort: { _id: 1 } }]);
    const seen = [];
    for await (const row of cursor) seen.push(row._id);
    expect(seen).toEqual(['a', 'b', 'c']);
    expect((await cursor.next()).done).toBe(true); // exhausted
    await db.close();
  });

  it('rejects unsupported stages loudly, naming the offending stage', async () => {
    const { db, sales } = await salesDb();
    const err = await sales.aggregate([{ $unwind: '$qty' }]).toArray().catch((e) => e);
    expect(err.message).toMatch(/unsupported stage/);
    // C reports which stage index failed; this side quotes it from the
    // pipeline it already holds.
    expect(err.message).toMatch(/stage 0/);
    expect(err.message).toMatch(/\$unwind/);
    await db.close();
  });

  it('gives a later $match the full engine grammar', async () => {
    // Previously the post-first-stage $match was a nine-operator JS
    // subset that threw on anything else -- a second, weaker matcher
    // alongside db_query.c's. Now every stage matches with the one
    // engine evaluator, so operators like $regex work throughout.
    const { db, sales } = await salesDb();
    const out = await sales.aggregate([
      { $group: { _id: '$region', n: { $sum: 1 } } },
      { $match: { _id: { $regex: '^a' } } },
      { $sort: { _id: 1 } }
    ]).toArray();
    expect(out.map((r) => r._id)).toEqual(['ap']);

    // ...and operators the subset never had, on synthesized documents.
    const typed = await sales.aggregate([
      { $group: { _id: '$region', n: { $sum: 1 } } },
      { $match: { n: { $type: 'number' } } }
    ]).toArray();
    expect(typed.length).toBeGreaterThan(0);
    await db.close();
  });
});

describe('db: explain()', () => {
  async function indexedDb() {
    const db = await connect(new MemoryStorageProvider());
    const users = await db.collection('users');
    await users.createIndex({ team: 1 }, { name: 'teamIdx' });
    await users.createIndex({ bio: 'text' }, { name: 'bioIdx' });
    await users.createIndex({ loc: '2dsphere' }, { name: 'locIdx' });
    await users.insertOne({ team: 'core', bio: 'writes tests', loc: point(0, 0), age: 30 });
    return { db, users };
  }

  it('reports the serving source for each dispatch case', async () => {
    const { db, users } = await indexedDb();
    expect(await users.explain({ team: 'core' })).toEqual({ source: 'equality', index: 'teamIdx' });
    expect(await users.explain({ $text: { $search: 'tests' } })).toEqual({ source: 'text', index: 'bioIdx' });
    expect(await users.explain({ loc: { $near: { $geometry: point(0, 0) } } })).toEqual({ source: 'geo', index: 'locIdx' });
    expect(await users.explain({ _id: new ObjectId() })).toEqual({ source: 'ids', index: null });
    expect(await users.explain({ age: { $gt: 20 } })).toEqual({ source: 'scan', index: null });
    // Operator conditions defeat the (deliberately conservative) equality planner.
    expect((await users.explain({ team: { $ne: 'core' } })).source).toBe('scan');
    await db.close();
  });

  it('is available as find(filter).explain() sugar', async () => {
    const { db, users } = await indexedDb();
    expect(await users.find({ team: 'core' }).explain()).toEqual({ source: 'equality', index: 'teamIdx' });
    await db.close();
  });

  it('find({_id}) actually uses the point lookup it reports', async () => {
    // The id fast path added alongside dc_explain: an exact-id find()
    // returns the document without a full scan (behavioral check: the
    // plan says 'ids' and the query agrees with findOne).
    const db = await connect(new MemoryStorageProvider());
    const c = await db.collection('t');
    const { insertedId } = await c.insertOne({ x: 1 });
    await c.insertMany(Array.from({ length: 300 }, (_, i) => ({ i }))); // > one cursor batch
    expect((await c.explain({ _id: insertedId })).source).toBe('ids');
    const viaFind = await c.find({ _id: insertedId }).toArray();
    expect(viaFind).toHaveLength(1);
    expect(viaFind[0].x).toBe(1);
    expect(await c.countDocuments({ _id: insertedId })).toBe(1);
    await db.close();
  });
});
