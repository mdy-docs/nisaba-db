/**
 * Document Database Example
 *
 * This example demonstrates how to use the document database (src/db.js)
 * directly from JavaScript — the same API the `db` CLI tool (bin/db.js)
 * and the driver-shaped Collection class in src/nisaba-wasm.js expose:
 * - Connecting to a database and creating collections
 * - insertOne / findOne / find / deleteOne / replaceOne / countDocuments
 * - Query operators ($gt, $in, $exists, $and, $or, ...), sort/skip/limit,
 *   and projections
 * - Secondary indexes (createIndex / findByIndex / listIndexes / dropIndex)
 *
 * See docs/db-plan.md for the design and current limitations.
 */

import { ready } from '../src/nisaba-wasm.js';
import { connect, OPFSStorageProvider } from '../src/db.js';

// Set up node-opfs for Node.js environment
async function setupOPFS() {
  try {
    const nodeOpfs = await import('node-opfs');
    if (nodeOpfs.navigator && typeof global !== 'undefined') {
      Object.defineProperty(global, 'navigator', {
        value: nodeOpfs.navigator,
        writable: true,
        configurable: true
      });
    }
  } catch (e) {
    console.error('Failed to load node-opfs. This example requires OPFS support.');
    throw e;
  }
}

async function main() {
  await setupOPFS();
  await ready();

  console.log('Document Database Example\n');

  // A database is a directory of files (a catalog plus one file per
  // collection/index); OPFSStorageProvider maps it onto an OPFS directory
  // handle. In a browser this would be navigator.storage.getDirectory()
  // (or a subdirectory of it) directly, with no node-opfs setup needed.
  const rootDirHandle = await navigator.storage.getDirectory();
  const dbDirHandle = await rootDirHandle.getDirectoryHandle('example-db', { create: true });
  const db = await connect(new OPFSStorageProvider(dbDirHandle));

  try {
    const users = await db.collection('users');

    console.log('=== insertOne ===');
    const inserted = [];
    for (const doc of [
      { name: 'Ada', team: 'core', age: 36, tags: ['admin', 'core'] },
      { name: 'Grace', team: 'core', age: 85, tags: ['core'] },
      { name: 'Linus', team: 'kernel', age: 54, tags: ['kernel'] },
      { name: 'Margaret', team: 'kernel', age: 45 } // no tags field
    ]) {
      const { insertedId } = await users.insertOne(doc);
      inserted.push(insertedId);
      console.log(`Inserted ${doc.name} as ${insertedId}`);
    }
    console.log();

    console.log('=== findOne ===');
    console.log(await users.findOne({ name: 'Ada' }));
    console.log();

    console.log('=== find with a query operator ($gt) ===');
    for (const doc of await users.find({ age: { $gt: 50 } }).toArray()) {
      console.log(`${doc.name} (${doc.age})`);
    }
    console.log();

    console.log('=== find with $and / $or ===');
    const coreOver40 = await users.find({ $and: [{ team: 'core' }, { age: { $gt: 40 } }] }).toArray();
    console.log('core team, over 40:', coreOver40.map(d => d.name));
    const coreOrYoung = await users.find({ $or: [{ team: 'core' }, { age: { $lt: 40 } }] }).toArray();
    console.log('core team, or under 40:', coreOrYoung.map(d => d.name));
    console.log();

    console.log('=== find with sort / skip / limit ===');
    // Both the options-object form and the driver's chainable form work.
    const oldestTwo = await users.find({}, { sort: { age: -1 }, limit: 2 }).toArray();
    console.log('oldest two:', oldestTwo.map(d => `${d.name} (${d.age})`));
    const page2 = await users.find({}).sort({ age: 1 }).skip(1).limit(2).toArray();
    console.log('sorted by age, skip 1, limit 2:', page2.map(d => d.name));
    console.log();

    console.log('=== find with a projection ===');
    const namesOnly = await users.find({ team: 'kernel' }).project({ name: 1, _id: 0 }).toArray();
    console.log(namesOnly);
    console.log();

    console.log('=== array field matching ($exists, element match) ===');
    const withTags = await users.find({ tags: { $exists: true } }).toArray();
    console.log('has tags:', withTags.map(d => d.name));
    const isAdmin = await users.find({ tags: 'admin' }).toArray();
    console.log('tagged admin:', isAdmin.map(d => d.name));
    console.log();

    console.log('=== countDocuments ===');
    console.log('kernel team:', await users.countDocuments({ team: 'kernel' }));
    console.log();

    console.log('=== secondary indexes ===');
    await users.createIndex({ team: 1 });
    console.log('indexes:', await users.listIndexes());
    // Equality lookups via findByIndex are served by an O(log n + k) index
    // scan instead of a full collection scan; find()/findOne() will use the
    // same index automatically for filters that pin it via equality (see
    // docs/db-plan.md's equality-index planner).
    console.log('core team via findByIndex:', (await users.findByIndex('team_1', ['core'])).map(d => d.name));
    await users.dropIndex('team_1');
    console.log();

    console.log('=== text index ($text) ===');
    const posts = await db.collection('posts');
    await posts.createIndex({ body: 'text' }); // at most one text index per collection
    await posts.insertOne({ title: 'Fox story', body: 'a quick fox runs through the forest' });
    await posts.insertOne({ title: 'Cat nap', body: 'a lazy cat sleeps all day' });
    const foxPosts = await posts.find({ $text: { $search: 'fox' } }).toArray();
    console.log('matching "fox":', foxPosts.map(d => d.title));
    console.log();

    console.log('=== geo index ($near / $geoWithin) ===');
    const places = await db.collection('places');
    await places.createIndex({ location: '2dsphere' }); // GeoJSON Point values only
    const point = (lng, lat) => ({ type: 'Point', coordinates: [lng, lat] });
    await places.insertOne({ name: 'London', location: point(-0.12, 51.5) });
    await places.insertOne({ name: 'Paris', location: point(2.35, 48.85) });
    await places.insertOne({ name: 'Tokyo', location: point(139.69, 35.68) });
    // $near/$geoWithin distances are in kilometers here (see docs/db-plan.md
    // milestone 6 for why that deviates from real MongoDB's meters/radians).
    const nearLondon = await places.find({
      location: { $near: { $geometry: point(-0.12, 51.5), $maxDistance: 1000 } }
    }).toArray();
    console.log('within 1000km of London, nearest first:', nearLondon.map(d => d.name));
    const europe = await places.find({
      location: { $geoWithin: { $box: [[-10, 40], [10, 60]] } }
    }).toArray();
    console.log('in a European bounding box:', europe.map(d => d.name));
    console.log();

    console.log('=== replaceOne (with upsert) ===');
    await users.replaceOne({ name: 'Ada' }, { name: 'Ada', team: 'core', age: 37 });
    console.log('after replace:', await users.findOne({ name: 'Ada' }));
    const { upsertedId } = await users.replaceOne(
      { name: 'Katherine' },
      { name: 'Katherine', team: 'core', age: 28 },
      { upsert: true }
    );
    console.log('upserted:', upsertedId);
    console.log();

    console.log('=== updateOne / updateMany (update operators) ===');
    // $set/$unset/$inc/$push/$pull -- a plain replacement document is
    // rejected here (that's replaceOne's job).
    await users.updateOne({ name: 'Ada' }, { $inc: { age: 1 }, $push: { tags: 'reviewed' } });
    console.log('after updateOne:', await users.findOne({ name: 'Ada' }));
    const many = await users.updateMany({ team: 'core' }, { $set: { onCall: true } });
    console.log(`updateMany matched/modified ${many.matchedCount}/${many.modifiedCount}`);
    // upsert seeds the new document from the filter's bare equality
    // fields, not just from the update operators.
    const upsertedMany = await users.updateMany(
      { name: 'Rear Admiral', team: 'core' },
      { $set: { age: 60 } },
      { upsert: true }
    );
    console.log('updateMany upserted:', await users.findOne({ _id: upsertedMany.upsertedId }));
    console.log();

    console.log('=== deleteOne ===');
    const { deletedCount } = await users.deleteOne({ name: 'Linus' });
    console.log(`Deleted ${deletedCount} document(s)`);
    console.log('remaining:', (await users.find({}).toArray()).map(d => d.name));
    console.log();

    console.log('=== compact ===');
    // Append-only files grow with write traffic, not live data; compact()
    // rewrites the collection's whole file set without its history and
    // atomically swaps it in (docs/compaction.md).
    const { generation, bytesBefore, bytesAfter, bytesFreed } = await users.compact();
    console.log(`compacted to generation ${generation}: ${bytesBefore} -> ${bytesAfter} bytes (${bytesFreed} freed)`);
    console.log('data intact:', (await users.find({}).toArray()).map(d => d.name));
    console.log();

    console.log('=== listCollections / dropCollection ===');
    console.log('collections:', await db.listCollections());
    await db.dropCollection('users');
    console.log('collections after drop:', await db.listCollections());
  } finally {
    await db.close();
    // Clean up the example's files (leaves ~/.node-opfs the way we found it).
    await rootDirHandle.removeEntry('example-db', { recursive: true });
    console.log('\nDatabase closed and example files removed');
  }
}

main().catch(console.error);
