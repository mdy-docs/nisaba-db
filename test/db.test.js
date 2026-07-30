/**
 * Milestone 1 of the document-database plan: catalog + collection
 * primitives (insertOne/findOne/find/deleteOne/replaceOne/countDocuments)
 * on top of the persistent B+ tree, no secondary indexes yet.
 */
import { describe, it, expect, vi } from 'vitest';
import { ready } from '../wasm/nisaba-wasm.js';
import { ObjectId } from '../wasm/nisaba-wasm.js';
import { connect, MemoryStorageProvider, OPFSStorageProvider } from '../src/db.js';
import { bootstrapOPFS } from './binjson.suite.js';

await ready();

describe('db: catalog + collection primitives', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  it('creates a collection on first access and lists it in the catalog', async () => {
    const db = await openDb();
    expect(await db.listCollections()).toEqual([]);
    const users = await db.collection('users');
    expect(users.name).toBe('users');
    expect(await db.listCollections()).toEqual(['users']);
    // Same name returns the same cached collection instance.
    expect(await db.collection('users')).toBe(users);
    await db.close();
  });

  it('insertOne assigns an ObjectId _id when none is given', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { acknowledged, insertedId } = await users.insertOne({ name: 'Ada' });
    expect(acknowledged).toBe(true);
    expect(insertedId).toBeInstanceOf(ObjectId);

    const doc = await users.findOne({ _id: insertedId });
    expect(doc.name).toBe('Ada');
    expect(doc._id.equals(insertedId)).toBe(true);
    await db.close();
  });

  it('insertOne accepts a caller-supplied _id (ObjectId or hex string)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const id = new ObjectId();
    await users.insertOne({ _id: id, name: 'Grace' });
    expect((await users.findOne({ _id: id })).name).toBe('Grace');
    await db.close();
  });

  it('findOne by _id requires an ObjectId, not a raw hex string (matches real MongoDB)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const id = new ObjectId();
    await users.insertOne({ _id: id, name: 'Grace' });
    // A plain string _id does not auto-coerce to ObjectId in a filter, same
    // as the real driver: {_id: "<hex>"} and {_id: ObjectId("<hex>")} are
    // different BSON types and do not match each other.
    expect(await users.findOne({ _id: id.toHexString() })).toBeNull();
    await db.close();
  });

  it('insertOne accepts a duck-typed ObjectId-shaped value (not instanceof, e.g. from a different binjson copy) for _id and other fields', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const real = new ObjectId();
    // Simulates a value built against a *different* copy of binjson's
    // ObjectId class (e.g. a thin client package depending only on
    // binjson, not this whole engine) -- same shape, not instanceof.
    const foreign = { toHexString: () => real.toHexString(), toBytes: () => real.toBytes() };
    expect(foreign).not.toBeInstanceOf(ObjectId);

    await users.insertOne({ _id: foreign, name: 'Ada', authorId: foreign });
    const doc = await users.findOne({ _id: real });
    expect(doc.name).toBe('Ada');
    expect(doc._id.equals(real)).toBe(true);
    expect(doc.authorId).toBeInstanceOf(ObjectId);
    expect(doc.authorId.equals(real)).toBe(true);
    await db.close();
  });

  it('insertOne rejects a duplicate _id', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const id = new ObjectId();
    await users.insertOne({ _id: id, name: 'Ada' });
    await expect(users.insertOne({ _id: id, name: 'Impostor' })).rejects.toThrow(/Duplicate _id/);
    await db.close();
  });

  it('findOne returns null when nothing matches', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    expect(await users.findOne({ _id: new ObjectId() })).toBeNull();
    expect(await users.findOne({ name: 'Nobody' })).toBeNull();
    await db.close();
  });

  it('findOne and find scan by non-_id field equality', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });

    expect((await users.findOne({ name: 'Grace' })).team).toBe('core');

    const core = await users.find({ team: 'core' }).toArray();
    expect(core.map(d => d.name).sort()).toEqual(['Ada', 'Grace']);

    const all = await users.find({}).toArray();
    expect(all.length).toBe(3);

    const names = [];
    for await (const doc of users.find({ team: 'kernel' })) names.push(doc.name);
    expect(names).toEqual(['Linus']);
    await db.close();
  });

  it('findOne projection includes or excludes fields, with _id defaulting to included', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core', age: 36 });

    const included = await users.findOne({ _id: insertedId }, { projection: { name: 1 } });
    expect(included).toEqual({ _id: insertedId, name: 'Ada' });

    const excluded = await users.findOne({ _id: insertedId }, { projection: { age: 0 } });
    expect(excluded).toEqual({ _id: insertedId, name: 'Ada', team: 'core' });

    const noId = await users.findOne({ _id: insertedId }, { projection: { name: 1, _id: 0 } });
    expect(noId).toEqual({ name: 'Ada' });

    // No projection given -- unchanged, full document (existing behavior).
    expect(await users.findOne({ _id: insertedId })).toEqual({ _id: insertedId, name: 'Ada', team: 'core', age: 36 });

    // A non-matching filter still returns null, projection or not.
    expect(await users.findOne({ name: 'Nobody' }, { projection: { name: 1 } })).toBeNull();
    await db.close();
  });

  it('findOne projection applies on the $near/equality-index/full-scan paths alike', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await users.insertOne({ name: 'Ada', team: 'core', age: 36 });
    await users.insertOne({ name: 'Grace', team: 'kernel', age: 40 });

    // Equality-index-planned path (team is indexed).
    const viaIndex = await users.findOne({ team: 'core' }, { projection: { name: 1 } });
    expect(viaIndex).toEqual({ _id: viaIndex._id, name: 'Ada' });

    // Full-scan path (age is not indexed).
    const viaScan = await users.findOne({ age: 40 }, { projection: { name: 1 } });
    expect(viaScan).toEqual({ _id: viaScan._id, name: 'Grace' });
    await db.close();
  });

  it('matches nested-document and array fields by exact value equality', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', address: { city: 'London', zip: 'W1' }, tags: ['core', 'admin'] });
    await users.insertOne({ name: 'Grace', address: { city: 'Arlington', zip: '22201' }, tags: ['core'] });

    expect((await users.findOne({ address: { city: 'London', zip: 'W1' } })).name).toBe('Ada');
    expect((await users.findOne({ tags: ['core', 'admin'] })).name).toBe('Ada');
    // Real MongoDB embedded-document equality is field-order sensitive.
    expect(await users.findOne({ address: { zip: 'W1', city: 'London' } })).toBeNull();
    // A filter array is matched as a whole value, not as "contains".
    expect(await users.findOne({ tags: ['core'] })).not.toBeNull();
    expect((await users.findOne({ tags: ['core'] })).name).toBe('Grace');
    await db.close();
  });

  it('deleteOne removes by _id and reports deletedCount', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada' });

    expect(await users.deleteOne({ _id: new ObjectId() })).toEqual({ acknowledged: true, deletedCount: 0 });
    expect(await users.deleteOne({ _id: insertedId })).toEqual({ acknowledged: true, deletedCount: 1 });
    expect(await users.findOne({ _id: insertedId })).toBeNull();
    await db.close();
  });

  it('deleteOne removes the first match for a non-_id filter', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });

    const { deletedCount } = await users.deleteOne({ team: 'core' });
    expect(deletedCount).toBe(1);
    expect(await users.countDocuments({ team: 'core' })).toBe(1);
    await db.close();
  });

  it('replaceOne replaces document content but preserves _id', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

    const result = await users.replaceOne({ _id: insertedId }, { name: 'Ada Lovelace' });
    expect(result).toEqual({ acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedId: null });

    const doc = await users.findOne({ _id: insertedId });
    expect(doc).toEqual({ _id: insertedId, name: 'Ada Lovelace' });
    await db.close();
  });

  it('replaceOne with no match and upsert:false is a no-op', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const result = await users.replaceOne({ name: 'Ghost' }, { name: 'Still a ghost' });
    expect(result).toEqual({ acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null });
    expect(await users.countDocuments()).toBe(0);
    await db.close();
  });

  it('replaceOne with upsert:true inserts a new document', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const result = await users.replaceOne({ name: 'Ghost' }, { name: 'Materialized' }, { upsert: true });
    expect(result.matchedCount).toBe(0);
    expect(result.upsertedId).toBeInstanceOf(ObjectId);

    const doc = await users.findOne({ _id: result.upsertedId });
    expect(doc.name).toBe('Materialized');
    await db.close();
  });

  it('replaceOne rejects changing _id on an existing document', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada' });
    await expect(
      users.replaceOne({ _id: insertedId }, { _id: new ObjectId(), name: 'Someone else' })
    ).rejects.toThrow(/cannot change the _id/);
    await db.close();
  });

  it('countDocuments counts the whole collection or a filtered subset', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });

    expect(await users.countDocuments()).toBe(3);
    expect(await users.countDocuments({ team: 'core' })).toBe(2);
    expect(await users.countDocuments({ team: 'nope' })).toBe(0);
    await db.close();
  });

  it('keeps collections independent of one another', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const posts = await db.collection('posts');
    await users.insertOne({ name: 'Ada' });
    await posts.insertOne({ title: 'Hello world' });

    expect(await users.countDocuments()).toBe(1);
    expect(await posts.countDocuments()).toBe(1);
    expect(await db.listCollections()).toEqual(['posts', 'users']);
    await db.close();
  });

  it('dropCollection removes the collection; recreating it starts empty', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada' });

    expect(await db.dropCollection('users')).toBe(true);
    expect(await db.dropCollection('users')).toBe(false);
    expect(await db.listCollections()).toEqual([]);

    const fresh = await db.collection('users');
    expect(await fresh.countDocuments()).toBe(0);
    await db.close();
  });

  it('rejects invalid collection names', async () => {
    const db = await openDb();
    await expect(db.collection('a/b')).rejects.toThrow(/Invalid collection name/);
    await expect(db.collection('')).rejects.toThrow(/Invalid collection name/);
    await db.close();
  });

  it('persists data across close/reopen on the same provider', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const id = new ObjectId();
    await (await db1.collection('users')).insertOne({ _id: id, name: 'Ada' });
    await db1.close();

    const db2 = await connect(provider);
    const users = await db2.collection('users');
    expect((await users.findOne({ _id: id })).name).toBe('Ada');
    await db2.close();
  });
});

describe('db: secondary indexes (milestone 2)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  it('createIndex on an empty collection and listIndexes reflects it', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const name = await users.createIndex({ team: 1 });
    expect(name).toBe('team_1');
    expect(await users.listIndexes()).toEqual([{ name: 'team_1', key: { team: 1 } }]);
    await db.close();
  });

  it('createIndex backfills existing documents', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });

    await users.createIndex({ team: 1 });
    const core = await users.findByIndex('team_1', ['core']);
    expect(core.map(d => d.name).sort()).toEqual(['Ada', 'Grace']);
    const kernel = await users.findByIndex('team_1', ['kernel']);
    expect(kernel.map(d => d.name)).toEqual(['Linus']);
    const none = await users.findByIndex('team_1', ['nope']);
    expect(none).toEqual([]);
    await db.close();
  });

  it('insertOne maintains an existing index', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });

    expect((await users.findByIndex('team_1', ['core'])).map(d => d.name)).toEqual(['Ada']);
    expect((await users.findByIndex('team_1', ['kernel'])).map(d => d.name)).toEqual(['Linus']);
    await db.close();
  });

  it('deleteOne removes the document from every index', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

    await users.deleteOne({ _id: insertedId });
    expect(await users.findByIndex('team_1', ['core'])).toEqual([]);
    await db.close();
  });

  it('replaceOne re-indexes when the indexed field changes', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

    await users.replaceOne({ _id: insertedId }, { name: 'Ada', team: 'kernel' });
    expect(await users.findByIndex('team_1', ['core'])).toEqual([]);
    expect((await users.findByIndex('team_1', ['kernel'])).map(d => d.name)).toEqual(['Ada']);
    await db.close();
  });

  it('replaceOne upsert builds the index entry for the new document', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await users.replaceOne({ name: 'Ghost' }, { name: 'Ghost', team: 'core' }, { upsert: true });

    expect((await users.findByIndex('team_1', ['core'])).map(d => d.name)).toEqual(['Ghost']);
    await db.close();
  });

  it('supports a compound (multi-field) index', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1, level: 1 });
    await users.insertOne({ name: 'Ada', team: 'core', level: 1 });
    await users.insertOne({ name: 'Grace', team: 'core', level: 2 });
    await users.insertOne({ name: 'Linus', team: 'kernel', level: 1 });

    expect((await users.findByIndex('team_1_level_1', ['core', 1])).map(d => d.name)).toEqual(['Ada']);
    expect((await users.findByIndex('team_1_level_1', ['core', 2])).map(d => d.name)).toEqual(['Grace']);
    expect(await users.findByIndex('team_1_level_1', ['kernel', 2])).toEqual([]);
    await db.close();
  });

  it('createIndex fails all-or-nothing when a document lacks the field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'NoTeam' }); // missing `team`

    await expect(users.createIndex({ team: 1 })).rejects.toThrow(/missing a field required by a non-sparse index/);
    expect(await users.listIndexes()).toEqual([]);
    // The collection itself must still be fully usable after the failed attempt.
    expect(await users.countDocuments()).toBe(2);
    await db.close();
  });

  it('reports a write missing a non-sparse indexed field as exactly that (DC_ERR_MISSING_INDEXED_FIELD)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await users.insertOne({ name: 'Ada', team: 'core' });

    // insert, and an update that removes the field, both trip the same
    // index-maintenance check -- and must say so, not "builder state error".
    await expect(users.insertOne({ name: 'NoTeam' }))
      .rejects.toThrow(/missing a field required by a non-sparse index/);
    await expect(users.updateOne({ name: 'Ada' }, { $unset: { team: '' } }))
      .rejects.toThrow(/missing a field required by a non-sparse index/);

    // Nothing landed: the failed writes rolled back whole.
    expect(await users.countDocuments()).toBe(1);
    expect((await users.findOne({ name: 'Ada' })).team).toBe('core');
    await db.close();
  });

  it('reports an unindexable indexed-field value as exactly that (DC_ERR_UNINDEXABLE_VALUE)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });

    // A boolean has no order-preserving key encoding (db_keyenc.h).
    await expect(users.insertOne({ name: 'Bool', team: true }))
      .rejects.toThrow(/cannot be key-encoded/);
    // Same for a lookup value on the read side.
    await expect(users.findByIndex('team_1', [true]))
      .rejects.toThrow(/cannot be key-encoded/);
    expect(await users.countDocuments()).toBe(0);
    await db.close();
  });

  it('a failed write rolls back in-process, not just on reopen', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ email: 1 }, { unique: true });
    await users.insertOne({ name: 'Ada', email: 'ada@example.com' });
    await users.insertOne({ name: 'Grace', email: 'grace@example.com' });

    // A unique-key rejection is discovered only after the matched
    // document's old index entries were cleared (see dc_replace_one's
    // check placement); the rewind must restore them in the live session.
    await expect(users.updateOne({ name: 'Grace' }, { $set: { email: 'ada@example.com' } }))
      .rejects.toThrow(/unique index/);
    expect((await users.findByIndex('email_1', ['grace@example.com'])).map(d => d.name)).toEqual(['Grace']);
    expect((await users.findOne({ name: 'Grace' })).email).toBe('grace@example.com');

    // Same for replaceOne, and for updateMany stopping mid-run: documents
    // it already committed stay, the failing one rolls back whole.
    await expect(users.replaceOne({ name: 'Grace' }, { name: 'Grace', email: 'ada@example.com' }))
      .rejects.toThrow(/unique index/);
    expect((await users.findByIndex('email_1', ['grace@example.com'])).map(d => d.name)).toEqual(['Grace']);

    expect(await users.countDocuments()).toBe(2);
    await db.close();
  });

  it('dropIndex removes it; findByIndex then reports it missing', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await users.insertOne({ name: 'Ada', team: 'core' });

    await users.dropIndex('team_1');
    expect(await users.listIndexes()).toEqual([]);
    await expect(users.findByIndex('team_1', ['core'])).rejects.toThrow(/Index not found/);
    // Dropping the index must not touch the documents.
    expect(await users.countDocuments()).toBe(1);
    await db.close();
  });

  it('rejects a duplicate index name', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await expect(users.createIndex({ team: 1 })).rejects.toThrow(/already exists/);
    await db.close();
  });

  it('rejects descending index fields as not yet supported (unique: see milestone 9)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await expect(users.createIndex({ team: -1 })).rejects.toThrow(/ascending/);
    await db.close();
  });

  it('indexes persist and stay maintained across close/reopen', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users1 = await db1.collection('users');
    await users1.createIndex({ team: 1 });
    await users1.insertOne({ name: 'Ada', team: 'core' });
    await db1.close();

    const db2 = await connect(provider);
    const users2 = await db2.collection('users');
    expect(await users2.listIndexes()).toEqual([{ name: 'team_1', key: { team: 1 } }]);
    expect((await users2.findByIndex('team_1', ['core'])).map(d => d.name)).toEqual(['Ada']);

    // Reopening must not re-run the backfill: insert a second core-team doc
    // and confirm the index sees exactly the two real documents, not
    // duplicated entries from a redundant backfill.
    await users2.insertOne({ name: 'Grace', team: 'core' });
    expect((await users2.findByIndex('team_1', ['core'])).map(d => d.name).sort()).toEqual(['Ada', 'Grace']);
    await db2.close();
  });
});

describe('db: query engine (milestone 3)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  async function seedPeople(users) {
    await users.insertOne({ name: 'Ada', team: 'core', age: 36, tags: ['admin', 'core'] });
    await users.insertOne({ name: 'Grace', team: 'core', age: 85, tags: ['core'] });
    await users.insertOne({ name: 'Linus', team: 'kernel', age: 54, tags: ['kernel', 'admin'] });
    await users.insertOne({ name: 'Margaret', team: 'kernel', age: 45 }); // no tags field
  }

  it('comparison operators: $gt/$gte/$lt/$lte/$ne', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.find({ age: { $gt: 50 } }).toArray()).map(d => d.name).sort())
      .toEqual(['Grace', 'Linus']);
    expect((await users.find({ age: { $gte: 54 } }).toArray()).map(d => d.name).sort())
      .toEqual(['Grace', 'Linus']);
    expect((await users.find({ age: { $lt: 45 } }).toArray()).map(d => d.name)).toEqual(['Ada']);
    expect((await users.find({ age: { $lte: 45 } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Margaret']);
    expect((await users.find({ team: { $ne: 'core' } }).toArray()).map(d => d.name).sort())
      .toEqual(['Linus', 'Margaret']);
    // Multiple operators on one field are ANDed.
    expect((await users.find({ age: { $gte: 40, $lt: 60 } }).toArray()).map(d => d.name).sort())
      .toEqual(['Linus', 'Margaret']);
    await db.close();
  });

  it('comparison operators order Date values (milestone 9 TTL prerequisite)', async () => {
    const db = await openDb();
    const events = await db.collection('events');
    const t0 = new Date('2020-01-01T00:00:00.000Z');
    const t1 = new Date('2021-01-01T00:00:00.000Z');
    const t2 = new Date('2022-01-01T00:00:00.000Z');
    await events.insertMany([{ at: t0, tag: 'a' }, { at: t1, tag: 'b' }, { at: t2, tag: 'c' }]);

    expect((await events.find({ at: { $lt: t1 } }).toArray()).map(d => d.tag)).toEqual(['a']);
    expect((await events.find({ at: { $gte: t1 } }).toArray()).map(d => d.tag).sort()).toEqual(['b', 'c']);
    expect((await events.find({ at: { $gt: t0, $lt: t2 } }).toArray()).map(d => d.tag)).toEqual(['b']);
    await db.close();
  });

  it('$in / $nin', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.find({ team: { $in: ['core'] } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Grace']);
    expect((await users.find({ team: { $nin: ['core'] } }).toArray()).map(d => d.name).sort())
      .toEqual(['Linus', 'Margaret']);
    await db.close();
  });

  it('$exists', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.find({ tags: { $exists: true } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Grace', 'Linus']);
    expect((await users.find({ tags: { $exists: false } }).toArray()).map(d => d.name))
      .toEqual(['Margaret']);
    await db.close();
  });

  it('array fields match by element or by whole-array value', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    // Element match.
    expect((await users.find({ tags: 'admin' }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Linus']);
    expect((await users.find({ tags: { $in: ['kernel'] } }).toArray()).map(d => d.name))
      .toEqual(['Linus']);
    // Whole-array equality still works.
    expect((await users.find({ tags: ['core'] }).toArray()).map(d => d.name)).toEqual(['Grace']);
    await db.close();
  });

  it('$and / $or / $nor', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.find({ $and: [{ team: 'core' }, { age: { $gt: 50 } }] }).toArray()).map(d => d.name))
      .toEqual(['Grace']);
    expect((await users.find({ $or: [{ team: 'kernel' }, { age: { $lt: 40 } }] }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Linus', 'Margaret']);
    expect((await users.find({ $nor: [{ team: 'kernel' }, { age: { $lt: 40 } }] }).toArray()).map(d => d.name))
      .toEqual(['Grace']);
    await db.close();
  });

  it('$not negates an operator expression', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.find({ age: { $not: { $gt: 50 } } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Margaret']);
    await db.close();
  });

  it('dot-notation resolves nested fields', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', address: { city: 'London', zip: 'W1' } });
    await users.insertOne({ name: 'Grace', address: { city: 'Arlington' } });

    expect((await users.find({ 'address.city': 'London' }).toArray()).map(d => d.name)).toEqual(['Ada']);
    expect(await users.find({ 'address.zip': { $exists: true } }).toArray()).toHaveLength(1);
    await db.close();
  });

  it('rejects an unrecognized query operator instead of silently matching everything', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);
    await expect(users.find({ name: { $foo: '^A' } }).toArray()).rejects.toThrow();
    await db.close();
  });

  it('sort ascending and descending, including a compound sort', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.find({}, { sort: { age: 1 } }).toArray()).map(d => d.name))
      .toEqual(['Ada', 'Margaret', 'Linus', 'Grace']);
    expect((await users.find({}).sort({ age: -1 }).toArray()).map(d => d.name))
      .toEqual(['Grace', 'Linus', 'Margaret', 'Ada']);
    expect((await users.find({}).sort({ team: 1, age: 1 }).toArray()).map(d => d.name))
      .toEqual(['Ada', 'Grace', 'Margaret', 'Linus']);
    await db.close();
  });

  it('skip and limit apply after sort', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    const page = await users.find({}).sort({ age: 1 }).skip(1).limit(2).toArray();
    expect(page.map(d => d.name)).toEqual(['Margaret', 'Linus']);
    await db.close();
  });

  it('projection includes or excludes fields, with _id defaulting to included', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core', age: 36 });

    const included = await users.find({ _id: insertedId }).project({ name: 1 }).toArray();
    expect(included).toEqual([{ _id: insertedId, name: 'Ada' }]);

    const excluded = await users.find({ _id: insertedId }).project({ age: 0 }).toArray();
    expect(excluded).toEqual([{ _id: insertedId, name: 'Ada', team: 'core' }]);

    const noId = await users.find({ _id: insertedId }).project({ name: 1, _id: 0 }).toArray();
    expect(noId).toEqual([{ name: 'Ada' }]);
    await db.close();
  });

  it('findOne/deleteOne/replaceOne/countDocuments also use the operator-aware matcher', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedPeople(users);

    expect((await users.findOne({ age: { $gt: 80 } })).name).toBe('Grace');
    expect(await users.countDocuments({ age: { $gte: 45 } })).toBe(3);
    expect((await users.deleteOne({ age: { $gt: 80 } })).deletedCount).toBe(1);
    expect(await users.countDocuments()).toBe(3);
    const r = await users.replaceOne({ age: { $lt: 40 } }, { name: 'Ada', team: 'core', age: 37 });
    expect(r.matchedCount).toBe(1);
    expect((await users.findOne({ name: 'Ada' })).age).toBe(37);
    await db.close();
  });

  it('an equality-index plan and a full scan agree on results (with sort/skip/limit/projection)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await seedPeople(users);
    // A fifth document sharing team 'core' so the plan has more than one
    // candidate to sift through with the (non-indexed) age filter below.
    await users.insertOne({ name: 'Katherine', team: 'core', age: 28, tags: ['core'] });

    // Planned: filter pins the whole index (team) via bare equality.
    const planned = await users.find({ team: 'core' }).sort({ age: 1 }).toArray();
    expect(planned.map(d => d.name)).toEqual(['Katherine', 'Ada', 'Grace']);

    // Planned index lookup, but the filter also carries a non-indexed
    // condition -- must still be honored (full filter re-applied).
    const plannedPlusExtra = await users.find({ team: 'core', age: { $gt: 30 } }).toArray();
    expect(plannedPlusExtra.map(d => d.name).sort()).toEqual(['Ada', 'Grace']);

    // Not planned (range condition, not equality) -- must still be correct.
    const scanned = await users.find({ team: 'core', age: { $gte: 0 } }).sort({ age: 1 }).toArray();
    expect(scanned.map(d => d.name)).toEqual(['Katherine', 'Ada', 'Grace']);

    // Not planned ($or at the top level) -- must still be correct.
    const orred = await users.find({ $or: [{ team: 'core' }, { name: 'Linus' }] }).toArray();
    expect(orred.map(d => d.name).sort()).toEqual(['Ada', 'Grace', 'Katherine', 'Linus']);

    await db.close();
  });
});

describe('db: query operator completeness (milestone 11)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  async function seedScores(users) {
    await users.insertMany([
      { name: 'Ada', tags: ['admin', 'core'], scores: [80, 90, 60], age: 36 },
      { name: 'Grace', tags: ['core'], scores: [70, 95], age: 85 },
      { name: 'Linus', tags: [], scores: [], age: 54 },
      { name: 'Margaret', scores: [50], age: 45 } // no tags field
    ]);
  }

  it('$size matches an array field with exactly n elements', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedScores(users);
    expect((await users.find({ tags: { $size: 2 } }).toArray()).map(d => d.name)).toEqual(['Ada']);
    expect((await users.find({ tags: { $size: 0 } }).toArray()).map(d => d.name)).toEqual(['Linus']);
    // A non-array field (or a missing one) never matches, regardless of n.
    expect(await users.find({ age: { $size: 1 } }).toArray()).toHaveLength(0);
    await db.close();
  });

  it('$all requires every listed value present; an empty $all never matches', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedScores(users);
    expect((await users.find({ tags: { $all: ['admin', 'core'] } }).toArray()).map(d => d.name)).toEqual(['Ada']);
    expect((await users.find({ tags: { $all: ['admin', 'nope'] } }).toArray())).toHaveLength(0);
    expect(await users.find({ tags: { $all: [] } }).toArray()).toHaveLength(0);
    await db.close();
  });

  it('$type matches by MongoDB-style string alias, including array-field elements', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedScores(users);
    expect((await users.find({ name: { $type: 'string' } }).toArray())).toHaveLength(4);
    expect((await users.find({ age: { $type: 'number' } }).toArray())).toHaveLength(4);
    expect((await users.find({ tags: { $type: 'array' } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Grace', 'Linus']);
    // Array-field elements are checked too, same any-element mechanism $eq/$gt
    // use -- Linus's scores is an empty array, so it has no int elements.
    expect((await users.find({ scores: { $type: 'int' } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Grace', 'Margaret']);
    await expect(users.find({ name: { $type: 'bogus' } }).toArray()).rejects.toThrow();
    await db.close();
  });

  it('$mod matches numeric candidates by divisor/remainder; non-numeric fields are skipped', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedScores(users);
    // Ada (36) and Linus (54) are even; Grace (85) and Margaret (45) are odd.
    expect((await users.find({ age: { $mod: [2, 0] } }).toArray()).map(d => d.name).sort())
      .toEqual(['Ada', 'Linus']);
    expect(await users.find({ name: { $mod: [2, 0] } }).toArray()).toHaveLength(0);
    await expect(users.find({ age: { $mod: [0, 0] } }).toArray()).rejects.toThrow();
    await db.close();
  });

  it('$elemMatch requires one element to satisfy every condition, unlike default any-element matching', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await seedScores(users);
    // Ada's scores [80,90,60]: 90 alone satisfies both $gt/$lt -- $elemMatch matches.
    expect((await users.find({ scores: { $elemMatch: { $gt: 85, $lt: 95 } } }).toArray()).map(d => d.name))
      .toEqual(['Ada']);
    // Contrast: the default (non-elemMatch) shape ANDs two *independent*
    // any-element checks -- Grace's scores [70,95] matches because 95
    // alone satisfies $gt:90 and 70 alone satisfies $lt:80, even though no
    // *single* element satisfies both. $elemMatch above requires the same
    // element to satisfy everything; this proves the default doesn't.
    expect((await users.find({ scores: { $gt: 90, $lt: 80 } }).toArray()).map(d => d.name))
      .toEqual(['Grace']);
    expect((await users.find({ scores: { $elemMatch: { $gt: 90, $lt: 80 } } }).toArray()))
      .toHaveLength(0); // no single element is both >90 and <80 -- correctly never matches

    const db2 = await openDb();
    const people = await db2.collection('people');
    await people.insertMany([
      { name: 'A', ratings: [{ score: 5, verified: true }, { score: 2, verified: false }] },
      { name: 'B', ratings: [{ score: 5, verified: false }, { score: 2, verified: true }] }
    ]);
    // Query-object sub-query: one element must have BOTH score>=5 AND verified.
    expect((await people.find({ ratings: { $elemMatch: { score: { $gte: 5 }, verified: true } } }).toArray()).map(d => d.name))
      .toEqual(['A']);
    await db2.close();
    await db.close();
  });

  describe('$regex', () => {
    async function seedNames(users) {
      await users.insertMany([{ name: 'Ada' }, { name: 'Grace' }, { name: 'ada2' }, { name: 'Bob' }]);
    }

    it('matches a literal substring anywhere, and respects anchors', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await seedNames(users);
      expect((await users.find({ name: { $regex: 'da' } }).toArray()).map(d => d.name).sort())
        .toEqual(['Ada', 'ada2']);
      expect((await users.find({ name: { $regex: '^Ada$' } }).toArray()).map(d => d.name)).toEqual(['Ada']);
      await db.close();
    });

    it('character classes, shorthand classes, alternation, groups, and bounded repetition', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await seedNames(users);
      expect((await users.find({ name: { $regex: '^[AG]' } }).toArray()).map(d => d.name).sort())
        .toEqual(['Ada', 'Grace']);
      expect((await users.find({ name: { $regex: '\\d+$' } }).toArray()).map(d => d.name)).toEqual(['ada2']);
      expect((await users.find({ name: { $regex: '^(Ada|Bob)$' } }).toArray()).map(d => d.name).sort())
        .toEqual(['Ada', 'Bob']);
      expect((await users.find({ name: { $regex: '^[A-Za-z]{3}$' } }).toArray()).map(d => d.name).sort())
        .toEqual(['Ada', 'Bob']);
      await db.close();
    });

    it('$options "i" makes matching case-insensitive; an unsupported flag is rejected', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await seedNames(users);
      expect((await users.find({ name: { $regex: '^ada', $options: 'i' } }).toArray()).map(d => d.name).sort())
        .toEqual(['Ada', 'ada2']);
      await expect(users.find({ name: { $regex: '^ada', $options: 'm' } }).toArray()).rejects.toThrow();
      await db.close();
    });

    it('rejects $options without a sibling $regex', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await seedNames(users);
      await expect(users.find({ name: { $options: 'i' } }).toArray()).rejects.toThrow();
      await db.close();
    });

    it('rejects a malformed pattern instead of silently matching everything', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await seedNames(users);
      await expect(users.find({ name: { $regex: '(unclosed' } }).toArray()).rejects.toThrow();
      await expect(users.find({ name: { $regex: '[unclosed' } }).toArray()).rejects.toThrow();
      await expect(users.find({ name: { $regex: 'a**' } }).toArray()).rejects.toThrow();
      await db.close();
    });

    // third_party/regex-engine (ECMAScript-flavored) replaced the old
    // byte-oriented hand-rolled engine -- these exercise capabilities the
    // old one could never have supported at all, through the real
    // Collection API (WASM + JS bridge + C engine together, not just the
    // C-level integration on its own).
    it('lookahead and lookbehind, unsupported by the old engine, now work', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([{ name: 'price: 42' }, { name: '$42' }, { name: 'no digits here' }]);

      expect((await users.find({ name: { $regex: '(?<!\\$)\\d{2}' } }).toArray()).map((d) => d.name))
        .toEqual(['price: 42']); // the $42 case is correctly excluded by the negative lookbehind

      expect((await users.find({ name: { $regex: '(?=.*\\d)price' } }).toArray()).map((d) => d.name))
        .toEqual(['price: 42']); // lookahead: "price" only when the string also contains a digit somewhere
      await db.close();
    });

    it('named groups compile and match (capture text is irrelevant to $regex\'s boolean result)', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([{ name: '2026-07' }, { name: 'not-a-date' }]);
      expect((await users.find({ name: { $regex: '^(?<year>\\d{4})-(?<month>\\d{2})$' } }).toArray()).map((d) => d.name))
        .toEqual(['2026-07']);
      await db.close();
    });

    it('matches a multi-byte UTF-8 character as one character, not one byte at a time', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([{ name: 'café' }, { name: 'cafe' }]);
      // '.' must consume exactly the one trailing character in each case --
      // the old byte-oriented engine would have needed a different pattern
      // length for the multi-byte 'é' than for the single-byte 'e'.
      expect((await users.find({ name: { $regex: '^caf.$' } }).toArray()).map((d) => d.name).sort())
        .toEqual(['cafe', 'café']);
      await db.close();
    });

    it('matches an astral codepoint (needs a UTF-16 surrogate pair internally)', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([{ name: 'hello 😀 world' }, { name: 'hello world' }]);
      expect((await users.find({ name: { $regex: '😀' } }).toArray()).map((d) => d.name))
        .toEqual(['hello 😀 world']);
      await db.close();
    });

    // A compiled Program is a ~2MB fixed-size struct (regex.c's own doc
    // comment), and $regex is evaluated once per candidate per document --
    // without the compile cache, scanning even a modest collection would
    // mean allocating/compiling megabytes *per document*. This doesn't
    // assert a strict latency budget (avoiding flakiness on a slow CI
    // runner), just that 2000 documents complete in a timeframe only
    // possible if the pattern was compiled once and reused, not
    // recompiled per document.
    it('a $regex scan over 2000 documents completes fast, proving the compile cache is doing its job', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      for (let i = 0; i < 2000; i++) await users.insertOne({ name: `user-${i}` });

      const start = performance.now();
      const matches = await users.find({ name: { $regex: '^user-1\\d{2}$' } }).toArray();
      const elapsedMs = performance.now() - start;

      expect(matches).toHaveLength(100); // user-100..user-199
      expect(elapsedMs).toBeLessThan(2000); // generous; uncached recompilation would take vastly longer than this
      await db.close();
    });
  });
});

describe('db: update operators (milestone 4)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  it('$set updates an existing field and creates a new one', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

    const result = await users.updateOne({ _id: insertedId }, { $set: { team: 'kernel', age: 36 } });
    expect(result).toEqual({ acknowledged: true, matchedCount: 1, modifiedCount: 1, upsertedId: null });
    expect(await users.findOne({ _id: insertedId })).toEqual({ _id: insertedId, name: 'Ada', team: 'kernel', age: 36 });
    await db.close();
  });

  it('$unset removes a field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

    await users.updateOne({ _id: insertedId }, { $unset: { team: '' } });
    expect(await users.findOne({ _id: insertedId })).toEqual({ _id: insertedId, name: 'Ada' });
    // Unsetting a field that isn't there is a harmless no-op.
    await users.updateOne({ _id: insertedId }, { $unset: { nope: '' } });
    expect(await users.findOne({ _id: insertedId })).toEqual({ _id: insertedId, name: 'Ada' });
    await db.close();
  });

  it('$inc increments an existing field and creates a missing one', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', age: 36 });

    await users.updateOne({ _id: insertedId }, { $inc: { age: 1, visits: 1 } });
    expect(await users.findOne({ _id: insertedId })).toEqual({ _id: insertedId, name: 'Ada', age: 37, visits: 1 });
    await users.updateOne({ _id: insertedId }, { $inc: { age: -10.5 } });
    expect((await users.findOne({ _id: insertedId })).age).toBe(26.5);
    await db.close();
  });

  it('$inc rejects a non-numeric existing field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada' });
    await expect(users.updateOne({ _id: insertedId }, { $inc: { name: 1 } })).rejects.toThrow();
    await db.close();
  });

  it('$push appends to an existing array and creates a new one', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', tags: ['core'] });

    await users.updateOne({ _id: insertedId }, { $push: { tags: 'admin', badges: 'first' } });
    expect(await users.findOne({ _id: insertedId })).toEqual({
      _id: insertedId, name: 'Ada', tags: ['core', 'admin'], badges: ['first']
    });
    await db.close();
  });

  it('$push rejects a non-array existing field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', tags: 'not-an-array' });
    await expect(users.updateOne({ _id: insertedId }, { $push: { tags: 'x' } })).rejects.toThrow();
    await db.close();
  });

  it('$pull removes matching elements and is a no-op on a missing field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', tags: ['core', 'admin', 'core'] });

    await users.updateOne({ _id: insertedId }, { $pull: { tags: 'core' } });
    expect((await users.findOne({ _id: insertedId })).tags).toEqual(['admin']);

    await users.updateOne({ _id: insertedId }, { $pull: { missing: 'x' } });
    expect((await users.findOne({ _id: insertedId })).tags).toEqual(['admin']);
    await db.close();
  });

  it('multiple operators apply together in one call', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core', age: 36, tags: ['core'] });

    await users.updateOne(
      { _id: insertedId },
      { $set: { team: 'kernel' }, $inc: { age: 1 }, $push: { tags: 'admin' }, $unset: { name: '' } }
    );
    expect(await users.findOne({ _id: insertedId })).toEqual({
      _id: insertedId, team: 'kernel', age: 37, tags: ['core', 'admin']
    });
    await db.close();
  });

  it('rejects a replacement-shaped update (use replaceOne instead)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada' });
    await expect(users.updateOne({ _id: insertedId }, { name: 'New name' })).rejects.toThrow();
    await db.close();
  });

  it('rejects targeting _id, an unrecognized operator, and a field targeted twice', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', age: 36 });
    await expect(users.updateOne({ _id: insertedId }, { $set: { _id: new ObjectId() } })).rejects.toThrow();
    await expect(users.updateOne({ _id: insertedId }, { $foo: { age: 2 } })).rejects.toThrow();
    await expect(
      users.updateOne({ _id: insertedId }, { $set: { age: 1 }, $inc: { age: 1 } })
    ).rejects.toThrow();
    await db.close();
  });

  it('updateOne with no match and no upsert is a no-op', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const result = await users.updateOne({ name: 'Ghost' }, { $set: { seen: true } });
    expect(result).toEqual({ acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null });
    expect(await users.countDocuments()).toBe(0);
    await db.close();
  });

  it('updateOne upsert seeds the new document from the filter\'s bare equality fields', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const result = await users.updateOne(
      { name: 'Ghost', team: 'core' },
      { $set: { age: 100 } },
      { upsert: true }
    );
    expect(result.upsertedId).toBeInstanceOf(ObjectId);
    expect(await users.findOne({ _id: result.upsertedId })).toEqual({
      _id: result.upsertedId, name: 'Ghost', team: 'core', age: 100
    });
    // A filter field wrapped in an operator expression is not a literal
    // equality condition, so it's not part of the upsert seed.
    const result2 = await users.updateOne(
      { name: 'Ghost2', age: { $gt: 5 } },
      { $set: { team: 'kernel' } },
      { upsert: true }
    );
    expect(await users.findOne({ _id: result2.upsertedId })).toEqual({
      _id: result2.upsertedId, name: 'Ghost2', team: 'kernel'
    });
    await db.close();
  });

  it('updateMany applies to every matching document', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });

    const result = await users.updateMany({ team: 'core' }, { $set: { onCall: true } });
    expect(result).toEqual({ acknowledged: true, matchedCount: 2, modifiedCount: 2, upsertedId: null });
    expect((await users.find({ onCall: true }).toArray()).map(d => d.name).sort()).toEqual(['Ada', 'Grace']);
    expect(await users.findOne({ name: 'Linus' })).not.toHaveProperty('onCall');
    await db.close();
  });

  it('updateMany with no match and upsert:true inserts exactly one document', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const result = await users.updateMany({ team: 'ghosts' }, { $set: { seen: true } }, { upsert: true });
    expect(result.matchedCount).toBe(0);
    expect(result.upsertedId).toBeInstanceOf(ObjectId);
    expect(await users.countDocuments()).toBe(1);
    expect(await users.findOne({ _id: result.upsertedId })).toEqual({
      _id: result.upsertedId, team: 'ghosts', seen: true
    });
    await db.close();
  });

  it('updateOne keeps an attached index in sync when the indexed field changes', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

    await users.updateOne({ _id: insertedId }, { $set: { team: 'kernel' } });
    expect(await users.findByIndex('team_1', ['core'])).toEqual([]);
    expect((await users.findByIndex('team_1', ['kernel'])).map(d => d.name)).toEqual(['Ada']);
    await db.close();
  });
});

describe('db: update operator completeness (milestone 10)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  it('$addToSet appends only if absent, including via $each', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', tags: ['core'] });

    await users.updateOne({ _id: insertedId }, { $addToSet: { tags: 'core' } });
    expect((await users.findOne({ _id: insertedId })).tags).toEqual(['core']);

    await users.updateOne({ _id: insertedId }, { $addToSet: { tags: 'admin' } });
    expect((await users.findOne({ _id: insertedId })).tags).toEqual(['core', 'admin']);

    await users.updateOne({ _id: insertedId }, { $addToSet: { tags: { $each: ['admin', 'root', 'root'] } } });
    expect((await users.findOne({ _id: insertedId })).tags).toEqual(['core', 'admin', 'root']);

    await users.updateOne({ _id: insertedId }, { $addToSet: { badges: 'first' } });
    expect((await users.findOne({ _id: insertedId })).badges).toEqual(['first']);
    await db.close();
  });

  it('$min/$max keep the smaller/larger value and seed a missing field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', score: 50 });

    await users.updateOne({ _id: insertedId }, { $min: { score: 80 } });
    expect((await users.findOne({ _id: insertedId })).score).toBe(50);
    await users.updateOne({ _id: insertedId }, { $min: { score: 20 } });
    expect((await users.findOne({ _id: insertedId })).score).toBe(20);

    await users.updateOne({ _id: insertedId }, { $max: { score: 10 } });
    expect((await users.findOne({ _id: insertedId })).score).toBe(20);
    await users.updateOne({ _id: insertedId }, { $max: { score: 99 } });
    expect((await users.findOne({ _id: insertedId })).score).toBe(99);

    await users.updateOne({ _id: insertedId }, { $min: { rank: 5 } });
    expect((await users.findOne({ _id: insertedId })).rank).toBe(5);

    await expect(users.updateOne({ _id: insertedId }, { $min: { name: 5 } })).rejects.toThrow();
    await db.close();
  });

  it('$mul multiplies an existing field and seeds a missing one at 0', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', price: 10 });

    await users.updateOne({ _id: insertedId }, { $mul: { price: 3, quantity: 5 } });
    const doc = await users.findOne({ _id: insertedId });
    expect(doc.price).toBe(30);
    expect(doc.quantity).toBe(0);
    await db.close();
  });

  it('$rename moves a field, no-ops if the source is absent, and rejects collisions', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', nick: 'Countess', team: 'core' });

    await users.updateOne({ _id: insertedId }, { $rename: { nick: 'nickname' } });
    expect(await users.findOne({ _id: insertedId })).toEqual({
      _id: insertedId, name: 'Ada', team: 'core', nickname: 'Countess'
    });

    // Missing source is a total no-op -- it doesn't touch a pre-existing
    // destination field either.
    await users.updateOne({ _id: insertedId }, { $rename: { ghost: 'team' } });
    expect((await users.findOne({ _id: insertedId })).team).toBe('core');

    // A rename's destination colliding with another operator's own target
    // field name is rejected (a pre-existing document field it isn't
    // otherwise targeting is not, though -- it's just overwritten).
    await expect(
      users.updateOne({ _id: insertedId }, { $set: { team: 'x' }, $rename: { name: 'team' } })
    ).rejects.toThrow();
    await expect(
      users.updateOne({ _id: insertedId }, { $rename: { name: 'x', nickname: 'x' } })
    ).rejects.toThrow();
    await db.close();
  });

  it('$rename overwrites a pre-existing destination field when the source is present', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', oldTeam: 'kernel', team: 'core' });

    await users.updateOne({ _id: insertedId }, { $rename: { oldTeam: 'team' } });
    expect(await users.findOne({ _id: insertedId })).toEqual({ _id: insertedId, name: 'Ada', team: 'kernel' });
    await db.close();
  });

  it('$currentDate sets a Date via true or {$type: "date"}, and validates', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada' });

    await users.updateOne({ _id: insertedId }, { $currentDate: { lastSeen: true } });
    const doc = await users.findOne({ _id: insertedId });
    expect(doc.lastSeen).toBeInstanceOf(Date);

    await users.updateOne({ _id: insertedId }, { $currentDate: { lastSeen: { $type: 'date' } } });
    expect((await users.findOne({ _id: insertedId })).lastSeen).toBeInstanceOf(Date);

    await expect(
      users.updateOne({ _id: insertedId }, { $currentDate: { lastSeen: { $type: 'timestamp' } } })
    ).rejects.toThrow();
    await expect(
      users.updateOne({ _id: insertedId }, { $currentDate: { name: true }, $set: { name: 'x' } })
    ).rejects.toThrow();
    await db.close();
  });

  it('$setOnInsert only applies when an upsert actually inserts', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', createdAt: 'original' });

    // Matched update: $setOnInsert is a complete no-op.
    await users.updateOne(
      { _id: insertedId },
      { $set: { team: 'core' }, $setOnInsert: { createdAt: 'ignored' } }
    );
    expect((await users.findOne({ _id: insertedId })).createdAt).toBe('original');

    // Upsert-and-inserted: $setOnInsert applies alongside $set.
    const result = await users.updateOne(
      { name: 'Ghost' },
      { $set: { team: 'core' }, $setOnInsert: { createdAt: 'fresh' } },
      { upsert: true }
    );
    expect(await users.findOne({ _id: result.upsertedId })).toEqual({
      _id: result.upsertedId, name: 'Ghost', team: 'core', createdAt: 'fresh'
    });

    await expect(
      users.updateOne({ _id: insertedId }, { $set: { team: 'x' }, $setOnInsert: { team: 'y' } })
    ).rejects.toThrow();
    await db.close();
  });

  it('$setOnInsert applies via updateMany\'s upsert-and-inserted path too', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const result = await users.updateMany(
      { team: 'ghosts' },
      { $set: { seen: true }, $setOnInsert: { createdAt: 'fresh' } },
      { upsert: true }
    );
    expect(await users.findOne({ _id: result.upsertedId })).toEqual({
      _id: result.upsertedId, team: 'ghosts', seen: true, createdAt: 'fresh'
    });
    await db.close();
  });

  it('$pop removes the last or first element, and no-ops on a missing field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', queue: ['a', 'b', 'c'] });

    await users.updateOne({ _id: insertedId }, { $pop: { queue: 1 } });
    expect((await users.findOne({ _id: insertedId })).queue).toEqual(['a', 'b']);
    await users.updateOne({ _id: insertedId }, { $pop: { queue: -1 } });
    expect((await users.findOne({ _id: insertedId })).queue).toEqual(['b']);

    await users.updateOne({ _id: insertedId }, { $pop: { missing: 1 } });
    expect(await users.findOne({ _id: insertedId })).not.toHaveProperty('missing');

    await expect(users.updateOne({ _id: insertedId }, { $pop: { name: 1 } })).rejects.toThrow();
    await expect(users.updateOne({ _id: insertedId }, { $pop: { queue: 2 } })).rejects.toThrow();
    await db.close();
  });

  it('$pullAll removes every listed value and no-ops on a missing field', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', tags: ['a', 'b', 'c', 'b'] });

    await users.updateOne({ _id: insertedId }, { $pullAll: { tags: ['b', 'c'] } });
    expect((await users.findOne({ _id: insertedId })).tags).toEqual(['a']);

    await users.updateOne({ _id: insertedId }, { $pullAll: { missing: ['x'] } });
    expect(await users.findOne({ _id: insertedId })).not.toHaveProperty('missing');
    await db.close();
  });

  it('$bit applies and/or/xor, chained, defaulting a missing field to 0', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', flags: 0b1010 });

    await users.updateOne({ _id: insertedId }, { $bit: { flags: { or: 0b0101 } } });
    expect((await users.findOne({ _id: insertedId })).flags).toBe(0b1111);

    await users.updateOne({ _id: insertedId }, { $bit: { flags: { and: 0b1100 } } });
    expect((await users.findOne({ _id: insertedId })).flags).toBe(0b1100);

    await users.updateOne({ _id: insertedId }, { $bit: { flags: { xor: 0b1111 } } });
    expect((await users.findOne({ _id: insertedId })).flags).toBe(0b0011);

    await users.updateOne({ _id: insertedId }, { $bit: { missing: { or: 0b101 } } });
    expect((await users.findOne({ _id: insertedId })).missing).toBe(0b101);

    await expect(users.updateOne({ _id: insertedId }, { $bit: { name: { or: 1 } } })).rejects.toThrow();
    await db.close();
  });

  it('$push modifiers: $each, $slice, $sort, $position', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', scores: [3, 1] });

    await users.updateOne({ _id: insertedId }, { $push: { scores: { $each: [4, 2] } } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([3, 1, 4, 2]);

    await users.updateOne({ _id: insertedId }, { $set: { scores: [3, 1] } });

    await users.updateOne({ _id: insertedId }, { $push: { scores: { $each: [4, 2], $slice: 3 } } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([3, 1, 4]);

    await users.updateOne({ _id: insertedId }, { $set: { scores: [3, 1] } });
    await users.updateOne({ _id: insertedId }, { $push: { scores: { $each: [4, 2], $slice: -2 } } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([4, 2]);

    await users.updateOne({ _id: insertedId }, { $set: { scores: [3, 1] } });
    await users.updateOne({ _id: insertedId }, { $push: { scores: { $each: [9], $sort: 1 } } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([1, 3, 9]);

    await users.updateOne({ _id: insertedId }, { $set: { scores: [3, 1] } });
    await users.updateOne({ _id: insertedId }, { $push: { scores: { $each: [2], $position: 1 } } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([3, 2, 1]);

    // $sort overrides $position when both are given.
    await users.updateOne({ _id: insertedId }, { $set: { scores: [3, 1] } });
    await users.updateOne(
      { _id: insertedId },
      { $push: { scores: { $each: [2], $position: 1, $sort: -1 } } }
    );
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([3, 2, 1]);
    await db.close();
  });

  it('$pull supports a query-operator condition in addition to byte equality', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', scores: [1, 5, 10, 15] });

    await users.updateOne({ _id: insertedId }, { $pull: { scores: { $lt: 10 } } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([10, 15]);

    await users.updateOne({ _id: insertedId }, { $pull: { scores: 10 } });
    expect((await users.findOne({ _id: insertedId })).scores).toEqual([15]);
    await db.close();
  });
});

describe('db: change streams (watch)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  /** Drains exactly `count` change events already queued/incoming on `stream`. */
  async function drain(stream, count) {
    const out = [];
    for (let i = 0; i < count; i++) out.push((await stream.next()).value);
    return out;
  }

  it('insertOne/insertMany emit one insert event per document', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const stream = users.watch();

    const { insertedId } = await users.insertOne({ name: 'Ada' });
    const [c1] = await drain(stream, 1);
    expect(c1).toEqual({
      ns: { coll: 'users' },
      operationType: 'insert',
      documentKey: { _id: insertedId },
      fullDocument: { _id: insertedId, name: 'Ada' }
    });

    const { insertedIds } = await users.insertMany([{ name: 'Grace' }, { name: 'Linus' }]);
    const [c2, c3] = await drain(stream, 2);
    expect(c2.operationType).toBe('insert');
    expect(c2.documentKey._id.equals(insertedIds[0])).toBe(true);
    expect(c3.fullDocument.name).toBe('Linus');

    stream.close();
    await db.close();
  });

  it('updateOne emits update on a match and insert on an upsert', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
    const stream = users.watch();

    await users.updateOne({ _id: insertedId }, { $set: { team: 'kernel' } });
    const [matched] = await drain(stream, 1);
    expect(matched.operationType).toBe('update');
    expect(matched.documentKey._id.equals(insertedId)).toBe(true);
    expect(matched.fullDocument.team).toBe('kernel');

    // A filter not already keyed on _id still resolves the right document.
    await users.updateOne({ name: 'Ada' }, { $set: { team: 'platform' } });
    const [byFilter] = await drain(stream, 1);
    expect(byFilter.operationType).toBe('update');
    expect(byFilter.fullDocument.team).toBe('platform');

    const result = await users.updateOne({ name: 'Ghost' }, { $set: { seen: true } }, { upsert: true });
    const [upserted] = await drain(stream, 1);
    expect(upserted.operationType).toBe('insert');
    expect(upserted.documentKey._id.equals(result.upsertedId)).toBe(true);
    expect(upserted.fullDocument.seen).toBe(true);

    stream.close();
    await db.close();
  });

  it('updateMany emits one update event per matched document, and insert on an upsert', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });
    const stream = users.watch();

    await users.updateMany({ team: 'core' }, { $set: { onCall: true } });
    const changes = await drain(stream, 2);
    expect(changes.every((c) => c.operationType === 'update')).toBe(true);
    expect(changes.map((c) => c.fullDocument.name).sort()).toEqual(['Ada', 'Grace']);

    await users.updateMany({ team: 'ghosts' }, { $set: { seen: true } }, { upsert: true });
    const [upserted] = await drain(stream, 1);
    expect(upserted.operationType).toBe('insert');
    expect(upserted.fullDocument.team).toBe('ghosts');

    stream.close();
    await db.close();
  });

  it('replaceOne emits replace on a match and insert on an upsert', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
    const stream = users.watch();

    await users.replaceOne({ _id: insertedId }, { name: 'Ada Lovelace' });
    const [replaced] = await drain(stream, 1);
    expect(replaced.operationType).toBe('replace');
    expect(replaced.fullDocument).toEqual({ _id: insertedId, name: 'Ada Lovelace' });

    const result = await users.replaceOne({ name: 'Ghost' }, { name: 'New Ghost' }, { upsert: true });
    const [upserted] = await drain(stream, 1);
    expect(upserted.operationType).toBe('insert');
    expect(upserted.documentKey._id.equals(result.upsertedId)).toBe(true);

    stream.close();
    await db.close();
  });

  it('deleteOne/deleteMany emit delete events with documentKey only (no fullDocument)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
    await users.insertOne({ name: 'Grace', team: 'core' });
    await users.insertOne({ name: 'Linus', team: 'kernel' });
    const stream = users.watch();

    await users.deleteOne({ _id: insertedId });
    const [deleted] = await drain(stream, 1);
    expect(deleted).toEqual({ ns: { coll: 'users' }, operationType: 'delete', documentKey: { _id: insertedId } });

    await users.deleteMany({ team: 'core' });
    const [remaining] = await drain(stream, 1);
    expect(remaining.operationType).toBe('delete');

    stream.close();
    await db.close();
  });

  it('findOneAndUpdate/findOneAndReplace/findOneAndDelete each emit the right event', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
    const stream = users.watch();

    await users.findOneAndUpdate({ _id: insertedId }, { $set: { team: 'kernel' } });
    const [updated] = await drain(stream, 1);
    expect(updated.operationType).toBe('update');
    expect(updated.fullDocument.team).toBe('kernel'); // post-image even though returnDocument defaults to 'before'

    await users.findOneAndReplace({ _id: insertedId }, { name: 'Ada Lovelace' });
    const [replaced] = await drain(stream, 1);
    expect(replaced.operationType).toBe('replace');
    expect(replaced.fullDocument).toEqual({ _id: insertedId, name: 'Ada Lovelace' });

    await users.findOneAndDelete({ _id: insertedId });
    const [deleted] = await drain(stream, 1);
    expect(deleted.operationType).toBe('delete');
    expect(deleted.documentKey._id.equals(insertedId)).toBe(true);

    stream.close();
    await db.close();
  });

  it('supports multiple concurrent watchers, on() plus for-await, and stops after close()', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const streamA = users.watch();
    const streamB = users.watch();

    const seenByA = [];
    streamA.on('change', (c) => seenByA.push(c));

    await users.insertOne({ name: 'Ada' });
    await new Promise((r) => setImmediate(r)); // let the on()-callback microtask flush
    expect(seenByA).toHaveLength(1);
    expect(seenByA[0].operationType).toBe('insert');

    const [viaB] = await drain(streamB, 1);
    expect(viaB.operationType).toBe('insert');

    streamA.close();
    await users.insertOne({ name: 'Grace' });
    await new Promise((r) => setImmediate(r));
    expect(seenByA).toHaveLength(1); // closed stream received nothing further

    const collected = [];
    (async () => {
      for await (const change of streamB) collected.push(change);
    })(); // not awaited: for-await only resolves when the stream closes
    await new Promise((r) => setImmediate(r));
    expect(collected).toHaveLength(1);
    expect(collected[0].fullDocument.name).toBe('Grace');

    streamB.close();
    await db.close();
  });

  it('rejects a non-empty pipeline (not supported yet)', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    expect(() => users.watch([{ $match: { operationType: 'insert' } }])).toThrow();
    await db.close();
  });

  it('a listener-only stream never buffers for the iterator nobody runs', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const stream = users.watch();
    const seen = [];
    stream.on('change', (c) => seen.push(c));

    for (let i = 0; i < 20; i++) await users.insertOne({ i });
    expect(seen).toHaveLength(20);        // every event delivered...
    expect(stream._queue).toHaveLength(0); // ...and none buried in a queue forever

    stream.close();
    await db.close();
  });

  it('an unconsumed iterator buffer is bounded: overflow closes the stream with a named error', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const stream = users.watch([], { maxBuffered: 5 });

    // Nobody consumes; the 6th event overflows the 5-slot buffer.
    for (let i = 0; i < 6; i++) await users.insertOne({ i });

    await expect(stream.next()).rejects.toMatchObject({ name: 'ChangeStreamOverflowError' });
    await expect(stream.next()).rejects.toMatchObject({ name: 'ChangeStreamOverflowError' }); // sticky
    // The stream closed itself: later writes emit nothing to it.
    await users.insertOne({ i: 99 });
    expect(stream._queue).toHaveLength(0);

    // for-await surfaces the same error.
    const stream2 = users.watch([], { maxBuffered: 2 });
    for (let i = 0; i < 3; i++) await users.insertOne({ i });
    await expect((async () => {
      for await (const change of stream2) void change;
    })()).rejects.toMatchObject({ name: 'ChangeStreamOverflowError' });

    await db.close();
  });

  it('a mixed consumer (listener + iterator) still sees every event on the iterator side', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const stream = users.watch();
    const viaListener = [];
    stream.on('change', (c) => viaListener.push(c));

    const firstPull = stream.next(); // parks -- marks the iterator side live
    await users.insertOne({ i: 1 });
    await users.insertOne({ i: 2 }); // lands while no next() is parked -- must buffer, not vanish

    const first = await firstPull;
    const second = await stream.next();
    expect(first.value.fullDocument.i).toBe(1);
    expect(second.value.fullDocument.i).toBe(2);
    expect(viaListener).toHaveLength(2);

    stream.close();
    await db.close();
  });

  it('costs nothing when nothing is watching: no extra findOne calls', async () => {
    const db = await openDb();
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
    const spy = vi.spyOn(users, 'findOne');

    await users.updateOne({ name: 'Ada' }, { $set: { team: 'kernel' } });
    await users.deleteOne({ _id: insertedId });
    expect(spy).not.toHaveBeenCalled();

    spy.mockRestore();
    await db.close();
  });

  it('updateMany with a watcher costs no extra queries either', async () => {
    // It used to cost one find() for the ids plus one findOne() per
    // matched document -- "O(matched) extra round trips", by its own
    // comment. C hands back every post-image alongside the counts now
    // (dc_update_many's `images`), because the update loop already holds
    // each one.
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertMany([
      { name: 'Ada', team: 'core' },
      { name: 'Grace', team: 'core' },
      { name: 'Linus', team: 'core' }
    ]);

    const seen = [];
    const stream = users.watch();
    stream.on('change', (e) => seen.push(e));

    const findOneSpy = vi.spyOn(users, 'findOne');
    const findSpy = vi.spyOn(users, 'find');
    await users.updateMany({ team: 'core' }, { $set: { onCall: true } });
    expect(findOneSpy).not.toHaveBeenCalled();
    expect(findSpy).not.toHaveBeenCalled();

    // ...and the events are still complete, with post-images.
    await new Promise((r) => setTimeout(r, 0));
    expect(seen).toHaveLength(3);
    expect(seen.every((e) => e.operationType === 'update')).toBe(true);
    expect(seen.every((e) => e.fullDocument.onCall === true)).toBe(true);
    expect(seen.map((e) => e.fullDocument.name).sort())
      .toEqual(['Ada', 'Grace', 'Linus']);
    expect(seen.every((e) => e.documentKey._id)).toBe(true);

    findSpy.mockRestore();
    findOneSpy.mockRestore();
    stream.close();
    await db.close();
  });

  it('deleteMany with a watcher costs no extra query either', async () => {
    // It used to run a projected find() first, purely to learn what was
    // about to disappear. C records each removed _id as it commits.
    const db = await openDb();
    const users = await db.collection('users');
    await users.insertMany([
      { name: 'Ada', team: 'core' },
      { name: 'Grace', team: 'core' }
    ]);

    const seen = [];
    const stream = users.watch();
    stream.on('change', (e) => seen.push(e));

    const findSpy = vi.spyOn(users, 'find');
    const { deletedCount } = await users.deleteMany({ team: 'core' });
    expect(deletedCount).toBe(2);
    expect(findSpy).not.toHaveBeenCalled();

    await new Promise((r) => setTimeout(r, 0));
    expect(seen).toHaveLength(2);
    expect(seen.every((e) => e.operationType === 'delete')).toBe(true);
    expect(seen.every((e) => e.documentKey._id)).toBe(true);
    // Distinct ids, not the same one twice.
    expect(new Set(seen.map((e) => String(e.documentKey._id))).size).toBe(2);

    findSpy.mockRestore();
    stream.close();
    await db.close();
  });
});

describe('db: text index ($text, milestone 6)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  async function seedArticles(posts) {
    await posts.insertOne({ title: 'Fox story', body: 'The quick brown fox jumps over the lazy dog' });
    await posts.insertOne({ title: 'Forest run', body: 'A fast fox runs through the forest' });
    await posts.insertOne({ title: 'Nap time', body: 'The lazy dog sleeps all day' });
    await posts.insertOne({ title: 'Space', body: 'Completely unrelated content about spacecraft' });
  }

  it('createIndex({field: "text"}) backfills existing documents', async () => {
    const db = await openDb();
    const posts = await db.collection('posts');
    await seedArticles(posts);

    const name = await posts.createIndex({ body: 'text' });
    expect(name).toBe('body_text');
    expect(await posts.listIndexes()).toEqual([{ name: 'body_text', key: { body: 'text' } }]);

    const foxDocs = await posts.find({ $text: { $search: 'fox' } }).toArray();
    expect(foxDocs.map(d => d.title).sort()).toEqual(['Forest run', 'Fox story']);
  });

  it('insertOne/deleteOne/updateOne maintain the text index', async () => {
    const db = await openDb();
    const posts = await db.collection('posts');
    await posts.createIndex({ body: 'text' });
    const { insertedId } = await posts.insertOne({ title: 'Fox story', body: 'a fox in the forest' });

    expect((await posts.find({ $text: { $search: 'fox' } }).toArray()).map(d => d.title)).toEqual(['Fox story']);

    await posts.updateOne({ _id: insertedId }, { $set: { body: 'nothing to see here' } });
    expect(await posts.find({ $text: { $search: 'fox' } }).toArray()).toEqual([]);
    expect((await posts.find({ $text: { $search: 'nothing' } }).toArray()).map(d => d.title)).toEqual(['Fox story']);

    await posts.deleteOne({ _id: insertedId });
    expect(await posts.find({ $text: { $search: 'nothing' } }).toArray()).toEqual([]);
  });

  it('$text combines with a residual filter', async () => {
    const db = await openDb();
    const posts = await db.collection('posts');
    await posts.createIndex({ body: 'text' });
    await posts.insertOne({ title: 'Fox story', body: 'a fox in the forest', section: 'nature' });
    await posts.insertOne({ title: 'Fox news', body: 'a fox spotted downtown', section: 'local' });

    const natureFoxes = await posts.find({ $text: { $search: 'fox' }, section: 'nature' }).toArray();
    expect(natureFoxes.map(d => d.title)).toEqual(['Fox story']);
  });

  it('tolerates documents missing the text field or holding a non-string value', async () => {
    const db = await openDb();
    const posts = await db.collection('posts');
    await posts.insertOne({ title: 'No body' });
    await posts.insertOne({ title: 'Weird body', body: 42 });
    await posts.insertOne({ title: 'Good body', body: 'a fox runs' });

    // Backfill must not fail even though two of the three documents can't
    // be text-indexed.
    await expect(posts.createIndex({ body: 'text' })).resolves.toBe('body_text');
    expect(await posts.countDocuments()).toBe(3);
    expect((await posts.find({ $text: { $search: 'fox' } }).toArray()).map(d => d.title)).toEqual(['Good body']);
  });

  it('rejects a second text index and $text without one', async () => {
    const db = await openDb();
    const posts = await db.collection('posts');
    await posts.createIndex({ body: 'text' });
    await expect(posts.createIndex({ title: 'text' })).rejects.toThrow();

    const db2 = await openDb();
    const users = await db2.collection('users');
    await expect(users.find({ $text: { $search: 'fox' } }).toArray()).rejects.toThrow();
  });

  it('persists and stays maintained across close/reopen', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const posts1 = await db1.collection('posts');
    await posts1.createIndex({ body: 'text' });
    await posts1.insertOne({ title: 'Fox story', body: 'a fox in the forest' });
    await db1.close();

    const db2 = await connect(provider);
    const posts2 = await db2.collection('posts');
    expect(await posts2.listIndexes()).toEqual([{ name: 'body_text', key: { body: 'text' } }]);
    expect((await posts2.find({ $text: { $search: 'fox' } }).toArray()).map(d => d.title)).toEqual(['Fox story']);

    await posts2.insertOne({ title: 'Fox news', body: 'another fox sighting' });
    expect((await posts2.find({ $text: { $search: 'fox' } }).toArray()).map(d => d.title).sort())
      .toEqual(['Fox news', 'Fox story']);
    await db2.close();
  });
});

describe('db: geo index ($near/$geoWithin, milestone 6)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  const point = (lng, lat) => ({ type: 'Point', coordinates: [lng, lat] });
  const LONDON = point(-0.12, 51.5);
  const PARIS = point(2.35, 48.85);
  const NEW_YORK = point(-74.0, 40.71);
  const TOKYO = point(139.69, 35.68);

  async function seedPlaces(places) {
    await places.insertOne({ name: 'London', location: LONDON });
    await places.insertOne({ name: 'Paris', location: PARIS });
    await places.insertOne({ name: 'New York', location: NEW_YORK });
    await places.insertOne({ name: 'Tokyo', location: TOKYO });
  }

  it('createIndex({field: "2dsphere"}) backfills existing documents', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await seedPlaces(places);

    const name = await places.createIndex({ location: '2dsphere' });
    expect(name).toBe('location_2dsphere');
    expect(await places.listIndexes()).toEqual([{ name: 'location_2dsphere', key: { location: '2dsphere' } }]);
    await db.close();
  });

  it('$near returns nearest-first and respects $maxDistance (km)', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.createIndex({ location: '2dsphere' });
    await seedPlaces(places);

    const all = await places.find({ location: { $near: { $geometry: LONDON } } }).toArray();
    expect(all.map(d => d.name)).toEqual(['London', 'Paris', 'New York', 'Tokyo']);

    const near = await places.find({ location: { $near: { $geometry: LONDON, $maxDistance: 1000 } } }).toArray();
    expect(near.map(d => d.name)).toEqual(['London', 'Paris']);
    await db.close();
  });

  it('$geoWithin $box finds points inside a bounding box', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.createIndex({ location: '2dsphere' });
    await seedPlaces(places);

    const europe = await places.find({
      location: { $geoWithin: { $box: [[-10, 40], [10, 60]] } }
    }).toArray();
    expect(europe.map(d => d.name).sort()).toEqual(['London', 'Paris']);
    await db.close();
  });

  it('$geoWithin $center finds points inside a circle (radius in km)', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.createIndex({ location: '2dsphere' });
    await seedPlaces(places);

    const nearLondon = await places.find({
      location: { $geoWithin: { $center: [[-0.12, 51.5], 500] } }
    }).toArray();
    expect(nearLondon.map(d => d.name).sort()).toEqual(['London', 'Paris']);
    await db.close();
  });

  it('$near/$geoWithin combine with a residual filter', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.createIndex({ location: '2dsphere' });
    await places.insertOne({ name: 'London', location: LONDON, capital: true });
    await places.insertOne({ name: 'Manchester', location: point(-2.24, 53.48), capital: false });

    const capitalsNearby = await places.find({
      location: { $near: { $geometry: LONDON, $maxDistance: 500 } },
      capital: true
    }).toArray();
    expect(capitalsNearby.map(d => d.name)).toEqual(['London']);
    await db.close();
  });

  it('insertOne/deleteOne/updateOne maintain the geo index', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.createIndex({ location: '2dsphere' });
    const { insertedId } = await places.insertOne({ name: 'London', location: LONDON });

    expect((await places.find({ location: { $near: { $geometry: LONDON, $maxDistance: 10 } } }).toArray()).map(d => d.name))
      .toEqual(['London']);

    await places.updateOne({ _id: insertedId }, { $set: { location: TOKYO } });
    expect(await places.find({ location: { $near: { $geometry: LONDON, $maxDistance: 10 } } }).toArray()).toEqual([]);
    expect((await places.find({ location: { $near: { $geometry: TOKYO, $maxDistance: 10 } } }).toArray()).map(d => d.name))
      .toEqual(['London']);

    await places.deleteOne({ _id: insertedId });
    expect(await places.find({ location: { $near: { $geometry: TOKYO, $maxDistance: 10 } } }).toArray()).toEqual([]);
    await db.close();
  });

  it('tolerates a missing geo field but rejects a malformed one', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.insertOne({ name: 'No location' });
    await expect(places.createIndex({ location: '2dsphere' })).resolves.toBe('location_2dsphere');

    await expect(places.insertOne({ name: 'Bad location', location: { type: 'Point', coordinates: [1] } }))
      .rejects.toThrow();
    await db.close();
  });

  it('rejects $near/$geoWithin without a geo index on that field', async () => {
    const db = await openDb();
    const places = await db.collection('places');
    await places.insertOne({ name: 'London', location: LONDON });
    await expect(places.find({ location: { $near: { $geometry: LONDON } } }).toArray()).rejects.toThrow();
    await db.close();
  });

  it('persists and stays maintained across close/reopen', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const places1 = await db1.collection('places');
    await places1.createIndex({ location: '2dsphere' });
    await places1.insertOne({ name: 'London', location: LONDON });
    await db1.close();

    const db2 = await connect(provider);
    const places2 = await db2.collection('places');
    expect(await places2.listIndexes()).toEqual([{ name: 'location_2dsphere', key: { location: '2dsphere' } }]);
    await places2.insertOne({ name: 'Paris', location: PARIS });

    const europe = await places2.find({ location: { $geoWithin: { $box: [[-10, 40], [10, 60]] } } }).toArray();
    expect(europe.map(d => d.name).sort()).toEqual(['London', 'Paris']);
    await db2.close();
  });
});

describe('db: cross-file write atomicity (milestone 5)', () => {
  // Every collection gets a commit journal automatically -- see docs/db-plan.md
  // milestone 5 and test/db.atomic-wasm.test.js for crash-simulation coverage.
  // These are non-crash sanity checks: normal operation is unaffected, and the
  // journal file's size tracks the documented bounds.
  function journalSize(provider, collName) {
    const handle = provider._files.get(`coll-${collName}-journal.bj`);
    return handle ? handle.getSize() : 0;
  }

  it('a collection with no secondary indexes never writes to its journal', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.insertOne({ name: 'Ada' });
    await users.insertOne({ name: 'Grace' });
    await users.deleteOne({ name: 'Ada' });
    expect(journalSize(provider, 'users')).toBe(0);
    await db.close();
  });

  it('journal size stays bounded by two slots once indexes are attached', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    // n = primary(1) + equality(1) = 2 files/slot -> slot = 24 + 8*2 = 40.
    const maxSize = 2 * (24 + 8 * 2);
    for (let i = 0; i < 20; i++) {
      await users.insertOne({ name: `user-${i}`, team: i % 2 === 0 ? 'core' : 'infra' });
      expect(journalSize(provider, 'users')).toBeLessThanOrEqual(maxSize);
    }
    await db.close();
  });

  it('CRUD operations are unaffected by journaling across close/reopen', async () => {
    const provider = new MemoryStorageProvider();
    const db1 = await connect(provider);
    const users1 = await db1.collection('users');
    await users1.createIndex({ team: 1 });
    const { insertedId } = await users1.insertOne({ name: 'Ada', team: 'core' });
    await users1.updateOne({ _id: insertedId }, { $set: { team: 'infra' } });
    await db1.close();

    const db2 = await connect(provider);
    const users2 = await db2.collection('users');
    const doc = await users2.findOne({ _id: insertedId });
    expect(doc.team).toBe('infra');
    expect((await users2.findByIndex('team_1', ['infra'])).map(d => d._id.toHexString()))
      .toEqual([insertedId.toHexString()]);
    await users2.deleteOne({ _id: insertedId });
    expect(await users2.findOne({ _id: insertedId })).toBeNull();
    await db2.close();
  });
});

describe('db: CRUD completeness (milestone 8)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  describe('insertMany', () => {
    it('inserts every document and assigns/keeps _id', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const suppliedId = new ObjectId();
      const result = await users.insertMany([{ name: 'Ada' }, { _id: suppliedId, name: 'Grace' }]);
      expect(result.acknowledged).toBe(true);
      expect(result.insertedCount).toBe(2);
      expect(result.insertedIds[1].equals(suppliedId)).toBe(true);
      const all = await users.find({}).toArray();
      expect(all.map(d => d.name).sort()).toEqual(['Ada', 'Grace']);
      await db.close();
    });

    it('ordered (default) stops at the first failing document', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const dupId = new ObjectId();
      await users.insertOne({ _id: dupId, name: 'Existing' });

      const err = await users.insertMany([
        { name: 'Ada' },
        { _id: dupId, name: 'Duplicate' }, // fails: _id already exists
        { name: 'Never attempted' }
      ]).catch(e => e);
      expect(err).toBeInstanceOf(Error);
      expect(err.result.insertedCount).toBe(1);
      expect(await users.countDocuments({})).toBe(2); // Existing + Ada, not the third
      await db.close();
    });

    it('unordered attempts every document despite an earlier failure', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const dupId = new ObjectId();
      await users.insertOne({ _id: dupId, name: 'Existing' });

      const err = await users.insertMany([
        { _id: dupId, name: 'Duplicate' },
        { name: 'Still inserted' }
      ], { ordered: false }).catch(e => e);
      expect(err).toBeInstanceOf(Error);
      expect(await users.findOne({ name: 'Still inserted' })).not.toBeNull();
      await db.close();
    });

    it('rejects an empty array', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await expect(users.insertMany([])).rejects.toThrow();
      await db.close();
    });
  });

  describe('deleteMany', () => {
    it('deletes every matching document and maintains indexes', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ team: 1 });
      await users.insertMany([
        { name: 'Ada', team: 'core' }, { name: 'Grace', team: 'core' }, { name: 'Kay', team: 'infra' }
      ]);

      const result = await users.deleteMany({ team: 'core' });
      expect(result).toEqual({ acknowledged: true, deletedCount: 2 });
      expect(await users.countDocuments({})).toBe(1);
      expect(await users.findByIndex('team_1', ['core'])).toEqual([]);
      expect((await users.findByIndex('team_1', ['infra'])).map(d => d.name)).toEqual(['Kay']);
      await db.close();
    });
  });

  describe('findOneAndUpdate / findOneAndReplace', () => {
    it('findOneAndUpdate returns the pre-image by default', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

      const doc = await users.findOneAndUpdate({ _id: insertedId }, { $set: { team: 'infra' } });
      expect(doc).toEqual({ _id: insertedId, name: 'Ada', team: 'core' });
      expect((await users.findOne({ _id: insertedId })).team).toBe('infra');
      await db.close();
    });

    it('findOneAndUpdate returns the post-image with returnDocument: "after"', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

      const doc = await users.findOneAndUpdate(
        { _id: insertedId }, { $set: { team: 'infra' } }, { returnDocument: 'after' }
      );
      expect(doc).toEqual({ _id: insertedId, name: 'Ada', team: 'infra' });
      await db.close();
    });

    it('findOneAndUpdate returns null when nothing matches and no upsert', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      expect(await users.findOneAndUpdate({ name: 'Nobody' }, { $set: { x: 1 } })).toBeNull();
      await db.close();
    });

    it('findOneAndUpdate with upsert: "before" returns null, "after" returns the new document', async () => {
      const db = await openDb();
      const users = await db.collection('users');

      const before = await users.findOneAndUpdate(
        { name: 'Ghost' }, { $set: { team: 'core' } }, { upsert: true }
      );
      expect(before).toBeNull();
      expect(await users.countDocuments({})).toBe(1);

      const after = await users.findOneAndUpdate(
        { name: 'Ghost2' }, { $set: { team: 'core' } }, { upsert: true, returnDocument: 'after' }
      );
      expect(after.name).toBe('Ghost2');
      expect(after.team).toBe('core');
      await db.close();
    });

    it('findOneAndReplace returns the pre-image and maintains indexes', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ team: 1 });
      const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

      const doc = await users.findOneAndReplace({ _id: insertedId }, { name: 'Ada', team: 'infra' });
      expect(doc).toEqual({ _id: insertedId, name: 'Ada', team: 'core' });
      expect((await users.findByIndex('team_1', ['infra'])).map(d => d.name)).toEqual(['Ada']);
      expect(await users.findByIndex('team_1', ['core'])).toEqual([]);
      await db.close();
    });

    it('findOneAndReplace returns the post-image with returnDocument: "after"', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });

      const doc = await users.findOneAndReplace(
        { _id: insertedId }, { name: 'Ada', team: 'infra' }, { returnDocument: 'after' }
      );
      expect(doc).toEqual({ _id: insertedId, name: 'Ada', team: 'infra' });
      await db.close();
    });
  });

  describe('findOneAndDelete', () => {
    it('deletes and returns the matched document', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const { insertedId } = await users.insertOne({ name: 'Ada' });

      const doc = await users.findOneAndDelete({ _id: insertedId });
      expect(doc).toEqual({ _id: insertedId, name: 'Ada' });
      expect(await users.findOne({ _id: insertedId })).toBeNull();
      await db.close();
    });

    it('returns null when nothing matches', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      expect(await users.findOneAndDelete({ name: 'Nobody' })).toBeNull();
      await db.close();
    });
  });

  describe('distinct', () => {
    it('returns unique values, skipping documents missing the field', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([
        { name: 'Ada', team: 'core' }, { name: 'Grace', team: 'core' },
        { name: 'Kay', team: 'infra' }, { name: 'NoTeam' }
      ]);
      const teams = await users.distinct('team');
      expect(teams.sort()).toEqual(['core', 'infra']);
      await db.close();
    });

    it('resolves a dotted path', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([
        { name: 'Ada', address: { city: 'London' } },
        { name: 'Grace', address: { city: 'Paris' } },
        { name: 'Kay', address: { city: 'London' } }
      ]);
      expect((await users.distinct('address.city')).sort()).toEqual(['London', 'Paris']);
      await db.close();
    });

    it('flattens array field values instead of returning the whole array', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([{ name: 'Ada', tags: ['core', 'admin'] }, { name: 'Grace', tags: ['admin'] }]);
      expect((await users.distinct('tags')).sort()).toEqual(['admin', 'core']);
      await db.close();
    });

    it('honors a filter', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([
        { name: 'Ada', team: 'core' }, { name: 'Grace', team: 'infra' }
      ]);
      expect(await users.distinct('team', { name: 'Ada' })).toEqual(['core']);
      await db.close();
    });
  });

  describe('estimatedDocumentCount', () => {
    it('matches countDocuments({})', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertMany([{ name: 'Ada' }, { name: 'Grace' }]);
      expect(await users.estimatedDocumentCount()).toBe(2);
      await db.close();
    });
  });

  describe('bulkWrite', () => {
    it('applies mixed operation types and aggregates counts', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
      await users.insertOne({ name: 'ToDelete' });

      const result = await users.bulkWrite([
        { insertOne: { document: { name: 'Grace', team: 'core' } } },
        { updateOne: { filter: { _id: insertedId }, update: { $set: { team: 'infra' } } } },
        { deleteOne: { filter: { name: 'ToDelete' } } }
      ]);
      expect(result.insertedCount).toBe(1);
      expect(result.matchedCount).toBe(1);
      expect(result.modifiedCount).toBe(1);
      expect(result.deletedCount).toBe(1);
      expect((await users.findOne({ _id: insertedId })).team).toBe('infra');
      expect(await users.findOne({ name: 'ToDelete' })).toBeNull();
      expect(await users.findOne({ name: 'Grace' })).not.toBeNull();
      await db.close();
    });

    it('ordered (default) stops at the first failing operation', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const dupId = new ObjectId();
      await users.insertOne({ _id: dupId, name: 'Existing' });

      const err = await users.bulkWrite([
        { insertOne: { document: { _id: dupId, name: 'Duplicate' } } },
        { insertOne: { document: { name: 'Never attempted' } } }
      ]).catch(e => e);
      expect(err).toBeInstanceOf(Error);
      expect(await users.findOne({ name: 'Never attempted' })).toBeNull();
      await db.close();
    });

    it('unordered attempts every operation despite an earlier failure', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      const dupId = new ObjectId();
      await users.insertOne({ _id: dupId, name: 'Existing' });

      const err = await users.bulkWrite([
        { insertOne: { document: { _id: dupId, name: 'Duplicate' } } },
        { insertOne: { document: { name: 'Still inserted' } } }
      ], { ordered: false }).catch(e => e);
      expect(err).toBeInstanceOf(Error);
      expect(err.writeErrors).toHaveLength(1);
      expect(await users.findOne({ name: 'Still inserted' })).not.toBeNull();
      await db.close();
    });

    it('rejects an empty operations array', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await expect(users.bulkWrite([])).rejects.toThrow();
      await db.close();
    });
  });
});

describe('db: index options (milestone 9)', () => {
  async function openDb() {
    return connect(new MemoryStorageProvider());
  }

  describe('unique', () => {
    it('rejects a duplicate value on insert, update, and upsert', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ email: 1 }, { unique: true, name: 'email_1' });
      await users.insertOne({ email: 'ada@example.com' });

      await expect(users.insertOne({ email: 'ada@example.com' })).rejects.toThrow(/[Dd]uplicate/);

      const { insertedId } = await users.insertOne({ email: 'grace@example.com' });
      await expect(users.updateOne({ _id: insertedId }, { $set: { email: 'ada@example.com' } }))
        .rejects.toThrow(/[Dd]uplicate/);
      // The rejected update must not have changed the document.
      expect((await users.findOne({ _id: insertedId })).email).toBe('grace@example.com');

      await expect(users.updateOne({ email: 'nobody@example.com' }, { $set: { email: 'ada@example.com' } }, { upsert: true }))
        .rejects.toThrow(/[Dd]uplicate/);
      await db.close();
    });

    it('allows a value again once the conflicting document is deleted', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ email: 1 }, { unique: true });
      const { insertedId } = await users.insertOne({ email: 'ada@example.com' });

      await expect(users.insertOne({ email: 'ada@example.com' })).rejects.toThrow();
      await users.deleteOne({ _id: insertedId });
      await expect(users.insertOne({ email: 'ada@example.com' })).resolves.toBeTruthy();
      await db.close();
    });

    it('createIndex fails when the collection already has duplicate values and leaves no partial index', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertOne({ email: 'ada@example.com' });
      await users.insertOne({ email: 'ada@example.com' });

      await expect(users.createIndex({ email: 1 }, { unique: true })).rejects.toThrow();
      expect(await users.listIndexes()).toEqual([]);
      // No index should be left half-built: creating a differently-named
      // non-unique index on the same field must still work cleanly.
      await expect(users.createIndex({ email: 1 })).resolves.toBeTruthy();
      await db.close();
    });

    it('composes with partialFilterExpression: uniqueness only enforced among matching documents', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ email: 1 }, { unique: true, partialFilterExpression: { active: true } });

      await users.insertOne({ email: 'ada@example.com', active: false });
      await users.insertOne({ email: 'ada@example.com', active: false }); // both inactive: not indexed, no conflict
      await users.insertOne({ email: 'ada@example.com', active: true });
      await expect(users.insertOne({ email: 'ada@example.com', active: true })).rejects.toThrow();
      await db.close();
    });
  });

  describe('sparse', () => {
    it('does not index (or error on) a document missing the field', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ email: 1 }, { sparse: true, name: 'email_1' });
      await expect(users.insertOne({ name: 'no-email' })).resolves.toBeTruthy();
      await users.insertOne({ name: 'has-email', email: 'ada@example.com' });
      // The missing-field document never got an entry -- only the one with
      // the field shows up via the index.
      expect((await users.findByIndex('email_1', ['ada@example.com'])).map(d => d.name)).toEqual(['has-email']);
      await db.close();
    });

    it('backfill tolerates pre-existing documents missing the field', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertOne({ name: 'no-email' });
      await users.insertOne({ name: 'has-email', email: 'ada@example.com' });
      await expect(users.createIndex({ email: 1 }, { sparse: true })).resolves.toBeTruthy();
      expect((await users.findByIndex('email_1', ['ada@example.com'])).map(d => d.name)).toEqual(['has-email']);
      await db.close();
    });

    it('a document removed from the index by an update does not error on removal', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ email: 1 }, { sparse: true });
      const { insertedId } = await users.insertOne({ email: 'ada@example.com' });
      await expect(users.updateOne({ _id: insertedId }, { $unset: { email: '' } })).resolves.toBeTruthy();
      expect(await users.findByIndex('email_1', ['ada@example.com'])).toEqual([]);
      await db.close();
    });
  });

  describe('partialFilterExpression', () => {
    it('only indexes documents matching the filter', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ team: 1 }, { partialFilterExpression: { active: true }, name: 'team_1' });
      await users.insertOne({ team: 'core', active: true });
      await users.insertOne({ team: 'core', active: false });
      expect((await users.findByIndex('team_1', ['core'])).length).toBe(1);
      await db.close();
    });

    it('an update crossing the filter boundary adds/removes the index entry', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.createIndex({ team: 1 }, { partialFilterExpression: { active: true }, name: 'team_1' });
      const { insertedId } = await users.insertOne({ team: 'core', active: false });
      expect(await users.findByIndex('team_1', ['core'])).toEqual([]);

      await users.updateOne({ _id: insertedId }, { $set: { active: true } });
      expect((await users.findByIndex('team_1', ['core'])).map(d => d._id.toHexString())).toEqual([insertedId.toHexString()]);

      await users.updateOne({ _id: insertedId }, { $set: { active: false } });
      expect(await users.findByIndex('team_1', ['core'])).toEqual([]);
      await db.close();
    });
  });

  describe('TTL (expireAfterSeconds)', () => {
    it('rejects a compound key spec', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await expect(users.createIndex({ a: 1, b: 1 }, { expireAfterSeconds: 60 })).rejects.toThrow(/single-field/);
      await db.close();
    });

    it('pruneExpired deletes only documents past the cutoff and reports the count', async () => {
      const db = await openDb();
      const events = await db.collection('events');
      await events.createIndex({ createdAt: 1 }, { expireAfterSeconds: 60 });
      await events.insertOne({ createdAt: new Date(Date.now() - 3600 * 1000), tag: 'old' });
      await events.insertOne({ createdAt: new Date(), tag: 'recent' });

      const deletedCount = await events.pruneExpired();
      expect(deletedCount).toBe(1);
      expect((await events.find({}).toArray()).map(d => d.tag)).toEqual(['recent']);
      await db.close();
    });

    it('a collection with no TTL index prunes nothing', async () => {
      const db = await openDb();
      const users = await db.collection('users');
      await users.insertOne({ name: 'Ada' });
      expect(await users.pruneExpired()).toBe(0);
      await db.close();
    });
  });

  describe('index options persist across close/reopen', () => {
    it('unique/sparse/partialFilterExpression/expireAfterSeconds survive a reopen', async () => {
      const provider = new MemoryStorageProvider();
      const db1 = await connect(provider);
      const users1 = await db1.collection('users');
      await users1.createIndex({ email: 1 }, { unique: true, sparse: true, partialFilterExpression: { active: true }, name: 'email_1' });
      await users1.createIndex({ createdAt: 1 }, { expireAfterSeconds: 60, name: 'ttl_1' });
      await db1.close();

      const db2 = await connect(provider);
      const users2 = await db2.collection('users');
      const indexes = await users2.listIndexes();
      const emailIx = indexes.find(i => i.name === 'email_1');
      expect(emailIx.unique).toBe(true);
      expect(emailIx.sparse).toBe(true);
      expect(emailIx.partialFilterExpression).toEqual({ active: true });
      const ttlIx = indexes.find(i => i.name === 'ttl_1');
      expect(ttlIx.expireAfterSeconds).toBe(60);

      // Still enforced after reopen (createdAt is required: ttl_1 isn't sparse).
      await users2.insertOne({ email: 'ada@example.com', active: true, createdAt: new Date() });
      await expect(users2.insertOne({ email: 'ada@example.com', active: true, createdAt: new Date() }))
        .rejects.toThrow();
      await db2.close();
    });
  });
});

const { hasOPFS } = await bootstrapOPFS();

describe.skipIf(!hasOPFS)('db: OPFS storage provider', () => {
  it('round-trips insertOne/findOne through real OPFS files', async () => {
    const rootDirHandle = await navigator.storage.getDirectory();
    const dirName = `test-db-opfs-${Date.now()}`;
    const dbDir = await rootDirHandle.getDirectoryHandle(dirName, { create: true });

    const db = await connect(new OPFSStorageProvider(dbDir));
    const users = await db.collection('users');
    const { insertedId } = await users.insertOne({ name: 'Ada' });
    expect((await users.findOne({ _id: insertedId })).name).toBe('Ada');
    await db.close();

    const db2 = await connect(new OPFSStorageProvider(dbDir));
    const users2 = await db2.collection('users');
    expect((await users2.findOne({ _id: insertedId })).name).toBe('Ada');
    await db2.close();

    await rootDirHandle.removeEntry(dirName, { recursive: true });
  });
});
