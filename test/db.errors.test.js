/**
 * Named, coded errors (docs/roadmap.md P0 #2): coded failures raise
 * NisabaError subclasses carrying the C error code (`code`) and a class
 * name (`name`), so programs branch on identity instead of matching
 * message strings. Across the coordinator's RPC wire the identity
 * survives as data (name/code on a rebuilt Error), not as a prototype.
 */
import { describe, it, expect } from 'vitest';
import {
  ready, MemoryStorageProvider, ObjectId,
  NisabaError, DuplicateKeyError, MissingIndexedFieldError, UnindexableValueError, InvalidIdError
} from '../src/nisaba-wasm.js';
import { connect } from '../src/db.js';
import { connectShared } from '../src/db-coordinator.js';

await ready();

async function freshUsers() {
  const db = await connect(new MemoryStorageProvider());
  return { db, users: await db.collection('users') };
}

describe('db: named, coded errors', () => {
  it('duplicate _id: DuplicateKeyError, code -10', async () => {
    const { db, users } = await freshUsers();
    const _id = new ObjectId();
    await users.insertOne({ _id, a: 1 });
    const err = await users.insertOne({ _id, a: 2 }).catch((e) => e);
    expect(err).toBeInstanceOf(DuplicateKeyError);
    expect(err).toBeInstanceOf(NisabaError);
    expect(err.name).toBe('DuplicateKeyError');
    expect(err.code).toBe(-10);
    await db.close();
  });

  it('unique-index violation: DuplicateKeyError, code -12', async () => {
    const { db, users } = await freshUsers();
    await users.createIndex({ email: 1 }, { unique: true });
    await users.insertOne({ email: 'ada@example.com' });
    const err = await users.insertOne({ email: 'ada@example.com' }).catch((e) => e);
    expect(err.name).toBe('DuplicateKeyError');
    expect(err.code).toBe(-12);
    await db.close();
  });

  it('missing non-sparse indexed field: MissingIndexedFieldError, code -13', async () => {
    const { db, users } = await freshUsers();
    await users.createIndex({ team: 1 });
    const err = await users.insertOne({ name: 'no team field' }).catch((e) => e);
    expect(err).toBeInstanceOf(MissingIndexedFieldError);
    expect(err.code).toBe(-13);
    await db.close();
  });

  it('unindexable value: UnindexableValueError, code -14', async () => {
    const { db, users } = await freshUsers();
    await users.createIndex({ team: 1 });
    const err = await users.insertOne({ team: { nested: 'object' } }).catch((e) => e);
    expect(err).toBeInstanceOf(UnindexableValueError);
    expect(err.code).toBe(-14);
    await db.close();
  });

  it('scalar _ids raise InvalidIdError pointing at the unique-index alternative', async () => {
    const { db, users } = await freshUsers();
    for (const bad of ['user-42', 1, new Date(), null]) {
      const err = await users.insertOne({ _id: bad, a: 1 }).catch((e) => e);
      expect(err, `_id ${String(bad)}`).toBeInstanceOf(InvalidIdError);
      expect(err.message).toMatch(/unique index/);
    }
    // The documented alternative works.
    await users.createIndex({ email: 1 }, { unique: true });
    await users.insertOne({ email: 'natural@key.example' });
    expect((await users.findOne({ email: 'natural@key.example' }))._id).toBeInstanceOf(ObjectId);
    await db.close();
  });

  it('name and code survive the coordinator RPC wire as data', async () => {
    const provider = new MemoryStorageProvider();
    const [a, b] = await Promise.all([
      connectShared('errors-wire', provider, {}),
      connectShared('errors-wire', provider, {})
    ]);
    const follower = a._coord.role === 'leader' ? b : a;

    const users = await follower.collection('users');
    const _id = new ObjectId();
    await users.insertOne({ _id, a: 1 });
    const err = await users.insertOne({ _id, a: 2 }).catch((e) => e);
    expect(err.name).toBe('DuplicateKeyError'); // identity as data...
    expect(err.code).toBe(-10);
    expect(err).not.toBeInstanceOf(NisabaError); // ...never as prototype (documented)

    await Promise.all([a, b].map((d) => d.close()));
  });
});
