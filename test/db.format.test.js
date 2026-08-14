/**
 * On-disk format compatibility (docs/format-compatibility.md, docs/
 * roadmap.md P1 #12): every database carries a version stamp in its
 * catalog under the reserved "__format__" key; Db.open() stamps fresh and
 * pre-stamp databases and refuses -- loudly, naming both versions --
 * anything stamped newer than the build understands.
 *
 * And, since format 2, converts what it can: a database stamped older
 * than this build is migrated collection by collection on open. The
 * second half of this file builds real v1 databases to run that on.
 */
import { describe, it, expect } from 'vitest';
import { ready, BPlusTree, MemoryHandle, dbFormatVersion } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider, ObjectId } from '../src/db.js';
import { downgradeToV1 } from './helpers/v1-fixture.js';

await ready();

/** Doctor a provider's catalog directly through the raw B+ tree. */
async function withCatalog(provider, fn) {
  const handle = await provider.openFile('__catalog__.bj', { create: false });
  const catalog = new BPlusTree(handle, 32);
  await catalog.open();
  fn(catalog);
  catalog.flush();
  await catalog.close();
}

describe('db: on-disk format stamp', () => {
  it('stamps a fresh database and hides the stamp from the collection surface', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.insertOne({ a: 1 });
    expect(await db.listCollections()).toEqual(['users']); // no __format__
    await db.close();

    await withCatalog(provider, (catalog) => {
      expect(catalog.search('__format__')).toEqual({ v: dbFormatVersion() });
    });
  });

  it('re-stamps a pre-stamp database on open, changing nothing else', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    await (await db.collection('users')).insertOne({ a: 1 });
    await db.close();
    // Simulate a database written before the stamp existed.
    await withCatalog(provider, (catalog) => catalog.delete('__format__'));

    const reopened = await connect(provider);
    expect(await (await reopened.collection('users')).countDocuments({})).toBe(1);
    await reopened.close();
    await withCatalog(provider, (catalog) => {
      expect(catalog.search('__format__')).toEqual({ v: dbFormatVersion() });
    });
  });

  it('refuses a database stamped with a future version, naming both versions', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    await (await db.collection('users')).insertOne({ a: 1 });
    await db.close();
    await withCatalog(provider, (catalog) => catalog.add('__format__', { v: 99 }));

    const v = dbFormatVersion();
    await expect(connect(provider)).rejects.toThrow(
      new RegExp(`version 99.*version ${v}|version ${v}.*version 99`, 's'));
    // ...and refused before mutating anything: the stamp is untouched.
    await withCatalog(provider, (catalog) => {
      expect(catalog.search('__format__')).toEqual({ v: 99 });
    });
  });

  it('reserves the stamp key from the collection namespace', async () => {
    const db = await connect(new MemoryStorageProvider());
    await expect(db.collection('__format__')).rejects.toThrow(/reserved/);
    await expect(db.dropCollection('__format__')).rejects.toThrow(/reserved/);
    await db.close();
  });
});

describe('db: format v1 -> v2 migration', () => {
  /** A database with documents and both index kinds the migration must carry. */
  async function seed(provider) {
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.createIndex({ email: 1 }, { unique: true });
    const ids = [];
    for (let i = 0; i < 40; i++) {
      const _id = new ObjectId();
      ids.push(_id);
      await users.insertOne({ _id, email: `u${i}@example.com`, n: i });
    }
    const notes = await db.collection('notes');
    await notes.insertOne({ body: 'kept' });
    await db.close();
    return ids;
  }

  it('re-keys every collection on open, and the data is all there', async () => {
    const provider = new MemoryStorageProvider();
    const ids = await seed(provider);
    await downgradeToV1(provider, ['users', 'notes']);

    const db = await connect(provider);
    const users = await db.collection('users');

    // The proof that the primary tree was re-keyed rather than merely
    // re-stamped: a v1-keyed tree searched with v2 keys answers nothing,
    // so a point lookup by _id is the whole migration in one assertion.
    expect(await users.countDocuments({})).toBe(40);
    for (const _id of ids) expect((await users.findOne({ _id }))._id.equals(_id)).toBe(true);
    expect((await (await db.collection('notes')).findOne({ body: 'kept' })).body).toBe('kept');

    // The index came across intact, still enforcing.
    expect(await users.findByIndex('email_1', ['u7@example.com'])).toHaveLength(1);
    await expect(users.insertOne({ email: 'u7@example.com' })).rejects.toThrow();

    // And the collection now holds ids v1 had no key for at all.
    await users.insertOne({ _id: 'natural-key', email: 'nat@example.com', n: 100 });
    expect((await users.findOne({ _id: 'natural-key' })).n).toBe(100);
    await db.close();

    await withCatalog(provider, (catalog) => {
      expect(catalog.search('__format__')).toEqual({ v: dbFormatVersion() });
      expect(catalog.search('users').keys).toBe(2);
      expect(catalog.search('notes').keys).toBe(2);
    });
  });

  it('migrates once: a second open re-keys nothing', async () => {
    const provider = new MemoryStorageProvider();
    await seed(provider);
    await downgradeToV1(provider, ['users', 'notes']);

    const first = await connect(provider);
    await first.close();
    let generations, files;
    await withCatalog(provider, (catalog) => {
      generations = catalog.toArray().filter(({ key }) => key !== '__format__')
        .map(({ value }) => value.gen ?? 0);
    });
    files = (await provider.listFiles()).sort();

    // The marker, not the stamp, is what stops the second pass: both
    // opens see a current stamp, and only the entries say whose keys are
    // already the new shape.
    const second = await connect(provider);
    expect(await (await second.collection('users')).countDocuments({})).toBe(40);
    await second.close();

    await withCatalog(provider, (catalog) => {
      expect(catalog.toArray().filter(({ key }) => key !== '__format__')
        .map(({ value }) => value.gen ?? 0)).toEqual(generations);
    });
    expect((await provider.listFiles()).sort()).toEqual(files);
  });

  it('resumes a migration that was interrupted after the stamp went down', async () => {
    // The crash this guards against: the stamp is raised durably FIRST
    // (so no older build can misread what follows), one collection's
    // flip lands, and the process dies before the next. The version now
    // reads current, so nothing but the flag could say there is work
    // left -- and the collection left behind is keyed in a shape no
    // reader derives any more, which is data loss, not a delay.
    const provider = new MemoryStorageProvider();
    await seed(provider);
    // 'users' got its flip in before the crash -- so it is left exactly
    // as this build wrote it, and marked -- while 'notes' is still v1.
    await downgradeToV1(provider, ['notes'], { v: dbFormatVersion(), migrating: true });
    await withCatalog(provider, (catalog) => {
      catalog.add('users', { ...catalog.search('users'), keys: 2 });
    });

    let usersGen;
    await withCatalog(provider, (catalog) => { usersGen = catalog.search('users').gen ?? 0; });

    const db = await connect(provider);
    // The unfinished one was picked up...
    expect((await (await db.collection('notes')).findOne({ body: 'kept' })).body).toBe('kept');
    expect(await (await db.collection('users')).countDocuments({})).toBe(40);
    await db.close();

    await withCatalog(provider, (catalog) => {
      expect(catalog.search('notes').keys).toBe(2);
      // ...and the one already marked was skipped, not rebuilt: a second
      // pass over it would leave a new generation behind.
      expect(catalog.search('users').gen ?? 0).toBe(usersGen);
      // The flag is cleared, so later opens stop offering to do this again.
      expect(catalog.search('__format__')).toEqual({ v: dbFormatVersion() });
    });
  });

  it('a fresh database is never migrated, and its entries carry no marker', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    await (await db.collection('users')).insertOne({ a: 1 });
    await db.close();
    await withCatalog(provider, (catalog) => {
      // Nothing to migrate means nothing to mark: the marker exists only
      // to record that an OLD database's keys were converted.
      expect(catalog.search('users').keys).toBeUndefined();
      expect(catalog.search('users').gen ?? 0).toBe(0);
    });
  });
});
