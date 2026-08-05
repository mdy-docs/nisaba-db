/**
 * On-disk format compatibility (docs/format-compatibility.md, docs/
 * roadmap.md P1 #12): every database carries a version stamp in its
 * catalog under the reserved "__format__" key; Db.open() stamps fresh and
 * pre-stamp databases and refuses -- loudly, naming both versions --
 * anything stamped newer than the build understands.
 */
import { describe, it, expect } from 'vitest';
import { ready, BPlusTree, MemoryHandle } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider } from '../src/db.js';

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
      expect(catalog.search('__format__')).toEqual({ v: 1 });
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
      expect(catalog.search('__format__')).toEqual({ v: 1 });
    });
  });

  it('refuses a database stamped with a future version, naming both versions', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connect(provider);
    await (await db.collection('users')).insertOne({ a: 1 });
    await db.close();
    await withCatalog(provider, (catalog) => catalog.add('__format__', { v: 99 }));

    await expect(connect(provider)).rejects.toThrow(/version 99.*version 1|version 1.*version 99/s);
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
