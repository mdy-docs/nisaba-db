/**
 * Client/connectClient tests: db(name) routes to a genuinely isolated
 * storage scope per name (a real OPFS subdirectory, or an independent
 * in-memory file map), mirroring the cloud service's per-tenant db(name)
 * routing (service/tenant-worker.js's createProvider(tenantId, dbName))
 * minus the tenant axis -- see docs/db-api.md's "Client (multiple named
 * databases)".
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ready } from '../src/nisaba-wasm.js';
import { connectClient, MemoryStorageProvider, OPFSStorageProvider } from '../src/db.js';
import { bootstrapOPFS } from './binjson.suite.js';

await ready();
const { hasOPFS } = await bootstrapOPFS();

describe('Client (in-memory provider)', () => {
  it('db(name) opens independent, isolated databases', async () => {
    const client = await connectClient(new MemoryStorageProvider());
    const a = await client.db('a');
    const b = await client.db('b');

    await (await a.collection('users')).insertOne({ name: 'Ada' });
    await (await b.collection('users')).insertOne({ name: 'Grace' });

    expect((await (await a.collection('users')).find({}).toArray()).map((d) => d.name)).toEqual(['Ada']);
    expect((await (await b.collection('users')).find({}).toArray()).map((d) => d.name)).toEqual(['Grace']);
    await client.close();
  });

  it('db(name) called twice returns the same cached Db instance', async () => {
    const client = await connectClient(new MemoryStorageProvider());
    const a1 = await client.db('a');
    const a2 = await client.db('a');
    expect(a1).toBe(a2);
    await client.close();
  });

  it('close() closes every database the client opened', async () => {
    const client = await connectClient(new MemoryStorageProvider());
    const a = await client.db('a');
    const b = await client.db('b');
    await client.close();
    expect(a.isOpen).toBe(false);
    expect(b.isOpen).toBe(false);
  });

  it('rejects invalid database names, same constraints as a collection name', async () => {
    const client = await connectClient(new MemoryStorageProvider());
    await expect(client.db('a/b')).rejects.toThrow(/Invalid database name/);
    await expect(client.db('')).rejects.toThrow(/Invalid database name/);
    await expect(client.dropDatabase('a/b')).rejects.toThrow(/Invalid database name/);
    await client.close();
  });

  it('listDatabases names every database under the root', async () => {
    const client = await connectClient(new MemoryStorageProvider());
    expect(await client.listDatabases()).toEqual([]);
    await client.db('analytics');
    await client.db('billing');
    expect((await client.listDatabases()).sort()).toEqual(['analytics', 'billing']);
    await client.close();
  });

  it('dropDatabase closes it, removes its files, and answers false the second time', async () => {
    const client = await connectClient(new MemoryStorageProvider());
    const analytics = await client.db('analytics');
    const billing = await client.db('billing');
    await (await analytics.collection('events')).insertOne({ n: 1 });
    await (await billing.collection('invoices')).insertOne({ n: 2 });

    expect(await client.dropDatabase('billing')).toBe(true);
    // Closed BEFORE the storage went: a Db still holding engine contexts
    // into files that no longer exist is the failure this ordering is for.
    expect(billing.isOpen).toBe(false);
    expect(await client.listDatabases()).toEqual(['analytics']);
    expect(await client.dropDatabase('billing')).toBe(false);

    // The neighbour is untouched, and the name is free again -- a fresh,
    // empty database rather than the old one reappearing.
    expect(await (await analytics.collection('events')).countDocuments({})).toBe(1);
    const remade = await client.db('billing');
    expect(await remade.listCollections()).toEqual([]);
    await client.close();
  });

  it('a provider without the two capabilities says so rather than failing oddly', async () => {
    // Every provider in this repository has them. A host's own provider
    // may not, and "listSubProviders is not a function" is not an answer.
    const bare = new MemoryStorageProvider();
    bare.listSubProviders = undefined;
    bare.deleteSubProvider = undefined;
    const client = await connectClient(bare);
    await expect(client.listDatabases()).rejects.toThrow(/cannot list databases/);
    await expect(client.dropDatabase('x')).rejects.toThrow(/cannot drop a database/);
    await client.close();
  });
});

describe.skipIf(!hasOPFS)('Client (OPFS provider)', () => {
  let root = null;
  const dirs = [];
  const base = () => `test-dbclient-${Date.now()}-${Math.random().toString(36).slice(2)}`;

  beforeAll(async () => {
    root = await navigator.storage.getDirectory();
  });

  afterAll(async () => {
    for (const d of dirs) await root.removeEntry(d, { recursive: true }).catch(() => {});
  });

  it('db(name) creates a real, separate OPFS subdirectory per name', async () => {
    const rootName = base();
    dirs.push(rootName);
    const dir = await root.getDirectoryHandle(rootName, { create: true });

    const client = await connectClient(new OPFSStorageProvider(dir));
    const app = await client.db('app');
    const analytics = await client.db('analytics');

    await (await app.collection('users')).insertOne({ name: 'Ada' });
    expect(await (await analytics.collection('users')).find({}).toArray()).toEqual([]);

    // Real, independent subdirectories on disk -- not a namespace prefix inside one shared catalog.
    const appDir = await dir.getDirectoryHandle('app');
    const analyticsDir = await dir.getDirectoryHandle('analytics');
    expect(appDir.name).toBe('app');
    expect(analyticsDir.name).toBe('analytics');

    await client.close();
  });

  it('listDatabases reads the directory, and dropDatabase removes it from disk', async () => {
    const rootName = base();
    dirs.push(rootName);
    const dir = await root.getDirectoryHandle(rootName, { create: true });

    const client = await connectClient(new OPFSStorageProvider(dir));
    const app = await client.db('app');
    const analytics = await client.db('analytics');
    await (await app.collection('users')).insertOne({ name: 'Ada' });
    await (await analytics.collection('users')).insertOne({ name: 'Grace' });
    expect((await client.listDatabases()).sort()).toEqual(['analytics', 'app']);

    expect(await client.dropDatabase('analytics')).toBe(true);
    expect(analytics.isOpen).toBe(false);
    expect(await client.listDatabases()).toEqual(['app']);
    // Really gone from OPFS, not merely forgotten by the client.
    await expect(dir.getDirectoryHandle('analytics')).rejects.toThrow();
    expect(await client.dropDatabase('analytics')).toBe(false);

    // The neighbour still works, which is what a recursive remove of the
    // wrong subtree would have taken with it.
    expect((await (await app.collection('users')).find({}).toArray()).map((d) => d.name)).toEqual(['Ada']);
    await client.close();
  });

  it('reopening a Client against the same root sees the same on-disk databases', async () => {
    const rootName = base();
    dirs.push(rootName);
    const dir1 = await root.getDirectoryHandle(rootName, { create: true });
    const client1 = await connectClient(new OPFSStorageProvider(dir1));
    const db1 = await client1.db('app');
    const users1 = await db1.collection('users');
    await users1.insertOne({ name: 'Ada' });
    await client1.close();

    const dir2 = await root.getDirectoryHandle(rootName, { create: true });
    const client2 = await connectClient(new OPFSStorageProvider(dir2));
    const users = await (await client2.db('app')).collection('users');
    expect((await users.find({}).toArray()).map((d) => d.name)).toEqual(['Ada']);
    await client2.close();
  });
});
