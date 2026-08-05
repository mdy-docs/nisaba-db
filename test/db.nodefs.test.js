/**
 * NodeFSStorageProvider (docs/roadmap.md P0 #4): the whole engine against
 * real files through plain node:fs -- no OPFS shim. Covers the two things
 * the provider adds over MemoryStorageProvider: durability across
 * close/reopen of real files (fsync-backed flush), and the advisory
 * per-directory lock (live holder refused, dead holder reclaimed).
 */
import { describe, it, expect } from 'vitest';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { ready, ObjectId } from '../src/nisaba-wasm.js';
import { connect, connectClient, NodeFSStorageProvider } from '../src/db-node.js';

await ready();

const roots = [];
function tmpRoot() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-test-'));
  roots.push(dir);
  return dir;
}
process.on('exit', () => {
  for (const dir of roots) fs.rmSync(dir, { recursive: true, force: true });
});

const point = (lng, lat) => ({ type: 'Point', coordinates: [lng, lat] });

describe('db: NodeFSStorageProvider', () => {
  it('full CRUD + every index kind against real files, durable across reopen', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.createIndex({ team: 1 }, { name: 'teamIdx' });
    await users.createIndex({ bio: 'text' }, { name: 'bioIdx' });
    await users.createIndex({ loc: '2dsphere' }, { name: 'locIdx' });
    for (let i = 0; i < 40; i++) {
      await users.insertOne({ i, team: `t${i % 4}`, bio: `person number${i}`, loc: point(i * 0.01, 0) });
    }
    await users.updateMany({ team: 't0' }, { $set: { team: 'zero' } });
    await users.deleteMany({ team: 't1' });
    expect(await users.countDocuments({})).toBe(30);
    await db.close();
    await provider.close();

    // Real bytes on disk, reopened by a fresh provider.
    expect(fs.readdirSync(root).some((f) => f.startsWith('coll-users'))).toBe(true);
    const provider2 = new NodeFSStorageProvider(root);
    const db2 = await connect(provider2);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(30);
    expect(await users2.find({ team: 'zero' }).toArray()).toHaveLength(10);
    expect((await users2.find({ $text: { $search: 'number3' } }).toArray()).length).toBeGreaterThan(0);
    const near = await users2.find({ loc: { $near: { $geometry: point(0.05, 0), $maxDistance: 3000 } } }).toArray();
    expect(near.length).toBeGreaterThan(0);
    await db2.close();
    await provider2.close();
  });

  it('compaction swaps generations of real files and survives reopen', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const db = await connect(provider);
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    for (let i = 0; i < 50; i++) await users.insertOne({ i, team: 'a', pad: 'x'.repeat(200) });
    await users.updateMany({}, { $set: { pad: 'y'.repeat(50) } });

    const stats = await users.compact();
    expect(stats.generation).toBe(1);
    expect(stats.bytesFreed).toBeGreaterThan(0);
    expect(fs.readdirSync(root).some((f) => f.startsWith('g1-coll-users'))).toBe(true);
    expect(fs.readdirSync(root).some((f) => f === 'coll-users.bj')).toBe(false); // old gen deleted
    await db.close();
    await provider.close();

    const provider2 = new NodeFSStorageProvider(root);
    const db2 = await connect(provider2);
    expect(await (await db2.collection('users')).countDocuments({})).toBe(50);
    await db2.close();
    await provider2.close();
  });

  it('refuses a directory locked by a live process, with a clear message', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const db = await connect(provider);

    const second = new NodeFSStorageProvider(root);
    await expect(connect(second)).rejects.toThrow(/locked by pid \d+/);

    await db.close();
    await provider.close(); // releases the lock...
    const third = new NodeFSStorageProvider(root);
    const db3 = await connect(third); // ...so a later open succeeds
    await db3.close();
    await third.close();
  });

  it('reclaims a stale lock left by a dead process', async () => {
    const root = tmpRoot();
    // A pid that cannot be alive: past kernel defaults, and no live claim.
    fs.mkdirSync(root, { recursive: true });
    fs.writeFileSync(path.join(root, '.nisaba-lock'), '999999999');

    const provider = new NodeFSStorageProvider(root);
    const db = await connect(provider);
    await (await db.collection('t')).insertOne({ ok: true });
    await db.close();
    await provider.close();
  });

  it('connectClient: each named database is an isolated, separately locked subdirectory', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const client = await connectClient(provider);
    const a = await client.db('alpha');
    const b = await client.db('beta');
    await (await a.collection('t')).insertOne({ from: 'alpha' });
    await (await b.collection('t')).insertOne({ from: 'beta' });

    expect(await (await a.collection('t')).countDocuments({})).toBe(1);
    expect((await (await b.collection('t')).findOne({})).from).toBe('beta');
    expect(fs.existsSync(path.join(root, 'alpha', '__catalog__.bj'))).toBe(true);
    expect(fs.existsSync(path.join(root, 'beta', '.nisaba-lock'))).toBe(true);

    await client.close();
    await provider.close();
    // Children's locks were released with the parent.
    expect(fs.existsSync(path.join(root, 'alpha', '.nisaba-lock'))).toBe(false);
  });

  it('rejects path-traversal file and database names', async () => {
    const provider = new NodeFSStorageProvider(tmpRoot());
    await expect(provider.openFile('../escape.bj', { create: true })).rejects.toThrow(/Invalid file name/);
    await expect(provider.openFile('a/b.bj', { create: true })).rejects.toThrow(/Invalid file name/);
    await expect(provider.subProvider('..')).rejects.toThrow(/Invalid database name/);
    // Same rules dropping as making one: a name one accepts and the other
    // refuses is a database that can be created and never removed.
    await expect(provider.deleteSubProvider('..')).rejects.toThrow(/Invalid database name/);
    await expect(provider.deleteSubProvider('a/b')).rejects.toThrow(/Invalid database name/);
    await provider.close();
  });

  it('listDatabases sees what is on disk, without opening or locking any of it', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const client = await connectClient(provider);
    await (await (await client.db('alpha')).collection('t')).insertOne({ x: 1 });
    await (await (await client.db('beta')).collection('t')).insertOne({ x: 2 });
    expect((await client.listDatabases()).sort()).toEqual(['alpha', 'beta']);
    await client.close();
    await provider.close();

    // A fresh client over the same root sees them without having opened
    // one -- the listing is the directory's, not this session's memory of
    // what it made. And nothing was locked to find out: the lock files
    // are gone, and listing does not put them back.
    const again = new NodeFSStorageProvider(root);
    const client2 = await connectClient(again);
    expect((await client2.listDatabases()).sort()).toEqual(['alpha', 'beta']);
    expect(fs.existsSync(path.join(root, 'alpha', '.nisaba-lock'))).toBe(false);
    await client2.close();
    await again.close();
  });

  it('dropDatabase deletes the directory, releases its lock, and lets the name be reused', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const client = await connectClient(provider);
    const beta = await client.db('beta');
    await (await beta.collection('t')).insertOne({ from: 'beta' });
    await (await (await client.db('alpha')).collection('t')).insertOne({ from: 'alpha' });
    expect(fs.existsSync(path.join(root, 'beta', '.nisaba-lock'))).toBe(true);

    expect(await client.dropDatabase('beta')).toBe(true);
    expect(beta.isOpen).toBe(false);              // closed before its files went
    expect(fs.existsSync(path.join(root, 'beta'))).toBe(false);
    expect(await client.listDatabases()).toEqual(['alpha']);
    expect(await client.dropDatabase('beta')).toBe(false);   // already gone

    // Reusable, which is what proves the lock came back: a stale fd on
    // the old lock file would make this the "locked by pid" refusal.
    const remade = await client.db('beta');
    expect(await (await remade.collection('t')).countDocuments({})).toBe(0);
    await (await remade.collection('t')).insertOne({ from: 'beta again' });
    expect((await (await remade.collection('t')).findOne({})).from).toBe('beta again');

    // The neighbour was untouched throughout.
    expect((await (await (await client.db('alpha')).collection('t')).findOne({})).from).toBe('alpha');
    await client.close();
    await provider.close();
  });

  it('refuses to drop a database somebody else has open', async () => {
    const root = tmpRoot();
    const provider = new NodeFSStorageProvider(root);
    const client = await connectClient(provider);
    await (await (await client.db('served')).collection('t')).insertOne({ x: 1 });
    await client.close();          // this client lets go of it entirely
    await provider.close();

    // Opened DIRECTLY, the way nisaba-server opens a database directory:
    // one process per directory, and it never came through the parent.
    const direct = new NodeFSStorageProvider(path.join(root, 'served'));
    const served = await connect(direct);

    const other = new NodeFSStorageProvider(root);
    const client2 = await connectClient(other);
    await expect(client2.dropDatabase('served')).rejects.toThrow(/is locked by pid/);
    // And the refusal left it exactly as it was -- files, and readable.
    expect(fs.existsSync(path.join(root, 'served', '__catalog__.bj'))).toBe(true);
    expect(await (await served.collection('t')).countDocuments({})).toBe(1);

    await served.close();
    await direct.close();
    // Once the holder has gone, the same call succeeds.
    expect(await client2.dropDatabase('served')).toBe(true);
    expect(fs.existsSync(path.join(root, 'served'))).toBe(false);
    await client2.close();
    await other.close();
  });

  it('ObjectIds round-trip byte-identically through real files', async () => {
    const provider = new NodeFSStorageProvider(tmpRoot());
    const db = await connect(provider);
    const coll = await db.collection('ids');
    const _id = new ObjectId();
    await coll.insertOne({ _id, tag: 'x' });
    await db.close();
    await provider.close();

    const provider2 = new NodeFSStorageProvider(provider._dir);
    const db2 = await connect(provider2);
    const found = await (await db2.collection('ids')).findOne({ _id });
    expect(found._id.toHexString()).toBe(_id.toHexString());
    await db2.close();
    await provider2.close();
  });
});
