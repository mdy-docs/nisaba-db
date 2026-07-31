/**
 * One open handle per file, enforced in Node (docs/roadmap.md P0 #4).
 *
 * A browser's OPFS sync access handle is exclusive per file: a second
 * createSyncAccessHandle on a file that already has one throws
 * NoModificationAllowedError, and so does removing that file. Neither
 * MemoryStorageProvider nor the node-opfs shim enforces that, so a leaked
 * handle is invisible to the whole Node suite and only surfaces in
 * test/*.browser.test.js -- which is exactly how compact() came to leak
 * every file it pre-opened for C, and leave three browser tests red for
 * as long as it took someone to run them.
 *
 * This file puts the browser's rule into the Node suite, the way
 * test/db.quota.test.js puts OPFS's quota failures there: a provider
 * wrapper that refuses the second open, and refuses to delete a file whose
 * handle is still live. It covers the operations that open files in bulk
 * -- open, compact, and compact's own error path -- so a handle the engine
 * forgets to give back fails here, seconds after it is written, rather
 * than in a browser run later.
 */
import { describe, it, expect } from 'vitest';
import { ready } from '../wasm/nisaba-wasm.js';
import { connect, MemoryStorageProvider, ObjectId } from '../src/db.js';

await ready();

/** The error a browser raises for both violations. */
function exclusivityError(name, what) {
  const err = new Error(
    `${what} "${name}": Access Handles cannot be created if there is another open Access Handle`);
  err.name = 'NoModificationAllowedError';
  return err;
}

/** MemoryStorageProvider with the browser's exclusivity rule: at most one
 * live handle per file, and no deleting a file that has one. `live` is the
 * set of currently-open names, so a test can assert what is held as well
 * as what was refused. */
class ExclusiveProvider extends MemoryStorageProvider {
  constructor() {
    super();
    this.live = new Set();
    this.failOpen = new Set(); // names whose next open throws, once
  }

  async openFile(name, opts) {
    if (this.live.has(name)) throw exclusivityError(name, 'open');
    if (this.failOpen.delete(name)) {
      const err = new Error(`simulated open failure: ${name}`);
      err.name = 'NotFoundError';
      throw err;
    }
    const handle = await super.openFile(name, opts);
    // super hands back the SAME handle object for a given name every
    // time, so wrap its close exactly once.
    if (!handle._exclusiveClose) {
      const close = handle.close.bind(handle);
      handle._exclusiveClose = true;
      handle.close = () => { this.live.delete(name); return close(); };
    }
    this.live.add(name);
    return handle;
  }

  async deleteFile(name) {
    if (this.live.has(name)) throw exclusivityError(name, 'delete');
    return super.deleteFile(name);
  }
}

const point = (lng, lat) => ({ type: 'Point', coordinates: [lng, lat] });

/** All three index kinds, churned so compact() has real garbage to drop --
 * seedUsers from test/db.compact.test.js, smaller. */
async function seedUsers(provider, count = 24) {
  const db = await connect(provider);
  const users = await db.collection('users');
  await users.createIndex({ team: 1 }, { name: 'teamIdx' });
  await users.createIndex({ bio: 'text' }, { name: 'bioIdx' });
  await users.createIndex({ loc: '2dsphere' }, { name: 'locIdx' });
  const docs = [];
  for (let i = 0; i < count; i++) {
    const doc = {
      _id: new ObjectId(),
      i,
      team: i % 3 === 0 ? 'core' : 'infra',
      bio: `person number${i} enjoys writing tests`,
      loc: point(i * 0.01, i * 0.01)
    };
    await users.insertOne(doc);
    docs.push(doc);
  }
  const survivors = [];
  for (const doc of docs) {
    if (doc.i % 4 === 0 && doc.i > 0) await users.deleteOne({ _id: doc._id });
    else survivors.push(doc);
  }
  return { db, users, survivors };
}

describe('db: one open handle per file, as a browser enforces it', () => {
  it('compact() gives back every handle it pre-opened for C, so the adopt step can re-open by name', async () => {
    const provider = new ExclusiveProvider();
    const { db, users, survivors } = await seedUsers(provider);

    // The pre-open registers the whole new generation in the namespace
    // scope; the adopt immediately after re-opens those same names. A
    // leak here is what NoModificationAllowedError reports in a browser.
    const stats = await users.compact();
    expect(stats.generation).toBe(1);

    // Only the live generation is held: primary + journal + 5 index files
    // (equality, 3 text roles, geo) -- and the catalog.
    expect([...provider.live].sort()).toEqual([
      '__catalog__.bj',
      'g1-coll-users-journal.bj',
      'g1-coll-users.bj',
      'g1-idx-users-bioIdx-documents.bj',
      'g1-idx-users-bioIdx-lengths.bj',
      'g1-idx-users-bioIdx-terms.bj',
      'g1-idx-users-locIdx.bj',
      'g1-idx-users-teamIdx.bj'
    ]);

    // The old generation was deletable, which it would not have been with
    // its replacements still open (they share no name, but a leak would
    // have failed the adopt long before this).
    expect(await provider.listFiles()).not.toContain('coll-users.bj');

    expect(await users.countDocuments({})).toBe(survivors.length);
    expect((await users.find({ team: 'core' }).toArray()).length)
      .toBe(survivors.filter(d => d.team === 'core').length);
    expect((await users.find({ $text: { $search: 'tests' } }).toArray()).length)
      .toBe(survivors.length);

    await db.close();
    expect([...provider.live]).toEqual([]);
  });

  it('compacts repeatedly, and a reopened Db can take the handles back', async () => {
    const provider = new ExclusiveProvider();
    const { db, users, survivors } = await seedUsers(provider);
    for (let gen = 1; gen <= 3; gen++) {
      expect((await users.compact()).generation).toBe(gen);
    }
    await db.close();
    expect([...provider.live]).toEqual([]);

    // Db.open()'s sweep and the collection open both re-open files the
    // previous Db held; either one holding on would fail here.
    const db2 = await connect(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(survivors.length);
    await db2.close();
    expect([...provider.live]).toEqual([]);
  });

  it('a compaction that fails part-way through its pre-open closes what it opened, and deletes it', async () => {
    const provider = new ExclusiveProvider();
    const { db, users, survivors } = await seedUsers(provider);

    // The journal is the last file the plan names, so this fails with the
    // primary and all five index files already open -- the pre-flip error
    // path, which must close them before it can delete them.
    provider.failOpen.add('g1-coll-users-journal.bj');
    await expect(users.compact()).rejects.toThrow(/simulated open failure/);

    expect((await provider.listFiles()).filter(f => f.startsWith('g1-'))).toEqual([]);
    expect([...provider.live].some(f => f.startsWith('g1-'))).toBe(false);

    // The old generation is still live and still correct...
    expect(await users.countDocuments({})).toBe(survivors.length);
    await users.insertOne({ _id: new ObjectId(), i: 999, team: 'core', bio: 'later', loc: point(1, 1) });

    // ...and the generation number was not consumed, so the retry writes
    // exactly the names the failed attempt left behind -- which it could
    // not do if any of them still existed with a handle on it.
    expect((await users.compact()).generation).toBe(1);
    expect(await users.countDocuments({})).toBe(survivors.length + 1);
    await db.close();
    expect([...provider.live]).toEqual([]);
  });
});
