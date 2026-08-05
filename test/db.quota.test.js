/**
 * Storage exhaustion (docs/roadmap.md P1 #9): the most likely real-world
 * browser failure is OPFS throwing QuotaExceededError mid-write. A JS
 * exception from a sync access handle must never propagate up through the
 * WASM frames -- before bridgeHandle (src/nisaba-wasm.js) existed, doing
 * so abandoned C mid-mutation and left a phantom document in the live
 * primary tree (absent from the indexes) that the next successful write
 * committed durably. These tests drive a provider whose handles throw once
 * a byte budget runs out, and assert the failure is instead: surfaced as a
 * normal coded error carrying the real exception as `cause`, rolled back
 * in-process (db.c's mut_begin/mut_end), harmless after space frees up,
 * and invisible after a crash-style reopen.
 */
import { describe, it, expect, vi } from 'vitest';
import { ready, MemoryHandle } from '../src/nisaba-wasm.js';
import { connect, MemoryStorageProvider } from '../src/db.js';

await ready();

/** MemoryStorageProvider whose handles throw QuotaExceededError on any
 * write once `budget` total bytes have been attempted -- the shape real
 * OPFS failures take, at a deterministic point. */
class QuotaProvider extends MemoryStorageProvider {
  constructor() {
    super();
    this.budget = Infinity;
    this.attempted = 0;
  }

  async openFile(name, opts) {
    const handle = await super.openFile(name, opts);
    const provider = this;
    const write = handle.write.bind(handle);
    handle.write = (buf, o) => {
      provider.attempted += buf.byteLength ?? buf.length ?? 0;
      if (provider.attempted > provider.budget) {
        const err = new Error('write exceeds storage quota');
        err.name = 'QuotaExceededError';
        throw err;
      }
      return write(buf, o);
    };
    return handle;
  }
}

async function seed(provider, count = 20) {
  const db = await connect(provider);
  const users = await db.collection('users');
  await users.createIndex({ team: 1 }, { name: 'teamIdx' });
  for (let i = 0; i < count; i++) {
    await users.insertOne({ i, team: `t${i % 3}`, pad: 'x'.repeat(200) });
  }
  return { db, users };
}

/** Crash-style reopen: copy the raw bytes into a fresh provider, as if the
 * process died right here and a new one opened the same files. */
async function reopenFromBytes(provider) {
  const p2 = new MemoryStorageProvider();
  for (const [name, handle] of provider._files) {
    p2._files.set(name, new MemoryHandle(handle.toBytes()));
  }
  const db = await connect(p2);
  return { db, users: await db.collection('users') };
}

describe('db: storage quota exhaustion', () => {
  it('a quota failure mid-insert surfaces with cause, rolls back in-process, and never persists a phantom', async () => {
    const provider = new QuotaProvider();
    const { db, users } = await seed(provider);
    const before = await users.countDocuments({});

    provider.budget = provider.attempted + 150; // the next insert dies mid-multi-file write

    let err = null;
    try {
      for (let i = 100; i < 130; i++) {
        await users.insertOne({ i, team: 'q', pad: 'y'.repeat(300) });
      }
    } catch (e) {
      err = e;
    }
    expect(err).toBeTruthy();
    expect(err.cause?.name).toBe('QuotaExceededError'); // the real story rides along

    // In-process rollback: no phantom in the primary tree or any index.
    expect(await users.countDocuments({})).toBe(before);
    expect(await users.find({ team: 'q' }).toArray()).toEqual([]);
    expect(await users.findOne({ i: 100 })).toBeNull();

    // Space frees up (user cleared other site data): the db just works,
    // no reopen required.
    provider.budget = Infinity;
    await users.insertOne({ i: 999, team: 'post', pad: 'z' });
    expect(await users.countDocuments({})).toBe(before + 1);

    // Crash-style reopen of those bytes: still no phantom, indexes intact.
    const reopened = await reopenFromBytes(provider);
    expect(await reopened.users.countDocuments({})).toBe(before + 1);
    expect(await reopened.users.find({ team: 'q' }).toArray()).toEqual([]);
    expect((await reopened.users.find({ team: 'post' }).toArray())).toHaveLength(1);
    await db.close();
    await reopened.db.close();
  });

  it('a quota failure mid-updateMany rolls the whole batch back', async () => {
    const provider = new QuotaProvider();
    const { db, users } = await seed(provider, 30);

    provider.budget = provider.attempted + 400; // dies partway through the batch
    await expect(
      users.updateMany({}, { $set: { pad: 'Z'.repeat(500), team: 'updated' } })
    ).rejects.toMatchObject({ cause: { name: 'QuotaExceededError' } });

    // All-or-nothing: no document shows the update, in-process or reopened.
    expect(await users.countDocuments({ team: 'updated' })).toBe(0);
    const reopened = await reopenFromBytes(provider);
    expect(await reopened.users.countDocuments({ team: 'updated' })).toBe(0);
    expect(await reopened.users.countDocuments({})).toBe(30);
    await db.close();
    await reopened.db.close();
  });

  it('storageEstimate(): null without platform support, passthrough with it', async () => {
    const db = await connect(new MemoryStorageProvider());
    // Plain Node (or a browser main thread without storage access).
    if (typeof navigator === 'undefined' || !navigator.storage?.estimate) {
      expect(await db.storageEstimate()).toBeNull();
    }
    // Platform provides it: passed straight through.
    vi.stubGlobal('navigator', { storage: { estimate: async () => ({ usage: 1234, quota: 5678 }) } });
    try {
      expect(await db.storageEstimate()).toEqual({ usage: 1234, quota: 5678 });
    } finally {
      vi.unstubAllGlobals();
    }
    await db.close();
  });
});
