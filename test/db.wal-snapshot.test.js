/**
 * Replication roadmap step 3 (docs/replicaton-roadmap.md): snapshots and
 * log compaction -- WalDb.snapshot()/restoreLatestSnapshot() in
 * src/db-wal.js on top of binjson-structures' SnapshotStore. Covers: the
 * log-bounding compaction, generation metadata and integrity, generation
 * supersession, writes continuing across a snapshot, snapshot + log-suffix
 * restore equivalence, boundary-exact restore, index survival through
 * restore, and the dropCollection barrier (no resurrection on replay).
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId, EntryLog, encode } from '../wasm/nisaba-wasm.js';
import { MemoryStorageProvider } from '../src/db.js';
import { readFileSync } from 'node:fs';
import {
  connectWal, restoreLatestSnapshot, WAL_FILE, SNAP_PREFIX, LEGACY_SNAP_PREFIX
} from '../src/db-wal.js';

await ready();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));

/** Full observable state of a collection, for equivalence checks. */
async function stateOf(db, name) {
  const col = await db.collection(name);
  return (await col.find({}, { sort: { _id: 1 } }).toArray())
    .map((d) => ({ ...d, _id: d._id.toHexString() }));
}

describe('WAL snapshots: log compaction', () => {
  it('snapshot() empties the log at the boundary and writes continue beyond it', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 10; i++) await users.insertOne({ _id: oid(i), i });
    expect(db.log.lastIndex).toBe(10);

    const snap = await db.snapshot();
    expect(snap.lastIncludedIndex).toBe(10);
    expect(snap.lastIncludedTerm).toBe(1);
    expect(db.log.baseIndex).toBe(10);   // compacted through the boundary
    expect(db.log.lastIndex).toBe(10);   // empty
    expect(db.log.currentTerm).toBe(1);  // hard state carried over

    // Writes keep flowing, with contiguous indexes beyond the boundary.
    await users.insertOne({ _id: oid(11), i: 11 });
    expect(db.log.lastIndex).toBe(11);
    expect(await users.countDocuments({})).toBe(11);
    await db.close();

    // Reopen adopts the generation log (the legacy file is gone) and
    // replays nothing it already has.
    const files = await provider.listFiles();
    expect(files).not.toContain(WAL_FILE);
    const db2 = await connectWal(provider);
    expect(await (await db2.collection('users')).countDocuments({})).toBe(11);
    expect(db2.log.baseIndex).toBe(10);
    expect(db2.log.lastIndex).toBe(11);
    await db2.close();
  });

  it('the generation is complete, CRC-verified, and supersedes its predecessor', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });
    await users.insertMany([{ _id: oid(1), team: 'a' }, { _id: oid(2), team: 'b' }]);

    const snap1 = await db.snapshot();
    expect(snap1.gen).toBe(1);
    // catalog + primary + one index file
    expect(snap1.files.length).toBe(3);
    expect(snap1.config.live.map((f) => f.name)).toContain('__catalog__.bj');
    expect(await db.snapshots.verify()).toBe(true);

    await users.insertOne({ _id: oid(3), team: 'a' });
    const snap2 = await db.snapshot();
    expect(snap2.gen).toBe(2);
    // createIndex(1) + insertMany(2) + insertOne(1) = 4 logged entries
    expect(snap2.lastIncludedIndex).toBe(4);

    // Generation 1's data files and paired log are gone; only gen 2 remains.
    const files = await provider.listFiles();
    expect(files.filter((f) => f.startsWith('__snap__-1')).length).toBe(0);
    expect(files).toContain('__snap__-log-2.bj');
    expect(files).not.toContain('__snap__-log-1.bj');
    await db.close();
  });

  it('a snapshot mid-history plus the remaining log suffix equals full history', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    await users.createIndex({ team: 1 });

    for (let i = 1; i <= 5; i++) await users.insertOne({ _id: oid(i), team: i % 2 ? 'a' : 'b', i });
    await db.snapshot();

    // History continues past the boundary: updates, deletes, an upsert.
    await users.updateMany({ team: 'a' }, { $set: { seen: true } });
    await users.deleteOne({ _id: oid(2) });
    await users.updateOne({ tag: 'x' }, { $set: { i: 99, team: 'a' } }, { upsert: true });
    const want = await stateOf(db, 'users');
    const wantIndex = (await users.find({ team: 'a' }).toArray()).length;
    await db.close();

    // Wipe the live database files and restore from the snapshot; the log
    // suffix beyond the boundary replays on open.
    await restoreLatestSnapshot(provider);
    const db2 = await connectWal(provider);
    expect(await stateOf(db2, 'users')).toEqual(want);
    expect((await (await db2.collection('users')).find({ team: 'a' }).toArray()).length).toBe(wantIndex);
    await db2.close();
  });

  it('deleting the log files first gives a boundary-exact restore', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    await users.insertOne({ _id: oid(4), i: 4 }); // beyond the boundary
    await db.close();

    await restoreLatestSnapshot(provider);
    for (const f of await provider.listFiles()) {
      if (/^__snap__-log-\d+\.bj$/.test(f) || f === WAL_FILE) await provider.deleteFile(f);
    }
    const db2 = await connectWal(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(3); // exactly the boundary
    expect(await users2.findOne({ _id: oid(4) })).toBeNull();
    // The fresh log starts at the snapshot boundary, ready for new entries.
    expect(db2.log.baseIndex).toBe(3);
    await users2.insertOne({ _id: oid(5), i: 5 });
    expect(db2.log.lastIndex).toBe(4);
    await db2.close();
  });

  it('every index kind survives snapshot + restore with its applied index', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const places = await db.collection('places');
    await places.createIndex({ team: 1 });
    await places.createIndex({ body: 'text' });
    await places.createIndex({ location: '2dsphere' });
    await places.insertOne({
      _id: oid(1), team: 'red', body: 'the quick brown fox',
      location: { type: 'Point', coordinates: [151.2, -33.8] }
    });
    const snap = await db.snapshot();
    expect(snap.files.length).toBe(7); // catalog + primary + 1 + 3 + 1 index files
    await db.close();

    await restoreLatestSnapshot(provider);
    const db2 = await connectWal(provider);
    const p2 = await db2.collection('places');
    expect((await p2.find({ team: 'red' }).toArray()).length).toBe(1);
    expect((await p2.find({ location: { $near: { $geometry: { type: 'Point', coordinates: [151.2, -33.8] }, $maxDistance: 1000 } } }).toArray()).length).toBe(1);
    expect(await p2.appliedIndex()).toBe(snap.lastIncludedIndex);
    await db2.close();
  });
});

describe('WAL snapshots: dropCollection barrier', () => {
  it('a dropped collection stays dropped across reopen (no replay resurrection)', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    const logs = await db.collection('logs');
    await users.insertMany([{ _id: oid(1) }, { _id: oid(2) }]);
    await logs.insertOne({ _id: oid(3), keep: true });

    expect(await db.dropCollection('users')).toBe(true);
    expect(await db.listCollections()).toEqual(['logs']);
    // The drop itself is a logged command: replay re-drops after any
    // transient resurrection, so no barrier snapshot is needed.
    await db.close();

    const db2 = await connectWal(provider);
    expect(await db2.listCollections()).toEqual(['logs']);
    expect((await (await db2.collection('logs')).findOne({ _id: oid(3) })).keep).toBe(true);
    await db2.close();
  });

  it('dropping a collection that does not exist is false and takes no snapshot', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    await (await db.collection('users')).insertOne({ n: 1 });
    expect(await db.dropCollection('nope')).toBe(false);
    expect(db.snapshots.latest).toBeNull();
    await db.close();
  });

  it('writes to a recreated collection after a drop replay correctly', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    await (await db.collection('users')).insertOne({ _id: oid(1), era: 'old' });
    await db.dropCollection('users');
    const again = await db.collection('users');
    await again.insertOne({ _id: oid(2), era: 'new' });
    await db.close();

    const db2 = await connectWal(provider);
    const users = await db2.collection('users');
    expect(await users.countDocuments({})).toBe(1);
    expect(await users.findOne({ _id: oid(1) })).toBeNull();
    expect((await users.findOne({ _id: oid(2) })).era).toBe('new');
    await db2.close();
  });
});

describe('WAL snapshots: crash windows', () => {
  it('a torn paired log falls back to its predecessor', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    await users.insertOne({ _id: oid(4), i: 4 });
    await db.close();

    // Forge a crash mid-way through writing the NEXT generation's log: a
    // garbage file at the successor name. Adoption must skip it and open
    // the durable gen-1 log.
    const torn = await provider.openFile('__snap__-log-2.bj', { create: true });
    torn.write(new Uint8Array([1, 2, 3, 4, 5]), { at: 0 });
    torn.flush();
    await torn.close();

    const db2 = await connectWal(provider);
    expect(await (await db2.collection('users')).countDocuments({})).toBe(4);
    expect(db2.log.baseIndex).toBe(3);
    expect(db2.log.lastIndex).toBe(4);
    await db2.close();
  });

  it('a torn manifest is not a snapshot: the generation is refused and swept', async () => {
    // The manifest is written LAST and its validity IS the commit, so a
    // generation whose manifest lost its tail must read as one that never
    // happened -- and its files must be swept rather than left to
    // accumulate. Nothing tested this before, which meant the CRC that
    // makes the whole protocol work was never observed to do anything.
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    await db.close();

    const manifest = '__snap__-1.manifest.bj';
    const h = await provider.openFile(manifest, { create: false });
    h.truncate(h.getSize() - 1);      // lose one byte of the CRC
    h.flush();
    await h.close();

    const db2 = await connectWal(provider);
    expect(db2.snapshots.latest).toBeNull();
    // Swept: the refused generation leaves nothing behind for the next
    // open to reconsider.
    const left = await provider.listFiles();
    expect(left.filter((n) => n.startsWith('__snap__-1.') || n.startsWith('__snap__-1-'))).toEqual([]);
    // The data itself is untouched -- a snapshot is derived state.
    expect(await (await db2.collection('users')).countDocuments({})).toBe(3);
    await db2.close();
  });

  it('a generation missing a data file is refused, not adopted with a hole', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    await users.insertOne({ _id: oid(1), i: 1 });
    await db.snapshot();                       // generation 1
    await users.insertOne({ _id: oid(2), i: 2 });
    await db.snapshot();                       // generation 2 supersedes it
    await db.close();

    // Generation 2's manifest validates, but one of its files is gone.
    // Presence at the recorded size is part of adoption, not something
    // discovered later when a restore reads a hole.
    const gone = (await provider.listFiles()).find((n) => n.startsWith('__snap__-2-'));
    expect(gone).toBeTruthy();
    await provider.deleteFile(gone);

    const db2 = await connectWal(provider);
    expect(db2.snapshots.latest).toBeNull();   // 1 was already swept by 2
    expect(await (await db2.collection('users')).countDocuments({})).toBe(2);
    await db2.close();
  });

  it('verify() catches a corrupted generation file that is the right length', async () => {
    // Length alone cannot see this, which is the entire reason a CRC is
    // carried per file. Same check the replicated install path runs
    // against a leader's manifest (snapstore.h's sst_check_files).
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    const snap = await db.snapshot();
    expect(await db.snapshots.verify()).toBe(true);

    const victim = snap.files[0];
    const h = await provider.openFile(victim.name, { create: false });
    const buf = new Uint8Array(8);
    h.read(buf, { at: 0 });
    buf[0] ^= 0xff;                     // same length, different bytes
    h.write(buf, { at: 0 });
    h.flush();
    await h.close();

    await expect(db.snapshots.verify()).rejects.toThrow(
      new RegExp(`Snapshot file ${victim.role} failed its checksum`)
    );
    await db.close();
  });

  it('entries beyond the boundary that never applied replay after reopen', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    await db.close();

    // Forge the crash window on the generation log: durable but unapplied.
    const log = new EntryLog(await provider.openFile('__snap__-log-1.bj', { create: false }));
    await log.open();
    log.append(log.currentTerm, encode({ c: 'users', op: 'i', doc: { _id: oid(9), from: 'log' } }));
    log.sync();
    await log.close();

    const db2 = await connectWal(provider);
    const users2 = await db2.collection('users');
    expect(await users2.countDocuments({})).toBe(4);
    expect((await users2.findOne({ _id: oid(9) })).from).toBe('log');
    expect(await users2.appliedIndex()).toBe(4);
    await db2.close();
  });
});

/**
 * The canonical snapshot prefix (docs/s3-backup.md, "One snapshot, two
 * hosts", step 1): this host writes the C server's prefix, and a root
 * written under the old '__snap' default is adopted into it at open —
 * through the store's own install machinery, so the migration inherits
 * the CRC verification and manifest-last commit it already trusts.
 */
describe('canonical snapshot prefix', () => {
  const isLegacyName = (n) => /^__snap-/.test(n);
  const isCanonicalName = (n) => n.startsWith(`${SNAP_PREFIX}-`);

  it('SNAP_PREFIX is the C server\'s REPLICA_SNAP_PREFIX, verbatim', () => {
    expect(SNAP_PREFIX).toBe('__snap__');
    // The tripwire: the C constant is the owner of this string. If the
    // server ever renames it, this is the test that says the two hosts
    // no longer write the same artifact.
    const replica = readFileSync(new URL('../server/replica.c', import.meta.url), 'utf8');
    expect(replica).toMatch(/#define\s+REPLICA_SNAP_PREFIX\s+"__snap__"/);
  });

  it('a fresh root snapshots under the canonical prefix only', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider);
    const users = await db.collection('users');
    for (let i = 1; i <= 3; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    await db.close();

    const names = await provider.listFiles();
    expect(names.some(isCanonicalName)).toBe(true);
    expect(names.some(isLegacyName)).toBe(false);
  });

  it('a legacy root is adopted at open: same data, same boundary, canonical files', async () => {
    const provider = new MemoryStorageProvider();
    let db = await connectWal(provider, { snapshotPrefix: LEGACY_SNAP_PREFIX });
    let users = await db.collection('users');
    for (let i = 1; i <= 5; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    // A suffix beyond the boundary: replay must survive the migration.
    for (let i = 6; i <= 8; i++) await users.insertOne({ _id: oid(i), i });
    const want = await stateOf(db, 'users');
    await db.close();
    expect((await provider.listFiles()).some(isLegacyName)).toBe(true);

    db = await connectWal(provider);   // the canonical default migrates
    expect(await stateOf(db, 'users')).toEqual(want);
    expect(db.log.baseIndex).toBe(5);              // the legacy boundary, kept
    expect(db.snapshots.latest.lastIncludedIndex).toBe(5);
    const names = await provider.listFiles();
    expect(names.some(isLegacyName)).toBe(false);  // nothing legacy survives
    expect(names.some(isCanonicalName)).toBe(true);

    // Fully operational after adoption: write, snapshot, reopen.
    users = await db.collection('users');
    await users.insertOne({ _id: oid(9), i: 9 });
    await db.snapshot();
    await db.close();
    db = await connectWal(provider);
    expect((await stateOf(db, 'users')).length).toBe(9);
    await db.close();
  });

  it('reopening a migrated root is a fixpoint', async () => {
    const provider = new MemoryStorageProvider();
    let db = await connectWal(provider, { snapshotPrefix: LEGACY_SNAP_PREFIX });
    await (await db.collection('users')).insertOne({ _id: oid(1), i: 1 });
    await db.snapshot();
    await db.close();

    db = await connectWal(provider);
    await db.close();
    const first = (await provider.listFiles()).sort();
    db = await connectWal(provider);
    await db.close();
    expect((await provider.listFiles()).sort()).toEqual(first);
  });

  it('restoreLatestSnapshot reads a legacy root without a migration first', async () => {
    const provider = new MemoryStorageProvider();
    const db = await connectWal(provider, { snapshotPrefix: LEGACY_SNAP_PREFIX });
    const users = await db.collection('users');
    for (let i = 1; i <= 5; i++) await users.insertOne({ _id: oid(i), i });
    await db.snapshot();
    const want = await stateOf(db, 'users');
    await db.close();

    const snap = await restoreLatestSnapshot(provider);   // canonical default
    expect(snap.lastIncludedIndex).toBe(5);
    const db2 = await connectWal(provider, { snapshotPrefix: LEGACY_SNAP_PREFIX });
    expect(await stateOf(db2, 'users')).toEqual(want);
    await db2.close();
  });
});
