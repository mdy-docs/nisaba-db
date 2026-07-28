/**
 * Smoke test for the EntryLog (Raft log / write-ahead log) surface compiled
 * into this package's own WASM binary (wasm/build-wasm.sh): create, append/
 * sync/get, hard state, truncation, reopen persistence, and compaction. The
 * full behavioral suite (durability, crash recovery, tiling) lives in the
 * binjson-structures repo itself -- this only proves the exports are wired
 * up and functional in nisaba's build.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { ready, EntryLog, ENTRYLOG_TYPE, deleteFile, getFileHandle } from '../wasm/nisaba-wasm.js';
import { bootstrapOPFS } from './binjson.suite.js';

await ready();
const { hasOPFS } = await bootstrapOPFS();

const enc = new TextEncoder();
const dec = new TextDecoder();

describe.skipIf(!hasOPFS)('EntryLog (nisaba wasm build)', () => {
  let root = null;
  let counter = 0;
  const files = [];

  beforeAll(async () => {
    root = await navigator.storage.getDirectory();
  });

  const name = () => {
    const n = `test-nisaba-entrylog-${counter++}.bj`;
    files.push(n);
    return n;
  };

  afterAll(async () => {
    for (const f of files) await deleteFile(root, f);
  });

  async function sync(filename, create = false) {
    const fh = await getFileHandle(root, filename, { create });
    return fh.createSyncAccessHandle();
  }

  async function openLog(filename, options) {
    const log = new EntryLog(await sync(filename, true), options);
    await log.open();
    return log;
  }

  it('creates an empty log with zeroed state', async () => {
    const log = await openLog(name());
    expect(log.baseIndex).toBe(0);
    expect(log.lastIndex).toBe(0);
    expect(log.currentTerm).toBe(0);
    expect(log.verify()).toBe(true);
    expect(log.getBatch(1)).toEqual([]);
    await log.close();
  });

  it('appends, syncs, and reads entries back', async () => {
    const log = await openLog(name());
    log.setHardState(1);
    expect(log.append(1, enc.encode('set x=1'))).toBe(1);
    expect(log.append(1, 'set y=2')).toBe(2);
    log.sync();

    const e1 = log.get(1);
    expect(e1.term).toBe(1);
    expect(e1.type).toBe(ENTRYLOG_TYPE.NORMAL);
    expect(dec.decode(e1.payload)).toBe('set x=1');

    const batch = log.getBatch(1);
    expect(batch.map((e) => e.index)).toEqual([1, 2]);
    expect(log.lastIndex).toBe(2);
    expect(log.lastTerm).toBe(1);
    await log.close();
  });

  it('persists hard state, entries, and commit index across reopen', async () => {
    const file = name();
    let log = await openLog(file);
    log.setHardState(3, 42);
    log.append(3, 'a');
    log.append(3, 'b');
    log.setCommitIndex(1);
    log.sync();
    await log.close();

    log = await openLog(file);
    expect(log.currentTerm).toBe(3);
    expect(log.votedFor).toBe(42);
    expect(log.commitIndex).toBe(1);
    expect(log.lastIndex).toBe(2);
    expect(dec.decode(log.get(2).payload)).toBe('b');
    expect(log.verify()).toBe(true);
    await log.close();
  });

  it('truncates a conflicting suffix (the Raft conflict rule)', async () => {
    const log = await openLog(name());
    log.setHardState(1);
    log.append(1, 'a');
    log.append(1, 'b');
    log.append(1, 'c');
    log.sync();
    log.truncateFrom(2);
    expect(log.lastIndex).toBe(1);
    log.setHardState(2);
    log.append(2, 'b2');
    log.sync();
    expect(log.get(2).term).toBe(2);
    expect(log.verify()).toBe(true);
    await log.close();
  });

  it('compacts through a snapshot boundary into a fresh file', async () => {
    const log = await openLog(name());
    log.setHardState(1);
    for (let i = 1; i <= 5; i++) log.append(1, `cmd${i}`);
    log.sync();

    const dstFile = name();
    const { newSize } = await log.compact(await sync(dstFile, true), 3, 1);
    expect(newSize).toBeGreaterThan(0);
    await log.close();

    const compacted = new EntryLog(await sync(dstFile));
    await compacted.open();
    expect(compacted.baseIndex).toBe(3);
    expect(compacted.baseTerm).toBe(1);
    expect(compacted.lastIndex).toBe(5);
    expect(() => compacted.get(3)).toThrow();
    expect(dec.decode(compacted.get(4).payload)).toBe('cmd4');
    expect(compacted.verify()).toBe(true);
    await compacted.close();
  });
});
