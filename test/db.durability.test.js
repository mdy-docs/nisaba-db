/**
 * db.durability.test.js — who actually performs the fsync.
 *
 * Before Phase 2, bj_io had no sync callback. elog_sync therefore reduced
 * to a bare io.write, and durability came from the JavaScript wrapper
 * calling handle.flush() straight afterwards. C declared the durability
 * point; JS supplied it. Two owners of one contract -- and a native host,
 * having no JS wrapper, got neither. docs/replicaton-roadmap.md flags this
 * as a production blocker: "the server storage provider's flush path must
 * be a real fsync -- Raft safety depends on sync-before-ack reaching
 * disk."
 *
 * The JS flush() calls are gone now and C fsyncs through bj_io.sync. That
 * is precisely the kind of change that can silently do nothing, so these
 * tests count the flushes reaching the handle. A regression here does not
 * fail any other test in the suite: the data still lands, it just stops
 * being durable.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { ready, EntryLog, MemoryHandle } from '../src/nisaba-wasm.js';

beforeAll(async () => { await ready(); });

/** A sync-access-handle that records how many times it was fsynced. */
function countingHandle() {
  const inner = new MemoryHandle();
  return {
    flushes: 0,
    getSize: () => inner.getSize(),
    read: (buf, opts) => inner.read(buf, opts),
    write: (buf, opts) => inner.write(buf, opts),
    truncate: (len) => inner.truncate(len),
    flush() { this.flushes++; if (inner.flush) inner.flush(); },
    close: () => inner.close?.()
  };
}

async function openLog(handle) {
  const log = new EntryLog(handle);
  await log.open();
  // elog_append refuses an entry whose term the durable hard state has not
  // reached -- "a leader acking with an unpersisted term is the classic
  // Raft bug". So establishing term 1 is setup, not part of what is being
  // measured, and it costs one fsync of its own.
  log.setHardState(1, 0);
  return log;
}

const payload = (s) => new TextEncoder().encode(s);

describe('durability: C drives the fsync, not the JS wrapper', () => {
  it('sync() reaches the handle exactly once', async () => {
    const h = countingHandle();
    const log = await openLog(h);
    const before = h.flushes;

    log.append(1, payload('a'));
    log.append(1, payload('b'));
    expect(h.flushes).toBe(before);      // append only buffers

    log.sync();
    // Exactly one: if the JS flush() had been left in alongside the new C
    // one, this would be two -- a doubled fsync per Raft ack.
    expect(h.flushes).toBe(before + 1);

    log.sync();
    expect(h.flushes).toBe(before + 2);
  });

  it('setHardState() is durable before it returns', async () => {
    // Raft may not answer a RequestVote until the term/vote it relies on
    // has reached the disk; a machine that loses this on a power cut can
    // vote twice in one term.
    const h = countingHandle();
    const log = await openLog(h);
    const before = h.flushes;
    log.setHardState(7, 3);
    expect(h.flushes).toBe(before + 1);
    expect(log.currentTerm).toBe(7);
    expect(log.votedFor).toBe(3);
  });

  it('truncateFrom() is durable before it returns', async () => {
    // A follower acks an AppendEntries only once the conflicting suffix is
    // really gone (Raft §5.3).
    const h = countingHandle();
    const log = await openLog(h);
    log.append(1, payload('a'));
    log.append(1, payload('b'));
    log.append(1, payload('c'));
    log.sync();

    const before = h.flushes;
    log.truncateFrom(3);
    expect(h.flushes).toBe(before + 1);
    expect(log.lastIndex).toBe(2);
  });

  it('setCommitIndex() rides along with the next sync rather than fsyncing', async () => {
    // Deliberately NOT a durability point: commitIndex is recoverable by
    // re-deriving it from the leader, so paying an fsync per advance would
    // be cost with no safety return. This pins the distinction so a later
    // change cannot make every commit-index advance hit the disk.
    const h = countingHandle();
    const log = await openLog(h);
    log.append(1, payload('a'));
    log.sync();

    const before = h.flushes;
    log.setCommitIndex(1);
    expect(h.flushes).toBe(before);
  });

  it('a failing fsync surfaces as an error rather than a silent success', async () => {
    // bridgeHandle swallows write failures into a short write so C can
    // unwind, but a flush failure must PROPAGATE: the caller asked for
    // durability and did not get it, and elog_sync's return value is what
    // sync-before-ack rests on.
    const h = countingHandle();
    const log = await openLog(h);
    log.append(1, payload('a'));
    h.flush = () => { throw new Error('disk on fire'); };
    expect(() => log.sync()).toThrow();
  });
});
