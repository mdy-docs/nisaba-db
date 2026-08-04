/**
 * raft-crash.test.js — crash-point testing across the REPLICATED write
 * path, in the deterministic simulator (roadmap step 7; docs/steps brief
 * "crash-point testing"). db.wal-crash.test.js sweeps the single-node
 * boundaries; this file owns the ones that only exist between nodes: a
 * follower dying between accepting entries and acking them, a leader
 * dying between commit and apply, crash-stops under load, and a snapshot
 * install interrupted at its stages — including the mid-adopt window,
 * the only place in the system that replaces live files wholesale.
 *
 * The lever is a disk whose DURABLE state is each file's bytes as of its
 * last successful flush(). A crash is "freeze the durable bytes and
 * reboot a node over a copy of them": everything written but not flushed
 * at the instant is gone, exactly as a power cut leaves it. Arming the
 * disk makes the Nth flush (or write) throw and stay dead — the
 * mid-fsync instant — which is how a crash lands INSIDE a handler
 * rather than between calls. The wrapping-handle approach, per the
 * brief; every schedule is seeded and replays exactly.
 *
 * The exactly-once oracle is the 'inc' command (raft-harness.js): a
 * counter's value must equal the number of committed inc entries in the
 * log it was applied from. A set is idempotent by accident and hides a
 * double-apply; a counter cannot.
 */
import { describe, it, expect } from 'vitest';
import { ready, decode, ENTRYLOG_TYPE, MemoryHandle } from '../wasm/nisaba-wasm.js';
import {
  Sim, MemoryNetwork, bootNode, stopNode, takeSnapshot, makeDisk,
  leaders, until, settle, kvSet, kvInc
} from './raft-harness.js';

await ready();

/**
 * A disk with flush-durability semantics. `crashBytes()` kills it (every
 * later mutation throws) and returns the durable bytes — boot the
 * reborn node over `crashDisk(bytes)`. `armCrash({flushes|writes})`
 * schedules the death mid-syscall instead.
 */
function crashDisk(seed = new Map()) {
  const disk = makeDisk();
  const durable = new Map();
  for (const [name, bytes] of seed) {
    disk._files.set(name, new MemoryHandle(bytes));
    durable.set(name, bytes.slice());
  }
  const state = { flushes: 0, writes: 0, failFlush: Infinity, failWrite: Infinity, dead: false };
  const boom = (what) => { state.dead = true; throw new Error(`crash-stop (${what})`); };

  const openFile = disk.openFile.bind(disk);
  disk.openFile = async (name, opts) => {
    const h = await openFile(name, opts);
    if (h._shadow) return h;
    const inner = { write: h.write.bind(h), truncate: h.truncate.bind(h), flush: h.flush?.bind(h) };
    h.write = (buf, o) => {
      if (state.dead || ++state.writes >= state.failWrite) boom('mid-write');
      return inner.write(buf, o);
    };
    h.truncate = (len) => { if (state.dead) boom('truncate'); return inner.truncate(len); };
    h.flush = () => {
      if (state.dead || ++state.flushes >= state.failFlush) boom('mid-fsync');
      if (inner.flush) inner.flush();
      durable.set(name, h.toBytes());
    };
    h._shadow = true;
    return h;
  };
  const deleteFile = disk.deleteFile.bind(disk);
  disk.deleteFile = async (name) => {
    if (state.dead) boom('delete');
    durable.delete(name);
    return deleteFile(name);
  };
  disk.armCrash = ({ flushes, writes } = {}) => {
    if (flushes) state.failFlush = state.flushes + flushes;
    if (writes) state.failWrite = state.writes + writes;
  };
  disk.crashBytes = () => {
    state.dead = true;
    return new Map([...durable].map(([n, b]) => [n, b.slice()]));
  };
  Object.defineProperty(disk, 'dead', { get: () => state.dead });
  return disk;
}

/** Crash-stop `member`: durable bytes frozen FIRST, then the carcass is
 * silenced. Returns the bytes to reboot over. */
async function crashStop(net, member) {
  const bytes = member.disk.crashBytes();
  net.unregister(member.node.id);
  try { await member.node.stop(); } catch { /* it is crashing */ }
  try { await member.log.close(); } catch { /* dead disk */ }
  try { await member.store.close(); } catch { /* dead disk */ }
  return bytes;
}

async function crashCluster(seed, n = 3, nodeOptions = {}) {
  const sim = new Sim(seed);
  const net = new MemoryNetwork(sim);
  const ids = Array.from({ length: n }, (_, i) => i + 1);
  const cluster = new Map();
  for (const id of ids) {
    cluster.set(id, await bootNode(id, ids, sim, net, crashDisk(), nodeOptions));
  }
  await until(sim, cluster, () => leaders(cluster).length === 1);
  return { sim, net, cluster, ids, leader: () => leaders(cluster)[0] };
}

const nodesOf = (cluster) => [...cluster.values()].map((m) => m.node).filter((n) => n.isRunning);

/** The oracle: on `member`, each counter equals the number of committed
 * inc entries for its key in the member's OWN applied prefix. */
function expectExactlyOnce(member, label) {
  const { log, node, machine } = member;
  const counts = new Map();
  for (let i = log.baseIndex + 1; i <= node.lastApplied; i++) {
    const e = log.get(i);
    if (e.type !== ENTRYLOG_TYPE.NORMAL) continue;
    const c = decode(e.payload);
    if (c.op === 'inc') counts.set(c.k, (counts.get(c.k) || 0) + 1);
  }
  for (const [k, n] of counts) {
    expect(machine.map.get(k), `${label}: counter ${k} != committed inc count`).toBe(n);
  }
  return counts;
}

async function converged(sim, cluster) {
  await until(sim, cluster, () => {
    const ls = leaders(cluster);
    if (ls.length !== 1) return false;
    const idx = ls[0].node.lastApplied;
    return [...cluster.values()].every((m) => m.node.lastApplied === idx);
  }, 30_000);
}

describe('raft crash-points: a leader dying mid-durability under load', () => {
  it('at every armed instant: acked writes survive, replay is exactly-once, replicas end identical', async () => {
    // Each armed flush count is a different instant inside the leader's
    // append/sync/apply sequence — mid-fsync of a log sync at the small
    // ones, mid-fsync of a state-machine persist or a ride-along commit
    // marker at the larger. Seeded, so each is one exact schedule.
    for (const failAt of [1, 2, 3, 5, 8, 12]) {
      const { sim, net, cluster, ids, leader } = await crashCluster(80 + failAt);
      const L = leader();
      const lid = L.node.id;
      let acked = 0;
      L.disk.armCrash({ flushes: failAt });
      for (let round = 0; round < 20 && !L.disk.dead; round++) {
        for (let i = 0; i < 3; i++) {
          L.node.propose(kvInc('c')).then(() => acked++, () => {});
        }
        await sim.advance(300, nodesOf(cluster));
      }
      expect(L.disk.dead, `failAt ${failAt}: the workload never reached the armed instant`).toBe(true);

      const bytes = await crashStop(net, L);
      cluster.set(lid, await bootNode(lid, ids, sim, net, crashDisk(bytes)));
      await until(sim, cluster, () => leaders(cluster).length === 1, 30_000);

      // The group serves again, and history is one history.
      const fresh = leaders(cluster)[0];
      const w = await settle(sim, cluster, fresh.node.propose(kvSet('after', failAt)), 30_000);
      expect(w.error, `failAt ${failAt}`).toBeUndefined();
      await converged(sim, cluster);

      const maps = [...cluster.values()].map((m) => m.machine.snapshot());
      expect(maps[1], `failAt ${failAt}`).toEqual(maps[0]);
      expect(maps[2], `failAt ${failAt}`).toEqual(maps[0]);
      for (const m of cluster.values()) {
        const counts = expectExactlyOnce(m, `failAt ${failAt}, node ${m.node.id}`);
        expect(counts.get('c') ?? 0, `failAt ${failAt}: acked inc lost`).toBeGreaterThanOrEqual(acked);
      }
    }
  }, 60_000);
});

describe('raft crash-points: a follower dying between accepting entries and acking', () => {
  it('the unacked entry never counts toward commit; the rebooted follower converges', async () => {
    // Two voters: commit needs BOTH, so the leader's propose can resolve
    // only if the follower's ack meant what an ack must mean — the entry
    // was durable first. The follower's next fsync dies mid-call: the
    // entry reached its buffer, never its disk, and no ack was sent.
    const { sim, net, cluster, ids, leader } = await crashCluster(91, 2);
    const L = leader();
    const F = [...cluster.values()].find((m) => m.node.id !== L.node.id);
    const fid = F.node.id;

    F.disk.armCrash({ flushes: 1 });
    let outcome = null;
    const p = L.node.propose(kvInc('c'));
    p.then(() => { outcome = 'acked'; }, () => { outcome = 'rejected'; });

    await sim.advance(2_000, nodesOf(cluster));
    expect(F.disk.dead).toBe(true);      // the append's sync hit the armed instant
    // Nothing acked it, so nothing may have committed it.
    expect(outcome).toBeNull();
    expect(L.machine.map.get('c')).toBeUndefined();

    // Reboot over the durable bytes: the entry is not there (its fsync
    // failed), which is legal precisely because it was never acked.
    const bytes = await crashStop(net, F);
    const reborn = await bootNode(fid, ids, sim, net, crashDisk(bytes));
    expect(reborn.log.lastIndex).toBeLessThan(L.log.lastIndex);
    cluster.set(fid, reborn);

    // The leader retries, the entry commits once, everywhere.
    await until(sim, cluster, () => outcome !== null, 30_000);
    expect(outcome).toBe('acked');
    await converged(sim, cluster);
    for (const m of cluster.values()) {
      expect(m.machine.map.get('c')).toBe(1);
      expectExactlyOnce(m, `node ${m.node.id}`);
    }
  }, 30_000);
});

describe('raft crash-points: a leader dying after commit, before apply', () => {
  it('a successor commits the same prefix; the reborn leader replays it exactly once', async () => {
    const { sim, net, cluster, ids, leader } = await crashCluster(92, 3);
    const L = leader();
    const lid = L.node.id;

    // Freeze the leader's apply pump: entries commit (durable on a
    // quorum, commitIndex advanced) but its state machine never moves —
    // the exact window between commit and apply, held open.
    const frozen = [];
    L.machine.apply = () => new Promise((_, reject) => frozen.push(reject));
    const before = L.node.commitIndex;
    L.node.propose(kvInc('c')).catch(() => {});
    await until(sim, cluster, () =>
      L.node.commitIndex > before && L.node.lastApplied <= before, 10_000);

    // Crash there. The entry exists in the leader's durable log and a
    // quorum's; its kv file knows nothing of it. The in-flight apply
    // dies WITH the process — reject it, or stop() would wait on a
    // promise no crashed process can settle.
    L.disk.crashBytes();
    for (const reject of frozen) reject(new Error('crash-stop mid-apply'));
    const bytes = await crashStop(net, L);

    // Raft §5.4: the successor must carry the committed prefix forward.
    await until(sim, cluster, () =>
      leaders(cluster).some((m) => m.node.id !== lid), 30_000);
    const successor = leaders(cluster).find((m) => m.node.id !== lid);
    await until(sim, cluster, () => successor.machine.map.get('c') === 1, 30_000);

    // The reborn old leader replays it from its own log — once.
    cluster.set(lid, await bootNode(lid, ids, sim, net, crashDisk(bytes)));
    await converged(sim, cluster);
    for (const m of cluster.values()) {
      expect(m.machine.map.get('c')).toBe(1);
      expectExactlyOnce(m, `node ${m.node.id}`);
    }
  }, 30_000);
});

describe('raft crash-points: chaos crash-stops under load', () => {
  it('random crash/reboot cycles: every acknowledged write survives, every counter exact, every replica identical', async () => {
    for (const seed of [71, 72, 73, 74]) {
      const sim = new Sim(seed);
      const net = new MemoryNetwork(sim);
      const ids = [1, 2, 3];
      const cluster = new Map();
      for (const id of ids) cluster.set(id, await bootNode(id, ids, sim, net, crashDisk()));
      await until(sim, cluster, () => leaders(cluster).length >= 1);

      const acked = new Map(); // key -> acked inc count (must all survive)
      let down = null;         // at most one member down: majority persists
      let counter = 0;
      let crashes = 0;         // the test is vacuous if the rng never kills anyone

      for (let round = 0; round < 12; round++) {
        const roll = sim.rng();
        if (down !== null && (roll < 0.5 || round === 11)) {
          // Reboot the fallen member over its durable bytes.
          const { id, bytes } = down;
          cluster.set(id, await bootNode(id, ids, sim, net, crashDisk(bytes)));
          down = null;
        } else if (down === null && roll < 0.4) {
          const victims = [...cluster.values()];
          const victim = victims[Math.floor(sim.rng() * victims.length)];
          if (sim.rng() < 0.5) {
            // Die at an armed instant partway into the next rounds' i/o.
            victim.disk.armCrash(sim.rng() < 0.5
              ? { flushes: 1 + Math.floor(sim.rng() * 4) }
              : { writes: 1 + Math.floor(sim.rng() * 12) });
          } else {
            victim.disk.crashBytes(); // die right now
          }
          down = { id: victim.node.id, member: victim };
          crashes++;
        }
        for (const m of leaders(cluster)) {
          if (!m.node.isRunning) continue;
          for (let i = 0; i < 3; i++) {
            const k = `k${counter++ % 5}`;
            m.node.propose(kvInc(k)).then(
              () => acked.set(k, (acked.get(k) || 0) + 1), () => {});
          }
        }
        await sim.advance(700, nodesOf(cluster));
        // A member whose disk died this round is crash-stopped now (the
        // freeze already happened at the armed instant).
        if (down && !down.bytes && down.member.disk.dead) {
          down.bytes = await crashStop(net, down.member);
        } else if (down && !down.bytes) {
          down.bytes = await crashStop(net, down.member); // armed but never tripped: stop it anyway
        }
      }

      // The last round may have felled someone: every member is rebooted
      // before convergence is demanded of all three.
      if (down !== null) {
        cluster.set(down.id, await bootNode(down.id, ids, sim, net, crashDisk(down.bytes)));
        down = null;
      }

      await converged(sim, cluster);
      expect(crashes, `seed ${seed}: the schedule never crashed anyone`).toBeGreaterThanOrEqual(2);
      const maps = [...cluster.values()].map((m) => m.machine.snapshot());
      expect(maps[1], `seed ${seed}`).toEqual(maps[0]);
      expect(maps[2], `seed ${seed}`).toEqual(maps[0]);
      const one = [...cluster.values()][0];
      const counts = expectExactlyOnce(one, `seed ${seed}`);
      for (const [k, n] of acked) {
        expect(counts.get(k) ?? 0, `seed ${seed}: acked incs of ${k} lost`).toBeGreaterThanOrEqual(n);
      }
      for (const m of cluster.values()) expectExactlyOnce(m, `seed ${seed}, node ${m.node.id}`);
    }
  }, 120_000);
});

describe('raft crash-points: snapshot install interrupted (the mid-adopt window)', () => {
  /** A cluster whose leader has compacted past a dead member's log — the
   * state every install test starts from. Returns the dead member's id
   * and its durable bytes at the crash. */
  async function compactedPastVictim(seed, victimCrash) {
    const { sim, net, cluster, ids, leader } = await crashCluster(seed, 3);
    for (let i = 1; i <= 4; i++) {
      await settle(sim, cluster, leader().node.propose(kvInc('c')));
    }
    const victim = [...cluster.values()].find((m) => m.node.role !== 'leader');
    const vid = victim.node.id;
    const bytes = await victimCrash(sim, net, cluster, victim);
    for (let i = 5; i <= 8; i++) {
      await settle(sim, cluster, leader().node.propose(kvInc('c')));
    }
    const boundary = await takeSnapshot(leader());
    // Drain in-flight pre-compaction messages (see raft.test.js 5b).
    await sim.advance(200, nodesOf(cluster));
    return { sim, net, cluster, ids, leader, vid, bytes, boundary };
  }

  async function expectHealed(ctx) {
    const { sim, cluster, leader, vid, boundary } = ctx;
    const reborn = cluster.get(vid);
    await until(sim, cluster, () => reborn.machine.map.get('c') === 8, 60_000);
    expect(reborn.machine.snapshot()).toEqual(leader().machine.snapshot());
    expect(reborn.log.baseIndex).toBeGreaterThanOrEqual(boundary);
    // Life continues past the install: replication, and the oracle over
    // the suffix the reborn log actually holds.
    await settle(sim, cluster, leader().node.propose(kvInc('post')));
    await until(sim, cluster, () => reborn.machine.map.get('post') === 1, 30_000);
  }

  it('crash mid-staging: partial chunks are no state at all; the next install completes', async () => {
    const ctx = await compactedPastVictim(101, async (sim, net, cluster, victim) => {
      return crashStop(net, victim);
    });
    const { sim, net, cluster, ids, vid, bytes } = ctx;

    // Reboot with a disk armed to die a few writes into the chunk
    // stream: staged generation files exist in part, no manifest.
    const first = crashDisk(bytes);
    first.armCrash({ writes: 3 });
    const dying = await bootNode(vid, ids, sim, net, first);
    cluster.set(vid, dying);
    await until(sim, cluster, () => first.dead, 30_000);
    const bytes2 = await crashStop(net, dying);

    // Second reboot: whatever half-staged bytes survived must not be
    // mistaken for state. The install runs again, whole.
    cluster.set(vid, await bootNode(vid, ids, sim, net, crashDisk(bytes2)));
    await expectHealed(ctx);
  }, 60_000);

  it('crash after the manifest commits, before adoption: the old state still governs, and the install is redone', async () => {
    const ctx = await compactedPastVictim(102, async (sim, net, cluster, victim) => {
      return crashStop(net, victim);
    });
    const { sim, net, cluster, ids, vid, bytes, boundary } = ctx;

    // Reboot with the swap sabotaged: the node stages the whole
    // generation and commits its manifest, and the host dies exactly
    // where adoption would have replaced the live files.
    const disk = crashDisk(bytes);
    const member = await bootNode(vid, ids, sim, net, disk);
    cluster.set(vid, member);
    let sabotaged = false;
    const realSwap = member.files.swap;
    member.files.swap = async () => {
      sabotaged = true;
      throw new Error('crash-stop inside the adopt window');
    };
    await until(sim, cluster, () => sabotaged, 30_000);
    const bytes2 = await crashStop(net, member);

    // The durable state now holds BOTH histories: the old kv file and
    // log, and a fully committed generation at the boundary. The reboot
    // must come up as the OLD state (its log is the newest that opens —
    // openWalStorage's rule) and heal by the leader installing again.
    const reboot = crashDisk(bytes2);
    const reborn = await bootNode(vid, ids, sim, net, reboot);
    expect(reborn.log.baseIndex).toBe(0);                  // the old log governs
    expect(reborn.machine.map.get('c') ?? 0).toBeLessThan(8); // old state, honestly
    member.files.swap = realSwap;
    cluster.set(vid, reborn);
    await expectHealed(ctx);
    expect(cluster.get(vid).log.baseIndex).toBeGreaterThanOrEqual(boundary);
  }, 60_000);

  it('crash between adoption and the reload: the swap was one durable act; reboot resumes from the boundary', async () => {
    const ctx = await compactedPastVictim(103, async (sim, net, cluster, victim) => {
      return crashStop(net, victim);
    });
    const { sim, net, cluster, ids, vid, bytes } = ctx;

    // The adopt call itself runs — files replaced, log rebased, both
    // synced by C before it returns — and the crash lands right after,
    // before the host's reload ever sees the new state.
    const disk = crashDisk(bytes);
    const member = await bootNode(vid, ids, sim, net, disk);
    cluster.set(vid, member);
    let adopted = false;
    const files = member.files;
    files.swap = async (adopt) => {
      const victims = (await disk.listFiles()).filter((n) => !n.startsWith('snap-'));
      await adopt(victims);
      adopted = true;
      throw new Error('crash-stop after adopt, before reload');
    };
    await until(sim, cluster, () => adopted, 30_000);
    const bytes2 = await crashStop(net, member);

    // Reboot: the store's log opens, based at the boundary, and the kv
    // file is the generation's — the adoption held, wholesale.
    const reborn = await bootNode(vid, ids, sim, net, crashDisk(bytes2));
    expect(reborn.log.baseIndex).toBeGreaterThanOrEqual(ctx.boundary);
    expect(reborn.machine.map.get('c')).toBe(8);
    cluster.set(vid, reborn);
    await expectHealed(ctx);
  }, 60_000);
});
