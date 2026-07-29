/**
 * Replication roadmap step 5 (docs/replicaton-roadmap.md): the Raft core,
 * driven entirely by the deterministic simulator (raft-harness.js) — a
 * virtual clock, seeded rng, and an in-memory network with delays and
 * partitions. Covers: elections (uniqueness, stability, pre-vote term
 * hygiene), replication and convergence, commit safety in minority
 * partitions, log conflict repair, crash-stop restarts from the persisted
 * EntryLog, and single-node degenerate clusters.
 */
import { describe, it, expect } from 'vitest';
import { ready } from '../wasm/nisaba-wasm.js';
import { NotLeaderError } from '../src/raft.js';
import {
  Sim, MemoryNetwork, makeCluster, bootNode, stopNode, takeSnapshot,
  leaders, until, settle, kvSet
} from './raft-harness.js';

await ready();

function maps(cluster) {
  return [...cluster.values()].map((m) => m.machine.snapshot());
}

async function electedCluster(seed, n = 3) {
  const sim = new Sim(seed);
  const net = new MemoryNetwork(sim);
  const cluster = await makeCluster(n, sim, net);
  await until(sim, cluster, () => leaders(cluster).length === 1);
  return { sim, net, cluster, leader: () => leaders(cluster)[0] };
}

describe('raft: elections', () => {
  it('elects exactly one leader; every node agrees on it and the term', async () => {
    const { sim, cluster, leader } = await electedCluster(1);
    await sim.advance(500, [...cluster.values()].map((m) => m.node));
    const ls = leaders(cluster);
    expect(ls.length).toBe(1);
    const l = ls[0].node;
    for (const m of cluster.values()) {
      expect(m.node.term).toBe(l.term);
      expect(m.node.leaderId).toBe(l.id);
    }
    expect(leader().node.id).toBe(l.id);
  });

  it('leadership is stable: no term churn across a long quiet period', async () => {
    const { sim, cluster, leader } = await electedCluster(2);
    const before = { id: leader().node.id, term: leader().node.term };
    await sim.advance(20_000, [...cluster.values()].map((m) => m.node));
    expect(leaders(cluster).length).toBe(1);
    expect(leader().node.id).toBe(before.id);
    expect(leader().node.term).toBe(before.term);
  });

  it('a single-node cluster elects itself and commits without a network', async () => {
    const sim = new Sim(3);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(1, sim, net);
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const { node, machine } = leaders(cluster)[0];
    const { value, error } = await settle(sim, cluster, node.propose(kvSet('a', 1)));
    expect(error).toBeUndefined();
    expect(value.index).toBeGreaterThan(0);
    expect(machine.map.get('a')).toBe(1);
  });

  it('propose on a follower rejects with the leader hint', async () => {
    const { cluster, leader } = await electedCluster(4);
    const follower = [...cluster.values()].find((m) => m.node.role !== 'leader');
    await expect(follower.node.propose(kvSet('x', 1))).rejects.toMatchObject({
      name: 'NotLeaderError',
      leaderId: leader().node.id
    });
  });
});

describe('raft: replication', () => {
  it('proposals replicate, commit, and apply identically on every node', async () => {
    const { sim, cluster, leader } = await electedCluster(5);
    const results = [];
    for (let i = 1; i <= 5; i++) {
      results.push(await settle(sim, cluster, leader().node.propose(kvSet(`k${i}`, i))));
    }
    for (const r of results) expect(r.error).toBeUndefined();
    // Indexes are strictly increasing (contiguous log, one NOOP offset).
    const indexes = results.map((r) => r.value.index);
    expect([...indexes].sort((a, b) => a - b)).toEqual(indexes);

    await until(sim, cluster, () =>
      [...cluster.values()].every((m) => m.node.lastApplied === leader().node.lastApplied));
    const [a, b, c] = maps(cluster);
    expect(b).toEqual(a);
    expect(c).toEqual(a);
    expect(a.length).toBe(5);
  });

  it('a burst of concurrent proposals applies in log order everywhere', async () => {
    const { sim, cluster, leader } = await electedCluster(6);
    const l = leader().node;
    const proposals = [];
    for (let i = 1; i <= 40; i++) proposals.push(l.propose(kvSet('seq', i)).catch((e) => e));
    await until(sim, cluster, () =>
      [...cluster.values()].every((m) => m.machine.map.get('seq') === 40));
    const settled = await Promise.all(proposals);
    for (const s of settled) expect(s).not.toBeInstanceOf(Error);
    expect(maps(cluster)[0]).toEqual(maps(cluster)[1]);
  });
});

describe('raft: snapshot install (5b)', () => {
  it('a follower behind the leader\'s compacted log catches up via a multi-chunk install', async () => {
    const sim = new Sim(31);
    const net = new MemoryNetwork(sim);
    // Tiny chunks force a real multi-chunk transfer.
    const cluster = await makeCluster(3, sim, net, { snapshotChunkBytes: 16 });
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const leader = () => leaders(cluster)[0];

    for (let i = 1; i <= 6; i++) await settle(sim, cluster, leader().node.propose(kvSet(`k${i}`, i)));

    const victimId = [...cluster.values()].find((m) => m.node.role !== 'leader').node.id;
    const victim = cluster.get(victimId);
    await stopNode(net, victim);

    for (let i = 7; i <= 12; i++) await settle(sim, cluster, leader().node.propose(kvSet(`k${i}`, i)));
    const boundary = await takeSnapshot(leader());
    expect(leader().log.baseIndex).toBe(boundary);
    // Drain in-flight messages to the dead node: heartbeats computed from
    // the pre-compaction log still sit in the virtual network, and a
    // delayed AppendEntries would catch the reboot up WITHOUT an install
    // (a legitimate heal, but not the path under test).
    await sim.advance(200, [...cluster.values()].filter((m) => m.node.isRunning).map((m) => m.node));

    // The rebooted follower's log ends far below the leader's base: only
    // an install can catch it up.
    const reborn = await bootNode(victimId, [...cluster.keys()], sim, net, victim.handle, { snapshotChunkBytes: 16 });
    cluster.set(victimId, reborn);
    await until(sim, cluster, () => reborn.machine.map.get('k12') === 12);
    expect(reborn.machine.snapshot()).toEqual(leader().machine.snapshot());
    expect(reborn.log.baseIndex).toBe(boundary);
    expect(reborn.node.lastApplied).toBeGreaterThanOrEqual(boundary);
    expect(leader().node.peersNeedingSnapshot).toEqual([]);
  });

  it('a replacement node with an empty disk is bootstrapped by install, then follows normally', async () => {
    const sim = new Sim(32);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const leader = () => leaders(cluster)[0];

    for (let i = 1; i <= 5; i++) await settle(sim, cluster, leader().node.propose(kvSet(`k${i}`, i)));
    const boundary = await takeSnapshot(leader());

    // A follower's disk is lost entirely; a blank replacement takes its
    // id (drain first — see the multi-chunk test's comment).
    const deadId = [...cluster.values()].find((m) => m.node.role !== 'leader').node.id;
    await stopNode(net, cluster.get(deadId));
    await sim.advance(200, [...cluster.values()].filter((m) => m.node.isRunning).map((m) => m.node));
    const { MemoryHandle } = await import('../wasm/nisaba-wasm.js');
    const replacement = await bootNode(deadId, [...cluster.keys()], sim, net, new MemoryHandle());
    cluster.set(deadId, replacement);

    await until(sim, cluster, () => replacement.machine.map.get('k5') === 5);
    expect(replacement.log.baseIndex).toBe(boundary);

    // Post-install traffic arrives by ordinary AppendEntries, not repeat
    // installs: the follower's log now extends past the boundary.
    await settle(sim, cluster, leader().node.propose(kvSet('post', 1)));
    await until(sim, cluster, () => replacement.machine.map.get('post') === 1);
    expect(replacement.log.lastIndex).toBeGreaterThan(boundary);
  });

  it('an install from a stale term is refused', async () => {
    const sim = new Sim(33);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const follower = [...cluster.values()].find((m) => m.node.role !== 'leader');
    const reply = await follower.node.handleMessage({
      kind: 'installSnapshot', term: 0, leaderId: 99,
      lastIncludedIndex: 100, lastIncludedTerm: 1,
      manifest: { config: null, files: [] }, role: null, offset: 0,
      data: new Uint8Array(0), done: true
    });
    expect(reply.success).toBe(false);
    expect(reply.term).toBe(follower.node.term);
    expect(follower.node.lastApplied).toBeLessThan(100);
  });

  it('without a leader-side snapshotter the peer parks; restoring it recovers', async () => {
    const sim = new Sim(34);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const leader = () => leaders(cluster)[0];

    for (let i = 1; i <= 4; i++) await settle(sim, cluster, leader().node.propose(kvSet(`k${i}`, i)));
    await takeSnapshot(leader());
    const disabled = leader().node.snapshotter;
    leader().node.snapshotter = null;

    const deadId = [...cluster.values()].find((m) => m.node.role !== 'leader').node.id;
    await stopNode(net, cluster.get(deadId));
    await sim.advance(200, [...cluster.values()].filter((m) => m.node.isRunning).map((m) => m.node));
    const { MemoryHandle } = await import('../wasm/nisaba-wasm.js');
    const replacement = await bootNode(deadId, [...cluster.keys()], sim, net, new MemoryHandle());
    cluster.set(deadId, replacement);

    await until(sim, cluster, () => leader().node.peersNeedingSnapshot.includes(deadId));
    expect(replacement.machine.map.size).toBe(0); // stuck, as designed

    leader().node.snapshotter = disabled; // host intervenes
    await until(sim, cluster, () => replacement.machine.map.get('k4') === 4);
    expect(leader().node.peersNeedingSnapshot).toEqual([]);
  });
});

describe('raft: failures and partitions', () => {
  it('a majority elects a new leader when the old one is cut off; committed data survives; the old leader converges on heal', async () => {
    const { sim, net, cluster, leader } = await electedCluster(7);
    const old = leader();
    await settle(sim, cluster, old.node.propose(kvSet('durable', 1)));

    // Cut the leader off; the other two elect among themselves.
    const others = [...cluster.keys()].filter((id) => id !== old.node.id);
    net.partition(others, [old.node.id]);
    await until(sim, cluster, () => {
      const ls = leaders(cluster).filter((m) => m.node.id !== old.node.id);
      return ls.length === 1;
    });
    const fresh = leaders(cluster).find((m) => m.node.id !== old.node.id);
    expect(fresh.machine.map.get('durable')).toBe(1); // committed data survived

    const { error } = await settle(sim, cluster, fresh.node.propose(kvSet('after', 2)));
    expect(error).toBeUndefined();

    net.heal();
    await until(sim, cluster, () =>
      old.node.role === 'follower' && old.machine.map.get('after') === 2);
    expect(leaders(cluster).length).toBe(1);
    expect(maps(cluster)[0]).toEqual(maps(cluster)[1]);
  });

  it('a minority leader cannot commit; its uncommitted entry is discarded on heal', async () => {
    const { sim, net, cluster, leader } = await electedCluster(8);
    const old = leader();
    const others = [...cluster.keys()].filter((id) => id !== old.node.id);
    net.partition(others, [old.node.id]);

    // The stranded leader accepts a proposal it can never commit.
    const doomed = old.node.propose(kvSet('phantom', 666)).catch((e) => e);
    // Majority side moves on and commits its own history.
    await until(sim, cluster, () =>
      leaders(cluster).some((m) => m.node.id !== old.node.id));
    const fresh = leaders(cluster).find((m) => m.node.id !== old.node.id);
    await settle(sim, cluster, fresh.node.propose(kvSet('real', 1)));

    net.heal();
    await until(sim, cluster, () =>
      old.node.role === 'follower' && old.machine.map.get('real') === 1);
    const rejection = await doomed;
    expect(rejection).toBeInstanceOf(NotLeaderError);
    // The phantom write exists nowhere.
    for (const m of cluster.values()) expect(m.machine.map.has('phantom')).toBe(false);
    expect(maps(cluster)[0]).toEqual(maps(cluster)[2]);
  });

  it('pre-vote: an isolated node does not inflate its term and rejoins without dethroning the leader', async () => {
    const { sim, net, cluster, leader } = await electedCluster(9);
    const l = leader();
    const isolated = [...cluster.values()].find((m) => m.node.role !== 'leader');
    const others = [...cluster.keys()].filter((id) => id !== isolated.node.id);
    const termBefore = isolated.node.term;

    net.partition(others, [isolated.node.id]);
    await sim.advance(5_000, [...cluster.values()].map((m) => m.node));
    // Many election timeouts elapsed; pre-vote failed each time, so the
    // term never moved (a non-pre-vote candidate would be at term+N).
    expect(isolated.node.term).toBe(termBefore);

    await settle(sim, cluster, l.node.propose(kvSet('while-away', 1)));
    net.heal();
    await until(sim, cluster, () => isolated.machine.map.get('while-away') === 1);
    expect(leader().node.id).toBe(l.node.id);
    expect(leader().node.term).toBe(termBefore);
  });

  it('a crashed node restarts from its persisted log and catches up', async () => {
    const { sim, net, cluster, leader } = await electedCluster(10);
    await settle(sim, cluster, leader().node.propose(kvSet('early', 1)));

    const victimId = [...cluster.values()].find((m) => m.node.role !== 'leader').node.id;
    const victim = cluster.get(victimId);
    const termAtCrash = victim.node.term;
    await stopNode(net, victim);

    await settle(sim, cluster, leader().node.propose(kvSet('while-down', 2)));

    // Reboot over the same bytes: term/vote restored, state machine
    // rebuilt by replay, then caught up by the leader.
    const reborn = await bootNode(victimId, [...cluster.keys()], sim, net, victim.handle);
    cluster.set(victimId, reborn);
    expect(reborn.log.currentTerm).toBeGreaterThanOrEqual(termAtCrash);
    await until(sim, cluster, () =>
      reborn.machine.map.get('early') === 1 && reborn.machine.map.get('while-down') === 2);
    expect(leaders(cluster).length).toBe(1);
    expect(reborn.node.role).toBe('follower');
  });

  it('chaos: random partition/heal cycles under continuous load converge, keeping every acknowledged write', async () => {
    for (const seed of [21, 22, 23, 24]) {
      const sim = new Sim(seed);
      const net = new MemoryNetwork(sim);
      const cluster = await makeCluster(3, sim, net);
      const nodes = [...cluster.values()].map((m) => m.node);
      await until(sim, cluster, () => leaders(cluster).length >= 1);

      const acked = new Set(); // keys whose propose() resolved — must survive
      let counter = 0;
      for (let round = 0; round < 10; round++) {
        const roll = sim.rng();
        if (roll < 0.35) {
          const odd = 1 + Math.floor(sim.rng() * 3);
          net.partition([...cluster.keys()].filter((id) => id !== odd), [odd]);
        } else if (roll < 0.5) {
          net.partition([1], [2], [3]); // no majority anywhere
        } else {
          net.heal();
        }
        for (const m of leaders(cluster)) {
          for (let i = 0; i < 3; i++) {
            const k = `k${counter++}`;
            m.node.propose(kvSet(k, 1)).then(() => acked.add(k), () => {});
          }
        }
        await sim.advance(700, nodes);
      }

      net.heal();
      await until(sim, cluster, () => {
        const ls = leaders(cluster);
        if (ls.length !== 1) return false;
        const idx = ls[0].node.lastApplied;
        return [...cluster.values()].every((m) => m.node.lastApplied === idx);
      }, 20_000);

      const [a, b, c] = maps(cluster);
      expect(b, `seed ${seed}`).toEqual(a);
      expect(c, `seed ${seed}`).toEqual(a);
      const finalMap = cluster.get(1).machine.map;
      for (const k of acked) {
        expect(finalMap.has(k), `seed ${seed}: acknowledged write ${k} lost`).toBe(true);
      }
    }
  });

  it('total leader crash-stop: remaining nodes recover the full committed history', async () => {
    const { sim, net, cluster, leader } = await electedCluster(11);
    for (let i = 1; i <= 3; i++) await settle(sim, cluster, leader().node.propose(kvSet(`k${i}`, i)));
    const old = leader();
    await stopNode(net, old);
    cluster.delete(old.node.id);

    await until(sim, cluster, () => leaders(cluster).length === 1);
    const fresh = leaders(cluster)[0];
    expect(fresh.machine.map.get('k3')).toBe(3);
    const { error } = await settle(sim, cluster, fresh.node.propose(kvSet('k4', 4)));
    expect(error).toBeUndefined();
    expect([...cluster.values()].every((m) => m.machine.map.get('k4') === 4)).toBe(true);
  });
});

describe('raft: config precedence on restart', () => {
  it('a restarted survivor keeps the latest-in-log CONFIG over older replayed ones (leader removed itself)', async () => {
    // The stuck-survivor shape behind graceful node retirement (drain):
    // in a two-voter group the leader commits its OWN removal and steps
    // down at apply, possibly before the survivor ever learns the new
    // commit index. The survivor restarts to heal: the boot scan adopts
    // the removal CONFIG (latest in log, uncommitted locally), but the
    // apply pump then replays from the state machine's floor — which
    // sits BELOW the older, committed CONFIG entries. Those replayed
    // configs must not regress membership over the scan's newer one
    // (RaftNode._configIndex), or the survivor solicits votes from the
    // removed leader forever and the group is dead.
    const { sim, net, cluster, leader } = await electedCluster(31, 2);
    const L = leader();
    const F = [...cluster.values()].find((m) => m.node.role !== 'leader');
    const [lid, fid] = [L.node.id, F.node.id];

    await settle(sim, cluster, L.node.propose(kvSet('kept', 1)));
    // Two CONFIG entries below the final one: add an absent member 9,
    // remove it again — the history the pump will later replay.
    expect((await settle(sim, cluster, L.node.changeMembership([lid, fid, { id: 9, host: 'node9', port: 7009 }]))).error).toBeUndefined();
    expect((await settle(sim, cluster, L.node.changeMembership([lid, fid]))).error).toBeUndefined();

    // The leader removes ITSELF: the entry commits under the old quorum
    // (both nodes ack), the leader steps down at apply — its in-flight
    // propose may reject NotLeaderError even though the change is in.
    const { error } = await settle(sim, cluster, L.node.changeMembership([fid]));
    if (error) expect(error).toBeInstanceOf(NotLeaderError);
    await until(sim, cluster, () => F.log.lastIndex === L.log.lastIndex);
    expect(L.node.role).not.toBe('leader');

    // The survivor is stuck live (its applied config still names both
    // voters and the removed leader refuses to vote) — restart it, the
    // documented heal.
    await stopNode(net, F);
    const reborn = await bootNode(fid, [lid, fid], sim, net, F.handle);
    cluster.set(fid, reborn);

    // The latest-in-log CONFIG survived the replay of its predecessors...
    await until(sim, cluster, () => reborn.node.voters.length === 1 && reborn.node.role === 'leader');
    expect(reborn.node.voters).toEqual([fid]);
    // ...and the sole survivor serves again, data intact.
    expect(reborn.machine.map.get('kept')).toBe(1);
    const w = await settle(sim, cluster, reborn.node.propose(kvSet('after', 2)));
    expect(w.error).toBeUndefined();
    expect(reborn.machine.map.get('after')).toBe(2);
  });
});
