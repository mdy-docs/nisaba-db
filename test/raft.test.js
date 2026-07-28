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
  Sim, MemoryNetwork, makeCluster, bootNode, stopNode,
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
