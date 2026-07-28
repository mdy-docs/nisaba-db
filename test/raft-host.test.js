/**
 * Replication roadmap step 5d: RaftGroupHost — many Raft groups
 * multiplexed on one process seat, one envelope transport, one clock,
 * with idle-group quiescence — plus membership changes (CONFIG entries)
 * from src/raft.js, all on the deterministic simulator.
 */
import { describe, it, expect } from 'vitest';
import { ready, EntryLog, MemoryHandle } from '../wasm/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';
import { RaftGroupHost } from '../src/raft-host.js';
import { Sim, MemoryNetwork, KvMachine, KvSnapshotter, kvSet, until, settle } from './raft-harness.js';

await ready();

/** Build N hosts, each seating the same `groups`, KV nodes throughout.
 * Returns { hosts: Map(nodeId -> host), members: Map(`${gid}:${nodeId}` -> member) }. */
async function makeHostedCluster(n, groups, sim, net, { quiesceAfterMs = 0, nodeOptions = {} } = {}) {
  const ids = Array.from({ length: n }, (_, i) => i + 1);
  const hosts = new Map();
  const members = new Map();
  for (const id of ids) {
    const host = new RaftGroupHost({
      transport: { call: (peer, env) => net.call(id, peer, env) },
      quiesceAfterMs,
      now: () => sim.time
    });
    net.register(id, (env) => host.handleEnvelope(env));
    hosts.set(id, host);
  }
  for (const gid of groups) {
    for (const id of ids) {
      const host = hosts.get(id);
      const log = new EntryLog(new MemoryHandle());
      await log.open();
      const machine = new KvMachine();
      const node = new RaftNode({
        id, peers: ids, log, stateMachine: machine,
        snapshotter: new KvSnapshotter(machine),
        transport: host.groupTransport(gid),
        random: sim.rng,
        ...nodeOptions
      });
      await node.start(sim.time);
      host.addGroup(gid, node);
      members.set(`${gid}:${id}`, { node, machine, log });
    }
  }
  return { hosts, members };
}

const groupLeaders = (members, gid) =>
  [...members.entries()].filter(([k, m]) => k.startsWith(`${gid}:`) && m.node.isRunning && m.node.role === 'leader').map(([, m]) => m);

describe('raft host: multi-group', () => {
  it('every group elects its own leader independently on shared hosts', async () => {
    const sim = new Sim(71);
    const net = new MemoryNetwork(sim);
    const groups = ['tenant-a', 'tenant-b', 'tenant-c'];
    const { hosts, members } = await makeHostedCluster(3, groups, sim, net);
    const tickables = [...hosts.values()];
    await until(sim, new Map([...hosts].map(([id, h]) => [id, { node: { tick: (t) => h.tick(t), isRunning: true } }])),
      () => groups.every((g) => groupLeaders(members, g).length === 1));

    for (const g of groups) {
      const ls = groupLeaders(members, g);
      expect(ls.length).toBe(1);
    }
    // Groups commit independently through their own leaders.
    for (const g of groups) {
      const leader = groupLeaders(members, g)[0];
      const p = leader.node.propose(kvSet('k', g));
      let done = null;
      p.then((v) => { done = { v }; }, (e) => { done = { e }; });
      for (let i = 0; i < 100 && done === null; i++) await sim.advance(50, tickables);
      expect(done?.e).toBeUndefined();
    }
    for (const g of groups) {
      for (let i = 0; i < 100; i++) {
        const all = [...members.entries()].filter(([k]) => k.startsWith(`${g}:`)).map(([, m]) => m);
        if (all.every((m) => m.machine.map.get('k') === g)) break;
        await sim.advance(50, tickables);
      }
      for (const [k, m] of members) {
        if (k.startsWith(`${g}:`)) expect(m.machine.map.get('k')).toBe(g);
      }
    }
  });

  it('idle groups quiesce to zero wire traffic and wake on use', async () => {
    const sim = new Sim(72);
    const net = new MemoryNetwork(sim);
    let calls = 0;
    const origCall = net.call.bind(net);
    net.call = (from, to, env) => { calls++; return origCall(from, to, env); };

    const { hosts, members } = await makeHostedCluster(3, ['g'], sim, net, { quiesceAfterMs: 500 });
    const tickables = [...hosts.values()];
    for (let i = 0; i < 100 && groupLeaders(members, 'g').length !== 1; i++) await sim.advance(50, tickables);
    const leader = groupLeaders(members, 'g')[0];
    const p = leader.node.propose(kvSet('a', 1));
    let done = false;
    p.then(() => { done = true; }, () => {});
    for (let i = 0; i < 100 && !done; i++) await sim.advance(50, tickables);
    expect(done).toBe(true);

    // Idle out: after quiesceAfterMs everyone parks and the wire goes silent.
    await sim.advance(2000, tickables);
    const before = calls;
    await sim.advance(5000, tickables);
    expect(calls).toBe(before); // ZERO messages while quiesced
    expect(leader.node._quiesced).toBe(true);

    // Local use wakes the group; replication resumes and converges.
    const hostOfLeader = [...hosts.values()].find((h) => h.group('g') === leader.node);
    hostOfLeader.touch('g');
    const p2 = leader.node.propose(kvSet('b', 2));
    let done2 = false;
    p2.then(() => { done2 = true; }, () => {});
    for (let i = 0; i < 100 && !done2; i++) await sim.advance(50, tickables);
    expect(done2).toBe(true);
    for (let i = 0; i < 100; i++) {
      const all = [...members.values()];
      if (all.every((m) => m.machine.map.get('b') === 2)) break;
      await sim.advance(50, tickables);
    }
    for (const m of members.values()) expect(m.machine.map.get('b')).toBe(2);
  });

  it('a leader that dies while quiesced is replaced lazily, on next use', async () => {
    const sim = new Sim(73);
    const net = new MemoryNetwork(sim);
    const { hosts, members } = await makeHostedCluster(3, ['g'], sim, net, { quiesceAfterMs: 500 });
    const tickables = [...hosts.values()];
    for (let i = 0; i < 100 && groupLeaders(members, 'g').length !== 1; i++) await sim.advance(50, tickables);
    const dead = groupLeaders(members, 'g')[0];

    await sim.advance(3000, tickables); // quiesced everywhere
    // The leader's whole host dies silently.
    const deadHostId = [...hosts.entries()].find(([, h]) => h.group('g') === dead.node)[0];
    net.unregister(deadHostId);
    await dead.node.stop();
    hosts.delete(deadHostId);
    await sim.advance(3000, [...hosts.values()]); // still silent: nobody noticed
    expect(groupLeaders(members, 'g').length).toBe(0);

    // A client touches the group on a surviving host: wake, stale timers
    // fire, pre-vote wakes the other member, a new leader emerges.
    [...hosts.values()][0].touch('g');
    for (let i = 0; i < 200 && groupLeaders(members, 'g').length !== 1; i++) {
      await sim.advance(50, [...hosts.values()]);
    }
    const fresh = groupLeaders(members, 'g')[0];
    expect(fresh).toBeDefined();
    expect(fresh.node).not.toBe(dead.node);
  });
});

describe('raft: membership changes (CONFIG entries)', () => {
  it('growing a cluster from 3 to 4: the new member is caught up and votes', async () => {
    const sim = new Sim(74);
    const net = new MemoryNetwork(sim);
    const { makeCluster, bootNode, leaders: rawLeaders } = await import('./raft-harness.js');
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => rawLeaders(cluster).length === 1);
    const leader = () => rawLeaders(cluster)[0];
    await settle(sim, cluster, leader().node.propose(kvSet('pre', 1)));

    // Boot node 4 (knowing the full new set), then propose the change.
    const { MemoryHandle: MH } = await import('../wasm/nisaba-wasm.js');
    const four = await bootNode(4, [1, 2, 3, 4], sim, net, new MH());
    cluster.set(4, four);
    const change = await settle(sim, cluster, leader().node.changeMembership([1, 2, 3, 4]));
    expect(change.error).toBeUndefined();
    await until(sim, cluster, () => leader().node.members.join(',') === '1,2,3,4');

    // The new member replicates...
    await settle(sim, cluster, leader().node.propose(kvSet('post', 2)));
    await until(sim, cluster, () => four.machine.map.get('post') === 2);
    expect(four.machine.map.get('pre')).toBe(1);
    // ...and counts: a 4-node cluster needs 3 for quorum, so killing one
    // follower still commits.
    const victimId = [...cluster.values()].find((m) => m.node.role !== 'leader').node.id;
    net.unregister(victimId);
    const w = await settle(sim, cluster, leader().node.propose(kvSet('q', 3)));
    expect(w.error).toBeUndefined();
  });

  it('removing a member: it stops campaigning and the cluster commits without it', async () => {
    const sim = new Sim(75);
    const net = new MemoryNetwork(sim);
    const { makeCluster, leaders: rawLeaders } = await import('./raft-harness.js');
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => rawLeaders(cluster).length === 1);
    const leader = () => rawLeaders(cluster)[0];
    const removed = [...cluster.values()].find((m) => m.node.role !== 'leader');

    const keep = [...cluster.keys()].filter((id) => id !== removed.node.id);
    const change = await settle(sim, cluster, leader().node.changeMembership(keep));
    expect(change.error).toBeUndefined();
    await until(sim, cluster, () => leader().node.members.length === 2);

    // The removed node learns of its removal (the CONFIG entry reached it
    // before the leader stopped talking to it, or not — either way it
    // must not disrupt): advance a long time; the survivors' leadership
    // holds and commits with a 2-node quorum.
    const termBefore = leader().node.term;
    await sim.advance(10_000, [...cluster.values()].map((m) => m.node));
    expect(rawLeaders(cluster).filter((m) => m.node.id !== removed.node.id).length).toBe(1);
    expect(leader().node.term).toBe(termBefore);
    const w = await settle(sim, cluster, leader().node.propose(kvSet('after-removal', 1)));
    expect(w.error).toBeUndefined();
  });

  it('membership survives restart via the committed CONFIG entry', async () => {
    const sim = new Sim(76);
    const net = new MemoryNetwork(sim);
    const { makeCluster, bootNode, stopNode, leaders: rawLeaders } = await import('./raft-harness.js');
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => rawLeaders(cluster).length === 1);
    const leader = () => rawLeaders(cluster)[0];

    const { MemoryHandle: MH } = await import('../wasm/nisaba-wasm.js');
    const four = await bootNode(4, [1, 2, 3, 4], sim, net, new MH());
    cluster.set(4, four);
    await settle(sim, cluster, leader().node.changeMembership([1, 2, 3, 4]));
    await settle(sim, cluster, leader().node.propose(kvSet('x', 1)));
    await until(sim, cluster, () => four.node.members.length === 4);

    // Restart a follower that was BOOTED with the old 3-member set: the
    // CONFIG entry in its log must win over the stale static peers.
    const victim = [...cluster.values()].find((m) => m.node.role !== 'leader' && m.node.id !== 4);
    await stopNode(net, victim);
    const reborn = await bootNode(victim.node.id, [1, 2, 3], sim, net, victim.handle);
    cluster.set(victim.node.id, reborn);
    expect(reborn.node.members.join(',')).toBe('1,2,3,4'); // recovered from the log
    await until(sim, cluster, () => reborn.machine.map.get('x') === 1);
  });
});
