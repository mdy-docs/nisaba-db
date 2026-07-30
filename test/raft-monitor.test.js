/**
 * Observability: RaftNode's onEvent transition stream and status()
 * snapshots (on the simulator), and the RaftMonitor's HTTP surface —
 * one-off GET /status and the SSE GET /events stream with its
 * snapshot-first contract — over a real server.
 */
import { describe, it, expect } from 'vitest';
import http from 'node:http';
import { ready, EntryLog, MemoryHandle } from '../wasm/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';
import { RaftGroupHost } from '../src/raft-host.js';
import { RaftMonitor } from '../src/raft-monitor.js';
import { Sim, MemoryNetwork, makeCluster, KvMachine, kvSet, leaders, until, settle, rpc } from './raft-harness.js';

await ready();

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

describe('raft observability: events and status', () => {
  it('emits transition events for elections, config, promotion, and reachability', async () => {
    const sim = new Sim(101);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    const events = [];
    for (const m of cluster.values()) {
      m.node.onEvent = (e) => events.push({ ...e, from: m.node.id });
    }
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const leader = () => leaders(cluster)[0];

    // Elections leave a trail: pre-vote rounds and a leader role change.
    expect(events.some((e) => e.type === 'election' && e.preVote === true)).toBe(true);
    expect(events.some((e) => e.type === 'role' && e.role === 'leader')).toBe(true);

    // A join produces config events on every node as the entry applies.
    events.length = 0;
    const joined = rpc(leader().node, {
      kind: 'join', member: { id: 4, host: 'node4', port: 7004 }
    });
    await until(sim, cluster, () => leader().node.members.includes(4));
    await joined;
    const configs = events.filter((e) => e.type === 'config');
    expect(configs.length).toBeGreaterThanOrEqual(3); // each member adopted it
    expect(configs[0].members.find((m) => m.id === 4).voting).toBe(false);

    // An unreachable peer is an EDGE event, not a per-retry flood.
    events.length = 0;
    const deadId = [...cluster.values()].find((m) => m.node.role !== 'leader').node.id;
    net.unregister(deadId);
    await sim.advance(2000, [...cluster.values()].map((m) => m.node));
    const unreachable = events.filter((e) => e.type === 'peer' && e.id === deadId && e.reachable === false);
    expect(unreachable.length).toBe(1);
    net.register(deadId, (msg) => cluster.get(deadId).node.handleMessage(msg));
    await sim.advance(500, [...cluster.values()].map((m) => m.node));
    expect(events.some((e) => e.type === 'peer' && e.id === deadId && e.reachable === true)).toBe(true);
  });

  it('halts loudly: a diverging state machine produces the halt event', async () => {
    const sim = new Sim(102);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const follower = [...cluster.values()].find((m) => m.node.role !== 'leader');
    const events = [];
    follower.node.onEvent = (e) => events.push(e);
    follower.machine.apply = () => { throw new Error('disk exploded'); };

    const leader = leaders(cluster)[0];
    leader.node.propose(kvSet('x', 1)).catch(() => {});
    await until(sim, cluster, () => !follower.node.isRunning);
    const halt = events.find((e) => e.type === 'halt');
    expect(halt).toBeDefined();
    expect(halt.error).toContain('disk exploded');
  });

  it('status() answers "what is true right now", including the leader\'s peer view', async () => {
    const sim = new Sim(103);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    await until(sim, cluster, () => leaders(cluster).length === 1);
    const leader = leaders(cluster)[0];
    await settle(sim, cluster, leader.node.propose(kvSet('a', 1)));

    const s = leader.node.status();
    expect(s.role).toBe('leader');
    expect(s.leaderId).toBe(leader.node.id);
    expect(s.commitIndex).toBe(s.log.lastIndex);
    expect(s.lastApplied).toBe(s.commitIndex);
    expect(s.voters).toEqual([1, 2, 3]);
    expect(s.peers.length).toBe(2);
    for (const p of s.peers) {
      expect(p.lag).toBe(0);
      expect(p.reachable).toBe(true);
      expect(p.needsSnapshot).toBe(false);
    }
    // JSON-able end to end.
    expect(() => JSON.stringify(s)).not.toThrow();

    const follower = [...cluster.values()].find((m) => m.node.role !== 'leader');
    expect(follower.node.status().peers).toBeNull();
  });
});

describe('raft monitor: HTTP surface', () => {
  function getJson(port, path) {
    return new Promise((resolve, reject) => {
      http.get({ host: '127.0.0.1', port, path }, (res) => {
        let body = '';
        res.on('data', (c) => { body += c; });
        res.on('end', () => resolve({ status: res.statusCode, body }));
      }).on('error', reject);
    });
  }

  it('GET /status is a one-off snapshot; GET /events streams snapshot-first then live events', async () => {
    // A real single-node group on a real host with real timers.
    const host = new RaftGroupHost({ transport: { call: async () => { throw new Error('lonely'); } }, tickMs: 20, quiesceAfterMs: 0 });
    const monitor = new RaftMonitor(host, { listenPort: 0 });
    await monitor.start();
    const port = monitor.address().port;

    const log = new EntryLog(new MemoryHandle());
    await log.open();
    const machine = new KvMachine();
    const node = new RaftNode({ id: 1, peers: [1], log, stateMachine: machine, transport: host.groupTransport('g') });

    try {
      // Connect the SSE stream BEFORE the group does anything, so the
      // election shows up as live events after the snapshot.
      const sse = await new Promise((resolve, reject) => {
        http.get({ host: '127.0.0.1', port, path: '/events' }, (res) => resolve(res)).on('error', reject);
      });
      let stream = '';
      sse.on('data', (c) => { stream += c; });
      // Snapshot-first contract: event zero arrives immediately, with no
      // groups yet.
      await sleep(50);
      expect(stream.startsWith('event: status\n')).toBe(true);
      expect(JSON.parse(stream.split('\n')[1].slice(6)).groupCount).toBe(0);

      host.addGroup('g', node);
      await node.start(Date.now());
      host.start();
      for (let i = 0; i < 100 && node.role !== 'leader'; i++) await sleep(20);
      expect(node.role).toBe('leader');
      await node.propose(kvSet('k', 1));

      // The one-off endpoint reflects the live cluster.
      const { status, body } = await getJson(port, '/status');
      expect(status).toBe(200);
      const snapshot = JSON.parse(body);
      expect(snapshot.groups.g.role).toBe('leader');
      expect(snapshot.groups.g.lastApplied).toBeGreaterThan(0);

      // The stream carried the election as tagged live events.
      await sleep(50);
      const raftEvents = stream.split('\n\n')
        .filter((f) => f.startsWith('event: raft'))
        .map((f) => JSON.parse(f.split('\n')[1].slice(6)));
      expect(raftEvents.some((e) => e.group === 'g' && e.type === 'role' && e.role === 'leader')).toBe(true);
      expect(raftEvents.some((e) => e.group === 'g' && e.type === 'started')).toBe(true);

      // Unknown paths point the way.
      expect((await getJson(port, '/nope')).status).toBe(404);
      sse.destroy();
    } finally {
      host.stop();
      await node.stop();
      await monitor.stop();
      await log.close();
    }
  }, 15_000);
});
