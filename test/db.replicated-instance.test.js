/**
 * The replicated INSTANCE (src/db-replicated-instance.js): a cluster of
 * ReplicatedInstances over MemoryStorageProviders on the deterministic
 * simulator — db.replicated.test.js's harness, one level up: many
 * databases behind one log, one leader, one member set. Covers:
 * cross-database convergence, replicated dropDatabase, and the
 * instance-wide snapshot install (blank member and stale-disk member,
 * including a database whose directory must NOT survive the install).
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId } from '../src/nisaba-wasm.js';
import { MemoryStorageProvider } from '../src/db.js';
import { connectReplicatedInstance, NotLeaderError } from '../src/db-replicated-instance.js';
import { Sim, MemoryNetwork, leaders, until, settle } from './raft-harness.js';

await ready();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));

async function bootMember(id, ids, sim, net, provider, raftOptions = {}) {
  const inst = await connectReplicatedInstance(provider, {
    id, peers: ids,
    transport: { call: (to, msg) => net.call(id, to, msg) },
    raft: { random: sim.rng, ...raftOptions },
    startNow: sim.time
  });
  net.register(id, (msg) => inst.raft.handleMessage(msg));
  return { id, inst, provider, node: inst.raft };
}

async function makeCluster(n, sim, net, raftOptions = {}) {
  const ids = Array.from({ length: n }, (_, i) => i + 1);
  const cluster = new Map();
  for (const id of ids) {
    cluster.set(id, await bootMember(id, ids, sim, net, new MemoryStorageProvider(), raftOptions));
  }
  await until(sim, cluster, () => {
    const ls = leaders(cluster);
    return ls.length === 1 &&
      [...cluster.values()].every((m) => m.node.leaderId === ls[0].node.id);
  });
  return cluster;
}

async function untilAsync(sim, cluster, predicate, maxMs = 10_000, step = 50) {
  const nodes = [...cluster.values()].map((m) => m.node);
  for (let waited = 0; waited <= maxMs; waited += step) {
    if (await predicate()) return;
    await sim.advance(step, nodes);
  }
  throw new Error(`condition not reached within ${maxMs}ms of virtual time`);
}

const leaderOf = (cluster) => leaders(cluster)[0];
const followersOf = (cluster) => [...cluster.values()].filter((m) => m.node.role !== 'leader');

const docsOf = async (inst, db, coll) => {
  const col = await (await inst.db(db)).collection(coll);
  return (await col.find({}, { sort: { _id: 1 } }).toArray())
    .map((d) => ({ ...d, _id: d._id.toHexString() }));
};

describe('replicated instance: basics', () => {
  it('writes to several databases commit on one log and converge everywhere', async () => {
    const sim = new Sim(91);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    const leader = leaderOf(cluster);

    const users = await (await leader.inst.db('analytics')).collection('users');
    const bills = await (await leader.inst.db('billing')).collection('invoices');
    await settle(sim, cluster, users.insertOne({ _id: oid(1), who: 'ada' }));
    await settle(sim, cluster, bills.insertOne({ _id: oid(2), amount: 7 }));
    await settle(sim, cluster, users.updateOne({ _id: oid(1) }, { $set: { seen: true } }));

    // Every replica converges in BOTH databases, byte-equal.
    const wantUsers = await docsOf(leader.inst, 'analytics', 'users');
    const wantBills = await docsOf(leader.inst, 'billing', 'invoices');
    await untilAsync(sim, cluster, async () => {
      for (const f of followersOf(cluster)) {
        const u = await docsOf(f.inst, 'analytics', 'users');
        const b = await docsOf(f.inst, 'billing', 'invoices');
        if (JSON.stringify(u) !== JSON.stringify(wantUsers)) return false;
        if (JSON.stringify(b) !== JSON.stringify(wantBills)) return false;
      }
      return true;
    });

    // Writes are leader-only, wherever they name a database.
    const f = followersOf(cluster)[0];
    const fu = await (await f.inst.db('analytics')).collection('users');
    await expect(fu.insertOne({ _id: oid(9) })).rejects.toBeInstanceOf(NotLeaderError);
    for (const m of cluster.values()) await m.inst.close();
  });

  it('dropDatabase replicates: the directory goes away on every member', async () => {
    const sim = new Sim(92);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net);
    const leader = leaderOf(cluster);

    await settle(sim, cluster,
      (await (await leader.inst.db('keep')).collection('a')).insertOne({ _id: oid(1) }));
    await settle(sim, cluster,
      (await (await leader.inst.db('doomed')).collection('b')).insertOne({ _id: oid(2) }));
    await untilAsync(sim, cluster, async () => {
      for (const f of followersOf(cluster)) {
        if (!(await f.inst.listDatabases()).includes('doomed')) return false;
      }
      return true;
    });

    const { value } = await settle(sim, cluster, leader.inst.dropDatabase('doomed'));
    expect(value).toEqual({ ok: true, dropped: true });
    await untilAsync(sim, cluster, async () => {
      for (const m of cluster.values()) {
        if ((await m.inst.listDatabases()).includes('doomed')) return false;
      }
      return true;
    });
    for (const m of cluster.values()) await m.inst.close();
  });
});

describe('replicated instance: the instance-wide install', () => {
  it('a blank member is bootstrapped with EVERY database by one install', async () => {
    const sim = new Sim(93);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net, { snapshotChunkBytes: 128 });
    const leader = leaderOf(cluster);

    const users = await (await leader.inst.db('analytics')).collection('users');
    await settle(sim, cluster, users.createIndex({ team: 1 }));
    for (let i = 1; i <= 4; i++) {
      await settle(sim, cluster, users.insertOne({ _id: oid(i), team: i % 2 ? 'a' : 'b' }));
    }
    await settle(sim, cluster,
      (await (await leader.inst.db('billing')).collection('invoices')).insertOne({ _id: oid(10), amount: 7 }));

    const victim = followersOf(cluster)[0];
    net.unregister(victim.id);
    await victim.inst.close();
    const snap = await leader.inst.snapshot();
    expect(snap.config.live.every((f) => f.name.includes('/'))).toBe(true);
    expect(leader.node.log.baseIndex).toBe(snap.lastIncludedIndex);
    await sim.advance(300, [...cluster.values()].filter((m) => m.node.isRunning).map((m) => m.node));

    const replacement = await bootMember(
      victim.id, [...cluster.keys()], sim, net, new MemoryStorageProvider(), { snapshotChunkBytes: 128 });
    cluster.set(victim.id, replacement);

    await untilAsync(sim, cluster, async () => {
      if (replacement.node.lastApplied < snap.lastIncludedIndex) return false;
      const col = await (await replacement.inst.db('analytics')).collection('users');
      return (await col.countDocuments({})) === 4;
    }, 20_000);

    // Both databases arrived, the index too, and the store holds the
    // installed generation with its "db/file" live names.
    const col = await (await replacement.inst.db('analytics')).collection('users');
    expect((await col.find({ team: 'a' }).toArray()).length).toBe(2);
    expect(await (await (await replacement.inst.db('billing')).collection('invoices')).countDocuments({})).toBe(1);
    expect(replacement.inst.snapshots.latest.lastIncludedIndex).toBe(snap.lastIncludedIndex);
    expect(replacement.node.log.baseIndex).toBe(snap.lastIncludedIndex);

    // And it follows normally afterwards.
    await settle(sim, cluster, users.insertOne({ _id: oid(5), team: 'a' }));
    await untilAsync(sim, cluster, async () =>
      (await (await (await replacement.inst.db('analytics')).collection('users')).findOne({ _id: oid(5) })) !== null);
    for (const m of cluster.values()) await m.inst.close();
  });

  it('an install onto a stale disk removes a database the generation does not hold', async () => {
    const sim = new Sim(94);
    const net = new MemoryNetwork(sim);
    const cluster = await makeCluster(3, sim, net, { snapshotChunkBytes: 128 });
    const leader = leaderOf(cluster);

    await settle(sim, cluster,
      (await (await leader.inst.db('keep')).collection('a')).insertOne({ _id: oid(1), v: 'old' }));
    await settle(sim, cluster,
      (await (await leader.inst.db('doomed')).collection('b')).insertOne({ _id: oid(2) }));

    // The victim goes down HOLDING both databases; while it is away the
    // cluster drops one, keeps writing the other, and compacts the log
    // past everything the victim ever saw.
    const victim = followersOf(cluster)[0];
    const disk = victim.provider;
    await untilAsync(sim, cluster, async () =>
      (await victim.inst.listDatabases()).includes('doomed'));
    net.unregister(victim.id);
    await victim.inst.close();

    await settle(sim, cluster, leader.inst.dropDatabase('doomed'));
    await settle(sim, cluster,
      (await (await leader.inst.db('keep')).collection('a')).insertOne({ _id: oid(3), v: 'new' }));
    const snap = await leader.inst.snapshot();
    await sim.advance(300, [...cluster.values()].filter((m) => m.node.isRunning).map((m) => m.node));

    const reborn = await bootMember(
      victim.id, [...cluster.keys()], sim, net, disk, { snapshotChunkBytes: 128 });
    cluster.set(victim.id, reborn);

    await untilAsync(sim, cluster, async () => {
      if (reborn.node.lastApplied < snap.lastIncludedIndex) return false;
      const col = await (await reborn.inst.db('keep')).collection('a');
      return (await col.countDocuments({})) === 2;
    }, 20_000);

    // The dropped database's directory did not survive the install --
    // the C transport removes it (adopt_install), and so does this host.
    expect(await reborn.inst.listDatabases()).toEqual(['keep']);
    for (const m of cluster.values()) await m.inst.close();
  });
});

/* ---- the mixed cluster: C members and Node members, one group -------- */

import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { TcpRaftTransport } from '../src/raft-transport-tcp.js';
import { connectServer } from '../src/db-server-client.js';

const NATIVE = 'build/lib/nisaba-server';
const haveNative = fs.existsSync(NATIVE);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let portBase = 9450 + (process.pid % 300);
const takePort = () => portBase++;

/** Real-time retry: the mixed cluster runs on real sockets and real
 * timers, so waits are wall-clock, unlike the Sim suites above. */
async function realUntil(fn, ms = 30_000) {
  const until = Date.now() + ms;
  for (;;) {
    try {
      const v = await fn();
      if (v !== false) return v;
    } catch (err) {
      if (Date.now() > until) throw err;
    }
    if (Date.now() > until) throw new Error('condition not reached in real time');
    await sleep(100);
  }
}

/**
 * One Node member on the real peer wire. The transport adapter passes
 * the node's message BYTES as the envelope directly -- the C server's
 * frames carry no group tag (server/peers.h), and the group-tagged
 * shape is RaftGroupHost's own layer, not the wire's.
 */
async function bootJsMember(id, listenPort, others, { election } = {}) {
  let handler = () => { throw new Error('member not booted yet'); };
  const tcp = new TcpRaftTransport({
    listenPort, peers: {},
    onMessage: (env) => handler(env),
    requestTimeoutMs: 2000
  });
  await tcp.start();
  for (const [pid, addr] of Object.entries(others)) tcp.setPeer(Number(pid), addr);
  const records = [
    { id, host: '127.0.0.1', port: listenPort },
    ...Object.entries(others).map(([pid, a]) => ({ id: Number(pid), host: a.host, port: a.port }))
  ].sort((a, b) => a.id - b.id);
  const inst = await connectReplicatedInstance(new MemoryStorageProvider(), {
    id, peers: records,
    transport: { call: (to, msg) => tcp.call(to, msg) },
    raft: { snapshotChunkBytes: 8192, ...(election ? { electionTimeoutMs: [election, election] } : {}) },
    startNow: Date.now()
  });
  handler = (env) => inst.raft.handleMessage(env);
  const iv = setInterval(() => inst.raft.tick(Date.now()), 20);
  iv.unref?.();
  return {
    inst,
    stop: async () => { clearInterval(iv); await inst.close(); await tcp.stop(); }
  };
}

function spawnNative(dir, args) {
  const proc = spawn(path.resolve(NATIVE), args, { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  proc.stderr.on('data', (c) => { stderr += c; });
  return {
    proc,
    errors: () => stderr,
    stop: () => new Promise((r) => { proc.once('exit', r); proc.kill(); })
  };
}

describe.skipIf(!haveNative)('replicated instance: a mixed C/Node cluster', () => {
  it('a C leader replicates into a Node member, then installs into a blank one', async () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-mixed-cj-'));
    const prC = takePort(), prJ = takePort(), pcC = takePort();
    const server = spawnNative(dir, [
      '--port', String(pcC), '--raft', '1', '--raft-port', String(prC),
      '--peer', `2@127.0.0.1:${prJ}`, '--snapshot-entries', '8'
    ]);
    // The Node member never campaigns (huge election timeout), so the C
    // member leads as soon as the Node member grants its vote.
    let js = await bootJsMember(2, prJ, { 1: { host: '127.0.0.1', port: prC } }, { election: 60_000 });
    const client = await realUntil(() => connectServer(`127.0.0.1:${pcC}`));
    try {
      const users = client.db('appa').collection('users');
      await realUntil(() => users.insertOne({ _id: oid(1), i: 1 }));
      for (let i = 2; i <= 12; i++) await users.insertOne({ _id: oid(i), i });
      await client.db('appb').collection('things').insertOne({ _id: oid(20), n: 20 });

      // The Node member converged, in BOTH databases, from a C leader.
      await realUntil(async () => {
        const a = await (await js.inst.db('appa')).collection('users');
        const b = await (await js.inst.db('appb')).collection('things');
        return (await a.countDocuments({})) === 12 && (await b.countDocuments({})) === 1;
      });

      // 13 entries crossed --snapshot-entries 8: the C leader compacts,
      // so a BLANK Node member can only be caught up by an install.
      await realUntil(async () => (await client.ping()).base > 0);
      await js.stop();
      js = await bootJsMember(2, prJ, { 1: { host: '127.0.0.1', port: prC } }, { election: 60_000 });
      await realUntil(async () => {
        const a = await (await js.inst.db('appa')).collection('users');
        const b = await (await js.inst.db('appb')).collection('things');
        return (await a.countDocuments({})) === 12 && (await b.countDocuments({})) === 1;
      });
      // The blank member's state arrived as the C server's generation:
      // adopted by the node, instance-wide.
      expect(js.inst.snapshots.latest).not.toBeNull();
      expect(js.inst.snapshots.latest.config.live.every((f) => f.name.includes('/'))).toBe(true);
    } finally {
      await client.close().catch(() => {});
      await js.stop();
      await server.stop();
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }, 120_000);

  it('a Node leader installs into a blank C member, which then leads and serves the data', async () => {
    const pr1 = takePort(), pr2 = takePort(), prC = takePort(), pcC = takePort();
    const others1 = { 2: { host: '127.0.0.1', port: pr2 }, 3: { host: '127.0.0.1', port: prC } };
    const others2 = { 1: { host: '127.0.0.1', port: pr1 }, 3: { host: '127.0.0.1', port: prC } };
    // Member 1 leads deterministically: member 2 never campaigns, and
    // member 3 (the C one) is not even running yet.
    const js1 = await bootJsMember(1, pr1, others1);
    const js2 = await bootJsMember(2, pr2, others2, { election: 60_000 });
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-mixed-jc-'));
    let server = null;
    let client = null;
    try {
      await realUntil(() => js1.inst.raft.role === 'leader');

      const users = await (await js1.inst.db('appa')).collection('users');
      await users.createIndex({ team: 1 });
      for (let i = 1; i <= 4; i++) await users.insertOne({ _id: oid(i), team: i % 2 ? 'a' : 'b' });
      await (await (await js1.inst.db('appb')).collection('things')).insertOne({ _id: oid(20), n: 20 });
      const snap = await js1.inst.snapshot();
      expect(js1.inst.raft.log.baseIndex).toBe(snap.lastIncludedIndex);

      // The blank C member joins below the compacted base: the Node
      // leader can only catch it up by an instance-wide install.
      server = spawnNative(dir, [
        '--port', String(pcC), '--raft', '3', '--raft-port', String(prC),
        '--peer', `1@127.0.0.1:${pr1}`, '--peer', `2@127.0.0.1:${pr2}`
      ]);
      client = await realUntil(() => connectServer(`127.0.0.1:${pcC}`));
      await realUntil(async () => (await client.ping()).base === snap.lastIncludedIndex);
      expect(server.errors()).toMatch(/snapshot install adopted at index/);

      // The proof the files really landed: leadership moves to the C
      // member, and the C member serves both databases over its own
      // client wire.
      await js1.inst.transferLeadership(3);
      await realUntil(async () => (await client.ping()).role === 'leader');
      const a = client.db('appa').collection('users');
      expect(await realUntil(() => a.countDocuments({}))).toBe(4);
      expect((await a.find({ team: 'a' }).toArray()).length).toBe(2);
      expect(await client.db('appb').collection('things').countDocuments({})).toBe(1);
      // And it accepts writes that the Node members then converge on.
      await a.insertOne({ _id: oid(5), team: 'a' });
      await realUntil(async () => {
        const mine = await (await js1.inst.db('appa')).collection('users');
        return (await mine.countDocuments({})) === 5;
      });
    } finally {
      await client?.close().catch(() => {});
      await js1.stop();
      await js2.stop();
      if (server) await server.stop();
    }
    fs.rmSync(dir, { recursive: true, force: true });
  }, 120_000);
});
