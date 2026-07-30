/**
 * Replication roadmap step 5c (docs/replicaton-roadmap.md): the WalDb
 * driven by the Raft core — src/db-replicated.js. A cluster of
 * ReplicatedDbs over MemoryStorageProviders runs on the deterministic
 * simulator (raft-harness.js): every write proposes through Raft, commits
 * on a quorum, and applies identically on every replica's real document
 * database. Covers: replica convergence, leader-only writes, proposal-
 * time determinism across replicas (_ids, upsert ids, $currentDate),
 * replicated DDL, deterministic command failures, leader failover,
 * restart catch-up, and snapshot install of a blank replacement member.
 */
import { describe, it, expect } from 'vitest';
import { ready, ObjectId } from '../wasm/nisaba-wasm.js';
import { MemoryStorageProvider } from '../src/db.js';
import { connectReplicated, NotLeaderError } from '../src/db-replicated.js';
import { Sim, MemoryNetwork, leaders, until, settle } from './raft-harness.js';

await ready();

const oid = (n) => new ObjectId(n.toString(16).padStart(24, '0'));

async function bootMember(id, ids, sim, net, provider, raftOptions = {}) {
  const rdb = await connectReplicated(provider, {
    id, peers: ids,
    transport: { call: (to, msg) => net.call(id, to, msg) },
    raft: { random: sim.rng, ...raftOptions },
    startNow: sim.time
  });
  net.register(id, (msg) => rdb.raft.handleMessage(msg));
  // Shape matches the raft harness (leaders/until/settle tick m.node).
  return { id, rdb, provider, node: rdb.raft };
}

async function makeDbCluster(n, sim, net, raftOptions = {}) {
  const ids = Array.from({ length: n }, (_, i) => i + 1);
  const cluster = new Map();
  for (const id of ids) {
    cluster.set(id, await bootMember(id, ids, sim, net, new MemoryStorageProvider(), raftOptions));
  }
  // Settled: one leader, and every member knows who it is.
  await until(sim, cluster, () => {
    const ls = leaders(cluster);
    return ls.length === 1 &&
      [...cluster.values()].every((m) => m.node.leaderId === ls[0].node.id);
  });
  return cluster;
}

/** Virtual-time wait on an async predicate (driver reads are async). */
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

describe('replicated db: basics', () => {
  it('writes commit on the leader and apply identically on every replica', async () => {
    const sim = new Sim(51);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const leader = leaderOf(cluster);
    const users = await leader.rdb.collection('users');

    const { error, value } = await settle(sim, cluster, users.insertOne({ _id: oid(1), name: 'Ada' }));
    expect(error).toBeUndefined();
    expect(value.insertedId.equals(oid(1))).toBe(true);
    await settle(sim, cluster, users.updateOne({ _id: oid(1) }, { $set: { seen: true } }));

    // Read-your-writes on the leader, replication lag on the followers.
    expect((await users.findOne({ _id: oid(1) })).seen).toBe(true);
    await untilAsync(sim, cluster, async () => {
      for (const f of followersOf(cluster)) {
        const doc = await (await f.rdb.collection('users')).findOne({ _id: oid(1) });
        if (!doc || doc.seen !== true) return false;
      }
      return true;
    });
    for (const m of cluster.values()) await m.rdb.close();
  });

  it('writes on a follower reject with NotLeaderError and the retry hint', async () => {
    const sim = new Sim(52);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const follower = followersOf(cluster)[0];
    const col = await follower.rdb.collection('users');
    await expect(col.insertOne({ n: 1 })).rejects.toMatchObject({
      name: 'NotLeaderError',
      leaderId: leaderOf(cluster).node.id
    });
    for (const m of cluster.values()) await m.rdb.close();
  });

  it('proposal-time determinism: upsert ids and $currentDate are identical on every replica', async () => {
    const sim = new Sim(53);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const users = await leaderOf(cluster).rdb.collection('users');

    const up = await settle(sim, cluster, users.updateOne(
      { tag: 'x' }, { $set: { n: 1 }, $currentDate: { ts: true } }, { upsert: true }
    ));
    expect(up.error).toBeUndefined();
    const upsertedId = up.value.upsertedId;

    await untilAsync(sim, cluster, async () => {
      for (const m of cluster.values()) {
        if (!(await (await m.rdb.collection('users')).findOne({ tag: 'x' }))) return false;
      }
      return true;
    });
    const docs = [];
    for (const m of cluster.values()) {
      docs.push(await (await m.rdb.collection('users')).findOne({ tag: 'x' }));
    }
    for (const d of docs) {
      expect(d._id.equals(upsertedId)).toBe(true);       // pinned upsert id
      expect(d.ts.getTime()).toBe(docs[0].ts.getTime()); // proposal-resolved clock
    }
    for (const m of cluster.values()) await m.rdb.close();
  });

  it('a deterministic command failure surfaces to the caller and leaves replicas identical', async () => {
    const sim = new Sim(54);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const users = await leaderOf(cluster).rdb.collection('users');
    await settle(sim, cluster, users.insertOne({ _id: oid(1), n: 1 }));

    const dup = await settle(sim, cluster, users.insertOne({ _id: oid(1), n: 2 }));
    expect(dup.error).toMatchObject({ name: 'DuplicateKeyError' });
    // Committed entries are never retracted: the failed command is a
    // deterministic result, and the log keeps it.
    expect(leaderOf(cluster).node.log.lastIndex).toBeGreaterThanOrEqual(3); // NOOP + 2 commands

    await untilAsync(sim, cluster, async () => {
      for (const m of cluster.values()) {
        const doc = await (await m.rdb.collection('users')).findOne({ _id: oid(1) });
        if (!doc || doc.n !== 1) return false;
      }
      return true;
    });
    for (const m of cluster.values()) await m.rdb.close();
  });

  it('an infrastructure failure halts the replica instead of becoming a result', async () => {
    // The other side of the same coin, and the one that actually matters:
    // swallowing a failure only THIS replica hit would let it skip an
    // entry and fork the state, silently. It must stop instead.
    //
    // The classification is a numeric code now (db_validate.h's
    // dc_is_deterministic) rather than `err.name === 'Error' ||
    // err.cause`, which was a JavaScript runtime detail holding up
    // consensus safety. Nothing tested this branch before, so the old
    // predicate could have been inverted and every test still passed.
    const sim = new Sim(56);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const leader = leaderOf(cluster);
    const users = await leader.rdb.collection('users');
    await settle(sim, cluster, users.insertOne({ _id: oid(1), n: 1 }));

    // A follower's apply throws something with no code at all -- the
    // shape a bridged storage exception or a bug arrives in.
    const follower = [...cluster.values()].find((m) => m !== leader);
    expect(follower.node.isRunning).toBe(true);
    const halted = [];
    follower.node.onEvent = (e) => { if (e.type === 'halt') halted.push(e); };
    follower.rdb._applyCommand = async () => { throw new Error('disk on fire'); };

    await settle(sim, cluster, users.insertOne({ _id: oid(2), n: 2 }));

    expect(follower.node.isRunning).toBe(false);   // stopped, not skipped
    expect(halted.length).toBe(1);
    // The leader and the healthy follower carry on: one replica halting
    // is an availability loss, which is the correct trade against a
    // silent fork.
    expect(leader.node.isRunning).toBe(true);
    expect((await (await leader.rdb.collection('users')).findOne({ _id: oid(2) })).n).toBe(2);

    for (const m of cluster.values()) await m.rdb.close();
  });
});

describe('replicated db: DDL', () => {
  it('createIndex and dropCollection replicate to every member', async () => {
    const sim = new Sim(55);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const leader = leaderOf(cluster);
    const users = await leader.rdb.collection('users');

    await settle(sim, cluster, users.insertOne({ _id: oid(1), team: 'red' }));
    const ix = await settle(sim, cluster, users.createIndex({ team: 1 }));
    expect(ix.error).toBeUndefined();
    expect(ix.value).toBe('team_1');

    await untilAsync(sim, cluster, async () => {
      for (const f of followersOf(cluster)) {
        const idx = await (await f.rdb.collection('users')).listIndexes();
        if (!idx.some((d) => d.name === 'team_1')) return false;
      }
      return true;
    });
    // The follower's index actually serves queries.
    const f0 = followersOf(cluster)[0];
    expect((await (await f0.rdb.collection('users')).find({ team: 'red' }).toArray()).length).toBe(1);

    const drop = await settle(sim, cluster, leader.rdb.dropCollection('users'));
    expect(drop.value).toBe(true);
    await untilAsync(sim, cluster, async () => {
      for (const m of cluster.values()) {
        if ((await m.rdb.listCollections()).includes('users')) return false;
      }
      return true;
    });
    for (const m of cluster.values()) await m.rdb.close();
  });
});

describe('replicated db: failures and recovery', () => {
  it('leader failover: a new leader accepts writes, committed data survives everywhere', async () => {
    const sim = new Sim(56);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const old = leaderOf(cluster);
    const users = await old.rdb.collection('users');
    await settle(sim, cluster, users.insertOne({ _id: oid(1), durable: true }));

    net.unregister(old.id);
    await old.rdb.close();
    cluster.delete(old.id);

    await until(sim, cluster, () => leaders(cluster).length === 1);
    const fresh = leaderOf(cluster);
    const users2 = await fresh.rdb.collection('users');
    // The new leader applies its backlog once its NOOP commits.
    await untilAsync(sim, cluster, async () => (await users2.findOne({ _id: oid(1) })) !== null);
    expect((await users2.findOne({ _id: oid(1) })).durable).toBe(true);
    const w = await settle(sim, cluster, users2.insertOne({ _id: oid(2), after: true }));
    expect(w.error).toBeUndefined();
    await untilAsync(sim, cluster, async () => {
      for (const m of cluster.values()) {
        if (!(await (await m.rdb.collection('users')).findOne({ _id: oid(2) }))) return false;
      }
      return true;
    });
    for (const m of cluster.values()) await m.rdb.close();
  });

  it('a member restarted from its own disk catches up by AppendEntries', async () => {
    const sim = new Sim(57);
    const net = new MemoryNetwork(sim);
    const cluster = await makeDbCluster(3, sim, net);
    const leader = leaderOf(cluster);
    const users = await leader.rdb.collection('users');
    await settle(sim, cluster, users.insertOne({ _id: oid(1), n: 1 }));

    const victim = followersOf(cluster)[0];
    await untilAsync(sim, cluster, async () =>
      (await (await victim.rdb.collection('users')).findOne({ _id: oid(1) })) !== null);
    net.unregister(victim.id);
    await victim.rdb.close();

    await settle(sim, cluster, users.insertOne({ _id: oid(2), n: 2 }));

    const reborn = await bootMember(victim.id, [...cluster.keys()], sim, net, victim.provider);
    cluster.set(victim.id, reborn);
    await untilAsync(sim, cluster, async () => {
      const col = await reborn.rdb.collection('users');
      return (await col.countDocuments({})) === 2;
    });
    expect(reborn.node.role).toBe('follower');
    for (const m of cluster.values()) await m.rdb.close();
  });

  it('a blank replacement member is bootstrapped by a full snapshot install', async () => {
    const sim = new Sim(58);
    const net = new MemoryNetwork(sim);
    // Small chunks exercise the SnapshotStore streaming adapter properly.
    const cluster = await makeDbCluster(3, sim, net, { snapshotChunkBytes: 128 });
    const leader = leaderOf(cluster);
    const users = await leader.rdb.collection('users');
    await settle(sim, cluster, users.createIndex({ team: 1 }));
    for (let i = 1; i <= 5; i++) {
      await settle(sim, cluster, users.insertOne({ _id: oid(i), team: i % 2 ? 'a' : 'b' }));
    }

    // Compact the leader's log behind a snapshot so AppendEntries cannot
    // serve history, then replace a follower with a blank disk.
    const victim = followersOf(cluster)[0];
    net.unregister(victim.id);
    await victim.rdb.close();
    const snap = await leader.rdb.snapshot();
    expect(snap.lastIncludedIndex).toBe(leader.node.lastApplied);
    expect(leader.node.log.baseIndex).toBe(snap.lastIncludedIndex);
    // Drain in-flight pre-compaction messages to the dead id (see the
    // raft suite's comment on delayed AppendEntries).
    await sim.advance(300, [...cluster.values()].filter((m) => m.node.isRunning).map((m) => m.node));

    const replacement = await bootMember(
      victim.id, [...cluster.keys()], sim, net, new MemoryStorageProvider(), { snapshotChunkBytes: 128 });
    cluster.set(victim.id, replacement);

    await untilAsync(sim, cluster, async () => {
      // lastApplied reaches the boundary only after the whole install —
      // data adoption AND the log swap — has finished.
      if (replacement.node.lastApplied < snap.lastIncludedIndex) return false;
      const col = await replacement.rdb.collection('users');
      return (await col.countDocuments({})) === 5;
    }, 20_000);
    // The installed replica has the data, the index, and a boundary-based log.
    const col = await replacement.rdb.collection('users');
    expect((await col.find({ team: 'a' }).toArray()).length).toBe(3);
    expect(replacement.node.log.baseIndex).toBe(snap.lastIncludedIndex);

    // And it follows normally afterwards.
    const w = await settle(sim, cluster, users.insertOne({ _id: oid(6), team: 'a' }));
    expect(w.error).toBeUndefined();
    await untilAsync(sim, cluster, async () =>
      (await (await replacement.rdb.collection('users')).findOne({ _id: oid(6) })) !== null);
    for (const m of cluster.values()) await m.rdb.close();
  });
});
