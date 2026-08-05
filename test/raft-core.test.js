/*
 * raft-core.test.js — the outbox seam, driven from JavaScript.
 *
 * test/native/main.c already runs a three-node cluster over raft_node.h
 * with no host language at all. This runs the same state machine from
 * the host that is about to adopt it, which is a different question: not
 * "does the C work" but "can a promise-shaped host drive it without
 * `await` inside the state machine".
 *
 * That matters because it is the exact rehearsal for cutting src/raft.js
 * over. If the seam is awkward from JavaScript, the cutover would
 * discover it 400 lines in; discovering it here costs nothing.
 *
 * The delivery loop below is the whole point. It is about fifteen lines,
 * it contains no Raft logic, and swapping its for-loop for real sockets
 * is the only difference between this and a server.
 */
import { describe, it, expect } from 'vitest';
import {
  ready, EntryLog, MemoryHandle, RaftCore, RAFT_ROLE, RN_EFFECT, ENTRYLOG_TYPE
} from '../src/nisaba-wasm.js';

await ready();

async function makeCluster(ids, { electionTimeoutMs = [150, 300], heartbeatMs = 50 } = {}) {
  const members = ids.map((id) => ({ id }));
  const nodes = [];
  for (const id of ids) {
    // EntryLog is file-resident: the C handle exists only after open().
    const log = new EntryLog(new MemoryHandle());
    await log.open();
    const core = new RaftCore(id, log, { electionTimeoutMs, heartbeatMs });
    core.setMembers(members);
    nodes.push({ id, log, core });
  }
  return nodes;
}

/**
 * One delivery round: hand every queued message to whoever it is
 * addressed to, and feed each answer back by correlation id.
 *
 * Note what it does NOT do: tell the receiving node who sent it. Every
 * message names its own sender, so the reply comes back out addressed
 * correctly without anyone here restating it -- which is the whole
 * reason this loop can route replies by `msg.peer` at all.
 *
 * `drop` lets a test partition the network by refusing to carry a
 * message -- the peer simply never answers, which is what onFail says.
 */
function pump(nodes, drop = () => false) {
  for (const from of nodes) {
    for (const msg of from.core.drainOutbox()) {
      const to = nodes.find((n) => n.id === msg.peer);
      if (!to || drop(from.id, msg.peer)) {
        if (!msg.isReply) from.core.onFail(msg.corr);
        continue;
      }
      if (msg.isReply) to.core.onReply(msg.corr, msg.bytes);
      else to.core.handle(msg.corr, msg.bytes);
    }
  }
}

function leaders(nodes) {
  return nodes.filter((n) => n.core.role === RAFT_ROLE.LEADER);
}

/** Run the clock until `done()`, or give up after `limit` ms. */
function run(nodes, { from = 0, limit = 4000, step = 10, drop } = {}, done = () => false) {
  for (let t = from; t <= from + limit; t += step) {
    for (const n of nodes) n.core.tick(t, 0.5);
    pump(nodes, drop);
    if (done()) return t;
  }
  return -1;
}

function cleanup(nodes) {
  for (const n of nodes) { n.core.free(); n.log.close?.(); }
}

describe('raft core: the outbox seam driven from JavaScript', () => {
  it('elects exactly one leader and commits its term-boundary entry', async () => {
    const nodes = await makeCluster([1, 2, 3]);
    try {
      // Staggered so the first election is deterministic rather than a
      // race the assertion has to tolerate.
      nodes[0].core.start(0, 0.0);
      nodes[1].core.start(0, 0.9);
      nodes[2].core.start(0, 0.95);
      expect(nodes[0].core.quorum).toBe(2);

      const at = run(nodes, {}, () => leaders(nodes).length === 1 &&
        leaders(nodes)[0].core.commitIndex > 0);
      expect(at).toBeGreaterThanOrEqual(0);
      expect(leaders(nodes)).toHaveLength(1);

      // Committing the no-op is what proves a quorum actually replicated,
      // rather than a node merely declaring itself leader.
      const leader = leaders(nodes)[0];
      expect(leader.core.commitIndex).toBeGreaterThan(0);
      expect(leader.core.hasQuorumContact(1000)).toBe(true);
      // Everyone agrees who won.
      for (const n of nodes) expect(n.core.leaderId).toBe(leader.id);
    } finally { cleanup(nodes); }
  });

  it('replicates an appended entry to every follower and commits it', async () => {
    const nodes = await makeCluster([1, 2, 3]);
    try {
      nodes[0].core.start(0, 0.0);
      nodes[1].core.start(0, 0.9);
      nodes[2].core.start(0, 0.95);
      run(nodes, {}, () => leaders(nodes).length === 1 && leaders(nodes)[0].core.commitIndex > 0);
      const leader = leaders(nodes)[0];

      const index = leader.log.append(leader.log.currentTerm,
        new TextEncoder().encode('hello'), ENTRYLOG_TYPE.NORMAL);
      leader.log.sync();
      for (const n of nodes) leader.core.replicate(n.id);

      const at = run(nodes, { from: 4000 }, () => leader.core.commitIndex >= index);
      expect(at).toBeGreaterThanOrEqual(0);
      expect(leader.core.commitIndex).toBeGreaterThanOrEqual(index);

      // Every replica holds it, at the leader's term.
      for (const n of nodes) {
        expect(n.log.lastIndex).toBeGreaterThanOrEqual(index);
        expect(n.log.termAt(index)).toBe(leader.log.currentTerm);
      }
      // And the leader's cursors say so.
      for (const n of nodes.filter((x) => x !== leader)) {
        expect(leader.core.matchOf(n.id)).toBeGreaterThanOrEqual(index);
      }
    } finally { cleanup(nodes); }
  });

  it('reports role and commit changes as effects rather than callbacks', async () => {
    // -sALLOW_TABLE_GROWTH=0 means C cannot call back into JS at all, so
    // everything the host must act on arrives as a drained record. This
    // is the same reason bj_ns has no list().
    const nodes = await makeCluster([1, 2, 3]);
    try {
      nodes[0].core.start(0, 0.0);
      nodes[1].core.start(0, 0.9);
      nodes[2].core.start(0, 0.95);

      const seen = new Map();
      const collect = () => {
        for (const n of nodes) {
          for (const e of n.core.drainEffects()) {
            if (!seen.has(n.id)) seen.set(n.id, []);
            seen.get(n.id).push(e);
          }
        }
      };
      for (let t = 0; t <= 4000; t += 10) {
        for (const n of nodes) n.core.tick(t, 0.5);
        collect();
        pump(nodes);
        // AFTER pump too: winning an election happens inside onReply, so
        // the ROLE effect that matters here is queued by delivery, not by
        // the tick that started the round.
        collect();
        if (leaders(nodes).length === 1 && leaders(nodes)[0].core.commitIndex > 0) break;
      }
      const leader = leaders(nodes)[0];
      const mine = seen.get(leader.id) ?? [];
      expect(mine.some((e) => e.kind === RN_EFFECT.ROLE && e.arg === RAFT_ROLE.LEADER)).toBe(true);
      expect(mine.some((e) => e.kind === RN_EFFECT.COMMIT)).toBe(true);
    } finally { cleanup(nodes); }
  });

  it('a partitioned minority cannot elect, and the majority still can', async () => {
    const nodes = await makeCluster([1, 2, 3, 4, 5]);
    try {
      // 4 and 5 are cut off from everyone, including each other: two
      // voters out of five can never reach a quorum of three.
      const isolated = (a, b) => a >= 4 || b >= 4;
      nodes[3].core.start(0, 0.0);   // shortest timeout is INSIDE the minority
      nodes[4].core.start(0, 0.05);
      nodes[0].core.start(0, 0.5);
      nodes[1].core.start(0, 0.9);
      nodes[2].core.start(0, 0.95);

      run(nodes, { limit: 6000, drop: isolated },
        () => leaders(nodes).length === 1 && leaders(nodes)[0].core.commitIndex > 0);

      const won = leaders(nodes);
      expect(won).toHaveLength(1);
      // The winner must be in the majority side, however eager the
      // minority was to campaign.
      expect(won[0].id).toBeLessThanOrEqual(3);
      expect(nodes[3].core.role).not.toBe(RAFT_ROLE.LEADER);
      expect(nodes[4].core.role).not.toBe(RAFT_ROLE.LEADER);
    } finally { cleanup(nodes); }
  });

  it('addresses every reply from the message itself, not from the caller', async () => {
    // The host is never asked who sent a message, so it cannot answer
    // wrongly — and this one genuinely does not know: its transport
    // pairs request and reply with a promise and carries no sender id.
    const nodes = await makeCluster([1, 2, 3]);
    try {
      nodes[0].core.start(0, 0.0);
      nodes[1].core.start(0, 0.9);
      nodes[2].core.start(0, 0.95);
      nodes[0].core.tick(200, 0.5);           // node 1 stands for election

      const requests = nodes[0].core.drainOutbox();
      expect(requests.length).toBe(2);
      for (const req of requests) {
        const to = nodes.find((n) => n.id === req.peer);
        to.core.handle(req.corr, req.bytes);  // no sender told
        const [reply] = to.core.drainOutbox();
        expect(reply.isReply).toBe(true);
        expect(reply.peer).toBe(nodes[0].id); // back to the candidate
        expect(reply.corr).toBe(req.corr);    // carrying its own id
      }
      // Ids are issued, never reused: two requests, two distinct ids.
      expect(requests[1].corr).toBeGreaterThan(requests[0].corr);
    } finally { cleanup(nodes); }
  });

  it('a single-voter group elects itself with no messages at all', async () => {
    const nodes = await makeCluster([1]);
    try {
      nodes[0].core.start(0, 0.0);
      expect(nodes[0].core.quorum).toBe(1);
      run(nodes, {}, () => nodes[0].core.role === RAFT_ROLE.LEADER);
      expect(nodes[0].core.role).toBe(RAFT_ROLE.LEADER);
      expect(nodes[0].core.commitIndex).toBeGreaterThan(0);
      // Nothing was ever sent: there is nobody to send to.
      expect(nodes[0].core.drainOutbox()).toHaveLength(0);
    } finally { cleanup(nodes); }
  });
});
