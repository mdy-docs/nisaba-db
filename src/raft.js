/**
 * raft.js — the transport-agnostic Raft core: replication roadmap step 5
 * (docs/replicaton-roadmap.md; step-4 decisions recorded there). One
 * RaftNode replicates one EntryLog (one WalDb's log — one group per
 * tenant database) across a static cluster of nodes, electing a leader,
 * appending proposed commands to a quorum, and feeding committed entries
 * to a state machine in log order.
 *
 * The division of labor with EntryLog (binjson-structures) is exact:
 * every persistence rule Raft imposes is the log's contract — hard state
 * (currentTerm/votedFor) commits immediately via setHardState, which this
 * node always calls BEFORE answering the RPC that changed it; an entry is
 * durable only after sync(), which runs before a follower acks an
 * AppendEntries or a leader counts itself toward a quorum; truncateFrom
 * is the conflict rule; termAt the consistency check; setCommitIndex the
 * advisory commit marker that rides the next sync. All EntryLog
 * operations are synchronous, so each RPC handler runs start-to-finish
 * without interleaving — the async seams are only the network and the
 * state machine.
 *
 * Deterministic by construction: the node never reads a clock or a
 * random source — time arrives through tick(now) and start(now), and
 * election jitter through the injected `random`. The step-7 simulator
 * drives a whole cluster on a virtual clock with a seeded rng; a real
 * host calls tick from setInterval and passes Math.random.
 *
 * The transport is one function: `call(peerId, message) -> Promise<reply>`
 * (binjson-framable messages; entry payloads are Uint8Arrays). Incoming
 * messages are handed to node.handleMessage(message), which returns the
 * reply — the host wires the two together (a WebSocket adapter in the
 * service, an in-memory network in tests). The node never owns a socket.
 *
 * Elections run pre-vote first (a round that persists nothing and bumps
 * no term — an isolated node rejoins without dethroning a live leader),
 * with leader stickiness: a node that has heard from a leader within the
 * minimum election timeout refuses pre-votes. On winning, a leader
 * appends a NOOP entry to commit its term's boundary (Raft §5.4.2: only
 * current-term entries commit by counting).
 *
 * Still to come (see roadmap): InstallSnapshot streaming (a follower
 * whose nextIndex falls below the leader's log base currently stalls —
 * the leader marks it `needsSnapshot` and the host must intervene), CONFIG
 * entries for membership changes, and the WalDb integration that turns
 * proposals into database commands (5c).
 */
import { ENTRYLOG_TYPE } from '../wasm/nisaba-wasm.js';

export class NotLeaderError extends Error {
  /** @param {number} leaderId - the last known leader (0 if unknown) */
  constructor(leaderId) {
    super(leaderId ? `Not the leader (try node ${leaderId})` : 'Not the leader (no known leader)');
    this.name = 'NotLeaderError';
    this.leaderId = leaderId;
  }
}

export const ROLE = { FOLLOWER: 'follower', CANDIDATE: 'candidate', LEADER: 'leader' };

const EMPTY = new Uint8Array(0);

export class RaftNode {
  /**
   * @param {object} options
   * @param {number} options.id - this node's id (nonzero small integer;
   *   0 is EntryLog's "voted for nobody")
   * @param {number[]} options.peers - every node id in the cluster
   *   (including or excluding this one — self is filtered out)
   * @param {object} options.log - an OPEN EntryLog
   * @param {object} options.stateMachine - { apply(entry) -> any|Promise,
   *   appliedIndex() -> number|Promise } — apply receives {index, term,
   *   type, payload} for NORMAL entries, strictly in index order, exactly
   *   once per index per process (crash replay is the state machine's
   *   appliedIndex contract, roadmap step 1)
   * @param {object} options.transport - { call(peerId, msg) -> Promise<reply> }
   * @param {[number, number]} [options.electionTimeoutMs=[150,300]]
   * @param {number} [options.heartbeatMs=50]
   * @param {number} [options.maxBatchBytes=65536] - AppendEntries batch cap
   * @param {() => number} [options.random=Math.random]
   */
  constructor({
    id, peers, log, stateMachine, transport,
    electionTimeoutMs = [150, 300], heartbeatMs = 50,
    maxBatchBytes = 65536, random = Math.random
  }) {
    if (!Number.isInteger(id) || id <= 0) throw new Error('RaftNode id must be a positive integer');
    this.id = id;
    this.peers = peers.filter((p) => p !== id);
    this.log = log;
    this.stateMachine = stateMachine;
    this.transport = transport;
    this.electionTimeoutMs = electionTimeoutMs;
    this.heartbeatMs = heartbeatMs;
    this.maxBatchBytes = maxBatchBytes;
    this.random = random;

    this.role = ROLE.FOLLOWER;
    this.leaderId = 0;
    /** Volatile commit index; EntryLog's persisted copy is advisory
     * (rides the next sync) and re-derived on restart. */
    this.commitIndex = Math.max(0, log.commitIndex);
    this.lastApplied = 0; // set in start()
    this.isRunning = false;

    this._now = 0;
    this._electionDeadline = Infinity;
    this._heartbeatDue = 0;
    this._lastLeaderContact = -Infinity;
    this._next = new Map();       // peer -> next index to send
    this._match = new Map();      // peer -> highest replicated index
    this._needsSnapshot = new Set(); // peers below our log base (5b: InstallSnapshot)
    this._inflight = new Set();   // peers with an AppendEntries in flight
    this._waiters = [];           // propose() promises: {index, term, resolve, reject}
    this._applyChain = Promise.resolve();
  }

  get term() { return this.log.currentTerm; }

  async start(now = 0) {
    this._now = now;
    this.lastApplied = await this.stateMachine.appliedIndex();
    this.commitIndex = Math.max(this.commitIndex, this.lastApplied);
    this.isRunning = true;
    this._resetElectionTimer();
    this._pumpApply();
  }

  async stop() {
    this.isRunning = false;
    this._rejectWaiters(new NotLeaderError(0));
    await this._applyChain.catch(() => {});
  }

  /** Drive timers. Call periodically (the simulator's virtual clock, or
   * setInterval on a real host); `now` must be monotonic. */
  tick(now) {
    if (!this.isRunning) return;
    this._now = Math.max(this._now, now);
    if (this.role === ROLE.LEADER) {
      if (this._now >= this._heartbeatDue) {
        this._heartbeatDue = this._now + this.heartbeatMs;
        for (const p of this.peers) this._replicate(p);
      }
    } else if (this._now >= this._electionDeadline) {
      this._startElection(true);
    }
  }

  /**
   * Propose a command: append to the local log (durable), replicate to a
   * quorum, apply. Resolves { index, term } once the entry is committed
   * AND applied locally (read-your-writes); rejects with NotLeaderError
   * if this node is not the leader or loses leadership before commit.
   */
  propose(payload, type = ENTRYLOG_TYPE.NORMAL) {
    if (!this.isRunning || this.role !== ROLE.LEADER) {
      return Promise.reject(new NotLeaderError(this.leaderId));
    }
    const term = this.log.currentTerm;
    const index = this.log.append(term, payload, type);
    this.log.sync();
    const promise = new Promise((resolve, reject) => {
      this._waiters.push({ index, term, resolve, reject });
    });
    for (const p of this.peers) this._replicate(p);
    this._advanceCommit(); // a single-node cluster commits immediately
    return promise;
  }

  /** Peers the leader knows are behind its log base and cannot be caught
   * up by AppendEntries — they need an InstallSnapshot (roadmap 5b). */
  get peersNeedingSnapshot() { return [...this._needsSnapshot]; }

  // ---- RPC handlers -------------------------------------------------------

  /** The transport hands every incoming message here; the return value is
   * the reply. Synchronous internally (all log operations are). */
  handleMessage(msg) {
    if (!this.isRunning) throw new Error('node is stopped');
    switch (msg.kind) {
      case 'requestVote': return this._onRequestVote(msg);
      case 'appendEntries': return this._onAppendEntries(msg);
      default: throw new Error(`raft: unknown message kind "${msg.kind}"`);
    }
  }

  _onRequestVote(msg) {
    const currentTerm = this.log.currentTerm;
    if (msg.term < currentTerm) return { term: currentTerm, voteGranted: false };

    const upToDate =
      msg.lastLogTerm > this.log.lastTerm ||
      (msg.lastLogTerm === this.log.lastTerm && msg.lastLogIndex >= this.log.lastIndex);

    if (msg.preVote) {
      // Persist nothing, grant nothing durable: "would I vote for you?".
      // Leader stickiness: refuse while a live leader is heard from.
      const sticky = this.leaderId !== 0 &&
        this._now - this._lastLeaderContact < this.electionTimeoutMs[0];
      return { term: currentTerm, voteGranted: !sticky && upToDate };
    }

    if (msg.term > currentTerm) this._becomeFollower(msg.term, 0);
    let granted = false;
    const votedFor = this.log.votedFor;
    if ((votedFor === 0 || votedFor === msg.candidateId) && upToDate) {
      if (votedFor !== msg.candidateId) {
        // Persist the vote BEFORE replying — Raft's cardinal rule, and
        // setHardState commits + fsyncs immediately (entrylog.h).
        this.log.setHardState(this.log.currentTerm, msg.candidateId);
      }
      granted = true;
      this._resetElectionTimer();
    }
    return { term: this.log.currentTerm, voteGranted: granted };
  }

  _onAppendEntries(msg) {
    let currentTerm = this.log.currentTerm;
    if (msg.term < currentTerm) return { term: currentTerm, success: false };
    if (msg.term > currentTerm || this.role !== ROLE.FOLLOWER) {
      this._becomeFollower(msg.term, msg.leaderId);
    }
    this.leaderId = msg.leaderId;
    this._lastLeaderContact = this._now;
    this._resetElectionTimer();
    currentTerm = this.log.currentTerm;

    const { prevLogIndex, prevLogTerm } = msg;
    if (prevLogIndex < this.log.baseIndex) {
      // The leader is offering entries our snapshot already covers; tell
      // it where our log actually starts so nextIndex can jump forward.
      return { term: currentTerm, success: false, hintIndex: this.log.baseIndex + 1 };
    }
    if (prevLogIndex > this.log.lastIndex) {
      return { term: currentTerm, success: false, hintIndex: this.log.lastIndex + 1 };
    }
    if (this.log.termAt(prevLogIndex) !== prevLogTerm) {
      return { term: currentTerm, success: false, hintIndex: prevLogIndex };
    }

    let appended = false;
    for (const e of msg.entries) {
      if (e.index <= this.log.lastIndex) {
        if (this.log.termAt(e.index) === e.term) continue; // already have it
        // The conflict rule (§5.3): ours is wrong, discard our suffix.
        // Raft guarantees no conflict at or below the commit index —
        // EntryLog's own truncate guard enforces exactly that invariant.
        this.log.truncateFrom(e.index);
      }
      this.log.append(e.term, e.payload, e.type);
      appended = true;
    }
    if (appended) this.log.sync(); // durable BEFORE the ack

    const matchIndex = prevLogIndex + msg.entries.length;
    if (msg.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(msg.leaderCommit, this.log.lastIndex);
      this.log.setCommitIndex(this.commitIndex); // advisory; rides next sync
      this._pumpApply();
    }
    return { term: currentTerm, success: true, matchIndex };
  }

  // ---- elections ----------------------------------------------------------

  _resetElectionTimer() {
    const [min, max] = this.electionTimeoutMs;
    this._electionDeadline = this._now + min + this.random() * (max - min);
  }

  _becomeFollower(term, leaderId) {
    if (term > this.log.currentTerm) this.log.setHardState(term);
    const wasLeader = this.role === ROLE.LEADER;
    this.role = ROLE.FOLLOWER;
    this.leaderId = leaderId;
    this._resetElectionTimer();
    if (wasLeader) this._rejectWaiters(new NotLeaderError(leaderId));
  }

  _startElection(preVote) {
    if (this.role === ROLE.LEADER) return;
    const term = this.log.currentTerm + 1;
    if (!preVote) {
      this.log.setHardState(term, this.id); // persist term + self-vote first
      this.role = ROLE.CANDIDATE;
      this.leaderId = 0;
    }
    this._resetElectionTimer();
    const quorum = this._quorum();
    if (quorum === 1) {
      return preVote ? this._startElection(false) : this._becomeLeader();
    }
    const req = {
      kind: 'requestVote', term, candidateId: this.id,
      lastLogIndex: this.log.lastIndex, lastLogTerm: this.log.lastTerm, preVote
    };
    let granted = 1; // self
    let settled = false;
    for (const p of this.peers) {
      this.transport.call(p, req).then((reply) => {
        if (settled || !this.isRunning) return;
        if (reply.term > this.log.currentTerm) {
          settled = true;
          return this._becomeFollower(reply.term, 0);
        }
        // The round is only valid while the world it was started in holds.
        if (preVote) {
          if (this.role === ROLE.LEADER || this.log.currentTerm !== term - 1) { settled = true; return; }
        } else if (this.role !== ROLE.CANDIDATE || this.log.currentTerm !== term) {
          settled = true;
          return;
        }
        if (reply.voteGranted && ++granted >= quorum) {
          settled = true;
          return preVote ? this._startElection(false) : this._becomeLeader();
        }
      }).catch(() => { /* unreachable peer; the timeout retries */ });
    }
  }

  _becomeLeader() {
    this.role = ROLE.LEADER;
    this.leaderId = this.id;
    this._heartbeatDue = this._now; // heartbeat on the next tick
    for (const p of this.peers) {
      this._next.set(p, this.log.lastIndex + 1);
      this._match.set(p, 0);
    }
    this._needsSnapshot.clear();
    // Commit the term boundary: only current-term entries commit by
    // counting (§5.4.2), so an idle leader could otherwise never commit
    // its predecessors' tail.
    this.log.append(this.log.currentTerm, EMPTY, ENTRYLOG_TYPE.NOOP);
    this.log.sync();
    for (const p of this.peers) this._replicate(p);
    this._advanceCommit(); // single-node clusters
  }

  // ---- replication (leader) -----------------------------------------------

  async _replicate(peer) {
    if (this.role !== ROLE.LEADER || this._inflight.has(peer)) return;
    const next = this._next.get(peer);
    if (next <= this.log.baseIndex) {
      this._needsSnapshot.add(peer); // 5b: InstallSnapshot goes here
      return;
    }
    this._needsSnapshot.delete(peer);
    const term = this.log.currentTerm;
    const prevLogIndex = next - 1;
    const prevLogTerm = this.log.termAt(prevLogIndex);
    const entries = next <= this.log.lastIndex ? this.log.getBatch(next, this.maxBatchBytes) : [];

    this._inflight.add(peer);
    let again = false;
    try {
      const reply = await this.transport.call(peer, {
        kind: 'appendEntries', term, leaderId: this.id,
        prevLogIndex, prevLogTerm, entries, leaderCommit: this.commitIndex
      });
      if (!this.isRunning || this.role !== ROLE.LEADER || this.log.currentTerm !== term) return;
      if (reply.term > term) return this._becomeFollower(reply.term, 0);
      if (reply.success) {
        if (reply.matchIndex > this._match.get(peer)) {
          this._match.set(peer, reply.matchIndex);
          this._next.set(peer, reply.matchIndex + 1);
          this._advanceCommit();
        }
        again = this._next.get(peer) <= this.log.lastIndex;
      } else {
        // Back up along the follower's hint (never past what's already
        // matched, never forward of a plain decrement).
        const hint = reply.hintIndex !== undefined ? reply.hintIndex : next - 1;
        this._next.set(peer, Math.max(this._match.get(peer) + 1, Math.min(hint, next - 1), 1));
        again = true;
      }
    } catch {
      // Unreachable; the next heartbeat retries.
    } finally {
      this._inflight.delete(peer);
    }
    if (again) this._replicate(peer);
  }

  _quorum() { return Math.floor((this.peers.length + 1) / 2) + 1; }

  _advanceCommit() {
    if (this.role !== ROLE.LEADER) return;
    const matches = [this.log.lastIndex, ...this.peers.map((p) => this._match.get(p))]
      .sort((a, b) => b - a);
    const n = matches[this._quorum() - 1];
    // §5.4.2: only entries of the CURRENT term commit by counting; earlier
    // terms commit implicitly once a current-term entry above them does.
    if (n > this.commitIndex && n > this.log.baseIndex && this.log.termAt(n) === this.log.currentTerm) {
      this.commitIndex = n;
      this.log.setCommitIndex(n);
      this._pumpApply();
    }
  }

  // ---- apply --------------------------------------------------------------

  _pumpApply() {
    this._applyChain = this._applyChain.then(() => this._applyLoop()).catch((err) => {
      // A state machine that throws on a committed entry is unrecoverable
      // divergence; stop rather than skip (skipping would fork replicas).
      this.isRunning = false;
      this._rejectWaiters(err);
    });
  }

  async _applyLoop() {
    while (this.isRunning && this.lastApplied < this.commitIndex) {
      for (const e of this.log.getBatch(this.lastApplied + 1, this.maxBatchBytes)) {
        if (e.index > this.commitIndex) break;
        if (e.type === ENTRYLOG_TYPE.NORMAL) await this.stateMachine.apply(e);
        this.lastApplied = e.index;
        this._settleWaiters();
      }
    }
  }

  _settleWaiters() {
    if (this._waiters.length === 0) return;
    const rest = [];
    for (const w of this._waiters) {
      if (w.index > this.lastApplied) { rest.push(w); continue; }
      // Applied at that index — but was it OUR entry, or did a new
      // leader's conflicting entry overwrite it before commit?
      if (this.log.termAt(w.index) === w.term) w.resolve({ index: w.index, term: w.term });
      else w.reject(new NotLeaderError(this.leaderId));
    }
    this._waiters = rest;
  }

  _rejectWaiters(err) {
    const waiters = this._waiters;
    this._waiters = [];
    for (const w of waiters) w.reject(err);
  }
}
