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
 * Snapshot install (5b): when a follower's nextIndex falls below the
 * leader's log base, AppendEntries can never catch it up — the entries
 * are compacted away. With a `snapshotter` configured, the leader streams
 * its latest snapshot instead: the manifest travels with the first chunk,
 * then every file chunk-by-chunk over the same request/response
 * transport, each chunk awaited (a stale term in any reply aborts). The
 * follower stages chunks through snapshotter.beginInstall's transaction;
 * commit() validates and adopts the whole state (its appliedIndex
 * becomes the boundary), then the node swaps its log for a fresh one
 * based at the boundary via the `rebaseLog` hook — EntryLog cannot
 * rebase in place — and re-persists its hard state onto it. The commit
 * and swap are serialized through the apply chain so an in-flight apply
 * loop can never observe the swap. Without a snapshotter, such peers
 * park in `peersNeedingSnapshot` for the host to notice.
 *
 * Membership (5d): the cluster's member set travels through the log as
 * CONFIG entries (payload: encode({ members })), proposed one at a time
 * via changeMembership() and adopted when APPLIED — the etcd convention
 * rather than the paper's adopt-on-append, trading the paper's edge-case
 * arguments for truncation-proof simplicity; single-server changes stay
 * safe because a change commits under the OLD quorum and the next one
 * can't start until it has. On restart the committed prefix is scanned
 * for the last CONFIG entry (static `peers` is only the bootstrap
 * default); a snapshot install adopts the leader's members from the
 * manifest. A node that applies its own removal steps down and stops
 * campaigning — the host closes it.
 *
 * Quiescence (5d): quiesce() parks the timers (a leader stops
 * heartbeating, a follower stops counting down), wake() restores them;
 * incoming messages and local proposals wake implicitly. An idle group
 * costs nothing on the wire; a leader that dies while quiesced is
 * detected lazily, on the group's next use — the RaftGroupHost
 * (src/raft-host.js) drives both ends.
 */
import { ENTRYLOG_TYPE, encode, decode } from '../wasm/nisaba-wasm.js';

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
   * @param {object} [options.snapshotter] - snapshot serving + install:
   *   { latest() -> { lastIncludedIndex, lastIncludedTerm, config,
   *       files: [{ role, size, crc, ... }] } | null,      // leader side
   *     openFile(role) -> { read(buf, {at}), close() },     // leader side
   *     beginInstall(manifest) -> {                         // follower side
   *       writeChunk(role, offset, data), commit(), abort() } }
   *   commit() must validate the staged bytes and adopt the whole state
   *   into the state machine (appliedIndex becomes the boundary).
   * @param {(lastIncludedIndex, lastIncludedTerm) => Promise<EntryLog>}
   *   [options.rebaseLog] - after an install commits, return a fresh OPEN
   *   EntryLog based at the boundary (the node closes the old log first
   *   and re-persists its hard state onto the new one). Required for a
   *   follower to accept installs.
   * @param {[number, number]} [options.electionTimeoutMs=[150,300]]
   * @param {number} [options.heartbeatMs=50]
   * @param {number} [options.maxBatchBytes=65536] - AppendEntries batch cap
   * @param {number} [options.snapshotChunkBytes=65536]
   * @param {() => number} [options.random=Math.random]
   */
  constructor({
    id, peers, log, stateMachine, transport,
    snapshotter = null, rebaseLog = null,
    electionTimeoutMs = [150, 300], heartbeatMs = 50,
    maxBatchBytes = 65536, snapshotChunkBytes = 65536, random = Math.random
  }) {
    if (!Number.isInteger(id) || id <= 0) throw new Error('RaftNode id must be a positive integer');
    this.id = id;
    this._setMembers([...new Set([...peers, id])]);
    this.log = log;
    this.stateMachine = stateMachine;
    this.transport = transport;
    this.snapshotter = snapshotter;
    this.rebaseLog = rebaseLog;
    this.electionTimeoutMs = electionTimeoutMs;
    this.heartbeatMs = heartbeatMs;
    this.maxBatchBytes = maxBatchBytes;
    this.snapshotChunkBytes = snapshotChunkBytes;
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
    this._install = null;         // follower: in-progress install transaction
    this._exclusive = null;       // runExclusive gate promise, or null
    this._quiesced = false;       // timers parked (see quiesce/wake)
    this._configInFlight = false; // one membership change at a time
  }

  /** Adopt a member set: `members` includes every node id (self too, if
   * still a member). Leader bookkeeping follows the set. */
  _setMembers(members) {
    this.members = [...members].sort((a, b) => a - b);
    this.peers = this.members.filter((p) => p !== this.id);
    if (this._next) {
      for (const p of this.peers) {
        if (!this._next.has(p)) {
          this._next.set(p, this.log.lastIndex + 1);
          this._match.set(p, 0);
        }
      }
      for (const p of [...this._next.keys()]) {
        if (!this.peers.includes(p)) {
          this._next.delete(p);
          this._match.delete(p);
          this._needsSnapshot.delete(p);
        }
      }
    }
  }

  /**
   * Run `fn` with the node quiesced: incoming messages queue behind the
   * gate, ticks skip, proposals wait, and in-flight applies drain first.
   * The host uses this for operations that swap the log out from under
   * the node (local snapshot + log compaction): inside `fn`, nothing else
   * can touch `this.log`. Reassign `node.log` inside `fn` if it swaps.
   */
  async runExclusive(fn) {
    while (this._exclusive) await this._exclusive;
    let release;
    this._exclusive = new Promise((resolve) => { release = resolve; });
    try {
      await this._applyChain.catch(() => {});
      return await fn();
    } finally {
      this._exclusive = null;
      release();
    }
  }

  get term() { return this.log.currentTerm; }

  async start(now = 0) {
    this._now = now;
    this.lastApplied = await this.stateMachine.appliedIndex();
    this.commitIndex = Math.max(this.commitIndex, this.lastApplied);
    // Recover membership: the last CONFIG entry in the log wins over the
    // static bootstrap `peers` (the paper's latest-in-log rule at
    // restart; a state machine's appliedIndex may sit past CONFIG
    // entries it never recorded, so the apply pump alone can't be relied
    // on to re-adopt them, and a committed-but-unadvertised entry — the
    // advisory commit index lags — must not be missed either).
    let scan = this.log.baseIndex + 1;
    while (scan <= this.log.lastIndex) {
      const batch = this.log.getBatch(scan, this.maxBatchBytes);
      if (batch.length === 0) break;
      for (const e of batch) {
        if (e.type === ENTRYLOG_TYPE.CONFIG) this._setMembers(decode(e.payload).members);
      }
      scan = batch[batch.length - 1].index + 1;
    }
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
    if (!this.isRunning || this._exclusive) return;
    this._now = Math.max(this._now, now);
    if (!this.members.includes(this.id)) return; // removed: never campaign
    if (this.role === ROLE.LEADER) {
      if (this._now >= this._heartbeatDue) {
        this._heartbeatDue = this._now + this.heartbeatMs;
        for (const p of this.peers) this._replicate(p);
      }
    } else if (this._now >= this._electionDeadline) {
      this._startElection(true);
    }
  }

  /** Park the group: a LEADER first tells every follower to park (a
   * final heartbeat with the quiesce flag — followers' election timeouts
   * are shorter than any idle threshold, so without this they would
   * misread the leader's silence as its death and churn elections), then
   * stops heartbeating. A follower parks its election countdown. Any
   * incoming message or local proposal wakes the node implicitly. */
  quiesce() {
    if (this.role === ROLE.LEADER) {
      for (const p of this.peers) this._replicate(p, { quiesce: true });
    }
    this._quiesced = true;
    this._electionDeadline = Infinity;
    this._heartbeatDue = Infinity;
  }

  wake(now = this._now) {
    if (!this._quiesced) return;
    this._quiesced = false;
    this._now = Math.max(this._now, now);
    this._heartbeatDue = this._now;
    this._resetElectionTimer();
  }

  /**
   * Propose a command: append to the local log (durable), replicate to a
   * quorum, apply. Resolves { index, term } once the entry is committed
   * AND applied locally (read-your-writes); rejects with NotLeaderError
   * if this node is not the leader or loses leadership before commit.
   */
  propose(payload, type = ENTRYLOG_TYPE.NORMAL) {
    if (this._exclusive) {
      return this._exclusive.then(() => this.propose(payload, type));
    }
    if (!this.isRunning || this.role !== ROLE.LEADER) {
      return Promise.reject(new NotLeaderError(this.leaderId));
    }
    this.wake(); // a quiesced leader must resume heartbeats to replicate
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

  /**
   * Propose a new member set (every node id, including or excluding this
   * one). Leader-only; one change may be in flight at a time — commit it
   * before proposing the next (the single-server-change safety argument
   * rests on changes serializing). The change takes effect on every node
   * when its CONFIG entry APPLIES; resolves like propose(). A brand-new
   * member typically needs a snapshot install to catch up — the ordinary
   * replication path handles that once it is a member.
   */
  async changeMembership(members) {
    const set = [...new Set(members)];
    if (set.length === 0 || set.some((m) => !Number.isInteger(m) || m <= 0)) {
      throw new Error('changeMembership requires a non-empty array of positive integer node ids');
    }
    if (this._configInFlight) {
      throw new Error('a membership change is already in flight; wait for it to commit');
    }
    this._configInFlight = true;
    try {
      return await this.propose(encode({ members: set }), ENTRYLOG_TYPE.CONFIG);
    } finally {
      this._configInFlight = false;
    }
  }

  // ---- RPC handlers -------------------------------------------------------

  /** The transport hands every incoming message here; the return value is
   * the reply (a promise for installSnapshot; synchronous otherwise —
   * all log operations are). */
  handleMessage(msg) {
    if (this._exclusive) {
      return this._exclusive.then(() => this.handleMessage(msg));
    }
    if (!this.isRunning) throw new Error('node is stopped');
    this.wake(); // any traffic un-quiesces the group on this node
    switch (msg.kind) {
      case 'requestVote': return this._onRequestVote(msg);
      case 'appendEntries': return this._onAppendEntries(msg);
      case 'installSnapshot': return this._onInstallSnapshot(msg);
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
      // Leader stickiness: refuse while a live leader is heard from —
      // and a healthy leader IS that leader, so it always refuses (the
      // classic disruptive case is a removed-but-unaware member whose
      // log is up to date pre-voting at the still-working leader).
      const sticky = this.role === ROLE.LEADER ||
        (this.leaderId !== 0 && this._now - this._lastLeaderContact < this.electionTimeoutMs[0]);
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
    if (msg.quiesce) {
      // The leader is parking the group: park with it (see quiesce()).
      this._quiesced = true;
      this._electionDeadline = Infinity;
    }
    return { term: currentTerm, success: true, matchIndex };
  }

  async _onInstallSnapshot(msg) {
    const currentTerm = this.log.currentTerm;
    if (msg.term < currentTerm) return { term: currentTerm, success: false };
    if (msg.term > currentTerm || this.role !== ROLE.FOLLOWER) {
      this._becomeFollower(msg.term, msg.leaderId);
    }
    this.leaderId = msg.leaderId;
    this._lastLeaderContact = this._now;
    this._resetElectionTimer();

    if (!this.snapshotter || !this.rebaseLog) {
      return { term: this.log.currentTerm, success: false };
    }

    if (msg.manifest) {
      // First chunk of a (re)started install: supersede anything staged.
      await this._abortInstall();
      this._install = {
        lastIncludedIndex: msg.lastIncludedIndex,
        lastIncludedTerm: msg.lastIncludedTerm,
        members: msg.manifest.members ?? null,
        tx: await this.snapshotter.beginInstall({
          lastIncludedIndex: msg.lastIncludedIndex,
          lastIncludedTerm: msg.lastIncludedTerm,
          config: msg.manifest.config,
          files: msg.manifest.files
        })
      };
    }
    const install = this._install;
    if (!install ||
        install.lastIncludedIndex !== msg.lastIncludedIndex ||
        install.lastIncludedTerm !== msg.lastIncludedTerm) {
      // A chunk for an install we never started (leader restarted, or we
      // aborted): ask for the manifest again.
      return { term: this.log.currentTerm, success: false, restart: true };
    }
    if (msg.role != null && msg.data && msg.data.length) {
      await install.tx.writeChunk(msg.role, msg.offset, msg.data);
    }

    if (msg.done) {
      // Commit + log swap serialize through the apply chain so an
      // in-flight apply loop can never observe the swap mid-batch.
      const finish = async () => {
        await install.tx.commit(); // validate + adopt into the state machine
        const term = this.log.currentTerm;
        const votedFor = this.log.votedFor;
        await this.log.close();
        this.log = await this.rebaseLog(msg.lastIncludedIndex, msg.lastIncludedTerm);
        if (term > 0) this.log.setHardState(term, votedFor); // fresh logs start at 0/0
        this.lastApplied = msg.lastIncludedIndex;
        this.commitIndex = Math.max(this.commitIndex, msg.lastIncludedIndex);
        if (install.members) this._setMembers(install.members);
        this._install = null;
      };
      const run = this._applyChain.then(finish);
      this._applyChain = run.catch(() => {});
      try {
        await run;
      } catch {
        // A failed commit (e.g. checksum mismatch on a corrupted
        // transfer) adopts nothing; have the leader start over.
        this._install = null;
        return { term: this.log.currentTerm, success: false, restart: true };
      }
    }
    return { term: this.log.currentTerm, success: true };
  }

  async _abortInstall() {
    const install = this._install;
    this._install = null;
    if (install) {
      try { await install.tx.abort(); } catch { /* best-effort */ }
    }
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
      this._abortInstall(); // a half-staged install belongs to the old world
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

  async _replicate(peer, { quiesce = false } = {}) {
    if (this.role !== ROLE.LEADER || this._inflight.has(peer)) return;
    const next = this._next.get(peer);
    if (next <= this.log.baseIndex) {
      // AppendEntries can never catch this peer up — the entries are
      // compacted away. Stream the snapshot instead, or park the peer
      // for the host if we can't serve one.
      if (this.snapshotter && this.snapshotter.latest()) return this._sendSnapshot(peer);
      this._needsSnapshot.add(peer);
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
      const msg = {
        kind: 'appendEntries', term, leaderId: this.id,
        prevLogIndex, prevLogTerm, entries, leaderCommit: this.commitIndex
      };
      if (quiesce) msg.quiesce = true;
      const reply = await this.transport.call(peer, msg);
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
        // Back up along the follower's hint (never forward of a plain
        // decrement). The hint may fall below matchIndex: a follower can
        // regress like that only by losing its disk (a blank replacement
        // reusing the id) — believe it, and drop matchIndex with it so a
        // replica that no longer holds the entries stops counting toward
        // quorums, and nextIndex can fall back to the install path.
        const hint = reply.hintIndex !== undefined ? reply.hintIndex : next - 1;
        const backedTo = Math.max(1, Math.min(hint, next - 1));
        if (backedTo <= this._match.get(peer)) this._match.set(peer, backedTo - 1);
        this._next.set(peer, backedTo);
        again = true;
      }
    } catch {
      // Unreachable; the next heartbeat retries.
    } finally {
      this._inflight.delete(peer);
    }
    if (again) this._replicate(peer);
  }

  /** Stream the latest snapshot to a peer, chunk by chunk, each chunk
   * awaited: the manifest rides the first chunk, `done` marks the last
   * chunk of the last file. On success the peer stands at the snapshot
   * boundary and ordinary AppendEntries resumes from there. */
  async _sendSnapshot(peer) {
    if (this._inflight.has(peer)) return;
    this._inflight.add(peer);
    const term = this.log.currentTerm;
    let installed = false;
    let boundary = 0;
    try {
      const snap = this.snapshotter.latest();
      const { lastIncludedIndex, lastIncludedTerm } = snap;
      boundary = lastIncludedIndex;
      const files = snap.files;
      const base = {
        kind: 'installSnapshot', term, leaderId: this.id,
        lastIncludedIndex, lastIncludedTerm
      };
      const manifest = {
        config: snap.config ?? null,
        files: files.map(({ role, size, crc }) => ({ role, size, crc })),
        // The member set travels with the install so a bootstrapped node
        // (whose log won't contain the CONFIG history) adopts it. This is
        // the CURRENT set, an approximation of "the set at the boundary"
        // that is exact whenever changes are committed and settled — the
        // only time snapshots should be taken anyway.
        members: this.members
      };
      const send = async (msg) => {
        const reply = await this.transport.call(peer, msg);
        if (!this.isRunning || this.role !== ROLE.LEADER || this.log.currentTerm !== term) return false;
        if (reply.term > term) { this._becomeFollower(reply.term, 0); return false; }
        return reply.success === true; // restart/false: give up, retry from the top later
      };

      if (files.length === 0) {
        installed = await send({ ...base, manifest, role: null, offset: 0, data: EMPTY, done: true });
      } else {
        let first = true;
        outer:
        for (let f = 0; f < files.length; f++) {
          const file = files[f];
          const handle = await this.snapshotter.openFile(file.role);
          try {
            let offset = 0;
            do {
              const n = Math.min(this.snapshotChunkBytes, file.size - offset);
              const data = new Uint8Array(n);
              if (n) handle.read(data, { at: offset });
              const done = f === files.length - 1 && offset + n >= file.size;
              const msg = { ...base, role: file.role, offset, data, done };
              if (first) { msg.manifest = manifest; first = false; }
              if (!await send(msg)) break outer;
              offset += n;
              if (done) installed = true;
            } while (offset < file.size);
          } finally {
            if (handle.close) await handle.close();
          }
        }
      }
      if (installed) {
        if (boundary > this._match.get(peer)) this._match.set(peer, boundary);
        this._next.set(peer, this._match.get(peer) + 1);
        this._needsSnapshot.delete(peer);
        this._advanceCommit();
      }
    } catch {
      // Peer unreachable mid-transfer; the next heartbeat starts over.
    } finally {
      this._inflight.delete(peer);
    }
    if (installed && this.role === ROLE.LEADER && this._next.get(peer) <= this.log.lastIndex) {
      this._replicate(peer);
    }
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
        else if (e.type === ENTRYLOG_TYPE.CONFIG) this._adoptConfig(decode(e.payload).members);
        this.lastApplied = e.index;
        this._settleWaiters();
      }
    }
  }

  _adoptConfig(members) {
    this._setMembers(members);
    if (!this.members.includes(this.id) && this.role !== ROLE.FOLLOWER) {
      // Applied our own removal: step down; the host closes us. (As
      // leader we first committed the entry, so the new set has it.)
      this._becomeFollower(this.log.currentTerm, 0);
    }
    if (this.role === ROLE.LEADER) {
      for (const p of this.peers) this._replicate(p); // catch new members up
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
