/**
 * raft.js — the transport-agnostic Raft core: replication roadmap step 5
 * (docs/replicaton-roadmap.md; step-4 decisions recorded there). One
 * RaftNode replicates one EntryLog (one WalDb's log — one group per
 * tenant database) across a cluster of nodes, electing a leader,
 * appending proposed commands to a quorum, and feeding committed entries
 * to a state machine in log order.
 *
 * THE STATE MACHINE IS NOT HERE ANY MORE (phase 7c). Role, term
 * transitions, the election timer and its round, the heartbeat timer, the
 * per-peer replication cursors, the commit arithmetic and the two hot RPC
 * handlers all live in C (wasm/include/raft_node.h, over raft_core.h /
 * raft_msg.h / raft_drive.h). This file is the HOST: it owns exactly the
 * things C cannot own without a JavaScript runtime, and nothing else.
 *
 *   C decides        -> queues a message in its outbox, or an "effect"
 *   the host delivers -> transport.call(peer, bytes), a promise, a file
 *   C consumes       -> onReply(corr, bytes) / onFail(corr)
 *
 * A correlation id, not a closure, ties a reply to the request that
 * caused it: a closure needs a stack to live on, an integer does not.
 * That is the whole reason `await transport.call(peer, msg)` — the single
 * line that used to keep replication in JavaScript — is gone from the
 * replication path. A host with no promises (the WASI/native server
 * binary) drives the identical state machine with a write() and a read().
 *
 * What is still JavaScript's, and why:
 *
 *   - the transport, and every promise that settles from one
 *   - the apply pump and its waiters (a state machine to apply into, a
 *     promise to resolve at read-your-writes)
 *   - the snapshot transfer, which reads FILES: the leader streams chunks
 *     it gets from `snapshotter`, the follower stages them; C's part is
 *     the chunk walk (raft_drive.h) and the cursor bookkeeping
 *   - membership orchestration: join/leave, learner promotion, the
 *     one-change-at-a-time gate, and the CONFIG scan at startup. The
 *     RULES for who is a member and who may vote are C's
 *     (raft_core.h's members_adopt/merge), fed back through
 *     RaftCore.setMembers so the voter list and the peer cursors cannot
 *     drift apart
 *   - leadership transfer (§3.10), whose fence is a host policy about
 *     proposals and whose trigger is a promise
 *   - observability: onEvent/status(), assembled from C's effects
 *
 * Deterministic by construction, still: the node never reads a clock or a
 * random source. Time arrives through tick(now)/start(now) and jitter
 * through the injected `random`, which the host passes INTO every C call
 * that can arm an election timer — a node that draws its own randomness
 * is a node the simulator cannot replay.
 *
 * The transport is one function: `call(peerId, bytes) -> Promise<bytes>`.
 * Incoming messages are handed to node.handleMessage(bytes), which
 * returns the reply bytes — the host wires the two together (a WebSocket
 * adapter in the service, an in-memory network in tests). The node never
 * owns a socket, and the transport never reads a field: it FRAMES, it
 * does not interpret. The grammar is C's (wasm/include/raft_msg.h), so
 * requestVote and appendEntries are never decoded on this side at all:
 * they go to C as the bytes they arrived as, run there against this
 * node's own log, and the reply comes back as the bytes to send.
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
 * are compacted away, and C says so with a NEEDS_SNAPSHOT effect. With a
 * `snapshotter` configured, the host streams its latest snapshot instead:
 * the manifest travels with the first chunk, then every file chunk-by-
 * chunk over the same request/response transport, each chunk awaited (a
 * stale term in any reply aborts). The follower stages chunks through
 * snapshotter.beginInstall's transaction; commit() validates and adopts
 * the whole state (its appliedIndex becomes the boundary), then the node
 * swaps its log for a fresh one based at the boundary via the `rebaseLog`
 * hook — EntryLog cannot rebase in place — and re-persists its hard state
 * onto it. Assigning `node.log` repoints the C node too (rn_set_log): it
 * holds a BORROWED pointer, so a swap it is not told about is a dangling
 * one. The commit and swap are serialized through the apply chain so an
 * in-flight apply loop can never observe the swap. Without a snapshotter,
 * such peers park in `peersNeedingSnapshot` for the host to notice.
 *
 * Membership (5d): the cluster's member set travels through the log as
 * CONFIG entries, proposed one at a time via changeMembership() and
 * adopted when APPLIED — the etcd convention rather than the paper's
 * adopt-on-append, trading the paper's edge-case arguments for
 * truncation-proof simplicity; single-server changes stay safe because
 * a change commits under the OLD quorum and the next one can't start
 * until it has. On restart the log is scanned for the last CONFIG entry
 * (static `peers` is only the bootstrap default); a snapshot install
 * adopts the leader's members from the manifest. A node that applies
 * its own removal steps down and stops campaigning — the host closes
 * it.
 *
 * Members are RECORDS, not bare ids: { id, host?, port?, ... } — the
 * CONFIG entry carries each member's address, making the log the single
 * source of truth for the cluster's shape. Every adoption path
 * (constructor, apply, restart scan, snapshot install) funnels through
 * one place and fires the `onConfig` hook with the full record list, so
 * a host keeps its transport's peer table in sync BY CONSTRUCTION —
 * there is no separate address book to drift (RaftGroupHost wires this
 * automatically; see src/raft-host.js). changeMembership() merges
 * id-only inputs with the known records so a plain [1,2,3,4] can never
 * silently erase addresses. Extra record fields ride along untouched.
 *
 * Join/leave: a node gets INTO the club by asking any member —
 * 'join'/'leave' messages ride the ordinary transport. A follower
 * answers with the leader's id AND address (it knows both from the
 * records); the leader upserts/removes the record via changeMembership
 * and replies with the adopted member list once committed. Retries are
 * idempotent: re-joining with an identical record, or leaving an
 * already-absent id, succeeds without a new CONFIG entry. The
 * joinGroup/leaveGroup helpers (src/raft-host.js) drive the
 * seed-address loop; the bootstrap (first) node should list itself
 * WITH its address in `peers` so later joiners learn it from the log.
 *
 * Learners: a NEW member always joins as a non-voter (`voting: false`
 * on its record — join forces it, so adding capacity can never thin the
 * failure margin). A learner receives everything a voter does —
 * AppendEntries, snapshot installs — but is excluded from quorum
 * arithmetic, never campaigns, and refuses votes. The leader promotes
 * it automatically: C raises a PROMOTE effect once a learner's match
 * index covers the commit index — "has everything committed" — and the
 * host proposes the same record with the flag dropped (one CONFIG at a
 * time as always; a busy moment just retries on the next success).
 * Voters-by-construction: the bootstrap set and explicit
 * changeMembership records default to voting.
 *
 * Quiescence (5d): quiesce() parks the timers (a leader stops
 * heartbeating, a follower stops counting down), wake() restores them;
 * incoming messages and local proposals wake implicitly. An idle group
 * costs nothing on the wire; a leader that dies while quiesced is
 * detected lazily, on the group's next use — the RaftGroupHost
 * (src/raft-host.js) drives both ends.
 */
import {
  ENTRYLOG_TYPE, encode, decode, raft, raftMsg, raftDrive,
  RaftCore, RAFT_ROLE, RN_EFFECT
} from '../wasm/nisaba-wasm.js';

export class NotLeaderError extends Error {
  /** @param {number} leaderId - the last known leader (0 if unknown) */
  constructor(leaderId) {
    super(leaderId ? `Not the leader (try node ${leaderId})` : 'Not the leader (no known leader)');
    this.name = 'NotLeaderError';
    this.leaderId = leaderId;
  }
}

export const ROLE = { FOLLOWER: 'follower', CANDIDATE: 'candidate', LEADER: 'leader' };

/** C's raft_role, in this file's spelling. */
const ROLE_NAME = {
  [RAFT_ROLE.FOLLOWER]: ROLE.FOLLOWER,
  [RAFT_ROLE.CANDIDATE]: ROLE.CANDIDATE,
  [RAFT_ROLE.LEADER]: ROLE.LEADER
};

const EMPTY = new Uint8Array(0);

/** raft_node.h's RAFT_ERR_PEER: a peer that is not a member, or a
 * correlation id nobody issued — never an error the host must act on. */
const RAFT_ERR_PEER = -52;

export class RaftNode {
  /**
   * @param {object} options
   * @param {number} options.id - this node's id (nonzero small integer;
   *   0 is EntryLog's "voted for nobody")
   * @param {Array<number|{id: number, host?: string, port?: number}>}
   *   options.peers - the BOOTSTRAP member set (ids or full records,
   *   self included or not; a CONFIG entry recovered from the log or a
   *   snapshot overrides it). The first node of a fresh cluster should
   *   list itself WITH its address ([{ id, host, port }]) so joiners
   *   learn it from the log.
   * @param {(members: Array<object>) => void} [options.onConfig] - fired
   *   with the full member-record list on EVERY adoption (bootstrap,
   *   apply, restart, install) — the hook that keeps a transport's peer
   *   table in sync with the log; RaftGroupHost wires it automatically.
   * @param {(event: object) => void} [options.onEvent] - observability
   *   stream: called with { type, time, node, term, ...fields } on state
   *   TRANSITIONS only (elections, role changes, config adoptions,
   *   promotions, snapshot installs, edge-triggered peer reachability,
   *   quiesce/wake, conflict truncations, the 'fault' a log or grammar
   *   failure raises where no caller is left to throw at, and the
   *   critical 'halt' when the apply loop stops on divergence) — never
   *   per-heartbeat or per-commit noise. Most of these are C's effects,
   *   relabelled here.
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
   *   and re-persists its hard state onto it). Required for a follower to
   *   accept installs.
   * @param {[number, number]} [options.electionTimeoutMs=[150,300]]
   * @param {number} [options.heartbeatMs=50]
   * @param {number} [options.maxBatchBytes=65536] - AppendEntries batch cap
   * @param {number} [options.snapshotChunkBytes=65536]
   * @param {() => number} [options.random=Math.random]
   */
  constructor({
    id, peers, log, stateMachine, transport,
    snapshotter = null, rebaseLog = null, onConfig = null, onEvent = null,
    electionTimeoutMs = [150, 300], heartbeatMs = 50,
    maxBatchBytes = 65536, snapshotChunkBytes = 65536, random = Math.random
  }) {
    if (!Number.isInteger(id) || id <= 0) throw new Error('RaftNode id must be a positive integer');
    this.id = id;
    this.onConfig = onConfig;
    this.onEvent = onEvent;
    this._log = log;
    this.stateMachine = stateMachine;
    this.transport = transport;
    this.snapshotter = snapshotter;
    this.rebaseLog = rebaseLog;
    this.electionTimeoutMs = electionTimeoutMs;
    this.heartbeatMs = heartbeatMs;
    this.maxBatchBytes = maxBatchBytes;
    this.snapshotChunkBytes = snapshotChunkBytes;
    this.random = random;

    /** The state machine itself (wasm/include/raft_node.h). Everything
     * below is the host's half; anything that looks like a Raft decision
     * is a question asked of this. */
    this._core = new RaftCore(id, log, { electionTimeoutMs, heartbeatMs, maxBatchBytes });

    this._reachable = new Map(); // peer -> bool (edge-triggered events)
    const boot = new Map();
    for (const m of peers) {
      const record = typeof m === 'number' ? { id: m } : { ...m };
      boot.set(record.id, record);
    }
    if (!boot.has(id)) boot.set(id, { id });
    /** Log index of the CONFIG entry currently in force (0 = the static
     * bootstrap peers). Guards the apply pump against REGRESSING
     * membership: the restart scan adopts the latest-in-log config —
     * possibly uncommitted, deliberately — while lastApplied resumes
     * from the state machine's floor, so the pump may re-encounter
     * OLDER committed CONFIG entries on its way up. Without this index
     * it would re-adopt them over the newer set, which un-heals exactly
     * the stuck-survivor case the scan exists for (a two-voter group
     * whose leader committed its own removal and stepped down). */
    this._configIndex = 0;
    this._setMembers([...boot.values()]);

    this.lastApplied = 0; // set in start()
    this.isRunning = false;

    this._now = 0;
    /** Shadow of the last role C reported, for edge detection only — the
     * `role` getter always asks C. */
    this._role = ROLE.FOLLOWER;
    this._needsSnapshot = new Set(); // peers parked for want of a snapshotter
    this._sending = new Set();       // peers with a snapshot transfer running
    this._waiters = [];              // propose() promises: {index, term, resolve, reject}
    this._applyChain = Promise.resolve();
    this._install = null;            // follower: in-progress install transaction
    this._exclusive = null;          // runExclusive gate promise, or null
    this._configInFlight = false;    // one membership change at a time
    this._transfer = null;           // in-flight transfer: {targetId, deadline, sent, resolve, reject}
    this._leaderAt = 0;              // this._now when we became leader
    /** Correlation ids for messages arriving from the transport. A
     * private counter, not C's: the wire carries no correlation id (the
     * transport is request/response), so the host mints one purely to
     * pick its own reply out of the outbox. */
    this._inCorr = 0;
  }

  /**
   * The EntryLog. Assigning it repoints the C node as well — it holds a
   * BORROWED pointer, and both compaction paths (a follower adopting an
   * install, a leader snapshotting under runExclusive) close the old log
   * and open a fresh one based at the boundary.
   */
  get log() { return this._log; }
  set log(next) {
    this._log = next;
    this._core.setLog(next);
  }

  /**
   * Adopt a member set — the ONE funnel every membership source goes
   * through (constructor bootstrap, CONFIG apply, restart scan, snapshot
   * install), which is what keeps address books in sync by design.
   * `input` is member records ({ id, host?, port?, ... }) or bare ids
   * (normalized to records); includes self, if still a member. The C
   * node adopts the same set — its peer cursors and voter arithmetic
   * come from there, so they cannot drift from this list.
   */
  _setMembers(input) {
    // Sorting, the voting-flag reading and the self-exclusion are C's
    // (raft_core.h): two nodes adopting the same set must derive the
    // same lists, because the quorum count is read from a position in
    // one of them, and a divergence there is a split brain.
    const { members, voters, peers } = raft.membersAdopt(input, this.id);
    // THE NODE FIRST, and it either takes the whole set or throws. If it
    // refuses, this side must adopt nothing either: these two lists are
    // one fact derived twice, and the moment they can disagree there are
    // two clusters. Assigning first and asking after is how that
    // happens, so the order here is load-bearing, not stylistic.
    this._core.setMembers(members);
    this.memberInfo = members;
    this.members = members.map((m) => m.id);
    /** Quorum electorate: members without `voting: false`. Replication
     * (this.peers) spans everyone; only voters count and campaign. */
    this.voters = voters;
    this.peers = peers;
    if (this.onConfig) {
      try { this.onConfig(this.memberInfo); } catch { /* host hook; never ours to crash on */ }
    }
    this._emit('config', { members: this.memberInfo, voters: this.voters });
  }

  /**
   * Run `fn` with the node quiesced: incoming messages queue behind the
   * gate, ticks skip, proposals wait, replies arriving from the network
   * wait, and in-flight applies drain first. The host uses this for
   * operations that swap the log out from under the node (local snapshot
   * + log compaction): inside `fn`, nothing else can touch `this.log`.
   * Reassign `node.log` inside `fn` if it swaps — the setter tells C.
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

  // ---- what C knows, in this file's vocabulary ----------------------------

  get role() { return ROLE_NAME[this._core.role]; }
  get leaderId() { return this._core.leaderId; }
  get term() { return this._log.currentTerm; }
  /** Volatile commit index; EntryLog's persisted copy is advisory (it
   * rides the next sync) and re-derived on restart. */
  get commitIndex() { return this._core.commitIndex; }
  get quiesced() { return this._core.quiesced; }
  /** The leader's replication cursors for `peer` (0 elsewhere). */
  matchOf(peer) { return this._core.matchOf(peer); }
  nextOf(peer) { return this._core.nextOf(peer); }

  /** Emit one observability event (see the onEvent option). Never
   * throws; a broken listener must not break consensus. */
  _emit(type, fields = {}) {
    if (!this.onEvent) return;
    try {
      this.onEvent({ type, time: this._now, node: this.id, term: this._log.currentTerm, ...fields });
    } catch { /* observer's problem */ }
  }

  /**
   * One JSON-able snapshot of everything this node knows about itself —
   * the "what is true right now" half of observability (the onEvent
   * stream is the "what changed" half). Peer replication state is the
   * leader's view and null elsewhere.
   */
  status() {
    const leading = this.role === ROLE.LEADER;
    return {
      id: this.id,
      role: this.role,
      term: this._log.currentTerm,
      leaderId: this.leaderId,
      isRunning: this.isRunning,
      quiesced: this.quiesced,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      configInFlight: this._configInFlight,
      log: {
        baseIndex: this._log.baseIndex,
        lastIndex: this._log.lastIndex,
        lastTerm: this._log.lastTerm,
        bytes: this._log.syncAccessHandle?.getSize?.() ?? null
      },
      members: this.memberInfo,
      voters: this.voters,
      peers: leading
        ? this.peers.map((p) => ({
            id: p,
            match: this._core.matchOf(p),
            next: this._core.nextOf(p),
            lag: this._log.lastIndex - this._core.matchOf(p),
            inflight: this._core.inflightOf(p) !== 0 || this._sending.has(p),
            needsSnapshot: this._needsSnapshot.has(p),
            reachable: this._reachable.get(p) !== false
          }))
        : null
    };
  }

  async start(now = 0) {
    this._now = now;
    this.lastApplied = await this.stateMachine.appliedIndex();
    // Both numbers are the host's: the log's advisory marker (what we
    // could prove committed before the crash) and the state machine's
    // floor. C starts at zero and would otherwise wait for a heartbeat
    // before replaying a prefix it already holds.
    this._core.seedCommit(Math.max(0, this._log.commitIndex, this.lastApplied));
    // Recover membership: the last CONFIG entry in the log wins over the
    // static bootstrap `peers` (the paper's latest-in-log rule at
    // restart; a state machine's appliedIndex may sit past CONFIG
    // entries it never recorded, so the apply pump alone can't be relied
    // on to re-adopt them, and a committed-but-unadvertised entry — the
    // advisory commit index lags — must not be missed either).
    let scan = this._log.baseIndex + 1;
    while (scan <= this._log.lastIndex) {
      const batch = this._log.getBatch(scan, this.maxBatchBytes);
      if (batch.length === 0) break;
      for (const e of batch) {
        if (e.type === ENTRYLOG_TYPE.CONFIG) {
          this._setMembers(decode(e.payload).members);
          this._configIndex = e.index;
        }
      }
      scan = batch[batch.length - 1].index + 1;
    }
    this.isRunning = true;
    this._core.start(now, this.random());
    this._flush();
    this._pumpApply();
    this._emit('started', { lastApplied: this.lastApplied, commitIndex: this.commitIndex });
  }

  async stop() {
    if (this.isRunning) this._emit('stopped');
    this.isRunning = false;
    this._core.stop();
    if (this._transfer) {
      const t = this._transfer;
      this._transfer = null;
      t.reject(new Error('node stopped during leadership transfer'));
    }
    this._rejectWaiters(new NotLeaderError(0));
    await this._applyChain.catch(() => {});
  }

  /** Release the C node. Optional — a stopped node keeps answering
   * status questions, which is why stop() does not do this; call it when
   * the host is done with the group for good. */
  free() {
    this._core.free();
  }

  /** Drive timers. Call periodically (the simulator's virtual clock, or
   * setInterval on a real host); `now` must be monotonic. */
  tick(now) {
    if (!this.isRunning || this._exclusive) return;
    this._now = Math.max(this._now, now);
    if (this._transfer && this._now >= this._transfer.deadline) {
      // The target never took over (down, unreachable, refusing) —
      // lift the fence and resume normal service; the caller retries
      // or picks another target.
      const t = this._transfer;
      this._transfer = null;
      this._emit('transfer', { phase: 'aborted', target: t.targetId });
      t.reject(new Error(`leadership transfer to node ${t.targetId} timed out; resuming normal service`));
    }
    this._core.tick(this._now, this.random());
    this._flush();
  }

  /** Park the group: a LEADER first tells every follower to park (a
   * final heartbeat with the quiesce flag — followers' election timeouts
   * are shorter than any idle threshold, so without this they would
   * misread the leader's silence as its death and churn elections), then
   * stops heartbeating. A follower parks its election countdown. Any
   * incoming message or local proposal wakes the node implicitly. */
  quiesce() {
    // The flag first: C stamps it onto whatever the next replication
    // pass builds, so these heartbeats ARE the parking order.
    this._core.quiesce();
    if (this.role === ROLE.LEADER) {
      for (const p of this.peers) this._core.replicate(p);
      this._flush();
    }
    this._emit('quiesce', { asleep: true });
  }

  wake(now = this._now) {
    if (!this._core.quiesced) return;
    this._now = Math.max(this._now, now);
    this._core.wake(this._now, this.random());
    this._emit('quiesce', { asleep: false });
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
    if (this._transfer) {
      // Transfer fence (§3.10: a transferring leader stops taking new
      // proposals). The TARGET rides as the leader hint, so rerouting
      // callers land where leadership is headed.
      return Promise.reject(new NotLeaderError(this._transfer.targetId));
    }
    this.wake(); // a quiesced leader must resume heartbeats to replicate
    const term = this._log.currentTerm;
    let index;
    try {
      // Append at the current term, sync, replicate, and run the commit
      // check — all one synchronous C call, so nothing can interleave
      // between the entry becoming durable and it counting.
      index = this._core.propose(type, payload);
    } catch (err) {
      return Promise.reject(err);
    }
    const promise = new Promise((resolve, reject) => {
      this._waiters.push({ index, term, resolve, reject });
    });
    this._flush();
    return promise;
  }

  /** Peers the leader knows are behind its log base and cannot be caught
   * up by AppendEntries — they need an InstallSnapshot (roadmap 5b) and
   * this node has no snapshotter to serve one. */
  get peersNeedingSnapshot() {
    return [...this._needsSnapshot].filter(
      (p) => this.peers.includes(p) && this._core.nextOf(p) <= this._log.baseIndex
    );
  }

  /**
   * Graceful leadership transfer (the paper's §3.10 / TimeoutNow flow —
   * the zero-data-copy rebalance a leader-skewed fleet wants). Leader
   * only. Fences NEW proposals (they reject NotLeaderError with the
   * TARGET as the leader hint, so rerouting callers land where
   * leadership is headed), brings the target fully up to date, then
   * tells it to campaign IMMEDIATELY — a real election that skips
   * pre-vote, whose leader-stickiness exists precisely to block
   * challengers while this still-live leader is heard from. The
   * target's RequestVote at term+1 makes this node step down and grant.
   *
   * Resolves once this node has actually left leadership (however that
   * happens — the target's election is the expected way). Rejects, and
   * lifts the fence so normal service resumes, if leadership hasn't
   * moved within `timeoutMs` (default 2x the max election timeout):
   * the target is down, unreachable, or refusing. In-flight proposals
   * ride the ordinary leadership-change semantics — committed entries
   * resolve, uncommitted ones reject NotLeaderError at step-down.
   * Transfer to self resolves immediately; a non-voter target is
   * refused outright (a learner cannot win the election this triggers).
   */
  transferLeadership(targetId, { timeoutMs = this.electionTimeoutMs[1] * 2 } = {}) {
    if (this._exclusive) {
      return this._exclusive.then(() => this.transferLeadership(targetId, { timeoutMs }));
    }
    if (!this.isRunning || this.role !== ROLE.LEADER) {
      return Promise.reject(new NotLeaderError(this.leaderId));
    }
    if (targetId === this.id) return Promise.resolve();
    if (!this.voters.includes(targetId)) {
      return Promise.reject(new Error(`transferLeadership: node ${targetId} is not a voting member`));
    }
    if (this._transfer) {
      return Promise.reject(new Error(`a leadership transfer to node ${this._transfer.targetId} is already in flight`));
    }
    this.wake(); // a parked leader must replicate to catch the target up
    const promise = new Promise((resolve, reject) => {
      this._transfer = { targetId, deadline: this._now + timeoutMs, sent: false, resolve, reject };
    });
    this._emit('transfer', { phase: 'started', target: targetId });
    this._core.replicate(targetId); // close the gap; every ack re-checks
    this._flush();                  // (which fires _maybeCompleteTransfer)
    return promise;
  }

  /** The transfer's trigger point, re-checked after every delivery and
   * every tick: once the TARGET's match reaches our last index it is as
   * up to date as we are, so send TimeoutNow. A failed or refused send
   * re-arms and retries on the next one (heartbeats keep those coming);
   * a target that never answers hits the tick() deadline. */
  _maybeCompleteTransfer() {
    const t = this._transfer;
    if (!t || t.sent || this.role !== ROLE.LEADER) return;
    if (this._core.matchOf(t.targetId) < this._log.lastIndex) return;
    t.sent = true;
    // Encoded, like every other message since the transport became
    // byte-oriented: handleMessage classifies the wire bytes through C
    // (raft_msg.h) and never sees a JS object.
    this.transport.call(t.targetId, encode({
      kind: 'timeoutNow', term: this._log.currentTerm, leaderId: this.id
    })).then((raw) => {
      const reply = raw && decode(raw);
      if (this._transfer === t && reply && reply.ok === false) t.sent = false;
    }).catch(() => {
      if (this._transfer === t) t.sent = false;
    });
  }

  /**
   * Propose a new member set — full replacement, as member records
   * ({ id, host?, port?, ... }) or bare ids. An id-only entry inherits
   * the currently-known record for that id, so `changeMembership([1, 2,
   * 3, 4])` can never silently erase addresses the log already carries.
   * Leader-only; one change may be in flight at a time — commit it
   * before proposing the next (the single-server-change safety argument
   * rests on changes serializing). The change takes effect on every node
   * when its CONFIG entry APPLIES; resolves like propose(). A brand-new
   * member typically needs a snapshot install to catch up — the ordinary
   * replication path handles that once it is a member.
   */
  async changeMembership(members) {
    // The merge is C's (raft_core.h): an id-only entry inherits the
    // known record, which is what stops changeMembership([1,2,3,4]) from
    // erasing the addresses the log carries -- and the log being the one
    // source of truth for the cluster's shape is what removes the
    // separate address book entirely.
    let next;
    try {
      next = raft.membersMerge(members, this.memberInfo);
    } catch (err) {
      throw new Error(
        'changeMembership requires a non-empty set of member records with positive integer ids',
        { cause: err }
      );
    }
    if (this._configInFlight) {
      throw new Error('a membership change is already in flight; wait for it to commit');
    }
    // Refuse an oversized set HERE, where a caller is standing and can
    // be told why. Proposed instead, it would commit, and then every
    // replica in the cluster would hit the same refusal at apply — where
    // the only honest response left is to halt (see _adoptConfig).
    const maxMembers = this._core.maxPeers + 1; // peers exclude self
    if (next.length > maxMembers) {
      throw new Error(
        `changeMembership: ${next.length} members exceeds this build's limit of ${maxMembers}`
      );
    }
    this._configInFlight = true;
    try {
      return await this.propose(encode({ members: next }), ENTRYLOG_TYPE.CONFIG);
    } finally {
      this._configInFlight = false;
    }
  }

  // ---- the outbox seam ----------------------------------------------------

  /**
   * Everything C decided, acted on: what it wants DONE (effects), then
   * what it wants SENT (the outbox). Effects first so an observer sees a
   * role change before the traffic that follows from it.
   *
   * Called after every call into the node, which is what makes the seam
   * work: C never blocks, so its whole answer to `tick` or `onReply` is
   * sitting in these two queues when the call returns.
   */
  _flush() {
    for (const eff of this._core.drainEffects()) this._onEffect(eff);
    for (const msg of this._core.drainOutbox()) this._send(msg);
    this._maybeCompleteTransfer();
  }

  /** One outgoing request: deliver it, then feed the answer back by
   * correlation id — or say it never came. This is the whole of what
   * `await transport.call(...)` used to do inside the state machine. */
  _send(msg) {
    // A reply the host did not ask for cannot go anywhere: this
    // transport is request/response, so replies travel as return values
    // (see _handleRaftMessage) and never as a fresh call.
    if (msg.isReply) return;
    const corr = msg.corr;
    this.transport.call(msg.peer, msg.bytes).then(
      (reply) => this._deliver((r) => this._core.onReply(corr, reply ?? EMPTY, r)),
      () => this._deliver(() => this._core.onFail(corr))
    );
  }

  /**
   * Hand something back to C from an asynchronous world. Two gates: a
   * stopped node accepts nothing, and a node under runExclusive waits —
   * its log may be mid-swap, and C holds that log by pointer.
   */
  _deliver(fn) {
    if (!this.isRunning) return;
    if (this._exclusive) {
      this._exclusive.then(() => this._deliver(fn));
      return;
    }
    const rc = fn(this.random());
    // A correlation id nobody is waiting on is RAFT_ERR_PEER and means
    // only that the round it belonged to is over. Anything else came
    // from the log or the grammar, and has to be VISIBLE rather than
    // retried forever as if the wire were down — there is nowhere to
    // throw it from inside a settled promise, so it goes to the event
    // stream, where a broken observer cannot break consensus either.
    if (rc && rc !== RAFT_ERR_PEER) this._emit('fault', { code: rc, at: 'reply' });
    this._flush();
  }

  /** What C could not do itself: apply an entry, read a snapshot file,
   * settle a promise, tell an observer. */
  _onEffect(eff) {
    switch (eff.kind) {
      case RN_EFFECT.ROLE:
        return this._onRole(ROLE_NAME[eff.arg]);
      case RN_EFFECT.COMMIT:
        return this._pumpApply();
      case RN_EFFECT.NEEDS_SNAPSHOT:
        return this._onNeedsSnapshot(eff.arg);
      case RN_EFFECT.PROMOTE:
        return this._maybePromote(eff.arg);
      case RN_EFFECT.REACHABLE:
        this._reachable.set(eff.arg, eff.flag);
        return this._emit('peer', { id: eff.arg, reachable: eff.flag });
      case RN_EFFECT.TRUNCATED:
        return this._emit('truncate', { from: eff.arg, lastIndex: this._log.lastIndex });
      case RN_EFFECT.ELECTION:
        return this._emit('election', { preVote: eff.flag, forTerm: eff.arg });
      default:
        return undefined;
    }
  }

  /**
   * A role transition, with the three consequences C cannot reach: the
   * promises a departing leader owes, an in-flight transfer's goal
   * state, and the event stream.
   */
  _onRole(role) {
    const wasLeader = this._role === ROLE.LEADER;
    this._role = role;
    if (role === ROLE.LEADER) {
      this._leaderAt = this._now;
      this._needsSnapshot.clear();
    }
    if (wasLeader && role !== ROLE.LEADER) {
      if (this._transfer) {
        // Leadership has left this node — the transfer's goal state
        // (normally via the target's election; any other usurper makes
        // the transfer moot the same way).
        const t = this._transfer;
        this._transfer = null;
        this._emit('transfer', { phase: 'finished', target: t.targetId });
        t.resolve();
      }
      this._rejectWaiters(new NotLeaderError(this.leaderId));
    }
    if (role === ROLE.CANDIDATE) this._abortInstall(); // a half-staged install belongs to the old world
    this._emit('role', { role, leaderId: this.leaderId, wasLeader });
  }

  /** A peer whose next index has fallen below our log base: only an
   * install can catch it up. With a snapshot to serve, stream it; without
   * one, park the peer where the host can see it. */
  _onNeedsSnapshot(peer) {
    if (this.snapshotter && this.snapshotter.latest()) {
      this._needsSnapshot.delete(peer);
      this._sendSnapshot(peer);
      return;
    }
    this._needsSnapshot.add(peer);
  }

  // ---- RPC handlers -------------------------------------------------------

  /** The transport hands every incoming message here; the return value is
   * the reply (a promise for installSnapshot; synchronous otherwise —
   * all log operations are). */
  handleMessage(bytes) {
    if (this._exclusive) {
      return this._exclusive.then(() => this.handleMessage(bytes));
    }
    if (!this.isRunning) throw new Error('node is stopped');
    this.wake(); // any traffic un-quiesces the group on this node
    // The grammar is C's (raft_msg.h): the kind comes back as a number,
    // and for the two hot handlers the message is never decoded on this
    // side at all -- it goes to C as the bytes it arrived as, and the
    // reply comes back as the bytes to send.
    const kind = raftMsg.kind(bytes);
    if (kind < 0) throw new Error('raft: unrecognized message');
    switch (kind) {
      case raftMsg.KIND.REQUEST_VOTE:
      case raftMsg.KIND.APPEND_ENTRIES:
        return this._handleRaftMessage(bytes);
      case raftMsg.KIND.INSTALL_SNAPSHOT: return this._onInstallSnapshot(decode(bytes));
      case raftMsg.KIND.JOIN: return this._onJoin(decode(bytes));
      case raftMsg.KIND.TIMEOUT_NOW: return this._onTimeoutNow(decode(bytes));
      default: return this._onLeave(decode(bytes));
    }
  }

  /**
   * §5.2/§5.4.1 and §5.3, entirely in C (raft_msg.h, driven by
   * raft_node.h): the consistency check, the conflict rule, the vote
   * decision, the appends and their sync all run against this node's own
   * log inside one synchronous call, with the message never leaving the
   * buffer it arrived in and the reply never becoming a JS object.
   *
   * The reply comes back through the outbox like anything else, tagged
   * with the correlation id minted below — the host picks its own out
   * and returns it; anything else queued alongside (there is normally
   * nothing) goes on the wire.
   */
  _handleRaftMessage(bytes) {
    const corr = (this._inCorr = (this._inCorr + 1) >>> 0) || 1;
    // `from` is 0: this transport does not name its sender, and C needs
    // it only to address the reply — which the host is about to take
    // back out of the outbox by correlation id rather than send.
    const rc = this._core.handle(0, corr, bytes, this.random());
    if (rc !== 0) throw new Error(`raft: message refused (${rc})`);
    let reply = EMPTY;
    for (const msg of this._core.drainOutbox()) {
      if (msg.isReply && msg.corr === corr) reply = msg.bytes;
      else this._send(msg);
    }
    for (const eff of this._core.drainEffects()) this._onEffect(eff);
    this._maybeCompleteTransfer();
    return reply;
  }

  /** TimeoutNow (§3.10): the transferring leader certifies we are fully
   * caught up and asks us to campaign NOW — a real election, skipping
   * pre-vote, whose leader-stickiness exists precisely to block
   * challengers while that leader still lives. Refused when
   * stale-termed, when we already lead, or when we hold no franchise
   * (a learner cannot win the election this would start). */
  _onTimeoutNow(msg) {
    const currentTerm = this._log.currentTerm;
    if (msg.term < currentTerm) return encode({ term: currentTerm, ok: false });
    if (this.role === ROLE.LEADER || !this.voters.includes(this.id)) {
      return encode({ term: currentTerm, ok: false });
    }
    this._core.campaign(this.random());
    this._flush();
    return encode({ term: this._log.currentTerm, ok: true });
  }

  /**
   * Handle a join request ({ kind: 'join', member: { id, host, port,
   * ... } }) — sent to ANY member; a non-leader answers with the
   * leader's id and address so the joiner can retry there. The leader
   * upserts the record and replies { ok: true, members } once the
   * CONFIG entry commits. Idempotent: re-joining with an identical
   * record succeeds without a new entry (safe to retry).
   *
   * A NEW member always enters as a learner (voting: false), whatever
   * its request claims — adding capacity must never thin the failure
   * margin, and the leader promotes it automatically once caught up
   * (_maybePromote). A re-join of an EXISTING member (address change,
   * restart-with-retry) keeps its current voting status: an established
   * voter is not demoted by re-announcing itself.
   */
  _onJoin(msg) {
    const member = msg.member;
    if (!member || !Number.isInteger(member.id) || member.id <= 0) {
      return encode({ ok: false, error: 'join requires member { id, host, port }' });
    }
    if (this.role !== ROLE.LEADER) return this._redirectToLeader();
    const existing = this.memberInfo.find((m) => m.id === member.id);
    if (existing && existing.host === member.host && existing.port === member.port) {
      return encode({ ok: true, members: this.memberInfo });
    }
    if (this._configInFlight) return encode({ ok: false, retry: true });
    const record = { ...member };
    delete record.voting;
    if (existing) {
      if (existing.voting === false) record.voting = false; // still a learner
    } else {
      record.voting = false; // new blood starts as a learner, always
    }
    const next = [...this.memberInfo.filter((m) => m.id !== member.id), record];
    return this.changeMembership(next).then(
      () => encode({ ok: true, members: this.memberInfo }),
      (err) => encode({ ok: false, error: String(err?.message ?? err) })
    );
  }

  /** Handle a leave request ({ kind: 'leave', id }) — graceful
   * decommission from the departing node itself, or an admin removing a
   * dead one. Same redirect/idempotence rules as join. */
  _onLeave(msg) {
    if (!Number.isInteger(msg.id) || msg.id <= 0) {
      return encode({ ok: false, error: 'leave requires a member id' });
    }
    if (this.role !== ROLE.LEADER) return this._redirectToLeader();
    if (!this.members.includes(msg.id)) {
      return encode({ ok: true, members: this.memberInfo });
    }
    if (this._configInFlight) return encode({ ok: false, retry: true });
    const next = this.memberInfo.filter((m) => m.id !== msg.id);
    return this.changeMembership(next).then(
      () => encode({ ok: true, members: this.memberInfo }),
      (err) => encode({ ok: false, error: String(err?.message ?? err) })
    );
  }

  /** Every reply leaves this node as bytes -- the transport frames, it
   * does not interpret (raft_msg.h). The handlers still written here
   * encode their own; the two in C already return bytes. */
  _redirectToLeader() {
    const leader = this.memberInfo.find((m) => m.id === this.leaderId);
    return encode({
      ok: false,
      leaderId: this.leaderId,
      leaderAddress: leader && leader.host !== undefined
        ? { host: leader.host, port: leader.port }
        : null
    });
  }

  /**
   * InstallSnapshot, the one message class the host answers: it writes
   * FILES, which is the whole reason it did not move. The term rules
   * around it are still C's — rn_observe_leader adopts the leader's term
   * exactly as an AppendEntries would, and refuses a stale one.
   */
  async _onInstallSnapshot(msg) {
    if (!this._core.observeLeader(msg.term, msg.leaderId, this.random())) {
      return encode({ term: this._log.currentTerm, success: false });
    }
    this._flush(); // a step-down is a role change the host has to see

    if (!this.snapshotter || !this.rebaseLog) {
      return encode({ term: this._log.currentTerm, success: false });
    }

    if (msg.manifest) {
      // First chunk of a (re)started install: supersede anything staged.
      this._emit('install', { phase: 'started', lastIncludedIndex: msg.lastIncludedIndex, from: msg.leaderId });
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
      return encode({ term: this._log.currentTerm, success: false, restart: true });
    }
    if (msg.role != null && msg.data && msg.data.length) {
      await install.tx.writeChunk(msg.role, msg.offset, msg.data);
    }

    if (msg.done) {
      // Commit + log swap serialize through the apply chain so an
      // in-flight apply loop can never observe the swap mid-batch.
      const finish = async () => {
        await install.tx.commit(); // validate + adopt into the state machine
        const term = this._log.currentTerm;
        const votedFor = this._log.votedFor;
        await this._log.close();
        // The setter repoints C too: it borrowed the log we just closed.
        this.log = await this.rebaseLog(msg.lastIncludedIndex, msg.lastIncludedTerm);
        if (term > 0) this._log.setHardState(term, votedFor); // fresh logs start at 0/0
        this.lastApplied = msg.lastIncludedIndex;
        this._core.seedCommit(msg.lastIncludedIndex);
        if (install.members) {
          this._setMembers(install.members);
          this._configIndex = msg.lastIncludedIndex; // the manifest's set stands in for every CONFIG at or below the boundary
        }
        this._install = null;
      };
      const run = this._applyChain.then(finish);
      this._applyChain = run.catch(() => {});
      try {
        await run;
        this._emit('install', { phase: 'finished', lastIncludedIndex: msg.lastIncludedIndex });
      } catch (err) {
        // A failed commit (e.g. checksum mismatch on a corrupted
        // transfer) adopts nothing; have the leader start over.
        this._install = null;
        this._emit('install', { phase: 'failed', lastIncludedIndex: msg.lastIncludedIndex, error: String(err?.message ?? err) });
        return encode({ term: this._log.currentTerm, success: false, restart: true });
      }
    }
    return encode({ term: this._log.currentTerm, success: true });
  }

  async _abortInstall() {
    const install = this._install;
    this._install = null;
    if (install) {
      try { await install.tx.abort(); } catch { /* best-effort */ }
    }
  }

  // ---- snapshot streaming (leader) ----------------------------------------

  /** Stream the latest snapshot to a peer, chunk by chunk, each chunk
   * awaited: the manifest rides the first chunk, `done` marks the last
   * chunk of the last file. On success the peer stands at the snapshot
   * boundary and ordinary AppendEntries resumes from there. */
  async _sendSnapshot(peer) {
    if (this._sending.has(peer)) return;
    this._sending.add(peer);
    const term = this._log.currentTerm;
    let installed = false;
    let boundary = 0;
    this._emit('send-snapshot', { phase: 'started', peer });
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
        // The member records (ids AND addresses) travel with the install
        // so a bootstrapped node — whose log won't contain the CONFIG
        // history — adopts the whole cluster shape. This is the CURRENT
        // set, an approximation of "the set at the boundary" that is
        // exact whenever changes are committed and settled — the only
        // time snapshots should be taken anyway.
        members: this.memberInfo
      };
      const send = async (msg) => {
        const reply = decode(await this.transport.call(peer, encode(msg)));
        if (!this.isRunning || this.role !== ROLE.LEADER || this._log.currentTerm !== term) return false;
        if (reply.term > term) {
          // A reply the host awaited, carrying a higher term: C decides
          // what that costs us, as it does for every other reply.
          this._core.stepDown(reply.term, this.random());
          this._flush();
          return false;
        }
        return reply.success === true; // restart/false: give up, retry from the top later
      };

      // The chunk walk is C's (raft_drive.h): which file, which offset,
      // which chunk carries the manifest and which ends the stream. It
      // replaces a doubly-nested loop with a labelled break, a `first`
      // flag mutated across both levels, and a `done` computed from two
      // indices at once. The cursor comes BACK from C rather than being
      // derived here -- offset + len cannot distinguish "sent the empty
      // file" from "have not", which is an infinite loop on any snapshot
      // whose last file is empty.
      const sizes = files.map((f) => f.size);
      let handle = null, openRole = null;
      try {
        let cursor = { file: 0, offset: 0 };
        for (;;) {
          const c = raftDrive.chunkNext(sizes, this.snapshotChunkBytes, cursor.file, cursor.offset);
          if (!c) break;
          const file = files[c.fileIndex] ?? null;

          // One handle at a time, held across the chunks of its own file.
          if (file && openRole !== file.role) {
            if (handle?.close) await handle.close();
            handle = await this.snapshotter.openFile(file.role);
            openRole = file.role;
          }
          const data = new Uint8Array(c.len);
          if (c.len) handle.read(data, { at: c.offset });

          const msg = { ...base, role: file ? file.role : null, offset: c.offset, data, done: c.isDone };
          if (c.isFirst) msg.manifest = manifest;
          if (!await send(msg)) break;
          if (c.isDone) { installed = true; break; }
          cursor = { file: c.nextFile, offset: c.nextOffset };
        }
      } finally {
        if (handle?.close) await handle.close();
      }
      if (installed) {
        // Where the peer now stands, and what that lets us commit: C's
        // (raft_drive.h's raft_repl_installed — match never regresses, a
        // peer already past the snapshot keeps what it had).
        this._core.installed(peer, boundary);
        this._needsSnapshot.delete(peer);
        this._flush();
      }
    } catch {
      // Peer unreachable mid-transfer; the next heartbeat starts over.
    } finally {
      this._sending.delete(peer);
      this._emit('send-snapshot', { phase: installed ? 'finished' : 'failed', peer, boundary });
    }
    if (installed && this.role === ROLE.LEADER && this._core.nextOf(peer) <= this._log.lastIndex) {
      this._core.replicate(peer);
      this._flush();
    }
  }

  /**
   * Promote a caught-up learner: C raises the PROMOTE effect once its
   * match index covers everything committed, and the host proposes the
   * same record with the voting flag dropped. A refused moment (another
   * CONFIG in flight) simply retries on the next effect. Promotion is
   * the only automatic membership change; it can only ever WIDEN the
   * electorate with a replica proven current.
   */
  _maybePromote(peer) {
    if (this.role !== ROLE.LEADER || this._configInFlight) return;
    const record = this.memberInfo.find((m) => m.id === peer);
    if (!record || record.voting !== false) return;
    // Explicit voting:true (not just dropping the flag): changeMembership
    // merges address-less records with the known ones, and an absent key
    // would re-inherit the old voting:false through that merge.
    this._emit('promote', { id: peer, match: this._core.matchOf(peer) });
    const next = this.memberInfo.map((m) => (m.id === peer ? { ...m, voting: true } : m));
    this.changeMembership(next).catch(() => { /* retried on the next success */ });
  }

  /** Majority of VOTERS — learners add capacity, not quorum weight. */
  get quorum() { return this._core.quorum; }

  /**
   * Check-quorum: has a quorum of voters (this node included) answered
   * this leader within `withinMs`? Raft's safety argument never required
   * this — a partitioned leader cannot COMMIT anything — but a caller
   * that reads the leader's local state without committing gets a stale
   * answer presented as authoritative, so anything serving reads off
   * `role === 'leader'` needs to ask this too. Writes benefit as well:
   * refusing early beats a propose() that can never resolve.
   *
   * Always true for a non-leader (nothing to vouch for) and for a
   * single-voter group (there is nobody to hear from). `false` does NOT
   * mean deposed — it means "cannot currently prove leadership", which
   * for a freshly woken quiesced group is a transient state the caller
   * should give heartbeats a moment to clear.
   */
  hasQuorumContact(withinMs) {
    return this._core.hasQuorumContact(withinMs);
  }

  /** How long this node has been leader, in its own clock (0 if not leader). */
  get leaderForMs() {
    return this.role === ROLE.LEADER ? this._now - this._leaderAt : 0;
  }

  // ---- apply --------------------------------------------------------------

  _pumpApply() {
    this._applyChain = this._applyChain.then(() => this._applyLoop()).catch((err) => {
      // A state machine that throws on a committed entry is unrecoverable
      // divergence; stop rather than skip (skipping would fork replicas).
      this.isRunning = false;
      this._core.stop();
      this._emit('halt', { error: String(err?.message ?? err), lastApplied: this.lastApplied, commitIndex: this.commitIndex });
      this._rejectWaiters(err);
    });
  }

  async _applyLoop() {
    while (this.isRunning && this.lastApplied < this.commitIndex) {
      const commitIndex = this.commitIndex;
      for (const e of this._log.getBatch(this.lastApplied + 1, this.maxBatchBytes)) {
        if (e.index > commitIndex) break;
        if (e.type === ENTRYLOG_TYPE.NORMAL) await this.stateMachine.apply(e);
        // The index guard skips CONFIG entries older than the one in
        // force (adopted by the restart scan or a snapshot install) —
        // replaying them would regress membership; see _configIndex.
        else if (e.type === ENTRYLOG_TYPE.CONFIG && e.index >= this._configIndex) this._adoptConfig(decode(e.payload).members, e.index);
        this.lastApplied = e.index;
        this._settleWaiters();
      }
    }
  }

  /**
   * A committed CONFIG entry, applied. _setMembers throws if the node
   * refuses the set (malformed, or larger than this build can hold), and
   * the throw is deliberately not caught: it reaches the apply pump,
   * which halts. Halting is the honest answer — every replica refuses
   * the same entry for the same reason, so the cluster stops together
   * instead of one node quietly replicating to a different membership
   * than the rest. changeMembership refuses such a set before it can
   * ever be proposed; reaching here means it arrived some other way.
   */
  _adoptConfig(members, index) {
    this._setMembers(members);
    this._configIndex = index;
    if (!this.voters.includes(this.id) && this.role !== ROLE.FOLLOWER) {
      // Applied our own removal (or demotion to learner): step down; the
      // host closes a removed node. (As leader we first committed the
      // entry, so the new set has it.)
      this._core.stepDown(this._log.currentTerm, this.random());
    }
    if (this.role === ROLE.LEADER) {
      for (const p of this.peers) this._core.replicate(p); // catch new members up
    }
    this._flush();
  }

  _settleWaiters() {
    if (this._waiters.length === 0) return;
    const rest = [];
    for (const w of this._waiters) {
      if (w.index > this.lastApplied) { rest.push(w); continue; }
      // Applied at that index — but was it OUR entry, or did a new
      // leader's conflicting entry overwrite it before commit?
      if (this._log.termAt(w.index) === w.term) w.resolve({ index: w.index, term: w.term });
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
