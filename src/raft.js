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
 * The transport is one function: `call(peerId, bytes) -> Promise<bytes>`.
 * Incoming messages are handed to node.handleMessage(bytes), which
 * returns the reply bytes — the host wires the two together (a WebSocket
 * adapter in the service, an in-memory network in tests). The node never
 * owns a socket, and the transport never reads a field: it FRAMES, it
 * does not interpret. The grammar is C's (wasm/include/raft_msg.h), so
 * the two hot RPCs are never decoded on this side at all — a
 * requestVote or appendEntries goes to C as the bytes it arrived as,
 * runs there against this node's own log, and the reply comes back as
 * the bytes to send. That is what a host with no JavaScript needs, and
 * it is also what stopped every AppendEntries batch crossing the bridge
 * twice.
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
 * silently erase addresses. Extra record fields ride along untouched —
 * `voting: false` (learners) is the planned next occupant.
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
 * it automatically: on every replication success it checks whether a
 * learner's match index covers the commit index — "has everything
 * committed" — and if so proposes the same record with the flag
 * dropped (one CONFIG at a time as always; a busy moment just retries
 * on the next success). Voters-by-construction: the bootstrap set and
 * explicit changeMembership records default to voting.
 *
 * Quiescence (5d): quiesce() parks the timers (a leader stops
 * heartbeating, a follower stops counting down), wake() restores them;
 * incoming messages and local proposals wake implicitly. An idle group
 * costs nothing on the wire; a leader that dies while quiesced is
 * detected lazily, on the group's next use — the RaftGroupHost
 * (src/raft-host.js) drives both ends.
 */
import { ENTRYLOG_TYPE, encode, decode, raft, raftMsg, raftDrive } from '../wasm/nisaba-wasm.js';

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
   *   quiesce/wake, conflict truncations, and the critical 'halt' when
   *   the apply loop stops on divergence) — never per-heartbeat or
   *   per-commit noise. RaftGroupHost aggregates these across groups;
   *   RaftMonitor (src/raft-monitor.js) serves them over HTTP/SSE.
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
    snapshotter = null, rebaseLog = null, onConfig = null, onEvent = null,
    electionTimeoutMs = [150, 300], heartbeatMs = 50,
    maxBatchBytes = 65536, snapshotChunkBytes = 65536, random = Math.random
  }) {
    if (!Number.isInteger(id) || id <= 0) throw new Error('RaftNode id must be a positive integer');
    this.id = id;
    this.onConfig = onConfig;
    this.onEvent = onEvent;
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
    this._transfer = null;        // in-flight leadership transfer: {targetId, deadline, sent, resolve, reject}
    /** Leader bookkeeping for check-quorum (see hasQuorumContact): when
     * each peer last answered us without deposing us, and when we became
     * leader. `_reachable` cannot serve this purpose — it is edge-
     * triggered on a FAILED call, so a leader that simply hasn't tried
     * recently still reads reachable. */
    this._ackAt = new Map();      // peer -> this._now of its last non-deposing reply
    this._leaderAt = 0;           // this._now when we became leader
  }

  /**
   * Adopt a member set — the ONE funnel every membership source goes
   * through (constructor bootstrap, CONFIG apply, restart scan, snapshot
   * install), which is what keeps address books in sync by design.
   * `input` is member records ({ id, host?, port?, ... }) or bare ids
   * (normalized to records); includes self, if still a member. Leader
   * bookkeeping follows the set, and `onConfig` fires with the records.
   */
  _setMembers(input) {
    // Sorting, the voting-flag reading and the self-exclusion are C's
    // (raft_core.h): two nodes adopting the same set must derive the
    // same lists, because the quorum count is read from a position in
    // one of them, and a divergence there is a split brain.
    const { members, voters, peers } = raft.membersAdopt(input, this.id);
    this.memberInfo = members;
    this.members = members.map((m) => m.id);
    /** Quorum electorate: members without `voting: false`. Replication
     * (this.peers) spans everyone; only voters count and campaign. */
    this.voters = voters;
    this.peers = peers;
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
    if (this.onConfig) {
      try { this.onConfig(this.memberInfo); } catch { /* host hook; never ours to crash on */ }
    }
    this._emit('config', { members: this.memberInfo, voters: this.voters });
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

  /** Emit one observability event (see the onEvent option). Never
   * throws; a broken listener must not break consensus. */
  _emit(type, fields = {}) {
    if (!this.onEvent) return;
    try {
      this.onEvent({ type, time: this._now, node: this.id, term: this.log.currentTerm, ...fields });
    } catch { /* observer's problem */ }
  }

  /**
   * One JSON-able snapshot of everything this node knows about itself —
   * the "what is true right now" half of observability (the onEvent
   * stream is the "what changed" half). Peer replication state is the
   * leader's view and null elsewhere.
   */
  status() {
    return {
      id: this.id,
      role: this.role,
      term: this.log.currentTerm,
      leaderId: this.leaderId,
      isRunning: this.isRunning,
      quiesced: this._quiesced,
      commitIndex: this.commitIndex,
      lastApplied: this.lastApplied,
      configInFlight: this._configInFlight,
      log: {
        baseIndex: this.log.baseIndex,
        lastIndex: this.log.lastIndex,
        lastTerm: this.log.lastTerm,
        bytes: this.log.syncAccessHandle?.getSize?.() ?? null
      },
      members: this.memberInfo,
      voters: this.voters,
      peers: this.role === ROLE.LEADER
        ? this.peers.map((p) => ({
            id: p,
            match: this._match.get(p) ?? 0,
            next: this._next.get(p) ?? 0,
            lag: this.log.lastIndex - (this._match.get(p) ?? 0),
            inflight: this._inflight.has(p),
            needsSnapshot: this._needsSnapshot.has(p),
            reachable: this._reachable.get(p) !== false
          }))
        : null
    };
  }

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
        if (e.type === ENTRYLOG_TYPE.CONFIG) {
          this._setMembers(decode(e.payload).members);
          this._configIndex = e.index;
        }
      }
      scan = batch[batch.length - 1].index + 1;
    }
    this.isRunning = true;
    this._resetElectionTimer();
    this._pumpApply();
    this._emit('started', { lastApplied: this.lastApplied, commitIndex: this.commitIndex });
  }

  async stop() {
    if (this.isRunning) this._emit('stopped');
    this.isRunning = false;
    if (this._transfer) {
      const t = this._transfer;
      this._transfer = null;
      t.reject(new Error('node stopped during leadership transfer'));
    }
    this._rejectWaiters(new NotLeaderError(0));
    await this._applyChain.catch(() => {});
  }

  /** Drive timers. Call periodically (the simulator's virtual clock, or
   * setInterval on a real host); `now` must be monotonic. */
  tick(now) {
    if (!this.isRunning || this._exclusive) return;
    this._now = Math.max(this._now, now);
    // Removed nodes and learners never campaign (leaders still heartbeat).
    if (!this.voters.includes(this.id) && this.role !== ROLE.LEADER) return;
    if (this.role === ROLE.LEADER) {
      if (this._transfer && this._now >= this._transfer.deadline) {
        // The target never took over (down, unreachable, refusing) —
        // lift the fence and resume normal service; the caller retries
        // or picks another target.
        const t = this._transfer;
        this._transfer = null;
        this._emit('transfer', { phase: 'aborted', target: t.targetId });
        t.reject(new Error(`leadership transfer to node ${t.targetId} timed out; resuming normal service`));
      }
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
    this._emit('quiesce', { asleep: true });
  }

  wake(now = this._now) {
    if (!this._quiesced) return;
    this._quiesced = false;
    this._emit('quiesce', { asleep: false });
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
    if (this._transfer) {
      // Transfer fence (§3.10: a transferring leader stops taking new
      // proposals). The TARGET rides as the leader hint, so rerouting
      // callers land where leadership is headed.
      return Promise.reject(new NotLeaderError(this._transfer.targetId));
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
    this._maybeCompleteTransfer(targetId); // already caught up -> TimeoutNow right away
    this._replicate(targetId);            // else close the gap; the ack re-checks
    return promise;
  }

  /** The transfer's trigger point, called from every successful
   * replication ack: once the TARGET's match reaches our last index it
   * is as up to date as we are, so send TimeoutNow. A failed or refused
   * send re-arms and retries on the next ack (heartbeats keep those
   * coming); a target that never answers hits the tick() deadline. */
  _maybeCompleteTransfer(peer) {
    const t = this._transfer;
    if (!t || t.sent || peer !== t.targetId) return;
    if ((this._match.get(peer) ?? 0) < this.log.lastIndex) return;
    t.sent = true;
    // Encoded, like every other message since the transport became
    // byte-oriented: handleMessage classifies the wire bytes through C
    // (raft_msg.h) and never sees a JS object.
    this.transport.call(peer, encode({ kind: 'timeoutNow', term: this.log.currentTerm, leaderId: this.id }))
      .then((raw) => {
        const reply = raw && decode(raw);
        if (this._transfer === t && reply && reply.ok === false) t.sent = false;
      })
      .catch(() => {
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
    this._configInFlight = true;
    try {
      return await this.propose(encode({ members: next }), ENTRYLOG_TYPE.CONFIG);
    } finally {
      this._configInFlight = false;
    }
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
   * does not interpret (raft_msg.h). The three handlers still written
   * here encode their own; the two in C already return bytes. */
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
      case raftMsg.KIND.REQUEST_VOTE: return this._onRequestVote(bytes);
      case raftMsg.KIND.APPEND_ENTRIES: return this._onAppendEntries(bytes);
      case raftMsg.KIND.INSTALL_SNAPSHOT: return this._onInstallSnapshot(decode(bytes));
      case raftMsg.KIND.JOIN: return this._onJoin(decode(bytes));
      case raftMsg.KIND.TIMEOUT_NOW: return this._onTimeoutNow(decode(bytes));
      default: return this._onLeave(decode(bytes));
    }
  }

  /** TimeoutNow (§3.10): the transferring leader certifies we are fully
   * caught up and asks us to campaign NOW — a real election, skipping
   * pre-vote, whose leader-stickiness exists precisely to block
   * challengers while that leader still lives. Refused when
   * stale-termed, when we already lead, or when we hold no franchise
   * (a learner cannot win the election this would start). */
  _onTimeoutNow(msg) {
    const currentTerm = this.log.currentTerm;
    if (msg.term < currentTerm) return encode({ term: currentTerm, ok: false });
    if (this.role === ROLE.LEADER || !this.voters.includes(this.id)) {
      return encode({ term: currentTerm, ok: false });
    }
    this._startElection(false);
    return encode({ term: this.log.currentTerm, ok: true });
  }

  /**
   * §5.2/§5.4.1. The decision is C's (raft_core.h's raft_decide_vote);
   * this gathers the inputs and carries out the answer IN THE ORDER
   * GIVEN — step down, then persist, then reply — because that order is
   * itself a safety rule: a vote must reach the disk before the reply
   * leaves, or a machine that loses power can vote twice in one term.
   */
  /** The volatile state the C handlers read (raft_msg.h). */
  _msgState() {
    return {
      selfId: this.id,
      isFollower: this.role === ROLE.FOLLOWER,
      isLeader: this.role === ROLE.LEADER,
      selfIsVoter: this.voters.includes(this.id),
      leaderId: this.leaderId,
      commitIndex: this.commitIndex,
      now: this._now,
      lastLeaderContact: this._lastLeaderContact,
      minElectionTimeout: this.electionTimeoutMs[0]
    };

  }

  /** Adopt what a C handler changed. Everything durable already
   * happened inside the call; these are the volatile fields the node
   * still owns. */
  _applyEffect(eff) {
    // Through _becomeFollower, not a copy of its body. This used to
    // inline the three lines it needed, which was fine until stepping
    // down grew a fourth thing to do -- resolving an in-flight
    // leadership transfer (section 3.10) -- and only one of the two
    // step-down paths learned about it. The C handlers reach this one.
    //
    // The term is already durable: C persisted it before returning, so
    // _becomeFollower's `term > currentTerm` guard is false here and it
    // does not write again.
    if (eff.becameFollower) this._becomeFollower(this.log.currentTerm, eff.leaderId);
    if (eff.touchedLeader) {
      this.leaderId = eff.leaderId;
      this._lastLeaderContact = eff.lastLeaderContact;
    }
    if (eff.resetTimer) this._resetElectionTimer();
    if (eff.truncatedFrom) {
      this._emit('truncate', { from: eff.truncatedFrom, oldLastIndex: this.log.lastIndex });
    }
    if (eff.commitIndex) {
      this.commitIndex = eff.commitIndex;
      this._pumpApply();
    }
    if (eff.quiesce) {
      // The leader is parking the group: park with it (see quiesce()).
      this._quiesced = true;
      this._electionDeadline = Infinity;
    }
  }

  /**
   * §5.2/§5.4.1, entirely in C (raft_msg.h): the message arrives as the
   * bytes it came in on, the vote reaches the disk before the call
   * returns, and the reply leaves as the bytes to send back. Nothing on
   * this side reads a field of either.
   */
  _onRequestVote(bytes) {
    const { reply, effect } = raftMsg.requestVote(this.log, this._msgState(), bytes);
    this._applyEffect(effect);
    return reply;
  }

  /**
   * §5.3, entirely in C (raft_msg.h): the consistency check, the
   * conflict rule, the append and the sync all run against this node's
   * own log inside one synchronous call, with the entries never leaving
   * the buffer they arrived in.
   */
  _onAppendEntries(bytes) {
    const { reply, effect } = raftMsg.appendEntries(this.log, this._msgState(), bytes);
    this._applyEffect(effect);
    return reply;
  }

  async _onInstallSnapshot(msg) {
    const currentTerm = this.log.currentTerm;
    if (msg.term < currentTerm) return encode({ term: currentTerm, success: false });
    if (msg.term > currentTerm || this.role !== ROLE.FOLLOWER) {
      this._becomeFollower(msg.term, msg.leaderId);
    }
    this.leaderId = msg.leaderId;
    this._lastLeaderContact = this._now;
    this._resetElectionTimer();

    if (!this.snapshotter || !this.rebaseLog) {
      return encode({ term: this.log.currentTerm, success: false });
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
      return encode({ term: this.log.currentTerm, success: false, restart: true });
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
        return encode({ term: this.log.currentTerm, success: false, restart: true });
      }
    }
    return encode({ term: this.log.currentTerm, success: true });
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
    const changed = this.role !== ROLE.FOLLOWER || term > this.log.currentTerm;
    if (term > this.log.currentTerm) this.log.setHardState(term);
    const wasLeader = this.role === ROLE.LEADER;
    this.role = ROLE.FOLLOWER;
    this.leaderId = leaderId;
    this._resetElectionTimer();
    if (wasLeader && this._transfer) {
      // Leadership has left this node — the transfer's goal state
      // (normally via the target's election; any other usurper makes
      // the transfer moot the same way).
      const t = this._transfer;
      this._transfer = null;
      this._emit('transfer', { phase: 'finished', target: t.targetId });
      t.resolve();
    }
    if (wasLeader) this._rejectWaiters(new NotLeaderError(leaderId));
    if (changed) this._emit('role', { role: this.role, leaderId, wasLeader });
  }

  _startElection(preVote) {
    if (this.role === ROLE.LEADER) return;
    const term = this.log.currentTerm + 1;
    if (!preVote) {
      this.log.setHardState(term, this.id); // persist term + self-vote first
      this.role = ROLE.CANDIDATE;
      this.leaderId = 0;
      this._abortInstall(); // a half-staged install belongs to the old world
      this._emit('role', { role: this.role });
    }
    this._emit('election', { preVote, forTerm: term });
    this._resetElectionTimer();
    const quorum = this._quorum();
    if (quorum === 1) {
      return preVote ? this._startElection(false) : this._becomeLeader();
    }
    const req = raftMsg.buildRequestVote(
      term, this.id, this.log.lastIndex, this.log.lastTerm, preVote
    );

    // The tally is C's (raft_drive.h). Four guards decide whether a
    // reply still belongs to the world it was asked in -- the node may
    // have stepped down, won, or moved on a term while it was in
    // flight -- and getting one wrong is a node declaring victory on
    // votes cast for a different term, which is a split brain reached
    // through liveness code rather than a safety rule.
    const round = raftDrive.round(term, quorum, preVote);
    const votingPeers = this.peers.filter((p) => this.voters.includes(p));
    let live = votingPeers.length;
    const done = () => { if (--live <= 0) round.free(); };

    for (const p of votingPeers) {
      this.transport.call(p, req).then((raw) => {
        if (!this.isRunning || round.settled) return;
        const reply = decode(raw);
        const { action, term: stepTerm } = round.onReply(
          reply.term, reply.voteGranted === true,
          {
            currentTerm: this.log.currentTerm,
            isLeader: this.role === ROLE.LEADER,
            isCandidate: this.role === ROLE.CANDIDATE
          }
        );
        switch (action) {
          case raftDrive.ROUND.STEP_DOWN: return this._becomeFollower(stepTerm, 0);
          case raftDrive.ROUND.WON:
            return preVote ? this._startElection(false) : this._becomeLeader();
          default: return; // IGNORE or PENDING
        }
      }).catch(() => { /* unreachable peer; the timeout retries */ })
        .then(done, done);
    }
  }

  _becomeLeader() {
    this.role = ROLE.LEADER;
    this.leaderId = this.id;
    this._ackAt.clear();       // acks from a previous term say nothing about this one
    this._leaderAt = this._now;
    this._emit('role', { role: this.role });
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
    // What this peer is owed, decided in C (raft_drive.h). The boundary
    // is next > baseIndex: below it, AppendEntries can never catch the
    // peer up however far the leader rewinds, because the entries are
    // compacted away.
    const action = raftDrive.replAction(
      next, this.log.baseIndex, !!(this.snapshotter && this.snapshotter.latest())
    );
    if (action === raftDrive.REPL.SNAPSHOT) return this._sendSnapshot(peer);
    if (action === raftDrive.REPL.PARK) {
      this._needsSnapshot.add(peer);  // the host serves this one
      return;
    }
    this._needsSnapshot.delete(peer);
    const term = this.log.currentTerm;
    const prevLogTerm = this.log.termAt(next - 1);

    // Framed BEFORE the try: the catch below reads any throw as an
    // unreachable peer, which is right for the network and wrong for a
    // log or grammar failure -- that one must surface, not be retried
    // forever as if the wire were down.
    //
    // C reads the batch straight out of the log and frames the message,
    // so the entries are copied once, into the buffer that goes on the
    // wire, and never become JavaScript objects at all.
    const { message } = raftMsg.buildAppendEntries(this.log, {
      term, leaderId: this.id, nextIndex: next, prevLogTerm,
      leaderCommit: this.commitIndex, maxBytes: this.maxBatchBytes, quiesce
    });

    this._inflight.add(peer);
    let again = false;
    try {
      const reply = decode(await this.transport.call(peer, message));
      if (!this.isRunning || this.role !== ROLE.LEADER || this.log.currentTerm !== term) return;
      if (this._reachable.get(peer) === false) {
        this._reachable.set(peer, true);
        this._emit('peer', { id: peer, reachable: true });
      }
      if (reply.term > term) return this._becomeFollower(reply.term, 0);
      // Any reply that did not depose us proves this peer is alive and
      // still accepts our leadership — a rejected-for-log-conflict reply
      // counts exactly as much as a successful one for check-quorum.
      this._ackAt.set(peer, this._now);
      if (reply.success) {
        if (reply.matchIndex > this._match.get(peer)) {
          this._match.set(peer, reply.matchIndex);
          this._next.set(peer, reply.matchIndex + 1);
          this._advanceCommit();
        }
        this._maybePromote(peer);
        this._maybeCompleteTransfer(peer);
        again = this._next.get(peer) <= this.log.lastIndex;
      } else {
        // Where to resume, and whether the peer's match index has to
        // regress with it -- raft_core.h's raft_backoff, which explains
        // why believing a hint below matchIndex is the safe reading.
        const b = raft.backoff(reply.hintIndex, next, this._match.get(peer) ?? 0);
        this._match.set(peer, b.match);
        this._next.set(peer, b.next);
        again = true;
      }
    } catch {
      // Unreachable; the next heartbeat retries. Edge-triggered event:
      // announce the transition, not every failed attempt.
      if (this._reachable.get(peer) !== false) {
        this._reachable.set(peer, false);
        this._emit('peer', { id: peer, reachable: false });
      }
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
        if (!this.isRunning || this.role !== ROLE.LEADER || this.log.currentTerm !== term) return false;
        if (reply.term > term) { this._becomeFollower(reply.term, 0); return false; }
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
        // Match never regresses: a peer already past the snapshot keeps
        // what it had (raft_drive.h).
        const at = raftDrive.installed(boundary, this._match.get(peer) ?? 0);
        this._match.set(peer, at.match);
        this._next.set(peer, at.next);
        this._needsSnapshot.delete(peer);
        this._advanceCommit();
      }
    } catch {
      // Peer unreachable mid-transfer; the next heartbeat starts over.
    } finally {
      this._inflight.delete(peer);
      this._emit('send-snapshot', { phase: installed ? 'finished' : 'failed', peer, boundary });
    }
    if (installed && this.role === ROLE.LEADER && this._next.get(peer) <= this.log.lastIndex) {
      this._replicate(peer);
    }
  }

  /**
   * Promote a caught-up learner: once its match index covers everything
   * committed, propose the same record with the voting flag dropped.
   * Checked on every replication success — a refused moment (another
   * CONFIG in flight) simply retries on the next one. Promotion is the
   * only automatic membership change; it can only ever WIDEN the
   * electorate with a replica proven current.
   */
  _maybePromote(peer) {
    if (this.role !== ROLE.LEADER || this._configInFlight) return;
    const record = this.memberInfo.find((m) => m.id === peer);
    if (!record || record.voting !== false) return;
    if (this.commitIndex === 0 || (this._match.get(peer) ?? 0) < this.commitIndex) return;
    // Explicit voting:true (not just dropping the flag): changeMembership
    // merges address-less records with the known ones, and an absent key
    // would re-inherit the old voting:false through that merge.
    this._emit('promote', { id: peer, match: this._match.get(peer) ?? 0 });
    const next = this.memberInfo.map((m) => (m.id === peer ? { ...m, voting: true } : m));
    this.changeMembership(next).catch(() => { /* retried on the next success */ });
  }

  /** Majority of VOTERS — learners add capacity, not quorum weight. */
  _quorum() { return raft.quorum(this.voters.length); }

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
    if (this.role !== ROLE.LEADER) return true;
    let live = this.voters.includes(this.id) ? 1 : 0;
    for (const p of this.peers) {
      if (!this.voters.includes(p)) continue;
      if (this._now - (this._ackAt.get(p) ?? -Infinity) <= withinMs) live++;
    }
    return live >= this._quorum();
  }

  /** How long this node has been leader, in its own clock (0 if not leader). */
  get leaderForMs() {
    return this.role === ROLE.LEADER ? this._now - this._leaderAt : 0;
  }

  /**
   * §5.4.2. Both halves are C's: which index a quorum holds, and whether
   * that index may actually commit. The second is the figure-8 rule --
   * only a CURRENT-term entry commits by counting replicas -- and
   * omitting it is the classic way to lose committed data.
   */
  _advanceCommit() {
    if (this.role !== ROLE.LEADER) return;
    const matches = this.peers
      .filter((p) => this.voters.includes(p))
      .map((p) => this._match.get(p) ?? 0);
    // The leader is always a voter and always holds its own last index.
    const n = raft.commitCandidate(this.log.lastIndex, matches, this._quorum());
    if (n === null) return;
    const termAt = n > this.log.baseIndex && n <= this.log.lastIndex ? this.log.termAt(n) : 0;
    if (!raft.mayCommit(n, this.commitIndex, this.log.baseIndex, termAt, this.log.currentTerm)) return;
    this.commitIndex = n;
    this.log.setCommitIndex(n);
    this._pumpApply();
  }

  // ---- apply --------------------------------------------------------------

  _pumpApply() {
    this._applyChain = this._applyChain.then(() => this._applyLoop()).catch((err) => {
      // A state machine that throws on a committed entry is unrecoverable
      // divergence; stop rather than skip (skipping would fork replicas).
      this.isRunning = false;
      this._emit('halt', { error: String(err?.message ?? err), lastApplied: this.lastApplied, commitIndex: this.commitIndex });
      this._rejectWaiters(err);
    });
  }

  async _applyLoop() {
    while (this.isRunning && this.lastApplied < this.commitIndex) {
      for (const e of this.log.getBatch(this.lastApplied + 1, this.maxBatchBytes)) {
        if (e.index > this.commitIndex) break;
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

  _adoptConfig(members, index) {
    this._setMembers(members);
    this._configIndex = index;
    if (!this.voters.includes(this.id) && this.role !== ROLE.FOLLOWER) {
      // Applied our own removal (or demotion to learner): step down; the
      // host closes a removed node. (As leader we first committed the
      // entry, so the new set has it.)
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
