/**
 * db-replicated.js — the WalDb driven by the Raft core: replication
 * roadmap step 5c. One ReplicatedDb is one member of one Raft group
 * replicating one database (the step-4 decision: one group per tenant
 * database). The division of labor:
 *
 *   - WalCollection (src/db-wal.js) is unchanged: it resolves all
 *     nondeterminism at proposal time and hands deterministic commands
 *     to the commit engine. Only the engine differs here: _commit
 *     proposes each command through the RaftNode — the ack point moves
 *     from "fsynced locally" to "committed by a quorum and applied
 *     locally" — and NOTHING is ever retracted: a committed command that
 *     fails deterministically (e.g. duplicate key) is a *result* every
 *     replica computes identically, not a log defect.
 *
 *   - DbStateMachine is the RaftNode's state machine: apply() runs
 *     WalDb._applyCommand (the same code the single-node live path and
 *     crash replay use), records each entry's result for the local
 *     proposer to pick up, and swallows deterministic command errors so
 *     the apply pump never mistakes an application-level failure for
 *     replica divergence. appliedIndex() is the database's replay floor:
 *     the max of every collection's persisted appliedIndex (apply is
 *     strictly ordered, so the max IS the applied prefix).
 *
 *   - The SnapshotStore (roadmap step 3) is handed to the RaftNode as
 *     its `files` seam, and the NODE runs the install: it streams the
 *     adopted generation's chunks, stages an incoming one, CRC-checks
 *     every staged file against the leader's manifest before writing the
 *     manifest that commits it, and rebases its own log onto the
 *     boundary (raft_node.h). What is left here is the two things C
 *     cannot do — opening a file, which is asynchronous in a browser,
 *     and the window the flip runs in: close the inner Db, let C replace
 *     the files, reconnect, and repoint every cached WalCollection.
 *
 *   - snapshot() must not run under a live node's feet (an AppendEntries
 *     mid-swap would append to a dead log object), so it runs inside
 *     RaftNode.runExclusive with the boundary at lastApplied — an
 *     uncommitted suffix is never baked into a snapshot, and the
 *     compacted log keeps it.
 *
 * Writes are leader-only: on a follower they reject with NotLeaderError
 * (leaderId is the retry hint); forwarding is the transport/service
 * layer's job (5d). Reads serve the local replica — the leader's are
 * read-your-writes (propose resolves after local apply); a follower's
 * are stale by exactly its replication lag, the step-4 "browser as
 * cache" semantics. There is no automatic recovery replay on open:
 * the RaftNode owns replay, applying exactly the committed prefix — an
 * uncommitted local suffix stays unapplied until a leader settles it.
 */
import { connect, isDeterministicError } from '../wasm/nisaba-wasm.js';
import { RaftNode, NotLeaderError } from './raft.js';
import {
  WalDb, SNAP_PREFIX, openWalStorage, reconcileLogWithAppliedFloor
} from './db-wal.js';

/** How many recent per-entry results the state machine retains for local
 * proposers to collect (only the leader consumes them; followers just
 * age them out). */
const RESULT_WINDOW = 4096;

export class DbStateMachine {
  constructor(rdb) {
    this._rdb = rdb;
    this._results = new Map(); // index -> { value } | { error }
  }

  async appliedIndex() {
    let max = 0;
    for (const name of await this._rdb._db.listCollections()) {
      const col = await this._rdb._db.collection(name);
      max = Math.max(max, await col.appliedIndex());
    }
    return max;
  }

  async apply(entry) {
    let outcome;
    try {
      outcome = { value: await this._rdb._applyCommand(entry.index, entry.payload) };
    } catch (err) {
      // A deterministic command failure (duplicate key, missing indexed
      // field, ...) is a normal result — every replica computes the same
      // one. Anything else (I/O, a bridged storage exception underneath)
      // is real trouble: rethrow so the apply pump stops the node rather
      // than let replicas diverge.
      //
      // This used to read `err.name === 'Error' || err.cause`, which
      // rested consensus safety on a JavaScript runtime detail: any code
      // path throwing a plain Error for a deterministic reason halted
      // the cluster, and a typed error raised by an I/O path was
      // swallowed as a result. It is a numeric classification now, and
      // it lives with the codes it classifies (db_validate.h).
      if (!isDeterministicError(err)) throw err;
      outcome = { error: err };
    }
    this._results.set(entry.index, outcome);
    if (this._results.size > RESULT_WINDOW) {
      const oldest = this._results.keys().next().value;
      this._results.delete(oldest);
    }
  }

  /** Collect (and forget) the apply outcome of a locally-proposed entry.
   * Valid once propose() has resolved — the entry is applied by then. */
  takeResult(index) {
    const outcome = this._results.get(index);
    this._results.delete(index);
    if (!outcome) throw new Error(`raft applied entry ${index} left no result (result window overrun?)`);
    return outcome;
  }
}

export class ReplicatedDb extends WalDb {
  constructor(db, log, { provider, store, dbOptions }) {
    super(db, log, { provider, store });
    this._dbOptions = dbOptions;
    this._raft = null;    // set by connectReplicated
    this._machine = null;
  }

  /** The RaftNode — role/term/leaderId for routing, tick() for the host's
   * clock, handleMessage() for the transport's receiving half. */
  get raft() { return this._raft; }

  /**
   * Graceful leadership transfer to another VOTING member — the
   * zero-data-copy rebalance (RaftNode.transferLeadership, §3.10):
   * fence new writes (they reject NotLeaderError hinting the target),
   * catch the target up, TimeoutNow, step down when it wins. Resolves
   * once leadership has left this node; rejects — and normal service
   * resumes — if the target doesn't take over in time. Leader-only:
   * callers route it like any write.
   */
  async transferLeadership(targetId, options) {
    if (!this._raft) throw new Error('transferLeadership: node is not started');
    return this._raft.transferLeadership(targetId, options);
  }

  /** One JSON-able snapshot: the RaftNode's status plus the database's
   * own facts (collections, snapshot generation, applied position).
   * Async because the collection list is. */
  async status() {
    return {
      ...this._raft.status(),
      db: {
        collections: await this._db.listCollections(),
        appliedIndex: await this._machine.appliedIndex(),
        snapshot: this._store.latest
          ? {
              gen: this._store.latest.gen,
              lastIncludedIndex: this._store.latest.lastIncludedIndex,
              lastIncludedTerm: this._store.latest.lastIncludedTerm
            }
          : null
      }
    };
  }

  /** Commit engine override: propose each command through Raft; the
   * result is whatever the local apply of the committed entry produced.
   * A NotLeaderError aborts the whole operation immediately — it is a
   * routing condition, not a per-document outcome. Nothing is retracted:
   * committed entries are immutable, and a failed command is a
   * deterministic result, not log garbage. */
  async _commit(cmds, { ordered = true } = {}) {
    const results = new Array(cmds.length);
    let firstError = null;
    for (let i = 0; i < cmds.length; i++) {
      let outcome;
      try {
        const { index } = await this._raft.propose(cmds[i]);
        outcome = this._machine.takeResult(index);
      } catch (err) {
        if (err instanceof NotLeaderError) throw err;
        outcome = { error: err };
      }
      if (outcome.error) {
        if (firstError === null) firstError = { index: i, error: outcome.error };
        if (ordered) break;
      } else {
        results[i] = outcome.value;
      }
    }
    return { results, firstError };
  }

  /**
   * Local snapshot + log compaction on a live member (leader or
   * follower), quiesced via runExclusive so no RPC handler or apply can
   * touch the log mid-swap. Boundary = lastApplied: the state machine
   * reflects exactly that prefix, and any uncommitted suffix survives in
   * the compacted log.
   */
  async snapshot() {
    this._requireStore('snapshot');
    return this._serialize(() => this._raft.runExclusive(async () => {
      const info = await this._snapshotLocked(this._raft.lastApplied);
      this._raft.log = this._log; // _snapshotLocked swapped it
      // A new generation is a new set of filenames, and a leader streams
      // chunks from them inside a synchronous tick — so they have to be
      // resolvable before the next one runs (see refreshSnapshotFiles).
      await this._raft.refreshSnapshotFiles();
      return info;
    }));
  }

  async close() {
    if (!this.isOpen) return;
    if (this._raft) {
      await this._raft.stop();
      // And give the C node back. The seat model is one group per tenant
      // database on a long-lived process, so a raft_node left behind per
      // close is a leak that grows with tenant churn rather than a
      // rounding error. Ordered: stop() first, because free() refuses a
      // running node.
      this._raft.free();
    }
    await super.close();
  }

  // ---- the file seam (RaftNode `files`, over the SnapshotStore) -----------

  /**
   * What the node needs to run an install itself: a store to name
   * generations in, a way to open and unlink a file, and the one thing
   * that stays on this side forever -- the window the flip happens in.
   *
   * Opening is this side's because it is asynchronous in a browser and
   * can never be inside a synchronous C call (bjns.h). Closing and
   * reopening the database is this side's for a different reason: the
   * reopen rebuilds every collection handle, which C has no idea exists.
   */
  _makeFiles() {
    return {
      store: this._store,
      open: (name) => this._provider.openFile(name, { create: true }),
      remove: (name) => this._provider.deleteFile(name),
      swap: (adopt) => this._swapForInstall(adopt)
    };
  }

  /**
   * The installed generation becomes the live database. Between the
   * close and the reopen the database is neither the old one nor the new
   * one and nothing may observe it there -- so what runs in the middle is
   * ONE synchronous call (raft_node.h's rn_adopt), and the read gate
   * holds async reads off the closed collections' freed WASM contexts
   * for the whole window.
   *
   * The victims are every file that is not the snapshot store's: the
   * live collection and index files, their journals, and the old WAL. A
   * journal is the sharp one -- left behind, recovery replays it over a
   * restored file and rewinds it. Which files are the database's is
   * exactly the knowledge a Raft node does not have, which is why it
   * asks rather than derives; ones the generation is about to restore
   * are skipped rather than removed, because bjns.h forbids ordering a
   * create against a remove.
   */
  async _swapForInstall(adopt) {
    let release;
    this._readGate = new Promise((resolve) => { release = resolve; });
    try {
      const mine = `${this._store.prefix}-`;
      const victims = (await this._provider.listFiles()).filter((n) => !n.startsWith(mine));
      await this._db.close();
      try {
        await adopt(victims);
      } finally {
        this._db = await connect(this._provider, this._dbOptions);
        for (const [name, col] of this._collections) {
          col._inner = await this._db.collection(name);
        }
      }
      // Superseded generations' logs, which the store's own scan leaves
      // alone by design (it cannot tell a torn newest one from a stale
      // one without opening it). The name to keep is the store's: the
      // node created that file, so this side never saw it.
      await this._store.pruneLogs(this._store.logName);
    } finally {
      this._readGate = null;
      release();
    }
  }
}

/**
 * Open one member of a replicated database group. The transport carries
 * this node's Raft RPCs (call(peerId, msg) → reply); incoming messages
 * must be routed to `rdb.raft.handleMessage`. The host drives time via
 * `rdb.raft.tick(now)`. Requires a provider with listFiles() — snapshot
 * install is not optional in a cluster.
 *
 * @param {object} provider - StorageProvider (openFile/deleteFile/listFiles)
 * @param {object} options
 * @param {number} options.id - this node's id (positive integer)
 * @param {number[]} options.peers - every node id in the group
 * @param {object} options.transport - { call(peerId, msg) -> Promise<reply> }
 * @param {object} [options.raft] - RaftNode tuning (electionTimeoutMs,
 *   heartbeatMs, snapshotChunkBytes, random, ...)
 * @param {number} [options.startNow=0] - initial clock value for tick()
 */
export async function connectReplicated(provider, options = {}) {
  const {
    id, peers, transport, raft: raftOptions = {},
    snapshotPrefix = SNAP_PREFIX, startNow = 0, ...dbOptions
  } = options;
  if (typeof provider.listFiles !== 'function') {
    throw new Error('connectReplicated requires a storage provider with listFiles()');
  }
  const db = await connect(provider, dbOptions);
  try {
    const { store, log: rawLog, logName } = await openWalStorage(provider, { snapshotPrefix });
    // Restored-applied-state shape (log behind the replay floor): re-base
    // before the RaftNode ever sees the log — see the helper's contract.
    const log = await reconcileLogWithAppliedFloor(db, rawLog, logName, provider);
    const rdb = new ReplicatedDb(db, log, { provider, store, dbOptions });
    const machine = new DbStateMachine(rdb);
    const node = new RaftNode({
      id, peers, log, stateMachine: machine, transport,
      files: rdb._makeFiles(),
      ...raftOptions
    });
    rdb._raft = node;
    rdb._machine = machine;
    // No blanket recovery replay here: the RaftNode applies exactly the
    // committed prefix; an uncommitted local suffix waits for a leader.
    await node.start(startNow);
    return rdb;
  } catch (err) {
    await db.close();
    throw err;
  }
}

export { NotLeaderError };
