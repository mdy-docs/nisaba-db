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
 *   - The SnapshotStore (roadmap step 3) is adapted onto the RaftNode's
 *     5b snapshotter contract: latest()/openFile() serve install chunks
 *     straight from the adopted generation; beginInstall() stages a new
 *     generation via store.begin(), CRC-checks every staged file against
 *     the leader's manifest BEFORE committing, then adopts it into the
 *     live database — close the inner Db, restore the generation onto
 *     the live filenames, reconnect, and repoint every cached
 *     WalCollection. rebaseLog() pairs a fresh boundary-based log with
 *     the installed generation.
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
import { connect, EntryLog, encode, decode, crc32 } from '../wasm/nisaba-wasm.js';
import { RaftNode, NotLeaderError } from './raft.js';
import {
  WalDb, WAL_FILE, SNAP_PREFIX,
  openWalStorage, restoreFromStore
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
    const cmd = this._rdb._decodeCommand(entry.payload);
    let outcome;
    try {
      outcome = { value: await this._rdb._applyCommand(entry.index, cmd) };
    } catch (err) {
      // A deterministic command failure (duplicate key, missing indexed
      // field, ...) is a normal result — every replica computes the same
      // one. Anything else (I/O, a bridged storage exception underneath)
      // is real trouble: rethrow so the apply pump stops the node rather
      // than let replicas diverge.
      if (err.name === 'Error' || err.cause) throw err;
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
        const { index } = await this._raft.propose(encode(cmds[i]));
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
      return info;
    }));
  }

  async close() {
    if (!this.isOpen) return;
    if (this._raft) await this._raft.stop();
    await super.close();
  }

  // ---- snapshotter adapter (RaftNode 5b contract over SnapshotStore) ------

  _makeSnapshotter() {
    const rdb = this;
    const store = this._store;
    return {
      latest() { return store.latest; },
      openFile(role) { return store.openFile(role); },
      async beginInstall(manifest) {
        const tx = await store.begin();
        const open = new Map();   // role -> { handle, written, crc }
        return {
          async writeChunk(role, offset, data) {
            let f = open.get(role);
            if (!f) {
              f = { handle: await tx.createFile(role), written: 0, crc: 0 };
              open.set(role, f);
            }
            if (offset !== f.written) {
              throw new Error(`snapshot chunk out of order for ${role}: at ${offset}, expected ${f.written}`);
            }
            f.handle.write(data, { at: offset });
            f.written += data.length;
            f.crc = crc32(data, f.crc);
          },
          async commit() {
            // Validate the staged bytes against the LEADER's manifest
            // before anything becomes adoptable.
            for (const want of manifest.files) {
              const f = open.get(want.role) || { written: 0, crc: 0 };
              if (f.written !== want.size || f.crc !== want.crc) {
                throw new Error(`snapshot file ${want.role} failed validation`);
              }
            }
            for (const f of open.values()) {
              f.handle.flush();
              await f.handle.close();
            }
            await tx.commit({
              lastIncludedIndex: manifest.lastIncludedIndex,
              lastIncludedTerm: manifest.lastIncludedTerm,
              config: manifest.config
            });
            await rdb._adoptInstalledSnapshot();
          },
          async abort() {
            for (const f of open.values()) {
              try { await f.handle.close(); } catch { /* best-effort */ }
            }
            await tx.abort();
          }
        };
      }
    };
  }

  /** The installed generation becomes the live database: close the inner
   * Db, restore the generation onto the live filenames, reconnect, and
   * repoint every cached WalCollection at the fresh inner collections.
   * The read gate holds async reads off the closed collections' freed
   * WASM contexts for the duration. */
  async _adoptInstalledSnapshot() {
    let release;
    this._readGate = new Promise((resolve) => { release = resolve; });
    try {
      await this._db.close();
      await restoreFromStore(this._provider, this._store);
      this._db = await connect(this._provider, this._dbOptions);
      for (const [name, col] of this._collections) {
        col._inner = await this._db.collection(name);
      }
    } finally {
      this._readGate = null;
      release();
    }
  }

  /** rebaseLog hook (5b): pair a fresh boundary-based log with the just-
   * installed generation; the node re-persists hard state onto it. */
  _makeRebaseLog() {
    return async (lastIncludedIndex, lastIncludedTerm) => {
      const { name, handle } = await this._store.createLogFile();
      handle.truncate(0); // may be a torn leftover at this generation
      const log = new EntryLog(handle, {
        baseIndex: lastIncludedIndex, baseTerm: lastIncludedTerm
      });
      await log.open();
      this._log = log;
      await this._store.pruneLogs(name);
      try { await this._provider.deleteFile(WAL_FILE); } catch { /* best-effort */ }
      return log;
    };
  }

  _decodeCommand(payload) {
    // A method so the service layer can interpose (e.g. metrics).
    return decode(payload);
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
    const { store, log } = await openWalStorage(provider, { snapshotPrefix });
    const rdb = new ReplicatedDb(db, log, { provider, store, dbOptions });
    const machine = new DbStateMachine(rdb);
    const node = new RaftNode({
      id, peers, log, stateMachine: machine, transport,
      snapshotter: rdb._makeSnapshotter(),
      rebaseLog: rdb._makeRebaseLog(),
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
