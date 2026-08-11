/**
 * db-replicated-instance.js — the WalInstance driven by the Raft core:
 * the second half of docs/s3-backup.md step 2, and the JavaScript twin
 * of what `nisaba-server --raft` is (server/replica.c over
 * db_instance.h). One ReplicatedInstance is one member of one Raft
 * group replicating one INSTANCE: many databases, one log, one leader,
 * one member set — "replication follows the instance, not the
 * database" (db_instance.h, including what it costs).
 *
 * The division of labor is db-replicated.js's, unchanged in kind:
 *
 *   - InstanceDb/WalCollection resolve all nondeterminism at proposal
 *     time; only the commit engine differs. _commitFor proposes each
 *     enveloped command through the RaftNode; the result is what the
 *     local apply of the committed entry produced; nothing is ever
 *     retracted. dropDatabase proposes `{ d, i: "drop" }` the same way.
 *
 *   - InstanceStateMachine.apply runs WalInstance._applyEnvelope — the
 *     same code single-node recovery replays through — swallowing
 *     deterministic command errors, halting on anything else.
 *     appliedIndex() is the instance floor across every database
 *     (dbi_applied_floor's contract).
 *
 *   - The node runs installs itself; this side's file seam resolves
 *     `"db/file"` live names to per-database subproviders — the same
 *     one-level namespace server/instns.c gives the C node — and owns
 *     the swap window: close every database, let C adopt, reconnect,
 *     repoint every cached handle. A scope the generation restored
 *     nothing into stops being a database, as the C transport removes
 *     the directory (server/main.c's adopt_install).
 *
 * A member of this group and a `nisaba-server --raft` member speak the
 * same log, the same envelope, the same generation artifact, and the
 * same peer wire (raft-transport-tcp.js ↔ server/peers.c) — which is
 * what makes a mixed C/Node cluster a deployment choice rather than a
 * feature.
 */
import {
  connectClient, isDeterministicError, decode, encode, EntryLog, dbCatalogFile
} from './nisaba-wasm.js';
import { RaftNode, NotLeaderError } from './raft.js';
import { SNAP_PREFIX, openWalStorage } from './db-wal.js';
import {
  WalInstance, generationCanRescue, restoreLatestInstanceSnapshot
} from './db-wal-instance.js';

/** How many recent per-entry results the state machine retains for
 * local proposers to collect (db-replicated.js says why). */
const RESULT_WINDOW = 4096;

export class InstanceStateMachine {
  constructor(inst) {
    this._inst = inst;
    this._results = new Map(); // index -> { value } | { error }
  }

  /** The instance replay floor — every database in the root, not just
   * the open ones. A startup call, priced accordingly. */
  async appliedIndex() {
    return this._inst._appliedFloor();
  }

  async apply(entry) {
    let outcome;
    try {
      outcome = { value: await this._inst._applyEnvelope(entry.index, decode(entry.payload)) };
    } catch (err) {
      // Deterministic command failures are results every replica
      // computes identically; anything else stops the node rather than
      // letting replicas diverge (db-replicated.js, verbatim).
      if (!isDeterministicError(err)) throw err;
      outcome = { error: err };
    }
    this._results.set(entry.index, outcome);
    if (this._results.size > RESULT_WINDOW) {
      const oldest = this._results.keys().next().value;
      this._results.delete(oldest);
    }
  }

  takeResult(index) {
    const outcome = this._results.get(index);
    this._results.delete(index);
    if (!outcome) throw new Error(`raft applied entry ${index} left no result (result window overrun?)`);
    return outcome;
  }
}

export class ReplicatedInstance extends WalInstance {
  constructor(client, log, { provider, store, dbOptions }) {
    super(client, log, { provider, store });
    this._dbOptions = dbOptions;
    this._raft = null;    // set by connectReplicatedInstance
    this._machine = null;
  }

  get raft() { return this._raft; }

  async transferLeadership(targetId, options) {
    if (!this._raft) throw new Error('transferLeadership: node is not started');
    return this._raft.transferLeadership(targetId, options);
  }

  async status() {
    return {
      ...this._raft.status(),
      instance: {
        databases: await this.listDatabases(),
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

  /** Commit engine override: wrap, propose through Raft, collect the
   * local apply's result. NotLeaderError aborts the whole operation —
   * a routing condition, not a per-document outcome. */
  async _commitFor(idb, cmds, { ordered = true } = {}) {
    const results = new Array(cmds.length);
    let firstError = null;
    for (let i = 0; i < cmds.length; i++) {
      let outcome;
      try {
        const { index } = await this._raft.propose(encode({ d: idb.name, c: decode(cmds[i]) }));
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

  /** The logged drop, ridden through Raft: every member removes the
   * directory when the entry applies. */
  async dropDatabase(name) {
    return this._serialize(async () => {
      const { index } = await this._raft.propose(encode({ d: name, i: 'drop' }));
      const outcome = this._machine.takeResult(index);
      if (outcome.error) throw outcome.error;
      return outcome.value;
    });
  }

  /** Local snapshot + log compaction on a live member, quiesced via
   * runExclusive; boundary = lastApplied (db-replicated.js says why). */
  async snapshot() {
    this._requireStore('snapshot');
    return this._serialize(() => this._raft.runExclusive(async () => {
      const info = await this._snapshotLocked(this._raft.lastApplied);
      this._raft.log = this._log; // _snapshotLocked swapped it
      await this._raft.refreshSnapshotFiles();
      return info;
    }));
  }

  async close() {
    if (!this.isOpen) return;
    if (this._raft) {
      await this._raft.stop();
      this._raft.free();
    }
    await super.close();
  }

  // ---- the file seam (RaftNode `files`, instance-shaped) ------------------

  /** Resolve a name the node asks for: flat names are the root's (the
   * log, generation files); `"db/file"` names are a database's — the
   * same one-level namespace instns.c gives the C server's node. */
  async _openAt(name, create) {
    const slash = name.indexOf('/');
    if (slash < 0) return this._provider.openFile(name, { create });
    const sub = await this._provider.subProvider(name.slice(0, slash));
    return sub.openFile(name.slice(slash + 1), { create });
  }

  async _removeAt(name) {
    const slash = name.indexOf('/');
    if (slash < 0) return this._provider.deleteFile(name);
    const sub = await this._provider.subProvider(name.slice(0, slash));
    return sub.deleteFile(name.slice(slash + 1));
  }

  _makeFiles() {
    return {
      store: this._store,
      open: (name) => this._openAt(name, true),
      remove: (name) => this._removeAt(name),
      swap: (adopt) => this._swapForInstall(adopt)
    };
  }

  /**
   * The installed generation becomes the live instance. The victims are
   * every live file of every database, as `"db/file"`, plus the root's
   * non-store files (the old log's legacy name) — stale journals are
   * the sharp ones, exactly as in db-replicated.js. Between the close
   * and the reopen nothing may observe the databases, so every open
   * InstanceDb gates its reads for the window and is repointed after.
   */
  async _swapForInstall(adopt) {
    const releases = [];
    for (const idb of this._dbs.values()) {
      idb._readGate = new Promise((resolve) => { releases.push(resolve); });
    }
    let keep = null;
    try {
      const mine = `${this._store.prefix}-`;
      const victims = (await this._provider.listFiles()).filter((n) => !n.startsWith(mine));
      for (const db of await this._provider.listSubProviders()) {
        const sub = await this._provider.subProvider(db);
        for (const f of await sub.listFiles()) victims.push(`${db}/${f}`);
      }
      await this._client.close();
      let adopted = false;
      try {
        await adopt(victims);
        adopted = true;
      } finally {
        // Which scopes the generation restored: only they are databases
        // now. Read after the adopt moved `latest` (or didn't, on a
        // failed adopt -- then everything reopens as it was).
        this._store.refresh();
        if (adopted) {
          keep = new Set((this._store.latest?.config.live ?? [])
            .map((f) => f.name.slice(0, f.name.indexOf('/'))));
        }
        this._client = await connectClient(this._provider, this._dbOptions);
        for (const [name, idb] of [...this._dbs]) {
          if (keep && !keep.has(name)) {
            this._dbs.delete(name);
            idb.isOpen = false;
            continue;
          }
          idb._db = await this._client.db(name);
          for (const [cname, col] of idb._collections) {
            col._inner = await idb._db.collection(cname);
          }
        }
      }
      if (keep) {
        // A directory the snapshot restored nothing into is removed
        // (server/main.c's adopt_install does the same).
        for (const db of await this._provider.listSubProviders()) {
          if (!keep.has(db)) await this._client.dropDatabase(db).catch(() => {});
        }
      }
      await this._store.pruneLogs(this._store.logName);
    } finally {
      for (const idb of this._dbs.values()) idb._readGate = null;
      for (const release of releases) release();
    }
  }
}

/**
 * Open one member of a replicated INSTANCE group — the JavaScript
 * equivalent of `nisaba-server --raft <id>`. Same option shape as
 * connectReplicated (db-replicated.js); the provider must be an
 * instance root: listFiles(), subProvider(), listSubProviders(), and
 * not itself a database.
 */
export async function connectReplicatedInstance(provider, options = {}) {
  const {
    id, peers, transport, raft: raftOptions = {},
    snapshotPrefix = SNAP_PREFIX, startNow = 0, restored = false, ...dbOptions
  } = options;
  if (typeof provider.listFiles !== 'function' ||
      typeof provider.subProvider !== 'function' ||
      typeof provider.listSubProviders !== 'function') {
    throw new Error('connectReplicatedInstance requires a storage provider with listFiles(), subProvider() and listSubProviders()');
  }
  if ((await provider.listFiles()).includes(dbCatalogFile())) {
    throw new Error(
      'this directory is a database, not an instance root (it holds ' +
      `${dbCatalogFile()}); open it with connectReplicated, or point at the directory above`
    );
  }
  const client = await connectClient(provider, dbOptions);
  try {
    const { store, log: rawLog, logName } = await openWalStorage(provider, { snapshotPrefix });
    const inst = new ReplicatedInstance(client, rawLog, { provider, store, dbOptions });
    // The restored-applied-state shape (connectWalInstance says why),
    // re-based before the RaftNode ever sees the log.
    const floor = await inst._appliedFloor();
    // And the other direction: a floor below the log's BASE means replay
    // cannot reach the live state at all, and the committed generation at
    // that boundary is the state that can (generationCanRescue says what
    // it costs not to, and why one attempt). Restored before the RaftNode
    // exists, so the node never sees the unusable state.
    if (!restored && generationCanRescue(inst, floor)) {
      await inst.close();
      await restoreLatestInstanceSnapshot(provider, { snapshotPrefix });
      return connectReplicatedInstance(provider, { ...options, restored: true });
    }
    if (floor > inst._log.lastIndex) {
      const { currentTerm, votedFor, lastTerm } = inst._log;
      await inst._log.close();
      const handle = await provider.openFile(logName, { create: true });
      handle.truncate(0);
      const fresh = new EntryLog(handle, { baseIndex: floor, baseTerm: lastTerm });
      await fresh.open();
      if (currentTerm > 0) fresh.setHardState(currentTerm, votedFor);
      inst._log = fresh;
    }
    const machine = new InstanceStateMachine(inst);
    const node = new RaftNode({
      id, peers, log: inst._log, stateMachine: machine, transport,
      files: inst._makeFiles(),
      ...raftOptions
    });
    inst._raft = node;
    inst._machine = machine;
    // No blanket recovery replay: the RaftNode applies exactly the
    // committed prefix; an uncommitted local suffix waits for a leader.
    await node.start(startNow);
    return inst;
  } catch (err) {
    await client.close();
    throw err;
  }
}

export { NotLeaderError };
