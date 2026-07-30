/**
 * db-wal.js — single-node write-ahead logging for the document database:
 * replication roadmap step 2 (docs/replicaton-roadmap.md), Raft minus
 * networking. Every write becomes a binjson-encoded command appended to a
 * shared EntryLog (`__wal__.bj`, outside the orphan sweep's
 * isDbFile) and made durable (sync()) BEFORE it is applied to the
 * collections; recovery replays the committed log suffix each collection
 * hasn't seen (its appliedIndex(), roadmap step 1). The propose/apply/
 * recover machinery here is exactly what the replicated apply path will
 * run once a Raft core sits in front of it — steps 4/5 change who calls
 * it, not what it does.
 *
 * Determinism: a command must replay to the identical state on any
 * replica, so ALL nondeterminism resolves at proposal time, before the
 * command is logged. The resolving itself is C's — wasm/include/db_wal.h
 * owns the command grammar and the planner that produces it — and what
 * it guarantees is stronger than a list of rules:
 *
 *   EVERY LOGGED DOCUMENT COMMAND NAMES EXACTLY ONE _id.
 *
 * So apply never runs a query, and never depends on the state replay
 * happens to be partway through reproducing. Concretely:
 *   - documents are logged with their _id already assigned;
 *   - $currentDate resolves to concrete Dates (resolveCurrentDate — the
 *     same helper the inner collection uses at apply time, which is a
 *     no-op on an already-resolved update);
 *   - matched updates/replaces/deletes are resolved to their target _id
 *     (a no-match non-upsert write is not logged at all);
 *   - an upsert that found no match is resolved the whole way: the
 *     planner builds the document the upsert would have inserted, and
 *     logs a plain insert. The `uu`/`ru` commands that used to carry a
 *     filter into the log, for apply to re-evaluate, are gone;
 *   - pruneExpired's TTL cutoffs become concrete Dates inside ordinary
 *     logged deletes.
 *
 * Multi-document writes (insertMany/updateMany/deleteMany/bulkWrite)
 * decompose into per-document commands — one log entry each, one sync()
 * for the whole batch (group commit) — because the engine commits those
 * ops per document (dc_update_many in wasm/src/db.c): a single entry
 * spanning several commits could persist its appliedIndex with the first
 * commit and lose the rest to a crash. Per-document entries make every
 * entry exactly one collection commit, which is what makes replay exact.
 *
 * Writes serialize through one db-wide chain (`_serialize`) so log order
 * IS apply order; reads bypass it and see whatever is applied (a write
 * awaited by the caller is always fully applied — read-your-writes
 * holds). If applying a freshly-logged command fails (e.g. duplicate
 * key), the entry — and, for an ordered batch, its never-applied suffix
 * — is truncated back out of the log (truncateFrom), keeping the log a
 * history of successful commands only; recovery replay tolerates any
 * residue from unordered batches by re-running it into the same
 * deterministic error.
 *
 * Not logged: reads, index DDL (createIndex/dropIndex — they commit
 * their own files, and a replayed entry that predates an index build was
 * by definition not yet applied when the build's backfill scanned the
 * primary, so maintenance-vs-backfill can't double-count), and compact
 * (appliedIndex is carried through, see step 1's tests).
 *
 * Snapshots + log compaction (roadmap step 3): snapshot() streams the
 * whole database — the catalog and every collection's structures — into
 * an immutable SnapshotStore generation (files `<prefix>-<gen>-<role>.bj`
 * plus a CRC-protected manifest committed last; crash-safe adoption and
 * sweeping are the store's contract), stamps it with the log boundary
 * (lastIncludedIndex/Term — every logged entry is applied at a quiesced
 * moment, so the boundary is simply the log tail), then compacts the
 * EntryLog through that boundary into the store's paired log file and
 * adopts it, pruning predecessors. This bounds log growth, is the
 * InstallSnapshot artifact for roadmap step 5, and provides the barrier
 * that makes dropCollection legal: the drop takes a snapshot, so no
 * pre-drop entry survives to resurrect the collection on replay.
 * restoreLatestSnapshot() is the local half of an install: copy every
 * generation file back to its live name (the manifest's config carries
 * the mapping) and reopen — recovery then replays whatever log suffix
 * lies beyond the boundary. Both need a provider with listFiles();
 * without one, snapshot() and dropCollection() are refused.
 */
import {
  connect,
  EntryLog,
  SnapshotStore,
  ObjectId,
  decode,
  resolveCurrentDate,
  runBulkWrite,
  ttlFilters,
  walPlan,
  walParse,
  WAL_OP,
  WAL_REQ,
  WAL_PLAN,
  dbCatalogFile,
  isDbFile
} from '../wasm/nisaba-wasm.js';

const WAL_FILE = '__wal__.bj';
const SNAP_PREFIX = '__snap';

/** Adapt a StorageProvider (openFile/deleteFile/listFiles) to the
 * directory-handle shape SnapshotStore consumes (getFileHandle/
 * removeEntry/entries). */
function providerDirectory(provider) {
  return {
    async getFileHandle(name, options = {}) {
      return { createSyncAccessHandle: () => provider.openFile(name, options) };
    },
    async removeEntry(name) { return provider.deleteFile(name); },
    async *entries() {
      for (const name of await provider.listFiles()) yield [name];
    }
  };
}

/**
 * One entry ~= one collection commit. The command grammar itself lives in
 * wasm/include/db_wal.h — which opcodes exist, what each requires, and
 * what a request resolves to. This file plans through walPlan and
 * dispatches on WAL_OP, so no opcode spelling appears in JavaScript.
 *
 * Two things about that grammar are worth restating here, because they
 * changed what this file does:
 *
 * Every logged document command names exactly one _id. The old grammar
 * had `uu`/`ru` commands that carried a FILTER, which apply re-evaluated
 * against the state replay was in the middle of reproducing. The planner
 * now resolves an upsert the whole way — no match means it builds the
 * document the upsert would have inserted and logs a plain insert — so
 * apply never runs a query.
 *
 * The planner returns what the proposer used to query for. Resolving the
 * target already read the matched document, so its _id, and the
 * pre-image findOneAndUpdate/Replace/Delete return, come back for free.
 * Every findOne and projected find that used to precede a logged write is
 * gone.
 */
class WalDb {
  constructor(db, log, { provider, store }) {
    this._db = db;
    this._log = log;
    this._provider = provider;
    this._store = store; // SnapshotStore, or null (provider lacks listFiles)
    this._collections = new Map(); // name -> WalCollection
    this._chain = Promise.resolve();
    this._broken = null; // Error that poisoned the log (failed sync), or null
    /** Non-null while the inner Db is being swapped out under a snapshot
     * install (ReplicatedDb): async reads await it so they can't touch a
     * closed collection's freed WASM context mid-swap. */
    this._readGate = null;
    this.isOpen = true;
  }

  /** The underlying EntryLog — read-only from the host's point of view
   * (lastIndex, getBatch for inspection); the Raft layer (roadmap steps
   * 4/5) will drive it directly. NOTE: snapshot() swaps this for the
   * compacted log — re-read this getter rather than caching the object. */
  get log() { return this._log; }

  /** The SnapshotStore (latest/verify/openFile for serving InstallSnapshot
   * chunks in roadmap step 5), or null if the provider lacks listFiles(). */
  get snapshots() { return this._store; }

  async collection(name) {
    let col = this._collections.get(name);
    if (!col) {
      col = new WalCollection(this, name, await this._db.collection(name));
      this._collections.set(name, col);
    }
    return col;
  }

  async listCollections() { return this._db.listCollections(); }

  /** Drop a collection — a logged command like any other write, so crash
   * replay (and, replicated, every follower) performs the drop too:
   * pre-drop entries may transiently resurrect the collection during a
   * replay, but the drop entry then re-drops it, and the next snapshot's
   * log compaction removes the churn entirely. */
  async dropCollection(name) {
    return this._serialize(async () => {
      const { commands } = walPlan(null, name, WAL_REQ.DROP_COLLECTION);
      const { results, firstError } = await this._commit(commands);
      if (firstError) throw firstError.error;
      return results[0];
    });
  }

  /**
   * Stream the whole database into a new immutable snapshot generation,
   * stamped with the current log boundary, then compact the log through
   * that boundary and adopt the compacted file (pruning predecessors).
   * Returns the store's adopted-snapshot descriptor ({ gen,
   * lastIncludedIndex, lastIncludedTerm, config, files }). Host-driven,
   * like compact()/pruneExpired(): call it on your own trigger (e.g. when
   * `db.log.fileLen` outgrows the data). Serialized against writes; reads
   * may run concurrently (structure compaction only reads live trees).
   */
  async snapshot() {
    this._requireStore('snapshot');
    return this._serialize(() => this._snapshotLocked());
  }

  _requireStore(what) {
    if (!this._store) {
      throw new Error(`${what} requires a storage provider with listFiles() (the SnapshotStore scans and sweeps by directory listing)`);
    }
  }

  /** `boundaryIndex` defaults to the log tail (single-node: quiesced
   * means everything is applied). ReplicatedDb passes its lastApplied —
   * an uncommitted suffix must never be baked into a snapshot. */
  async _snapshotLocked(boundaryIndex = null) {
    const log = this._log;
    const lastIncludedIndex = boundaryIndex ?? log.lastIndex;
    const lastIncludedTerm = log.termAt(lastIncludedIndex);

    // Build the generation: every structure streams its live entries into
    // a fresh role file (compact() reads from the live tree and closes the
    // destination). `live` maps each role back to the file name the
    // database actually opens -- what a restore/install copies onto.
    const tx = await this._store.begin();
    const live = [];
    let n = 0;
    const add = async (liveName, structure) => {
      const role = `f${n++}`;
      await structure.compact(await tx.createFile(role));
      live.push({ role, name: liveName });
    };
    try {
      await add(dbCatalogFile(), this._db._catalog);
      for (const name of await this._db.listCollections()) {
        const col = await this._db.collection(name);
        await add(this._db._catalog.search(name).file, col._tree);
        for (const ix of col._indexes.values()) {
          if (ix.kind === 'equality') await add(ix.file, ix.tree);
          else if (ix.kind === 'text') {
            await add(ix.files.index, ix.trees.index);
            await add(ix.files.docTerms, ix.trees.docTerms);
            await add(ix.files.docLengths, ix.trees.docLengths);
          } else await add(ix.file, ix.rt);
        }
      }
      await tx.commit({ lastIncludedIndex, lastIncludedTerm, config: { live } });
    } catch (err) {
      await tx.abort();
      throw err;
    }

    // Pair the compacted log with the generation and adopt it: the old
    // log is only pruned once its successor is durable, so a crash
    // anywhere in this window leaves an openable predecessor behind.
    const { name: logName, handle } = await this._store.createLogFile();
    await log.compact(handle, lastIncludedIndex, lastIncludedTerm);
    await log.close();
    const fresh = new EntryLog(await this._provider.openFile(logName, { create: false }));
    await fresh.open();
    this._log = fresh;
    await this._store.pruneLogs(logName);
    if (logName !== WAL_FILE) {
      try { await this._provider.deleteFile(WAL_FILE); } catch { /* best-effort */ }
    }
    return this._store.latest;
  }

  /** Serialized against writes and snapshots -- a compact() swapping a
   * collection's files mid-snapshot would tear the generation. */
  async compact(options) { return this._serialize(() => this._db.compact(options)); }

  async close() {
    if (!this.isOpen) return;
    this.isOpen = false;
    await this._chain.catch(() => {});
    await this._db.close();
    await this._log.close();
  }

  /** All writes flow through here: log order is apply order. Errors
   * propagate to the caller but never break the chain. */
  _serialize(fn) {
    const run = this._chain.then(fn, fn);
    this._chain = run.then(() => {}, () => {});
    return run;
  }

  /** Append every command (one entry each), then one durable sync().
   * Returns the first entry's index; the rest are contiguous.
   *
   * `cmds` are already-encoded payloads straight from the planner, not
   * objects: the live path and the replay path now take the identical
   * input, which is the point — a command this side could build but not
   * parse, or parse but not build, would be a divergence with nothing to
   * catch it. */
  _propose(cmds) {
    if (this._broken) {
      throw new Error(`WAL is poisoned by an earlier sync failure: ${this._broken.message}`, { cause: this._broken });
    }
    const term = this._log.currentTerm;
    let first = 0;
    for (const payload of cmds) {
      const index = this._log.append(term, payload);
      if (!first) first = index;
    }
    try {
      this._log.sync();
    } catch (err) {
      // The un-synced appends stay in EntryLog's buffer and would ride
      // along with a later sync as stale commands; there is no discard
      // API, so refuse all further writes on this handle.
      this._broken = err;
      throw err;
    }
    return first;
  }

  /** Retract log entries from `index` up — only ever the tail this very
   * write just appended (the serialize chain guarantees nothing follows
   * it). Used when applying a freshly-logged command fails: the log stays
   * a history of successful commands. Best-effort: if the truncate itself
   * fails, recovery replays the entry into the same deterministic error. */
  _retractFrom(index) {
    try { this._log.truncateFrom(index); } catch { /* see doc comment */ }
  }

  /**
   * The commit engine: make every command durable in the log, then apply
   * each in order, returning per-command results. This local (single-node)
   * implementation appends all commands under one sync() (group commit)
   * and truncates a failed command — with its never-applied suffix, when
   * `ordered` — back out of the log. ReplicatedDb overrides this with the
   * Raft propose path (roadmap 5c): same commands, same results, but
   * durability means a quorum and nothing is ever retracted.
   */
  async _commit(cmds, { ordered = true } = {}) {
    const first = this._propose(cmds);
    const results = new Array(cmds.length);
    let firstError = null;
    for (let i = 0; i < cmds.length; i++) {
      const index = first + i;
      try {
        results[i] = await this._applyCommand(index, cmds[i]);
      } catch (err) {
        if (firstError === null) firstError = { index: i, error: err };
        if (ordered) {
          this._retractFrom(index);
          break;
        }
      }
    }
    return { results, firstError };
  }

  /** Apply one logged command to the state machine — the ONLY code that
   * mutates collections, shared verbatim by the live write path and
   * recovery replay (and, in ReplicatedDb, by every replica's apply
   * loop). Document mutations stage the entry's index first (step 1's
   * contract), so the mutation's own commit persists it. DDL commands
   * don't stage (createIndex commits catalog + index files but not the
   * primary tree, so a staged value wouldn't persist anyway); their
   * replay is idempotent instead — a re-run createIndex resolves to the
   * existing index, a re-run drop of a missing target reports "nothing
   * to do" — bounded by the next snapshot's log compaction.
   *
   * Takes the raw payload, not a decoded command: walParse validates it
   * against the grammar first, so an entry naming an op this build cannot
   * execute is REFUSED rather than skipped. That distinction is the
   * difference between a node that stops and a node that has quietly
   * diverged from its peers.
   *
   * Every document command names one _id, so nothing here runs a query
   * (db_wal.h) — {_id: ...} is a point lookup on the primary tree. */
  async _applyCommand(index, payload) {
    const op = walParse(payload);
    const cmd = decode(payload);
    if (op === WAL_OP.DROP_COLLECTION) {
      const dropped = await this._db.dropCollection(cmd.c);
      this._collections.delete(cmd.c);
      return dropped;
    }
    const col = await this._db.collection(cmd.c);
    if (op === WAL_OP.CREATE_INDEX) return col.createIndex(cmd.keys, cmd.options);
    if (op === WAL_OP.DROP_INDEX) return col.dropIndex(cmd.name);

    await col.setAppliedIndex(index);
    switch (op) {
      case WAL_OP.INSERT: return col.insertOne(cmd.doc);
      case WAL_OP.UPDATE: return col.updateOne({ _id: cmd.id }, cmd.update);
      case WAL_OP.REPLACE: return col.replaceOne({ _id: cmd.id }, cmd.doc);
      // No default: walParse already rejected anything else, which is
      // why this switch has no "unknown op" arm to keep in step with C's.
      default: return col.deleteOne({ _id: cmd.id });
    }
  }

  /** Recovery: replay every committed entry a collection hasn't applied.
   * Entries replay in strict log order; the per-collection guard makes a
   * second replay of anything a no-op, and a deterministic apply error
   * (the residue of a crashed unordered batch) is swallowed — it failed
   * identically when it was first proposed. */
  async _recover() {
    const log = this._log;
    let from = log.baseIndex + 1;
    while (from <= log.lastIndex) {
      const batch = log.getBatch(from, 1 << 20);
      if (batch.length === 0) break;
      for (const entry of batch) {
        const cmd = decode(entry.payload);
        const col = await this._db.collection(cmd.c);
        if (entry.index <= await col.appliedIndex()) continue;
        try {
          await this._applyCommand(entry.index, entry.payload);
        } catch { /* deterministic re-failure; see doc comment */ }
      }
      from = batch[batch.length - 1].index + 1;
    }
  }
}

class WalCollection {
  constructor(walDb, name, inner) {
    this._wal = walDb;
    this.name = name;
    this._inner = inner;
  }

  // ---- reads and un-logged operations: straight through -------------------
  // Async reads await the install read-gate (see WalDb._readGate); the
  // synchronously-shaped ones (find/aggregate cursors, watch) cannot —
  // a cursor obtained across an install adoption window is a documented
  // 5d hazard, not a supported operation.

  async _read(fn) {
    if (this._wal._readGate) await this._wal._readGate;
    return fn();
  }

  find(filter, options) { return this._inner.find(filter, options); }
  findOne(filter, options) { return this._read(() => this._inner.findOne(filter, options)); }
  findByIndex(indexName, values) { return this._read(() => this._inner.findByIndex(indexName, values)); }
  countDocuments(filter) { return this._read(() => this._inner.countDocuments(filter)); }
  estimatedDocumentCount() { return this._read(() => this._inner.estimatedDocumentCount()); }
  distinct(field, filter) { return this._read(() => this._inner.distinct(field, filter)); }
  aggregate(pipeline) { return this._inner.aggregate(pipeline); }
  explain(filter) { return this._read(() => this._inner.explain(filter)); }
  watch(options) { return this._inner.watch(options); }
  listIndexes() { return this._read(() => this._inner.listIndexes()); }
  /** Compact stays un-logged but serialized like WalDb.compact -- it
   * changes the collection's file set, which must not happen mid-snapshot. */
  compact() { return this._wal._serialize(() => this._inner.compact()); }
  appliedIndex() { return this._inner.appliedIndex(); }

  // ---- logged writes ------------------------------------------------------

  /**
   * Plan → log → apply: the one write path, and the whole of this class's
   * logged half.
   *
   * The planner (db_wal.h) resolves the request into id-targeted commands
   * — running at most one query to do it — and hands back the matched or
   * upserted _id and the matched document's pre-image alongside them.
   * Every findOne and projected find this class used to run before
   * logging is inside that single call now.
   *
   * Returns the plan, so callers can shape their driver-facing result
   * from what was PLANNED rather than from what apply happened to return:
   * an upsert is logged as a plain insert, and only the plan still knows
   * it was an upsert.
   */
  async _writeCollecting(req, a, b, { upsert = false, ordered = true } = {}) {
    const plan = walPlan(this._inner, this.name, req, a, b, { upsert });
    if (plan.commands.length === 0) return { plan, results: [], firstError: null };
    const { results, firstError } = await this._wal._commit(plan.commands, { ordered });
    return { plan, results, firstError };
  }

  /** _writeCollecting, throwing the first apply error — what every caller
   * but insertMany wants, since a single-document write has exactly one
   * error to report and no partial result to carry it. */
  async _write(req, a, b, opts) {
    const out = await this._writeCollecting(req, a, b, opts);
    if (out.firstError) throw out.firstError.error;
    return out;
  }

  /** The driver's result shape for the update/replace family, read off
   * the plan: matched-and-written, upserted, or neither. */
  static _writeResult(plan) {
    if (plan.outcome === WAL_PLAN.UPSERT) {
      return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: plan.targetId };
    }
    const n = plan.commands.length;
    return { acknowledged: true, matchedCount: n, modifiedCount: n, upsertedId: null };
  }

  /** Index DDL is logged (step-4 decision: replicas and crash replay must
   * perform it too); see _applyCommand for the idempotent-replay story. */
  async createIndex(keys, options = {}) {
    return this._wal._serialize(async () =>
      (await this._write(WAL_REQ.CREATE_INDEX, keys, options)).results[0]);
  }

  async dropIndex(name) {
    return this._wal._serialize(async () =>
      (await this._write(WAL_REQ.DROP_INDEX, name)).results[0]);
  }

  async insertOne(doc) {
    if (doc === null || typeof doc !== 'object' || Array.isArray(doc)) {
      throw new Error('insertOne requires a document object');
    }
    const _id = doc._id !== undefined ? doc._id : new ObjectId();
    return this._wal._serialize(async () =>
      (await this._write(WAL_REQ.INSERT_ONE, { ...doc, _id })).results[0]);
  }

  async insertMany(docs, { ordered = true } = {}) {
    if (!Array.isArray(docs) || docs.length === 0) {
      throw new Error('insertMany requires a non-empty array of documents');
    }
    const withIds = docs.map((doc) => ({ ...doc, _id: doc._id !== undefined ? doc._id : new ObjectId() }));
    return this._wal._serialize(async () => {
      // The one caller that collects rather than throws: insertMany's
      // contract is to report a partial result on the failing document,
      // which the scan below builds.
      const { results } = await this._writeCollecting(WAL_REQ.INSERT_MANY, withIds, null, { ordered });
      // Mirror the inner insertMany's result/throw contract: scan in doc
      // order, throw at the first failed document with the partial result.
      const insertedIds = {};
      let insertedCount = 0;
      for (let i = 0; i < withIds.length; i++) {
        if (results[i]) {
          insertedIds[i] = results[i].insertedId;
          insertedCount++;
        } else {
          const err = new Error(`insertMany (document ${i}) failed`);
          err.result = { acknowledged: true, insertedCount, insertedIds };
          throw err;
        }
      }
      return { acknowledged: true, insertedCount, insertedIds };
    });
  }

  async deleteOne(filter = {}) {
    return this._wal._serialize(async () => {
      const { results } = await this._write(WAL_REQ.DELETE_ONE, filter);
      return { acknowledged: true, deletedCount: results[0]?.deletedCount ?? 0 };
    });
  }

  async deleteMany(filter = {}) {
    return this._wal._serialize(async () => {
      // Summed rather than counted: a planned delete of a known _id that
      // reports 0 means the log holds a delete for a document that was
      // not there, and reporting 1 would hide it.
      const { results } = await this._write(WAL_REQ.DELETE_MANY, filter);
      return { acknowledged: true, deletedCount: results.reduce((n, r) => n + r.deletedCount, 0) };
    });
  }

  async findOneAndDelete(filter = {}) {
    return this._wal._serialize(async () => {
      // The pre-image is the planner's, not a second query's.
      const { plan } = await this._write(WAL_REQ.DELETE_ONE, filter);
      return plan.preimage;
    });
  }

  async updateOne(filter, update, { upsert = false } = {}) {
    return this._wal._serialize(async () => {
      const { plan } = await this._write(WAL_REQ.UPDATE_ONE, filter, resolveCurrentDate(update), { upsert });
      return WalCollection._writeResult(plan);
    });
  }

  async replaceOne(filter, replacement, { upsert = false } = {}) {
    return this._wal._serialize(async () => {
      const { plan } = await this._write(WAL_REQ.REPLACE_ONE, filter, replacement, { upsert });
      return WalCollection._writeResult(plan);
    });
  }

  async updateMany(filter, update, { upsert = false } = {}) {
    return this._wal._serialize(async () => {
      // ordered: mirror the engine's stop-at-first-error.
      const { plan } = await this._write(WAL_REQ.UPDATE_MANY, filter, resolveCurrentDate(update), { upsert });
      return WalCollection._writeResult(plan);
    });
  }

  /** 'before' is the planner's pre-image — null for an upsert, matching
   * the driver (there was no prior state). 'after' reads back the _id the
   * plan resolved; writes are serialized, so nothing intervenes. */
  async _findOneAndWrite(req, filter, arg, upsert, returnDocument) {
    return this._wal._serialize(async () => {
      const { plan } = await this._write(req, filter, arg, { upsert });
      if (plan.outcome === WAL_PLAN.NOTHING) return null;
      return returnDocument === 'after'
        ? this._inner.findOne({ _id: plan.targetId })
        : plan.preimage;
    });
  }

  async findOneAndUpdate(filter, update, { upsert = false, returnDocument = 'before' } = {}) {
    return this._findOneAndWrite(WAL_REQ.UPDATE_ONE, filter, resolveCurrentDate(update), upsert, returnDocument);
  }

  async findOneAndReplace(filter, replacement, { upsert = false, returnDocument = 'before' } = {}) {
    return this._findOneAndWrite(WAL_REQ.REPLACE_ONE, filter, replacement, upsert, returnDocument);
  }

  /** Same loop as the inner bulkWrite (wasm/nisaba-wasm.js), dispatching
   * to this class's logged methods; each sub-operation proposes and
   * applies individually, exactly as the inner one commits per call. */
  async bulkWrite(operations, { ordered = true } = {}) {
    if (!Array.isArray(operations)) {
      throw new Error('bulkWrite requires a non-empty array of operations');
    }
    // The same shared loop the inner collection uses; `this` is what makes
    // it the logged one, since every sub-operation goes through this
    // class's own write methods.
    return runBulkWrite(this, operations, ordered);
  }

  /** Same TTL loop as the inner pruneExpired, but the cutoffs become
   * concrete Dates inside this class's logged deleteMany — the one clock
   * read in the write path, resolved at proposal time like everything
   * else. */
  async pruneExpired() {
    // Same policy as the inner collection (db_ttl.h computes both the
    // cutoff and the filter shape); the difference that matters is what
    // happens next -- these deletes go through the logged path, so the
    // cutoff is a concrete Date inside an ordinary logged command rather
    // than a rule re-evaluated at apply time.
    let deletedCount = 0;
    for (const filter of ttlFilters(this._inner._indexes, Date.now())) {
      const { deletedCount: n } = await this.deleteMany(filter);
      deletedCount += n;
    }
    return deletedCount;
  }
}

/**
 * Open a WAL-fronted database: connect the inner Db, adopt the snapshot
 * store's newest valid generation (sweeping crashed/superseded ones),
 * adopt the newest entry log that opens — generation-paired logs newest
 * first (a torn newest falls back to its predecessor, which is only ever
 * pruned after a successor is durable), then the legacy fixed-name file
 * — and replay whatever committed suffix the collections haven't
 * applied. Options pass through to connect() (order, autoCompact), plus
 * `snapshotPrefix` (default '__snap').
 */
/**
 * Open the snapshot store (when the provider can list files) and adopt
 * the newest entry log that opens — generation-paired logs newest first,
 * the legacy fixed-name file last. Shared by connectWal and
 * connectReplicated (src/db-replicated.js). `setHardState` is left to
 * the caller: single-node WAL pins term 1; a Raft node owns its terms.
 */
export async function openWalStorage(provider, { snapshotPrefix = SNAP_PREFIX } = {}) {
  let store = null;
  if (typeof provider.listFiles === 'function') {
    store = new SnapshotStore(providerDirectory(provider), { prefix: snapshotPrefix });
    await store.open();
  }

  let log = null;
  let logName = null;
  const candidates = store ? await store.logCandidates() : [];
  candidates.push(WAL_FILE);
  for (const name of candidates) {
    try {
      const l = new EntryLog(await provider.openFile(name, { create: false }));
      await l.open();
      log = l;
      logName = name;
      break;
    } catch { /* missing or torn: try the predecessor */ }
  }
  if (!log) {
    if (store && store.latest) {
      // A snapshot with no openable log: start a fresh one at the
      // snapshot boundary so recovery/replication resume from there.
      const { name, handle } = await store.createLogFile();
      handle.truncate(0); // the name may be a torn leftover
      log = new EntryLog(handle, {
        baseIndex: store.latest.lastIncludedIndex,
        baseTerm: store.latest.lastIncludedTerm
      });
      logName = name;
    } else {
      log = new EntryLog(await provider.openFile(WAL_FILE, { create: true }));
      logName = WAL_FILE;
    }
    await log.open();
  }

  if (store) {
    await store.pruneLogs(logName);
    if (logName !== WAL_FILE) {
      try { await provider.deleteFile(WAL_FILE); } catch { /* best-effort */ }
    }
  }
  return { store, log, logName };
}

export async function connectWal(provider, options = {}) {
  const { snapshotPrefix = SNAP_PREFIX, ...dbOptions } = options;
  const db = await connect(provider, dbOptions);
  try {
    const { store, log } = await openWalStorage(provider, { snapshotPrefix });
    if (log.currentTerm === 0) log.setHardState(1);
    const walDb = new WalDb(db, log, { provider, store });
    await walDb._recover();
    return walDb;
  } catch (err) {
    await db.close();
    throw err;
  }
}

/**
 * The local half of a snapshot install (and the disaster-recovery path):
 * put the database's live files exactly at the store's adopted snapshot.
 * Deletes the current catalog and every collection/index/journal file
 * (a stale journal against restored files could rewind them), then
 * stream-copies each generation file to its live name, verifying each
 * against the manifest's CRC. Entry-log files are untouched: a following
 * connectWal() adopts the newest log and replays the suffix beyond the
 * snapshot boundary — delete the log files first for a boundary-exact
 * restore. Returns the adopted snapshot descriptor.
 */
export async function restoreLatestSnapshot(provider, { snapshotPrefix = SNAP_PREFIX } = {}) {
  if (typeof provider.listFiles !== 'function') {
    throw new Error('restoreLatestSnapshot requires a storage provider with listFiles()');
  }
  const store = new SnapshotStore(providerDirectory(provider), { prefix: snapshotPrefix });
  await store.open();
  return restoreFromStore(provider, store);
}

/** The store-half of restoreLatestSnapshot, for callers that already
 * hold an open store (the replicated install path). */
export async function restoreFromStore(provider, store) {
  if (!store.latest) throw new Error('No snapshot to restore');
  for (const name of await provider.listFiles()) {
    if (name === dbCatalogFile() || isDbFile(name)) await provider.deleteFile(name);
  }
  for (const { role, name } of store.latest.config.live) {
    await store.copyFile(role, await provider.openFile(name, { create: true }));
  }
  return store.latest;
}

export { WalDb, WalCollection, WAL_FILE, SNAP_PREFIX, providerDirectory };
