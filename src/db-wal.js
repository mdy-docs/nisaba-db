/**
 * db-wal.js — single-node write-ahead logging for the document database:
 * replication roadmap step 2 (docs/replicaton-roadmap.md), Raft minus
 * networking. Every write becomes a binjson-encoded command appended to a
 * shared EntryLog (`__wal__.bj`, outside the orphan sweep's
 * DB_FILE_PATTERN) and made durable (sync()) BEFORE it is applied to the
 * collections; recovery replays the committed log suffix each collection
 * hasn't seen (its appliedIndex(), roadmap step 1). The propose/apply/
 * recover machinery here is exactly what the replicated apply path will
 * run once a Raft core sits in front of it — steps 4/5 change who calls
 * it, not what it does.
 *
 * Determinism: a command must replay to the identical state on any
 * replica, so ALL nondeterminism resolves at proposal time, before the
 * command is logged:
 *   - documents are logged with their _id already assigned;
 *   - $currentDate resolves to concrete Dates (resolveCurrentDate — the
 *     same helper the inner collection uses at apply time, which is a
 *     no-op on an already-resolved update);
 *   - matched updates/replaces/deletes are resolved to their target _id
 *     (a no-match non-upsert write is not logged at all);
 *   - an upsert that found no match pins the id the inserted document
 *     will get (`did`), threaded to the engine as _defaultId;
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
 * (appliedIndex is carried through, see step 1's tests). dropCollection
 * is REFUSED on a WAL database: its old entries would resurrect the
 * collection on replay; the safe barrier is log compaction (roadmap
 * step 3).
 */
import {
  connect,
  EntryLog,
  ObjectId,
  encode,
  decode,
  resolveCurrentDate
} from '../wasm/nisaba-wasm.js';

const WAL_FILE = '__wal__.bj';

/** One entry ~= one collection commit. Command shapes ('c' = collection):
 *   { c, op: 'i',  doc }                  insert (doc._id resolved)
 *   { c, op: 'u',  id, update }           update the document `id`
 *   { c, op: 'uu', filter, update, did }  upsert-update (no match at
 *                                         proposal; did = pinned _id)
 *   { c, op: 'r',  id, doc }              replace the document `id`
 *   { c, op: 'ru', filter, doc, did }     upsert-replace
 *   { c, op: 'd',  id }                   delete the document `id`
 */
class WalDb {
  constructor(db, log) {
    this._db = db;
    this._log = log;
    this._collections = new Map(); // name -> WalCollection
    this._chain = Promise.resolve();
    this._broken = null; // Error that poisoned the log (failed sync), or null
    this.isOpen = true;
  }

  /** The underlying EntryLog — read-only from the host's point of view
   * (lastIndex, getBatch for inspection); the Raft layer (roadmap steps
   * 4/5) will drive it directly. */
  get log() { return this._log; }

  async collection(name) {
    let col = this._collections.get(name);
    if (!col) {
      col = new WalCollection(this, name, await this._db.collection(name));
      this._collections.set(name, col);
    }
    return col;
  }

  async listCollections() { return this._db.listCollections(); }

  async dropCollection() {
    throw new Error(
      'dropCollection is not supported on a WAL database yet: the log still ' +
      'holds the collection\'s entries, which would resurrect it on replay. ' +
      'Log compaction (replication roadmap step 3) provides the barrier.'
    );
  }

  async compact(options) { return this._db.compact(options); }

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
   * Returns the first entry's index; the rest are contiguous. */
  _propose(cmds) {
    if (this._broken) {
      throw new Error(`WAL is poisoned by an earlier sync failure: ${this._broken.message}`, { cause: this._broken });
    }
    const term = this._log.currentTerm;
    let first = 0;
    for (const cmd of cmds) {
      const index = this._log.append(term, encode(cmd));
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

  /** Apply one logged command to the state machine — the ONLY code that
   * mutates collections, shared verbatim by the live write path and
   * recovery replay. Stages the entry's index first (step 1's contract),
   * so the mutation's own commit persists it. */
  async _applyCommand(index, cmd) {
    const col = await this._db.collection(cmd.c);
    await col.setAppliedIndex(index);
    switch (cmd.op) {
      case 'i': return col.insertOne(cmd.doc);
      case 'u': return col.updateOne({ _id: cmd.id }, cmd.update);
      case 'uu': return col.updateOne(cmd.filter, cmd.update, { upsert: true, _defaultId: cmd.did });
      case 'r': return col.replaceOne({ _id: cmd.id }, cmd.doc);
      case 'ru': return col.replaceOne(cmd.filter, cmd.doc, { upsert: true, _defaultId: cmd.did });
      case 'd': return col.deleteOne({ _id: cmd.id });
      default: throw new Error(`WAL: unknown command op "${cmd.op}"`);
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
          await this._applyCommand(entry.index, cmd);
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

  find(filter, options) { return this._inner.find(filter, options); }
  findOne(filter, options) { return this._inner.findOne(filter, options); }
  findByIndex(indexName, values) { return this._inner.findByIndex(indexName, values); }
  countDocuments(filter) { return this._inner.countDocuments(filter); }
  estimatedDocumentCount() { return this._inner.estimatedDocumentCount(); }
  distinct(field, filter) { return this._inner.distinct(field, filter); }
  aggregate(pipeline) { return this._inner.aggregate(pipeline); }
  explain(filter) { return this._inner.explain(filter); }
  watch(options) { return this._inner.watch(options); }
  listIndexes() { return this._inner.listIndexes(); }
  createIndex(keys, options) { return this._inner.createIndex(keys, options); }
  dropIndex(name) { return this._inner.dropIndex(name); }
  compact() { return this._inner.compact(); }
  appliedIndex() { return this._inner.appliedIndex(); }

  // ---- logged writes ------------------------------------------------------

  /** Propose-then-apply for a batch of already-deterministic commands.
   * `apply(i, index)` applies commands[i] (logged at `index`) and returns
   * its result; `ordered` stops at the first failure and retracts the
   * never-applied suffix. Returns { results, firstError } — per-command
   * results (undefined where skipped/failed). */
  async _run(cmds, { ordered = true, apply } = {}) {
    const first = this._wal._propose(cmds);
    const results = new Array(cmds.length);
    let firstError = null;
    for (let i = 0; i < cmds.length; i++) {
      const index = first + i;
      try {
        results[i] = await (apply ? apply(i, index) : this._wal._applyCommand(index, cmds[i]));
      } catch (err) {
        if (firstError === null) firstError = { index: i, error: err };
        if (ordered) {
          this._wal._retractFrom(index);
          break;
        }
      }
    }
    return { results, firstError };
  }

  async insertOne(doc) {
    if (doc === null || typeof doc !== 'object' || Array.isArray(doc)) {
      throw new Error('insertOne requires a document object');
    }
    const _id = doc._id !== undefined ? doc._id : new ObjectId();
    return this._wal._serialize(async () => {
      const { results, firstError } = await this._run([{ c: this.name, op: 'i', doc: { ...doc, _id } }]);
      if (firstError) throw firstError.error;
      return results[0];
    });
  }

  async insertMany(docs, { ordered = true } = {}) {
    if (!Array.isArray(docs) || docs.length === 0) {
      throw new Error('insertMany requires a non-empty array of documents');
    }
    const withIds = docs.map((doc) => ({ ...doc, _id: doc._id !== undefined ? doc._id : new ObjectId() }));
    return this._wal._serialize(async () => {
      const cmds = withIds.map((doc) => ({ c: this.name, op: 'i', doc }));
      const { results } = await this._run(cmds, { ordered });
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
      const target = await this._inner.findOne(filter, { projection: { _id: 1 } });
      if (!target) return { acknowledged: true, deletedCount: 0 };
      const { results, firstError } = await this._run([{ c: this.name, op: 'd', id: target._id }]);
      if (firstError) throw firstError.error;
      return results[0];
    });
  }

  async deleteMany(filter = {}) {
    return this._wal._serialize(async () => {
      const ids = (await this._inner.find(filter, { projection: { _id: 1 } }).toArray()).map((d) => d._id);
      if (ids.length === 0) return { acknowledged: true, deletedCount: 0 };
      const cmds = ids.map((id) => ({ c: this.name, op: 'd', id }));
      const { results, firstError } = await this._run(cmds);
      if (firstError) throw firstError.error;
      return { acknowledged: true, deletedCount: results.reduce((n, r) => n + r.deletedCount, 0) };
    });
  }

  async findOneAndDelete(filter = {}) {
    return this._wal._serialize(async () => {
      const doc = await this._inner.findOne(filter);
      if (!doc) return null;
      const { firstError } = await this._run([{ c: this.name, op: 'd', id: doc._id }]);
      if (firstError) throw firstError.error;
      return doc;
    });
  }

  /** Shared proposal shape for updateOne/replaceOne/findOneAndUpdate/
   * findOneAndReplace: resolve the target to its _id (matched) or pin the
   * upsert id, log the one command, apply via `applyMatched`/`applyUpsert`
   * (which run the inner method so return semantics are exact). */
  async _updateLike(filter, matchedCmd, upsertCmd, { upsert, noMatch, applyMatched, applyUpsert }) {
    return this._wal._serialize(async () => {
      const target = await this._inner.findOne(filter, { projection: { _id: 1 } });
      if (!target && !upsert) return noMatch;
      const did = target ? null : new ObjectId();
      const cmd = target ? matchedCmd(target._id) : upsertCmd(did);
      const { results, firstError } = await this._run([cmd], {
        apply: async (_, index) => {
          const col = await this._wal._db.collection(this.name);
          await col.setAppliedIndex(index);
          return target ? applyMatched(col, target._id) : applyUpsert(col, did);
        }
      });
      if (firstError) throw firstError.error;
      return results[0];
    });
  }

  async updateOne(filter, update, { upsert = false } = {}) {
    update = resolveCurrentDate(update);
    return this._updateLike(
      filter,
      (id) => ({ c: this.name, op: 'u', id, update }),
      (did) => ({ c: this.name, op: 'uu', filter, update, did }),
      {
        upsert,
        noMatch: { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null },
        applyMatched: (col, id) => col.updateOne({ _id: id }, update),
        applyUpsert: (col, did) => col.updateOne(filter, update, { upsert: true, _defaultId: did })
      }
    );
  }

  async replaceOne(filter, replacement, { upsert = false } = {}) {
    return this._updateLike(
      filter,
      (id) => ({ c: this.name, op: 'r', id, doc: replacement }),
      (did) => ({ c: this.name, op: 'ru', filter, doc: replacement, did }),
      {
        upsert,
        noMatch: { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null },
        applyMatched: (col, id) => col.replaceOne({ _id: id }, replacement),
        applyUpsert: (col, did) => col.replaceOne(filter, replacement, { upsert: true, _defaultId: did })
      }
    );
  }

  async findOneAndUpdate(filter, update, { upsert = false, returnDocument = 'before' } = {}) {
    update = resolveCurrentDate(update);
    return this._updateLike(
      filter,
      (id) => ({ c: this.name, op: 'u', id, update }),
      (did) => ({ c: this.name, op: 'uu', filter, update, did }),
      {
        upsert,
        noMatch: null,
        applyMatched: (col, id) => col.findOneAndUpdate({ _id: id }, update, { returnDocument }),
        applyUpsert: (col, did) => col.findOneAndUpdate(filter, update, { upsert: true, returnDocument, _defaultId: did })
      }
    );
  }

  async findOneAndReplace(filter, replacement, { upsert = false, returnDocument = 'before' } = {}) {
    return this._updateLike(
      filter,
      (id) => ({ c: this.name, op: 'r', id, doc: replacement }),
      (did) => ({ c: this.name, op: 'ru', filter, doc: replacement, did }),
      {
        upsert,
        noMatch: null,
        applyMatched: (col, id) => col.findOneAndReplace({ _id: id }, replacement, { returnDocument }),
        applyUpsert: (col, did) => col.findOneAndReplace(filter, replacement, { upsert: true, returnDocument, _defaultId: did })
      }
    );
  }

  async updateMany(filter, update, { upsert = false } = {}) {
    update = resolveCurrentDate(update);
    return this._wal._serialize(async () => {
      const ids = (await this._inner.find(filter, { projection: { _id: 1 } }).toArray()).map((d) => d._id);
      if (ids.length === 0) {
        if (!upsert) return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: null };
        const did = new ObjectId();
        const { results, firstError } = await this._run([{ c: this.name, op: 'uu', filter, update, did }]);
        if (firstError) throw firstError.error;
        return { acknowledged: true, matchedCount: 0, modifiedCount: 0, upsertedId: results[0].upsertedId };
      }
      const cmds = ids.map((id) => ({ c: this.name, op: 'u', id, update }));
      const { firstError } = await this._run(cmds); // ordered: mirror the engine's stop-at-first-error
      if (firstError) throw firstError.error;
      return { acknowledged: true, matchedCount: ids.length, modifiedCount: ids.length, upsertedId: null };
    });
  }

  /** Same loop as the inner bulkWrite (wasm/nisaba-wasm.js), dispatching
   * to this class's logged methods; each sub-operation proposes and
   * applies individually, exactly as the inner one commits per call. */
  async bulkWrite(operations, { ordered = true } = {}) {
    if (!Array.isArray(operations) || operations.length === 0) {
      throw new Error('bulkWrite requires a non-empty array of operations');
    }
    const result = {
      acknowledged: true, insertedCount: 0, matchedCount: 0, modifiedCount: 0,
      deletedCount: 0, upsertedCount: 0, insertedIds: {}, upsertedIds: {}
    };
    const errors = [];
    for (let i = 0; i < operations.length; i++) {
      const op = operations[i];
      const type = Object.keys(op)[0];
      const spec = op[type];
      try {
        switch (type) {
          case 'insertOne': {
            const { insertedId } = await this.insertOne(spec.document);
            result.insertedIds[i] = insertedId;
            result.insertedCount++;
            break;
          }
          case 'updateOne':
          case 'updateMany':
          case 'replaceOne': {
            const method = type === 'replaceOne' ? spec.replacement : spec.update;
            const r = type === 'updateOne' ? await this.updateOne(spec.filter, method, { upsert: spec.upsert })
              : type === 'updateMany' ? await this.updateMany(spec.filter, method, { upsert: spec.upsert })
              : await this.replaceOne(spec.filter, method, { upsert: spec.upsert });
            result.matchedCount += r.matchedCount;
            result.modifiedCount += r.modifiedCount;
            if (r.upsertedId) { result.upsertedIds[i] = r.upsertedId; result.upsertedCount++; }
            break;
          }
          case 'deleteOne': {
            const r = await this.deleteOne(spec.filter);
            result.deletedCount += r.deletedCount;
            break;
          }
          case 'deleteMany': {
            const r = await this.deleteMany(spec.filter);
            result.deletedCount += r.deletedCount;
            break;
          }
          default:
            throw new Error(`bulkWrite: unknown operation type "${type}"`);
        }
      } catch (err) {
        errors.push({ index: i, error: err });
        if (ordered) break;
      }
    }
    if (errors.length > 0) {
      const err = new Error(
        `bulkWrite: ${errors.length} operation(s) failed (first at index ${errors[0].index}: ${errors[0].error.message})`
      );
      err.result = result;
      err.writeErrors = errors;
      throw err;
    }
    return result;
  }

  /** Same TTL loop as the inner pruneExpired, but the cutoffs become
   * concrete Dates inside this class's logged deleteMany — the one clock
   * read in the write path, resolved at proposal time like everything
   * else. */
  async pruneExpired() {
    let deletedCount = 0;
    for (const ix of this._inner._indexes.values()) {
      if (ix.kind !== 'equality' || ix.expireAfterSeconds === undefined) continue;
      const cutoff = new Date(Date.now() - ix.expireAfterSeconds * 1000);
      const { deletedCount: n } = await this.deleteMany({ [ix.fields[0]]: { $lt: cutoff } });
      deletedCount += n;
    }
    return deletedCount;
  }
}

/**
 * Open a WAL-fronted database: connect the inner Db, open (or create) the
 * shared entry log, and replay whatever committed suffix the collections
 * haven't applied. Options pass through to connect() (order, autoCompact).
 */
export async function connectWal(provider, options = {}) {
  const db = await connect(provider, options);
  const handle = await provider.openFile(WAL_FILE, { create: true });
  const log = new EntryLog(handle);
  try {
    await log.open();
    if (log.currentTerm === 0) log.setHardState(1);
  } catch (err) {
    await db.close();
    throw err;
  }
  const walDb = new WalDb(db, log);
  await walDb._recover();
  return walDb;
}

export { WalDb, WalCollection, WAL_FILE };
