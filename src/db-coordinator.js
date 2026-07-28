/**
 * db-coordinator.js — multi-tab/multi-worker coordination for the OPFS-backed
 * document database (docs/db-plan.md, "Open decisions / risks: OPFS
 * concurrency").
 *
 * FileSystemSyncAccessHandle (what every OPFS file in this repo uses, via
 * OPFSStorageProvider/registerHandle in wasm/nisaba-wasm.js) takes an exclusive,
 * origin-wide lock per file: only one context can have a collection's files
 * open at a time. `connectShared` lets many tabs/workers share one logical
 * database anyway, by electing exactly one of them the "leader" (the only
 * one that actually opens the real Db/OPFS files) and proxying every other
 * context's calls to it.
 *
 * This module must run somewhere with `navigator.locks`/`BroadcastChannel`
 * — a dedicated Worker, matching the existing constraint that OPFS access
 * itself already requires a Worker (see README.md, src/db-worker.js). Every
 * tab already needs its own worker for OPFS regardless of this feature, so
 * the whole coordinator — election, RPC, and the real Db — lives inside
 * that worker; the tab's main thread is unaware any of this exists and
 * talks to its own worker exactly as it always has.
 *
 * Coordination mechanism (chosen over a SharedWorker-as-sole-owner design
 * specifically because iOS Safari has never supported SharedWorker at all,
 * which would silently break multi-tab on a platform this repo already
 * targets via OPFS's own Safari 16.4+ requirement):
 *
 *   - Leader election: `navigator.locks.request(lockName, ...)` — the
 *     context whose callback fires holds the lock (is the leader) until it
 *     releases it (on close()), at which point the next queued context is
 *     granted the lock automatically. No polling, no split-brain: the
 *     browser guarantees at most one holder.
 *   - RPC: one BroadcastChannel per logical database. Followers broadcast
 *     `request` messages; whichever context currently holds the lock is the
 *     only one listening for them and replies with `response`/`error`.
 *     Payloads are binjson-encoded (encode/decode from ./binjson.js, the
 *     same codec every document already round-trips through) rather than
 *     JSON, so ObjectId/Date survive the trip unchanged.
 *
 * `SharedDb`/`SharedCollection` mirror Db/Collection's public API exactly
 * (wasm/nisaba-wasm.js) so existing single-tab code barely changes to adopt
 * this -- test/db-coordinator.test.js enforces the parity by reflection.
 * The only structural difference is `find()`'s cursor resolving with one
 * RPC call on its first pull (toArray()/next()/iteration) rather than
 * streaming batches; partial-failure details (insertMany/bulkWrite's
 * `err.result`/`err.writeErrors`) are re-attached on the follower side so
 * error handling code works unchanged too.
 */
import { encode, decode, connect, ChangeStream } from '../wasm/nisaba-wasm.js';

const REQUEST_TIMEOUT_MS = 5000;
const REELECT_WAIT_MS = 2000;
const HEARTBEAT_INTERVAL_MS = 2000;
// Completed request replies the leader keeps for replay (see
// _handleRequest's dedup). Sized for "every tab retried at once", not for
// history: a retry arrives within one REQUEST_TIMEOUT_MS + REELECT_WAIT_MS
// window, during which even a busy leader sees far fewer distinct requests.
const RPC_REPLY_CACHE = 128;

function lockName(dbName) { return `binjson-db-lock:${dbName}`; }
function channelName(dbName) { return `binjson-db-coord:${dbName}`; }

/**
 * Run the real db.collection(name)[method](...args) (or db[method](...args)
 * when collectionName is null), special-casing 'find' since the real
 * Collection.find() returns a lazy cursor rather than a promise — RPC
 * always wants the fully resolved array. Shared by the leader's own local
 * calls and its handling of followers' `request` messages, so there is
 * exactly one execution path regardless of who is asking. `coordinator`,
 * when given, subscribes once per collection name to rebroadcast that
 * collection's watch() change events to every other tab (see
 * Coordinator._ensureRebroadcast) — every write, whether local or a
 * follower's proxied RPC, passes through here.
 */
async function executeOnRealDb(realDb, collectionName, method, args, coordinator) {
  if (collectionName === null) return realDb[method](...args);
  const coll = await realDb.collection(collectionName);
  if (coordinator) coordinator._ensureRebroadcast(collectionName, coll);
  if (method === 'find') {
    const [filter, options] = args;
    return coll.find(filter, options).toArray();
  }
  if (method === 'aggregate') {
    return coll.aggregate(args[0]).toArray(); // like find: RPC wants the resolved array, not a cursor
  }
  return coll[method](...args);
}

class Coordinator {
  constructor(dbName) {
    this.dbName = dbName;
    this.role = 'unknown'; // 'unknown' | 'leader' | 'follower'
    this.realDb = null;
    this.channel = new BroadcastChannel(channelName(dbName));
    this.pending = new Map(); // requestId -> { resolve, reject }
    this._roleResolvers = [];  // resolved once, on the first role transition
    this._leaderSignalResolvers = []; // resolved (repeatedly) on any leader sighting
    this._heartbeatTimer = null;
    this._resolveHold = null;
    this._closed = false;
    this._rebroadcasting = new Set(); // collection names already subscribed for rebroadcast (leader only)
    this._changeListeners = new Map(); // collectionName -> Set<ChangeStream>, this context's own watch() callers
    this._rpcReplies = new Map(); // requestId -> Promise<{type, payload}>, leader-side replay cache (see _handleRequest)

    this.channel.addEventListener('message', (event) => this._onMessage(event.data));
  }

  async start(provider, options) {
    const holdPromise = new Promise((resolve) => { this._resolveHold = resolve; });
    let rejectStart;
    const startFailed = new Promise((_, reject) => { rejectStart = reject; });

    // Fired, not awaited: resolves only once this context is granted the
    // lock, i.e. becomes leader. Not awaiting lets connect() return as soon
    // as *either* this fires or another leader announces itself.
    this._abortController = new AbortController();
    navigator.locks.request(lockName(this.dbName), { mode: 'exclusive', signal: this._abortController.signal }, async () => {
      if (this._closed) return; // closed before the lock was granted -- release immediately
      try {
        await this._becomeLeader(provider, options);
      } catch (err) {
        rejectStart(err);
        return; // never became leader; release the lock for the next waiter
      }
      return holdPromise;
    }).catch((err) => {
      if (err && err.name === 'AbortError') return; // expected: close() before the lock was granted
      rejectStart(err);
    });

    const becameLeader = new Promise((resolve) => this._roleResolvers.push(resolve));
    this.channel.postMessage({ type: 'whoIsLeader' });
    await Promise.race([becameLeader, startFailed]); // settles via _becomeLeader(), an iAmLeader/announce reply, or connect() failing
  }

  async _becomeLeader(provider, options) {
    if (this.role !== 'leader') this.realDb = await connect(provider, options);
    this.role = 'leader';
    this.channel.postMessage({ type: 'announce' });
    this._settleRole();
    this._notifyLeaderSignal();
    if (!this._heartbeatTimer) {
      this._heartbeatTimer = setInterval(() => {
        if (this.role === 'leader') this.channel.postMessage({ type: 'announce' });
      }, HEARTBEAT_INTERVAL_MS);
    }
  }

  _settleRole() {
    const resolvers = this._roleResolvers;
    this._roleResolvers = [];
    for (const r of resolvers) r();
  }

  _notifyLeaderSignal() {
    const resolvers = this._leaderSignalResolvers;
    this._leaderSignalResolvers = [];
    for (const r of resolvers) r();
  }

  /** Resolves on the next iAmLeader/announce/self-election, or after `ms`. */
  _waitForLeaderSignal(ms) {
    return new Promise((resolve) => {
      this._leaderSignalResolvers.push(resolve);
      setTimeout(resolve, ms);
    });
  }

  _onMessage(msg) {
    switch (msg.type) {
      case 'whoIsLeader':
        if (this.role === 'leader') this.channel.postMessage({ type: 'iAmLeader' });
        return;
      case 'iAmLeader':
      case 'announce':
        if (this.role === 'unknown') { this.role = 'follower'; this._settleRole(); }
        this._notifyLeaderSignal();
        return;
      case 'request':
        if (this.role === 'leader') this._handleRequest(msg);
        return;
      case 'response':
      case 'error': {
        const p = this.pending.get(msg.requestId);
        if (!p) return; // not ours, or already timed out
        this.pending.delete(msg.requestId);
        if (msg.type === 'error') {
          // Rebuild the error shape _handleRequest flattened for the wire:
          // insertMany/bulkWrite callers rely on err.result (what DID land)
          // and bulkWrite on err.writeErrors, so a follower must see them
          // exactly like a local caller would.
          const info = decode(msg.payload);
          const err = new Error(info.message);
          if (info.name) err.name = info.name;         // coded-error identity survives the wire...
          if (info.code !== undefined && info.code !== null) err.code = info.code; // ...as data, not prototype
          if (info.result !== undefined && info.result !== null) err.result = info.result;
          if (info.writeErrors) err.writeErrors = info.writeErrors.map((w) => ({ index: w.index, error: new Error(w.message) }));
          p.reject(err);
        } else {
          p.resolve(decode(msg.payload));
        }
        return;
      }
      case 'change':
        this._deliverChange(msg.collectionName, decode(msg.payload));
        return;
    }
  }

  /**
   * Exactly-once against THIS leader: a follower whose request timed out
   * retries with the same requestId (see _rpcCall), so the leader keeps a
   * bounded map of requestId -> reply promise and replays rather than
   * re-executes -- covering both a request whose response got lost after
   * executing AND one still executing when the retry lands (the in-flight
   * promise is shared, never a second execution). The one remaining
   * at-most-once hole is a leadership change: a new leader has an empty
   * cache, and whether the dead leader's execution reached disk is
   * genuinely unknowable from here -- the same ambiguity every
   * failover-retry design has without a durable session log.
   */
  async _handleRequest(msg) {
    let reply = this._rpcReplies.get(msg.requestId);
    if (!reply) {
      reply = this._executeRequest(msg.payload);
      this._rpcReplies.set(msg.requestId, reply);
      reply.then(() => {
        // Trim oldest beyond the cap only once settled -- Map preserves
        // insertion order, so the front is the stalest.
        if (this._rpcReplies.size <= RPC_REPLY_CACHE) return;
        for (const id of this._rpcReplies.keys()) {
          if (this._rpcReplies.size <= RPC_REPLY_CACHE) break;
          this._rpcReplies.delete(id);
        }
      });
    }
    const { type, payload } = await reply;
    this.channel.postMessage({ type, requestId: msg.requestId, payload });
  }

  /** Run one decoded request to a posted-message shape. Never rejects:
   * errors become the wire's 'error' reply, so _rpcReplies can replay
   * them identically. */
  async _executeRequest(payload) {
    const [collectionName, method, args] = decode(payload);
    try {
      const result = await executeOnRealDb(this.realDb, collectionName, method, args, this);
      return { type: 'response', payload: encode(result === undefined ? null : result) };
    } catch (err) {
      // Flatten the partial-failure side channel some methods attach
      // (insertMany's err.result, bulkWrite's err.result/err.writeErrors)
      // into encodable data; the follower side rebuilds the Error shape.
      // writeErrors nest real Error objects, which can't cross the codec --
      // only their index + message survive the trip.
      const info = { message: err.message };
      // Coded errors (NisabaError subclasses) keep their identity across
      // the wire as data -- the follower re-attaches name/code to the
      // rebuilt Error (branch on err.name/err.code there, not instanceof).
      if (err.name && err.name !== 'Error') info.name = err.name;
      if (err.code !== undefined) info.code = err.code;
      if (err.result !== undefined) info.result = err.result;
      if (err.writeErrors) info.writeErrors = err.writeErrors.map((w) => ({ index: w.index, message: w.error?.message ?? String(w.error) }));
      return { type: 'error', payload: encode(info) };
    }
  }

  /**
   * Subscribe (once per collection name) to the leader's own real
   * Collection's watch() feed and rebroadcast every change to the whole
   * BroadcastChannel -- plus deliver it locally, since BroadcastChannel
   * never delivers a context's own messages back to itself (see
   * _rpcCall's identical note), which would otherwise silently drop events
   * from a SharedCollection.watch() call made by the leader's own tab.
   */
  _ensureRebroadcast(collectionName, coll) {
    if (this._rebroadcasting.has(collectionName)) return;
    this._rebroadcasting.add(collectionName);
    coll.watch().on('change', (change) => {
      this.channel.postMessage({ type: 'change', collectionName, payload: encode(change) });
      this._deliverChange(collectionName, change);
    });
  }

  _deliverChange(collectionName, change) {
    const listeners = this._changeListeners.get(collectionName);
    if (!listeners) return;
    for (const stream of listeners) stream._emit(change);
  }

  /** Backs SharedCollection.watch(). */
  watch(collectionName, options = {}) {
    const stream = new ChangeStream(() => {
      const set = this._changeListeners.get(collectionName);
      if (!set) return;
      set.delete(stream);
      if (set.size === 0) this._changeListeners.delete(collectionName);
    }, { maxBuffered: options.maxBuffered });
    let set = this._changeListeners.get(collectionName);
    if (!set) { set = new Set(); this._changeListeners.set(collectionName, set); }
    set.add(stream);
    return stream;
  }

  async _rpcCall(collectionName, method, args, retried = false, requestId = null) {
    // Re-checked on every (re)entry, not just in dispatch(): if this very
    // context was promoted to leader while an earlier attempt was in
    // flight, a broadcast request it sent as a follower will never be
    // answered (BroadcastChannel never delivers a context's own messages
    // back to itself) -- serve locally instead of retrying over the wire.
    if (this.role === 'leader') return executeOnRealDb(this.realDb, collectionName, method, args, this);
    // Kept identical across the one retry, so a leader that DID execute
    // the first attempt (response lost / just slow) replays its reply
    // from _rpcReplies instead of running the operation twice.
    if (!requestId) requestId = crypto.randomUUID();
    const payload = encode([collectionName, method, args]);
    const result = new Promise((resolve, reject) => this.pending.set(requestId, { resolve, reject }));
    this.channel.postMessage({ type: 'request', requestId, payload });

    let timeoutId;
    const timeout = new Promise((_, reject) => {
      timeoutId = setTimeout(() => {
        const err = new Error(`db-coordinator: request timed out (${method})`);
        err.rpcTimeout = true;
        reject(err);
      }, REQUEST_TIMEOUT_MS);
    });
    try {
      return await Promise.race([result, timeout]);
    } catch (err) {
      this.pending.delete(requestId);
      // Retry ONLY when nothing answered (rpcTimeout): the leader may be
      // gone (its tab/worker closed), so re-broadcast, give a new leader a
      // bounded window to appear (another follower's announce, or our own
      // lock finally being granted), and retry once -- with the SAME
      // requestId, so a leader that actually executed the first attempt
      // replays its cached reply (_handleRequest) rather than re-running
      // a non-idempotent operation. An error *response* means the leader
      // is alive and the operation genuinely failed -- never retried.
      if (!err.rpcTimeout || retried) throw err;
      this.channel.postMessage({ type: 'whoIsLeader' });
      await this._waitForLeaderSignal(REELECT_WAIT_MS);
      return this._rpcCall(collectionName, method, args, true, requestId);
    } finally {
      clearTimeout(timeoutId);
    }
  }

  async dispatch(collectionName, method, args) {
    if (this.role === 'leader') return executeOnRealDb(this.realDb, collectionName, method, args, this);
    return this._rpcCall(collectionName, method, args);
  }

  async close() {
    if (this._closed) return;
    this._closed = true;
    if (this._heartbeatTimer) clearInterval(this._heartbeatTimer);
    for (const { reject } of this.pending.values()) reject(new Error('db-coordinator: closed'));
    this.pending.clear();
    this._rpcReplies.clear();
    for (const set of this._changeListeners.values()) {
      for (const stream of [...set]) stream.close();
    }
    this._changeListeners.clear();
    if (this.role === 'leader' && this.realDb) await this.realDb.close();
    if (this._abortController) this._abortController.abort(); // cancel the lock request if still queued
    if (this._resolveHold) this._resolveHold(); // release the Web Lock, if held
    this.channel.close();
  }
}

class SharedCollection {
  constructor(coordinator, name) {
    this._coord = coordinator;
    this.name = name;
  }

  async createIndex(keys, options = {}) { return this._coord.dispatch(this.name, 'createIndex', [keys, options]); }
  async dropIndex(name) { return this._coord.dispatch(this.name, 'dropIndex', [name]); }
  async listIndexes() { return this._coord.dispatch(this.name, 'listIndexes', []); }
  async findByIndex(name, values) { return this._coord.dispatch(this.name, 'findByIndex', [name, values]); }
  async insertOne(doc) { return this._coord.dispatch(this.name, 'insertOne', [doc]); }
  async insertMany(docs, options = {}) { return this._coord.dispatch(this.name, 'insertMany', [docs, options]); }
  async findOne(filter = {}, options = {}) { return this._coord.dispatch(this.name, 'findOne', [filter, options]); }

  /**
   * Mirrors Collection.find()'s lazy cursor (wasm/nisaba-wasm.js): chain
   * setters mutate local state only; toArray()/next()/iteration make
   * exactly one RPC call with the fully resolved filter+options, then hand
   * documents out locally. There is no streaming batch protocol across the
   * BroadcastChannel, so unlike the real cursor, next() here materializes
   * the full result set up front -- which also means it works after
   * .sort(), where the real cursor's streaming path refuses. Consumption
   * semantics match the real cursor: one pass, toArray() returns whatever
   * next() hasn't handed out yet, close()/early-`for await` exit end it.
   */
  find(filter = {}, options = {}) {
    const coord = this._coord, name = this.name;
    const state = {
      sort: options.sort || null,
      skip: options.skip || 0,
      limit: options.limit || 0,
      projection: options.projection || null
    };
    let items = null; // full result set, fetched by the first pull
    let idx = 0;
    let closed = false;
    const cursor = {
      sort(spec) { state.sort = spec; return cursor; },
      skip(n) { state.skip = n; return cursor; },
      limit(n) { state.limit = n; return cursor; },
      project(spec) { state.projection = spec; return cursor; },

      async toArray() {
        if (closed) return [];
        if (items === null) items = await coord.dispatch(name, 'find', [filter, { ...state }]);
        const rest = items.slice(idx);
        idx = items.length;
        closed = true;
        return rest;
      },

      /** Manual pull, `{ value, done }` -- same shape as the real cursor's. */
      async next() {
        if (closed) return { value: undefined, done: true };
        if (items === null) items = await coord.dispatch(name, 'find', [filter, { ...state }]);
        if (idx >= items.length) { closed = true; return { value: undefined, done: true }; }
        return { value: items[idx++], done: false };
      },

      [Symbol.asyncIterator]() { return cursor; },

      /** The plan this cursor's filter gets on the leader -- see Collection.explain. */
      async explain() { return coord.dispatch(name, 'explain', [filter]); },

      /** Safe to call more than once, or on an already-exhausted cursor. */
      async close() { closed = true; },

      /** Invoked by `for await` on early exit (break/throw). */
      async return() { await cursor.close(); return { value: undefined, done: true }; }
    };
    return cursor;
  }

  async deleteOne(filter = {}) { return this._coord.dispatch(this.name, 'deleteOne', [filter]); }
  async deleteMany(filter = {}) { return this._coord.dispatch(this.name, 'deleteMany', [filter]); }
  async findOneAndDelete(filter = {}) { return this._coord.dispatch(this.name, 'findOneAndDelete', [filter]); }
  async replaceOne(filter, replacement, options = {}) { return this._coord.dispatch(this.name, 'replaceOne', [filter, replacement, options]); }
  async findOneAndReplace(filter, replacement, options = {}) { return this._coord.dispatch(this.name, 'findOneAndReplace', [filter, replacement, options]); }
  async updateOne(filter, update, options = {}) { return this._coord.dispatch(this.name, 'updateOne', [filter, update, options]); }
  async findOneAndUpdate(filter, update, options = {}) { return this._coord.dispatch(this.name, 'findOneAndUpdate', [filter, update, options]); }
  async updateMany(filter, update, options = {}) { return this._coord.dispatch(this.name, 'updateMany', [filter, update, options]); }
  async countDocuments(filter = {}) { return this._coord.dispatch(this.name, 'countDocuments', [filter]); }
  async estimatedDocumentCount() { return this._coord.dispatch(this.name, 'estimatedDocumentCount', []); }
  async appliedIndex() { return this._coord.dispatch(this.name, 'appliedIndex', []); }
  async setAppliedIndex(index) { return this._coord.dispatch(this.name, 'setAppliedIndex', [index]); }
  async distinct(field, filter = {}) { return this._coord.dispatch(this.name, 'distinct', [field, filter]); }
  async explain(filter = {}) { return this._coord.dispatch(this.name, 'explain', [filter]); }
  /** Same cursor-like shape as the real aggregate(); one RPC on first pull. */
  aggregate(pipeline = []) {
    const coord = this._coord, name = this.name;
    let items = null;
    let idx = 0;
    const cursor = {
      async toArray() {
        if (items === null) items = await coord.dispatch(name, 'aggregate', [pipeline]);
        const rest = items.slice(idx);
        idx = items.length;
        return rest;
      },
      async next() {
        if (items === null) items = await coord.dispatch(name, 'aggregate', [pipeline]);
        return idx < items.length ? { value: items[idx++], done: false } : { value: undefined, done: true };
      },
      [Symbol.asyncIterator]() { return cursor; },
      async close() { items = items || []; idx = items.length; },
      async return() { await cursor.close(); return { value: undefined, done: true }; }
    };
    return cursor;
  }
  async bulkWrite(operations, options = {}) { return this._coord.dispatch(this.name, 'bulkWrite', [operations, options]); }
  async pruneExpired() { return this._coord.dispatch(this.name, 'pruneExpired', []); }
  /** Runs on the leader (the only context holding the files). An operation
   * from any tab that races the swap simply queues behind it on the real
   * Collection's compaction gate (wasm/nisaba-wasm.js, _compacting) -- a
   * brief wait inside the normal RPC timeout, not an error. */
  async compact() { return this._coord.dispatch(this.name, 'compact', []); }

  /** Same shape/scope limits as Collection.watch() (wasm/nisaba-wasm.js),
   * but sees writes from every tab sharing this database, not just this
   * one -- the leader rebroadcasts its real Collection's change events to
   * every other tab (see Coordinator._ensureRebroadcast). */
  watch(pipeline = [], options = {}) {
    if (pipeline.length) {
      throw new Error('SharedCollection.watch: pipeline stages are not supported yet');
    }
    return this._coord.watch(this.name, options);
  }
}

class SharedDb {
  constructor(coordinator) {
    this._coord = coordinator;
  }

  /** Synchronous like Db.collection is async in name only -- no per-name
   * open step is needed client-side; the leader opens (or reuses its own
   * already-open) real Collection lazily on first RPC for that name. */
  async collection(name) { return new SharedCollection(this._coord, name); }
  async listCollections() { return this._coord.dispatch(null, 'listCollections', []); }
  async dropCollection(name) { return this._coord.dispatch(null, 'dropCollection', [name]); }
  async compact(options = {}) { return this._coord.dispatch(null, 'compact', [options]); }
  /** Same as Db.storageEstimate(). Quota is origin-wide, so it's answered
   * locally rather than proxied to the leader. */
  async storageEstimate() {
    if (typeof navigator === 'undefined' || !navigator.storage || typeof navigator.storage.estimate !== 'function') return null;
    return navigator.storage.estimate();
  }
  async close() { return this._coord.close(); }
}

/**
 * Join (or create) a database shared across every tab/worker that calls
 * connectShared with the same `dbName` against the same OPFS directory.
 * Exactly one caller becomes the leader and actually opens `provider`'s
 * files; every other caller transparently proxies to it. `options` is
 * forwarded to connect() (./db.js) verbatim -- which means an
 * `autoCompact` option reaches every newly elected leader's connect(),
 * so a leadership handover re-runs the compaction sweep exactly when a
 * tab has just closed and nobody is mid-interaction.
 */
async function connectShared(dbName, provider, options) {
  if (typeof dbName !== 'string' || dbName.length === 0) {
    throw new Error(`Invalid dbName: ${JSON.stringify(dbName)}`);
  }
  if (typeof navigator === 'undefined' || !navigator.locks || typeof BroadcastChannel === 'undefined') {
    throw new Error('connectShared requires navigator.locks and BroadcastChannel (run it inside a Worker)');
  }
  const coordinator = new Coordinator(dbName);
  await coordinator.start(provider, options);
  return new SharedDb(coordinator);
}

export { connectShared };
