/**
 * db-http-client.js — the JavaScript client for the HTTP FRONT END
 * (db-http-front.js): the same MongoDB-driver-shaped surface
 * db-server-client.js offers over TCP, spelled against fetch. This is
 * the one client in the family that runs in a BROWSER; it runs in Node
 * (≥18, global fetch) identically, because nothing in it is
 * platform-shaped.
 *
 * NO ENGINE, AND NO SOCKET EITHER. The imports are the Extended JSON
 * convention and binjson's ObjectId — both pure JS, neither touching a
 * node: module — and the transport is fetch. The front end holds the
 * real sockets; this side holds URLs.
 *
 * A MIRROR, NOT A SHARED IMPLEMENTATION. db-server-client.js is the
 * reference for every method here: same names, same arguments, same
 * results, same errors-as-sentences for ops the wire does not have —
 * so code written against one connects with the other by swapping
 * connectServer for connectHttp. They cannot share a body, because the
 * TCP client imports node:net and this file's whole claim is that it
 * imports nothing a browser lacks; what pins them together instead is
 * the test's comparison, the same way the front end itself is pinned
 * to the wire (test/db.http-front.test.js's shape, again).
 *
 * WHAT TRAVELS IS EXTENDED JSON. Bodies out and answers back cross as
 * {$oid} / {$date} / {$binary} literals (extended-json.js, the one
 * owner), converted at this boundary so the caller sees real ObjectId,
 * Date and Uint8Array values — the same values the TCP client hands
 * over, which is what makes the mirror honest.
 *
 * IDS ARE MINTED HERE, as in every client in this repository: an
 * insert without an _id and an upsert without an id get an ObjectId
 * minted on this side, and `now` rides along where the wire wants a
 * clock reading. The front end would mint them too (it is the client
 * for a caller that is just curl) — minting here instead means
 * insertedId is known without reading the echo, and the two clients
 * agree about who owns identity.
 *
 * A BATCHED CURSOR RIDES A SESSION. The front end's sessions exist
 * because a cursor is state on one server connection and HTTP has none
 * (docs/http-front.md); this client keeps that plumbing out of the
 * caller's sight: find({...}, {batchSize}) opens a session for the
 * cursor, pages getMore through it, and deletes it when the cursor
 * drains or closes. One cursor, one session, one server socket —
 * exactly what the cursor costs the wire anyway.
 *
 * A CHANGE STREAM IS THE FRONT END'S SSE, read with fetch rather than
 * EventSource — deliberately: EventSource cannot say "the watch was
 * refused" (it swallows the status and retries), and the refusal is
 * the part a caller must see. The stream object is the TCP client's:
 * .on('change'), async iteration, close(), resumeFrom, ready. Resuming
 * is watch({ from }) here as there; the front end already bridges
 * server-side page boundaries itself, so an `overflow` event reaching
 * this side is the no-log kind, and ends the stream the way the TCP
 * client's ChangeStreamOverflowError does, resumeFrom null and all.
 *
 * A REFUSAL IS A ServerError. The front end answers the wire's refusal
 * object verbatim under an HTTP status; this side rebuilds the error
 * the TCP client would have thrown — code, index, leaderId, leader —
 * with `status` alongside. Its class is this module's own (importing
 * the TCP client's would drag node:net into a page); catch by `code`,
 * which is the part the server meant.
 */
import { ObjectId } from '../third_party/binjson/js/binjson.js';
import { fromExtendedJson, toExtendedJson } from './extended-json.js';

export { ObjectId };

/**
 * The ops the wire has — the same list db-server-client.js declares,
 * for the same reason: to say what a refusal is refusing. The owner is
 * OP_NAMES in engine/src/db_request.c.
 */
export const WIRE_OPS = [
  'ping', 'listDatabases', 'dropDatabase',
  'find', 'findOne', 'count', 'distinct', 'aggregate', 'explain',
  'insert', 'insertMany', 'update', 'updateMany', 'replace', 'delete', 'deleteMany',
  'findOneAndUpdate', 'findOneAndReplace', 'findOneAndDelete',
  'bulkWrite',
  'findByIndex', 'pruneExpired', 'watch', 'closeStream',
  'getMore', 'closeCursor', 'compact',
  'snapshot', 'latestSnapshot', 'readSnapshotFile',
  'createCollection', 'dropCollection', 'createIndex', 'dropIndex', 'listIndexes',
  'listCollections'
];

/** A refusal from the server, carried through the front end: `code` is
 * the DC_ERR_* it answered with, `status` the HTTP class it rode in
 * under. The same shape the TCP client throws, minus its instanceof. */
export class ServerError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ServerError';
    this.code = code;
  }
}

/** The change stream ended because the server stopped holding events
 * for it. Reaching this side means the server had no log to resume
 * from (the front end bridges the resumable kind itself), so the
 * remedy is the old one: watch again and re-read current state. */
export class ChangeStreamOverflowError extends Error {
  constructor(resumeFrom = null) {
    super('change stream overflow: the server stopped holding events for this ' +
      'stream and closed it -- consume faster, or watch() again and re-read ' +
      'current state');
    this.name = 'ChangeStreamOverflowError';
    this.resumeFrom = resumeFrom;
  }
}

/** `http://host:port`, `https://…`, `host:port` or a bare port —
 * normalized to an origin with no trailing slash. Scheme-less means
 * http, the same default the front end listens with. */
export function parseBaseUrl(base) {
  let s = String(base).trim().replace(/\/+$/, '');
  if (!/^https?:\/\//.test(s)) {
    if (/^\d+$/.test(s)) s = `127.0.0.1:${s}`;
    s = `http://${s}`;
  }
  let url;
  try { url = new URL(s); } catch {
    throw new Error(`not a front-end address: '${base}' (want a URL, host:port, or a port)`);
  }
  if (url.pathname !== '/' || url.search || url.hash) {
    throw new Error(`not a front-end address: '${base}' (the front end serves at the origin, not under a path)`);
  }
  return url.origin;
}

/* The response side of one fetch: JSON either way, a refusal rebuilt
 * as the error the TCP client would have thrown. */
async function answer(res) {
  let body;
  try { body = await res.json(); } catch {
    throw new Error(`the front end sent a non-JSON answer (HTTP ${res.status})`);
  }
  if (res.ok) return fromExtendedJson(body);
  if (body && body.ok === false && typeof body.code === 'number') {
    const err = new ServerError(body.code, body.msg || `error ${body.code}`);
    if (typeof body.index === 'number') err.index = body.index;
    if (typeof body.leaderId === 'number') err.leaderId = body.leaderId;
    if (body.leader) err.leader = fromExtendedJson(body.leader);
    err.status = res.status;
    return Promise.reject(err);
  }
  const err = new Error(body?.error || `HTTP ${res.status}`);
  err.status = res.status;
  throw err;
}

/**
 * Wrap an object so that a method it does not have is a sentence
 * rather than a TypeError — db-server-client.js's guard, word for
 * word, because the sentences are part of the mirrored surface.
 */
function guard(impl, what) {
  return new Proxy(impl, {
    get(target, prop) {
      if (prop in target) return target[prop];
      if (typeof prop !== 'string') return undefined;
      if (prop === 'then' || prop === 'catch' || prop === 'finally') return undefined;
      return () => {
        if (WIRE_OPS.includes(prop)) {
          throw new Error(
            `the server has no ${what}.${prop}() -- the wire's ${prop} is a ` +
            `collection operation; ask a collection for it.`);
        }
        throw new Error(
          `the server has no ${what}.${prop}() -- its wire carries ` +
          `${WIRE_OPS.join(', ')}. Open the database directly for the rest.`);
      };
    }
  });
}

/** Name the stage a refusal was about, and quote it — the TCP
 * client's atStage, for the same division of labor. */
function atStage(err, pipeline) {
  if (typeof err?.index !== 'number' || err.index < 0 || err.index >= pipeline.length) return err;
  err.message = `${err.message} (stage ${err.index}: ${JSON.stringify(pipeline[err.index])})`;
  return err;
}

/** A cursor over a result that arrived whole — one answer, no session. */
function materialized(docs, extras = {}) {
  let at = 0;
  return guard({
    ...extras,
    toArray: () => docs,
    async next() {
      const all = await docs;
      return at < all.length ? { value: all[at++], done: false }
                             : { value: undefined, done: true };
    },
    close: async () => {},
    async *[Symbol.asyncIterator]() {
      const all = await docs;
      while (at < all.length) yield all[at++];
    }
  }, 'cursor');
}

/** find's options, as the wire reads them: absent is none. */
function findOpts(options) {
  const out = {};
  if (options?.sort) out.sort = options.sort;
  if (options?.projection) out.projection = options.projection;
  if (options?.skip) out.skip = options.skip;
  if (options?.limit) out.limit = options.limit;
  if (options?.batchSize > 0) out.batchSize = options.batchSize;
  return Object.keys(out).length ? out : null;
}

/**
 * The consumer end of a change stream — the TCP client's
 * RemoteChangeStream, fed by SSE frames instead of binjson ones.
 */
class HttpChangeStream {
  constructor(close) {
    this._listeners = new Set();
    this._queue = [];
    this._waiting = [];
    this._closed = false;
    this._error = null;
    this._close = close;
    this.resumeFrom = null;
  }

  on(event, cb) {
    if (event !== 'change') throw new Error(`unknown change-stream event: ${event}`);
    this._listeners.add(cb);
    return this;
  }

  off(event, cb) { this._listeners.delete(cb); return this; }

  /** @internal an SSE change frame arrived */
  _emit(change) {
    if (this._closed) return;
    if (typeof change?.index === 'number') this.resumeFrom = change.index;
    for (const cb of this._listeners) cb(change);
    if (this._waiting.length) { this._waiting.shift().resolve({ value: change, done: false }); return; }
    this._queue.push(change);
  }

  /** @internal the stream ended with a reason that is an error */
  _fail(err) {
    if (this._closed) return;
    this._error = err;
    this._closed = true;
    const waiting = this._waiting;
    this._waiting = [];
    for (const w of waiting) w.reject(err);
  }

  /** @internal the stream ended and that is the whole story */
  _end() {
    if (this._closed) return;
    this._closed = true;
    const waiting = this._waiting;
    this._waiting = [];
    for (const w of waiting) w.resolve({ value: undefined, done: true });
  }

  async next() {
    if (this._queue.length) return { value: this._queue.shift(), done: false };
    if (this._error) throw this._error;
    if (this._closed) return { value: undefined, done: true };
    return new Promise((resolve, reject) => this._waiting.push({ resolve, reject }));
  }

  async close() {
    if (this._closed) { await this._close(); return; }
    this._end();
    await this._close();
  }

  [Symbol.asyncIterator]() { return this; }
  async return() { await this.close(); return { value: undefined, done: true }; }
}

/**
 * Read one SSE response body, calling `onFrame` per parsed frame:
 * { id?, event, data } with `data` still a string. The front end
 * writes LF-separated frames and comment keep-alives; this reads
 * exactly those, not the whole of the SSE grammar.
 */
async function readSse(body, onFrame) {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buf = '';
  for (;;) {
    const { value, done } = await reader.read();
    if (done) return;
    buf += decoder.decode(value, { stream: true });
    for (;;) {
      const at = buf.indexOf('\n\n');
      if (at < 0) break;
      const frame = buf.slice(0, at);
      buf = buf.slice(at + 2);
      if (frame.startsWith(':')) continue;   // keep-alive comment
      const ev = {};
      for (const line of frame.split('\n')) {
        const i = line.indexOf(': ');
        if (i < 0) continue;
        ev[line.slice(0, i)] = line.slice(i + 2);
      }
      onFrame(ev);
    }
  }
}

function collection(sc, name) {
  const call = (req) => sc.call({ coll: name, ...req });

  const impl = {
    collectionName: name,

    /*
     * Without a batchSize: one POST, one answer, a cursor over an array
     * this side already has. With one: a session-backed cursor — the
     * session is opened when the first batch is asked for and deleted
     * when the scan drains or close() gives the slot back early, so a
     * cursor built and abandoned unread costs the front end nothing.
     */
    find(filter = {}, options = undefined) {
      const opts = findOpts(options);
      const batchSize = options?.batchSize > 0 ? options.batchSize : 0;

      if (!batchSize) {
        return materialized(call({ op: 'find', filter, ...(opts ? { opts } : {}) })
          .then((res) => res.docs || []),
          { explain: () => impl.explain(filter) });
      }

      let session = null;     // the front end's session id while one is open
      let id = null;          // the server's cursor id while one is open
      let started = false;
      let done = false;

      const take = async () => {
        if (done) return [];
        if (!started) session = (await sc.openSession()).session;
        const res = started
          ? await call({ op: 'getMore', cursor: id, session })
          : await call({ op: 'find', filter, ...(opts ? { opts } : {}), session });
        started = true;
        id = res.cursor ?? null;
        if (id === null) {
          done = true;
          const dying = session;
          session = null;
          await sc.closeSession(dying);
        }
        return res.docs || [];
      };

      let buffered = [], at = 0;
      const cursor = {
        async toArray() {
          const all = [];
          while (!done) all.push(...(await take()));
          return all;
        },
        async nextBatch() { return take(); },
        async next() {
          while (at >= buffered.length) {
            if (done) return { value: undefined, done: true };
            buffered = await take();
            at = 0;
          }
          return { value: buffered[at++], done: false };
        },
        /** Give the cursor and its session back early. Draining does
         * it for you. */
        async close() {
          if (id === null && session === null) return;
          const dyingCursor = id, dyingSession = session;
          id = null;
          session = null;
          done = true;
          if (dyingCursor !== null && dyingSession !== null) {
            await call({ op: 'closeCursor', cursor: dyingCursor, session: dyingSession }).catch(() => {});
          }
          if (dyingSession !== null) await sc.closeSession(dyingSession);
        },
        async *[Symbol.asyncIterator]() {
          try {
            while (!done) yield* await take();
          } finally {
            await cursor.close();   // a `break` mid-scan still gives both slots back
          }
        },
        explain: () => impl.explain(filter)
      };
      return guard(cursor, 'cursor');
    },

    async findOne(filter = {}) {
      const res = await call({ op: 'findOne', filter });
      return res.found ? res.doc : null;
    },

    aggregate(pipeline = []) {
      if (!Array.isArray(pipeline)) throw new Error('aggregate requires a pipeline array');
      return materialized(
        call({ op: 'aggregate', stages: pipeline })
          .then((res) => res.docs || [])
          .catch((err) => { throw atStage(err, pipeline); })
      );
    },

    async explain(filter = {}) {
      return (await call({ op: 'explain', filter })).plan;
    },

    async countDocuments(filter = {}) {
      return (await call({ op: 'count', filter })).n;
    },

    async distinct(field, filter = {}) {
      return (await call({ op: 'distinct', field, filter })).values || [];
    },

    async insertOne(doc) {
      const _id = doc?._id ?? new ObjectId();
      const res = await call({ op: 'insert', doc: { _id, ...doc }, id: _id });
      return { ...res.result, insertedId: _id };
    },

    async insertMany(docs, { ordered = true } = {}) {
      if (!Array.isArray(docs) || docs.length === 0) {
        throw new Error('insertMany requires a non-empty array of documents');
      }
      const ids = docs.map((doc) => doc?._id ?? new ObjectId());
      const res = await call({
        op: 'insertMany',
        docs: docs.map((doc, i) => ({ _id: ids[i], ...doc })),
        ordered
      });
      const failed = new Map((res.errors || []).map((w) => [w.index, w]));
      const insertedIds = {};
      let insertedCount = 0;
      for (let i = 0; i < res.attempted; i++) {
        const bad = failed.get(i);
        if (!bad) {
          insertedIds[i] = ids[i];
          insertedCount++;
          continue;
        }
        const err = new ServerError(bad.code, `${bad.msg} (insertMany, document ${i})`);
        err.result = { acknowledged: true, insertedCount, insertedIds };
        throw err;
      }
      return { acknowledged: true, insertedCount, insertedIds };
    },

    async bulkWrite(operations, { ordered = true } = {}) {
      if (!Array.isArray(operations) || operations.length === 0) {
        throw new Error('bulkWrite requires a non-empty array of operations');
      }
      const ids = [];
      const writes = operations.map((op) => {
        const [opName, spec] = Object.entries(op ?? {})[0] ?? [];
        if (!opName || spec === null || typeof spec !== 'object') {
          ids.push(null);
          return op;              // malformed: the server says so, not us
        }
        if (opName === 'insertOne') {
          const _id = spec.document?._id ?? new ObjectId();
          ids.push(_id);
          return { insertOne: { ...spec, document: { _id, ...spec.document } } };
        }
        if (spec.upsert) {
          const id = new ObjectId();
          ids.push(id);
          return { [opName]: { ...spec, id } };
        }
        ids.push(null);
        return op;
      });

      const res = await call({ op: 'bulkWrite', writes, ordered, now: Date.now() })
        .catch((err) => {
          if (typeof err?.index === 'number') err.message += ` (operation ${err.index})`;
          throw err;
        });
      const result = {
        acknowledged: true,
        insertedCount: res.result.insertedCount,
        matchedCount: res.result.matchedCount,
        modifiedCount: res.result.modifiedCount,
        deletedCount: res.result.deletedCount,
        upsertedCount: res.result.upsertedCount,
        insertedIds: {},
        upsertedIds: {}
      };
      const errors = res.errors || [];
      const failed = new Set(errors.map((w) => w.index));
      for (let i = 0; i < res.attempted; i++) {
        if (failed.has(i) || ids[i] === null) continue;
        if (Object.keys(operations[i])[0] === 'insertOne') result.insertedIds[i] = ids[i];
      }
      for (const u of res.upserted || []) result.upsertedIds[u.index] = u.id;

      if (errors.length) {
        const err = new Error(`bulkWrite: ${errors.length} operation(s) failed ` +
          `(first at index ${errors[0].index}: ${errors[0].msg})`);
        err.result = result;
        err.writeErrors = errors.map((w) => ({ index: w.index, error: new ServerError(w.code, w.msg) }));
        throw err;
      }
      return result;
    },

    async findOneAndUpdate(filter, update, options = undefined) {
      return findAndModify('findOneAndUpdate', { filter, update }, options);
    },

    async findOneAndReplace(filter, replacement, options = undefined) {
      return findAndModify('findOneAndReplace', { filter, doc: replacement }, options);
    },

    async findOneAndDelete(filter = {}) {
      return findAndModify('findOneAndDelete', { filter }, undefined);
    },

    async deleteOne(filter = {}) {
      return (await call({ op: 'delete', filter })).result;
    },

    async deleteMany(filter = {}) {
      return (await call({ op: 'deleteMany', filter })).result;
    },

    async replaceOne(filter, doc, options = undefined) {
      return write('replace', { filter, doc }, options);
    },

    async updateOne(filter, update, options = undefined) {
      return write('update', { filter, update }, options);
    },

    async updateMany(filter, update, options = undefined) {
      return write('updateMany', { filter, update }, options);
    },

    async compact() {
      return (await call({ op: 'compact' })).result;
    },

    async createIndex(keys, options = undefined) {
      const req = { op: 'createIndex', keys };
      if (options && Object.keys(options).length) req.options = options;
      return (await call(req)).name;
    },

    async dropIndex(name) {
      await call({ op: 'dropIndex', index: name });
    },

    async findByIndex(name, values) {
      return (await call({ op: 'findByIndex', index: name, values })).docs || [];
    },

    /*
     * A live feed of this collection's changes: the front end's SSE
     * endpoint, read with fetch. `ready` resolves when the front end
     * has committed the stream (its `watching` frame) and rejects with
     * the refusal otherwise — the same contract the TCP client's
     * subscribe answer carries. `watch({ from })` resumes on a
     * replicated server; `stream.resumeFrom` keeps the place.
     */
    watch({ from } = {}) {
      return sc.watch(name, from ?? null);
    },

    async pruneExpired() {
      return (await call({ op: 'pruneExpired', now: Date.now() })).deletedCount;
    },

    async listIndexes() {
      return (await call({ op: 'listIndexes' })).indexes || [];
    }
  };

  /* Upserts need an id and updates carry `now`, for the wire's reasons
   * (db-server-client.js spells them out); both are this side's to
   * provide, being the client. */
  async function findAndModify(op, fields, options) {
    const upsert = !!options?.upsert;
    const res = await call({
      op, ...fields, now: Date.now(),
      ...(options?.returnDocument === 'after' ? { returnNew: true } : {}),
      ...(upsert ? { upsert, id: new ObjectId() } : {})
    });
    return res.found ? res.doc : null;
  }

  async function write(op, fields, options) {
    const upsert = !!options?.upsert;
    const res = await call({
      op, ...fields, now: Date.now(),
      ...(upsert ? { upsert, id: new ObjectId() } : {})
    });
    return res.result;
  }

  return guard(impl, 'collection');
}

/* One database's scope over the client's transport: adds `db` to the
 * URL the way the TCP client's scope() adds it to the frame. */
function database(client, name) {
  const sc = {
    call: (req) => client._call({ db: name, ...req }),
    openSession: () => client._openSession(),
    closeSession: (id) => client._closeSession(id),
    watch: (coll, from) => client._watch(name, coll, from)
  };
  const impl = {
    name,
    get isOpen() { return client.isOpen !== false; },
    collection: (coll) => collection(sc, coll),
    async createCollection(coll) {
      return (await sc.call({ op: 'createCollection', coll })).created;
    },
    async dropCollection(coll) {
      return (await sc.call({ op: 'dropCollection', coll })).dropped;
    },
    async compact(options = undefined) {
      const req = { op: 'compact' };
      if (options?.minBytes) req.minBytes = options.minBytes;
      if (options?.factor) req.factor = options.factor;
      if (options?.skipBusy) req.skipBusy = true;
      return (await sc.call(req)).result || {};
    },
    async listCollections() {
      return (await sc.call({ op: 'listCollections' })).collections || [];
    },
    /* The escape hatch, as everywhere: send an op the wire has and
     * read the response object as it came. `op` and `coll` become the
     * URL; the rest is the body. */
    request: (req) => sc.call(req),
    close: () => client.close()
  };
  return guard(impl, 'db');
}

/**
 * A client for the HTTP front end, and through it for the server or
 * cluster behind it.
 *
 *     const client = await connectHttp('http://127.0.0.1:8080');
 *     const users = client.db('app').collection('users');
 *     await users.insertOne({ name: 'Ada' });
 *
 * The connect is verified with one ping — a wrong URL is an error
 * here, at the name that says "connect", not on the first query. What
 * close() closes is this side's held state (open change streams,
 * cursor sessions); HTTP itself holds nothing.
 *
 * @param {string} base `http://host:port`, `host:port`, or a port
 * @returns {Promise<object>} db(name), listDatabases(), dropDatabase(),
 *   ping(), request(), close()
 */
export async function connectHttp(base) {
  const origin = parseBaseUrl(base);

  const streams = new Set();    // open HttpChangeStreams' abort controllers
  const sessions = new Set();   // open cursor sessions' ids
  const dbs = new Map();        // name -> the handle, cached like the TCP client's

  const impl = {
    isOpen: true,
    address: origin,

    db(name) {
      const cached = dbs.get(name);
      if (cached) return cached;
      const d = database(impl, name);
      dbs.set(name, d);
      return d;
    },

    async listDatabases() {
      return (await impl._call({ op: 'listDatabases' })).databases || [];
    },

    async dropDatabase(name) {
      dbs.delete(name);
      return (await impl._call({ op: 'dropDatabase', db: name })).dropped;
    },

    async ping() {
      const { ok, ...status } = await impl._call({ op: 'ping' });
      return status;
    },

    /* The escape hatch: {op, db?, coll?, ...rest} — the named parts
     * become the URL, the rest the body, which is the front end's own
     * split. */
    request: (req) => impl._call(req),

    async close() {
      impl.isOpen = false;
      for (const abort of [...streams]) abort.abort();
      streams.clear();
      const held = [...sessions];
      sessions.clear();
      for (const id of held) {
        await fetch(`${origin}/session/${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {});
      }
    },

    /* ---- transport ---------------------------------------------------- */

    /** One request: op/db/coll into the URL, the rest into the body. */
    async _call(req) {
      if (!impl.isOpen) throw new Error('this client is closed');
      const { op, db, coll, session, ...rest } = req;
      let path;
      if (coll !== undefined) path = `/db/${encodeURIComponent(db)}/${encodeURIComponent(coll)}/${encodeURIComponent(op)}`;
      else if (db !== undefined) path = `/db/${encodeURIComponent(db)}/${encodeURIComponent(op)}`;
      else path = `/${encodeURIComponent(op)}`;
      const q = session ? `?session=${encodeURIComponent(session)}` : '';
      let res;
      try {
        res = await fetch(`${origin}${path}${q}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(toExtendedJson(rest))
        });
      } catch (err) {
        throw new Error(`cannot reach the nisaba HTTP front end at ${origin}: ${err.message}`);
      }
      return answer(res);
    },

    async _openSession() {
      if (!impl.isOpen) throw new Error('this client is closed');
      const res = await fetch(`${origin}/session`, { method: 'POST' });
      const opened = await answer(res);
      sessions.add(opened.session);
      return opened;
    },

    async _closeSession(id) {
      sessions.delete(id);
      await fetch(`${origin}/session/${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {});
    },

    _watch(db, coll, from) {
      const abort = new AbortController();
      streams.add(abort);
      const stream = new HttpChangeStream(async () => {
        streams.delete(abort);
        abort.abort();
      });
      if (from !== null) stream.resumeFrom = from;

      const url = `${origin}/db/${encodeURIComponent(db)}/${encodeURIComponent(coll)}/watch` +
                  (from !== null ? `?from=${from}` : '');

      let readyResolve, readyReject;
      /* The subscribe's own outcome, as the TCP client's `ready`: a
       * holder that must know the stream EXISTS before promising it on
       * awaits this; iterating alone surfaces the refusal from next()
       * too. The rejection is pre-handled so ignoring `ready` leaks no
       * unhandled rejection. */
      stream.ready = new Promise((resolve, reject) => { readyResolve = resolve; readyReject = reject; });
      stream.ready.catch(() => {});

      (async () => {
        let res;
        try {
          res = await fetch(url, { signal: abort.signal });
        } catch (err) {
          const failure = abort.signal.aborted ? null
            : new Error(`cannot reach the nisaba HTTP front end at ${origin}: ${err.message}`);
          if (failure) { readyReject(failure); stream._fail(failure); }
          else readyReject(new Error('the stream was closed before it was ready'));
          return;
        }
        if (!res.ok) {
          const refusal = await answer(res).then(
            () => new Error(`the front end refused the watch (HTTP ${res.status})`),
            (err) => err);
          readyReject(refusal);
          stream._fail(refusal);
          return;
        }
        try {
          await readSse(res.body, (ev) => {
            switch (ev.event) {
              case 'watching': {
                const hello = JSON.parse(ev.data);
                if (typeof hello.index === 'number' && stream.resumeFrom === null) {
                  stream.resumeFrom = hello.index;
                }
                stream.member = hello.member;
                readyResolve(stream);
                break;
              }
              case 'change':
                stream._emit(fromExtendedJson(JSON.parse(ev.data)));
                break;
              case 'overflow':
                stream._fail(new ChangeStreamOverflowError(null));
                break;
              case 'end': {
                const { reason } = JSON.parse(ev.data);
                if (reason === 'closed') stream._end();
                else stream._fail(new Error(`the change stream ended: ${reason}`));
                break;
              }
            }
          });
          /* The response ended. After an end/overflow frame that is
           * the contract; without one it is a transport loss, and
           * nothing ends in silence — not even here. */
          stream._fail(new Error('the change stream\'s connection closed without an end event'));
        } catch (err) {
          if (abort.signal.aborted) stream._end();
          else stream._fail(err);
        } finally {
          streams.delete(abort);
        }
      })();

      return stream;
    }
  };

  const client = guard(impl, 'client');
  await client.ping();   // a wrong URL fails here, not on the first query
  return client;
}
