/**
 * db-server-client.js — the JavaScript client for the database SERVER
 * (server/main.c), over a TCP socket.
 *
 * The wire it speaks is documented in docs/db-server.md. The server is a
 * wasm32-wasip2 (or native) process with no JavaScript in it; this is the
 * other end of the same wire, and between them there is nothing but
 * binjson frames.
 *
 * NO ENGINE IN THIS PROCESS. The only import is the pure-JS binjson codec
 * -- no WASM module, no ready(), no storage provider. That is the whole
 * claim being made: the database is somewhere else, and a client needs to
 * agree with it about a format, not share an implementation with it. It is
 * also why this file is short.
 *
 * FRAMING IS THE FORMAT'S. A binjson value carries its own total size in
 * its header, so a reader takes five bytes, learns the length, and waits
 * for the rest. Same rule server/main.c reads by, from the other side; no
 * length prefix of our own and nothing to keep in sync with an encoder.
 * (raft-transport-tcp.js does prefix its frames -- it carries opaque byte
 * payloads that are not themselves binjson values, so it has nothing to
 * measure.)
 *
 * IDS ARE MINTED HERE. C will not invent an _id, because inventing one
 * needs a clock and db.h keeps clocks out of the engine deliberately
 * (db_session.h says so at the seam). So every write that might need one
 * carries `id`, and this side is the one that generated it.
 *
 * A CONNECTION COSTS A SLOT, AND THE SLOTS ARE COUNTED. The server polls
 * every client it has accepted, so holding a connection open no longer
 * makes anyone else wait -- but it does occupy one of --max-clients
 * places, and a client arriving when they are all taken is told so
 * ({ok:false, code:-44}) and disconnected rather than left waiting. That
 * refusal can arrive before this side has asked anything, which is why a
 * response to no request is kept rather than discarded: it is the answer
 * to the next call.
 *
 * A SLOT ALSO HAS TO BE EARNED. The server closes a connection that has
 * asked nothing for --idle-timeout seconds (60 by default), because a
 * crashed client and a dropped route look exactly like a quiet one. So
 * this client pings on a timer -- keepAliveMs, a third of the server's
 * default, off with 0 -- and the timer is unref'd, so it never holds a
 * process open on its own. A CLI invocation that connects, asks and
 * exits never sends one.
 *
 * A CURSOR IS THE SERVER'S, HELD FOR YOU. find({...}, {batchSize}) pages
 * a scan instead of returning one frame: the server keeps the position,
 * this side keeps an id, and the id comes back null when the scan ends.
 * The table of them is bounded, so an abandoned cursor is a slot nobody
 * can use -- close() gives it back, draining gives it back, and losing
 * the connection gives it back.
 *
 * A LIST OF WRITES IS ONE ROUND TRIP, AND THE LOOP IS THE SERVER'S.
 * insertMany and bulkWrite are looped in JavaScript when the engine is in
 * the same process (db_bulk.h says why); over a socket that same loop is
 * N round trips, and this side has no dc_bulk_parse to check a list of
 * operations with even if it wanted to. So the list goes over whole and
 * the answer says how many members were attempted and which failed.
 *
 * A FRAME IS AN ANSWER OR AN EVENT. Every frame used to be an answer, in
 * request order, which is why there are no request ids on this wire and
 * none are needed. watch() adds the other kind: a frame the server sends
 * because somebody ELSE wrote something. They are told apart by shape --
 * an answer carries `ok`, an event carries `stream` -- and a client that
 * never watches never sees one, so the old sentence still holds for it
 * word for word.
 *
 * WHAT IS NOT HERE. The wire has thirty-one ops (WIRE_OPS below) and this
 * client has exactly those. Asking for anything else gets a sentence
 * saying so rather than a TypeError about undefined not being a
 * function. Adding a method here without adding the op to db_request.c
 * would be inventing a second opinion about what the server does.
 */
import net from 'node:net';
import { encode, decode, ObjectId, Pointer } from '../third_party/binjson/js/binjson.js';

export { encode, decode, ObjectId, Pointer };

/**
 * The ops the wire has, named here once. The other copy is OP_NAMES in
 * wasm/src/db_request.c, which is the owner -- this list exists to say
 * what a refusal is refusing, not to decide anything.
 */
export const WIRE_OPS = [
  'ping', 'listDatabases', 'dropDatabase',
  'find', 'findOne', 'count', 'distinct', 'aggregate', 'explain',
  'insert', 'insertMany', 'update', 'updateMany', 'replace', 'delete', 'deleteMany',
  'findOneAndUpdate', 'findOneAndReplace', 'findOneAndDelete',
  'bulkWrite',
  'findByIndex', 'pruneExpired', 'watch', 'closeStream',
  'getMore', 'closeCursor', 'compact',
  'createCollection', 'dropCollection', 'createIndex', 'dropIndex', 'listIndexes',
  'listCollections'
];

/** Pings per idle timeout. The server's default is 60s; a third of that
 * survives one lost ping and one slow round trip without arithmetic. */
const DEFAULT_KEEPALIVE_MS = 20000;

/**
 * A client-side sanity bound on a response frame. The server bounds
 * requests at 16 MB (FRAME_MAX in server/main.c); a response can be
 * legitimately larger, since a find returns documents, so this is looser
 * -- but not unbounded, because a client that allocates whatever a length
 * field claims has a failure mode nobody tests either.
 */
const MAX_FRAME = 64 * 1024 * 1024;

/** A refusal the server sent: {ok:false, code, msg} with dc_strerror text. */
export class ServerError extends Error {
  constructor(code, message) {
    super(message);
    this.name = 'ServerError';
    this.code = code;
  }
}

/**
 * A change stream fell too far behind and the server closed it. There
 * are no resume tokens (a documented non-goal), so the remedy is the
 * in-process one: watch again and re-read current state.
 */
export class ChangeStreamOverflowError extends Error {
  constructor() {
    super('change stream overflow: the server stopped holding events for this ' +
          'stream and closed it -- consume faster, or watch() again and re-read ' +
          'current state');
    this.name = 'ChangeStreamOverflowError';
  }
}

/**
 * The consumer end of a change stream: an EventEmitter-lite
 * (.on('change', cb)) and an async iterator -- the dual API the
 * in-process ChangeStream has, and the real driver's.
 *
 * Nothing is bounded here, deliberately. The bound that matters is the
 * SERVER's: it is the side holding events for a consumer that stopped
 * reading, and it is the side that has to stop. By the time an event
 * reaches here it has already crossed the socket.
 */
class RemoteChangeStream {
  constructor(close) {
    this._listeners = new Set();
    this._queue = [];
    this._waiting = [];
    this._closed = false;
    this._error = null;
    this._close = close;
  }

  on(event, cb) {
    if (event !== 'change') throw new Error(`unknown change-stream event: ${event}`);
    this._listeners.add(cb);
    return this;
  }

  off(event, cb) { this._listeners.delete(cb); return this; }

  /** @internal a frame arrived for this stream */
  _emit(change) {
    if (this._closed) return;
    for (const cb of this._listeners) cb(change);
    if (this._waiting.length) { this._waiting.shift().resolve({ value: change, done: false }); return; }
    this._queue.push(change);
  }

  /** @internal the server gave up on us, or the connection did */
  _fail(err) {
    if (this._closed) return;
    this._error = err;
    this._closed = true;
    const waiting = this._waiting;
    this._waiting = [];
    for (const w of waiting) w.reject(err);
  }

  async next() {
    if (this._queue.length) return { value: this._queue.shift(), done: false };
    if (this._error) throw this._error;
    if (this._closed) return { value: undefined, done: true };
    return new Promise((resolve, reject) => this._waiting.push({ resolve, reject }));
  }

  async close() {
    if (this._closed) return;
    this._closed = true;
    const waiting = this._waiting;
    this._waiting = [];
    for (const w of waiting) w.resolve({ value: undefined, done: true });
    await this._close();
  }

  [Symbol.asyncIterator]() { return this; }
  async return() { await this.close(); return { value: undefined, done: true }; }
}

/** `host:port`, `[::1]:port`, `:port` or a bare port. */
export function parseAddress(address) {
  if (address && typeof address === 'object') {
    return { host: address.host || '127.0.0.1', port: Number(address.port) };
  }
  const s = String(address).trim();
  let host = '127.0.0.1';
  let port = s;
  const v6 = s.match(/^\[(.+)\]:(\d+)$/);
  if (v6) {
    host = v6[1];
    port = v6[2];
  } else if (s.includes(':')) {
    const i = s.lastIndexOf(':');
    host = s.slice(0, i) || '127.0.0.1';
    port = s.slice(i + 1);
  }
  const n = Number(port);
  if (!Number.isInteger(n) || n < 1 || n > 65535) {
    throw new Error(`not a server address: '${address}' (want host:port, or a port)`);
  }
  return { host, port: n };
}

/**
 * One socket, one request at a time in flight order. The server answers a
 * connection's requests in the order they arrive, so pending resolvers are
 * a queue -- there is no request id on the wire and none is needed.
 */
class Connection {
  constructor(socket, address) {
    this._socket = socket;
    this._address = address;
    this._pending = [];
    this._buf = Buffer.alloc(0);
    this._dead = null;      // the Error every later call fails with
    this._closing = false;  // our own close(), so EOF is not a surprise
    this._streams = new Map();   // stream id -> RemoteChangeStream

    socket.on('data', (chunk) => this._onData(chunk));
    socket.on('error', (err) => this._die(err));
    socket.on('close', () => this._die(new Error(
      `the nisaba server at ${this._address} closed the connection`)));
  }

  _die(err) {
    if (this._dead) return;
    this._dead = this._closing ? new Error('this connection is closed') : err;
    const waiting = this._pending;
    this._pending = [];
    for (const p of waiting) p.reject(this._dead);
    /* A watcher is owed the news too: its events came down this socket,
     * and there will be no more of them. */
    const streams = [...this._streams.values()];
    this._streams.clear();
    for (const st of streams) st._fail(this._dead);
    /* And so is a holder of many connections (the HTTP front end keeps a
     * table of them): a session whose socket the server reaped is an
     * entry nobody will ever hit again, and the close is the only moment
     * anybody can learn that without sending traffic -- which would
     * reset the very idle timer that reaps it. */
    if (this.onDead) this.onDead(this._dead);
  }

  /** @internal register a stream so its frames can find it */
  _watching(id, stream) { this._streams.set(id, stream); }
  _unwatching(id) { this._streams.delete(id); }

  _onData(chunk) {
    this._buf = this._buf.length ? Buffer.concat([this._buf, chunk]) : chunk;
    for (;;) {
      if (this._buf.length < 5) return;
      const total = this._buf.readUInt32LE(1) + 5;
      if (total > MAX_FRAME) {
        this._socket.destroy();
        return this._die(new Error(`response frame too large: ${total} bytes`));
      }
      if (this._buf.length < total) return;
      const frame = new Uint8Array(this._buf.subarray(0, total)); // copy out of the pool
      this._buf = this._buf.subarray(total);
      /*
       * A frame is an ANSWER or an EVENT, told apart by shape: an answer
       * carries `ok`, an event carries `stream`. Events are not queued
       * against pending requests -- they answer nothing -- so this test
       * comes first, before the queue is touched at all.
       */
      let routed = null;
      try { routed = decode(frame); } catch { /* handled below */ }
      if (routed && typeof routed === 'object' && routed.ok === undefined &&
          typeof routed.stream === 'number') {
        const st = this._streams.get(routed.stream);
        if (st) {
          if (routed.overflow !== undefined) {
            this._streams.delete(routed.stream);
            st._fail(new ChangeStreamOverflowError());
          } else {
            st._emit(routed.event);
          }
        }
        continue;   // never an answer to anything
      }

      const p = this._pending.shift();
      if (!p) {
        /* A response nobody asked for. If it is a refusal, it is the
         * server saying why it is about to close this connection before
         * we ever got to ask -- its connection table is full. Anything
         * else means framing is gone, and so is the connection. */
        this._socket.destroy();
        if (routed && routed.ok === false) {
          return this._die(new ServerError(routed.code, routed.msg));
        }
        return this._die(new Error('the server sent a response to no request'));
      }
      if (routed === null) {
        const err = new Error('the server sent a frame that is not a value');
        this._socket.destroy();
        p.reject(err);
        return this._die(err);
      }
      p.resolve(routed);
    }
  }

  /** Send one request, resolve with the response object, refusals included. */
  request(req) {
    if (this._dead) return Promise.reject(this._dead);
    return new Promise((resolve, reject) => {
      this._pending.push({ resolve, reject });
      this._socket.write(Buffer.from(encode(req)));
    });
  }

  /** Send one request; a refusal becomes a ServerError. */
  async call(req) {
    const res = await this.request(req);
    if (!res || typeof res !== 'object') {
      throw new Error('the server sent something that is not a response object');
    }
    if (res.ok === false) {
      /* A refusal can name a POSITION -- which member of a list of
       * operations or of pipeline stages was wrong. It stays a number
       * here; what to call that position, and what was at it, is known
       * only by whoever built the list. */
      const err = new ServerError(res.code, res.msg || `error ${res.code}`);
      if (typeof res.index === 'number') err.index = res.index;
      /* A refusal can also name WHO to ask instead. A replicated server
       * refuses a write it cannot take with the leader's id and, when it
       * knows one, the member record carrying its address -- an id alone
       * would send a caller back to the member it just asked. Carried
       * through rather than interpreted: following a redirect is the
       * caller's decision, and this layer only declines to lose it. */
      if (typeof res.leaderId === 'number') err.leaderId = res.leaderId;
      if (res.leader && typeof res.leader === 'object') err.leader = res.leader;
      throw err;
    }
    return res;
  }

  close() {
    this._closing = true;
    return new Promise((resolve) => {
      if (this._socket.destroyed) { this._die(new Error('closed')); return resolve(); }
      this._socket.end(() => resolve());
      this._socket.once('close', () => resolve());
    });
  }
}

/**
 * Wrap an object so that a method it does not have is a sentence rather
 * than a TypeError. `then`/`catch`/`finally` and symbols stay undefined:
 * a proxy that answers `then` with a function is a thenable, and `await
 * db.collection(name)` would call it.
 */
function guard(impl, what) {
  return new Proxy(impl, {
    get(target, prop) {
      if (prop in target) return target[prop];
      if (typeof prop !== 'string') return undefined;
      if (prop === 'then' || prop === 'catch' || prop === 'finally') return undefined;
      return () => {
        /* An op that IS on the wire, asked of the wrong thing, gets said
         * so plainly -- `db.compact()` listing `compact` among the
         * available ops reads as a contradiction, and the useful fact is
         * that compaction is per-collection. */
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

/**
 * Name the stage a refusal was about, and quote it.
 *
 * The server reports WHICH member of a list was wrong (`index`) and
 * nothing about its contents -- C does not format messages around user
 * data. This side is holding the list, so it is the side that can say
 * what was in it. Same division as the in-process aggregate, whose
 * message this reproduces.
 */
function atStage(err, pipeline) {
  if (typeof err?.index !== 'number' || err.index < 0 || err.index >= pipeline.length) return err;
  err.message = `${err.message} (stage ${err.index}: ${JSON.stringify(pipeline[err.index])})`;
  return err;
}

/**
 * A cursor over a result that arrived whole, in one frame -- an unbatched
 * find, or an aggregate. Nothing is held on the server: the "cursor" is a
 * position in an array this side already has, which is exactly what the
 * in-process cursor over a materialized result is too.
 *
 * `docs` is the promise of that array, so a caller that never asks for a
 * document never waits for one.
 */
function materialized(docs, extras = {}) {
  let at = 0;
  return guard({
    ...extras,
    toArray: () => docs,
    /* {value, done}, the shape the in-process cursor uses -- so a caller
     * that pulls one document at a time reads the same either way,
     * whichever side of a socket it is on. */
    async next() {
      const all = await docs;
      return at < all.length ? { value: all[at++], done: false }
                             : { value: undefined, done: true };
    },
    close: async () => {},
    /* Shares the position with next(), the way the in-process cursor
     * does -- iterating after pulling one document continues rather than
     * handing that document out twice. */
    async *[Symbol.asyncIterator]() {
      const all = await docs;
      while (at < all.length) yield all[at++];
    }
  }, 'cursor');
}

/** find's options, as db_request.c's read_opts reads them: absent is none. */
function findOpts(options) {
  const out = {};
  if (options?.sort) out.sort = options.sort;
  if (options?.projection) out.projection = options.projection;
  if (options?.skip) out.skip = options.skip;
  if (options?.limit) out.limit = options.limit;
  /* The server refuses batchSize with sort rather than materializing the
   * result and calling it a cursor (DC_ERR_CURSOR_SORTED). Both are
   * passed through deliberately: the refusal belongs to the layer that
   * owns the rule, not to a second opinion here. */
  if (options?.batchSize > 0) out.batchSize = options.batchSize;
  return Object.keys(out).length ? out : null;
}

function collection(conn, name) {
  const call = (req) => conn.call({ coll: name, ...req });

  const impl = {
    collectionName: name,

    /*
     * Without a batchSize this is one request and one frame, which is
     * what it has always been. WITH one, the server opens a cursor over
     * the scan and hands back a batch and an id; every batch after that
     * is a getMore, and the id comes back null when the scan is over --
     * so a drained cursor costs no extra round trip to close.
     *
     * A cursor is state on a server with a bounded table of them, so a
     * caller that stops early should close() rather than leave one held
     * until the connection ends.
     */
    find(filter = {}, options = undefined) {
      const opts = findOpts(options);
      const batchSize = options?.batchSize > 0 ? options.batchSize : 0;

      if (!batchSize) {
        return materialized(call({ op: 'find', filter, ...(opts ? { opts } : {}) })
          .then((res) => res.docs || []),
          { explain: () => impl.explain(filter) });
      }

      let id = null;          // the server's cursor id while one is open
      let started = false;
      let done = false;

      const take = async () => {
        if (done) return [];
        const res = started
          ? await call({ op: 'getMore', cursor: id })
          : await call({ op: 'find', filter, ...(opts ? { opts } : {}) });
        started = true;
        id = res.cursor ?? null;
        if (id === null) done = true;
        return res.docs || [];
      };

      let buffered = [], at = 0;
      const cursor = {
        async toArray() {
          const all = [];
          while (!done) all.push(...(await take()));
          return all;
        },
        /** One batch at a time, so a caller can stop without having
         * asked the server for everything. */
        async nextBatch() { return take(); },
        /** One document at a time, buffered a batch at a time. */
        async next() {
          while (at >= buffered.length) {
            if (done) return { value: undefined, done: true };
            buffered = await take();
            at = 0;
          }
          return { value: buffered[at++], done: false };
        },
        /** Give the slot back early. Draining does it for you. */
        async close() {
          if (id === null) return;
          const dying = id;
          id = null;
          done = true;
          await call({ op: 'closeCursor', cursor: dying });
        },
        async *[Symbol.asyncIterator]() {
          try {
            while (!done) yield* await take();
          } finally {
            await cursor.close();   // a `break` mid-scan still gives the slot back
          }
        },
        /** The plan this cursor's filter gets -- the same sugar the
         * in-process FindCursor carries. */
        explain: () => impl.explain(filter)
      };
      return guard(cursor, 'cursor');
    },

    async findOne(filter = {}) {
      const res = await call({ op: 'findOne', filter });
      return res.found ? res.doc : null;
    },

    /*
     * The pipeline runs in C (db_agg.h), including the decision to push a
     * leading $match into the scan so an index can serve it. This side
     * marshals the stages in and the documents out, and knows no stage
     * names -- which is the point: the subset is one list, in one place.
     *
     * One frame, no server cursor: $sort and $group need every match
     * before the first result exists, so there is no scan left to resume.
     */
    aggregate(pipeline = []) {
      if (!Array.isArray(pipeline)) throw new Error('aggregate requires a pipeline array');
      return materialized(
        call({ op: 'aggregate', stages: pipeline })
          .then((res) => res.docs || [])
          .catch((err) => { throw atStage(err, pipeline); })
      );
    },

    /* Which source the query dispatch would use for this filter,
     * without running it. The plan's name is C's (dc_explain_source), so
     * this reports what an in-process host reports, word for word. */
    async explain(filter = {}) {
      return (await call({ op: 'explain', filter })).plan;
    },

    async countDocuments(filter = {}) {
      return (await call({ op: 'count', filter })).n;
    },

    async distinct(field, filter = {}) {
      return (await call({ op: 'distinct', field, filter })).values || [];
    },

    /* The id is this side's, and it is also the answer: an insert's
     * result counts what happened, and the only thing that knows which id
     * happened is whoever minted it. */
    async insertOne(doc) {
      const _id = doc?._id ?? new ObjectId();
      const res = await call({ op: 'insert', doc: { _id, ...doc }, id: _id });
      return { ...res.result, insertedId: _id };
    },

    /*
     * Every document in one frame, and the loop that inserts them is the
     * server's. `ordered` (default true) stops at the first failing
     * document; false attempts every one regardless.
     *
     * The contract is the in-process one: throw at the first failed
     * document, carrying what DID land as `err.result`. The server says
     * how many documents it attempted -- with ordered:true "never tried"
     * and "tried and succeeded" are different answers -- and which of
     * them failed. The ids are already known here, because this side
     * minted them.
     */
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

    /*
     * A list of writes of six kinds, in one frame. The GRAMMAR -- which
     * operation names exist and which fields each needs -- is C's
     * (db_bulk.h), checked over the whole list before any of it runs, so
     * nothing here validates it: a client that had its own opinion about
     * what a bulkWrite may contain would be a second one.
     *
     * What this side does own is the ids, as everywhere else: an
     * insertOne's document gets one, and so does any operation that might
     * upsert.
     */
    async bulkWrite(operations, { ordered = true } = {}) {
      if (!Array.isArray(operations) || operations.length === 0) {
        throw new Error('bulkWrite requires a non-empty array of operations');
      }
      const ids = [];
      const writes = operations.map((op) => {
        const [name, spec] = Object.entries(op ?? {})[0] ?? [];
        if (!name || spec === null || typeof spec !== 'object') {
          ids.push(null);
          return op;              // malformed: the server says so, not us
        }
        if (name === 'insertOne') {
          const _id = spec.document?._id ?? new ObjectId();
          ids.push(_id);
          return { insertOne: { ...spec, document: { _id, ...spec.document } } };
        }
        if (spec.upsert) {
          const id = new ObjectId();
          ids.push(id);
          return { [name]: { ...spec, id } };
        }
        ids.push(null);
        return op;
      });

      /* One clock reading for the whole list, so two members dating the
       * same field cannot disagree about when it was. */
      const res = await call({ op: 'bulkWrite', writes, ordered, now: Date.now() })
        .catch((err) => {
          /* A malformed list names its position; this side says what
           * that position is called, as the in-process bulkWrite does. */
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
      /* An upserted id is the SERVER's answer, not this side's guess: an
       * upsert whose filter named an _id uses that one. */
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

    /*
     * Read and write one document in one request, answering with the
     * document itself: `returnDocument` picks the image, 'before' by
     * default, exactly as the in-process API and the driver do. null
     * when nothing matched -- and also for an upsert asked for 'before',
     * because no prior state exists to return.
     *
     * No `sort`, for the same reason the in-process API has none: this
     * would then be the only place in the library where you could
     * choose WHICH matching document gets claimed.
     */
    async findOneAndUpdate(filter, update, options = undefined) {
      return findAndModify('findOneAndUpdate', { filter, update }, options);
    },

    async findOneAndReplace(filter, replacement, options = undefined) {
      return findAndModify('findOneAndReplace', { filter, doc: replacement }, options);
    },

    /** The deleted document, or null. `returnDocument` has no meaning
     * here: the document is gone, so `before` is the only image there
     * is. */
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

    /*
     * Rewrite this collection's files without their append-only history
     * (docs/compaction.md). Refused while any client -- including this
     * one -- has a cursor open over it, since the scan is positioned in
     * the files being replaced.
     */
    async compact() {
      return (await call({ op: 'compact' })).result;
    },

    /* ---- schema. The collection this names need not exist yet: that is
     * what createCollection is for, and an insert makes one anyway. */
    async createIndex(keys, options = undefined) {
      const req = { op: 'createIndex', keys };
      if (options && Object.keys(options).length) req.options = options;
      return (await call(req)).name;
    },

    async dropIndex(name) {
      await call({ op: 'dropIndex', index: name });
    },

    /* The lookup that names its index rather than describing what it
     * wants: no planner, an O(log n + k) range scan. Whether that index
     * exists and is the right kind is the collection's to say, not
     * this side's -- it is the thing holding the indexes. */
    async findByIndex(name, values) {
      return (await call({ op: 'findByIndex', index: name, values })).docs || [];
    },

    /*
     * A live feed of this collection's changes, over the same socket.
     *
     * The stream costs one of the server's bounded slots and one queue
     * on it, so close() it when done -- losing the connection does it
     * for you, and so does falling far enough behind that the server
     * gives up (ChangeStreamOverflowError, which has no resume token to
     * offer: watch again and re-read).
     *
     * The collection need not exist yet. Watching one before its first
     * insert is the case a change stream is most useful for, and the
     * insert that creates it is an event like any other.
     */
    watch() {
      let id = null;
      let closed = false;
      /* Synchronous, like the in-process watch(): a caller writes
       * `for await (const c of coll.watch())` and a promise there is not
       * iterable. The subscribe is a round trip, so the stream exists
       * first and learns its id a moment later -- which changes nothing
       * on the wire, since the server sends nothing until it has one. */
      const stream = new RemoteChangeStream(async () => {
        closed = true;
        await pending;
        if (id === null) return;
        conn._unwatching(id);
        /* Best effort: a connection already gone has released it. */
        try { await conn.call({ op: 'closeStream', stream: id }); } catch { /* closed either way */ }
      });
      const subscribed = call({ op: 'watch' }).then((res) => {
        id = res.stream;
        if (closed) {          // closed before the id arrived: give the slot back
          conn.call({ op: 'closeStream', stream: id }).catch(() => {});
          return;
        }
        conn._watching(id, stream);
      });
      const pending = subscribed.catch((err) => stream._fail(err));
      /* The subscribe's own outcome, for a holder that has to know the
       * stream EXISTS before promising it to somebody else -- the HTTP
       * front end answers 503 or 200 with it. Iterating never needed
       * this: a refused subscribe surfaces from next() too, and still
       * does. The rejection is handled (pending above), so a caller that
       * ignores `ready` leaks no unhandled rejection. */
      stream.ready = subscribed;
      return stream;
    },

    /* Delete every document past a TTL index's cutoff. Expiry is a
     * sweep somebody asks for, not a background thread: the engine runs
     * no timers, so WHEN to prune stays with whoever is driving -- and
     * the clock reading travels with the request, as it does for
     * $currentDate and for every _id this client mints. */
    async pruneExpired() {
      return (await call({ op: 'pruneExpired', now: Date.now() })).deletedCount;
    },

    async listIndexes() {
      return (await call({ op: 'listIndexes' })).indexes || [];
    }
  };

  /* An upsert that matches nothing needs an id, for the same reason an
   * insert does; sending one costs a field and saves a round trip that
   * would otherwise end in DC_ERR_REQ_MISSING_FIELD.
   *
   * `now` is the same bargain for the same reason. C will not read a
   * clock, so {$currentDate: {...}} is rewritten into a concrete $set by
   * the server using THIS side's milliseconds -- the rule stays in C
   * (upd_resolve_current_date, which also owns the collision check),
   * while the clock stays with the caller, exactly as it does for the
   * timestamp inside every _id this client mints. Sent on every update
   * rather than only when the update looks like it needs one: deciding
   * where $currentDate may appear is the grammar's business, not this
   * file's. */
  /* The wire says `returnNew`, which is what C's own API calls it
   * (dc_find_one_and_update's return_new); the driver's spelling stays
   * on this side, where drivers live. */
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

/*
 * One database, over a connection that carries many.
 *
 * Every request it sends names its database, which is what `db` is doing
 * in front of every op: the CONNECTION is not stateful about which one,
 * so two of these can be held at once and switched between freely. That
 * is `client.db(name)`'s whole contract, and the reason there is no
 * "use" op -- a request whose meaning depends on an earlier request
 * could not be interleaved with another database's at all.
 *
 * Duck-typed as a connection rather than wrapping one, so `collection()`
 * and the change-stream plumbing below it need to know nothing about
 * databases: they call `.call` and get the field added underneath them.
 */
function scope(conn, name) {
  return {
    call: (req) => conn.call({ db: name, ...req }),
    request: (req) => conn.request({ db: name, ...req }),
    _watching: (...a) => conn._watching(...a),
    _unwatching: (...a) => conn._unwatching(...a)
  };
}

function database(conn, name) {
  const sc = scope(conn, name);
  const impl = {
    name,
    get isOpen() { return conn.isOpen !== false; },
    collection: (coll) => collection(sc, coll),
    /** Make a collection with no documents in it. Idempotent: an
     * existing one answers `false` and is left alone. An insert creates
     * one too, so this is for the empty-collection case and for saying
     * when it happened. */
    async createCollection(coll) {
      return (await sc.call({ op: 'createCollection', coll })).created;
    },
    async dropCollection(coll) {
      return (await sc.call({ op: 'dropCollection', coll })).dropped;
    },
    /*
     * Compact every collection (docs/compaction.md), reporting
     * { [name]: stats | null } -- null meaning skipped.
     *
     * The three options are the whole difference between this and a loop
     * over collection.compact(), and two of them read state this side
     * cannot see: `factor` compares a file set against the size the
     * catalog recorded right after its last compaction, and `skipBusy`
     * asks whether anyone is scanning it. So the sweep is the server's,
     * as it is in-process.
     */
    async compact(options = undefined) {
      const req = { op: 'compact' };
      if (options?.minBytes) req.minBytes = options.minBytes;
      if (options?.factor) req.factor = options.factor;
      if (options?.skipBusy) req.skipBusy = true;
      return (await sc.call(req)).result || {};
    },
    /** Every collection in THIS database. */
    async listCollections() {
      return (await sc.call({ op: 'listCollections' })).collections || [];
    },
    /* The escape hatch, and the only thing here that is not shaped like
     * the in-process API: send an op the wire has and read the response
     * object as it came. A new op is usable from JavaScript the day it
     * lands in C, before it has a method. */
    request: (req) => sc.call(req),
    /** A handle, not a resource: the CONNECTION is what closes, and it
     * is the client's. Here so that code holding a `Db` can say it is
     * done without knowing which. */
    close: () => conn.close()
  };
  return guard(impl, 'db');
}

/*
 * A connection to a nisaba-server, which holds an INSTANCE: one root
 * directory, a subdirectory per database.
 *
 *     const client = await connectServer('127.0.0.1:8097');
 *     const analytics = client.db('analytics');
 *     const billing   = client.db('billing');
 *
 * `client.db(name)` sends nothing -- it is a handle, exactly as
 * `Client.db(name)` is in process (wasm/nisaba-wasm.js). The mirror is
 * deliberate: `connect`/`connectClient` there, and here the one that
 * holds many, with a socket where the provider is.
 *
 * ONE PROCESS PER ROOT is still the whole answer to concurrent writers,
 * with the scope widened: the server owns the root directory and
 * everything beneath it, so no two openers ever meet.
 *
 * @param {string|{host:string,port:number}} address `host:port`, or a port
 * @returns {Promise<object>} db(name), listDatabases(), dropDatabase(),
 *   ping(), close()
 */
export async function connectServer(address, { keepAliveMs = DEFAULT_KEEPALIVE_MS, onClose = null } = {}) {
  const { host, port } = parseAddress(address);
  const socket = await new Promise((resolve, reject) => {
    const s = net.connect({ host, port });
    const onError = (err) => reject(new Error(
      `cannot reach a nisaba server at ${host}:${port}: ${err.message}`));
    s.once('error', onError);
    s.once('connect', () => {
      s.off('error', onError);
      s.setNoDelay(true);
      resolve(s);
    });
  });

  const conn = new Connection(socket, `${host}:${port}`);
  /* `onClose` fires once, however the connection dies -- the server
   * reaping an idle slot, the process going away, or close() below. It
   * reports; it must not be used to resurrect, because by the time it
   * runs every pending call and stream has already been failed. */
  if (onClose) conn.onDead = onClose;

  /* Keep the slot. A failed ping is not raised here -- there is nobody to
   * raise it to -- but it kills the connection the same way any other
   * failure does, so the next real call reports it. */
  let keepAlive = null;
  if (keepAliveMs > 0) {
    keepAlive = setInterval(() => {
      conn.request({ op: 'ping' }).catch(() => {});
    }, keepAliveMs);
    keepAlive.unref?.();
  }

  const dbs = new Map();   // name -> the handle, cached like Client's
  const impl = {
    isOpen: true,
    address: `${host}:${port}`,
    /** A database on this connection. Sends nothing; the same name
     * returns the same handle, as `Client.db(name)` does in process. */
    db(name) {
      const cached = dbs.get(name);
      if (cached) return cached;
      const d = database(conn, name);
      dbs.set(name, d);
      return d;
    },
    /** Every database under the server's root. */
    async listDatabases() {
      return (await conn.call({ op: 'listDatabases' })).databases || [];
    },
    /** Delete one and every file in it; `false` if there was none. */
    async dropDatabase(name) {
      dbs.delete(name);
      return (await conn.call({ op: 'dropDatabase', db: name })).dropped;
    },
    /**
     * The one op that names no database and touches nothing: it exists
     * so a connection can stay warm without pretending to be a query.
     *
     * On a REPLICATED server it is also the only thing a follower will
     * answer — reads belong to the leader — so it is where a member says
     * what it is: `{ pong, role, leaderId, applied, commit }`. Those
     * numbers report and promise nothing. `applied` is that member's own
     * floor at the moment it was asked, and it is precisely the number
     * that may not be used to serve a read.
     */
    async ping() {
      const { ok, ...status } = await conn.call({ op: 'ping' });
      return status;
    },
    /* The escape hatch: send an op the wire has, `db` and all, and read
     * the response object as it came. A new op is usable from JavaScript
     * the day it lands in C, before it has a method. */
    request: (req) => conn.call(req),
    async close() {
      impl.isOpen = false;
      if (keepAlive) clearInterval(keepAlive);
      await conn.close();
    }
  };
  return guard(impl, 'client');
}
