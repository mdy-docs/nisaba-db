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
 * WHAT IS NOT HERE. The wire has twenty-two ops (WIRE_OPS below) and this
 * client has exactly those. Change streams, aggregate and the
 * find-one-and-* family are not on the wire yet; asking for one gets a
 * sentence saying so rather than a TypeError about undefined not being a
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
  'ping',
  'find', 'findOne', 'count', 'distinct',
  'insert', 'insertMany', 'update', 'updateMany', 'replace', 'delete', 'deleteMany',
  'bulkWrite',
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
  }

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
      const p = this._pending.shift();
      if (!p) {
        /* A response nobody asked for. If it is a refusal, it is the
         * server saying why it is about to close this connection before
         * we ever got to ask -- its connection table is full. Anything
         * else means framing is gone, and so is the connection. */
        this._socket.destroy();
        let unsolicited = null;
        try { unsolicited = decode(frame); } catch { /* not even a value */ }
        if (unsolicited && unsolicited.ok === false) {
          return this._die(new ServerError(unsolicited.code, unsolicited.msg));
        }
        return this._die(new Error('the server sent a response to no request'));
      }
      let value;
      try {
        value = decode(frame);
      } catch (err) {
        this._socket.destroy();
        p.reject(err);
        return this._die(err);
      }
      p.resolve(value);
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
      /* A refusal that names a POSITION is one about a list of
       * operations (bulkWrite): which one of them was malformed. */
      const at = typeof res.index === 'number' ? ` (operation ${res.index})` : '';
      const err = new ServerError(res.code, (res.msg || `error ${res.code}`) + at);
      if (typeof res.index === 'number') err.index = res.index;
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
        const docs = call({ op: 'find', filter, ...(opts ? { opts } : {}) })
          .then((res) => res.docs || []);
        let at = 0;
        return guard({
          toArray: () => docs,
          /* {value, done}, the shape the in-process cursor uses -- so a
           * caller that pulls one document at a time reads the same
           * either way, whichever side of a socket it is on. */
          async next() {
            const all = await docs;
            return at < all.length ? { value: all[at++], done: false }
                                   : { value: undefined, done: true };
          },
          close: async () => {},
          async *[Symbol.asyncIterator]() { yield* await docs; }
        }, 'cursor');
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
        }
      };
      return guard(cursor, 'cursor');
    },

    async findOne(filter = {}) {
      const res = await call({ op: 'findOne', filter });
      return res.found ? res.doc : null;
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

      const res = await call({ op: 'bulkWrite', writes, ordered });
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

    async listIndexes() {
      return (await call({ op: 'listIndexes' })).indexes || [];
    }
  };

  /* An upsert that matches nothing needs an id, for the same reason an
   * insert does; sending one costs a field and saves a round trip that
   * would otherwise end in DC_ERR_REQ_MISSING_FIELD. */
  async function write(op, fields, options) {
    const upsert = !!options?.upsert;
    const res = await call({ op, ...fields, ...(upsert ? { upsert, id: new ObjectId() } : {}) });
    return res.result;
  }

  return guard(impl, 'collection');
}

/**
 * Connect to a server. The DIRECTORY is not this end's choice: the server
 * was pointed at one when it started and serves that one for its lifetime
 * (one process per database directory, the invariant that makes the
 * concurrency question disappear), so there is no database name here.
 *
 * @param {string|{host:string,port:number}} address `host:port`, or a port
 * @returns {Promise<object>} a Db-shaped handle: collection(), close()
 */
export async function connectServer(address, { keepAliveMs = DEFAULT_KEEPALIVE_MS } = {}) {
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

  const impl = {
    isOpen: true,
    address: `${host}:${port}`,
    collection: (name) => collection(conn, name),
    /** Make a collection with no documents in it. Idempotent: an
     * existing one answers `false` and is left alone. An insert creates
     * one too, so this is for the empty-collection case and for saying
     * when it happened. */
    async createCollection(name) {
      return (await conn.call({ op: 'createCollection', coll: name })).created;
    },
    async dropCollection(name) {
      return (await conn.call({ op: 'dropCollection', coll: name })).dropped;
    },
    /** Every collection in the database this server holds. */
    async listCollections() {
      return (await conn.call({ op: 'listCollections' })).collections || [];
    },
    /** The one op that touches no collection: it exists so a connection
     * can stay warm without pretending to be a query. */
    async ping() {
      await conn.call({ op: 'ping' });
      return true;
    },
    /* The escape hatch, and the only thing here that is not shaped like
     * the in-process API: send an op the wire has and read the response
     * object as it came. A new op is usable from JavaScript the day it
     * lands in C, before it has a method. */
    request: (req) => conn.call(req),
    async close() {
      impl.isOpen = false;
      if (keepAlive) clearInterval(keepAlive);
      await conn.close();
    }
  };
  return guard(impl, 'db');
}
