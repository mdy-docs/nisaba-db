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
 * WHAT IS NOT HERE. The wire has ten ops (WIRE_OPS below) and this client
 * has exactly those. Indexes, compaction, change streams, listing
 * collections and the find-one-and-* family are not on the wire yet;
 * asking for one gets a sentence saying so rather than a TypeError about
 * undefined not being a function. Adding a method here without adding the
 * op to db_request.c would be inventing a second opinion about what the
 * server does.
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
  'find', 'findOne', 'count', 'distinct',
  'insert', 'update', 'updateMany', 'replace', 'delete', 'deleteMany'
];

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
    if (res.ok === false) throw new ServerError(res.code, res.msg || `error ${res.code}`);
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
function guard(impl) {
  return new Proxy(impl, {
    get(target, prop) {
      if (prop in target) return target[prop];
      if (typeof prop !== 'string') return undefined;
      if (prop === 'then' || prop === 'catch' || prop === 'finally') return undefined;
      return () => {
        throw new Error(
          `the server has no ${prop}() -- its wire carries ${WIRE_OPS.join(', ')}. ` +
          `Open the database directly for the rest.`);
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
  return Object.keys(out).length ? out : null;
}

function collection(conn, name) {
  const call = (req) => conn.call({ coll: name, ...req });

  const impl = {
    collectionName: name,

    find(filter = {}, options = undefined) {
      const opts = findOpts(options);
      const docs = call({ op: 'find', filter, ...(opts ? { opts } : {}) })
        .then((res) => res.docs || []);
      return guard({
        toArray: () => docs,
        async *[Symbol.asyncIterator]() { yield* await docs; }
      });
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

  return guard(impl);
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
export async function connectServer(address) {
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
  const impl = {
    isOpen: true,
    address: `${host}:${port}`,
    collection: (name) => collection(conn, name),
    /* The escape hatch, and the only thing here that is not shaped like
     * the in-process API: send an op the wire has and read the response
     * object as it came. A new op is usable from JavaScript the day it
     * lands in C, before it has a method. */
    request: (req) => conn.call(req),
    async close() {
      impl.isOpen = false;
      await conn.close();
    }
  };
  return guard(impl);
}
