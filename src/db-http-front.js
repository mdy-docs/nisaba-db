/**
 * db-http-front.js — HTTP in front of one nisaba-server, or of a cluster
 * of them (docs/steps → built; the decision it implements is Decision B
 * of docs/deployment-shapes.md: HTTP lives in a Node process in front,
 * over db-server-client.js, not in server/main.c).
 *
 * NO ENGINE IN THIS PROCESS. It holds sockets and maps requests onto
 * them; it never opens a file, and the only imports are node:http and
 * the server client. The moment it needed either, it would have become
 * a second implementation of the thing behind it.
 *
 * THE URL GRAMMAR IS RPC-SHAPED, decided here once:
 *
 *     POST /<op>                     instance ops (ping, listDatabases, …)
 *     POST /db/<db>/<op>             database ops (listCollections, …)
 *     POST /db/<db>/<coll>/<op>      collection ops (find, insert, …)
 *     GET  /db/<db>/<coll>/watch     a change stream, as Server-Sent Events
 *     POST /session                  a session (below); DELETE /session/<id>
 *
 * The path contributes `op`, `db` and `coll` to the wire request and the
 * JSON body is the REST of it, verbatim — so this file maps one-to-one
 * onto the wire and cannot drift from it. The alternative, a REST-shaped
 * GET/PATCH/DELETE grammar, was considered and declined: it is a second
 * grammar, every op the wire grows needs a URL invented for it by hand,
 * and it can be layered over this one later without unbuilding anything.
 * The op names are NOT checked here: the server owns the list of ops that
 * exist (OP_NAMES in db_request.c), refuses an unknown one with -41, and
 * a new op is usable over HTTP the day it lands in C. For the same reason
 * a body that names `op`, `db` or `coll` itself is refused: those facts
 * are the URL's, and two copies of one of them is how they disagree.
 *
 * JSON, IN MONGODB'S EXTENDED SPELLING. Bodies and responses are JSON;
 * the wire values JSON cannot say — ObjectId, Date, binary — cross as
 * {$oid} / {$date} / {$binary} literals, in both directions. The
 * convention has one owner, src/extended-json.js, shared with bin/db.js
 * so the CLI and HTTP can never disagree about what a literal means.
 *
 * TWO FIELDS ARE FILLED IN, because on this wire they are the CLIENT's to
 * provide and for an HTTP caller this process is the client: `id` — the
 * engine will not invent a document identity because that needs a clock
 * (db_session.h) — and `now`, the clock reading the ops that resolve
 * $currentDate and TTL sweeps carry. Anything minted is echoed back under
 * `minted` in the response, and a caller that names its own `_id` (or
 * sends its own `now`) gets nothing added — the fill is a fallback, never
 * an override. That echo is the ONLY thing this side adds to a response;
 * everything else is the server's answer, verbatim.
 *
 * SESSIONS, because a cursor belongs to a CONNECTION (db_session.h) and
 * HTTP has none. POST /session opens one real socket and answers an id;
 * requests carrying `?session=<id>` ride that socket, so a paged find's
 * getMore reaches the member holding the cursor. The reaper is the
 * SERVER's: a session socket sends no keep-alive pings, so a session
 * nobody comes back to is closed by the server's own --idle-timeout, and
 * this table learns of it from the socket closing — one timeout policy,
 * owned where it always was. A session is pinned to the member it opened
 * on; if leadership moves, its reads are refused with the leader's
 * address (503 below) and the honest remedy is a new session, because
 * the cursor state exists on that member and nowhere else.
 *
 * A CHANGE STREAM IS A SERVER PUSH, so GET /db/<db>/<coll>/watch is
 * Server-Sent Events (the idiom src/raft-monitor.js established here):
 * `event: watching` once subscribed, `event: change` per change, `event:
 * end` with a reason for every way it stops. Nothing ends in silence.
 * On a replicated server the stream is RESUMABLE, by SSE's own
 * machinery: each event's log index rides as its `id:`, `?from=` or a
 * Last-Event-ID header replays everything after that index before
 * anything live, and an EventSource that reconnects therefore continues
 * exactly where it got to. A server-side overflow with a token to
 * resume from is a page boundary this side crosses silently; one
 * without a token (a server with no log) is `event: overflow` then the
 * end, the old contract. An SSE connection IS its own session — it
 * holds a dedicated socket, kept alive, released when the HTTP request
 * closes.
 *
 * WHICH MEMBER TAKES A REQUEST: the leader, for reads and writes alike
 * (reads are linearizable and leader-only; a follower refuses both with
 * -63 and the leader's identity). This side keeps a HINT — the address
 * that last behaved like the leader — and when a member refuses with -63
 * or cannot be reached, it re-asks the members themselves: ping every
 * configured address, believe whichever answers `role: "leader"`. The
 * refusal's `leader` record names the peer-wire address, not the client
 * port, so it is treated as the signal to go look, not as a dial string.
 * No election, no health checker, no proxy table — a second opinion
 * about who leads is the bug that costs a split brain. -66 (a leader
 * that cannot prove it still leads) is retried in place: the member is
 * right, the quorum is catching its breath.
 *
 * A REFUSAL IS A RESPONSE. The body is the wire's refusal object
 * verbatim — { ok:false, code, msg }, plus leaderId/leader when the
 * server named one — and the HTTP status carries the class: 400 for
 * "the request is wrong", 503 for "true right now, ask again" (-63
 * no-leader-here, -64 write-lost-to-an-election, -66 no-quorum, -44 no
 * free slot), 502 for "the member went away mid-request" — which for a
 * write means UNKNOWN, not failed, and is exactly why this side never
 * silently retries one. A -64 IS retried, because its meaning is the
 * opposite of unknown: db_session.h defines it as "the write did not
 * happen and no replica holds it, so a retry is safe". 404 is only
 * ever about the URL grammar or a session id, never about data.
 *
 * Authentication, TLS, rate limiting, tenancy: a gateway's job, as
 * everywhere in this repository. CORS is open for the same reason —
 * there is no credential here to protect, and the browser REST client
 * this front end exists for cannot reach it otherwise.
 */
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import {
  connectServer, parseAddress, ServerError, ChangeStreamOverflowError, ObjectId
} from './db-server-client.js';
import { fromExtendedJson, toExtendedJson } from './extended-json.js';

const SSE_HEADERS = {
  'content-type': 'text/event-stream',
  'cache-control': 'no-cache',
  'connection': 'keep-alive',
  'access-control-allow-origin': '*'
};

const JSON_HEADERS = {
  'content-type': 'application/json',
  'access-control-allow-origin': '*'
};

/** Refusals that are about NOW rather than about the request: a caller
 * should ask again, so they are 503 rather than 400. The codes are the
 * server's (db_session.h): -63 not-the-leader, -64 write-lost ("the
 * write did not happen and no replica holds it, so a retry is safe" --
 * the header's words, which is what licenses retrying a WRITE), -66
 * leader-without-proof, -44 every-slot-taken. */
const RETRYABLE = new Set([-63, -64, -66, -44]);

/** The refusals the front end chases itself, within retryMs: leadership
 * is elsewhere (-63), the write was lost to an election and is safe to
 * re-propose (-64), or the leader is waiting on its quorum (-66). -44
 * is 503 but NOT chased: a full connection table does not empty in a
 * retry window, and hammering it makes it fuller. */
const CHASEABLE = new Set([-63, -64, -66]);

/** The ops whose requests carry a clock reading (db-server-client.js
 * sends one for exactly these). Filled only when absent. */
const NOW_OPS = new Set([
  'update', 'updateMany', 'replace',
  'findOneAndUpdate', 'findOneAndReplace', 'findOneAndDelete',
  'bulkWrite', 'pruneExpired'
]);

/** A request body may not restate what the URL already said. */
const URL_FACTS = ['op', 'db', 'coll'];

/* ---- the client-owned fields ---------------------------------------- */

/**
 * Fill `id` and `now` where the wire needs the client to have provided
 * them and the caller did not: the same rules db-server-client.js
 * applies for its own callers, applied here for HTTP's. Returns what was
 * minted, keyed the way the response will echo it — nothing, when the
 * caller named everything itself.
 */
function fill(req) {
  const minted = {};
  switch (req.op) {
    case 'insert':
      if (req.doc && typeof req.doc === 'object') {
        if (req.doc._id === undefined) {
          req.doc = { _id: new ObjectId(), ...req.doc };
          minted.id = req.doc._id;
        }
        if (req.id === undefined) req.id = req.doc._id;
      }
      break;
    case 'insertMany':
      if (Array.isArray(req.docs)) {
        const ids = {};
        req.docs = req.docs.map((d, i) => {
          if (d && typeof d === 'object' && !Array.isArray(d) && d._id === undefined) {
            const id = new ObjectId();
            ids[i] = id;
            return { _id: id, ...d };
          }
          return d;
        });
        if (Object.keys(ids).length) minted.ids = ids;
      }
      break;
    case 'update': case 'updateMany': case 'replace':
    case 'findOneAndUpdate': case 'findOneAndReplace':
      /* An upsert that matches nothing needs an identity; one that
       * matches needs nothing and ignores it. Minted only for upserts,
       * exactly as the JS client does. */
      if (req.upsert && req.id === undefined) {
        req.id = new ObjectId();
        minted.id = req.id;
      }
      break;
    case 'bulkWrite':
      if (Array.isArray(req.writes)) {
        const ids = {};
        req.writes = req.writes.map((w, i) => {
          const [name, spec] = Object.entries(w ?? {})[0] ?? [];
          if (!name || spec === null || typeof spec !== 'object') return w; // the server says so, not us
          if (name === 'insertOne' && spec.document && spec.document._id === undefined) {
            const id = new ObjectId();
            ids[i] = id;
            return { insertOne: { ...spec, document: { _id: id, ...spec.document } } };
          }
          if (spec.upsert && spec.id === undefined) {
            const id = new ObjectId();
            ids[i] = id;
            return { [name]: { ...spec, id } };
          }
          return w;
        });
        if (Object.keys(ids).length) minted.ids = ids;
      }
      break;
  }
  if (NOW_OPS.has(req.op) && req.now === undefined) req.now = Date.now();
  return minted;
}

/* ---- errors as responses -------------------------------------------- */

function statusOf(err) {
  if (err instanceof ServerError) {
    /* A resume token compacted out of the log is GONE, not malformed:
     * the caller's remedy is a fresh watch, and 410 is HTTP's word for
     * exactly that. */
    if (err.code === -68) return 410;
    return RETRYABLE.has(err.code) ? 503 : 400;
  }
  return 502;
}

function bodyOf(err) {
  if (err instanceof ServerError) {
    const body = { ok: false, code: err.code, msg: err.message };
    if (typeof err.index === 'number') body.index = err.index;
    if (typeof err.leaderId === 'number') body.leaderId = err.leaderId;
    if (err.leader) body.leader = err.leader;
    return body;
  }
  return { ok: false, error: err.message };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export class DbHttpFront {
  /**
   * @param {string|string[]} targets - client address(es) of the
   *   server(s): every member of the cluster, or the one server there is.
   * @param {object} [options]
   * @param {number} [options.listenPort=0] - 0 for an ephemeral port
   * @param {string} [options.listenHost='127.0.0.1'] - loopback by
   *   default, deliberately; widen it consciously
   * @param {number} [options.retryMs=8000] - how long a request keeps
   *   chasing a leader (refused with -63/-66, or unreachable) before the
   *   last refusal is the answer. Covers an election, not an outage.
   * @param {number} [options.heartbeatMs=15000] - SSE keep-alive comment
   *   interval (defeats idle-connection reapers between curl and node)
   */
  constructor(targets, { listenPort = 0, listenHost = '127.0.0.1', retryMs = 8000, heartbeatMs = 15_000 } = {}) {
    const list = Array.isArray(targets) ? targets : [targets];
    if (list.length === 0) throw new Error('the front end needs at least one server address');
    this._targets = list.map((t) => { const { host, port } = parseAddress(t); return `${host}:${port}`; });
    this.listenPort = listenPort;
    this.listenHost = listenHost;
    this.retryMs = retryMs;
    this.heartbeatMs = heartbeatMs;

    this._leader = this._targets[0];   // a hint, corrected by refusals
    this._pool = new Map();            // address -> Promise<client>, shared stateless traffic
    this._sessions = new Map();        // session id -> { client, member }
    this._sse = new Set();             // open SSE responses, for heartbeats
    this._server = null;
    this._timer = null;
  }

  async start() {
    this._server = http.createServer((req, res) => {
      this._serve(req, res).catch((err) => {
        /* The last-resort answer: nothing below should get here, but a
         * bug that closes the socket with no response would be a hang
         * from the caller's side, which is the one failure shape this
         * file promises never to produce. */
        if (!res.headersSent) res.writeHead(500, JSON_HEADERS);
        if (!res.writableEnded) res.end(JSON.stringify({ ok: false, error: err.message }));
      });
    });
    await new Promise((resolve, reject) => {
      this._server.once('error', reject);
      this._server.listen(this.listenPort, this.listenHost, () => {
        this._server.removeListener('error', reject);
        resolve();
      });
    });
    this._timer = setInterval(() => {
      for (const res of this._sse) res.write(': keep-alive\n\n');
    }, this.heartbeatMs);
    this._timer.unref?.();
  }

  /** The actual bound address (useful with listenPort 0 in tests). */
  address() { return this._server?.address(); }

  async stop() {
    if (this._timer) clearInterval(this._timer);
    this._timer = null;
    for (const res of this._sse) res.end();
    this._sse.clear();
    const held = [...this._sessions.values()].map((s) => s.client);
    this._sessions.clear();
    const pooled = [...this._pool.values()];
    this._pool.clear();
    for (const c of held) await c.close().catch(() => {});
    for (const p of pooled) await p.then((c) => c.close()).catch(() => {});
    if (this._server) {
      this._server.closeAllConnections?.();
      await new Promise((resolve) => this._server.close(() => resolve()));
      this._server = null;
    }
  }

  /* ---- reaching the cluster ----------------------------------------- */

  /** The pooled connection to one member, made on first use, forgotten
   * the moment it dies -- the next request makes a fresh one. */
  _clientFor(address) {
    let p = this._pool.get(address);
    if (!p) {
      p = connectServer(address, { onClose: () => this._pool.delete(address) })
        .catch((err) => { this._pool.delete(address); throw err; });
      this._pool.set(address, p);
    }
    return p;
  }

  /**
   * Ask the members who leads: ping every target, believe the one that
   * says `role: "leader"` about itself. Nobody else's opinion is
   * consulted -- not the refusal's address (it names the peer wire), not
   * a cached table (it would be a second one). null when no member says
   * so, which during an election is the truth.
   */
  async _locate() {
    const answers = await Promise.all(this._targets.map(async (addr) => {
      try {
        const client = await this._clientFor(addr);
        const status = await Promise.race([
          client.ping(),
          sleep(1000).then(() => null)
        ]);
        return status && (status.role === 'leader' || status.role === undefined) ? addr : null;
      } catch {
        return null;   // unreachable members simply do not vote
      }
    }));
    const found = answers.find((a) => a !== null) ?? null;
    if (found) this._leader = found;
    return found;
  }

  /**
   * One request, delivered to whoever leads, however long that takes
   * within retryMs. Only REFUSALS are retried -- a refusal is the server
   * declining before doing anything, so re-asking is safe for a write.
   * A transport failure mid-request is not retried for exactly the
   * opposite reason: the answer is unknown, and this is the layer that
   * must say so rather than guess (502).
   */
  async _call(req) {
    const deadline = Date.now() + this.retryMs;
    let lastErr = null;
    for (;;) {
      let client = null;
      try {
        client = await this._clientFor(this._leader);
      } catch (err) {
        lastErr = err;                      // nothing was sent: retrying is free
        if (Date.now() >= deadline) throw err;
        await sleep(150);
        await this._locate();
        continue;
      }
      try {
        return await client.request(req);
      } catch (err) {
        if (err instanceof ServerError && CHASEABLE.has(err.code)) {
          lastErr = err;
          if (Date.now() >= deadline) throw err;
          await sleep(150);
          await this._locate();             // -66 relocates to the same member, which is correct
          continue;
        }
        throw err;
      }
    }
  }

  /* ---- HTTP --------------------------------------------------------- */

  async _serve(req, res) {
    if (req.method === 'OPTIONS') {
      res.writeHead(204, {
        'access-control-allow-origin': '*',
        'access-control-allow-methods': 'GET, POST, DELETE, OPTIONS',
        'access-control-allow-headers': 'content-type'
      });
      return res.end();
    }

    const url = new URL(req.url, 'http://x');
    const parts = url.pathname.split('/').filter(Boolean).map(decodeURIComponent);

    if (req.method === 'GET' && parts.length === 0) {
      res.writeHead(200, { 'content-type': 'text/plain', 'access-control-allow-origin': '*' });
      return res.end(
        'nisaba HTTP front end. POST /<op>, /db/<db>/<op>, /db/<db>/<coll>/<op> ' +
        'with a JSON body; GET /db/<db>/<coll>/watch for Server-Sent Events; ' +
        'POST /session and ?session=<id> for cursors. docs/http-front.md has the grammar.\n');
    }

    /* Sessions. */
    if (parts[0] === 'session') {
      if (req.method === 'POST' && parts.length === 1) return this._openSession(res);
      if (req.method === 'DELETE' && parts.length === 2) return this._closeSession(parts[1], res);
      return this._notFound(res);
    }

    /* The change stream. `from` resumes: the query parameter for a
     * first request, the Last-Event-ID header on an SSE reconnect --
     * and the header wins, because a reconnecting consumer knows better
     * than its original URL where it actually got to. */
    if (req.method === 'GET' && parts.length === 4 && parts[0] === 'db' && parts[3] === 'watch') {
      const header = req.headers['last-event-id'];
      const raw = header !== undefined ? header : url.searchParams.get('from');
      let from = null;
      if (raw !== null && raw !== undefined && raw !== '') {
        from = Number(raw);
        if (!Number.isInteger(from) || from < 0) {
          return this._json(res, 400, {
            ok: false,
            error: `not a resume index: '${raw}' (want the \`index\` of a delivered event)`
          });
        }
      }
      return this._watch(parts[1], parts[2], from, res, req);
    }

    if (req.method !== 'POST') return this._notFound(res);

    /* The RPC grammar: the path is op/db/coll, the body is the rest. */
    let wire = null;
    if (parts.length === 1) wire = { op: parts[0] };
    else if (parts.length === 3 && parts[0] === 'db') wire = { op: parts[2], db: parts[1] };
    else if (parts.length === 4 && parts[0] === 'db') wire = { op: parts[3], db: parts[1], coll: parts[2] };
    if (!wire) return this._notFound(res);

    let body = null;
    try {
      const text = await this._readBody(req);
      body = text.trim() ? fromExtendedJson(JSON.parse(text)) : {};
    } catch (err) {
      return this._json(res, 400, { ok: false, error: `the body is not JSON: ${err.message}` });
    }
    if (body === null || typeof body !== 'object' || Array.isArray(body)) {
      return this._json(res, 400, { ok: false, error: 'the body must be a JSON object' });
    }
    const restated = URL_FACTS.filter((k) => body[k] !== undefined);
    if (restated.length) {
      return this._json(res, 400, {
        ok: false,
        error: `${restated.join(', ')}: the URL owns ${restated.length > 1 ? 'these' : 'this'}; ` +
               'a body that restates one could disagree with it'
      });
    }

    const request = { ...wire, ...body };
    const minted = fill(request);

    const sessionId = url.searchParams.get('session');
    try {
      let answer;
      if (sessionId !== null) {
        const session = this._sessions.get(sessionId);
        if (!session) {
          return this._json(res, 404, {
            ok: false,
            error: 'no such session -- it was closed, reaped by the server\'s idle timeout, ' +
                   'or never existed. Open another with POST /session.'
          });
        }
        /* Pinned, not followed: the cursor state this session exists for
         * lives on one member. A -63 here is answered to the caller,
         * leader's address and all, because following it would reach a
         * member that has never heard of the cursor. */
        answer = await session.client.request(request);
      } else {
        answer = await this._call(request);
      }
      const out = toExtendedJson(answer);
      if (Object.keys(minted).length) out.minted = toExtendedJson(minted);
      return this._json(res, 200, out);
    } catch (err) {
      return this._json(res, statusOf(err), bodyOf(err));
    }
  }

  async _openSession(res) {
    try {
      /* Sessions open on the leader -- the only member that will answer
       * the reads a cursor is made of. */
      const member = (await this._locate()) ?? this._leader;
      const id = randomUUID();
      const client = await connectServer(member, {
        keepAliveMs: 0,   // the server's --idle-timeout is the reaper, on purpose
        onClose: () => this._sessions.delete(id)
      });
      this._sessions.set(id, { client, member });
      return this._json(res, 200, { ok: true, session: id, member });
    } catch (err) {
      return this._json(res, statusOf(err), bodyOf(err));
    }
  }

  async _closeSession(id, res) {
    const session = this._sessions.get(id);
    this._sessions.delete(id);
    if (session) await session.client.close().catch(() => {});
    return this._json(res, 200, { ok: true, closed: !!session });
  }

  /**
   * The SSE endpoint. Its own socket to the leader, subscribed before
   * the 200 is committed -- a refused watch is a JSON refusal with the
   * right status, not an empty stream -- and released when either side
   * goes: the HTTP request closing closes the stream and the socket; the
   * socket dying ends the response, with a reason, always.
   *
   * Every event that has a log index carries it as its SSE `id:`, which
   * is what makes the stream RESUMABLE by the protocol's own machinery:
   * an EventSource that reconnects sends Last-Event-ID, and the watch
   * picks up right after it -- exactly once, in order, across the
   * reconnect. An overflow with a token is handled here and never
   * reaches the consumer: the server closed the stream at a page
   * boundary, so this side re-watches from the last index it wrote and
   * the response simply continues -- which is also how a long history
   * pages through the server's bounded queue. Only an overflow with no
   * token (a server with no log) still ends the stream, because there
   * is nothing to resume from and pretending otherwise would be a gap.
   */
  async _watch(db, coll, from, res, req) {
    const deadline = Date.now() + this.retryMs;
    let client = null;
    let stream = null;
    for (;;) {
      try {
        const member = this._leader;
        client = await connectServer(member);
        stream = client.db(db).collection(coll)
          .watch(from !== null ? { from } : {});
        await stream.ready;
        break;
      } catch (err) {
        await client?.close().catch(() => {});
        client = null;
        const chase = (err instanceof ServerError && CHASEABLE.has(err.code)) ||
                      !(err instanceof ServerError);
        if (!chase || Date.now() >= deadline) {
          return this._json(res, statusOf(err), bodyOf(err));
        }
        await sleep(150);
        await this._locate();
      }
    }

    res.writeHead(200, SSE_HEADERS);
    /* The subscribe position rides as the first frame's id: a consumer
     * that reconnects before any change resumes from here rather than
     * from nothing. */
    const hello = { db, coll, member: client.address };
    if (stream.resumeFrom !== null) hello.index = stream.resumeFrom;
    res.write((stream.resumeFrom !== null ? `id: ${stream.resumeFrom}\n` : '') +
              `event: watching\ndata: ${JSON.stringify(hello)}\n\n`);
    this._sse.add(res);

    const finish = (event, data) => {
      if (res.writableEnded) return;
      res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
      res.end();
    };

    req.on('close', async () => {
      this._sse.delete(res);
      await stream.close().catch(() => {});
      await client.close().catch(() => {});
    });

    try {
      for (;;) {
        try {
          for await (const change of stream) {
            const id = typeof change.index === 'number' ? `id: ${change.index}\n` : '';
            res.write(`${id}event: change\ndata: ${JSON.stringify(toExtendedJson(change))}\n\n`);
          }
          finish('end', { reason: 'closed' });
          break;
        } catch (err) {
          if (err instanceof ChangeStreamOverflowError && err.resumeFrom !== null &&
              !res.writableEnded) {
            /* A page boundary, not a loss: continue from where the
             * server stopped, on the same connection. */
            stream = client.db(db).collection(coll).watch({ from: err.resumeFrom });
            try { await stream.ready; continue; } catch (again) {
              finish('end', { reason: again.message });
              break;
            }
          }
          if (err instanceof ChangeStreamOverflowError) {
            finish('overflow', { reason: err.message });
          } else {
            finish('end', { reason: err.message });
          }
          break;
        }
      }
    } finally {
      this._sse.delete(res);
      await client.close().catch(() => {});
    }
  }

  _readBody(req) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      let size = 0;
      req.on('data', (c) => {
        size += c.length;
        if (size > 64 * 1024 * 1024) {
          reject(new Error('body larger than 64 MB'));
          req.destroy();
          return;
        }
        chunks.push(c);
      });
      req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
      req.on('error', reject);
    });
  }

  _json(res, status, body) {
    if (res.headersSent) return;
    res.writeHead(status, JSON_HEADERS);
    res.end(JSON.stringify(body));
  }

  _notFound(res) {
    return this._json(res, 404, {
      ok: false,
      error: 'no such route. POST /<op>, /db/<db>/<op> or /db/<db>/<coll>/<op>; ' +
             'GET /db/<db>/<coll>/watch; POST /session; DELETE /session/<id>.'
    });
  }
}
