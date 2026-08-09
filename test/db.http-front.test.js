/**
 * The HTTP front end (src/db-http-front.js): curl's way into a
 * nisaba-server, and into a cluster of them.
 *
 * The front end's whole job is to change the TRANSPORT and nothing else,
 * so the shape of every test here is the comparison the brief asked for:
 * drive an operation over HTTP, drive the same operation over
 * db-server-client.js directly, and the answers must agree. What HTTP
 * adds beyond the wire -- the URL grammar, Extended JSON, minted-id
 * echoes, sessions, SSE, statuses for refusals -- is asserted alongside.
 *
 * NO WASM IN THIS FILE, deliberately: the processes under test are the C
 * server and a front end that holds no engine, and a test that imported
 * the engine to check them would be blurring exactly the line the front
 * end exists to keep sharp.
 *
 * The cluster half runs against the native binary only: which engine the
 * members run is a server property, proven per-engine in
 * db.server.test.js, and the front end cannot see the difference.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { spawn, spawnSync } from 'node:child_process';
import http from 'node:http';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer, ObjectId } from '../src/db-server-client.js';
import { toExtendedJson } from '../src/extended-json.js';
import { DbHttpFront } from '../src/db-http-front.js';

const NATIVE = 'build/lib/nisaba-server';
const WASIP2 = 'build/lib/nisaba-server-wasip2.wasm';
const have = (p) => fs.existsSync(p);
const wasmtime = (() => {
  const r = spawnSync('sh', ['-c', 'command -v wasmtime'], { encoding: 'utf8' });
  return r.status === 0 ? r.stdout.trim() : null;
})();
const REQUIRED = process.env.NISABA_SERVER_TESTS === 'required';

/* A base far from db.server.test.js's 18000 block: the two files run in
 * parallel workers and can be alive at once. */
let portSlot = 1;
const nextPort = () => 40000 + (portSlot++) * 1000 + (process.pid % 1000);

const ENGINES = [
  {
    name: 'native',
    ready: () => have(NATIVE),
    argv: (dir, port, extra) => [path.resolve(NATIVE), ['--port', String(port), ...extra], { cwd: dir }]
  },
  {
    name: 'wasm32-wasip2 under wasmtime',
    ready: () => have(WASIP2) && !!wasmtime,
    argv: (dir, port, extra) => [wasmtime, [
      'run', '-S', 'inherit-network', '--dir', `${dir}::.`,
      path.resolve(WASIP2), '--port', String(port), ...extra
    ], {}]
  }
];

async function startServer(engine, port, extra = [], reuse = null) {
  const dir = reuse ?? fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-http-'));
  const [cmd, args, opts] = engine.argv(dir, port, extra);
  const proc = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${engine.name} server did not start`)), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve({ proc, dir }); }
    });
  });
}

/** POST a JSON body, read a JSON answer. What curl does, as a function. */
async function post(front, pathname, body = undefined) {
  const { port } = front.address();
  const res = await fetch(`http://127.0.0.1:${port}${pathname}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    ...(body !== undefined ? { body: JSON.stringify(body) } : {})
  });
  return { status: res.status, body: await res.json() };
}

/**
 * An open SSE response, parsed into events as they arrive. Raw node:http
 * rather than EventSource, because what is being tested is the frames on
 * the wire, not a browser's reconnect policy.
 */
function sse(front, pathname, headers = {}) {
  const { port } = front.address();
  const events = [];
  const waiting = [];
  let req = null;
  const push = (ev) => {
    const w = waiting.shift();
    if (w) w(ev); else events.push(ev);
  };
  const ready = new Promise((resolve, reject) => {
    req = http.get(`http://127.0.0.1:${port}${pathname}`, { headers }, (res) => {
      resolve(res.statusCode);
      let buf = '';
      res.on('data', (chunk) => {
        buf += chunk;
        for (;;) {
          const at = buf.indexOf('\n\n');
          if (at < 0) return;
          const frame = buf.slice(0, at);
          buf = buf.slice(at + 2);
          if (frame.startsWith(':')) continue;   // keep-alive comment
          const ev = {};
          for (const line of frame.split('\n')) {
            const [k, ...rest] = line.split(': ');
            ev[k] = rest.join(': ');
          }
          if (ev.data !== undefined) ev.data = JSON.parse(ev.data);
          push(ev);
        }
      });
      res.on('end', () => push(null));
    });
    req.on('error', reject);
  });
  return {
    ready,
    next: (timeoutMs = 10000) => new Promise((resolve, reject) => {
      if (events.length) return resolve(events.shift());
      const t = setTimeout(() => reject(new Error('no SSE event arrived')), timeoutMs);
      waiting.push((ev) => { clearTimeout(t); resolve(ev); });
    }),
    close: () => req.destroy()
  };
}

describe.each(ENGINES.filter((e) => REQUIRED || e.ready()))(
  'http front end: every op over one server ($name)', (engine) => {
  const DB = 'httpdb';
  const port = nextPort();
  let proc, front, direct;

  beforeAll(async () => {
    ({ proc } = await startServer(engine, port));
    front = new DbHttpFront(`127.0.0.1:${port}`, { listenPort: 0 });
    await front.start();
    direct = await connectServer(port);
    return async () => {
      await direct.close();
      await front.stop();
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
    };
  }, 60000);

  it('pings, and the answer is the wire\'s', async () => {
    const { status, body } = await post(front, '/ping');
    expect(status).toBe(200);
    expect(body).toEqual({ ok: true, pong: true });
  });

  it('creates a collection, inserts, and the direct client sees the same database', async () => {
    expect((await post(front, `/db/${DB}/users/createCollection`)).body)
      .toEqual({ ok: true, created: true });

    // A document with no _id: the front end mints one and says so.
    const one = await post(front, `/db/${DB}/users/insert`, { doc: { name: 'Ada', team: 'core' } });
    expect(one.status).toBe(200);
    expect(one.body.ok).toBe(true);
    expect(one.body.minted.id.$oid).toMatch(/^[0-9a-f]{24}$/);

    // A document that names its own: nothing is minted, nothing echoed.
    const mine = new ObjectId().toHexString();
    const two = await post(front, `/db/${DB}/users/insert`, {
      doc: { _id: { $oid: mine }, name: 'Grace', team: 'core' }
    });
    expect(two.body.ok).toBe(true);
    expect(two.body.minted).toBeUndefined();

    const found = await direct.db(DB).collection('users').findOne({ name: 'Grace' });
    expect(found._id.toHexString()).toBe(mine);
  });

  it('insertMany mints per document and the count agrees both ways', async () => {
    const res = await post(front, `/db/${DB}/users/insertMany`, {
      docs: [{ name: 'Alan', team: 'research' }, { name: 'Edsger', team: 'research' }]
    });
    expect(res.body.ok).toBe(true);
    expect(res.body.attempted).toBe(2);
    expect(Object.keys(res.body.minted.ids)).toEqual(['0', '1']);

    const overHttp = await post(front, `/db/${DB}/users/count`, { filter: {} });
    expect(overHttp.body.n).toBe(await direct.db(DB).collection('users').countDocuments({}));
    expect(overHttp.body.n).toBe(4);
  });

  it('reads agree with the direct client, Extended JSON and all', async () => {
    const overHttp = await post(front, `/db/${DB}/users/find`, {
      filter: { team: 'core' }, opts: { sort: { name: 1 } }
    });
    const directly = await direct.db(DB).collection('users')
      .find({ team: 'core' }, { sort: { name: 1 } }).toArray();
    // The comparison IS the test: same docs, with ObjectIds spelled {$oid}.
    expect(overHttp.body.docs).toEqual(toExtendedJson(directly));

    const one = await post(front, `/db/${DB}/users/findOne`, { filter: { name: 'Ada' } });
    expect(one.body.found).toBe(true);
    expect(one.body.doc.team).toBe('core');

    const distinct = await post(front, `/db/${DB}/users/distinct`, { field: 'team', filter: {} });
    expect([...distinct.body.values].sort()).toEqual(['core', 'research']);

    const agg = await post(front, `/db/${DB}/users/aggregate`, {
      stages: [{ $group: { _id: '$team', n: { $sum: 1 } } }, { $sort: { _id: 1 } }]
    });
    expect(agg.body.docs).toEqual([{ _id: 'core', n: 2 }, { _id: 'research', n: 2 }]);

    const plan = await post(front, `/db/${DB}/users/explain`, { filter: { name: 'Ada' } });
    expect(plan.body.plan).toBeDefined();
  });

  it('updates, upserts (minting the id an upsert may need), and modifies-and-returns', async () => {
    const up = await post(front, `/db/${DB}/users/update`, {
      filter: { name: 'Ada' }, update: { $set: { lang: 'lovelace' } }
    });
    expect(up.body.result.modifiedCount).toBe(1);

    const upsert = await post(front, `/db/${DB}/users/update`, {
      filter: { name: 'Barbara' }, update: { $set: { team: 'compilers' } }, upsert: true
    });
    expect(upsert.body.minted.id.$oid).toMatch(/^[0-9a-f]{24}$/);
    // The upserted identity is the one the front end minted -- the echo
    // and the server's answer are the same fact, told by both sides.
    expect(upsert.body.result.upsertedId).toEqual(upsert.body.minted.id);

    const fetched = await post(front, `/db/${DB}/users/findOneAndUpdate`, {
      filter: { name: 'Barbara' }, update: { $set: { lang: 'fortran' } }, returnNew: true
    });
    expect(fetched.body.doc.lang).toBe('fortran');

    const gone = await post(front, `/db/${DB}/users/findOneAndDelete`, { filter: { name: 'Barbara' } });
    expect(gone.body.doc.name).toBe('Barbara');
    expect(await direct.db(DB).collection('users').countDocuments({ name: 'Barbara' })).toBe(0);
  });

  it('bulkWrite crosses whole, ids minted where the list needs them', async () => {
    const res = await post(front, `/db/${DB}/users/bulkWrite`, {
      writes: [
        { insertOne: { document: { name: 'Tony', team: 'core' } } },
        { updateOne: { filter: { name: 'Nobody' }, update: { $set: { team: 'x' } }, upsert: true } },
        { deleteOne: { filter: { name: 'Tony' } } }
      ]
    });
    expect(res.body.ok).toBe(true);
    expect(res.body.result.insertedCount).toBe(1);
    expect(res.body.result.upsertedCount).toBe(1);
    expect(res.body.result.deletedCount).toBe(1);
    expect(Object.keys(res.body.minted.ids)).toEqual(['0', '1']);
    await post(front, `/db/${DB}/users/deleteMany`, { filter: { name: 'Nobody' } });
  });

  it('indexes: create, list, look up by, drop', async () => {
    const made = await post(front, `/db/${DB}/users/createIndex`, { keys: { team: 1 } });
    expect(made.body.name).toBe('team_1');
    const listed = await post(front, `/db/${DB}/users/listIndexes`);
    expect(listed.body.indexes.map((i) => i.name)).toContain('team_1');
    const hit = await post(front, `/db/${DB}/users/findByIndex`, { index: 'team_1', values: ['research'] });
    expect(hit.body.docs.map((d) => d.name).sort()).toEqual(['Alan', 'Edsger']);
    expect((await post(front, `/db/${DB}/users/dropIndex`, { index: 'team_1' })).body.ok).toBe(true);
  });

  it('the instance ops: listDatabases, listCollections, compact, prune, drop', async () => {
    const dbs = await post(front, '/listDatabases');
    expect(dbs.body.databases).toContain(DB);
    const colls = await post(front, `/db/${DB}/listCollections`);
    expect(colls.body.collections).toContain('users');
    expect((await post(front, `/db/${DB}/users/pruneExpired`)).body.ok).toBe(true);
    expect((await post(front, `/db/${DB}/users/compact`)).body.ok).toBe(true);
    expect((await post(front, `/db/${DB}/scratch/createCollection`)).body.created).toBe(true);
    expect((await post(front, `/db/${DB}/scratch/dropCollection`)).body.dropped).toBe(true);
    // dropDatabase names its database in the URL like everything else.
    expect((await post(front, '/db/doomed/junk/insert', { doc: { x: 1 } })).body.ok).toBe(true);
    expect((await post(front, '/db/doomed/dropDatabase')).body.dropped).toBe(true);
  });

  it('a refusal is a response: the wire\'s code, under the right status', async () => {
    // An op the server does not know: ITS refusal, not a 404 of ours.
    const unknown = await post(front, `/db/${DB}/users/explodinate`);
    expect(unknown.status).toBe(400);
    expect(unknown.body).toMatchObject({ ok: false, code: -41 });
    expect(unknown.body.msg).toMatch(/does not know/);

    // A body restating what the URL owns.
    const restated = await post(front, `/db/${DB}/users/find`, { coll: 'users' });
    expect(restated.status).toBe(400);
    expect(restated.body.error).toMatch(/URL owns/);

    // A body that is not JSON at all.
    const { port } = front.address();
    const bad = await fetch(`http://127.0.0.1:${port}/db/${DB}/users/find`, {
      method: 'POST', body: '{nope' });
    expect(bad.status).toBe(400);

    // A route the grammar does not have.
    const lost = await post(front, `/db/${DB}/users/find/extra`);
    expect(lost.status).toBe(404);
  });

  it('a session pages a cursor across requests, and dies loudly when unknown', async () => {
    const docs = Array.from({ length: 5 }, (_, i) => ({ n: i }));
    await post(front, `/db/${DB}/pages/insertMany`, { docs });

    const opened = await post(front, '/session');
    expect(opened.body.ok).toBe(true);
    const s = opened.body.session;

    const first = await post(front, `/db/${DB}/pages/find?session=${s}`, {
      filter: {}, opts: { batchSize: 2 }
    });
    expect(first.body.docs).toHaveLength(2);
    expect(typeof first.body.cursor).toBe('number');

    let cursor = first.body.cursor;
    const rest = [];
    while (cursor !== null && cursor !== undefined) {
      const page = await post(front, `/db/${DB}/pages/getMore?session=${s}`, { cursor });
      rest.push(...page.body.docs);
      cursor = page.body.cursor;
    }
    expect(first.body.docs.length + rest.length).toBe(5);

    // A cursor is session state: the shared connection has never heard
    // of it, and the SERVER says so -- the refusal crosses as itself.
    const elsewhere = await post(front, `/db/${DB}/pages/getMore`, { cursor: first.body.cursor });
    expect(elsewhere.status).toBe(400);
    expect(elsewhere.body.ok).toBe(false);

    expect((await post(front, `/session/${s}`)).status).toBe(404); // POST is not DELETE
    const { port } = front.address();
    const closed = await fetch(`http://127.0.0.1:${port}/session/${s}`, { method: 'DELETE' });
    expect((await closed.json()).closed).toBe(true);

    const gone = await post(front, `/db/${DB}/pages/find?session=${s}`, { filter: {} });
    expect(gone.status).toBe(404);
    expect(gone.body.error).toMatch(/no such session/);
  });

  it('a change stream is Server-Sent Events: subscribe, change, close', async () => {
    const stream = sse(front, `/db/${DB}/watched/watch`);
    expect(await stream.ready).toBe(200);

    const hello = await stream.next();
    expect(hello.event).toBe('watching');
    expect(hello.data).toMatchObject({ db: DB, coll: 'watched' });

    await post(front, `/db/${DB}/watched/insert`, { doc: { name: 'first' } });
    const change = await stream.next();
    expect(change.event).toBe('change');
    expect(change.data.operationType).toBe('insert');
    expect(change.data.fullDocument.name).toBe('first');
    expect(change.data.fullDocument._id.$oid).toMatch(/^[0-9a-f]{24}$/);

    stream.close();
  });
});

describe.skipIf(!(REQUIRED || have(NATIVE)))('http front end: a three-member cluster (native)', () => {
  const DB = 'clusterdb';
  const base = nextPort();
  const MEMBERS = [1, 2, 3].map((id) => ({
    id,
    port: base + id - 1,
    raftPort: base + 10 + id - 1
  }));
  const engine = ENGINES[0];
  const argsFor = (m) => [
    '--raft', String(m.id), '--raft-port', String(m.raftPort),
    ...MEMBERS.filter((o) => o.id !== m.id)
      .flatMap((o) => ['--peer', `${o.id}@127.0.0.1:${o.raftPort}`])
  ];
  let front;

  const boot = async (m) => {
    const { proc, dir } = await startServer(engine, m.port, argsFor(m), m.dir);
    m.proc = proc;
    m.dir = dir;
    m.alive = true;
  };
  const stop = async (m) => {
    if (!m.alive) return;
    m.alive = false;
    m.proc.kill();
    await new Promise((r) => m.proc.once('exit', r));
  };
  const leaderOf = async () => {
    for (let i = 0; i < 100; i++) {
      for (const m of MEMBERS.filter((n) => n.alive)) {
        try {
          const c = await connectServer(m.port, { keepAliveMs: 0 });
          const s = await c.ping();
          await c.close();
          if (s.role === 'leader') return m;
        } catch { /* between elections, or booting */ }
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error('no member ever led');
  };

  beforeAll(async () => {
    for (const m of MEMBERS) await boot(m);
    front = new DbHttpFront(MEMBERS.map((m) => `127.0.0.1:${m.port}`), {
      listenPort: 0,
      retryMs: 20000   // covers an election, which is what this suite provokes
    });
    await front.start();
    return async () => {
      await front.stop();
      for (const m of MEMBERS) await stop(m);
    };
  }, 90000);

  it('a write reaches the leader without the caller knowing which member that is', async () => {
    /*
     * The error body is in the assertion message on purpose. This test
     * was seen to fail once with a bare `expected 400 to be 200`, on a
     * machine so loaded the whole suite took twenty times its usual
     * wall clock, and has not been reproducible since. A 400 here means
     * a refusal the front end classified as the CALLER's fault
     * (src/db-http-front.js: anything outside RETRYABLE) — which for a
     * well-formed write to a healthy cluster is either an engine code
     * that ought to be retryable and is not, or a request body that did
     * not fully arrive. Both are worth knowing, and neither is
     * recoverable from a status code alone.
     */
    const res = await post(front, `/db/${DB}/events/insert`, { doc: { seq: 1 } });
    expect(res.status, JSON.stringify(res.body)).toBe(200);
    expect(res.body.ok).toBe(true);

    // And so does the read that checks it -- reads are the leader's too.
    const count = await post(front, `/db/${DB}/events/count`, { filter: {} });
    expect(count.status, JSON.stringify(count.body)).toBe(200);
    expect(count.body.n).toBe(1);
  }, 60000);

  it('follows leadership when it moves: kill the leader, write again', async () => {
    const deposed = await leaderOf();
    await stop(deposed);

    // The front end's hint is now a dead member. The next write finds
    // the new leader by asking the survivors, not us.
    const res = await post(front, `/db/${DB}/events/insert`, { doc: { seq: 2 } });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);

    const count = await post(front, `/db/${DB}/events/count`, { filter: {} });
    expect(count.body.n).toBe(2);

    await boot(deposed);   // leave the cluster whole for the next test
  }, 60000);

  it('transferLeadership rides the grammar: leadership moves, the front follows', async () => {
    // An op the wire grew is usable over HTTP the day it lands in C --
    // the front end never checked op names, and this one proves it: the
    // URL grammar carries it, the leader answers once leadership has
    // actually LEFT it, and the next write finds the member the
    // transfer chose, exactly as it finds one after a kill.
    const leader = await leaderOf();
    const target = MEMBERS.find((m) => m.alive && m.id !== leader.id);
    const res = await post(front, '/transferLeadership', { to: target.id });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);

    const write = await post(front, `/db/${DB}/events/insert`, { doc: { seq: 3 } });
    expect(write.status).toBe(200);
    expect((await leaderOf()).id).toBe(target.id);
  }, 60000);

  it('a change stream through the front end sees a write made through the front end', async () => {
    const stream = sse(front, `/db/${DB}/feed/watch`);
    expect(await stream.ready).toBe(200);
    expect((await stream.next()).event).toBe('watching');

    await post(front, `/db/${DB}/feed/insert`, { doc: { note: 'replicated' } });
    const change = await stream.next(20000);
    expect(change.event).toBe('change');
    expect(change.data.operationType).toBe('insert');
    expect(change.data.fullDocument.note).toBe('replicated');
    stream.close();
  }, 60000);

  /*
   * Resume where the watch ANSWER is itself deferred: on a real cluster
   * a read is held behind the ReadIndex barrier, so the subscribe reply
   * and the replayed events cross the socket in one burst once the
   * quorum confirms -- the ordering (answer first, then events; events
   * held for an unclaimed stream id) is exactly what this exercises,
   * and what a group of one cannot.
   */
  it('resumes a stream on a cluster, events replayed behind a barrier-held answer', async () => {
    await post(front, `/db/${DB}/history/insert`, { doc: { n: 1 } });
    await post(front, `/db/${DB}/history/insert`, { doc: { n: 2 } });

    const stream = sse(front, `/db/${DB}/history/watch?from=0`);
    expect(await stream.ready).toBe(200);
    expect((await stream.next()).event).toBe('watching');
    const a = await stream.next(20000);
    const b = await stream.next(20000);
    expect([a, b].map((e) => e.data.fullDocument.n)).toEqual([1, 2]);
    expect(Number(a.id)).toBe(a.data.index);
    stream.close();
  }, 60000);
});

/**
 * Resumable SSE (roadmap step 6 over HTTP; docs/http-front.md is the
 * contract): the log index rides as each event's SSE `id:`, so
 * SSE's own reconnect machinery -- Last-Event-ID -- is the resume
 * protocol, and a server-side overflow with a token is a page boundary
 * the front end crosses without the consumer ever seeing it.
 *
 * A group of one (`--raft 1`): the log without the election mechanics.
 * The cluster suite above already proves the leader-following half.
 */
describe.skipIf(!(REQUIRED || have(NATIVE)))('http front end: resumable SSE (--raft 1, native)', () => {
  const DB = 'ssedb';
  const port = nextPort();
  let proc, front;

  beforeAll(async () => {
    ({ proc } = await startServer(ENGINES[0], port, ['--raft', '1']));
    front = new DbHttpFront(`127.0.0.1:${port}`, { listenPort: 0 });
    await front.start();
    return async () => {
      await front.stop();
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
    };
  }, 60000);

  it('replays history from ?from=0, each event carrying its log index as the SSE id', async () => {
    for (const n of [1, 2, 3]) {
      expect((await post(front, `/db/${DB}/hist/insert`, { doc: { n } })).status).toBe(200);
    }
    const stream = sse(front, `/db/${DB}/hist/watch?from=0`);
    expect(await stream.ready).toBe(200);
    expect((await stream.next()).event).toBe('watching');

    const seen = [];
    for (let i = 0; i < 3; i++) seen.push(await stream.next());
    expect(seen.map((e) => e.data.fullDocument.n)).toEqual([1, 2, 3]);
    // The SSE id IS the event's log index -- what Last-Event-ID sends back.
    for (const e of seen) expect(Number(e.id)).toBe(e.data.index);
    expect(seen[0].data.index).toBeLessThan(seen[2].data.index);
    stream.close();
  });

  it('resumes from Last-Event-ID: only what came after it, exactly as a reconnect would', async () => {
    const all = sse(front, `/db/${DB}/hist/watch?from=0`);
    await all.ready;
    await all.next();                       // watching
    const first = await all.next();         // n: 1
    all.close();

    // The header wins over the URL, because a reconnecting consumer
    // knows better than its original URL where it actually got to.
    const rest = sse(front, `/db/${DB}/hist/watch?from=0`, { 'last-event-id': first.id });
    expect(await rest.ready).toBe(200);
    await rest.next();                      // watching
    expect((await rest.next()).data.fullDocument.n).toBe(2);
    expect((await rest.next()).data.fullDocument.n).toBe(3);
    rest.close();
  });

  it('pages a 300-event history through the bounded queue without the consumer noticing', async () => {
    for (const half of [0, 150]) {
      const docs = Array.from({ length: 150 }, (_, i) => ({ n: half + i }));
      expect((await post(front, `/db/${DB}/paged/insertMany`, { docs })).status).toBe(200);
    }
    // The server's stream queue holds 256 events; the replay overflows
    // at the boundary and the front end resumes from the overflow's
    // token -- the SSE consumer just sees 300 changes, in order.
    const stream = sse(front, `/db/${DB}/paged/watch?from=0`);
    expect(await stream.ready).toBe(200);
    expect((await stream.next()).event).toBe('watching');
    const seen = [];
    while (seen.length < 300) {
      const ev = await stream.next(20000);
      expect(ev.event).toBe('change');
      seen.push(ev.data.fullDocument.n);
    }
    expect(seen).toEqual(Array.from({ length: 300 }, (_, i) => i));
    stream.close();
  });

  it('refuses a token from the future with the wire\'s own code', async () => {
    const { port: p } = front.address();
    const res = await fetch(`http://127.0.0.1:${p}/db/${DB}/hist/watch?from=999999`);
    expect(res.status).toBe(400);
    expect((await res.json()).code).toBe(-69);
  });
});
