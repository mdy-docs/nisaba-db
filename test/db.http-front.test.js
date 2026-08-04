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

const NATIVE = 'wasm/lib/nisaba-server';
const WASIP2 = 'wasm/lib/nisaba-server-wasip2.wasm';
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
function sse(front, pathname) {
  const { port } = front.address();
  const events = [];
  const waiting = [];
  let req = null;
  const push = (ev) => {
    const w = waiting.shift();
    if (w) w(ev); else events.push(ev);
  };
  const ready = new Promise((resolve, reject) => {
    req = http.get(`http://127.0.0.1:${port}${pathname}`, (res) => {
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
    const res = await post(front, `/db/${DB}/events/insert`, { doc: { seq: 1 } });
    expect(res.status).toBe(200);
    expect(res.body.ok).toBe(true);

    // And so does the read that checks it -- reads are the leader's too.
    const count = await post(front, `/db/${DB}/events/count`, { filter: {} });
    expect(count.status).toBe(200);
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
});
