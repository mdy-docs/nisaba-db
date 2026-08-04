/**
 * The database server (docs/db-server.md): a process
 * with no JavaScript in it, serving binjson frames.
 *
 * What these prove that test/native/main.c cannot: the WIRING. The
 * request grammar is tested there, over buffers, with no transport --
 * that is the whole point of dbs_handle being a function. What is left
 * to check is that frames survive a pipe and a socket, and that a client
 * in another language, over another codec, reading files a THIRD
 * implementation wrote, gets the same answers. That last part is the
 * claim this repo rests on, and it cannot be checked from inside C.
 *
 * Both cases skip unless their artifact has been built --
 * ./wasm/build-server.sh --native and --wasip2 -- because `npm test`
 * does not build the server, the same way it does not build the WASM
 * module. CI builds both and then runs this.
 */
import { describe, it, expect, beforeAll } from 'vitest';
import { spawn, spawnSync } from 'node:child_process';
import net from 'node:net';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { ready, encode, decode } from '../wasm/nisaba-wasm.js';
import { connect, connectClient, ObjectId } from '../src/db.js';
import { NodeFSStorageProvider } from '../src/db-node.js';
import { connectServer, ServerError, WIRE_OPS, ChangeStreamOverflowError } from '../src/db-server-client.js';
import { BPlusTree, EntryLog, MemoryHandle } from '../wasm/nisaba-wasm.js';
import { RaftNode } from '../src/raft.js';
import { TcpRaftTransport } from '../src/raft-transport-tcp.js';
import { joinGroup, leaveGroup } from '../src/raft-host.js';

await ready();

const NATIVE = 'wasm/lib/nisaba-server';
const WASIP2 = 'wasm/lib/nisaba-server-wasip2.wasm';
const have = (p) => fs.existsSync(p);
const wasmtime = (() => {
  const r = spawnSync('sh', ['-c', 'command -v wasmtime'], { encoding: 'utf8' });
  return r.status === 0 ? r.stdout.trim() : null;
})();

/**
 * A frame is a binjson value that carries its own length: type byte, then
 * a u32 that counts everything AFTER those five bytes. So the reader
 * takes five, adds five, and waits for that many. No length prefix of our
 * own, and nothing to keep in sync with the encoder.
 */
function framer(onValue) {
  let buf = Buffer.alloc(0);
  return (chunk) => {
    buf = Buffer.concat([buf, chunk]);
    for (;;) {
      if (buf.length < 5) return;
      const total = buf.readUInt32LE(1) + 5;
      if (buf.length < total) return;
      onValue(decode(buf.subarray(0, total)));
      buf = buf.subarray(total);
    }
  };
}

/* The database every suite here works in. A server holds an INSTANCE --
 * one root directory, a subdirectory per database -- so a name is needed
 * on both sides, and this is it. */
const DB = 'main';

/** A root with one database in it, three people in that, written by the
 * JS implementation -- so the server under test is opening somebody
 * else's files, in somebody else's layout. */
async function seedDb(extra = 0) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-server-'));
  const root = new NodeFSStorageProvider(dir);
  const provider = await root.subProvider(DB);
  const db = await connect(provider);
  const users = await db.collection('users');
  await users.insertOne({ name: 'Ada', team: 'core' });
  await users.insertOne({ name: 'Grace', team: 'core' });
  await users.insertOne({ name: 'Alan', team: 'research' });
  for (let i = 0; i < extra; i++) {
    await users.insertOne({ name: `Extra ${i}`, team: 'bulk', n: i });
  }
  await db.close();
  // Release the advisory lock as well as the handles: one writer per
  // database directory is the rule the server relies on too, and the
  // provider holds it until asked, not until Db.close().
  await root.close();
  return dir;
}

/**
 * Queue of requests over one duplex pair, resolved in order -- plus the
 * one thing that is not an answer to any of them.
 *
 * Answers come back in request order, so pending resolvers are a queue
 * and no request ids are needed. Change events are the exception: they
 * answer nothing, so they are routed by SHAPE (an answer carries `ok`,
 * an event carries `stream`) into `call.events` rather than being handed
 * to whoever asked last. A client that skipped this would hand an event
 * to a caller expecting an answer, or -- as this helper used to --
 * silently drop it for want of anyone waiting.
 */
function client(write, onData) {
  const waiting = [];
  const events = [];
  onData(framer((value) => {
    if (value && typeof value === 'object' && value.ok === undefined &&
        typeof value.stream === 'number') {
      events.push(value);
      return;
    }
    const next = waiting.shift();
    if (next) next(value);
  }));
  /* Every request names its database, because the CONNECTION does not:
   * that is what lets one carry several, and it is what
   * db-server-client.js's scope() does underneath a Db handle. Named
   * here rather than in each test for the same reason. */
  const call = (req) => new Promise((resolve) => {
    waiting.push(resolve);
    write(Buffer.from(encode({ db: DB, ...req })));
  });
  call.events = events;
  return call;
}

/*
 * A server that only listens. It answers each request with the least
 * plausible thing that shape of request accepts, and keeps every request
 * it was sent -- so a test can assert what the CLIENT put on the wire
 * rather than what a database made of it.
 *
 * No engine, no artifacts, nothing to skip: the rule below is the
 * client's, and gating it on a built server would be gating it on
 * something it does not use.
 */
function recordingServer() {
  const requests = [];
  const reply = (req) => {
    const n = req.docs?.length ?? req.writes?.length ?? 1;
    const counts = {
      acknowledged: true, insertedCount: n, matchedCount: 0,
      modifiedCount: 0, deletedCount: 0, upsertedCount: 0, upsertedId: null
    };
    if (req.op === 'ping') return { ok: true, pong: true };
    if (req.op === 'insert') return { ok: true, result: counts };
    if (req.op === 'insertMany' || req.op === 'bulkWrite') {
      return { ok: true, result: counts, attempted: n, upserted: null, errors: null };
    }
    return { ok: true };
  };
  const server = net.createServer((sock) => {
    sock.on('data', framer((req) => {
      requests.push(req);
      sock.write(Buffer.from(encode(reply(req))));
    }));
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => resolve({
      port: server.address().port,
      requests,
      close: () => new Promise((r) => server.close(r))
    }));
  });
}

/* Every document an insert-shaped command carries, from any of the three
 * shapes one can arrive in. */
function documentsIn(req) {
  if (req.op === 'insert') return [req.doc];
  if (req.op === 'insertMany') return req.docs;
  if (req.op === 'bulkWrite') {
    return req.writes.filter((w) => w.insertOne).map((w) => w.insertOne.document);
  }
  return [];
}

describe('nisaba-server: the client mints an _id before it sends', () => {
  /*
   * C will not invent an _id -- that needs a clock and randomness, which
   * db.h keeps out of the engine deliberately, and under replication an
   * id invented at apply time would differ on every replica. So the
   * server REFUSES an insert whose document has none (-42), and this
   * side is what makes sure it never sees one.
   *
   * That guarantee currently rests on three separate `?? new ObjectId()`
   * expressions, one per path, with nothing asserting all three exist.
   * The suites below would catch a missing one -- they insert documents
   * without _ids -- but only as a refusal from a server about a field,
   * or (because `{_id: undefined, ...doc}` is a PRESENT key with an
   * unencodable value) as "Unsupported type: undefined" from the codec.
   * Both are a long way from "this client stopped minting", which is
   * what `via` below turns them into.
   */
  it('sends no insert-shaped command carrying a document without one', async () => {
    const server = await recordingServer();
    const db = (await connectServer(server.port, { keepAliveMs: 0 })).db(DB);
    const c = db.collection('users');

    /* A path that stops minting can fail in three places -- the codec,
     * the server, or the assertions below -- and only the last of those
     * would say which path it was. */
    const via = async (path, fn) => {
      try {
        return await fn();
      } catch (err) {
        throw new Error(`${path} did not mint an _id before sending: ${err.message}`);
      }
    };

    // Not one _id between them, by any route in.
    const one = await via('insertOne', () => c.insertOne({ name: 'Ada' }));
    const many = await via('insertMany',
      () => c.insertMany([{ name: 'Grace' }, { name: 'Alan' }]));
    const bulk = await via('bulkWrite', () => c.bulkWrite([
      { insertOne: { document: { name: 'Edsger' } } },
      { updateOne: { filter: { name: 'Nobody' }, update: { $set: { x: 1 } }, upsert: true } }
    ]));

    await db.close();
    await server.close();

    const inserts = server.requests.filter((r) => documentsIn(r).length > 0);
    expect(inserts.map((r) => r.op)).toEqual(['insert', 'insertMany', 'bulkWrite']);

    const sent = [];
    for (const req of inserts) {
      for (const doc of documentsIn(req)) {
        expect(doc, `${req.op} sent a document with no _id`).toHaveProperty('_id');
        expect(doc._id).toBeInstanceOf(ObjectId);
        sent.push(doc._id.toHexString());
      }
    }
    expect(sent).toHaveLength(4);
    // Distinct, which is the other half of minting: a counter that
    // handed out the same bytes twice would satisfy everything above.
    expect(new Set(sent).size).toBe(4);

    // And the caller is told the ids that actually went, rather than
    // being handed something the server would have had to agree to.
    expect(one.insertedId.toHexString()).toBe(sent[0]);
    expect(Object.values(many.insertedIds).map((i) => i.toHexString()))
      .toEqual([sent[1], sent[2]]);
    expect(bulk.insertedIds[0].toHexString()).toBe(sent[3]);
    expect(bulk.insertedIds[1]).toBeUndefined();   // the upsert is not an insert
  });

  it('leaves an _id the caller chose exactly as it was', async () => {
    // Minting is a fallback, not a policy: a document that names its own
    // identity keeps it, and that is what makes an insert idempotent to
    // retry from the caller's side.
    const server = await recordingServer();
    const db = (await connectServer(server.port, { keepAliveMs: 0 })).db(DB);
    const mine = new ObjectId();
    await db.collection('users').insertOne({ _id: mine, name: 'Ada' });
    await db.close();
    await server.close();

    const [req] = server.requests;
    expect(req.doc._id.toHexString()).toBe(mine.toHexString());
    // `id` rides along for the writes that only discover they need one
    // after they have matched; for an insert it is the same bytes.
    expect(req.id.toHexString()).toBe(mine.toHexString());
  });
});

describe.skipIf(!have(NATIVE))('nisaba-server: frames over a pipe (native, --stdio)', () => {
  let dir, proc, call;

  beforeAll(async () => {
    dir = await seedDb();
    proc = spawn(path.resolve(NATIVE), ['--stdio'], { cwd: dir, stdio: ['pipe', 'pipe', 'pipe'] });
    call = client((b) => proc.stdin.write(b), (h) => proc.stdout.on('data', h));
    return () => { proc.kill(); };
  });

  it('answers count, find and a refusal, in order, over one pipe', async () => {
    expect(await call({ op: 'count', coll: 'users' })).toEqual({ ok: true, n: 3 });

    const found = await call({ op: 'find', coll: 'users', filter: { team: 'core' } });
    expect(found.ok).toBe(true);
    expect(found.docs.map(d => d.name).sort()).toEqual(['Ada', 'Grace']);

    // A refusal is a response: the connection survives it, and the next
    // request is answered normally.
    const bad = await call({ op: 'explodinate', coll: 'users' });
    expect(bad.ok).toBe(false);
    expect(bad.code).toBe(-41);
    expect(bad.msg).toMatch(/does not know/);

    expect(await call({ op: 'count', coll: 'users' })).toEqual({ ok: true, n: 3 });
  });

  it('delivers change events behind the answer that caused them', async () => {
    // --stdio has no poll loop -- it is one client by construction, and
    // the only thing that ever happens is a request -- so events go out
    // immediately after the answer to whatever produced them.
    //
    // Its own collection: this suite shares one server and one database,
    // and a test that writes into `users` moves the next one's counts.
    expect(await call({ op: 'watch', coll: 'watched' })).toMatchObject({ ok: true, stream: 1 });
    await call({ op: 'insert', coll: 'watched', doc: { _id: new ObjectId(), name: 'Watched' } });

    // The answer to the ping proves the event frame ahead of it did not
    // take the ping's place in the queue: they are told apart by shape,
    // not by position.
    expect(await call({ op: 'ping' })).toEqual({ ok: true, pong: true });
    expect(call.events).toHaveLength(1);
    const { stream, event } = call.events[0];
    expect(stream).toBe(1);
    expect(event.operationType).toBe('insert');
    expect(event.ns).toEqual({ coll: 'watched' });
    expect(event.fullDocument.name).toBe('Watched');

    expect(await call({ op: 'closeStream', stream: 1 })).toEqual({ ok: true, closed: true });
  });

  it('writes through the WAL grammar, and the JS side sees them afterwards', async () => {
    // The client generates the id, because generating one needs a clock
    // and db.h keeps clocks out of the engine deliberately.
    const res = await call({
      op: 'insert', coll: 'users',
      doc: { _id: new ObjectId(), name: 'Edsger', team: 'core' }
    });
    expect(res.ok).toBe(true);

    expect((await call({ op: 'count', coll: 'users' })).n).toBe(4);

    // The point of the whole exercise: a C process wrote it, and the
    // JavaScript implementation reads it back from the same files.
    proc.kill();
    await new Promise(r => proc.once('exit', r));
    const provider = new NodeFSStorageProvider(path.join(dir, DB));
    const db = await connect(provider);
    const users = await db.collection('users');
    expect(await users.countDocuments({})).toBe(4);
    expect((await users.find({ name: 'Edsger' }).toArray()).length).toBe(1);
    await db.close();
    await provider.close();
  });
});

describe.skipIf(!have(WASIP2) || !wasmtime)('nisaba-server: frames over TCP (wasm32-wasip2)', () => {
  let dir, proc, sock, call;
  const port = 18000 + (process.pid % 1000);

  beforeAll(async () => {
    dir = await seedDb();
    proc = spawn(wasmtime, [
      'run', '-S', 'inherit-network', '--dir', `${dir}::.`,
      path.resolve(WASIP2), '--port', String(port)
    ], { stdio: ['ignore', 'pipe', 'pipe'] });
    // The listener is up when it says so on stderr.
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('server did not start')), 20000);
      proc.stderr.on('data', (d) => {
        if (String(d).includes('serving')) { clearTimeout(t); resolve(); }
      });
    });
    sock = net.connect(port, '127.0.0.1');
    await new Promise(r => sock.once('connect', r));
    call = client((b) => sock.write(b), (h) => sock.on('data', h));
    return () => { sock.destroy(); proc.kill(); };
  });

  it('serves a database written by the JS implementation, with no JS in the process', async () => {
    expect(await call({ op: 'count', coll: 'users' })).toEqual({ ok: true, n: 3 });

    const core = await call({ op: 'find', coll: 'users', filter: { team: 'core' } });
    expect(core.docs.map(d => d.name).sort()).toEqual(['Ada', 'Grace']);

    const one = await call({ op: 'findOne', coll: 'users', filter: { name: 'Alan' } });
    expect(one.found).toBe(true);
    expect(one.doc.team).toBe('research');

    const values = await call({ op: 'distinct', coll: 'users', field: 'team' });
    expect(values.ok).toBe(true);
    expect([...values.values].sort()).toEqual(['core', 'research']);

    const gone = await call({ op: 'count', coll: 'ghosts' });
    expect(gone.ok).toBe(false);
    expect(gone.code).toBe(-37);

    // The one op with no collection in it, and none required.
    expect(await call({ op: 'ping' })).toEqual({ ok: true, pong: true });
  });

  it('deletes many and reports one summed result', async () => {
    const res = await call({ op: 'deleteMany', coll: 'users', filter: { team: 'core' } });
    expect(res.ok).toBe(true);
    expect(res.result.deletedCount).toBe(2);
    expect((await call({ op: 'count', coll: 'users' })).n).toBe(1);
  });
});

/**
 * The client (src/db-server-client.js) and the CLI driving it -- steps 4
 * and 5.
 *
 * The tests above speak the protocol by hand, which is what makes them a
 * check on the SERVER. These go through the shipped client instead,
 * because what is being checked here is different: that the same commands
 * a user types locally reach a database in another process, and that a
 * command the wire does not carry says so instead of failing as a
 * TypeError somewhere inside a proxy.
 *
 * BOTH ENGINES, not just the convenient one. Everything below runs twice:
 * against the native binary, which needs nothing but a cc, and against
 * the wasm32-wasip2 command under wasmtime, which is what actually gets
 * deployed. They are the same C over the same socket, so the second run
 * looks redundant right up until it is not -- preview1 and preview2
 * disagreed about directory rights and about writing to a reopened file,
 * both found by running the same code under a second host, both invisible
 * to a native build. A client the deployment target has never been driven
 * by is a client nobody has tested.
 *
 * ONE SERVER EACH: every suite here writes to the database it is given,
 * so they get their own seeded directory and their own port for the same
 * reason a test that needs a database makes its own. (It used to be
 * load-bearing for a different reason -- the server served one connection
 * at a time, and a suite holding a connection open hung the next suite's
 * CLI subprocesses in the listen backlog. That is what the last suite
 * below now tests is over.)
 */
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

/* Distinct ports, allocated as suites are declared: two servers are alive
 * at once whenever vitest's teardown of one overlaps the setup of the
 * next, and a bound port is the one resource these tests cannot make a
 * fresh copy of. 18000 belongs to the protocol suite above. */
let portSlot = 1;
const nextPort = () => 18000 + (portSlot++) * 1000 + (process.pid % 1000);

async function startServer(engine, port, extra = [], docs = 0, reuse = null) {
  // docs < 0: an EMPTY directory -- no catalog, no collection, nothing.
  // The server makes the database itself, which is the whole point of
  // the suite that asks for it.
  //
  // `reuse` is the other case, and the one a replica needs: START OVER
  // THE SAME FILES. A restart onto a fresh directory proves nothing
  // about recovery -- it is a first boot with extra steps.
  const dir = reuse ?? (docs < 0
    ? fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-empty-'))
    : await seedDb(docs));
  const [cmd, args, opts] = engine.argv(dir, port, extra);
  const proc = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
  // The directory comes back too: a test that wants to read the files
  // afterwards -- with the JS implementation, once the server is gone --
  // needs to know which ones.
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${engine.name} server did not start`)), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve({ proc, dir }); }
    });
  });
}

/*
 * A skip that is loud where it matters. `npm test` does not build the
 * server, so a developer who has not run ./wasm/build-server.sh should
 * see these skip rather than fail -- but CI builds both artifacts and
 * installs wasmtime, and a suite that quietly stops covering the thing it
 * names is worse than no suite. NISABA_SERVER_TESTS=required (set in
 * .github/workflows/ci.yml) turns a skip there into a failure here.
 */
const REQUIRED = process.env.NISABA_SERVER_TESTS === 'required';

/*
 * A directory that IS a database, handed to a server that now holds a
 * directory OF them. Serving it as a root would open an instance with no
 * databases in it, standing on top of somebody's data and reporting
 * nothing wrong -- so it says what it found and what to do instead.
 */
/*
 * --order is a CREATION parameter, not something a reader has to be told.
 *
 * The tree records its own order in its metadata and bpt_open reads it
 * back (meta_apply), so `s->order` in db_session.c reaches bpt_create
 * and rtree_create and nothing else. docs/db-server.md used to say the
 * opposite -- "open a tree with the wrong one and its pages read as
 * nonsense" -- which would have had operators carrying a number around
 * forever to avoid a corruption that cannot happen.
 *
 * So: make the files with one order, read them with another, and with a
 * different implementation that was never told either.
 */
describe.each(ENGINES.filter((e) => e.ready()))(
  'nisaba-server: --order is for files it CREATES ($name)', (engine) => {
    it('reads back files made with a different order, and so does the JS engine',
       async () => {
      const port = nextPort();
      // 8, which is nothing like the default 32: if the reader had to be
      // told, this would not survive the restart.
      const first = await startServer(engine, port, ['--order', '8'], -1);
      {
        const db = (await connectServer(port)).db(DB);
        await db.collection('users').insertMany(
          Array.from({ length: 50 }, (_, i) => ({ n: i, name: `u${i}` })));
        expect(await db.collection('users').countDocuments({})).toBe(50);
        await db.close();
      }
      first.proc.kill();
      await new Promise((r) => first.proc.once('exit', r));

      // Again, on the same files, with NO --order at all.
      const again = await startServer(engine, port, [], -1, first.dir);
      try {
        const db = (await connectServer(port)).db(DB);
        expect(await db.collection('users').countDocuments({})).toBe(50);
        expect((await db.collection('users').find({ n: 7 }).toArray())[0].name)
          .toBe('u7');
        await db.close();
      } finally {
        again.proc.kill();
        await new Promise((r) => again.proc.once('exit', r));
      }

      // And a third implementation, told nothing about it either.
      const provider = new NodeFSStorageProvider(path.join(first.dir, DB));
      const jsDb = await connect(provider);
      const users = await jsDb.collection('users');
      expect(await users.countDocuments({})).toBe(50);
      await jsDb.close();
      await provider.close();
    }, 60000);
  });

describe.each(ENGINES.filter((e) => e.ready()))(
  'nisaba-server: a root that is a database ($name)', (engine) => {
    it('refuses to serve it, and says how to move it', async () => {
      const dir = await seedDb();
      // seedDb builds root/main; hand the server the DATABASE.
      const [cmd, args, opts] = engine.argv(path.join(dir, DB), nextPort(), []);
      const proc = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
      let err = '';
      proc.stderr.on('data', (d) => { err += String(d); });
      const code = await new Promise((r) => proc.once('exit', r));
      expect(code).not.toBe(0);
      expect(err).toMatch(/is a DATABASE, not a directory of them/);
      expect(err).toMatch(/subdirectory named for the database/);
    });
  });

describe.runIf(REQUIRED)('nisaba-server: the artifacts CI promises', () => {
  it.each(ENGINES)('$name is built and runnable', (engine) => {
    expect(engine.ready()).toBe(true);
  });
});

for (const engine of ENGINES) {
  const enabled = engine.ready();

  /*
   * docs/steps/server-as-replica.md. With --raft the server stops
   * applying a write where it lands: it plans it, proposes it to a log,
   * and applies it only once the entry has committed -- the same path a
   * write takes on a member of a real cluster. This group has one
   * member, so the quorum is itself and "committed" is immediate; what
   * that removes from the test is waiting, not steps.
   *
   * The point being proved is that a client cannot tell. Every answer
   * here is the answer the unreplicated server gives, because the
   * response is built by the same code from the same apply results.
   */
  describe.skipIf(!enabled)(`nisaba-server: replicated writes (${engine.name})`, () => {
    let proc, db, dir;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc, dir } = await startServer(engine, port, ['--raft', '1']));
      db = (await connectServer(port)).db(DB);
      return async () => { await db.close(); proc.kill(); };
    });

    it('answers a write only after it has been through the log', async () => {
      const users = db.collection('users');
      expect(await users.countDocuments({})).toBe(3);

      const { insertedId } = await users.insertOne({ name: 'Edsger', team: 'core' });
      expect(insertedId).toBeInstanceOf(ObjectId);
      expect((await users.findOne({ name: 'Edsger' })).team).toBe('core');
      expect(await users.countDocuments({})).toBe(4);

      // Applied ONCE. The pump performs every committed command, and the
      // request that proposed it must not perform it again -- on a
      // leader that is the same command twice, and the second insert
      // comes back a duplicate of the first.
      expect(await users.countDocuments({ name: 'Edsger' })).toBe(1);
    });

    it('carries DDL through the log too, and applies that once as well', async () => {
      const users = db.collection('users');
      // createIndex applied twice would answer "that index already
      // exists" to the client that just created it.
      expect(await users.createIndex({ team: 1 })).toBe('team_1');
      expect((await users.listIndexes()).map(i => i.name)).toContain('team_1');
    });

    it('a list of writes takes one trip to the log per operation', async () => {
      const users = db.collection('users');
      const id = new ObjectId();
      // The second operation can only match if the first has landed --
      // which on a replica means committed and applied, not merely
      // planned. Same semantics as the in-process bulkWrite.
      const r = await users.bulkWrite([
        { insertOne: { document: { _id: id, name: 'Barbara', team: 'core' } } },
        { updateOne: { filter: { _id: id }, update: { $set: { onCall: true } } } }
      ]);
      expect(r.insertedCount).toBe(1);
      expect(r.matchedCount).toBe(1);
      expect(r.modifiedCount).toBe(1);
      expect((await users.findOne({ _id: id })).onCall).toBe(true);
    });

    it('reads are answered without touching the log at all', async () => {
      const users = db.collection('users');
      expect(await users.distinct('team')).toEqual(expect.arrayContaining(['core', 'research']));
      expect((await users.find({ team: 'core' }, { sort: { name: 1 }, limit: 1 }).toArray())
        .map(d => d.name)).toEqual(['Ada']);
    });

    it('replays nothing it already applied, and the JS engine reads it the same way', async () => {
      const users = db.collection('users');
      const before = await users.countDocuments({});
      const edsgers = await users.countDocuments({ name: 'Edsger' });
      await db.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      // Restarted over ITS OWN log and ITS OWN files, which is the only
      // arrangement that tests anything: the replay floor is the
      // DATABASE's applied index, and a server that read it as zero
      // replays a prefix it has already applied. That is not a
      // survivable mistake -- the collection refuses an applied index it
      // has already passed, which is not a deterministic failure, so the
      // replica halts on the way up rather than answering.
      const restarted = await startServer(engine, port + 1, ['--raft', '1'], 0, dir);
      const back = (await connectServer(port + 1)).db(DB);
      const backUsers = back.collection('users');
      expect(await backUsers.countDocuments({})).toBe(before);
      expect(await backUsers.countDocuments({ name: 'Edsger' })).toBe(edsgers);
      expect((await backUsers.listIndexes()).map(i => i.name)).toContain('team_1');
      await back.close();
      restarted.proc.kill();
      await new Promise(r => restarted.proc.once('exit', r));

      const provider = new NodeFSStorageProvider(path.join(dir, DB));
      const jsDb = await connect(provider);
      const jsUsers = await jsDb.collection('users');
      expect(await jsUsers.countDocuments({})).toBe(before);
      expect((await jsUsers.findOne({ name: 'Edsger' })).team).toBe('core');
      expect((await jsUsers.listIndexes()).map(i => i.name)).toContain('team_1');
      await jsDb.close();
      await provider.close();
    });
  });

  /*
   * docs/steps/server-as-replica.md, the last of it: three processes, no
   * JavaScript in any of them, and a peer transport between them.
   *
   * This is the suite that could not be written before, and the reason
   * is worth stating: everything up to here could be checked with one
   * process, because one process is a whole replica minus other
   * replicas. Leadership, replication, failover and catch-up are the
   * four things that only exist between processes, and each of them has
   * a failure mode that a group of one cannot have.
   *
   * A JavaScript test driving C processes is not a contradiction. It is
   * a CLIENT, which is what the wire is for.
   */
  describe.skipIf(!enabled)(`nisaba-server: a three-process cluster (${engine.name})`, () => {
    const base = nextPort();
    const MEMBERS = [1, 2, 3].map((id) => ({
      id,
      port: base + id - 1,
      raftPort: base + 10 + id - 1
    }));
    const argsFor = (m) => [
      '--raft', String(m.id), '--raft-port', String(m.raftPort),
      ...MEMBERS.filter((o) => o.id !== m.id)
        .flatMap((o) => ['--peer', `${o.id}@127.0.0.1:${o.raftPort}`])
    ];
    let nodes = [];

    /* Each member gets its own EMPTY directory. A cluster that had to be
     * seeded from identical files would be hiding the thing being
     * tested: three databases become one because the log made them, not
     * because they started out the same. */
    const boot = async (m) => {
      const { proc, dir } = await startServer(engine, m.port, argsFor(m), -1, m.dir);
      m.proc = proc;
      m.dir = dir;
      m.alive = true;
      return m;
    };
    const stop = async (m) => {
      if (!m.alive) return;
      m.alive = false;
      m.proc.kill();
      await new Promise((r) => m.proc.once('exit', r));
    };

    /*
     * Whichever member takes it. Only the leader will, and a follower
     * says so rather than forwarding -- so finding the leader IS the
     * retry loop, and a client that keeps a hint is doing exactly this
     * with one fewer attempt.
     */
    const write = async (name, tries = 100) => {
      let last = null;
      for (let i = 0; i < tries; i++) {
        for (const m of nodes.filter((n) => n.alive)) {
          let db = null;
          try {
            db = (await connectServer(m.port)).db(DB);
            await db.collection('users').insertOne({ name });
            await db.close();
            return m;
          } catch (err) {
            last = err;
            try { await db?.close(); } catch { /* it is already gone */ }
          }
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      throw new Error(`no member took the write "${name}": ${last?.message}`);
    };

    /*
     * What the cluster holds, and how far each member has got.
     *
     * ONLY THE LEADER MAY BE READ. Reads are linearizable and the leader
     * alone serves them
     * (docs/replicaton-roadmap.md, the step 6 decision), so polling
     * every member for its documents -- which is what this used to do --
     * now gets -63 from every follower. What a follower will still
     * answer is `ping`, and that is where it says what it is and how far
     * its apply pump has got.
     *
     * So convergence is checked as: the LEADER's documents are what was
     * expected, and every live member has applied at least as much of
     * the log as the leader had when it was asked. That is a stronger
     * claim than the old one, not a weaker substitute -- it is the log
     * index rather than a re-derived read -- and the file-level checks
     * elsewhere in this file are what tie a log prefix back to bytes on
     * disk.
     */
    const statusOf = async (m) => {
      const c = await connectServer(m.port);
      try { return await c.ping(); } finally { await c.close(); }
    };

    const namesOn = async (m) => {
      const db = (await connectServer(m.port)).db(DB);
      try {
        return (await db.collection('users').find({}, { sort: { name: 1 } }).toArray())
          .map((d) => d.name);
      } catch (err) {
        /* A leader that has applied nothing has no `users` at all: the
         * collection is made by the first insert applying. */
        if (err.code === -37) return [];
        throw err;
      } finally { await db.close(); }
    };

    /*
     * One write, offered to every live member AT ONCE. Exactly one takes
     * it -- that is what having a leader means -- and the refusals come
     * back to be read.
     *
     * Concurrent rather than in turn, because leadership is ALLOWED to
     * move between two requests: a test that asked one member, then
     * another, would be asserting that it had not, and a member that had
     * since been elected would take the write and fail the test for
     * behaving correctly.
     */
    const offerToAll = async (name) => {
      const attempts = await Promise.allSettled(nodes.filter((n) => n.alive).map(async (m) => {
        const db = (await connectServer(m.port)).db(DB);
        try {
          await db.collection('users').insertOne({ name });
          return m.id;
        } finally { await db.close(); }
      }));
      return {
        took: attempts.filter((a) => a.status === 'fulfilled').map((a) => a.value),
        refusals: attempts.filter((a) => a.status === 'rejected').map((a) => a.reason)
      };
    };

    /* Followers are behind by their replication lag, always -- the
     * leader answers as soon as IT has applied, which is a heartbeat
     * before anyone else has. So agreement is waited for, and the wait
     * is the assertion. How LONG it took comes back, because for one of
     * these tests that is the assertion. */
    const agree = async (expected, withinMs = 20000) => {
      const started = Date.now();
      let seen = null;
      let lag = null;
      while (Date.now() - started < withinMs) {
        const live = nodes.filter((n) => n.alive);
        /* Concurrently, so the leader's number and the followers' are
         * from the same instant rather than one poll apart. */
        const stats = await Promise.all(live.map(async (m) =>
          [m, await statusOf(m).catch(() => null)]));
        const lead = stats.find(([, st]) => st?.role === 'leader');
        if (lead) {
          const at = lead[1].applied;
          seen = await namesOn(lead[0]).catch(() => null);
          lag = stats.map(([m, st]) => `${m.id}:${st ? st.applied : '?'}`);
          if (JSON.stringify(seen) === JSON.stringify(expected) &&
              stats.every(([, st]) => st && st.applied >= at)) {
            return Date.now() - started;
          }
        }
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error(`members never agreed on ${JSON.stringify(expected)}: ` +
                      `leader held ${JSON.stringify(seen)}, applied ${lag}`);
    };

    beforeAll(async () => {
      nodes = [];
      for (const m of MEMBERS) nodes.push(await boot(m));
      return async () => { for (const m of nodes) await stop(m); };
    });

    it('elects one leader, and only the leader takes a write', async () => {
      const leader = await write('alpha');
      expect(MEMBERS.map((m) => m.id)).toContain(leader.id);
      await agree(['alpha']);

      // One leader, not one per member: offered the same write at the
      // same moment, exactly one of the three takes it.
      const { took, refusals } = await offerToAll('contested');
      expect(took.length).toBe(1);
      expect(refusals.length).toBe(2);
      for (const r of refusals) expect(r.code).toBe(-63);
      await agree(['alpha', 'contested']);
    });

    it('a follower refusing a write says who leads, and where', async () => {
      await write('beta');
      await agree(['alpha', 'beta', 'contested']);

      const { took, refusals } = await offerToAll('redirected');
      expect(took.length).toBe(1);
      expect(refusals.length).toBe(2);
      const refusal = refusals[0];
      expect(refusal.code).toBe(-63);

      // It names a real member and carries THAT member's real peer
      // address. An id alone would send a caller back to whichever
      // member it just asked; the address is what makes a refusal
      // something a client can act on, which is what the HTTP front end
      // will do with it.
      const named = MEMBERS.find((m) => m.id === refusal.leaderId);
      expect(named).toBeDefined();
      expect(refusal.leader).toMatchObject({
        id: named.id, host: '127.0.0.1', port: named.raftPort
      });

      // And it named the member that actually took the write -- the two
      // happened at once, so this is the redirect being RIGHT rather
      // than merely populated.
      expect(took[0]).toBe(named.id);
      await agree(['alpha', 'beta', 'contested', 'redirected']);
    });

    /*
     * The step 6 read decision (docs/replicaton-roadmap.md): every
     * read is linearizable and the leader alone serves one.
     *
     * A follower is behind by at least a round trip and cannot tell by
     * how much, so an answer from it is staleness presented as
     * authority. It refuses with the same code and the same address a
     * write refusal carries, because a client acts on both the same way.
     */
    it('refuses a READ on a follower, exactly as it refuses a write', async () => {
      await agree(['alpha', 'beta', 'contested', 'redirected']);
      const live = nodes.filter((n) => n.alive);
      const stats = await Promise.all(live.map(async (m) => [m, await statusOf(m)]));

      const leaders = stats.filter(([, s]) => s.role === 'leader');
      expect(leaders.length).toBe(1);
      const followers = stats.filter(([, s]) => s.role !== 'leader');
      expect(followers.length).toBe(2);

      for (const [m, s] of followers) {
        const c = await connectServer(m.port);
        try {
          await expect(c.db(DB).collection('users').countDocuments({}))
            .rejects.toMatchObject({ code: -63, leaderId: leaders[0][1].leaderId });
          /* The instance's own ops are classified too, and by the same
           * table: listDatabases reads, dropDatabase writes. A follower
           * refuses both -- which for dropDatabase matters more than it
           * looks, because it is not replicated (docs/steps/README.md)
           * and a follower performing one would silently diverge. */
          await expect(c.listDatabases()).rejects.toMatchObject({ code: -63 });
          await expect(c.dropDatabase(DB)).rejects.toMatchObject({ code: -63 });
        } finally { await c.close(); }
        /* And it still answers the one thing that touches nothing --
         * which is how anybody watches it replicate at all now. */
        expect(s).toMatchObject({ pong: true, role: 'follower' });
        expect(s.applied).toBeGreaterThan(0);
      }

      // The leader does serve it, and serves the truth.
      expect(await namesOn(leaders[0][0]))
        .toEqual(['alpha', 'beta', 'contested', 'redirected']);
    });

    /*
     * ...and the other half of linearizable: a LEADER that cannot prove
     * it still leads refuses too.
     *
     * Raft does not depose a leader that has lost its quorum -- it
     * simply stops being able to commit -- so without a barrier this
     * member would go on answering reads from a log a newer leader may
     * already have moved past. -66 rather than -63 because it is a
     * different operator problem: not "ask somebody else", but "there
     * may be nobody to ask".
     */
    it('refuses a read on a leader whose quorum has gone quiet', async () => {
      const live = nodes.filter((n) => n.alive);
      const stats = await Promise.all(live.map(async (m) => [m, await statusOf(m)]));
      const leader = stats.find(([, s]) => s.role === 'leader')[0];
      const others = live.filter((m) => m.id !== leader.id);
      expect(others.length).toBe(2);

      /* The quorum comes back whatever this test concludes. A failure
       * here used to leave one member standing and every test after it
       * waiting for a quorum that was never coming -- which reads as a
       * hang rather than as the assertion that failed. */
      try {
        for (const m of others) await stop(m);

        // Still the leader by its own reckoning -- that is the point.
        expect((await statusOf(leader)).role).toBe('leader');

        const db = (await connectServer(leader.port)).db(DB);
        try {
          await expect(db.collection('users').countDocuments({}))
            .rejects.toMatchObject({ code: -66 });
        } finally { await db.close(); }
      } finally {
        for (const m of others) if (!m.alive) await boot(m);
      }

      // And it clears the moment a quorum answers again: no restart, no
      // election, just a member coming back. It takes a redial and a
      // heartbeat, so the read is retried rather than assumed -- what is
      // being checked is that it starts working, not how fast.
      let back = null;
      const until = Date.now() + 15000;
      while (Date.now() < until && !back) {
        back = await namesOn(leader).catch(() => null);
        if (!back) await new Promise((r) => setTimeout(r, 100));
      }
      expect(back).toEqual(['alpha', 'beta', 'contested', 'redirected']);
      await agree(['alpha', 'beta', 'contested', 'redirected']);
    }, 60000);

    it('survives the leader being killed, and takes writes again', async () => {
      const leader = await write('gamma');
      await agree(['alpha', 'beta', 'contested', 'gamma', 'redirected']);
      await stop(leader);

      // Two of three is still a quorum. The new leader is one of the
      // survivors, and its log already has everything the old one
      // committed -- that is what the election rules buy.
      const next = await write('delta');
      expect(next.id).not.toBe(leader.id);
      await agree(['alpha', 'beta', 'contested', 'delta', 'gamma', 'redirected']);
    });

    it('catches a restarted member up on everything it missed', async () => {
      const dead = nodes.find((m) => !m.alive);
      expect(dead).toBeDefined();
      // Its own files, its own log: it comes back where it left off and
      // is caught up from there, rather than starting over.
      await boot(dead);
      const caughtUpIn = await agree(['alpha', 'beta', 'contested', 'delta', 'gamma', 'redirected']);

      /*
       * PROMPTLY, and that is the assertion rather than a nicety. The
       * honest path here is a heartbeat, a redial and a rewind -- a few
       * hundred milliseconds. The dishonest one is the transport's
       * five-second request timeout eventually noticing that a dial
       * nobody diagnosed had stranded a correlation id, which is what
       * happens when a refused connect is only looked for on POLLOUT.
       * Both end with the member caught up; only the clock tells them
       * apart, so the clock is what is checked.
       */
      expect(caughtUpIn).toBeLessThan(2500);

      // And it is a full member again, not merely a reader: a write
      // taken now still reaches it.
      await write('epsilon');
      await agree(['alpha', 'beta', 'contested', 'delta', 'epsilon', 'gamma', 'redirected']);
    }, 30000);
  });

  /*
   * ONE CLUSTER, TWO HOSTS: two C members and one member running in
   * Node.
   *
   * What is and is not being proved matters here. The Node member's
   * RaftNode wraps RaftCore -- the SAME C raft_node, compiled to WASM --
   * so this is not two implementations of Raft agreeing. It is one
   * implementation with two hosts around it, and the only thing that
   * differs between them is the transport: server/peers.c on one side,
   * src/raft-transport-tcp.js on the other. So this suite tests the
   * FRAMING, which is exactly where they had drifted apart -- C spliced
   * the message into `env` as a nested object where JavaScript, which
   * hands its transport already-encoded bytes, puts binjson BINARY.
   * C-to-C agreed with itself and could not talk to a Node member at
   * all.
   *
   * Across the three suites every combination is covered: C writes
   * `env` and C reads it (the three-process cluster above), JavaScript
   * writes it and JavaScript reads it (test/raft-tcp.test.js, a real
   * 3-node cluster over real sockets), and here C writes `env` for
   * JavaScript to read while JavaScript writes `value` for C to read.
   *
   * The Node member is a full VOTER, which is the part that carries the
   * proof: with one C member stopped, nothing commits without it.
   *
   * It is also entitled to WIN an election, and that is a problem the
   * suite has to solve rather than prevent -- a leader with no database
   * is a leader no client can send a write to. Not ticking it does not
   * work and it is worth writing down why: a follower campaigns only
   * from tick(), but its clock advances only from tick() too, so leader
   * stickiness never expires and it refuses every vote forever. (It did.
   * The surviving C member could not get elected at all.) So it ticks
   * like any member, and when it wins it hands leadership to a C member
   * with TimeoutNow -- section 3.10, and also the one message this suite
   * sends from the Node member TO C.
   */
  describe.skipIf(!enabled)(`nisaba-server: a C cluster with a Node member (${engine.name})`, () => {
    const base = nextPort();
    const C_MEMBERS = [1, 2].map((id) => ({
      id, port: base + id - 1, raftPort: base + 10 + id - 1
    }));
    const NODE = { id: 3, raftPort: base + 12 };
    const RECORDS = [
      ...C_MEMBERS.map((m) => ({ id: m.id, host: '127.0.0.1', port: m.raftPort })),
      { id: NODE.id, host: '127.0.0.1', port: NODE.raftPort }
    ];
    const argsFor = (m) => [
      '--raft', String(m.id), '--raft-port', String(m.raftPort),
      ...RECORDS.filter((r) => r.id !== m.id)
        .flatMap((r) => ['--peer', `${r.id}@${r.host}:${r.port}`])
    ];

    /* Records what the log gave it and nothing else. The C members
     * replicate DATABASE commands, and this member has no database to
     * apply them to -- what is being watched is that the entries arrive
     * at all, in order, through a transport written in the other
     * language. */
    class RecordingMachine {
      constructor() { this.applied = 0; this.count = 0; this.bytes = 0; }
      appliedIndex() { return this.applied; }
      async apply(entry) {
        this.count++;
        this.bytes += entry.payload.length;
        this.applied = entry.index;
      }
    }

    let cNodes = [];
    let node = null, transport = null, log = null, machine = null, ticker = null;

    const stopC = async (m) => {
      if (!m.alive) return;
      m.alive = false;
      m.proc.kill();
      await new Promise((r) => m.proc.once('exit', r));
    };

    const until = async (pred, ms = 15000) => {
      const started = Date.now();
      while (Date.now() - started < ms) {
        if (await pred()) return Date.now() - started;
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error('condition never held');
    };

    /* Leadership somewhere a client can reach. */
    const ensureCLeads = async () => {
      await until(() => node.role === 'leader' ||
                        cNodes.some((m) => m.alive && m.id === node.leaderId));
      if (node.role !== 'leader') return;
      await node.transferLeadership(cNodes.find((m) => m.alive).id);
      await until(() => node.role !== 'leader');
    };

    const writeVia = async (name, tries = 100) => {
      await ensureCLeads();
      let last = null;
      for (let i = 0; i < tries; i++) {
        for (const m of cNodes.filter((n) => n.alive)) {
          let db = null;
          try {
            db = (await connectServer(m.port)).db(DB);
            await db.collection('users').insertOne({ name });
            await db.close();
            return m;
          } catch (err) {
            last = err;
            try { await db?.close(); } catch { /* already gone */ }
          }
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      throw new Error(`no C member took the write "${name}": ${last?.message}`);
    };

    beforeAll(async () => {
      cNodes = [];
      for (const m of C_MEMBERS) {
        const { proc, dir } = await startServer(engine, m.port, argsFor(m), -1);
        cNodes.push(Object.assign(m, { proc, dir, alive: true }));
      }

      log = new EntryLog(new MemoryHandle());
      await log.open();
      machine = new RecordingMachine();
      transport = new TcpRaftTransport({
        listenPort: NODE.raftPort,
        peers: Object.fromEntries(C_MEMBERS.map((m) => [m.id, { host: '127.0.0.1', port: m.raftPort }])),
        // `env` arrives decoded. It must be BYTES: handleMessage hands
        // them straight to C without looking inside, which is the whole
        // reason the field is BINARY rather than a nested object.
        onMessage: (env) => node.handleMessage(env),
        requestTimeoutMs: 1000
      });
      await transport.start();
      node = new RaftNode({ id: NODE.id, peers: RECORDS, log, stateMachine: machine, transport });
      await node.start(Date.now());
      // The clock, on the same interval RaftGroupHost uses. Without it
      // nothing about this member's timers is true -- including, and
      // least obviously, when it may grant a vote.
      ticker = setInterval(() => node.tick(Date.now()), 20);
      ticker.unref?.();

      return async () => {
        clearInterval(ticker);
        for (const m of cNodes) await stopC(m);
        await node.stop();
        await transport.stop();
        await log.close();
      };
    });

    it('a C leader replicates its log into the Node member', async () => {
      await writeVia('from-c');
      // Entries, in order, decoded by a host that never saw a byte of
      // this frame written. Two at least: the leader's own NOOP and the
      // insert -- the NOOP is not applied, so `count` is the documents.
      await until(() => log.lastIndex >= 2);
      await until(() => machine.count >= 1 && machine.bytes > 0);
      expect(machine.applied).toBe(log.lastIndex);
    });

    it('and its vote and its acks are what the quorum needs', async () => {
      const before = log.lastIndex;
      // Three voters, so a quorum is two. With one C member gone there
      // is exactly one C member left: it cannot elect itself and cannot
      // commit anything without the member running in Node.
      await stopC(cNodes.find((m) => m.alive));
      expect(cNodes.filter((m) => m.alive).length).toBe(1);

      const took = await writeVia('needs-the-node-member');
      expect(took.id).toBeDefined();
      // Committed means a quorum matched, and the quorum is this member.
      await until(() => log.lastIndex > before);
      expect(node.leaderId).toBe(took.id);

      // And the surviving C member really has it, read back over the
      // wire like any other client.
      const db = (await connectServer(took.port)).db(DB);
      try {
        expect((await db.collection('users').find({}, { sort: { name: 1 } }).toArray())
          .map((d) => d.name)).toEqual(['from-c', 'needs-the-node-member']);
      } finally { await db.close(); }
    }, 30000);

    it('leaves files the JS engine reads, after a cluster of two languages wrote them', async () => {
      const survivor = cNodes.find((m) => m.alive);
      await stopC(survivor);
      // The claim this repository rests on, through a mixed cluster: a
      // third implementation opens what the others agreed on.
      const provider = new NodeFSStorageProvider(path.join(survivor.dir, DB));
      const jsDb = await connect(provider);
      const users = await jsDb.collection('users');
      expect((await users.find({}, { sort: { name: 1 } }).toArray()).map((d) => d.name))
        .toEqual(['from-c', 'needs-the-node-member']);
      await jsDb.close();
      await provider.close();
    });
  });

  /*
   * A member set that is the LOG's rather than argv's.
   *
   * The three things being checked, and they are different things. That
   * a process knowing one ADDRESS can be admitted -- it has no ids, and
   * the cluster's shape is not on its command line. That what it is
   * admitted as is a LEARNER, promoted only once its match index proves
   * it caught up, so admitting it never thins the failure margin between
   * the moment it arrives and the moment it can help. And that once the
   * log carries a CONFIG entry, argv has stopped mattering: a restart
   * with no --peer list at all rejoins nothing and is simply still a
   * member.
   *
   * The joiner is deliberately given ONE seed and it is not necessarily
   * the leader, because following a redirect is most of what a seed loop
   * is for.
   */
  describe.skipIf(!enabled)(`nisaba-server: joining a cluster (${engine.name})`, () => {
    const base = nextPort();
    const SEEDS = [1, 2].map((id) => ({
      id, port: base + id - 1, raftPort: base + 10 + id - 1
    }));
    const JOINER = { id: 3, port: base + 2, raftPort: base + 12 };
    const ALL = [...SEEDS, JOINER];

    const seedArgs = (m) => [
      '--raft', String(m.id), '--raft-port', String(m.raftPort),
      ...SEEDS.filter((o) => o.id !== m.id)
        .flatMap((o) => ['--peer', `${o.id}@127.0.0.1:${o.raftPort}`])
    ];
    let nodes = [];

    const boot = async (m, extra) => {
      const { proc, dir } = await startServer(engine, m.port, extra, -1, m.dir);
      m.proc = proc;
      m.dir = dir;
      m.alive = true;
      /* Everything it says, kept: some of what this suite checks is
       * whether a member had to complain. */
      m.err = m.err ?? '';
      proc.stderr.on('data', (d) => { m.err += String(d); });
      if (!nodes.includes(m)) nodes.push(m);
      return m;
    };
    const stop = async (m) => {
      if (!m.alive) return;
      m.alive = false;
      m.proc.kill();
      await new Promise((r) => m.proc.once('exit', r));
    };

    /* Run the server binary as a one-shot COMMAND rather than a server:
     * --leave asks and exits, and it needs no directory of its own. */
    const runOnce = (args, dir) => new Promise((resolve) => {
      const [cmd, argv, opts] = engine.argv(dir ?? os.tmpdir(), 0, args);
      // --port is meaningless here and never bound; drop it so a stray
      // 0 cannot be mistaken for a request to listen.
      const cleaned = argv.filter((a, i) => a !== '--port' && argv[i - 1] !== '--port');
      const proc = spawn(cmd, cleaned, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
      let err = '';
      proc.stderr.on('data', (d) => { err += String(d); });
      proc.once('exit', (code) => resolve({ code, err }));
    });

    const write = async (name, tries = 100) => {
      let last = null;
      for (let i = 0; i < tries; i++) {
        for (const m of nodes.filter((n) => n.alive)) {
          let db = null;
          try {
            db = (await connectServer(m.port)).db(DB);
            await db.collection('users').insertOne({ name });
            await db.close();
            return m;
          } catch (err) {
            last = err;
            try { await db?.close(); } catch { /* already gone */ }
          }
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      throw new Error(`no member took the write "${name}": ${last?.message}`);
    };

    /*
     * What the cluster holds, and how far each member has got.
     *
     * ONLY THE LEADER MAY BE READ. Reads are linearizable and the leader
     * alone serves them
     * (docs/replicaton-roadmap.md, the step 6 decision), so polling
     * every member for its documents -- which is what this used to do --
     * now gets -63 from every follower. What a follower will still
     * answer is `ping`, and that is where it says what it is and how far
     * its apply pump has got.
     *
     * So convergence is checked as: the LEADER's documents are what was
     * expected, and every live member has applied at least as much of
     * the log as the leader had when it was asked. That is a stronger
     * claim than the old one, not a weaker substitute -- it is the log
     * index rather than a re-derived read -- and the file-level checks
     * elsewhere in this file are what tie a log prefix back to bytes on
     * disk.
     */
    const statusOf = async (m) => {
      const c = await connectServer(m.port);
      try { return await c.ping(); } finally { await c.close(); }
    };

    const namesOn = async (m) => {
      const db = (await connectServer(m.port)).db(DB);
      try {
        return (await db.collection('users').find({}, { sort: { name: 1 } }).toArray())
          .map((d) => d.name);
      } catch (err) {
        /* A leader that has applied nothing has no `users` at all: the
         * collection is made by the first insert applying. */
        if (err.code === -37) return [];
        throw err;
      } finally { await db.close(); }
    };

    const agree = async (members, expected, withinMs = 20000) => {
      const started = Date.now();
      let seen = null;
      let lag = null;
      while (Date.now() - started < withinMs) {
        const live = members.filter((n) => n.alive);
        /* Concurrently, so the leader's number and the followers' are
         * from the same instant rather than one poll apart. */
        const stats = await Promise.all(live.map(async (m) =>
          [m, await statusOf(m).catch(() => null)]));
        const lead = stats.find(([, st]) => st?.role === 'leader');
        if (lead) {
          const at = lead[1].applied;
          seen = await namesOn(lead[0]).catch(() => null);
          lag = stats.map(([m, st]) => `${m.id}:${st ? st.applied : '?'}`);
          if (JSON.stringify(seen) === JSON.stringify(expected) &&
              stats.every(([, st]) => st && st.applied >= at)) {
            return Date.now() - started;
          }
        }
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error(`members never agreed on ${JSON.stringify(expected)}: ` +
                      `leader held ${JSON.stringify(seen)}, applied ${lag}`);
    };

    /*
     * Whether a write can still be committed, in bounded time. This is
     * the only honest way to ask about the quorum from outside: a member
     * that CANNOT commit is a member that never answers, and "never" has
     * to be measured.
     *
     * The short keepalive is deliberate and this is a second assertion
     * hiding in the first. ORDERING IS THE CONTRACT: answers are paired
     * with requests by arrival order, because nothing on the wire
     * identifies them. A server that answered the keepalive PING while
     * the write ahead of it was still in the log would hand the ping's
     * `{ok:true}` back as the insert's answer, and this would read
     * "committed" for a write that never happened. (It did.)
     */
    const canCommit = async (m, name, withinMs = 4000) => {
      const db = (await connectServer(m.port, { keepAliveMs: 500 })).db(DB);
      try {
        const answered = await Promise.race([
          db.collection('users').insertOne({ name }).then(() => 'committed', (e) => e),
          new Promise((r) => setTimeout(() => r('waiting'), withinMs))
        ]);
        return answered;
      } finally { await db.close(); }
    };

    beforeAll(async () => {
      nodes = [];
      for (const m of SEEDS) await boot(m, seedArgs(m));
      return async () => { for (const m of ALL) await stop(m); };
    });

    it('admits a process that knows one address, and catches it up on the past',
       async () => {
      const leader = await write('before-it-existed');
      await agree(SEEDS, ['before-it-existed']);

      /*
       * ONE seed, and the one that is NOT the leader whenever that is
       * knowable -- so the path under test is dial, be redirected, ask
       * the address the redirect named. No --peer at all: everything
       * this member learns about the cluster it learns from the answer.
       */
      const seed = SEEDS.find((m) => m.id !== leader.id) ?? SEEDS[0];
      await boot(JOINER, [
        '--raft', String(JOINER.id), '--raft-port', String(JOINER.raftPort),
        '--join', `127.0.0.1:${seed.raftPort}`
      ]);

      // A write made before it existed, which it can only have from the
      // log: its directory started empty.
      await agree(ALL, ['before-it-existed']);
      // And it takes new ones like any other member.
      await write('after');
      await agree(ALL, ['after', 'before-it-existed']);

      /*
       * AND NOBODY HAD TO COMPLAIN, which is a separate claim and the
       * one with nothing else watching it.
       *
       * A join's answer is DEFERRED: the node parks the requester,
       * queues nothing, and builds the reply later -- when the CONFIG
       * entry applies, in a call that is handling no message at all. If
       * the conversation it arrived on did not outlive the call that
       * took it, that reply is built and then dropped, and the node
       * says so out loud rather than losing it silently.
       *
       * Everything above would still pass. The joiner's own retry is
       * idempotent, so a SECOND join of an identical record is answered
       * on the spot and it gets in anyway, a call timeout later. This
       * line is the difference between that and the answer arriving.
       */
      for (const m of nodes) {
        expect(m.err, `${m.id} said: ${m.err}`).not.toMatch(/no conversation/);
      }
    }, 60000);

    it('is promoted to a voter, so two of the three can commit without the third',
       async () => {
      /*
       * The assertion is arithmetic. Three members, one of them the
       * joiner: if it is a VOTER the quorum is two and the cluster
       * survives losing one. If it were still a learner the quorum
       * would be two of the ORIGINAL two, and losing either would stop
       * the cluster dead.
       *
       * So: stop a seed, and require a write to go through anyway. The
       * survivors are one seed and the joiner, which is only a quorum if
       * the joiner counts.
       */
      const victim = SEEDS[0];
      await stop(victim);
      const took = await write('needs-the-joiner');
      expect(took.id).not.toBe(victim.id);
      await agree(ALL, ['after', 'before-it-existed', 'needs-the-joiner']);

      await boot(victim, seedArgs(victim));
      await agree(ALL, ['after', 'before-it-existed', 'needs-the-joiner']);
    }, 60000);

    it('comes back a member with no --peer list and no second join', async () => {
      /*
       * ARGV IS A BOOTSTRAP AND THE LOG IS THE TRUTH. Restarted with
       * neither a member list nor a seed, this member's own CONFIG
       * entries are the only place its cluster is written down -- and a
       * member that read nothing there would boot as a group of one,
       * elect itself at a term of its own choosing, and disrupt a
       * cluster that already has a leader.
       */
      await stop(JOINER);
      await write('while-it-was-down');
      await boot(JOINER, ['--raft', String(JOINER.id), '--raft-port', String(JOINER.raftPort)]);

      const expected = ['after', 'before-it-existed', 'needs-the-joiner', 'while-it-was-down'];
      await agree(ALL, expected);

      // Still a VOTER, not merely a reader: the same arithmetic as
      // before, against a member whose voting status survived a restart
      // only because the log carried it.
      const victim = SEEDS[1];
      await stop(victim);
      await write('still-a-voter');
      await agree(ALL, [...expected, 'still-a-voter'].sort());
      await boot(victim, seedArgs(victim));
      await agree(ALL, [...expected, 'still-a-voter'].sort());
    }, 60000);

    it('takes a re-join of the same member as the no-op it is', async () => {
      /*
       * A joiner that treats a lost reply as failure asks again, and the
       * node answers an IDENTICAL record with the current set and
       * changes nothing. Restarting with --join is that retry, made
       * deliberately.
       */
      await stop(JOINER);
      await boot(JOINER, [
        '--raft', String(JOINER.id), '--raft-port', String(JOINER.raftPort),
        '--join', `127.0.0.1:${SEEDS[0].raftPort}`
      ]);
      const expected = ['after', 'before-it-existed', 'needs-the-joiner',
                        'still-a-voter', 'while-it-was-down'];
      await agree(ALL, expected);
      await write('after-the-rejoin');
      await agree(ALL, [...expected, 'after-the-rejoin'].sort());
    }, 60000);

    it('removes a member, and the survivors\' quorum changes with it', async () => {
      /*
       * The arithmetic again, run backwards. Three voters need two;
       * remove one and the remaining two still need two, so stopping
       * either of THEM must stop the cluster. That is the whole
       * observable difference between a member that left and a member
       * that is merely down, and it is the reason removing one is worth
       * being able to do.
       */
      const [a, b] = SEEDS;
      const left = await runOnce(['--leave', String(JOINER.id),
                                  '--join', `127.0.0.1:${a.raftPort}`,
                                  '--join', `127.0.0.1:${b.raftPort}`]);
      expect(left.code).toBe(0);
      expect(left.err).toMatch(new RegExp(`node ${JOINER.id} removed`));
      await stop(JOINER);

      // Two voters left. Stop one: the other cannot reach a quorum, and
      // a write offered to it is never answered rather than refused --
      // it is the leader, it just cannot commit.
      const victim = (await write('two-left')).id === a.id ? b : a;
      const survivor = victim === a ? b : a;
      await stop(victim);
      expect(await canCommit(survivor, 'no-quorum')).toBe('waiting');

      // ...and it comes back the moment the second voter does.
      await boot(victim, seedArgs(victim));
      const took = await write('quorum-again');
      expect([a.id, b.id]).toContain(took.id);
    }, 60000);

    it('says why, rather than starting, when the flags contradict each other',
       async () => {
      /* Not zero, rather than 2: wasmtime reports any non-zero guest
       * exit as 1, so the CODE only says "it refused" and the SENTENCE
       * is what says which refusal it was. */
      const both = await runOnce(['--raft', '9', '--raft-port', String(base + 30),
                                  '--peer', `1@127.0.0.1:${base + 31}`,
                                  '--join', `127.0.0.1:${base + 32}`]);
      expect(both.code).not.toBe(0);
      expect(both.err).toMatch(/--peer and --join are two ways to learn the same thing/);

      const noPort = await runOnce(['--raft', '9', '--join', `127.0.0.1:${base + 32}`]);
      expect(noPort.code).not.toBe(0);
      expect(noPort.err).toMatch(/--join needs --raft-port/);

      const noSeed = await runOnce(['--leave', '9']);
      expect(noSeed.code).not.toBe(0);
      expect(noSeed.err).toMatch(/--leave needs --join/);

      const stdio = await runOnce(['--stdio', '--raft', '9', '--raft-port', String(base + 30),
                                   '--join', `127.0.0.1:${base + 32}`]);
      expect(stdio.code).not.toBe(0);
      expect(stdio.err).toMatch(/--stdio cannot join a cluster/);
    }, 30000);

    it('gives up out loud on a seed that is not there', async () => {
      // Nothing is listening on this port. Twenty rounds at a quarter
      // second is the budget; what matters is that it ENDS, and says
      // which address never answered.
      const dead = await runOnce(['--raft', '9', '--raft-port', String(base + 30),
                                  '--join', `127.0.0.1:${base + 40}`]);
      expect(dead.code).toBe(1);
      expect(dead.err).toMatch(/could not join/);
      expect(dead.err).toMatch(new RegExp(`127\\.0\\.0\\.1:${base + 40} did not answer`));
    }, 60000);
  });

  /*
   * The other direction across the two hosts: a member running in Node
   * that JOINS a C cluster, knowing one C address and nothing else.
   *
   * The existing mixed-cluster suite proves the steady-state framing.
   * This proves the one exchange that is not steady state, and it is the
   * one with the most room to disagree: a join's answer is DEFERRED --
   * built when a CONFIG entry applies, seconds after the call that asked
   * -- so C has to hold a conversation open across calls, and JavaScript
   * has to hold a promise. Neither end can be tested against itself for
   * that.
   *
   * It is also where the { group, msg } envelope had to stop: a native
   * member hosts one group and wraps nothing, so joinGroup is given a
   * null group id and sends the message bare.
   */
  describe.skipIf(!enabled)(`nisaba-server: a Node member joins a C cluster (${engine.name})`, () => {
    const base = nextPort();
    const C_MEMBERS = [1, 2].map((id) => ({
      id, port: base + id - 1, raftPort: base + 10 + id - 1
    }));
    const NODE = { id: 3, raftPort: base + 12 };
    const argsFor = (m) => [
      '--raft', String(m.id), '--raft-port', String(m.raftPort),
      ...C_MEMBERS.filter((o) => o.id !== m.id)
        .flatMap((o) => ['--peer', `${o.id}@127.0.0.1:${o.raftPort}`])
    ];

    class RecordingMachine {
      constructor() { this.applied = 0; this.count = 0; }
      appliedIndex() { return this.applied; }
      async apply(entry) { this.count++; this.applied = entry.index; }
    }

    let cNodes = [];
    let node = null, transport = null, log = null, machine = null, ticker = null;
    let admitted = null;

    const stopC = async (m) => {
      if (!m.alive) return;
      m.alive = false;
      m.proc.kill();
      await new Promise((r) => m.proc.once('exit', r));
    };
    const bootC = async (m) => {
      const { proc, dir } = await startServer(engine, m.port, argsFor(m), -1, m.dir);
      Object.assign(m, { proc, dir, alive: true });
      return m;
    };
    const until = async (pred, ms = 20000) => {
      const started = Date.now();
      while (Date.now() - started < ms) {
        if (await pred()) return Date.now() - started;
        await new Promise((r) => setTimeout(r, 50));
      }
      throw new Error('condition never held');
    };
    /* Leadership somewhere a client can reach. The Node member is a
     * full voter and entitled to WIN, and a leader with no database is
     * a leader no client can send a write to -- so it hands leadership
     * back rather than being prevented from taking it. */
    const ensureCLeads = async () => {
      if (!node) return;
      await until(() => node.role === 'leader' ||
                        cNodes.some((m) => m.alive && m.id === node.leaderId));
      if (node.role !== 'leader') return;
      await node.transferLeadership(cNodes.find((m) => m.alive).id);
      await until(() => node.role !== 'leader');
    };
    const writeToC = async (name, tries = 100) => {
      await ensureCLeads();
      let last = null;
      for (let i = 0; i < tries; i++) {
        for (const m of cNodes.filter((n) => n.alive)) {
          let db = null;
          try {
            db = (await connectServer(m.port)).db(DB);
            await db.collection('users').insertOne({ name });
            await db.close();
            return m;
          } catch (err) {
            last = err;
            try { await db?.close(); } catch { /* already gone */ }
          }
        }
        await new Promise((r) => setTimeout(r, 100));
      }
      throw new Error(`no C member took "${name}": ${last?.message}`);
    };

    beforeAll(async () => {
      cNodes = [];
      for (const m of C_MEMBERS) {
        const { proc, dir } = await startServer(engine, m.port, argsFor(m), -1);
        cNodes.push(Object.assign(m, { proc, dir, alive: true }));
      }
      // A write before the Node member exists, so what reaches it later
      // is the log rather than live traffic.
      await writeToC('before-the-node-member');

      log = new EntryLog(new MemoryHandle());
      await log.open();
      machine = new RecordingMachine();
      transport = new TcpRaftTransport({
        listenPort: NODE.raftPort,
        onMessage: (env) => node.handleMessage(env),
        requestTimeoutMs: 1000
      });
      await transport.start();

      // One seed, no ids, no member list -- and a NULL group, because a
      // native member wraps nothing around the message.
      admitted = await joinGroup(transport, null,
        { id: NODE.id, host: '127.0.0.1', port: NODE.raftPort },
        { seeds: C_MEMBERS.map((m) => ({ host: '127.0.0.1', port: m.raftPort })) });

      node = new RaftNode({ id: NODE.id, peers: admitted, log, stateMachine: machine, transport });
      await node.start(Date.now());
      ticker = setInterval(() => node.tick(Date.now()), 20);
      ticker.unref?.();

      return async () => {
        clearInterval(ticker);
        for (const m of cNodes) await stopC(m);
        await node?.stop();   /* the last test stops it: it has left */
        await transport.stop();
        await log.close();
      };
    });

    it('is admitted by a C leader, as a learner among the voters it names', () => {
      // The adopted records, decided by C and read by JavaScript: every
      // member with the address the cluster holds for it, and the
      // applicant NOT a voter, whatever it asked for.
      expect(admitted.map((m) => m.id).sort()).toEqual([1, 2, 3]);
      for (const m of C_MEMBERS) {
        expect(admitted.find((r) => r.id === m.id)).toMatchObject({
          host: '127.0.0.1', port: m.raftPort
        });
      }
      expect(admitted.find((r) => r.id === NODE.id)).toMatchObject({ voting: false });
    });

    it('is caught up from the log, and then promoted by the C leader', async () => {
      // Entries it never saw happen, arriving over the wire it just
      // joined on.
      await until(() => machine.count >= 1);

      // And the promotion is C's own decision, on match index -- watched
      // from the other side of the wire, in the member records this
      // node adopts as the CONFIG entry applies.
      await until(() => node.memberInfo.find((m) => m.id === NODE.id)?.voting !== false);
      expect(node.voters.sort()).toEqual([1, 2, 3]);
    }, 60000);

    it('is what the quorum needs once it is a voter', async () => {
      await stopC(cNodes.find((m) => m.alive));
      expect(cNodes.filter((m) => m.alive).length).toBe(1);
      const before = log.lastIndex;
      const took = await writeToC('needs-the-node-member');
      await until(() => log.lastIndex > before);
      expect(node.leaderId).toBe(took.id);
    }, 60000);

    it('leaves, and the two C members left are the whole electorate', async () => {
      // Both C members back first, so what is being measured afterwards
      // is the departure and not the outage.
      await bootC(cNodes.find((m) => !m.alive));
      await writeToC('both-back');

      const left = await leaveGroup(transport, null, NODE.id,
        { seeds: cNodes.map((m) => ({ host: '127.0.0.1', port: m.raftPort })) });
      // The adopted set, decided by C: this member is gone from it.
      expect(left.map((m) => m.id).sort()).toEqual([1, 2]);
      clearInterval(ticker);
      await node.stop();
      node = null;

      /*
       * Two voters need two. Find whichever C member leads, stop the
       * other, and the leader is left unable to commit anything -- a
       * write it is not refusing, because it IS the leader, and not
       * answering, because it cannot reach a quorum. Before the leave
       * there were three voters and this would have gone through.
       */
      const leader = await writeToC('two-of-two');
      await stopC(cNodes.find((m) => m.alive && m.id !== leader.id));
      const db = (await connectServer(leader.port, { keepAliveMs: 500 })).db(DB);
      try {
        const outcome = await Promise.race([
          db.collection('users').insertOne({ name: 'alone' }).then(() => 'committed', (e) => e),
          new Promise((r) => setTimeout(() => r('waiting'), 4000))
        ]);
        expect(outcome).toBe('waiting');
      } finally { await db.close(); }
    }, 60000);
  });

  /*
   * docs/steps/databases-in-the-server.md: the server holds an INSTANCE.
   *
   * One connection, many databases, switched between at will -- which
   * only works because `client.db(name)` sends NOTHING and the
   * connection is not stateful about which one. Every request names its
   * own, and the two halves of that are what the suite is checking:
   * that the handles are cheap, and that nothing leaks between them.
   */
  /*
   * One log for the INSTANCE, not one per database. That is the whole
   * of what replication had to learn here: server/replica.c and
   * server/peers.c are untouched, and a log entry gained an envelope
   * saying which database its command is for.
   */
  describe.skipIf(!enabled)(`nisaba-server: a replicated instance (${engine.name})`, () => {
    let proc, client, dir;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc, dir } = await startServer(engine, port, ['--raft', '1'], -1));
      client = await connectServer(port);
      return async () => { if (client.isOpen) await client.close(); proc.kill(); };
    });

    it('puts writes to every database through the one log', async () => {
      const a = client.db('analytics');
      const b = client.db('billing');
      await a.collection('events').insertOne({ n: 1 });
      await b.collection('invoices').insertOne({ n: 2 });
      await a.collection('events').insertOne({ n: 3 });
      expect(await a.collection('events').countDocuments({})).toBe(2);
      expect(await b.collection('invoices').countDocuments({})).toBe(1);
      // Applied ONCE each, in both: the pump is the only applier, and
      // the envelope is what tells it which database to apply into.
      expect((await a.collection('events').find({}).toArray()).map(d => d.n).sort())
        .toEqual([1, 3]);

      // One log, beside the databases rather than inside one.
      expect(fs.existsSync(path.join(dir, '__wal__.bj'))).toBe(true);
      expect(fs.existsSync(path.join(dir, 'analytics', '__wal__.bj'))).toBe(false);
      // ...and it is not a database, so nothing lists it as one.
      expect((await client.listDatabases()).sort()).toEqual(['analytics', 'billing']);
    });

    it('a list of writes still takes one trip to the log per operation', async () => {
      const id = new ObjectId();
      const r = await client.db('billing').collection('invoices').bulkWrite([
        { insertOne: { document: { _id: id, n: 9 } } },
        { updateOne: { filter: { _id: id }, update: { $set: { paid: true } } } }
      ]);
      expect(r.insertedCount).toBe(1);
      expect(r.matchedCount).toBe(1);
      expect((await client.db('billing').collection('invoices').findOne({ _id: id })).paid)
        .toBe(true);
    });

    it('resumes across a restart at the floor of EVERY database, not one', async () => {
      const before = {
        analytics: await client.db('analytics').collection('events').countDocuments({}),
        billing: await client.db('billing').collection('invoices').countDocuments({})
      };
      await client.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      /*
       * The replay floor is the instance's: the highest index applied
       * across every database in the root, including ones nothing has
       * opened yet. Taken from only the open ones it would be zero on
       * the way up, and replaying a prefix that was already applied
       * hands a collection an applied index it has already passed --
       * which is refused, is not deterministic, and halts the replica
       * before it serves anything.
       */
      const again = await startServer(engine, port + 1, ['--raft', '1'], 0, dir);
      const back = await connectServer(port + 1);
      expect(await back.db('analytics').collection('events').countDocuments({}))
        .toBe(before.analytics);
      expect(await back.db('billing').collection('invoices').countDocuments({}))
        .toBe(before.billing);
      await back.close();
      again.proc.kill();
      await new Promise(r => again.proc.once('exit', r));
    }, 30000);
  });

  describe.skipIf(!enabled)(`nisaba-server: bin/db.js names a database (${engine.name})`, () => {
    let proc;
    const port = nextPort();
    const cli = (...args) => spawnSync(process.execPath,
      ['bin/db.js', '--server', `127.0.0.1:${port}`, ...args], { encoding: 'utf8' });

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port, [], -1));
      return () => { proc.kill(); };
    });

    it('reaches every database on the server, and the instance itself', () => {
      expect(cli('shop', 'insert', 'products', '{"name":"widget"}').status).toBe(0);
      expect(cli('warehouse', 'insert', 'bins', '{"id":7}').status).toBe(0);
      expect(cli('shop', 'count', 'products').stdout.trim()).toBe('1');
      // The first word means the same thing it does locally, which it
      // did not before: the server holds an instance.
      expect(cli('databases').stdout.trim().split('\n').sort())
        .toEqual(['shop', 'warehouse']);
      // Both spellings a person would type.
      expect(cli('drop-database', 'warehouse').stdout).toMatch(/Dropped database warehouse/);
      expect(cli('shop', 'drop-database').stdout).toMatch(/Dropped database shop/);
      expect(cli('databases').stdout.trim()).toBe('No databases.');
    });
  });

  describe.skipIf(!enabled)(`nisaba-server: an instance of databases (${engine.name})`, () => {
    let proc, client, dir;
    const port = nextPort();

    beforeAll(async () => {
      // An EMPTY root: no database in it at all, so everything below is
      // something the server made.
      ({ proc, dir } = await startServer(engine, port, [], -1));
      client = await connectServer(port);
      return async () => { await client.close(); proc.kill(); };
    });

    it('holds two databases at once over one connection, interleaved', async () => {
      expect(await client.listDatabases()).toEqual([]);

      const analytics = client.db('analytics');
      const billing = client.db('billing');
      // Handles, not round trips: the same name is the same object, and
      // nothing has been sent yet.
      expect(client.db('analytics')).toBe(analytics);
      expect(await client.listDatabases()).toEqual([]);

      await analytics.collection('events').insertOne({ n: 1 });
      await billing.collection('invoices').insertOne({ n: 2 });
      // Interleaved on the one socket, which a per-connection "use"
      // could not do at all.
      await analytics.collection('events').insertOne({ n: 3 });
      await billing.collection('invoices').insertOne({ n: 4 });

      expect(await analytics.listCollections()).toEqual(['events']);
      expect(await billing.listCollections()).toEqual(['invoices']);
      expect((await analytics.collection('events').find({}).toArray()).map(d => d.n).sort())
        .toEqual([1, 3]);
      expect((await billing.collection('invoices').find({}).toArray()).map(d => d.n).sort())
        .toEqual([2, 4]);

      // A collection name they share is two different collections.
      await analytics.collection('users').insertOne({ from: 'analytics' });
      await billing.collection('users').insertOne({ from: 'billing' });
      expect((await analytics.collection('users').findOne({})).from).toBe('analytics');
      expect((await billing.collection('users').findOne({})).from).toBe('billing');

      // A real subdirectory each, which is the guarantee: two names
      // never share a catalog or a collection file.
      expect(fs.existsSync(path.join(dir, 'analytics', '__catalog__.bj'))).toBe(true);
      expect(fs.existsSync(path.join(dir, 'billing', '__catalog__.bj'))).toBe(true);
      expect(fs.existsSync(path.join(dir, '__catalog__.bj'))).toBe(false);
    });

    it('lists them, and drops one without touching its neighbour', async () => {
      expect((await client.listDatabases()).sort()).toEqual(['analytics', 'billing']);

      expect(await client.dropDatabase('billing')).toBe(true);
      expect(await client.listDatabases()).toEqual(['analytics']);
      expect(fs.existsSync(path.join(dir, 'billing'))).toBe(false);
      // Dropping the already-gone is what the caller asked for.
      expect(await client.dropDatabase('billing')).toBe(false);

      expect((await client.db('analytics').collection('events').find({}).toArray()).length).toBe(2);
      // And the name is free: a fresh, empty database rather than the
      // old one coming back.
      expect(await client.db('billing').listCollections()).toEqual([]);
      expect(await client.dropDatabase('billing')).toBe(true);
    });

    it('refuses an operation that names no database, rather than guessing one', async () => {
      // No default. A write landing in a database nobody named is worse
      // than a refusal a client can act on, and "whatever was used last"
      // is exactly the connection state this shape exists to avoid.
      await expect(client.request({ op: 'count', coll: 'events' }))
        .rejects.toMatchObject({ code: -42 });
      await expect(client.request({ op: 'listCollections' }))
        .rejects.toMatchObject({ code: -42 });
      // ping names none and needs none -- it is how a connection stays
      // warm before it has opened anything. It answers with what the
      // member IS; unreplicated, that is just the pong.
      expect(await client.ping()).toMatchObject({ pong: true });
    });

    it('refuses a database name that is not one', async () => {
      await expect(client.request({ op: 'count', db: 'a/b', coll: 'x' }))
        .rejects.toMatchObject({ code: -16 });
      await expect(client.request({ op: 'count', db: '', coll: 'x' }))
        .rejects.toMatchObject({ code: -16 });
    });

    it('keeps cursors apart: two databases never mint the same id', async () => {
      const a = client.db('analytics');
      const b = client.db('billing');
      for (let i = 0; i < 20; i++) {
        await a.collection('rows').insertOne({ from: 'analytics', i });
        await b.collection('rows').insertOne({ from: 'billing', i });
      }
      // A cursor open in EACH, which is the only arrangement in which
      // ids can collide -- and where per-session counters would both
      // start at 1.
      const ca = await a.request({ op: 'find', coll: 'rows', opts: { batchSize: 5 } });
      const cb = await b.request({ op: 'find', coll: 'rows', opts: { batchSize: 5 } });
      expect(ca.cursor).toBeGreaterThan(0);
      expect(cb.cursor).toBeGreaterThan(0);

      /*
       * Minted from ONE counter for the whole process. Routing by the
       * request's `db` alone would work right up until a caller named
       * the wrong one, and then it would hand back somebody else's scan
       * rather than refusing -- which is not a bug you find by testing
       * the happy path.
       */
      expect(ca.cursor).not.toBe(cb.cursor);
      await expect(b.request({ op: 'getMore', cursor: ca.cursor }))
        .rejects.toMatchObject({ code: -46 });
      await expect(a.request({ op: 'getMore', cursor: cb.cursor }))
        .rejects.toMatchObject({ code: -46 });

      // And each is still its own, still where it was.
      const nextA = await a.request({ op: 'getMore', cursor: ca.cursor });
      expect(nextA.docs.length).toBe(5);
      expect(nextA.docs.every(d => d.from === 'analytics')).toBe(true);
      await a.request({ op: 'closeCursor', cursor: ca.cursor });
      await b.request({ op: 'closeCursor', cursor: cb.cursor });
      await a.dropCollection('rows');
      await b.dropCollection('rows');
    });

    it('keeps change events apart, including a collection of the same name', async () => {
      const a = client.db('analytics');
      const b = client.db('billing');
      const seen = [];
      const stream = await a.collection('watched').watch();
      stream.on('change', (c) => seen.push(c));

      await b.collection('watched').insertOne({ from: 'billing' });
      await a.collection('watched').insertOne({ from: 'analytics' });
      await new Promise(r => setTimeout(r, 200));

      // dbs_watched and dbs_emit match on the collection NAME, so two
      // databases with a `watched` each would cross-deliver if they were
      // not separate sessions. They are.
      expect(seen.map(c => c.fullDocument?.from)).toEqual(['analytics']);
      await stream.close();
    });

    it('leaves a directory per database that the JS Client opens by name', async () => {
      await client.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      // The claim this repository rests on, one level up: a root the C
      // server built, opened by the in-process Client that writes the
      // same layout.
      const root = new NodeFSStorageProvider(dir);
      const js = await connectClient(root);
      expect((await js.listDatabases()).sort()).toEqual(['analytics', 'billing']);
      const events = await (await js.db('analytics')).collection('events');
      expect((await events.find({}).toArray()).map(d => d.n).sort()).toEqual([1, 3]);
      await js.close();
      await root.close();
    });
  });

  describe.skipIf(!enabled)(`nisaba-server: the JS client (${engine.name})`, () => {
    let proc, db;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port));
      db = (await connectServer(port)).db(DB);   // a bare port means loopback
      return async () => { await db.close(); proc.kill(); };
    });

    it('reads and writes through connectServer(), with no engine in the client', async () => {
      const users = db.collection('users');
      expect(await users.countDocuments({})).toBe(3);
      expect(await users.distinct('team')).toEqual(expect.arrayContaining(['core', 'research']));

      const sorted = await users.find({ team: 'core' }, { sort: { name: 1 }, limit: 1 }).toArray();
      expect(sorted.map(d => d.name)).toEqual(['Ada']);

      // The id comes back because this side minted it -- C will not invent
      // one, and the result it computes counts what happened rather than
      // naming it.
      const { insertedId } = await users.insertOne({ name: 'Edsger', team: 'core' });
      expect(insertedId).toBeInstanceOf(ObjectId);
      expect((await users.findOne({ name: 'Edsger' }))._id.toHexString()).toBe(insertedId.toHexString());

      expect((await users.updateOne({ name: 'Ada' }, { $set: { onCall: true } })).modifiedCount).toBe(1);
      expect((await users.findOne({ name: 'Ada' })).onCall).toBe(true);
      expect((await users.deleteMany({ team: 'research' })).deletedCount).toBe(1);
      expect(await users.countDocuments({})).toBe(3);
    });

    it('dates a field with this end clock, since C will not read one', async () => {
      // $currentDate is not an operator the engine knows: a host rewrites
      // it into $set against a concrete clock reading before proposing,
      // so what gets written down is a date rather than a rule. This
      // server is the host and has no clock, so the milliseconds travel
      // with the request -- the same bargain as an insert's _id.
      const users = db.collection('users');
      const before = Date.now();
      await users.updateOne({ name: 'Ada' }, { $currentDate: { at: true, seen: { $type: 'date' } } });
      const ada = await users.findOne({ name: 'Ada' });
      expect(ada.at).toBeInstanceOf(Date);
      expect(ada.seen).toBeInstanceOf(Date);
      expect(ada.at.getTime()).toBeGreaterThanOrEqual(before);
      expect(ada.at.getTime()).toBeLessThanOrEqual(Date.now());

      // The rules stay in C: this side sends the update as written and
      // does not know that a field cannot be both $set and dated.
      await expect(users.updateOne({ name: 'Ada' }, { $set: { at: 1 }, $currentDate: { at: true } }))
        .rejects.toMatchObject({ code: -28 });

      // Without the clock reading it is refused, not dated from thin air.
      await expect(db.request({
        op: 'update', coll: 'users', filter: { name: 'Ada' },
        update: { $currentDate: { at: true } }
      })).rejects.toMatchObject({ code: -42 });
    });

    it('turns a refusal into an error carrying the code and the sentence', async () => {
      await expect(db.collection('ghosts').countDocuments({})).rejects.toThrow(ServerError);
      await expect(db.collection('ghosts').countDocuments({})).rejects.toMatchObject({ code: -37 });
    });

    it('runs a pipeline in C and pages the result from this side', async () => {
      // Its own collection: this suite shares one server, and a pipeline
      // asserted against documents an earlier test edits is a test of
      // the order tests happen to run in.
      const users = db.collection('sales');
      await users.insertMany([
        { name: 'Ada', team: 'core', qty: 5 }, { name: 'Grace', team: 'core', qty: 3 },
        { name: 'Alan', team: 'research', qty: 7 }
      ]);

      const grouped = await users.aggregate([
        { $match: { qty: { $gt: 0 } } },
        { $group: { _id: '$team', n: { $count: {} }, total: { $sum: '$qty' } } },
        { $sort: { n: -1 } }
      ]).toArray();
      expect(grouped.map(g => g._id)).toEqual(['core', 'research']);
      expect(grouped[0].total).toBe(8);

      // The result arrived whole, so the cursor is a position in an
      // array this side holds -- and next() and for-await share it.
      const cursor = users.aggregate([{ $sort: { name: 1 } }, { $project: { name: 1, _id: 0 } }]);
      const first = await cursor.next();
      expect(first.done).toBe(false);
      const rest = [];
      for await (const doc of cursor) rest.push(doc.name);
      expect(rest).not.toContain(first.value.name);

      // A stage this subset does not have is refused by C, which names
      // the position; this side names what was at it.
      let err = null;
      try { await users.aggregate([{ $match: {} }, { $obliterate: {} }]).toArray(); }
      catch (e) { err = e; }
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(-24);
      expect(err.index).toBe(1);
      expect(err.message).toMatch(/stage 1: \{"\$obliterate":\{\}\}/);
      // And the sentence listing the subset is C's, not a copy here.
      expect(err.message).toMatch(/\$match, \$sort, \$skip, \$limit, \$project, \$group, \$count/);
    });

    it('reports the plan a query would use, in the words C chose', async () => {
      const users = db.collection('planned');
      const { insertedIds } = await users.insertMany([
        { team: 'core', n: 1 }, { team: 'research', n: 2 }
      ]);
      await users.createIndex({ team: 1 });

      expect(await users.explain({ team: 'core' })).toEqual({ source: 'equality', index: 'team_1' });
      expect(await users.explain({ _id: insertedIds[0] })).toEqual({ source: 'ids', index: null });
      expect(await users.explain({ n: { $gt: 0 } })).toEqual({ source: 'scan', index: null });

      // The cursor sugar the in-process FindCursor has, on both cursor
      // shapes -- and neither of them ran the query to answer.
      expect(await users.find({ team: 'core' }).explain())
        .toEqual({ source: 'equality', index: 'team_1' });
      expect(await users.find({ team: 'core' }, { batchSize: 1 }).explain())
        .toEqual({ source: 'equality', index: 'team_1' });
    });

    it('reads and writes one document in one request, and hands it back', async () => {
      const jobs = db.collection('jobs');
      await jobs.insertMany([{ name: 'a', n: 1 }, { name: 'b', n: 2 }]);

      // 'before' by default -- the image the planner already had.
      const before = await jobs.findOneAndUpdate({ name: 'a' }, { $set: { n: 10 } });
      expect(before.n).toBe(1);
      // ...and the write did happen.
      expect((await jobs.findOne({ name: 'a' })).n).toBe(10);

      const after = await jobs.findOneAndUpdate(
        { name: 'a' }, { $inc: { n: 5 } }, { returnDocument: 'after' });
      expect(after.n).toBe(15);
      expect(after._id.toHexString()).toBe(before._id.toHexString());

      expect(await jobs.findOneAndUpdate({ name: 'zz' }, { $set: { n: 0 } })).toBe(null);

      // An upsert asked for 'before' answers null -- no prior state
      // exists -- but the document is made, which 'after' can show.
      expect(await jobs.findOneAndUpdate(
        { name: 'new' }, { $set: { n: 99 } }, { upsert: true })).toBe(null);
      expect((await jobs.findOne({ name: 'new' })).n).toBe(99);
      const born = await jobs.findOneAndUpdate(
        { name: 'newer' }, { $set: { n: 7 } }, { upsert: true, returnDocument: 'after' });
      expect(born).toMatchObject({ name: 'newer', n: 7 });

      const replaced = await jobs.findOneAndReplace(
        { name: 'b' }, { name: 'b', n: 42 }, { returnDocument: 'after' });
      expect(replaced.n).toBe(42);

      // A delete has one image, and it is the document that is gone.
      const deleted = await jobs.findOneAndDelete({ name: 'b' });
      expect(deleted.n).toBe(42);
      expect(await jobs.findOneAndDelete({ name: 'b' })).toBe(null);

      // $currentDate travels the same road it does for updateOne: the
      // rewrite is C's, the clock is this side's.
      const dated = await jobs.findOneAndUpdate(
        { name: 'a' }, { $currentDate: { at: true } }, { returnDocument: 'after' });
      expect(dated.at).toBeInstanceOf(Date);
    });

    it('looks a document up through the index it names', async () => {
      const c = db.collection('indexed');
      await c.insertMany([
        { name: 'Ada', team: 'core', level: 3 },
        { name: 'Grace', team: 'core', level: 4 },
        { name: 'Alan', team: 'research', level: 3 }
      ]);
      // Before there is an index of that name, there is nothing to name.
      await expect(c.findByIndex('team_1', ['core'])).rejects.toMatchObject({ code: -57 });

      await c.createIndex({ team: 1 });
      await c.createIndex({ team: 1, level: 1 });
      await c.createIndex({ name: 'text' });

      expect((await c.findByIndex('team_1', ['core'])).map(d => d.name).sort())
        .toEqual(['Ada', 'Grace']);
      expect((await c.findByIndex('team_1_level_1', ['core', 4])).map(d => d.name))
        .toEqual(['Grace']);
      // A value nothing has is an empty answer, not a refusal.
      expect(await c.findByIndex('team_1', ['nobody'])).toEqual([]);

      // The three ways to ask wrong, each with its own code -- they were
      // one BJ_ERR_STATE between them until this went on a wire.
      await expect(c.findByIndex('name_text', ['Ada'])).rejects.toMatchObject({ code: -58 });
      await expect(c.findByIndex('team_1', ['core', 'extra'])).rejects.toMatchObject({ code: -59 });
    });

    it('sweeps what a TTL index says is over, on this side clock', async () => {
      const c = db.collection('events');
      const now = Date.now();
      await c.insertMany([
        { tag: 'ancient', at: new Date(now - 7200e3) },
        { tag: 'old', at: new Date(now - 3700e3) },
        { tag: 'fresh', at: new Date(now - 60e3) },
        { tag: 'nodate' }                       // sparse tolerates it
      ]);
      // No TTL index: a sweep is owed nothing, which is zero rather than
      // a refusal.
      expect(await c.pruneExpired()).toBe(0);

      await c.createIndex({ at: 1 }, { expireAfterSeconds: 3600, sparse: true });
      expect(await c.pruneExpired()).toBe(2);
      expect((await c.find({}).toArray()).map(d => d.tag).sort()).toEqual(['fresh', 'nodate']);
      // Idempotent at the same instant: nothing else has expired yet.
      expect(await c.pruneExpired()).toBe(0);

      // The clock travels with the request, as it does for $currentDate:
      // asking without one is refused rather than dated from thin air.
      await expect(db.request({ op: 'pruneExpired', coll: 'events' }))
        .rejects.toMatchObject({ code: -42 });
    });

    it('says what the wire does not carry, rather than failing as a TypeError', () => {
      expect(() => db.storageEstimate()).toThrow(/no db\.storageEstimate/);
      expect(() => db.collection('users').estimatedDocumentCount())
        .toThrow(/no collection\.estimatedDocumentCount/);
      // The sentence names the ops that DO exist, so the refusal is
      // actionable without reading the source.
      expect(() => db.collection('users').estimatedDocumentCount()).toThrow(WIRE_OPS.join(', '));
      // And an op that IS on the wire, asked of the wrong thing, says
      // that rather than listing it as available while refusing it --
      // which is what this used to do for `compact`, before compact
      // became a db method as well as a collection one.
      expect(() => db.find({})).toThrow(/find is a collection operation/);
      expect(() => db.watch()).toThrow(/watch is a collection operation/);
    });

    it('refuses an insert whose document has no _id, and names the field', async () => {
      // insertOne() cannot produce this -- it mints the _id into the
      // document, as does every member of insertMany's list -- but a
      // hand-built request can, and `id` does not stand in for it: two
      // places for one fact would need a precedence rule between them.
      // `id` answers the other question, the one only an upsert asks.
      //
      // It used to answer -2 "builder state error", from dc_document_id
      // by way of the planner. The refusal was right; the sentence was
      // about a builder.
      const c = db.collection('users');
      const before = await c.countDocuments({});
      await expect(db.request({
        op: 'insert', coll: 'users', doc: { name: 'Anonymous' }, id: new ObjectId()
      })).rejects.toMatchObject({ code: -42 });
      expect(await c.countDocuments({})).toBe(before);

      // An upsert with nothing to match is the write that genuinely
      // needs `id`, and it still works with no _id in sight.
      const upserted = await c.updateOne(
        { name: 'Nobody At All' }, { $set: { team: 'new' } }, { upsert: true });
      expect(upserted.upsertedId).toBeInstanceOf(ObjectId);
      await c.deleteOne({ _id: upserted.upsertedId });
    });
  });

  describe.skipIf(!enabled)(`nisaba-server: bin/db.js as a client (${engine.name})`, () => {
    let proc;
    const port = nextPort();
    const cli = (...args) => spawnSync(process.execPath, [
      'bin/db.js', '--server', `127.0.0.1:${port}`, DB, ...args
    ], { encoding: 'utf8' });

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port));
      return () => { proc.kill(); };
    });

    it('lists collections, and dumps one, which listing unblocked', () => {
      expect(cli('collections').stdout).toMatch(/^0: users$/m);
      // dump walks listCollections -> listIndexes -> a find cursor, so
      // it is the whole of what this step added, exercised as a user
      // would meet it.
      const dumped = cli('dump');
      expect(dumped.status).toBe(0);
      expect(dumped.stdout).toMatch(/^\{"collection":"users","indexes":\[\]\}$/m);
      expect(dumped.stdout.split('\n').filter(l => l.includes('"doc"'))).toHaveLength(3);
    });

    it('drives the same CLI commands over the socket', () => {
      expect(cli('count', 'users').stdout.trim()).toBe('3');

      const inserted = cli('insert', 'users', '{"name":"Barbara","team":"research"}');
      expect(inserted.status).toBe(0);
      expect(inserted.stdout).toMatch(/^Inserted ObjectId\([0-9a-f]{24}\)\.$/m);

      const found = cli('find', 'users', '{"team":"research"}');
      expect(found.stdout).toContain('Barbara');

      expect(cli('count', 'users').stdout.trim()).toBe('4');
    });

    it('restores a dump into the server, which insertMany unblocked', () => {
      // dump out, restore back into a collection of another name: the
      // round trip the CLI could not complete until a list of documents
      // was one request.
      const n = Number(cli('count', 'users').stdout.trim());
      const dumped = cli('dump', 'users');
      expect(dumped.status).toBe(0);
      const renamed = dumped.stdout
        .split('\n').filter(Boolean)
        .map((l) => JSON.stringify({ ...JSON.parse(l), collection: 'copied' }))
        .join('\n');

      const restored = spawnSync(process.execPath, [
        'bin/db.js', '--server', `127.0.0.1:${port}`, DB, 'restore'
      ], { encoding: 'utf8', input: renamed });
      expect(restored.status).toBe(0);
      expect(restored.stdout).toMatch(
        new RegExp(`Restored ${n} document\\(s\\) across 1 collection\\(s\\)\\.`));
      expect(cli('count', 'copied').stdout.trim()).toBe(String(n));

      // insert-many is over the same wire, and a list is one frame.
      const many = cli('insert-many', 'copied', '[{"name":"Ada2"},{"name":"Grace2"}]');
      expect(many.status).toBe(0);
      expect(many.stdout).toMatch(/Inserted 2 document\(s\)\./);
      expect(cli('count', 'copied').stdout.trim()).toBe(String(n + 2));

      // And a bulk-write, whose grammar is refused by the server rather
      // than by anything in this process.
      const bulk = cli('bulk-write', 'copied', '[{"deleteOne":{"filter":{"name":"Ada2"}}}]');
      expect(bulk.status).toBe(0);
      expect(cli('count', 'copied').stdout.trim()).toBe(String(n + 1));
    });

    it('refuses what the server owns, and names an address it cannot reach', () => {
      // Nothing is left to refuse: every CLI command works over the
      // socket now, database-wide compact included. What the server
      // still owns is the things that are not commands at all.
      // --order is the order NEW files are created with, and creating
      // them is the server's job -- not a number a reader has to know.
      const ordered = cli('count', 'users', '--order', '64');
      expect(ordered.status).toBe(1);
      expect(ordered.stderr).toMatch(/--order is the server's/);

      // And an address with nothing behind it names the address.
      const nowhere = spawnSync(process.execPath, ['bin/db.js', '--server', '127.0.0.1:1', DB, 'count', 'users'], { encoding: 'utf8' });
      expect(nowhere.status).toBe(1);
      expect(nowhere.stderr).toMatch(/cannot reach a nisaba server at 127\.0\.0\.1:1/);
    });
  });

  /*
   * The idle timeout, at 1 second rather than the default 60 -- a timer
   * nothing reaches is a timer nothing tests. What it is really for is
   * the connection whose peer is GONE (a crashed client, a dropped NAT
   * mapping), which TCP cannot tell us about and which would otherwise
   * hold its slot until the process restarts; a client that is merely
   * quiet is the same thing from here.
   */
  describe.skipIf(!enabled)(`nisaba-server: idle connections (${engine.name})`, () => {
    let proc;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port, ['--idle-timeout', '1']));
      return () => { proc.kill(); };
    });

    it('takes back the slot of a connection that asks nothing, and says why', async () => {
      // keepAliveMs: 0 -- this client is deliberately silent.
      const quiet = (await connectServer(port, { keepAliveMs: 0 })).db(DB);
      expect(await quiet.collection('users').countDocuments({})).toBe(3);

      await new Promise(r => setTimeout(r, 2500));

      // Not a bare disconnect: the code and the sentence say what happened.
      let err = null;
      try { await quiet.collection('users').countDocuments({}); }
      catch (e) { err = e; }
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(-45);
      expect(err.message).toMatch(/idle-timeout/);
      await quiet.close();
    });

    it('leaves a connection alone while it pings, which the client does on its own', async () => {
      // 300ms against a 1s timeout: the same third-of-the-budget the
      // default keepalive uses, scaled to a test that has to finish.
      const warm = await connectServer(port, { keepAliveMs: 300 });
      try {
        await new Promise(r => setTimeout(r, 2500));
        // Still there, still answering, three timeouts later.
        expect(await warm.db(DB).collection('users').countDocuments({})).toBe(3);
        // ping is the CLIENT's: it names no database, which is what
        // makes it usable by a connection that has not opened one.
        expect(await warm.ping()).toMatchObject({ pong: true });
      } finally {
        await warm.close();
      }
    });
  });

  /*
   * More than one connection, and a limit on how many. --max-clients 2
   * rather than the default 64 because a bound nothing reaches is a bound
   * nothing tests: two is the smallest number that is more than one.
   */
  describe.skipIf(!enabled)(`nisaba-server: many clients, bounded (${engine.name})`, () => {
    let proc;
    const port = nextPort();
    const cli = (...args) => spawnSync(process.execPath, [
      'bin/db.js', '--server', `127.0.0.1:${port}`, DB, ...args
    ], { encoding: 'utf8' });

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port, ['--max-clients', '2']));
      return () => { proc.kill(); };
    });

    it('serves two connections at once, and a CLI while one of them sits idle', async () => {
      const a = (await connectServer(port)).db(DB);
      const b = (await connectServer(port)).db(DB);
      try {
        // Both live, both answered -- interleaved, not one after the other.
        const [x, y] = await Promise.all([
          a.collection('users').countDocuments({}),
          b.collection('users').countDocuments({})
        ]);
        expect([x, y]).toEqual([3, 3]);

        // Writes from one connection are visible on the other: one process,
        // one engine, one open collection behind both sockets.
        await a.collection('users').insertOne({ name: 'Edsger', team: 'core' });
        expect(await b.collection('users').countDocuments({})).toBe(4);
      } finally {
        await b.close();
      }

      // The bug this replaced: `a` is connected and idle, and a CLI
      // invocation used to wait behind it until something was killed.
      try {
        expect(cli('count', 'users').stdout.trim()).toBe('4');
      } finally {
        await a.close();
      }
    });

    it('refuses past the limit, saying so, and takes the next client after a slot frees', async () => {
      const a = (await connectServer(port)).db(DB);
      const b = (await connectServer(port)).db(DB);
      try {
        // Accepted and TOLD, not left in the listen backlog looking slow.
        // One rejection, inspected once: the connection is closed behind
        // the refusal, so a second request on it races the close and
        // would be asserting on whichever won.
        const third = (await connectServer(port)).db(DB);
        let refusal = null;
        try { await third.collection('users').countDocuments({}); }
        catch (err) { refusal = err; }
        expect(refusal).toBeInstanceOf(ServerError);
        expect(refusal.code).toBe(-44);
        expect(refusal.message).toMatch(/max-clients/);
        await third.close();

        // And a client that connects and says nothing still learns why:
        // the refusal arrives before it has asked anything, which is a
        // response to no request -- kept, and raised at the first ask.
        const quiet = (await connectServer(port)).db(DB);
        await new Promise(r => setTimeout(r, 200));
        await expect(quiet.collection('users').countDocuments({}))
          .rejects.toMatchObject({ name: 'ServerError', code: -44 });
        await quiet.close();

        // The two that were already there are unharmed by the refusal.
        expect(await a.collection('users').countDocuments({})).toBe(4);
        expect(await b.collection('users').countDocuments({})).toBe(4);
      } finally {
        await a.close();
      }

      // A slot came back when `a` closed, so this one is served.
      const c = (await connectServer(port)).db(DB);
      try {
        expect(await c.collection('users').countDocuments({})).toBe(4);
      } finally {
        await c.close();
        await b.close();
      }
    });
  });

  /*
   * From nothing. Every other suite here hands the server a database
   * the JavaScript implementation wrote; this one hands it an empty
   * directory and asks it to build one -- catalog, collections,
   * documents, an index -- and then checks that what came out is a
   * database the JavaScript implementation can open.
   *
   * That round trip is the claim: two implementations, one format,
   * either of them able to create what the other reads.
   */
  describe.skipIf(!enabled)(`nisaba-server: from an empty directory (${engine.name})`, () => {
    let proc, db, dir;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc, dir } = await startServer(engine, port, [], -1));
      db = (await connectServer(port)).db(DB);
      return async () => { if (db.isOpen) await db.close(); proc.kill(); };
    });

    it('makes a database, a collection, documents and an index', async () => {
      // The ROOT had nothing in it, and naming a database made one:
      // there is no separate act of creation whose only effect is a
      // directory, the same way an insert makes a collection.
      expect(fs.existsSync(path.join(dir, DB))).toBe(false);
      expect(await db.createCollection('users')).toBe(true);
      expect(fs.existsSync(path.join(dir, DB, '__catalog__.bj'))).toBe(true);
      expect(await db.listCollections()).toEqual(['users']);
      expect(await db.createCollection('users')).toBe(false);   // idempotent

      const users = db.collection('users');
      expect(await users.countDocuments({})).toBe(0);

      await users.insertOne({ name: 'Ada', team: 'core' });
      await users.insertOne({ name: 'Grace', team: 'core' });
      await users.insertOne({ name: 'Alan', team: 'research' });

      // A collection nobody created: an insert makes one, the way it
      // does in every other host of this library.
      await db.collection('notes').insertOne({ body: 'from nothing' });
      expect(await db.collection('notes').countDocuments({})).toBe(1);

      // ...but a READ of a name that does not exist is a typo far more
      // often than an intention, and says so.
      await expect(db.collection('ghosts').countDocuments({}))
        .rejects.toMatchObject({ code: -37 });

      // The catalog's keys ARE the names, minus the reserved format
      // stamp -- which is a key in that same tree and must not appear.
      expect((await db.listCollections()).sort()).toEqual(['notes', 'users']);

      expect(await users.createIndex({ team: 1 })).toBe('team_1');
      expect((await users.listIndexes()).map(i => i.name)).toEqual(['team_1']);
      // Backfilled against the documents already there -- the only way
      // an index over a non-empty collection can answer correctly.
      expect(await users.countDocuments({ team: 'core' })).toBe(2);
      await expect(users.createIndex({ team: 1 })).rejects.toMatchObject({ code: -56 });
    });

    it('drops what it made, and refuses a name it does not have', async () => {
      const users = db.collection('users');
      await users.dropIndex('team_1');
      expect(await users.listIndexes()).toEqual([]);
      await expect(users.dropIndex('team_1')).rejects.toMatchObject({ code: -57 });
      // The documents are untouched by an index going away.
      expect(await users.countDocuments({ team: 'core' })).toBe(2);

      expect(await db.dropCollection('users')).toBe(true);
      expect(await db.dropCollection('users')).toBe(false);
      expect(await db.listCollections()).toEqual(['notes']);
      await expect(users.countDocuments({})).rejects.toMatchObject({ code: -37 });
    });

    it('leaves a database the JS implementation opens as its own', async () => {
      await db.collection('notes').createIndex({ body: 1 });
      await db.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      const provider = new NodeFSStorageProvider(path.join(dir, DB));

      // The format stamp first, straight out of the catalog: a database
      // is one format however many implementations write it, and this is
      // the only field whose absence a future version could not
      // interpret. Nothing else in this suite would notice if C stopped
      // writing it -- JS reads an unstamped database as version 1 -- so
      // it is asserted here rather than assumed.
      const catalog = new BPlusTree(await provider.openFile('__catalog__.bj', { create: false }), 32);
      await catalog.open();
      expect(catalog.search('__format__')).toEqual({ v: 1 });
      await catalog.close();

      const jsDb = await connect(provider);
      // Every collection the server made, and only those: `users` was
      // dropped in the test above.
      expect((await jsDb.listCollections()).sort()).toEqual(['notes']);
      const notes = await jsDb.collection('notes');
      expect(await notes.countDocuments({})).toBe(1);
      expect((await notes.findOne({}))?.body).toBe('from nothing');
      // Including the index, with the name C chose and the key spec it
      // reconstructed -- one naming convention, two implementations.
      expect((await notes.listIndexes()).map(i => i.name)).toEqual(['body_1']);
      await jsDb.close();
      await provider.close();
    });
  });

  /*
   * Compaction over the wire: the same plan/stream/flip/reopen/delete
   * the browser drives with an await between every step, asked for with
   * one object -- and refused while a cursor is reading the files it
   * would replace.
   */
  describe.skipIf(!enabled)(`nisaba-server: compact (${engine.name})`, () => {
    let proc, db, dir;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc, dir } = await startServer(engine, port, [], 40));
      db = (await connectServer(port)).db(DB);
      return async () => { await db.close(); proc.kill(); };
    });

    it('reclaims what an append-only file holds, and keeps every document', async () => {
      const users = db.collection('users');
      // Churn: every bulk document rewritten several times over, so the
      // file holds far more than the live set.
      for (let round = 0; round < 6; round++) {
        await users.updateMany({ team: 'bulk' }, { $set: { round } });
      }
      const before = await users.countDocuments({});
      expect(before).toBe(43);

      const res = await users.compact();
      expect(res.generation).toBe(1);
      expect(res.bytesFreed).toBeGreaterThan(0);
      expect(res.bytesAfter).toBeLessThan(res.bytesBefore);

      // The session reopened the new generation for itself -- the next
      // request is answered from it without anybody reconnecting.
      expect(await users.countDocuments({})).toBe(before);
      expect((await users.findOne({ name: 'Ada' })).team).toBe('core');
      expect(await users.countDocuments({ team: 'bulk' })).toBe(40);

      // And again: a second compaction of an already-compact collection
      // is legal, cheap, and lands on the next generation.
      expect((await users.compact()).generation).toBe(2);
    });

    it('refuses while a cursor is reading, and proceeds once it is closed', async () => {
      const users = db.collection('users');
      const cursor = users.find({}, { batchSize: 5 });
      await cursor.nextBatch();

      let err = null;
      try { await users.compact(); } catch (e) { err = e; }
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(-49);
      expect(err.message).toMatch(/compact a collection while a cursor/);

      await cursor.close();
      expect((await users.compact()).generation).toBeGreaterThan(0);
    });

    it('sweeps every collection, and skips on its own terms', async () => {
      const big = db.collection('users'), small = db.collection('quiet');
      await small.insertOne({ n: 1 });
      for (let r = 0; r < 4; r++) await big.updateMany({ team: 'bulk' }, { $set: { r } });

      // No options: unconditional, exactly like asking for each in turn.
      const all = await db.compact();
      expect(Object.keys(all).sort()).toEqual(['quiet', 'users']);
      expect(all.users.bytesFreed).toBeGreaterThan(0);

      // minBytes: nothing here is a megabyte, so nothing is worth doing.
      const none = await db.compact({ minBytes: 1_000_000 });
      expect(none).toEqual({ users: null, quiet: null });

      // factor: only what grew past twice its post-compaction size. The
      // catalog recorded that size at the flip, which is why this side
      // does not have to estimate it.
      for (let r = 0; r < 6; r++) await big.updateMany({ team: 'bulk' }, { $set: { r } });
      const grown = await db.compact({ factor: 2 });
      expect(grown.users).not.toBe(null);
      expect(grown.quiet).toBe(null);

      // skipBusy: a collection being scanned gets its turn next sweep.
      const cursor = big.find({}, { batchSize: 5 });
      await cursor.nextBatch();
      const busy = await db.compact({ skipBusy: true });
      expect(busy.users).toBe(null);
      expect(busy.quiet).not.toBe(null);
      // Without it, the same sweep is refused -- which is what a caller
      // who named one collection wants to hear.
      await expect(db.compact()).rejects.toMatchObject({ code: -49 });
      await cursor.close();

      expect(await big.countDocuments({})).toBe(43);
    });

    it('leaves a database the JS implementation can still open', async () => {
      // The whole point of the format claim: a C process rewrote every
      // file, and the JavaScript implementation reads the result.
      const users = db.collection('users');
      await users.compact();
      await db.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      const provider = new NodeFSStorageProvider(path.join(dir, DB));
      const jsDb = await connect(provider);
      const jsUsers = await jsDb.collection('users');
      expect(await jsUsers.countDocuments({})).toBe(43);
      expect((await jsUsers.find({ team: 'core' }).toArray()).length).toBe(2);
      await jsDb.close();
      await provider.close();
    });
  });

  /*
   * Cursors. 43 documents and a batch of 10, so the paging is real:
   * four full batches, a remainder, and an id that goes null exactly
   * once. What the server holds between calls is a POSITION in a B+ tree
   * scan, not a materialized result -- which is the difference between
   * paging a million documents and being sent a million documents.
   */
  describe.skipIf(!enabled)(`nisaba-server: cursors (${engine.name})`, () => {
    let proc, db;
    const port = nextPort();
    const TOTAL = 43;

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port, [], TOTAL - 3));
      db = (await connectServer(port)).db(DB);
      return async () => { await db.close(); proc.kill(); };
    });

    it('pages a scan in batches and stops without being asked twice', async () => {
      const users = db.collection('users');
      const cursor = users.find({}, { batchSize: 10 });

      const sizes = [];
      for (;;) {
        const batch = await cursor.nextBatch();
        if (!batch.length) break;
        sizes.push(batch.length);
      }
      // Four tens and a three: every document once, and the last batch
      // carried the end of the scan with it.
      expect(sizes).toEqual([10, 10, 10, 10, 3]);

      // A drained cursor is already closed on the server -- no slot left
      // behind, and no extra round trip to discover it.
      expect(await users.find({}, { batchSize: 10 }).toArray()).toHaveLength(TOTAL);
    });

    it('streams for-await, and a break gives the slot back', async () => {
      const users = db.collection('users');
      const seen = [];
      for await (const doc of users.find({ team: 'bulk' }, { batchSize: 7 })) {
        seen.push(doc.n);
        if (seen.length === 12) break;       // walk away mid-scan
      }
      // Twelve distinct documents: a scan repeats nothing and skips
      // nothing across a batch boundary (7 does not divide 12). Not in
      // insertion order -- a scan is in _id order, and ObjectIds are not
      // handed out in the order documents are written.
      expect(seen).toHaveLength(12);
      expect(new Set(seen).size).toBe(12);
      expect(seen.every(n => n >= 0 && n < TOTAL - 3)).toBe(true);

      // Had the abandoned cursor leaked, this would eventually fail to
      // open one: the server's table is bounded. Twenty rounds of it.
      for (let i = 0; i < 20; i++) {
        const c = users.find({}, { batchSize: 1 });
        expect(await c.nextBatch()).toHaveLength(1);
        await c.close();
      }
      expect(await users.countDocuments({})).toBe(TOTAL);
    });

    it('refuses to batch a sorted find, and says why', async () => {
      const users = db.collection('users');
      let err = null;
      try { await users.find({}, { sort: { name: 1 }, batchSize: 5 }).toArray(); }
      catch (e) { err = e; }
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(-48);
      expect(err.message).toMatch(/sorted find cannot be batched/);

      // Sorted and whole is fine -- that is what a sorted find is.
      const sorted = await users.find({ team: 'core' }, { sort: { name: 1 } }).toArray();
      expect(sorted.map(d => d.name)).toEqual(['Ada', 'Grace']);
    });

    it('will not let one connection touch another connection cursor', async () => {
      const other = (await connectServer(port)).db(DB);
      try {
        // Raw, so the test holds the server's actual id -- the client
        // keeps it to itself, and a live id is the only thing that can
        // tell "not yours" apart from "no such cursor".
        const opened = await db.request({ op: 'find', coll: 'users', opts: { batchSize: 5 } });
        expect(opened.cursor).toBeGreaterThan(0);

        await expect(other.request({ op: 'getMore', cursor: opened.cursor }))
          .rejects.toMatchObject({ name: 'ServerError', code: -46 });

        // Still the owner's, and still where it was: the refusal did not
        // advance it.
        const next = await db.request({ op: 'getMore', cursor: opened.cursor });
        expect(next.docs).toHaveLength(5);
        await db.request({ op: 'closeCursor', cursor: opened.cursor });
      } finally {
        await other.close();
      }
    });

    it('drops the cursors of a connection that goes away', async () => {
      // Fill the server's table from a connection that then disappears.
      const doomed = (await connectServer(port)).db(DB);
      for (let i = 0; i < 16; i++) {
        const c = doomed.collection('users').find({}, { batchSize: 1 });
        await c.nextBatch();
      }
      // Full: the table is 16 and this one connection is holding all of it.
      await expect(doomed.collection('users').find({}, { batchSize: 1 }).nextBatch())
        .rejects.toMatchObject({ code: -47 });

      await doomed.close();
      await new Promise(r => setTimeout(r, 100));

      // The slots came back with the connection, without anyone asking.
      const c = db.collection('users').find({}, { batchSize: 1 });
      expect(await c.nextBatch()).toHaveLength(1);
      await c.close();
    });
  });

  /*
   * Change streams: the only frames on this wire that answer nothing.
   * What matters here and cannot be checked in C is the ROUTING -- that
   * a client tells an event from an answer by shape, that events and
   * answers interleave on one socket without either being mistaken for
   * the other, and that a watcher hears about writes it did not make.
   */
  describe.skipIf(!enabled)(`nisaba-server: change streams (${engine.name})`, () => {
    let proc, db, other;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port));
      db = (await connectServer(port)).db(DB);
      other = (await connectServer(port)).db(DB);
      return async () => { await db.close(); await other.close(); proc.kill(); };
    });

    /** Wait for the watcher to have been registered server-side. */
    const settle = () => new Promise(r => setTimeout(r, 120));

    it('tells a watcher what another connection wrote', async () => {
      const seen = [];
      const stream = db.collection('notes').watch();
      stream.on('change', (c) => seen.push(`${c.operationType}:${c.fullDocument?.body ?? ''}`));
      await settle();

      const w = other.collection('notes');   // a different connection
      await w.insertOne({ body: 'first' });
      await w.updateOne({ body: 'first' }, { $set: { body: 'edited' } });
      await w.replaceOne({ body: 'edited' }, { body: 'replaced' });
      await w.deleteOne({ body: 'replaced' });
      await settle();

      // An update names its changes, not its outcome, so the event
      // carries the document as it now is -- read back by the id the
      // command named.
      expect(seen).toEqual([
        'insert:first', 'update:edited', 'replace:replaced', 'delete:'
      ]);
      await stream.close();
    });

    it('carries the collection, and only that collection', async () => {
      const stream = db.collection('watched').watch();
      await settle();
      await other.collection('watched').insertOne({ n: 1 });
      await other.collection('ignored').insertOne({ n: 2 });
      await settle();

      const first = await stream.next();
      expect(first.value.ns).toEqual({ coll: 'watched' });
      expect(first.value.operationType).toBe('insert');
      // The other collection's write is not in the queue behind it.
      const race = await Promise.race([
        stream.next().then(() => 'another'),
        new Promise(r => setTimeout(() => r('nothing else'), 200))
      ]);
      expect(race).toBe('nothing else');
      await stream.close();
    });

    it('keeps answers and events apart on one socket', async () => {
      const stream = db.collection('mixed').watch();
      const seen = [];
      stream.on('change', (c) => seen.push(c.operationType));
      await settle();

      // Ordinary requests down the same connection, interleaved with
      // events caused by the other one. Every answer must still be the
      // answer to its own question.
      await other.collection('mixed').insertMany([{ i: 1 }, { i: 2 }, { i: 3 }]);
      expect(await db.collection('mixed').countDocuments({})).toBe(3);
      await other.collection('mixed').deleteMany({ i: { $lt: 3 } });
      expect(await db.collection('mixed').countDocuments({})).toBe(1);
      expect(await db.collection('mixed').distinct('i')).toEqual([3]);
      await settle();

      expect(seen).toEqual(['insert', 'insert', 'insert', 'delete', 'delete']);
      await stream.close();
    });

    it('watches a collection that does not exist yet', async () => {
      // The insert that creates it is the event a watcher most wants.
      const stream = db.collection('unborn').watch();
      await settle();
      await other.collection('unborn').insertOne({ hello: 'world' });
      const { value } = await stream.next();
      expect(value.operationType).toBe('insert');
      expect(value.fullDocument.hello).toBe('world');
      await stream.close();
    });

    it('stops when closed, and when its connection goes', async () => {
      const stream = db.collection('stopping').watch();
      await settle();
      await stream.close();
      await other.collection('stopping').insertOne({ n: 1 });
      await settle();
      expect(await stream.next()).toEqual({ value: undefined, done: true });

      // A stream is its connection's: losing one ends the other.
      const doomed = (await connectServer(port)).db(DB);
      const orphan = doomed.collection('stopping').watch();
      await settle();
      const pending = orphan.next();
      await doomed.close();
      await expect(pending).rejects.toThrow(/closed/);
    });

    it('gives up on a consumer that stopped reading, and says so', async () => {
      // A raw socket, so it can stop reading -- which the client never
      // does. The server holds a bounded queue per stream and closes the
      // stream rather than growing it, which is the in-process contract
      // too (there are no resume tokens to offer instead).
      const sock = net.connect(port, '127.0.0.1');
      await new Promise(r => sock.once('connect', r));
      const frames = [];
      let buf = Buffer.alloc(0);
      sock.on('data', (chunk) => {
        buf = Buffer.concat([buf, chunk]);
        for (;;) {
          if (buf.length < 5) return;
          const total = buf.readUInt32LE(1) + 5;
          if (buf.length < total) return;
          frames.push(decode(buf.subarray(0, total)));
          buf = buf.subarray(total);
        }
      });
      sock.write(Buffer.from(encode({ db: DB, op: 'watch', coll: 'flood' })));
      await settle();
      expect(frames.shift()).toMatchObject({ ok: true });
      sock.pause();                                   // and stop reading

      // One request, deliberately: the session's queue fills DURING it,
      // before the transport gets a chance to move anything, so this
      // asserts the session's bound without depending on how much a
      // kernel will buffer for a socket nobody is reading. (The
      // transport's own bound -- it stops handing events to a connection
      // holding OUT_HIGH_WATER unsent bytes -- is what keeps that
      // backlog from moving into an uncounted buffer instead. Reaching
      // it end-to-end means first filling the OS buffers, which are an
      // order of magnitude bigger on Linux than on macOS, so it is
      // falsified by hand rather than asserted here.)
      await other.collection('flood').insertMany(
        Array.from({ length: 1500 }, (_, i) => ({ i })));
      await settle();
      sock.resume();
      await new Promise(r => setTimeout(r, 800));

      const overflow = frames.find(f => f.overflow !== undefined);
      expect(overflow).toBeDefined();
      expect(overflow.overflow).toBe(true);   // a flag: any count would be a lie
      // Everything it managed to hold came first, in order.
      expect(frames.indexOf(overflow)).toBeGreaterThan(100);
      sock.destroy();
    });

    it('surfaces an overflow to the real client as a thrown error', async () => {
      // The client never stops reading of its own accord, so something
      // else must stop it: spawnSync blocks this event loop outright
      // while a separate process does the writing. Nothing is read off
      // the socket for the duration, which is exactly the condition the
      // server's bound exists for.
      const stream = db.collection('blocked').watch();
      await settle();
      const docs = JSON.stringify(Array.from({ length: 1500 }, (_, i) => ({ i })));
      const wrote = spawnSync(process.execPath, [
        'bin/db.js', '--server', `127.0.0.1:${port}`, DB, 'insert-many', 'blocked', docs
      ], { encoding: 'utf8', maxBuffer: 32 * 1024 * 1024 });
      expect(wrote.status).toBe(0);

      // What it managed to hold arrives first -- those events are still
      // true -- and then the news that the rest is gone.
      let delivered = 0;
      let err = null;
      try {
        for (let i = 0; i < 2000; i++) {
          const { done } = await stream.next();
          if (done) break;
          delivered++;
        }
      } catch (e) { err = e; }

      expect(err).toBeInstanceOf(ChangeStreamOverflowError);
      expect(err.message).toMatch(/watch\(\) again and re-read/);
      expect(delivered).toBeGreaterThan(100);
      expect(delivered).toBeLessThan(1500);   // it did lose some
    });
  });

  /*
   * Lists of writes. insertMany and bulkWrite are different operations --
   * one list holds documents, the other holds writes of six kinds -- but
   * they are one round trip each and they fail the same way, which is
   * the part a socket makes matter. The engine-side rules are pinned in
   * test/native/main.c; what is checked here is that a client with no
   * engine in it reconstructs the same results and the same throws the
   * in-process driver produces.
   */
  describe.skipIf(!enabled)(`nisaba-server: lists of writes (${engine.name})`, () => {
    let proc, db, dir;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc, dir } = await startServer(engine, port));
      db = (await connectServer(port)).db(DB);
      return async () => { await db.close(); proc.kill(); };
    });

    it('inserts a whole array in one round trip, ids and all', async () => {
      const c = db.collection('teams');   // nobody made this: an insert does
      const { acknowledged, insertedCount, insertedIds } =
        await c.insertMany([{ n: 1 }, { n: 2 }, { n: 3 }]);
      expect(acknowledged).toBe(true);
      expect(insertedCount).toBe(3);
      expect(Object.keys(insertedIds)).toEqual(['0', '1', '2']);
      expect(insertedIds[0]).toBeInstanceOf(ObjectId);

      // The ids in the result are the ids in the database: this side
      // minted them and the server wrote exactly those.
      const found = await c.findOne({ _id: insertedIds[2] });
      expect(found.n).toBe(3);
      expect(await c.countDocuments({})).toBe(3);
    });

    it('stops where ordered says to, and reports what landed', async () => {
      const c = db.collection('ordered');
      const dupe = new ObjectId();

      let err = null;
      try { await c.insertMany([{ _id: dupe, i: 0 }, { _id: dupe, i: 1 }, { i: 2 }]); }
      catch (e) { err = e; }
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(-10);                       // duplicate _id
      expect(err.message).toMatch(/insertMany, document 1/);
      // The partial result is the driver's contract: what DID land.
      expect(err.result).toMatchObject({ acknowledged: true, insertedCount: 1 });
      // And the third document was never attempted, because the first
      // failure ended the run.
      expect(await c.countDocuments({})).toBe(1);

      // Unordered attempts every document; the throw still names the
      // first failure, exactly as the in-process insertMany does.
      const other = new ObjectId();
      err = null;
      try {
        await c.insertMany([{ _id: other, i: 3 }, { _id: other, i: 4 }, { i: 5 }],
                           { ordered: false });
      } catch (e) { err = e; }
      expect(err.code).toBe(-10);
      expect(await c.countDocuments({})).toBe(3);       // 3 and 5 both landed
    });

    it('runs six kinds of write in one frame and sums what they did', async () => {
      const c = db.collection('users');
      const result = await c.bulkWrite([
        { insertOne: { document: { name: 'Edsger', team: 'core' } } },
        { updateOne: { filter: { name: 'Ada' }, update: { $set: { onCall: true } } } },
        { updateMany: { filter: { team: 'core' }, update: { $set: { seen: 1 } } } },
        { deleteOne: { filter: { name: 'Alan' } } },
        { replaceOne: { filter: { name: 'Grace' }, replacement: { name: 'Grace', team: 'core' } } },
        { updateOne: { filter: { name: 'Nobody' }, update: { $set: { team: 'new' } }, upsert: true } }
      ]);

      expect(result.insertedCount).toBe(1);
      expect(result.deletedCount).toBe(1);
      expect(result.upsertedCount).toBe(1);
      expect(result.matchedCount).toBeGreaterThanOrEqual(4);
      expect(result.insertedIds[0]).toBeInstanceOf(ObjectId);
      // The upserted id is the SERVER's answer, not a guess from here.
      expect(result.upsertedIds[5]).toBeInstanceOf(ObjectId);
      expect(result.insertedIds[5]).toBeUndefined();     // upserted, not inserted

      expect((await c.findOne({ name: 'Ada' })).onCall).toBe(true);
      expect(await c.findOne({ name: 'Alan' })).toBe(null);
      expect((await c.findOne({ _id: result.upsertedIds[5] })).team).toBe('new');
    });

    it('refuses a malformed list whole, naming the operation that was wrong', async () => {
      const c = db.collection('users');
      const before = await c.countDocuments({});

      let err = null;
      try {
        await c.bulkWrite([
          { insertOne: { document: { name: 'Never' } } },
          { obliterateOne: { filter: {} } }
        ]);
      } catch (e) { err = e; }
      expect(err).toBeInstanceOf(ServerError);
      expect(err.code).toBe(-21);
      expect(err.index).toBe(1);
      expect(err.message).toMatch(/\(operation 1\)/);

      // Nothing ran. That is the whole reason the grammar is checked
      // before the list is: an unordered run must be able to attempt
      // every operation.
      expect(await c.countDocuments({})).toBe(before);
      expect(await c.findOne({ name: 'Never' })).toBe(null);
    });

    it('carries a partial failure the way the in-process bulkWrite does', async () => {
      const c = db.collection('partial');
      const dupe = new ObjectId();
      await c.insertOne({ _id: dupe, keep: true });

      let err = null;
      try {
        await c.bulkWrite([
          { insertOne: { document: { _id: dupe, i: 1 } } },
          { insertOne: { document: { i: 2 } } }
        ], { ordered: false });
      } catch (e) { err = e; }

      expect(err.message).toMatch(/bulkWrite: 1 operation\(s\) failed \(first at index 0/);
      expect(err.writeErrors).toHaveLength(1);
      expect(err.writeErrors[0].index).toBe(0);
      expect(err.writeErrors[0].error).toBeInstanceOf(ServerError);
      expect(err.result.insertedCount).toBe(1);         // the second one landed
      expect(err.result.insertedIds[1]).toBeInstanceOf(ObjectId);
      expect(await c.countDocuments({})).toBe(2);

      // Ordered, the same list stops at the failure -- and the insert
      // that never ran is not in insertedIds. This is what `attempted`
      // is for: without it a client would report an id for an operation
      // the server never reached.
      err = null;
      try {
        await c.bulkWrite([
          { insertOne: { document: { _id: dupe, i: 3 } } },
          { insertOne: { document: { i: 4 } } }
        ]);
      } catch (e) { err = e; }
      expect(err.writeErrors).toHaveLength(1);
      expect(err.result.insertedCount).toBe(0);
      expect(err.result.insertedIds).toEqual({});
      expect(await c.countDocuments({})).toBe(2);
    });

    it('dates every member of a list from one clock reading', async () => {
      const c = db.collection('dated');
      await c.insertMany([{ i: 1 }, { i: 2 }]);
      await c.bulkWrite([
        { updateOne: { filter: { i: 1 }, update: { $currentDate: { at: true } } } },
        { updateOne: { filter: { i: 2 }, update: { $currentDate: { at: true } } } }
      ]);
      const [one, two] = [await c.findOne({ i: 1 }), await c.findOne({ i: 2 })];
      expect(one.at).toBeInstanceOf(Date);
      // Two members dating the same field agree about when it was: one
      // reading for the request, not one per operation.
      expect(two.at.getTime()).toBe(one.at.getTime());

      // And a list that needs a clock and was sent without one is
      // refused whole, before any of it runs -- like every other rule
      // checked up front.
      await expect(db.request({
        op: 'bulkWrite', coll: 'dated',
        writes: [{ updateOne: { filter: { i: 1 }, update: { $currentDate: { later: true } } } }]
      })).rejects.toMatchObject({ code: -42 });
      expect((await c.findOne({ i: 1 })).later).toBeUndefined();
    });

    it('leaves a database the JS implementation reads the same way', async () => {
      await db.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      const provider = new NodeFSStorageProvider(path.join(dir, DB));
      const jsDb = await connect(provider);
      expect(await (await jsDb.collection('teams')).countDocuments({})).toBe(3);
      expect(await (await jsDb.collection('ordered')).countDocuments({})).toBe(3);
      const users = await jsDb.collection('users');
      expect((await users.findOne({ name: 'Edsger' })).team).toBe('core');
      await jsDb.close();
      await provider.close();
    });
  });
}

/**
 * Resumable change streams (roadmap step 6, documented in
 * docs/db-server.md): the log index IS the resume token.
 *
 * A group of ONE, deliberately: `--raft 1` gives the server a real entry
 * log -- which is what makes a watch resumable -- while election and
 * barrier mechanics stay out of the way (a group of one leads instantly
 * and a read is answered outright). The cluster half of the story is the
 * front end's suite, where a deferred watch answer and the replay have
 * to keep their order across a socket.
 *
 * Absolute log indexes are never asserted: the log also carries the
 * entries Raft itself writes (a bootstrap CONFIG, a term-opening no-op),
 * so a test that assumed "first insert is entry 1" would be asserting an
 * implementation detail. Tokens come from events, which is where a
 * consumer gets them.
 */
describe.each(ENGINES.filter((e) => e.ready()))(
  'nisaba-server: resumable change streams, --raft 1 ($name)', (engine) => {
  const port = nextPort();
  let proc, client, db;

  beforeAll(async () => {
    ({ proc } = await startServer(engine, port, ['--raft', '1'], -1));
    client = await connectServer(port);
    db = client.db('streams');
    return async () => {
      await client.close();
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
    };
  }, 60000);

  it('events carry the log index, and the watch reply says where live begins', async () => {
    const coll = db.collection('marked');
    const w = coll.watch();
    await w.ready;
    // The subscribe position: "you are here", before any event.
    expect(typeof w.resumeFrom).toBe('number');
    const before = w.resumeFrom;

    await coll.insertOne({ n: 1 });
    const { value: ev } = await w.next();
    expect(ev.operationType).toBe('insert');
    expect(typeof ev.index).toBe('number');
    expect(ev.index).toBeGreaterThan(before);
    // The stream keeps the token so a consumer does not have to.
    expect(w.resumeFrom).toBe(ev.index);
    await w.close();
  });

  it('resumes from a token: everything after it, exactly once, in order, then live', async () => {
    const coll = db.collection('resume');
    await coll.insertOne({ n: 1 });
    await coll.insertOne({ n: 2 });
    await coll.insertOne({ n: 3 });

    // From the log's start: the full history replays, in log order.
    const all = coll.watch({ from: 0 });
    const events = [];
    for (let i = 0; i < 3; i++) events.push((await all.next()).value);
    expect(events.map((e) => e.fullDocument.n)).toEqual([1, 2, 3]);
    await all.close();

    // From the first event's token: only what came after it.
    const later = coll.watch({ from: events[0].index });
    expect((await later.next()).value.fullDocument.n).toBe(2);
    expect((await later.next()).value.fullDocument.n).toBe(3);

    // And the stream is LIVE after the replay: a new write follows the
    // replayed ones on the same stream, in order.
    await coll.insertOne({ n: 4 });
    expect((await later.next()).value.fullDocument.n).toBe(4);
    await later.close();
  });

  it('replays an update with the document as it NOW stands (the stated contract)', async () => {
    const coll = db.collection('images');
    const { insertedId } = await coll.insertOne({ n: 1 });
    await coll.updateOne({ _id: insertedId }, { $set: { n: 2 } });
    await coll.updateOne({ _id: insertedId }, { $set: { n: 3 } });

    const w = coll.watch({ from: 0 });
    const first = (await w.next()).value;
    expect(first.operationType).toBe('insert');
    expect(first.fullDocument.n).toBe(1);      // the log carries the insert whole
    const up = (await w.next()).value;
    expect(up.operationType).toBe('update');
    // updateLookup semantics: the replayed update's image is current,
    // not as-of -- db_session.h says so, and this asserts it stays said.
    expect(up.fullDocument.n).toBe(3);
    await w.close();
  });

  it('refuses a token from the future: it is not this log\'s', async () => {
    const w = db.collection('resume').watch({ from: 999999 });
    await expect(w.ready).rejects.toMatchObject({ code: -69 });
  });

  it('pages a long history through the bounded queue: overflow carries the resume point', async () => {
    const coll = db.collection('paged');
    const docs = Array.from({ length: 300 }, (_, i) => ({ n: i }));
    /* Two batches, not one: a single proposal is bounded by the node's
     * await table (RN_MAX_AWAIT, 256), which is a fact about proposing,
     * not about replaying -- the log ends up with 300 entries either
     * way, which is what this test needs: more than one stream queue
     * (DBS_STREAM_EVENTS, 256) can hold. */
    await coll.insertMany(docs.slice(0, 150));
    await coll.insertMany(docs.slice(150));

    // The stream's queue holds 256 events (DBS_STREAM_EVENTS), so a
    // 300-event replay overflows -- which with a token is a PAGE
    // BOUNDARY: everything queued is delivered first, the overflow
    // names the last delivered index, and resuming from it misses
    // nothing.
    const got = [];
    let resumeAt = null;
    const first = coll.watch({ from: 0 });
    try {
      for await (const c of first) got.push(c);
    } catch (err) {
      expect(err).toBeInstanceOf(ChangeStreamOverflowError);
      resumeAt = err.resumeFrom;
    }
    expect(got.length).toBe(256);
    expect(resumeAt).toBe(got[got.length - 1].index);

    const rest = coll.watch({ from: resumeAt });
    while (got.length < docs.length) got.push((await rest.next()).value);
    await rest.close();
    expect(got.map((c) => c.fullDocument.n)).toEqual(docs.map((d) => d.n));
  });
});

/* The refusal an unreplicated server owes a resume: there is no log, and
 * saying so is the contract -- a watch without `from` still works there,
 * and its events carry no index because no log minted one. */
describe.each(ENGINES.filter((e) => e.ready()))(
  'nisaba-server: resume without a log is refused ($name)', (engine) => {
  const port = nextPort();
  let proc, client, db;

  beforeAll(async () => {
    ({ proc } = await startServer(engine, port, [], -1));
    client = await connectServer(port);
    db = client.db('nolog');
    return async () => {
      await client.close();
      proc.kill();
      await new Promise((r) => proc.once('exit', r));
    };
  }, 60000);

  it('watch({from}) is -67; a plain watch still works, unindexed', async () => {
    const refused = db.collection('things').watch({ from: 0 });
    await expect(refused.ready).rejects.toMatchObject({ code: -67 });

    const live = db.collection('things').watch();
    await live.ready;
    expect(live.resumeFrom).toBeNull();   // no log, no token
    await db.collection('things').insertOne({ n: 1 });
    const { value: ev } = await live.next();
    expect(ev.operationType).toBe('insert');
    expect(ev.index).toBeUndefined();
    await live.close();
  });
});
