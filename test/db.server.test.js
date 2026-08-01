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
import { connect, ObjectId } from '../src/db.js';
import { NodeFSStorageProvider } from '../src/db-node.js';
import { connectServer, ServerError, WIRE_OPS } from '../src/db-server-client.js';
import { BPlusTree } from '../wasm/nisaba-wasm.js';

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

/** A database with three people in it, written by the JS implementation --
 * so the server under test is opening somebody else's files. */
async function seedDb(extra = 0) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-server-'));
  const provider = new NodeFSStorageProvider(dir);
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
  await provider.close();
  return dir;
}

/** Queue of requests over one duplex pair, resolved in order. */
function client(write, onData) {
  const waiting = [];
  onData(framer((value) => {
    const next = waiting.shift();
    if (next) next(value);
  }));
  return (req) => new Promise((resolve) => {
    waiting.push(resolve);
    write(Buffer.from(encode(req)));
  });
}

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
    const provider = new NodeFSStorageProvider(dir);
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

async function startServer(engine, port, extra = [], docs = 0) {
  // docs < 0: an EMPTY directory -- no catalog, no collection, nothing.
  // The server makes the database itself, which is the whole point of
  // the suite that asks for it.
  const dir = docs < 0
    ? fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-empty-'))
    : await seedDb(docs);
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

describe.runIf(REQUIRED)('nisaba-server: the artifacts CI promises', () => {
  it.each(ENGINES)('$name is built and runnable', (engine) => {
    expect(engine.ready()).toBe(true);
  });
});

for (const engine of ENGINES) {
  const enabled = engine.ready();

  describe.skipIf(!enabled)(`nisaba-server: the JS client (${engine.name})`, () => {
    let proc, db;
    const port = nextPort();

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port));
      db = await connectServer(port);   // a bare port means loopback
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
      expect(() => db.collection('users').watch()).toThrow(/no collection\.watch/);
      // The sentence names the ops that DO exist, so the refusal is
      // actionable without reading the source.
      expect(() => db.collection('users').watch()).toThrow(WIRE_OPS.join(', '));
      // And an op that IS on the wire, asked of the wrong thing, says
      // that instead of listing `compact` as available while refusing
      // compact -- which is what it used to do.
      expect(() => db.compact()).toThrow(/compact is a collection operation/);
    });
  });

  describe.skipIf(!enabled)(`nisaba-server: bin/db.js as a client (${engine.name})`, () => {
    let proc;
    const port = nextPort();
    const cli = (...args) => spawnSync(process.execPath, [
      'bin/db.js', '--server', `127.0.0.1:${port}`, ...args
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
        'bin/db.js', '--server', `127.0.0.1:${port}`, 'restore'
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

    it('refuses, from the CLI, what only a local database can do', () => {
      // `watch`, not `prune-expired`: TTL sweeps are on the wire now.
      // What is left is the one command that cannot be an op at all --
      // it needs frames the client did not ask for.
      const watched = cli('watch', 'users');
      expect(watched.status).toBe(1);
      expect(watched.stderr).toMatch(/no collection\.watch/);

      // --order is the server's, decided when it opened the directory.
      const ordered = cli('count', 'users', '--order', '64');
      expect(ordered.status).toBe(1);
      expect(ordered.stderr).toMatch(/--order is the server's/);

      // And an address with nothing behind it names the address.
      const nowhere = spawnSync(process.execPath, ['bin/db.js', '--server', '127.0.0.1:1', 'count', 'users'], { encoding: 'utf8' });
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
      const quiet = await connectServer(port, { keepAliveMs: 0 });
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
        expect(await warm.collection('users').countDocuments({})).toBe(3);
        expect(await warm.ping()).toBe(true);
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
      'bin/db.js', '--server', `127.0.0.1:${port}`, ...args
    ], { encoding: 'utf8' });

    beforeAll(async () => {
      ({ proc } = await startServer(engine, port, ['--max-clients', '2']));
      return () => { proc.kill(); };
    });

    it('serves two connections at once, and a CLI while one of them sits idle', async () => {
      const a = await connectServer(port);
      const b = await connectServer(port);
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
      const a = await connectServer(port);
      const b = await connectServer(port);
      try {
        // Accepted and TOLD, not left in the listen backlog looking slow.
        // One rejection, inspected once: the connection is closed behind
        // the refusal, so a second request on it races the close and
        // would be asserting on whichever won.
        const third = await connectServer(port);
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
        const quiet = await connectServer(port);
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
      const c = await connectServer(port);
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
      db = await connectServer(port);
      return async () => { if (db.isOpen) await db.close(); proc.kill(); };
    });

    it('makes a database, a collection, documents and an index', async () => {
      // The directory had no catalog: starting the server made one.
      expect(fs.existsSync(path.join(dir, '__catalog__.bj'))).toBe(true);

      expect(await db.createCollection('users')).toBe(true);
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

      const provider = new NodeFSStorageProvider(dir);

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
      db = await connectServer(port);
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

    it('leaves a database the JS implementation can still open', async () => {
      // The whole point of the format claim: a C process rewrote every
      // file, and the JavaScript implementation reads the result.
      const users = db.collection('users');
      await users.compact();
      await db.close();
      proc.kill();
      await new Promise(r => proc.once('exit', r));

      const provider = new NodeFSStorageProvider(dir);
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
      db = await connectServer(port);
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
      const other = await connectServer(port);
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
      const doomed = await connectServer(port);
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
      db = await connectServer(port);
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

      const provider = new NodeFSStorageProvider(dir);
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
