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
  const [cmd, args, opts] = engine.argv(await seedDb(docs), port, extra);
  const proc = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], ...opts });
  return new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`${engine.name} server did not start`)), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve(proc); }
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
      proc = await startServer(engine, port);
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

    it('turns a refusal into an error carrying the code and the sentence', async () => {
      await expect(db.collection('ghosts').countDocuments({})).rejects.toThrow(ServerError);
      await expect(db.collection('ghosts').countDocuments({})).rejects.toMatchObject({ code: -37 });
    });

    it('says what the wire does not carry, rather than failing as a TypeError', () => {
      expect(() => db.listCollections()).toThrow(/no listCollections/);
      expect(() => db.collection('users').createIndex({ team: 1 })).toThrow(/no createIndex/);
      // The sentence names the ops that DO exist, so the refusal is
      // actionable without reading the source.
      expect(() => db.collection('users').watch()).toThrow(WIRE_OPS.join(', '));
    });
  });

  describe.skipIf(!enabled)(`nisaba-server: bin/db.js as a client (${engine.name})`, () => {
    let proc;
    const port = nextPort();
    const cli = (...args) => spawnSync(process.execPath, [
      'bin/db.js', '--server', `127.0.0.1:${port}`, ...args
    ], { encoding: 'utf8' });

    beforeAll(async () => {
      proc = await startServer(engine, port);
      return () => { proc.kill(); };
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

    it('refuses, from the CLI, what only a local database can do', () => {
      const listed = cli('collections');
      expect(listed.status).toBe(1);
      expect(listed.stderr).toMatch(/no listCollections/);

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
      proc = await startServer(engine, port, ['--idle-timeout', '1']);
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
      proc = await startServer(engine, port, ['--max-clients', '2']);
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
      proc = await startServer(engine, port, [], TOTAL - 3);
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
        await db.request({ op: 'killCursor', cursor: opened.cursor });
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
}
