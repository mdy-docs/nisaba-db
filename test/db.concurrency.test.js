/**
 * test/db.concurrency.test.js — many clients at once, and deep pipelines.
 *
 * WHY THIS FILE EXISTS. Before it, the most this suite ever asked of one
 * server was TWO requests in flight (`db.server.test.js`'s "many clients,
 * bounded" opens two connections and issues one `countDocuments` on each).
 * Everything else that looks concurrent is one connection per cluster
 * member across separate processes. So the whole question of what happens
 * when a server is genuinely busy -- pipelined requests behind each other,
 * reads arriving while a write commits, a cursor paging while its
 * collection is written to -- was untested.
 *
 * That was survivable while `server/main.c` served one request at a time
 * on one thread and said so ("There are no threads and there is no second
 * engine"). It stops being survivable the moment anything moves off that
 * thread, and this file is the net that has to exist FIRST -- green
 * against today's serial server, so that the first red light after a
 * threading change is a real finding rather than a hole nobody had
 * looked in.
 *
 * WHAT IT ASSERTS, AND WHY EACH ONE IS CHECKABLE
 *
 * Every claim here is a property with an oracle, not "it did not crash":
 *
 *  - ANSWERS PAIR WITH REQUESTS. The wire carries no request ids; the
 *    client pairs answers to requests by arrival order (see
 *    db-server-client.js, and main.c's note on the keepalive ping that
 *    once came back as an insert's answer). So every request here asks
 *    for something only IT could be answered with, and mispairing shows
 *    up as a wrong value rather than as a hang.
 *  - READS SEE WHOLE STATES. A reader racing a writer may see the write
 *    or not see it; what it may never see is half of it. The writer
 *    keeps a document and an index entry in step, so a torn read is
 *    detectable without a reference implementation.
 *  - READS ARE PREFIX-CONSISTENT. With ids inserted in increasing order,
 *    any snapshot must be a prefix: seeing id N means seeing every id
 *    below it. A gap is a real bug; being behind is not.
 *
 * Run it against a sanitized server for the version that finds memory
 * bugs too:
 *
 *   ./build/build-server.sh --native --san
 *   NISABA_SERVER_BIN=build/lib/nisaba-server-asan npx vitest run test/db.concurrency.test.js
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer, ObjectId } from '../src/db-server-client.js';

const NATIVE = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const have = (p) => fs.existsSync(p);
const REQUIRED = process.env.NISABA_SERVER_TESTS === 'required';
const enabled = have(NATIVE);
if (REQUIRED && !enabled) {
  throw new Error(`NISABA_SERVER_TESTS=required but ${NATIVE} is missing`);
}

const DB = 'busy';
/* Clear of db.server.test.js's 18000 block and db.http-front.test.js's
 * 40000 one; those two already meet at the top of their ranges, so this
 * file takes a small fixed window of its own rather than a slot scheme
 * that could grow into either. */
const PORT = 33000 + (process.pid % 900);

async function startServer(port, extra = []) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-conc-'));
  const proc = spawn(path.resolve(NATIVE), ['--port', String(port), ...extra], {
    cwd: dir, stdio: ['ignore', 'pipe', 'pipe']
  });
  let err = '';
  proc.stderr.on('data', (d) => { err += String(d); });
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`server did not start: ${err}`)), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve(); }
    });
  });
  return { proc, dir, stderr: () => err };
}

describe.skipIf(!enabled)('a busy server: many clients, deep pipelines', () => {
  let proc, dir, stderr;

  beforeAll(async () => {
    /* --max-clients at its ceiling: this file's whole point is to get
     * near the bounds rather than to prove a small number works. */
    ({ proc, dir, stderr } = await startServer(PORT, ['--max-clients', '64']));
  }, 60000);

  afterAll(async () => {
    proc?.kill();
    if (dir) fs.rmSync(dir, { recursive: true, force: true });
  });

  /**
   * N requests issued without awaiting any of them: the client holds a
   * FIFO of pending resolvers and the server answers in arrival order,
   * so this is the deepest pipeline the protocol allows.
   */
  it('answers a deep pipeline in order, every answer matching its own request', async () => {
    const c = await connectServer(PORT);
    try {
      const coll = c.db(DB).collection('pipe');
      const N = 200;
      // Seed distinguishable documents: doc i is the only one with n === i.
      for (let i = 0; i < N; i += 50) {
        await coll.insertMany(Array.from({ length: 50 }, (_, k) => ({ n: i + k })));
      }

      // Fire everything, await nothing, then check each answer against
      // the request that asked for it.
      const asked = Array.from({ length: N }, (_, i) => i);
      const answers = await Promise.all(asked.map((i) => coll.findOne({ n: i })));
      for (const i of asked) {
        expect(answers[i]?.n, `answer ${i} paired with the wrong request`).toBe(i);
      }
    } finally { await c.close(); }
  }, 60000);

  it('serves many connections at once, each seeing only its own answers', async () => {
    /* One collection per connection, and each asks only about its own:
     * a mispaired answer across connections is then a wrong document,
     * not a plausible one. */
    const CONNS = 24, PER = 25;
    const clients = await Promise.all(
      Array.from({ length: CONNS }, () => connectServer(PORT)));
    try {
      await Promise.all(clients.map(async (c, ci) => {
        const coll = c.db(DB).collection(`own-${ci}`);
        await coll.insertMany(Array.from({ length: PER }, (_, k) => ({ who: ci, n: k })));
      }));

      // Every connection pipelines its whole read set at once.
      await Promise.all(clients.map(async (c, ci) => {
        const coll = c.db(DB).collection(`own-${ci}`);
        const docs = await Promise.all(
          Array.from({ length: PER }, (_, k) => coll.findOne({ n: k })));
        for (let k = 0; k < PER; k++) {
          expect(docs[k]?.who, `connection ${ci} got another connection's document`).toBe(ci);
          expect(docs[k]?.n).toBe(k);
        }
      }));
    } finally {
      await Promise.all(clients.map((c) => c.close()));
    }
  }, 120000);

  /*
   * The one that matters most for anything that later moves reads off
   * the writer's thread: a reader may be BEHIND a write, and may never
   * be INSIDE one.
   */
  it('a read racing a write sees a whole state, never half of one', async () => {
    const w = await connectServer(PORT);
    const readers = await Promise.all(
      Array.from({ length: 6 }, () => connectServer(PORT)));
    try {
      const writes = w.db(DB).collection('torn');
      /* Each document carries the same fact twice. A writer that is
       * halfway through is the only way they can disagree, so equality
       * is the torn-read oracle and it needs no reference run. */
      const N = 150;
      let stop = false;

      /* Document 0 BEFORE the readers start, so the collection exists
       * when they first look. Without it a reader can arrive before the
       * writer has created it and get -37 -- a real refusal, but the
       * test's own race rather than the server's, and tolerating it here
       * would blunt the same code's ability to report a genuine one. */
      await writes.insertOne({ n: 0, echo: 0, tag: 'v0' });

      const writing = (async () => {
        for (let i = 1; i < N; i++) {
          await writes.insertOne({ n: i, echo: i, tag: `v${i}` });
        }
        stop = true;
      })();

      const reading = readers.map(async (rc) => {
        const coll = rc.db(DB).collection('torn');
        let seen = 0;
        while (!stop) {
          const docs = await coll.find({}).toArray();
          for (const d of docs) {
            expect(d.echo, `torn document at n=${d.n}`).toBe(d.n);
            expect(d.tag).toBe(`v${d.n}`);
          }
          // Prefix consistency: ids go in ascending order, so the set
          // seen must be exactly 0..max with no holes.
          const ns = docs.map((d) => d.n).sort((a, b) => a - b);
          for (let k = 0; k < ns.length; k++) {
            expect(ns[k], 'a read skipped an id below one it returned').toBe(k);
          }
          seen = Math.max(seen, ns.length);
        }
        return seen;
      });

      await writing;
      const highs = await Promise.all(reading);
      // The readers were actually reading, not spinning on an empty set.
      expect(Math.max(...highs)).toBeGreaterThan(0);
      expect(await writes.countDocuments({})).toBe(N);
      /* Worth knowing rather than asserting a number on: with the server
       * serving one request at a time and poll() round-robining, six
       * scanning readers and one writer share the thread roughly evenly,
       * so the writer's own progress is throttled by the readers. That
       * is the head-of-line cost this whole line of work exists to
       * address, visible here as wall time. */
    } finally {
      await w.close();
      await Promise.all(readers.map((c) => c.close()));
    }
  }, 120000);

  it('compiles a fresh $regex on every connection at once', async () => {
    /*
     * The one read path that touches process-lifetime state outside the
     * collection: compiling a pattern. engine/src/regex.c caches compiled
     * patterns PER THREAD and serializes the compile itself, because
     * regex-engine keeps statics only its compiler touches and documents
     * that an embedder must serialize it (RX_COMPILE_LOCK says which
     * statics, and which is one flag away from mattering).
     *
     * Every other test here reads with a plain filter, which compiles
     * nothing -- so without this one the whole compile path is
     * unreachable from the suite built to find concurrency bugs, and a
     * TSan run over it would come back green having never entered the
     * code at risk.
     *
     * The patterns must all DIFFER, or the per-thread cache answers from
     * the second read onward and nothing compiles. `(?:[0-9]+|zzz<k>)`
     * cannot match a `v<n>` tag through its second branch, so every
     * pattern selects every document however k varies, and the count is
     * still exact.
     */
    const w = await connectServer(PORT);
    const readers = await Promise.all(
      Array.from({ length: 8 }, () => connectServer(PORT)));
    try {
      const N = 60;
      const writes = w.db(DB).collection('patterns');
      for (let i = 0; i < N; i++) {
        await writes.insertOne({ n: i, echo: i, tag: `v${i}` });
      }

      /* 8 connections × 12 distinct patterns = 96 compiles, well past the
       * 8-entry cache, so eviction and re-compilation both happen. */
      const counts = await Promise.all(readers.map(async (rc, r) => {
        const coll = rc.db(DB).collection('patterns');
        const got = [];
        for (let k = 0; k < 12; k++) {
          const docs = await coll
            .find({ tag: { $regex: `^v(?:[0-9]+|zzz${r}_${k})$` } })
            .toArray();
          for (const d of docs) expect(d.tag).toBe(`v${d.n}`);
          got.push(docs.length);
        }
        return got;
      }));

      // Every one of the 96 reads saw the whole collection.
      for (const perReader of counts) {
        for (const n of perReader) expect(n).toBe(N);
      }
      // And a pattern that matches nothing still answers, rather than
      // matching everything because a stale compiled handle was reused.
      const none = await w.db(DB).collection('patterns')
        .find({ tag: { $regex: '^nothing-matches-this$' } }).toArray();
      expect(none).toHaveLength(0);
    } finally {
      await w.close();
      await Promise.all(readers.map((c) => c.close()));
    }
  }, 120000);

  it('pages a cursor while the collection is being written to', async () => {
    /* A cursor pins the tree's root at open, so it iterates a consistent
     * snapshot and simply does not see later appends (bplustree.h's
     * cursor contract). Asserted rather than assumed, because it is the
     * property every future parallel read path leans on. */
    const w = await connectServer(PORT);
    const r = await connectServer(PORT);
    try {
      const writes = w.db(DB).collection('paged');
      await writes.insertMany(Array.from({ length: 300 }, (_, i) => ({ n: i, gen: 0 })));

      const cursor = r.db(DB).collection('paged').find({}, { batchSize: 25 });
      const first = await cursor.nextBatch();
      expect(first).toHaveLength(25);

      // Append a second generation while the scan is open.
      for (let i = 0; i < 100; i++) await writes.insertOne({ n: 1000 + i, gen: 1 });

      const rest = [...first];
      for (;;) {
        const batch = await cursor.nextBatch();
        if (!batch.length) break;
        rest.push(...batch);
      }
      // Exactly the pinned snapshot: everything from generation 0, and
      // nothing appended after the cursor opened.
      expect(rest).toHaveLength(300);
      expect(rest.every((d) => d.gen === 0)).toBe(true);
      expect(await writes.countDocuments({})).toBe(400);
    } finally {
      await w.close();
      await r.close();
    }
  }, 120000);

/*
 * ---- reader threads (--read-threads, server/readers.h) --------------------
 *
 * Its own server, because the flag is per-process and the whole point is to
 * compare a server that has reader threads against the one above that does
 * not.
 *
 * Native only, like the rest of this file: wasm has no threads on either
 * target and refuses the flag outright.
 */
describe.skipIf(!enabled)('a busy server: long reads on reader threads', () => {
  /* Small, and forced through a worker by a floor of 0: correctness does
   * not need a big collection, and every second of seeding is a second of
   * CI. The measurement further down needs a big one and says so. */
  const TINY_PORT = PORT + 40;

  it('answers an offloaded read exactly as an inline one, whatever the shape', async () => {
    /*
     * --read-offload-min 0 sends EVERY scanning read to a worker, however
     * small the collection. That is not how a server would be run -- it is
     * how the worker path gets exercised by a test that takes a second
     * rather than a minute, and `movedReads` proves each read really went.
     *
     * The answers are compared against a second server running with no
     * reader threads at all, over the same requests in the same order. Not
     * against hand-written expectations: those would encode what I believe
     * the engine answers, and the property under test is that moving a read
     * changes nothing about it.
     */
    const a = await startServer(TINY_PORT, ['--raft', '1', '--read-threads', '2',
                                            '--read-offload-min', '0']);
    const b = await startServer(TINY_PORT + 1, ['--raft', '1']);
    try {
      /*
       * DETERMINISTIC _ids, so the two collections are identical down to
       * the bytes. Ordinary ObjectIds are minted client-side and carry a
       * random component, so two independently seeded servers would differ
       * in every document's id -- and, because the primary tree is keyed by
       * id, in the ORDER an unsorted find returns them. That would leave
       * this comparison unable to check ordering at all, which is one of
       * the things most worth checking about a read that moved threads.
       */
      const idOf = (i) => new ObjectId(String(i).padStart(24, '0'));
      const seed = async (port) => {
        const c = await connectServer(port);
        const coll = c.db(DB).collection('shapes');
        const ids = [];
        for (let n = 0; n < 300; n += 100) {
          const batch = Array.from({ length: 100 }, (_, k) => ({
            _id: idOf(n + k),
            n: n + k, tag: `v${n + k}`, team: (n + k) % 3 === 0 ? 'core' : 'other',
          }));
          await coll.insertMany(batch);
          ids.push(...batch.map((d) => d._id));
        }
        await coll.createIndex({ team: 1 });
        return { c, coll, ids };
      };
      const A = await seed(TINY_PORT), B = await seed(TINY_PORT + 1);

      /* Every read shape that can be offloaded, plus ones that cannot --
       * so a difference in EITHER direction shows up. */
      const shapes = [
        (k) => k.coll.countDocuments({}),
        (k) => k.coll.countDocuments({ tag: 'v7' }),
        (k) => k.coll.countDocuments({ nope: 'zz' }),
        (k) => k.coll.find({ tag: 'v7' }).toArray(),
        (k) => k.coll.find({}).toArray(),
        (k) => k.coll.find({ n: { $gt: 290 } }).toArray(),
        (k) => k.coll.find({ tag: { $regex: '^v29[0-9]$' } }).toArray(),
        (k) => k.coll.findOne({ tag: 'v42' }),
        (k) => k.coll.findOne({ nope: 'zz' }),
        (k) => k.coll.findOne({ _id: k.ids[3] }),          // ids plan: never moved
        (k) => k.coll.countDocuments({ team: 'core' }),    // equality index: not moved
        (k) => k.coll.distinct('team'),
        (k) => k.coll.distinct('tag'),
        (k) => k.coll.find({}, { sort: { n: -1 }, limit: 3 }).toArray(),
        (k) => k.coll.find({}, { projection: { tag: 1 } , limit: 2 }).toArray(),
      ];
      /* Everything compared, ids and order included. */
      for (let i = 0; i < shapes.length; i++) {
        const [ra, rb] = [await shapes[i](A), await shapes[i](B)];
        expect(JSON.stringify(ra), `shape ${i} differs on a reader thread`)
          .toBe(JSON.stringify(rb));
      }

      /* And they really did go to a worker -- most of them. A test that
       * exercised the inline path by accident would pass everything above. */
      const p = await A.c.ping();
      expect(p.movedReads).toBeGreaterThan(8);
      expect(p.movedReads).toBe(p.longReads);   // nothing judged long fell back

      /* A refusal travels the same way: distinct with no field is missing a
       * required one, and it must come back as a refusal rather than as a
       * dead connection. */
      await expect(A.coll.distinct('')).rejects.toThrow();
      expect((await A.c.ping()).pong).toBe(true);

      await A.c.close(); await B.c.close();
    } finally {
      a.proc.kill(); b.proc.kill();
      fs.rmSync(a.dir, { recursive: true, force: true });
      fs.rmSync(b.dir, { recursive: true, force: true });
    }
  }, 120000);

  it('survives a client hanging up in the middle of its own long read', async () => {
    /*
     * The socket goes while a worker is inside the scan. The answer has
     * nowhere to go -- which is fine, and is exactly what happens to a
     * write whose client leaves while it is in the log -- but the pending
     * that was waiting for it is released and its slot reused, so a
     * completion matched by pointer or by slot index would land on somebody
     * else's request. It is matched by a never-reused seq instead.
     *
     * Fifty of them, so a leak or a misdelivery has fifty chances rather
     * than one, and the server must still be serving at the end.
     */
    const port = TINY_PORT + 2;
    const { proc, dir } = await startServer(port, ['--raft', '1',
      '--read-threads', '2', '--read-offload-min', '0']);
    try {
      const seeder = await connectServer(port);
      const coll = seeder.db(DB).collection('hangup');
      for (let n = 0; n < 2000; n += 100) {
        await coll.insertMany(Array.from({ length: 100 }, (_, k) => ({ n: n + k })));
      }

      for (let i = 0; i < 50; i++) {
        const c = await connectServer(port);
        /* Issued and NOT awaited: the close lands while it is in flight. */
        c.db(DB).collection('hangup').countDocuments({ nope: 'zz' }).catch(() => {});
        await c.close().catch(() => {});
      }

      /* Still serving, still correct, and the reads that were abandoned
       * did not take anybody else's answers with them. */
      expect(await coll.countDocuments({})).toBe(2000);
      expect(await coll.findOne({ n: 1999 })).toMatchObject({ n: 1999 });
      await seeder.close();
    } finally {
      proc.kill();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }, 120000);

  it('does not reap a connection while it is waiting for its own read', async () => {
    /*
     * The idle timer takes back a slot held by a client that is not there.
     * A client owed an answer is the opposite of that -- and while it is
     * owed one, the pollset stops asking its socket for POLLIN, so the
     * quiet clock cannot advance however busy the server is on its behalf.
     * Without an exemption a read that outlives --idle-timeout gets its own
     * connection closed underneath it.
     *
     * One second of timeout, and a scan of a collection large enough to
     * take a while, repeated so the connection is owed across several
     * timeouts' worth of wall time.
     */
    const port = TINY_PORT + 6;
    const { proc, dir } = await startServer(port, ['--raft', '1',
      '--read-threads', '2', '--read-offload-min', '0', '--idle-timeout', '1']);
    try {
      const c = await connectServer(port);
      const coll = c.db(DB).collection('patient');
      /*
       * ONE read has to outlive the timeout -- many quick ones would not
       * test this, because the quiet clock is refreshed between them. A
       * plain scan of this collection takes ~30ms, so the length comes from
       * the FILTER: `^x*y$` against 2,000 x's per document walks every
       * character of every one and takes seconds, where the same scan
       * without a regex takes milliseconds.
       */
      /* The cost is documents x characters, and the SEEDING cost is bytes.
       * Fewer, wider documents buy the same scan length for less inserting
       * -- which matters because this file runs beside suites holding
       * three-member clusters on default election timeouts, and a heavy
       * neighbour is how those start losing leaders mid-test. */
      const N = 6000;
      const PAD = 'x'.repeat(4000);
      for (let n = 0; n < N; n += 100) {
        await coll.insertMany(Array.from({ length: 100 }, (_, k) => ({ n: n + k, pad: PAD })));
      }

      const started = Date.now();
      const matched = await coll.countDocuments({ pad: { $regex: '^x*y$' } });
      const took = Date.now() - started;
      expect(matched).toBe(0);               // no document ends in y
      /* The read really did outlive the timeout -- otherwise this asserts
       * nothing at all about the exemption. */
      expect(took, `the read took ${took}ms, which is inside the 1s timeout`)
        .toBeGreaterThan(1200);

      /* THE POINT: the connection that asked is still there and still
       * usable. Without the exemption the sweep closed it mid-read and this
       * is where it fails. */
      expect((await c.ping()).pong).toBe(true);
      expect(await coll.countDocuments({ n: { $lt: 5 } })).toBe(5);
      await c.close();
    } finally {
      proc.kill();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }, 120000);

  it('makes an unmaking operation wait for a reader that is inside a view', async () => {
    /*
     * THE DRAIN, ASSERTED RATHER THAN RACED.
     *
     * A read view shares the live handles' ios, so it is valid exactly as
     * long as its files are only appended to. Anything that unmakes one --
     * drop, compact, index DDL, an install -- has to wait for every reader
     * to come out first, or a worker's pread lands on a descriptor that has
     * been closed and possibly reused, which returns another file's bytes
     * with NO ERROR AT ALL. The soak found that failure on its first run
     * with threads; what a soak cannot do is prove the fix RAN, because a
     * missing barrier is found by racing approximately never.
     *
     * So the server counts it. `drainWaits` is destructive operations that
     * found a reader inside a view and waited; `drainedReads` is how many
     * reads those waits were for. Both stay zero on a server that never
     * overlapped the two, which is what makes a non-zero one evidence.
     *
     * Two independent assertions per operation: the counter moved, and the
     * operation actually BLOCKED for roughly the rest of the scan. Either
     * alone could pass a broken drain -- a counter incremented without
     * waiting, or a wait that happened for an unrelated reason.
     */
    const port = TINY_PORT + 8;
    const { proc, dir } = await startServer(port, ['--raft', '1',
      '--read-threads', '2', '--read-offload-min', '0']);
    try {
      const reader = await connectServer(port);
      const other = await connectServer(port);

      /* Long enough to still be running when the destructive op arrives.
       * The length comes from the FILTER -- `^x*y$` against 4,000 x's walks
       * every character of every document -- because a plain scan of this
       * collection is milliseconds. */
      const N = 6000;
      const PAD = 'x'.repeat(4000);
      const seed = async () => {
        for (let n = 0; n < N; n += 100) {
          await other.db(DB).collection('drained').insertMany(
            Array.from({ length: 100 }, (_, k) => ({ n: n + k, pad: PAD })));
        }
      };
      await seed();

      const SLOW = { pad: { $regex: '^x*y$' } };
      /* How long one costs HERE, so the blocking bound below is derived
       * rather than guessed at on somebody else's hardware. */
      const t0 = Date.now();
      expect(await reader.db(DB).collection('drained').countDocuments(SLOW)).toBe(0);
      const scanMs = Date.now() - t0;
      expect(scanMs, `a scan takes ${scanMs}ms, too short to overlap`).toBeGreaterThan(500);

      /*
       * Every kind of unmaking, one at a time. `compact` is performed rather
       * than logged, so it drains on the REQUEST path; the index DDL and the
       * drop are logged and drain on the APPLY path. Both paths are covered
       * here, which is the point of the list rather than one example.
       */
      const cases = [
        ['compact', (db) => db.collection('drained').compact()],
        ['createIndex', (db) => db.collection('drained').createIndex({ n: 1 })],
        ['dropIndex', (db) => db.collection('drained').dropIndex('n_1')],
        ['dropCollection', (db) => db.dropCollection('drained')],
      ];
      for (const [what, run] of cases) {
        const before = await other.ping();
        /* Issued and NOT awaited: it is running on a worker by the time the
         * destructive operation below arrives. */
        const scanning = reader.db(DB).collection('drained').countDocuments(SLOW);
        await new Promise((r) => setTimeout(r, 120));   // let it get inside

        const started = Date.now();
        await run(other.db(DB));
        const blocked = Date.now() - started;
        const after = await other.ping();

        /* It waited, and it waited for a read. */
        expect(after.drainWaits, `${what} did not wait for the reader`)
          .toBeGreaterThan(before.drainWaits);
        expect(after.drainedReads).toBeGreaterThan(before.drainedReads);
        /* ...and it really blocked, for something like the rest of the
         * scan. A third of one is a wide bound for a shared machine; what
         * it excludes is a counter that moves without a wait behind it. */
        expect(blocked, `${what} returned in ${blocked}ms against a ${scanMs}ms scan`)
          .toBeGreaterThan(scanMs / 3);

        /* And the read that was waited for still got its own answer: the
         * drain DELIVERS them rather than discarding them, or every one of
         * those clients would wait forever on a reply that was built. */
        expect(await scanning, `${what} lost the reader's answer`).toBe(0);

        if (what === 'dropCollection') await seed();   // the next case needs it back
      }

      /* A destructive operation on an IDLE server waits for nothing, which
       * is what makes the assertions above evidence rather than noise. */
      const quiet = await other.ping();
      await other.db(DB).collection('drained').compact();
      expect((await other.ping()).drainWaits).toBe(quiet.drainWaits);

      await reader.close();
      await other.close();
    } finally {
      proc.kill();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }, 180000);

  it('makes a FOLLOWER wait for its own reader before applying a drop', async () => {
    /*
     * THE APPLY-PATH DRAIN, WHICH THE TEST ABOVE DOES NOT REACH.
     *
     * On a leader, a destructive request drains before it is even proposed,
     * so the drain inside the apply loop is redundant there -- removing it
     * leaves the test above passing, which is how this gap was found. The
     * apply drain earns its place on a FOLLOWER: destruction arrives as a
     * committed entry with no client request behind it, and a follower is
     * exactly the member holding read views, because stale reads are the
     * only reads it serves.
     *
     * So: two members, both with reader threads. The follower is given a
     * slow stale read. The leader drops the collection. The follower has to
     * finish with its view before applying that entry, or its worker is
     * reading a file that has been unlinked -- and a pread on a recycled
     * descriptor returns another file's bytes with no error at all.
     */
    const base = TINY_PORT + 20;
    const MEMBERS = [1, 2].map((id) => ({
      id, port: base + id, raftPort: base + 10 + id
    }));
    const argsFor = (m) => [
      '--raft', String(m.id), '--raft-port', String(m.raftPort),
      /* Wide, because this test deliberately stalls a member for most of a
       * second: at 150:300 the follower's own drain looks like a leader
       * that has gone quiet, and what fails is an election. */
      '--election-timeout', '2000:4000', '--heartbeat', '400',
      '--read-threads', '2', '--read-offload-min', '0',
      ...MEMBERS.filter((o) => o.id !== m.id)
        .flatMap((o) => ['--peer', `${o.id}@127.0.0.1:${o.raftPort}`])
    ];

    const started = [];
    try {
      for (const m of MEMBERS) started.push(await startServer(m.port, argsFor(m)));

      /* Whoever leads; the other one is the follower under test. */
      let leader = null, follower = null;
      for (let i = 0; i < 100 && !leader; i++) {
        for (const m of MEMBERS) {
          const c = await connectServer(m.port);
          const role = (await c.ping()).role;
          if (role === 'leader') leader = c; else if (role === 'follower') follower = c;
          if (![leader, follower].includes(c)) await c.close();
        }
        if (!leader || !follower) {
          for (const c of [leader, follower]) await c?.close().catch(() => {});
          leader = follower = null;
          await new Promise((r) => setTimeout(r, 200));
        }
      }
      expect(leader, 'no leader emerged').not.toBeNull();
      expect(follower, 'no follower emerged').not.toBeNull();

      const N = 6000;
      const PAD = 'x'.repeat(4000);
      for (let n = 0; n < N; n += 100) {
        await leader.db(DB).collection('replicated').insertMany(
          Array.from({ length: 100 }, (_, k) => ({ n: n + k, pad: PAD })));
      }

      /* Wait for the follower to hold all of it, then time one stale scan
       * so the assertions below are against this machine's own speed. */
      const stale = { stale: true };
      const fc = follower.db(DB).collection('replicated');
      for (let i = 0; i < 100; i++) {
        if (await fc.countDocuments({}, stale).catch(() => 0) === N) break;
        await new Promise((r) => setTimeout(r, 100));
      }
      expect(await fc.countDocuments({}, stale)).toBe(N);

      const SLOW = { pad: { $regex: '^x*y$' } };
      const t0 = Date.now();
      expect(await fc.countDocuments(SLOW, stale)).toBe(0);
      const scanMs = Date.now() - t0;
      expect(scanMs, `a stale scan takes ${scanMs}ms, too short to overlap`)
        .toBeGreaterThan(500);

      /* The follower offloads its stale reads -- which is the point: they
       * are all it serves, and a scan left on its serving thread delays
       * both its other readers and the pump keeping it current. */
      const before = await follower.ping();
      expect(before.movedReads, 'the follower offloaded nothing').toBeGreaterThan(1);

      /* A slow stale read on the follower, in flight... */
      const scanning = fc.countDocuments(SLOW, stale);
      await new Promise((r) => setTimeout(r, 120));
      /* ...while the LEADER unmakes the files underneath it. */
      expect(await leader.db(DB).dropCollection('replicated')).toBe(true);

      /* The read still gets its own answer, from the state it was captured
       * in -- the drain delivers, it does not discard. */
      expect(await scanning).toBe(0);

      /* And the follower waited, on the apply path, with no client request
       * of its own to have drained for it. */
      for (let i = 0; i < 100; i++) {
        if ((await follower.ping()).drainWaits > before.drainWaits) break;
        await new Promise((r) => setTimeout(r, 100));
      }
      const after = await follower.ping();
      expect(after.drainWaits, 'the follower applied the drop without waiting')
        .toBeGreaterThan(before.drainWaits);
      expect(after.drainedReads).toBeGreaterThan(before.drainedReads);

      /* Both members are still alive and agree the collection is gone. */
      expect((await follower.ping()).pong).toBe(true);
      await expect(fc.countDocuments({}, stale)).rejects.toMatchObject({ code: -37 });

      await leader.close();
      await follower.close();
    } finally {
      for (const s of started) {
        s.proc.kill();
        fs.rmSync(s.dir, { recursive: true, force: true });
      }
    }
  }, 180000);

  it('keeps small reads fast while another client scans', async () => {
    /*
     * THE MEASUREMENT THIS WHOLE MILESTONE EXISTS FOR.
     *
     * Measured at 50,000 documents: with no reader threads, one connection
     * running a scanning count took eight connections of point lookups from
     * 53,152 reads in three seconds to 2,160 -- FOUR PERCENT -- and their
     * median from 0.35ms to 11.08ms, which is one scan exactly. With one
     * reader thread the same eight held 91% and a median of 0.35ms, and
     * scan throughput did not fall.
     *
     * Asserted here at a smaller size and with a wide bound, because this
     * runs on whatever CI is given: 8,000 documents (a ~1.8ms scan against
     * a ~0.35ms lookup), and the requirement is HALF of idle throughput
     * where the unthreaded server manages a twentieth. Both margins are
     * large; the effect is 20x, and the bound is 10x off the failure.
     */
    const port = TINY_PORT + 4;
    const N = 8000;
    const { proc, dir } = await startServer(port,
      ['--raft', '1', '--read-threads', '2', '--max-clients', '32']);
    try {
      const seeder = await connectServer(port);
      const items = seeder.db(DB).collection('isolated');
      const ids = [];
      while (ids.length < N) {
        const r = await items.insertMany(Array.from({ length: 100 },
          (_, k) => ({ n: ids.length + k, pad: 'x'.repeat(60) })));
        ids.push(...Object.values(r.insertedIds));
      }
      const pick = () => ids[(Math.random() * ids.length) | 0];

      const readers = await Promise.all(Array.from({ length: 8 }, () => connectServer(port)));
      const round = async (ms) => {
        const stop = Date.now() + ms;
        let n = 0;
        await Promise.all(readers.map(async (c) => {
          const coll = c.db(DB).collection('isolated');
          while (Date.now() < stop) { await coll.findOne({ _id: pick() }); n++; }
        }));
        return n;
      };

      await round(300);                       // warm
      const idle = await round(1000);

      let go = true, scans = 0;
      const scanner = await connectServer(port);
      const scanning = (async () => {
        const coll = scanner.db(DB).collection('isolated');
        while (go) { await coll.countDocuments({ pad: 'zzzzzz' }); scans++; }
      })();
      const busy = await round(1000);
      go = false; await scanning;

      const held = busy / idle;
      /* The scanner was really scanning -- otherwise this asserts nothing. */
      expect(scans, 'no scan ran, so nothing was competing').toBeGreaterThan(2);
      expect((await scanner.ping()).movedReads,
        'the scans were not offloaded').toBeGreaterThan(2);
      expect(held, `held ${(held * 100).toFixed(0)}% of idle throughput` +
                   ` (${busy} vs ${idle}) while one client scanned`)
        .toBeGreaterThan(0.5);

      await Promise.all([...readers, scanner, seeder].map((c) => c.close().catch(() => {})));
    } finally {
      proc.kill();
      fs.rmSync(dir, { recursive: true, force: true });
    }
  }, 180000);
});

  it('says nothing alarming on stderr through all of it', () => {
    /* A sanitized build reports on stderr and keeps going for anything
     * UBSan can recover from; without this the suite would pass while
     * the report scrolled past. */
    const text = stderr();
    expect(text).not.toMatch(/runtime error|AddressSanitizer|ThreadSanitizer|LeakSanitizer/);
  });
});
