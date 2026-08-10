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
import { connectServer } from '../src/db-server-client.js';

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

      const writing = (async () => {
        for (let i = 0; i < N; i++) {
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

  it('says nothing alarming on stderr through all of it', () => {
    /* A sanitized build reports on stderr and keeps going for anything
     * UBSan can recover from; without this the suite would pass while
     * the report scrolled past. */
    const text = stderr();
    expect(text).not.toMatch(/runtime error|AddressSanitizer|ThreadSanitizer|LeakSanitizer/);
  });
});
