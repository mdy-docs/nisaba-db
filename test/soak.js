/**
 * test/soak.js — a busy server, for as long as you like.
 *
 *   node test/soak.js                        # 30s, 8 readers, default seed
 *   node test/soak.js --seconds 600 --readers 16
 *   node test/soak.js --seed 12345           # replay a failure
 *   NISABA_SERVER_BIN=build/lib/nisaba-server-asan node test/soak.js
 *
 * WHAT THIS IS FOR. test/db.concurrency.test.js pins the properties in a
 * few seconds so CI can afford them. This runs the same shapes for
 * minutes or hours, with the DESTRUCTIVE operations mixed in -- compact,
 * dropCollection, dropIndex, createIndex -- because those are the ones
 * that free handles and unlink files underneath whoever is reading, and
 * they are the reason anything that moves reads off the writer's thread
 * needs a reclamation story at all.
 *
 * IT ASSERTS CONTENT, NOT LIVENESS, and that is deliberate. The failure
 * mode that matters here is quiet: a file closed under a reader means a
 * pread against a recycled descriptor, which returns another file's
 * bytes with no error at all. A soak that only watched for crashes would
 * run green through exactly the bug it was built to find. So every
 * document carries enough to check itself:
 *
 *   { n, echo: n, tag: `v<n>`, coll: '<collection name>' }
 *
 *   - echo !== n            a torn read, or bytes from another commit
 *   - tag mismatch          the same, caught a second way
 *   - coll !== this one     bytes from a DIFFERENT FILE: the fd-reuse bug
 *   - a gap below the max   a read that skipped an id it should have had
 *
 * SEEDED. Every choice comes from a small deterministic PRNG, so a
 * failure prints its seed and `--seed N` replays the same interleaving
 * of operations. The thread interleaving underneath is not reproducible
 * -- nothing can make it so -- but the workload is.
 *
 * Exit code 0 = clean, 1 = a violation (printed), 2 = bad usage.
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer } from '../src/db-server-client.js';

const NATIVE = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const DB = 'soak';

function usage(msg) {
  if (msg) console.error(`error: ${msg}`);
  console.error(
    'usage: node test/soak.js [--seconds N] [--readers N] [--seed N] [--port N]\n' +
    '                            [--readThreads N] [--raft 0|1]\n' +
    '                            [--idleTimeout SECONDS] [--quiet]');
  process.exit(2);
}

/*
 * `readThreads` is the server's --read-threads, and asking for any turns on
 * --raft 1 as well: a read is moved off the serving thread by being
 * DEFERRED, and only the replicated transport can defer one (server/main.c
 * refuses the flag without it). So 0 -- the default -- soaks exactly the
 * unreplicated server this file always soaked, and anything above it soaks
 * a replicated one with reader threads. Both are worth running; they are
 * different servers.
 *
 * `raft` exists to separate those two, and it exists because the run that
 * needed it could not be asked for. When a soak with reader threads fails,
 * the control run is not "the same server without threads" -- turning them
 * off also turns off the replicated transport, so the comparison moves two
 * things at once and says which of them mattered only by luck. `--raft 1
 * --readThreads 0` is the middle case: every deferral the replicated
 * transport does, and not one read off the serving thread.
 */
/*
 * `idleTimeout` is the server's --idle-timeout, and it is here because a
 * bug in the idle accounting takes as long to surface as the timeout is
 * long. A connection wrongly judged silent has to be wrongly judged for the
 * WHOLE timeout before anyone hears about it, so at the default sixty
 * seconds the first sighting cost an hour of soaking and 8.7 million reads.
 * At three seconds the same wrongness shows up in a minute -- and a run
 * that stays clean at three seconds is a much stronger statement about the
 * accounting than one that stays clean at sixty.
 */
const opts = { seconds: 30, readers: 8, seed: 1, port: 34000 + (process.pid % 900),
               readThreads: 0, raft: 0, idleTimeout: 0, quiet: false };
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === '--quiet') { opts.quiet = true; continue; }
  const key = a.replace(/^--/, '');
  if (!(key in opts) || a[0] !== '-') usage(`unknown option ${a}`);
  const v = Number(process.argv[++i]);
  /* Zero is a MEANING for readThreads, not a missing value: it is the
   * default, and it is the control run anything measured against reader
   * threads has to be compared with. Refusing it made the one comparison
   * this file exists to support impossible to ask for. */
  const floor = (key === 'readThreads' || key === 'raft' ||
                 key === 'idleTimeout') ? 0 : 1;
  if (!Number.isFinite(v) || v < floor) {
    usage(`${a} needs a number of at least ${floor}`);
  }
  opts[key] = v;
}

/** xorshift32: small, seeded, and identical on every platform. */
function rng(seed) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >> 17;
    s ^= s << 5;  s >>>= 0;
    return s / 0x100000000;
  };
}

/*
 * Refusals this workload legitimately produces, by CODE rather than by
 * message text -- the codes are the wire's and do not move, the
 * sentences are prose and do. Anything not on this list is news, which
 * is the whole point of running a randomized workload against a server.
 *
 *   -37 the collection was dropped between choosing it and using it
 *   -49 compact refused: a cursor is scanning the collection
 *   -56 createIndex on an index this loop already made
 *   -57 dropIndex on one it already dropped
 */
const EXPECTED = new Set([-37, -49, -56, -57]);
const expected = (err) => EXPECTED.has(err?.code);

const say = (...a) => { if (!opts.quiet) console.log(...a); };

/*
 * WHAT EVERY CONNECTION IS WAITING FOR, printed with the first violation.
 *
 * A refusal nobody expected is only half the evidence. The two ways this
 * server can fail a client that is reading flat out look identical from the
 * error alone and are not the same bug:
 *
 *   a LOST ANSWER -- the request was sent, the answer was built and
 *     dropped, and the client has been awaiting it ever since. Its slot
 *     shows a request in flight for tens of seconds.
 *   a STARVED or STALLED SERVER -- the client's last answer was moments
 *     ago and the server simply stopped asking it for more. Several slots
 *     go quiet together, and none of them is waiting on anything.
 *
 * The first version of this file reported `[-45] Connection closed: it
 * asked nothing...` and left both open. These slots close that.
 */
const slots = [];
const newSlot = (label) => {
  const s = { label, what: null, at: 0, done: 0, lastDone: Date.now() };
  slots.push(s);
  return s;
};
const slotReport = () => {
  const now = Date.now();
  return slots.map((s) => `    ${s.label}: ${s.done} done,` +
    ` last answer ${((now - s.lastDone) / 1000).toFixed(1)}s ago,` +
    ` in flight ${s.what ? `${((now - s.at) / 1000).toFixed(1)}s (${s.what})` : 'none'}`);
};

const violations = [];
const note = (what) => {
  const first = violations.length === 0;
  violations.push(what);
  console.error(`VIOLATION: ${what}`);
  /* Once, with the first one: by the second the loops are unwinding and
   * every slot reads as idle. */
  if (first && slots.length) console.error(slotReport().join('\n'));
};

async function startServer(dir, port) {
  const args = ['--port', String(port), '--max-clients', '64'];
  if (opts.idleTimeout > 0) args.push('--idle-timeout', String(opts.idleTimeout));
  /* Reader threads need it; --raft asks for it on its own, so the
   * replicated transport can be soaked with every read inline. */
  if (opts.readThreads > 0 || opts.raft > 0) args.push('--raft', '1');
  if (opts.readThreads > 0) {
    /* A floor of 0 so every scanning read goes to a worker: the reads here
     * are over small collections, and a soak that never reached the worker
     * path would be soaking the wrong server. */
    args.push('--read-threads', String(opts.readThreads),
              /* A floor of 0 so every scanning read goes to a worker: the
               * collections here are small, and a soak that never reached
               * the worker path would be soaking the wrong server. */
              '--read-offload-min', '0');
  }
  const proc = spawn(path.resolve(NATIVE), args, {
    cwd: dir, stdio: ['ignore', 'pipe', 'pipe']
  });
  let err = '';
  proc.stderr.on('data', (d) => {
    err += String(d);
    const text = String(d);
    // A sanitized build reports here and may keep going; the soak must
    // not scroll past that.
    if (/AddressSanitizer|ThreadSanitizer|LeakSanitizer|runtime error/.test(text)) {
      note(`sanitizer: ${text.trim().split('\n')[0]}`);
    }
  });
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`server did not start: ${err}`)), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve(); }
    });
  });
  return { proc, stderr: () => err };
}

/** The self-describing document, and the check that reads it back. */
const docFor = (coll, n) => ({ n, echo: n, tag: `v${n}`, coll });

/*
 * A $regex filter whose pattern is DIFFERENT EVERY TIME, matching exactly
 * the same documents.
 *
 * Reads here were all `find({})` until now, which never compiles a regex,
 * so the whole compile path -- and the process-lifetime state
 * regex-engine keeps inside it (engine/src/regex.c's RX_COMPILE_LOCK
 * says which) -- was unreachable from the one harness built to find
 * concurrency bugs. That is a hole exactly where reads leaving the
 * serving thread are sharpest.
 *
 * Varying `k` is the whole point: engine/src/regex.c caches 8 compiled
 * patterns per thread, so a fixed pattern compiles once and every later
 * read is a cache hit that proves nothing. `(?:[0-9]+|zzz<k>)` can never
 * match a tag through its second branch -- tags are `v<n>` -- so the
 * result set is every document in the collection whatever k is, and the
 * content check below stays exact.
 */
const regexFor = (k) => ({ tag: { $regex: `^v(?:[0-9]+|zzz${k})$` } });

function checkBatch(coll, docs) {
  const ns = [];
  for (const d of docs) {
    if (d.coll !== coll) { note(`${coll}: a document from collection "${d.coll}" -- wrong file`); return; }
    if (d.echo !== d.n) { note(`${coll}: torn document, n=${d.n} echo=${d.echo}`); return; }
    if (d.tag !== `v${d.n}`) { note(`${coll}: torn document, n=${d.n} tag=${d.tag}`); return; }
    ns.push(d.n);
  }
  ns.sort((a, b) => a - b);
  for (let i = 0; i < ns.length; i++) {
    if (ns[i] !== i) { note(`${coll}: read skipped id ${i} but returned ${ns[i]}`); return; }
  }
}

const main = async () => {
  if (!fs.existsSync(NATIVE)) usage(`no server binary at ${NATIVE} -- ./build/build-server.sh --native`);

  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-soak-'));
  const { proc, stderr } = await startServer(dir, opts.port);
  const deadline = Date.now() + opts.seconds * 1000;
  const rand = rng(opts.seed);
  const COLLS = ['alpha', 'beta', 'gamma'];
  const counts = new Map(COLLS.map((c) => [c, 0]));
  const stats = { writes: 0, reads: 0, regexReads: 0, compacts: 0, drops: 0, indexes: 0 };

  const writer = await connectServer(opts.port);
  /* What is RUNNING, not what was asked for: the server lowers
   * --read-threads to the cpus it can spare, and a soak that reports the
   * ask would credit eight workers for two workers' coverage. */
  const running = (await writer.ping()).readThreads ?? 0;
  say(`soak: ${opts.seconds}s, ${opts.readers} readers, seed ${opts.seed}, ` +
      `port ${opts.port}, ${running} reader thread(s)` +
      `${opts.readThreads > running ? ` (asked for ${opts.readThreads})` : ''}` +
      `${opts.readThreads > 0 || opts.raft > 0 ? ' (+--raft 1)' : ''}, ${NATIVE}`);

  const readers = await Promise.all(
    Array.from({ length: opts.readers }, () => connectServer(opts.port)));

  /*
   * A LONG RUN HAS TO SAY SOMETHING BEFORE IT ENDS. The gate for turning
   * reader threads on by default is measured in hours, and a soak of hours
   * that printed only at the end is indistinguishable from one that hung --
   * which is exactly the failure a threading bug is most likely to look
   * like. Every minute, and only while there is more than a minute left, so
   * a short run prints what it always printed.
   */
  const heartbeat = opts.seconds > 90 && !opts.quiet ? setInterval(() => {
    const left = Math.round((deadline - Date.now()) / 1000);
    if (left < 30) return;
    say(`  +${opts.seconds - left}s (${left}s left): ${stats.writes} writes,` +
        ` ${stats.reads} reads (${stats.regexReads} compiling),` +
        ` ${stats.compacts} compacts, ${stats.indexes} index ops,` +
        ` ${stats.drops} drops`);
  }, 60000) : null;
  heartbeat?.unref?.();

  /*
   * ONE WRITER, and it owns `counts`. Everything destructive happens
   * here too, so the expected content of a collection is always known to
   * exactly one place -- a reader that disagrees with it is reporting a
   * real disagreement rather than a race in the test's own bookkeeping.
   */
  const wslot = newSlot('writer');
  const writing = (async () => {
    const db = writer.db(DB);
    while (Date.now() < deadline && !violations.length) {
      const coll = COLLS[(rand() * COLLS.length) | 0];
      const roll = rand();
      wslot.at = Date.now();
      try {
        if (roll < 0.80) {
          const n = counts.get(coll);
          wslot.what = `insertOne ${coll} at ${n}`;
          await db.collection(coll).insertOne(docFor(coll, n));
          counts.set(coll, n + 1);
          stats.writes++;
        } else if (roll < 0.88) {
          wslot.what = `compact ${coll}`;
          await db.collection(coll).compact();
          stats.compacts++;
        } else if (roll < 0.94) {
          wslot.what = `createIndex ${coll}`;
          await db.collection(coll).createIndex({ n: 1 });
          stats.indexes++;
        } else if (roll < 0.97) {
          wslot.what = `dropIndex ${coll}`;
          await db.collection(coll).dropIndex('n_1');
          stats.indexes++;
        } else {
          wslot.what = `dropCollection ${coll}`;
          // The most destructive thing available: the files go away
          // while readers may be inside them.
          await db.dropCollection(coll);
          counts.set(coll, 0);
          stats.drops++;
        }
        wslot.done++;
        wslot.lastDone = Date.now();
      } catch (err) {
        if (!expected(err)) note(`writer: [${err?.code}] ${err.message}`);
      }
      wslot.what = null;
    }
  })();

  let pattern = 0;   /* only the readers touch it, and only to differ */
  const reading = readers.map(async (rc, k) => {
    const db = rc.db(DB);
    const slot = newSlot(`reader ${k}`);
    while (Date.now() < deadline && !violations.length) {
      const coll = COLLS[(rand() * COLLS.length) | 0];
      /* A quarter of reads compile a fresh pattern; the rest stay on the
       * plain scan, which is still the shape most reads have. */
      const useRegex = rand() < 0.25;
      slot.at = Date.now();
      slot.what = `${useRegex ? 'regex find' : 'find'} ${coll}`;
      try {
        const filter = useRegex ? regexFor(pattern++) : {};
        const docs = await db.collection(coll).find(filter).toArray();
        checkBatch(coll, docs);
        slot.done++;
        slot.lastDone = Date.now();
        stats.reads++;
        if (useRegex) stats.regexReads++;
      } catch (err) {
        if (!expected(err)) note(`reader ${k}: [${err?.code}] ${err.message}`);
      }
      slot.what = null;
    }
  });

  await Promise.all([writing, ...reading]);
  if (heartbeat) clearInterval(heartbeat);
  await writer.close().catch(() => {});
  await Promise.all(readers.map((c) => c.close().catch(() => {})));
  proc.kill();
  await new Promise((r) => proc.once('exit', r));

  const leftovers = stderr();
  if (/AddressSanitizer|ThreadSanitizer|LeakSanitizer|runtime error/.test(leftovers)) {
    note('the server printed a sanitizer report');
  }

  fs.rmSync(dir, { recursive: true, force: true });
  say(`done: ${stats.writes} writes, ${stats.reads} reads ` +
      `(${stats.regexReads} compiling a fresh $regex), ${stats.compacts} compacts, ` +
      `${stats.indexes} index ops, ${stats.drops} drops`);
  /* A run that compiled nothing did not exercise the compile path, and
   * would pass vacuously for the hazard that path carries. */
  if (!stats.regexReads) note('no read compiled a $regex -- the compile path went untested');

  if (violations.length) {
    /* The server's own last words. A violation is one side of an
     * interaction; main.c says why it closed a connection, and reading only
     * the client's half is how the first -45 here cost an hour. */
    const tail = leftovers.trim().split('\n').slice(-15);
    if (tail.length) console.error(`server stderr tail:\n    ${tail.join('\n    ')}`);
    console.error(`\n${violations.length} violation(s) -- replay with --seed ${opts.seed}`);
    process.exit(1);
  }
  say('clean');
};

main().catch((err) => { console.error(err); process.exit(1); });
