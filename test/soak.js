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
    '                            [--readThreads N] [--quiet]');
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
 */
const opts = { seconds: 30, readers: 8, seed: 1, port: 34000 + (process.pid % 900),
               readThreads: 0, quiet: false };
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === '--quiet') { opts.quiet = true; continue; }
  const key = a.replace(/^--/, '');
  if (!(key in opts) || a[0] !== '-') usage(`unknown option ${a}`);
  const v = Number(process.argv[++i]);
  if (!Number.isFinite(v) || v <= 0) usage(`${a} needs a positive number`);
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
const violations = [];
const note = (what) => {
  violations.push(what);
  console.error(`VIOLATION: ${what}`);
};

async function startServer(dir, port) {
  const args = ['--port', String(port), '--max-clients', '64'];
  if (opts.readThreads > 0) {
    /* A floor of 0 so every scanning read goes to a worker: the reads here
     * are over small collections, and a soak that never reached the worker
     * path would be soaking the wrong server. */
    args.push('--raft', '1',
              '--read-threads', String(opts.readThreads),
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

  say(`soak: ${opts.seconds}s, ${opts.readers} readers, seed ${opts.seed}, ` +
      `port ${opts.port}, ${opts.readThreads} reader thread(s)` +
      `${opts.readThreads > 0 ? ' (+--raft 1)' : ''}, ${NATIVE}`);

  const writer = await connectServer(opts.port);
  const readers = await Promise.all(
    Array.from({ length: opts.readers }, () => connectServer(opts.port)));

  /*
   * ONE WRITER, and it owns `counts`. Everything destructive happens
   * here too, so the expected content of a collection is always known to
   * exactly one place -- a reader that disagrees with it is reporting a
   * real disagreement rather than a race in the test's own bookkeeping.
   */
  const writing = (async () => {
    const db = writer.db(DB);
    while (Date.now() < deadline && !violations.length) {
      const coll = COLLS[(rand() * COLLS.length) | 0];
      const roll = rand();
      try {
        if (roll < 0.80) {
          const n = counts.get(coll);
          await db.collection(coll).insertOne(docFor(coll, n));
          counts.set(coll, n + 1);
          stats.writes++;
        } else if (roll < 0.88) {
          await db.collection(coll).compact();
          stats.compacts++;
        } else if (roll < 0.94) {
          await db.collection(coll).createIndex({ n: 1 });
          stats.indexes++;
        } else if (roll < 0.97) {
          await db.collection(coll).dropIndex('n_1');
          stats.indexes++;
        } else {
          // The most destructive thing available: the files go away
          // while readers may be inside them.
          await db.dropCollection(coll);
          counts.set(coll, 0);
          stats.drops++;
        }
      } catch (err) {
        if (!expected(err)) note(`writer: [${err?.code}] ${err.message}`);
      }
    }
  })();

  let pattern = 0;   /* only the readers touch it, and only to differ */
  const reading = readers.map(async (rc) => {
    const db = rc.db(DB);
    while (Date.now() < deadline && !violations.length) {
      const coll = COLLS[(rand() * COLLS.length) | 0];
      /* A quarter of reads compile a fresh pattern; the rest stay on the
       * plain scan, which is still the shape most reads have. */
      const useRegex = rand() < 0.25;
      try {
        const filter = useRegex ? regexFor(pattern++) : {};
        const docs = await db.collection(coll).find(filter).toArray();
        checkBatch(coll, docs);
        stats.reads++;
        if (useRegex) stats.regexReads++;
      } catch (err) {
        if (!expected(err)) note(`reader: [${err?.code}] ${err.message}`);
      }
    }
  });

  await Promise.all([writing, ...reading]);
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
    console.error(`\n${violations.length} violation(s) -- replay with --seed ${opts.seed}`);
    process.exit(1);
  }
  say('clean');
};

main().catch((err) => { console.error(err); process.exit(1); });
