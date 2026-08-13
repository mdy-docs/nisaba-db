/**
 * test/crash-matrix.js — KILL THE SERVER INSIDE EACH DESTRUCTIVE OP, AND
 * REQUIRE IT BACK.
 *
 *   node test/crash-matrix.js                       # every op
 *   node test/crash-matrix.js --op compact --tries 40
 *   node test/crash-matrix.js --op all --tries 25 --boots 3 --seed 7
 *
 * WHY THIS SHAPE. `test/repro-halt-on-drop.js` is one op of this matrix,
 * and the first time it was run it found a database that could never be
 * opened again -- a completed drop and a polite restart were enough. That
 * result is the argument for doing the same thing to every other op that
 * unmakes files, rather than reasoning about them:
 *
 *   compact       performed, NOT logged (docs/compaction.md), so replay
 *                 cannot repair it -- only the catalog commit and the
 *                 orphan sweep can. The whole file set is swapped at once.
 *   createIndex   builds, attaches, then the catalog commit is the
 *                 decisive act; a kill between leaves either an orphan or,
 *                 far worse, an ATTACHED HALF-BUILT INDEX that answers
 *                 queries with fewer documents than the collection holds.
 *   dropIndex     catalog first, then the file removal -- the orphan
 *                 window in the other direction.
 *   snapshot      writes a generation, then the manifest IS the commit; a
 *                 torn generation must not be adopted, and its debris
 *                 must be swept.
 *   dropCollection/dropDatabase  the two that are already fixed, kept here
 *                 as controls: a run where they come back dirty means a
 *                 regression, not a discovery.
 *
 * WHAT IS ASSERTED, per try, after the kill:
 *
 *   1. IT SERVES -- every boot, not just the first. Replay is
 *      deterministic, so a halt on boot 1 halts on boot 4 as well, which
 *      is what turns a crash into a dead database.
 *   2. THE DOCUMENTS ARE WHAT THEY WERE. A content oracle captured before
 *      the kill, compared field by field.
 *   3. EVERY ATTACHED INDEX AGREES WITH A SCAN. For each index the
 *      catalog still names, findByIndex(name, [v]) is compared against
 *      countDocuments({field: v}). This is the check that would catch a
 *      half-built index the catalog adopted anyway -- the failure mode
 *      that is invisible to "does it start".
 *   4. NO DEBRIS. The recovered database's file count must match one of
 *      the two states the op is allowed to leave (before, or after), as
 *      measured on clean runs. An orphan the sweep missed shows up here.
 *   5. IT IS STILL USABLE, not merely readable: a write lands, and the op
 *      itself runs again to completion.
 *
 * THE VACUOUS-PASS GUARD. A run whose kills all landed after the op
 * finished proves nothing while passing, so every try records whether the
 * server had ANSWERED when it was killed -- observed on this side, from
 * the request's own promise, which is the only honest way to know the op
 * was still in flight. The run FAILS unless a third of the kills
 * interrupted an unanswered op AND at least one landed after completion:
 * the first says the window reaches into the op, the second says it
 * reaches past it, and together they say it straddles the commit point.
 *
 * The recovered STATE is reported rather than required to vary, because
 * for a logged op it barely can: the entry is durable before it applies,
 * so replay finishes what the crash interrupted and "applied" is the
 * correct answer at almost every offset. For compact and snapshot -- not
 * logged -- both sides show up.
 *
 * The window is measured, not guessed: a clean run of the op is timed
 * first, and kills are spread over [0, 1.3x] that.
 *
 * Exit 0 = clean, 1 = a finding (or a vacuous run), 2 = the harness
 * itself could not run.
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { writeSync } from 'node:fs';
import { connectServer } from '../src/db-server-client.js';

const NATIVE = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const DB = 'matrix';
const say = (...a) => writeSync(1, a.join(' ') + '\n');

const opts = {
  op: 'all', tries: 25, boots: 3, docs: 1500, seed: 1234, strict: 0,
  port: 39100 + (process.pid % 100) * 4
};
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in opts)) {
    say('usage: node test/crash-matrix.js [--op NAME|all] [--tries N] [--boots N]' +
        ' [--docs N] [--seed N] [--port N] [--strict 1]');
    process.exit(2);
  }
  const raw = process.argv[++i];
  opts[key] = key === 'op' ? raw : Number(raw);
}

let seed = opts.seed >>> 0 || 1;
const rand = () => {
  seed ^= seed << 13; seed >>>= 0;
  seed ^= seed >> 17;
  seed ^= seed << 5;  seed >>>= 0;
  return seed / 0x100000000;
};

/* ---- the server, started and killed ------------------------------------- */

function start(dir) {
  const p = spawn(path.resolve(NATIVE),
    ['--port', String(opts.port), '--raft', '1', '--snapshot-entries', '8'],
    { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
  let err = '';
  p.stderr.on('data', (d) => { err += String(d); });
  return { p, err: () => err };
}

/* Serving, halted, or neither. A halt ENDS serve_forever, so the process
 * exits on its own -- never wait for an exit that may already have
 * happened. */
const settle = async (h) => {
  for (let i = 0; i < 600; i++) {
    if (h.err().includes('serving') || /halted/.test(h.err())) break;
    await new Promise((r) => setTimeout(r, 20));
  }
  await new Promise((r) => setTimeout(r, 150));
  return {
    serving: h.err().includes('serving'),
    halted: /halted/.test(h.err()),
    said: () => h.err().trim().split('\n').slice(-4).join('\n      ')
  };
};
const stop = async (h) => {
  try { h.p.kill('SIGKILL'); } catch { /* already gone */ }
  await new Promise((r) => setTimeout(r, 120));
};

/* ---- the fixture, the oracle, and the ops ------------------------------- */

const PAD = 'x'.repeat(180);

/** Documents, in a shape worth comparing: a `bucket` with repeats so an
 * index lookup has more than one answer to get wrong. */
const seedDocs = async (c, n) => {
  const col = c.db(DB).collection('c');
  for (let at = 0; at < n; at += 20) {
    const batch = [];
    for (let k = at; k < Math.min(at + 20, n); k++) {
      batch.push({ n: k, bucket: k % 7, pad: PAD });
    }
    await col.insertMany(batch);
  }
  /* An unrelated collection, so "the data survived" means more than one
   * collection's worth, and history for compact() to reclaim. */
  const keep = c.db(DB).collection('keep');
  await keep.insertMany(Array.from({ length: 10 }, (_, k) => ({ n: k })));
  for (let k = 0; k < 10; k++) await keep.updateOne({ n: k }, { $set: { seen: k } });
  await keep.deleteMany({ n: { $lt: 3 } });
};

const docsOf = async (c, coll) => {
  const docs = await c.db(DB).collection(coll).find({}, { sort: { n: 1 } }).toArray();
  return docs.map((d) => ({ n: d.n, bucket: d.bucket ?? null, seen: d.seen ?? null }));
};

/** The whole database, as a comparable value. */
const stateOf = async (c) => {
  const out = { collections: (await c.db(DB).listCollections()).sort(), docs: {}, indexes: [] };
  for (const coll of out.collections) out.docs[coll] = await docsOf(c, coll);
  if (out.collections.includes('c')) {
    out.indexes = (await c.db(DB).collection('c').listIndexes())
      .map((i) => i.name).sort();
  }
  return out;
};

/** Files the DATABASE owns, which is where debris would sit. The root's
 * log and generation files are the store's business and are named for
 * their generation, so they are counted separately. */
const dbFiles = (dir) => {
  const at = path.join(dir, DB);
  return fs.existsSync(at) ? fs.readdirSync(at).sort() : [];
};

/*
 * Each op: how to build the state it needs, how to fire it WITHOUT
 * awaiting (so the kill lands inside), how to tell afterwards whether it
 * happened, and what it is allowed to have done to the documents.
 */
const OPS = {
  compact: {
    what: 'rewriting a collection\'s whole file set, unlogged',
    prepare: async (c) => {
      await seedDocs(c, opts.docs);
      await c.db(DB).collection('c').createIndex({ bucket: 1 });
      /* Garbage to reclaim: every update appends a node path. */
      for (let k = 0; k < opts.docs; k += 2) {
        await c.db(DB).collection('c').updateOne({ n: k }, { $set: { touched: true } });
      }
    },
    fire: (c) => c.db(DB).collection('c').compact(),
    retryAlways: true,
    /* Unlogged, so "did it happen" is a file-name question -- and the
     * honest one is whether the OLD primary is gone, not whether a new
     * one exists: compaction writes `g<N>-` files first (db_names.h) and
     * only the catalog commit adopts them, so a g1 file beside a live
     * gen-0 one means the opposite of done. */
    done: (c, dir) => !dbFiles(dir).includes('coll-c.bj'),
    keepsDocuments: true
  },

  createIndex: {
    what: 'building an index and making the catalog adopt it',
    prepare: (c) => seedDocs(c, opts.docs),
    fire: (c) => c.db(DB).collection('c').createIndex({ bucket: 1 }),
    done: async (c) => (await c.db(DB).collection('c').listIndexes())
      .some((i) => i.name === 'bucket_1'),
    keepsDocuments: true
  },

  dropIndex: {
    what: 'removing an index from the catalog and then its files',
    prepare: async (c) => {
      await seedDocs(c, opts.docs);
      await c.db(DB).collection('c').createIndex({ bucket: 1 });
    },
    fire: (c) => c.db(DB).collection('c').dropIndex('bucket_1'),
    /*
     * A CATALOG WRITE AND SOME UNLINKS FINISHES IN UNDER A MILLISECOND, so
     * no wall-clock sleep can land inside one. Looping the op against its
     * own inverse turns a single sub-millisecond window into a stream of
     * them, and the kill lands inside one of the calls -- which is the
     * condition under test, not which of the two it was in.
     */
    loop: true,
    undo: (c) => c.db(DB).collection('c').createIndex({ bucket: 1 }),
    done: async (c) => !(await c.db(DB).collection('c').listIndexes())
      .some((i) => i.name === 'bucket_1'),
    keepsDocuments: true
  },

  snapshot: {
    what: 'writing a generation, whose manifest is the commit',
    prepare: async (c) => {
      await seedDocs(c, opts.docs);
      await c.db(DB).collection('c').createIndex({ bucket: 1 });
    },
    fire: (c) => c.snapshot(),
    retryAlways: true,
    done: async (c) => {
      try { return (await c.latestSnapshot()) !== null; } catch { return false; }
    },
    keepsDocuments: true
  },

  dropCollection: {
    what: 'the control: a fix this matrix must not un-find',
    prepare: async (c) => {
      await seedDocs(c, opts.docs);
      await c.db(DB).collection('c').createIndex({ bucket: 1 });
    },
    fire: (c) => c.db(DB).dropCollection('c'),
    done: async (c) => !(await c.db(DB).listCollections()).includes('c'),
    keepsDocuments: false
  },

  dropDatabase: {
    what: 'the other control, fixed by restoring the generation',
    prepare: async (c) => {
      await seedDocs(c, opts.docs);
      await c.db(DB).collection('c').createIndex({ bucket: 1 });
      /* The shape that made this fatal: a generation at the log's base,
       * so replay alone cannot reach the live state. */
      await c.snapshot();
    },
    fire: (c) => c.dropDatabase(DB),
    done: async (c) => !(await c.listDatabases()).includes(DB),
    keepsDocuments: false
  }
};

/* ---- the checks --------------------------------------------------------- */

/**
 * Every index the catalog still names must answer like a scan. A
 * half-built index that got attached anyway passes every other check in
 * this file and fails this one.
 */
async function indexesAgree(c) {
  if (!(await c.db(DB).listCollections()).includes('c')) return null;
  const col = c.db(DB).collection('c');
  for (let ix of await col.listIndexes()) {
    if (ix.name === '_id_' || !/^bucket_/.test(ix.name)) continue;
    /*
     * A kill inside a staged createIndex leaves a BUILDING definition, and
     * the reboot legitimately RESUMES it (resume_builds): that is recovery
     * doing its job, not debris, and findByIndex answers -79 until the
     * backfill commits. So a building index is waited out -- BOUNDED,
     * because a resumed build that never commits is exactly the regression
     * this file must not wave through -- and then held to the same
     * agreement as any other. One that vanishes instead was aborted or
     * dropped by a replayed inverse, which the file-count check judges.
     */
    const deadline = Date.now() + 15000;
    while (ix && ix.building) {
      if (Date.now() > deadline) {
        return `index ${ix.name} was still building 15s after boot:` +
               ` a resumed build never committed`;
      }
      await new Promise((res) => setTimeout(res, 50));
      ix = (await col.listIndexes()).find((i) => i.name === ix.name);
    }
    if (!ix) continue;
    for (let v = 0; v < 7; v++) {
      const viaIndex = (await col.findByIndex(ix.name, [v])).length;
      const viaScan = await col.countDocuments({ bucket: v });
      if (viaIndex !== viaScan) {
        return `index ${ix.name} answers ${viaIndex} for bucket=${v};` +
               ` a scan finds ${viaScan}`;
      }
    }
  }
  return null;
}

/** The op, run to completion on a fresh fixture: its duration, the file
 * set it leaves, and the state it produces. The kill window and the
 * "allowed" file sets both come from here rather than from a guess. */
async function reference(name) {
  const op = OPS[name];
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-matrix-ref-'));
  const h = start(dir);
  try {
    if (!(await settle(h)).serving) throw new Error('reference server did not start');
    const c = await connectServer(opts.port, { keepAliveMs: 0 });
    await op.prepare(c);
    const before = { state: await stateOf(c), files: dbFiles(dir).length };
    const t0 = Date.now();
    await op.fire(c);
    const ms = Date.now() - t0;
    const after = { state: await stateOf(c), files: dbFiles(dir).length };
    await c.close();
    return { ms: Math.max(ms, 4), before, after };
  } finally {
    await stop(h);
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

/* ---- one op, `tries` kills -------------------------------------------- */

async function runOp(name) {
  const op = OPS[name];
  const ref = await reference(name);
  const window = Math.ceil(ref.ms * 1.3) + 2;
  say(`\n---- ${name}: ${op.what}`);
  say(`     clean run ${ref.ms}ms, killing over [0,${window}ms],` +
      ` ${ref.before.files} db files before / ${ref.after.files} after`);

  const outcomes = new Set();
  const findings = [];
  const leaks = new Set();
  let interrupted = 0;   // killed while the op was still unanswered
  let completed = 0;     // killed after the server had answered
  for (let attempt = 1; attempt <= opts.tries && !findings.length; attempt++) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-matrix-'));
    let h = start(dir);
    let oracle = null;
    if ((await settle(h)).serving) {
      try {
        const c = await connectServer(opts.port, { keepAliveMs: 0 });
        await op.prepare(c);
        oracle = await stateOf(c);
        /* Issued and NOT awaited: the kill has to land inside it. Whether
         * it did is read off the call itself -- in flight when the kill
         * landed, or already answered. A `loop` op runs against its own
         * inverse until the process dies, so that a window too short to
         * aim at becomes one that comes round again. */
        let inFlight = false;
        const drive = async () => {
          do {
            inFlight = true;
            try { await op.fire(c); } finally { inFlight = false; }
            if (op.loop) {
              inFlight = true;
              try { await op.undo(c); } finally { inFlight = false; }
            }
          } while (op.loop);
        };
        drive().catch(() => { inFlight = false; });
        await new Promise((r) => setTimeout(r, Math.floor(rand() * window)));
        if (inFlight) interrupted++; else completed++;
        await stop(h);
        await c.close().catch(() => {});
      } catch { /* the kill raced the request, which is the point */ }
    }
    if (!oracle) { await stop(h); fs.rmSync(dir, { recursive: true, force: true }); continue; }

    /* Every boot, because a deterministic replay fails identically. */
    let recovered = null;
    for (let boot = 1; boot <= opts.boots && !findings.length; boot++) {
      h = start(dir);
      const st = await settle(h);
      if (!st.serving || st.halted) {
        findings.push(`boot ${boot} of ${opts.boots} did not serve` +
          `${st.halted ? ' (HALTED)' : ''}:\n      ${st.said()}`);
        break;
      }
      if (boot === 1) {
        try {
          const c = await connectServer(opts.port, { keepAliveMs: 0 });
          const state = await stateOf(c);
          recovered = (await op.done(c, dir)) ? 'applied' : 'not applied';

          /* 2. the documents */
          if (op.keepsDocuments &&
              JSON.stringify(state.docs) !== JSON.stringify(oracle.docs)) {
            findings.push('the documents changed across the crash\n' +
              `      before: ${JSON.stringify(oracle.docs).slice(0, 160)}\n` +
              `      after:  ${JSON.stringify(state.docs).slice(0, 160)}`);
          }
          /* 3. the indexes */
          const disagreement = await indexesAgree(c);
          if (disagreement && !findings.length) findings.push(disagreement);
          /*
           * 4. debris. A SPACE fault, not a correctness one, so it is
           * reported separately and does not stop the run unless asked:
           * the files are unreferenced, every check above still passes,
           * and the database is correct -- just bigger for ever. Strict
           * mode is for the day something collects them.
           */
          const files = dbFiles(dir).length;
          if (op.keepsDocuments &&
              files !== ref.before.files && files !== ref.after.files) {
            leaks.add(`${files} db files where a clean run leaves` +
              ` ${ref.before.files} (before) or ${ref.after.files} (after):` +
              ` ${dbFiles(dir).join(' ')}`);
          }
          /* 5. still usable: a write, and the op again */
          if (!findings.length) {
            await c.db(DB).collection('after').insertOne({ ok: attempt });
            if (await c.db(DB).collection('after').countDocuments({}) !== 1) {
              findings.push('a write after recovery did not land');
            }
          }
          /*
           * 5b. THE OP MUST STILL BE COMPLETABLE. Only meaningful in one
           * direction: a crash that left the op undone must not leave a
           * database that cannot finish it -- half-built files in the way,
           * a name the catalog still claims. Where the op DID complete,
           * re-running is a different request entirely ("create an index
           * that now exists"), and its refusal is correct rather than a
           * finding. Idempotent ops are retried either way.
           */
          if (!findings.length && (recovered === 'not applied' || op.retryAlways)) {
            try {
              await op.fire(c);
            } catch (err) {
              findings.push(`the op could not be completed after the crash` +
                ` (it had ${recovered}): ${err.message}`);
            }
          }
          await c.close();
        } catch (err) {
          findings.push(`serving, but not usable: ${err.message}\n      ${st.said()}`);
        }
      }
      await stop(h);
    }
    await stop(h);
    if (recovered) outcomes.add(recovered);
    fs.rmSync(dir, { recursive: true, force: true });
  }

  const spread = `${interrupted} kills inside the op, ${completed} after it,` +
    ` recovered to ${[...outcomes].join(' and ') || 'nothing'}`;
  for (const l of leaks) say(`     LEAK  ${l}`);
  if (findings.length) {
    say(`     FINDING (${spread}):`);
    for (const f of findings) say(`       ${f}`);
    return { name, ok: false, leaks, why: findings[0] };
  }
  /* The vacuous-pass guard: a run that never interrupted the op is not a
   * run that survived it. */
  /* A looped op is never idle, so "killed after it finished" is not a
   * state to wait for -- the completions it needs are the thousands it
   * performed while the clock ran. */
  if (interrupted * 3 < opts.tries || (completed === 0 && !op.loop)) {
    say(`     VACUOUS: ${spread} -- the window does not straddle the op's` +
        ` commit point, so this proves nothing (the clean run measured ${ref.ms}ms).`);
    return { name, ok: false, leaks, why: `vacuous: ${spread}` };
  }
  say(`     ${leaks.size ? 'correct but leaking' : 'clean'} over ${opts.tries}` +
      ` kills (${spread}), ${opts.boots} boots each`);
  return { name, ok: !(opts.strict && leaks.size), leaks };
}

/* ---- main -------------------------------------------------------------- */

const main = async () => {
  if (!fs.existsSync(NATIVE)) {
    say(`no server at ${NATIVE} -- ./build/build-server.sh --native`);
    process.exit(2);
  }
  const names = opts.op === 'all' ? Object.keys(OPS) : opts.op.split(',');
  for (const n of names) {
    if (!OPS[n]) { say(`no such op: ${n} (have ${Object.keys(OPS).join(', ')})`); process.exit(2); }
  }
  say(`crash-matrix: ${names.join(', ')} -- ${opts.tries} kills each,` +
      ` ${opts.boots} boots per kill, ${opts.docs} documents, seed ${opts.seed},` +
      ` ${NATIVE}`);

  const results = [];
  for (const n of names) results.push(await runOp(n));

  say('\n==== summary');
  for (const r of results) {
    const tag = r.ok ? (r.leaks?.size ? 'leaks  ' : 'clean  ') : 'FINDING';
    say(`  ${tag}  ${r.name}${r.why ? ` -- ${r.why}` : ''}` +
        `${r.leaks?.size && r.ok ? ' -- unreferenced files nothing collects' : ''}`);
  }
  if (results.some((r) => r.leaks?.size)) {
    say('\n  A LEAK is space, not correctness: the files are unreferenced, every');
    say('  check above passed, and the database is right -- just permanently');
    say('  bigger. This is now expected to be EMPTY: the server sweeps every');
    say('  database at startup and on the open that first reads one');
    say('  (dbs_sweep_orphans), so a leak here is a regression in that. It was');
    say('  how the missing sweep was found -- an interrupted compact kept a');
    say('  whole second copy of the collection for ever. --strict 1 fails on one.');
  }
  process.exit(results.every((r) => r.ok) ? 0 : 1);
};

main().catch((e) => { console.error(e); process.exit(2); });
