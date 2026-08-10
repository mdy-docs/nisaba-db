/**
 * test/repro-halt-on-drop.js — a REPLICATED MEMBER KILLED DURING A
 * dropCollection NEVER STARTS AGAIN. FIXED (the catalog now carries an
 * applied index a drop cannot delete -- db_session.c's
 * catalog_note_applied); this is kept because it is the sharpest statement
 * of what the fix is for, it reproduces the failure in one go against any
 * build without it, and it exits 1 if it ever comes back.
 *
 *   node test/repro-halt-on-drop.js
 *   node test/repro-halt-on-drop.js --tries 200 --maxDelay 40
 *
 * WHAT HAPPENS. Insert, index, then drop a collection, and kill -9 the
 * member a few milliseconds into the drop. On restart it replays, and:
 *
 *   replica: entry 42 (opcode 4, collection 'c') would not apply:
 *            No collection of that name in this database's catalog
 *   replica: halted (-37: ...)
 *
 * Opcode 4 is DC_WAL_CREATE_INDEX. So the entry being replayed is the
 * createIndex at 42, and the collection it names is gone -- because the
 * dropCollection at 43 removed it. The drop's effect on the FILES became
 * durable while the recorded applied index stayed below 42, so replay
 * starts at an index whose state no longer exists.
 *
 * WHY IT MATTERS MORE THAN A HALT USUALLY WOULD. The halt itself is
 * correct: -37 on apply is either a log this member cannot apply or a
 * state that has drifted, and db_validate.c deliberately resolves that
 * ambiguity toward stopping rather than diverging in silence. But replay
 * is deterministic, so every subsequent boot fails identically -- measured
 * at four consecutive boots, all halted. The database cannot be opened
 * again. On a deployment that runs one replicated server per tenant, that
 * is a tenant's data unavailable after one ill-timed crash, OOM or reboot.
 *
 * WHAT IT IS NOT. Not a reader-thread bug: this passes no --read-threads
 * and needs no cluster, no snapshot install, and no sanitizer. It was
 * FOUND by test/soak-install.js, which kills a member every few seconds,
 * but it predates every part of that milestone.
 *
 * THE INVARIANT BROKEN, and where it was repaired. The replay floor is a
 * MAX over surviving collections, so a drop could make it regress: the
 * record of what had been applied lived in the very files the drop
 * deleted. It now also lives in the CATALOG, which is the one structure a
 * drop both keeps and writes -- so the floor cannot go backwards, and
 * replay never resumes below the state the live files are in.
 *
 * The alternative considered and rejected was to let a replayed DDL whose
 * collection a later entry removed count as convergence, the way a
 * re-applied createIndex (-56) and dropIndex (-57) already do. It is
 * smaller, but it treats the symptom: the floor would still be wrong, and
 * -37 on apply would stop meaning "this member's state has drifted".
 *
 * Exit 0 = did not reproduce (say so, loudly, rather than passing
 * quietly), 1 = reproduced, 2 = the harness itself failed.
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { writeSync } from 'node:fs';
import { connectServer } from '../src/db-server-client.js';

const NATIVE = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const DB = 'reproduced';

/*
 * `via` chooses WHICH drop deletes the record:
 *
 *   collection  FIXED. A dropCollection deletes the collection's files, and
 *               with them the applied index they carried; the catalog now
 *               carries one too, and a drop keeps the catalog.
 *   database    STILL OPEN. A dropDatabase removes the whole directory, the
 *               catalog included, so the INSTANCE-level floor (a max over
 *               databases) regresses exactly as the database-level one did.
 *               Reproduces on the first try; there is nowhere left inside
 *               the instance for the record to survive, so the fix is not
 *               another catalog but either a root-level record or restoring
 *               the committed generation when the floor sits below the log's
 *               base. See docs/db-server.md.
 */
const opts = { tries: 60, maxDelay: 25, boots: 4, via: 'collection',
               port: 38600 + (process.pid % 120) * 4 };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in opts)) {
    console.error('usage: node test/repro-halt-on-drop.js [--tries N]' +
                  ' [--maxDelay MS] [--boots N] [--port N]');
    process.exit(2);
  }
  const raw = process.argv[++i];
  opts[key] = key === 'via' ? raw : Number(raw);
}
if (opts.via !== 'collection' && opts.via !== 'database') {
  console.error("--via must be 'collection' or 'database'");
  process.exit(2);
}

const say = (...a) => writeSync(1, a.join(' ') + '\n');

let seed = 999;
const rand = () => {
  seed ^= seed << 13; seed >>>= 0;
  seed ^= seed >> 17;
  seed ^= seed << 5;  seed >>>= 0;
  return seed / 0x100000000;
};

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
 * happened, which is a hang rather than a result. */
const settle = async (h) => {
  for (let i = 0; i < 500; i++) {
    if (h.err().includes('serving') || /halted/.test(h.err())) break;
    await new Promise((r) => setTimeout(r, 20));
  }
  await new Promise((r) => setTimeout(r, 200));
  return { serving: h.err().includes('serving'), halted: /halted/.test(h.err()) };
};
const stop = async (h) => {
  try { h.p.kill('SIGKILL'); } catch { /* already gone */ }
  await new Promise((r) => setTimeout(r, 120));
};

const main = async () => {
  if (!fs.existsSync(NATIVE)) {
    say(`no server at ${NATIVE} -- ./build/build-server.sh --native`);
    process.exit(2);
  }
  say(`repro-halt-on-drop: drop a ${opts.via}, up to ${opts.tries} kills,` +
      ` ${opts.maxDelay}ms window, ${opts.boots} boots each, ${NATIVE}`);

  for (let attempt = 0; attempt < opts.tries; attempt++) {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-repro-'));
    let h = start(dir);
    if (!(await settle(h)).serving) {
      await stop(h);
      fs.rmSync(dir, { recursive: true, force: true });
      continue;
    }
    try {
      const c = await connectServer(opts.port, { keepAliveMs: 0 });
      const db = c.db(DB);
      await db.collection('c').insertMany(
        Array.from({ length: 40 }, (_, k) => ({ n: k, pad: 'x'.repeat(200) })));
      /* The entry that will be replayed into a collection that is gone. */
      await db.collection('c').createIndex({ n: 1 });
      /* Issued and NOT awaited, so the kill lands inside it. */
      if (opts.via === 'database') c.dropDatabase(DB).catch(() => {});
      else db.dropCollection('c').catch(() => {});
      await new Promise((r) => setTimeout(r, Math.floor(rand() * opts.maxDelay)));
      await stop(h);
      await c.close().catch(() => {});
    } catch { /* the kill raced the request, which is the point */ }

    let halted = 0, words = '';
    for (let boot = 1; boot <= opts.boots; boot++) {
      h = start(dir);
      const st = await settle(h);
      if (st.halted) {
        if (!halted) words = h.err().trim().split('\n').slice(-3).join('\n    ');
        halted++;
      }
      await stop(h);
      if (!st.halted) break;
    }
    fs.rmSync(dir, { recursive: true, force: true });

    if (halted) {
      say(`\nREPRODUCED after ${attempt + 1} kill(s):\n    ${words}`);
      say(halted >= opts.boots
        ? `\n${halted} consecutive boots ALL halted: the database cannot be` +
          ' opened again.'
        : `\nrecovered after ${halted} halted boot(s).`);
      process.exit(1);
    }
  }
  say(`no halt in ${opts.tries} kills dropping a ${opts.via}: the fix holds.` +
      ' A REPRODUCED line here means it is back -- widen --maxDelay or raise' +
      ' --tries to push harder.');
};
main().catch((e) => { console.error(e); process.exit(2); });
