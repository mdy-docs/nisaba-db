/**
 * test/repro-halt-on-drop.js — a REPLICATED MEMBER KILLED DURING A
 * dropCollection NEVER STARTS AGAIN. Open bug; this reproduces it in
 * seconds, and exits 1 when it does.
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
 * THE INVARIANT BROKEN. A destructive file operation must not become
 * durable before the record of the entries that precede it. Which of the
 * two ends to fix -- the ordering, or making a replayed DDL whose
 * collection a later entry removed count as convergence the way a
 * re-applied createIndex (-56) and dropIndex (-57) already do -- is a
 * decision about the durability contract, and is deliberately not taken
 * here.
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

const opts = { tries: 60, maxDelay: 25, boots: 4,
               port: 38600 + (process.pid % 120) * 4 };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in opts)) {
    console.error('usage: node test/repro-halt-on-drop.js [--tries N]' +
                  ' [--maxDelay MS] [--boots N] [--port N]');
    process.exit(2);
  }
  opts[key] = Number(process.argv[++i]);
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
  say(`repro-halt-on-drop: up to ${opts.tries} kills, ${opts.maxDelay}ms window,` +
      ` ${opts.boots} boots each, ${NATIVE}`);

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
      db.dropCollection('c').catch(() => {});
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
  say(`did not reproduce in ${opts.tries} kills -- which is not the same as` +
      ' fixed; widen --maxDelay or raise --tries');
};
main().catch((e) => { console.error(e); process.exit(2); });
