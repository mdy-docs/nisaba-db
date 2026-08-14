/**
 * test/apply-oracle.js — THE SAME LOG THROUGH TWO CONFIGS MUST PRODUCE THE
 * SAME BYTES.
 *
 *   node test/apply-oracle.js
 *   node test/apply-oracle.js --a "--read-threads 0" --b "--read-threads 4"
 *   node test/apply-oracle.js --seed 7 --writes 400
 *
 * WHAT IT PROVES, AND WHY BYTES. Replication's whole correctness story is
 * that applying a committed log is deterministic: every replica, and every
 * RESTART of every replica, must arrive at the same state from the same
 * entries. The engine already stakes this ("PLANNING IS WHERE NONDETERMINISM
 * DIES" — an upsert's id, a $currentDate, every generated value is resolved
 * once, by the proposer, and travels IN the command), which has a testable
 * consequence sharper than any behavioral suite: run a workload against
 * config A, hand A's log — just the log — to a fresh directory, boot config
 * B on it, and every data file B replays into existence must be BYTE-EQUAL
 * to A's. Not equivalent. Equal.
 *
 * That is the oracle the write-isolation milestone gates on: when applies
 * move to a writer thread, `--write-thread 0` vs the writer must pass this
 * with the SAME bytes out, which is what makes "byte-for-byte today's path"
 * a proof rather than a claim. It is equally the oracle for any future
 * change to apply ordering, wave sizing, or the pump.
 *
 * WHAT IS COMPARED. Every file replay is responsible for: each database's
 * catalog, collection and index files. The entry log is A's own bytes
 * (copied in, so trivially equal — asserted anyway, against the harness
 * copying the wrong thing). Journals are excluded and the exclusion is
 * load-bearing to state: a journal records the LAST commit's fsync
 * choreography, which differs between "applied while serving" and "applied
 * by boot replay" without either state being wrong — the journal is
 * recovery scaffolding, not state, and a mismatch in actual state would
 * show in the files the journal protects.
 *
 * THE WORKLOAD deliberately reaches every apply path: inserts (single and
 * bulk), updates with $set/$inc/$currentDate (frozen at plan time — that is
 * the point), upserts (planner-resolved ids), deletes, updateMany/deleteMany
 * (one command per matched doc), createIndex and dropIndex (catalog-noted),
 * dropCollection and dropDatabase (the floor cases), across two databases.
 * Snapshots are disabled so the log holds the whole history and replay
 * starts from zero.
 *
 * Exit 0 = byte-equal, 1 = a divergence (each differing file named), 2 =
 * harness failure.
 */
import { spawn } from 'node:child_process';
import crypto from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { writeSync } from 'node:fs';
import { ready, ObjectId } from '../src/nisaba-wasm.js';
import { connectServer } from '../src/db-server-client.js';

await ready();

const NATIVE = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const say = (...a) => writeSync(1, a.join(' ') + '\n');

const opts = {
  a: '', b: '', seed: 1, writes: 250,
  port: 39400 + (process.pid % 200) * 2
};
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in opts)) {
    say('usage: node test/apply-oracle.js [--a "args"] [--b "args"] [--seed N] [--writes N] [--port N]');
    process.exit(2);
  }
  const raw = process.argv[++i];
  opts[key] = (key === 'a' || key === 'b') ? raw : Number(raw);
}

let seed = opts.seed >>> 0 || 1;
const rand = () => {
  seed ^= seed << 13; seed >>>= 0;
  seed ^= seed >> 17;
  seed ^= seed << 5;  seed >>>= 0;
  return seed / 0x100000000;
};
const pick = (arr) => arr[(rand() * arr.length) | 0];
/**
 * Deterministic ids, one of every type format 2 can key: the workload's
 * own ids never vary between runs, so a divergence is the SERVER's,
 * never the driver's.
 *
 * All four types, because this is the sharpest available statement about
 * the key forms they are stored under: a primary key or an index
 * back-pointer derived even slightly differently on the replay path
 * would show up here as bytes that differ, in a way no behavioural test
 * can catch. (It also retires the reason this used to mint ObjectIds
 * alone -- a hex STRING was refused as a scalar _id, which once made the
 * whole workload a 250-write no-op that "passed" its own floor check at
 * applied=1. The vacuous guard below is that lesson made permanent.)
 */
let idAt = 0;
const oid = () => {
  const n = ++idAt;
  switch (n % 4) {
    case 0:  return `id-${n}`;                          // string
    case 1:  return new ObjectId(n.toString(16).padStart(24, '0'));
    case 2:  return n * 1.5;                            // number, non-integer
    default: return new Date(1700000000000 + n);        // date
  }
};

function start(dir, extra) {
  /* Snapshots off: the log must hold the whole history so B replays from
   * zero rather than adopting A's generation -- the oracle is about APPLY,
   * and a restore would let it pass without applying anything. */
  const args = ['--port', String(opts.port), '--raft', '1',
                '--snapshot-entries', '0', ...extra];
  const p = spawn(path.resolve(NATIVE), args, { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
  let err = '';
  p.stderr.on('data', (d) => { err += String(d); });
  return { p, err: () => err };
}
const settle = async (h) => {
  for (let i = 0; i < 600; i++) {
    if (h.err().includes('serving') || /halted/.test(h.err())) break;
    await new Promise((r) => setTimeout(r, 20));
  }
  return h.err().includes('serving');
};
const stop = async (h) => {
  try { h.p.kill('SIGKILL'); } catch { /* gone */ }
  await new Promise((r) => new Promise(() => {}) && r(), 0);
};

/** Every regular file under root, relative paths, sorted. */
function walk(root, at = '') {
  const out = [];
  for (const name of fs.readdirSync(path.join(root, at)).sort()) {
    const rel = at ? `${at}/${name}` : name;
    const st = fs.statSync(path.join(root, rel));
    if (st.isDirectory()) out.push(...walk(root, rel));
    else if (st.isFile()) out.push(rel);
  }
  return out;
}
const sha = (f) => crypto.createHash('sha256').update(fs.readFileSync(f)).digest('hex');

/*
 * WHAT THE COMPARISON OWNS: the files replay creates -- everything inside a
 * database subdirectory except its journals. Root files are excluded for
 * stated reasons, not convenience: the entry log gains B's OWN boot (an
 * election bumps the term in the hard state, and a new leader appends the
 * section-5.4.2 NOOP), and the group file was copied in. Journals are
 * recovery scaffolding -- they record the last commit's fsync choreography,
 * which legitimately differs between "applied while serving" and "applied
 * by boot replay" without either state being wrong; a difference in actual
 * state shows in the files the journal protects.
 *
 * The log's EQUALITY is asserted differently: B must replay to A's applied
 * floor exactly (checked via ping before any bytes are compared).
 */
const excluded = (rel) =>
  !rel.includes('/') || /-journal\.bj$/.test(rel) || /\.nisaba-lock$/.test(rel);

/** The deterministic workload. Every value it generates comes off the
 * seeded PRNG or the oid counter, so two invocations are identical. */
async function drive(port) {
  const c = await connectServer(port, { keepAliveMs: 0 });
  drive.last = '';
  const dbs = ['alpha', 'beta'];
  const colls = ['users', 'items', 'doomed'];
  const made = new Set();

  for (let i = 0; i < opts.writes; i++) {
    const dbName = pick(dbs);
    const collName = pick(colls);
    const db = c.db(dbName);
    const coll = db.collection(collName);
    const key = `${dbName}/${collName}`;
    const kind = rand();
    drive.last = `#${i} kind=${kind.toFixed(3)} ${key}`;
    if (process.env.ORACLE_TRACE) say(`  ${drive.last} -> ${
      kind < 0.30 || !made.has(key) ? 'insertOne' : kind < 0.45 ? 'insertMany'
      : kind < 0.60 ? 'updateMany' : kind < 0.70 ? 'upsert' : kind < 0.80 ? 'deleteMany'
      : kind < 0.88 ? 'createIndex' : kind < 0.94 ? 'dropIndex'
      : kind < 0.98 ? 'dropCollection' : 'dropDatabase'}`);
    try {
      if (kind < 0.30 || !made.has(key)) {
        await coll.insertOne({ _id: oid(), n: (rand() * 1000) | 0, s: `w${i}` });
        made.add(key);
      } else if (kind < 0.45) {
        await coll.insertMany(Array.from({ length: 1 + ((rand() * 8) | 0) },
          () => ({ _id: oid(), n: (rand() * 1000) | 0 })));
      } else if (kind < 0.60) {
        await coll.updateMany({ n: { $lt: (rand() * 500) | 0 } },
          process.env.ORACLE_NO_CD ? { $inc: { bumped: 1 } }
                                   : { $inc: { bumped: 1 }, $currentDate: { seen: true } });
      } else if (kind < 0.70) {
        await coll.updateOne({ _id: oid() }, { $set: { up: i } }, { upsert: true });
      } else if (kind < 0.80) {
        await coll.deleteMany({ n: { $gt: (rand() * 900) | 0 } });
      } else if (kind < 0.88) {
        await coll.createIndex({ n: 1 });
      } else if (kind < 0.94) {
        await coll.dropIndex('n_1');
      } else if (kind < 0.98) {
        await db.dropCollection(collName);
        made.delete(key);
      } else {
        await c.dropDatabase(dbName);
        for (const k of [...made]) if (k.startsWith(`${dbName}/`)) made.delete(k);
      }
    } catch (err) {
      /* A refused write (duplicate key, no such index) is itself
       * deterministic -- the planner refused it identically both times,
       * and nothing reached the log. Counted, because a workload that is
       * SECRETLY all refusals proves nothing (it happened: applied=9 of
       * 249 "successful" ops, hidden by a bare catch). */
      drive.refused = (drive.refused ?? 0) + 1;
      if (drive.refused <= 10) say(`    refusal at [${drive.last}]: ${err.code ?? ''} ${err.message}`);
    }
  }
  drive.last = 'final ping';
  const applied = (await c.ping()).applied;
  await c.close();
  if (drive.refused) say(`  refused: ${drive.refused}/${opts.writes}`);
  return applied;
}

const main = async () => {
  if (!fs.existsSync(NATIVE)) {
    say(`no server at ${NATIVE} -- ./build/build-server.sh --native`);
    process.exit(2);
  }
  const argsA = opts.a ? opts.a.split(/\s+/) : [];
  const argsB = opts.b ? opts.b.split(/\s+/) : [];
  say(`apply-oracle: ${opts.writes} writes, seed ${opts.seed},` +
      ` A=[${argsA.join(' ') || 'default'}] B=[${argsB.join(' ') || 'default'}], ${NATIVE}`);

  /* ---- run A: serve the workload, keep everything ---------------------- */
  const dirA = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-oracle-a-'));
  let h = start(dirA, argsA);
  if (!(await settle(h))) { say('A did not start:\n' + h.err()); process.exit(2); }
  let applied;
  try {
    applied = await drive(opts.port);
  } catch (e) {
    say(`A died mid-workload at [${drive.last}]: ${e.message}`);
    say(`A process alive: ${h.p.exitCode === null}, exitCode ${h.p.exitCode}, signal ${h.p.signalCode}`);
    say('A said:\n  ' + h.err().trim().split('\n').slice(-6).join('\n  '));
    try {
      const c2 = await connectServer(opts.port, { keepAliveMs: 0 });
      const pg = await c2.ping();
      say(`a NEW connection works: applied=${pg.applied} -- only the CONNECTION was killed`);
      await c2.close();
    } catch (e2) {
      say(`a new connection also fails: ${e2.message}`);
    }
    process.exit(2);
  }
  h.p.kill('SIGKILL');
  await new Promise((r) => h.p.once('exit', r));
  say(`  A applied ${applied} entries across ${walk(dirA).length} files`);
  /* The vacuous guard: a workload whose writes were all refused leaves a
   * floor of almost nothing, and an oracle comparing two empty rooms
   * proves only that they are empty. */
  if (applied < opts.writes / 2) {
    say(`VACUOUS: ${opts.writes} writes applied only ${applied} entries --` +
        ` the workload is not reaching the log.`);
    process.exit(1);
  }

  /* ---- run B: ONLY the log, replayed by boot --------------------------- */
  const dirB = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-oracle-b-'));
  for (const f of fs.readdirSync(dirA)) {
    /* The root's own files: the entry log and the group id. Nothing from
     * inside a database directory -- those are what replay must recreate. */
    if (fs.statSync(path.join(dirA, f)).isFile()) {
      fs.copyFileSync(path.join(dirA, f), path.join(dirB, f));
    }
  }
  h = start(dirB, argsB);
  if (!(await settle(h))) { say('B did not start (replay failed?):\n' + h.err()); process.exit(1); }
  /* Replay finishes before "serving", but the floor is the proof. */
  const cb = await connectServer(opts.port, { keepAliveMs: 0 });
  for (let i = 0; i < 200; i++) {
    if ((await cb.ping()).applied >= applied) break;
    await new Promise((r) => setTimeout(r, 50));
  }
  const appliedB = (await cb.ping()).applied;
  await cb.close();
  h.p.kill('SIGKILL');
  await new Promise((r) => h.p.once('exit', r));
  if (appliedB < applied) {
    say(`DIVERGED before comparing: B replayed to ${appliedB}, A applied ${applied}`);
    process.exit(1);
  }

  /* ---- the comparison --------------------------------------------------- */
  const filesA = walk(dirA).filter((f) => !excluded(f));
  const filesB = walk(dirB).filter((f) => !excluded(f));
  const only = (xs, ys) => xs.filter((x) => !ys.includes(x));
  const findings = [];
  for (const f of only(filesA, filesB)) findings.push(`only in A: ${f}`);
  for (const f of only(filesB, filesA)) findings.push(`only in B: ${f}`);
  let compared = 0;
  for (const f of filesA) {
    if (!filesB.includes(f)) continue;
    const a = sha(path.join(dirA, f)), b = sha(path.join(dirB, f));
    compared++;
    if (a !== b) findings.push(`differs: ${f} (${a.slice(0, 12)} vs ${b.slice(0, 12)})`);
  }

  if (process.env.ORACLE_KEEP) {
    say(`  kept: A=${dirA} B=${dirB}; A applied ${applied}, B ${appliedB}`);
  } else {
    fs.rmSync(dirA, { recursive: true, force: true });
    fs.rmSync(dirB, { recursive: true, force: true });
  }

  if (findings.length) {
    say(`DIVERGED -- the same log produced different bytes:`);
    for (const f of findings) say(`  ${f}`);
    process.exit(1);
  }
  if (compared < 3) {
    say(`VACUOUS: only ${compared} files compared -- the workload made nothing.`);
    process.exit(1);
  }
  say(`byte-equal: ${compared} files identical after replaying ${applied} entries` +
      ` through a different config. Journals excluded (recovery scaffolding,` +
      ` not state).`);
};
main().catch((e) => { console.error(e); process.exit(2); });
