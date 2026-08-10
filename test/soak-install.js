/**
 * test/soak-install.js — the destructive case test/soak.js cannot reach:
 * a SNAPSHOT INSTALL replacing every file in an instance while reader
 * threads are inside read views of those files.
 *
 *   node test/soak-install.js                          # 60s, 3 members
 *   node test/soak-install.js --seconds 3600 --readThreads 4
 *   NISABA_SERVER_BIN=build/lib/nisaba-server-asan node test/soak-install.js
 *
 * WHY A SECOND SOAK, rather than a flag on the first. `test/soak.js` runs
 * ONE member, so the only destruction it can produce is the kind a client
 * asks for: `compact`, `dropCollection`, `dropIndex`. Installs are not
 * available there at all -- a member only receives one from a leader whose
 * log has been compacted past what that member still needs -- and an
 * install is the largest unmaking the server does. It closes the whole
 * `dbi` and puts DIFFERENT FILES where every database looks
 * (`adopt_install` in server/main.c), so it invalidates not one
 * collection's read views but all of them at once, and it arrives as a
 * committed entry with no client request behind it.
 *
 * That last part is what makes it worth its own harness. On a leader, a
 * destructive request drains the reader pool before it is even proposed.
 * An install has no request: the drain has to be on the apply side, on a
 * member nobody is writing to -- and a follower is exactly the member
 * holding read views, because stale reads are the only reads it serves.
 *
 * HOW IT FORCES ONE. Three members with `--snapshot-entries 4`, so a
 * leader's log base advances almost continuously. Every few seconds a
 * NON-LEADER is killed and restarted, occasionally onto a wiped directory:
 * a member whose log is behind the leader's base cannot be caught up by
 * AppendEntries, so the leader sends it an install.
 * Meanwhile every reader is running stale scanning reads against all three
 * members, which `--read-offload-min 0` puts on worker threads.
 *
 * WHAT IT ASSERTS, and it is content rather than liveness, for the same
 * reason soak.js is: a view whose files were unlinked under it means a
 * `pread` on a recycled descriptor, which returns another file's bytes
 * with NO ERROR AT ALL. So every document describes itself --
 *
 *   { n, echo: n, tag: `v<n>`, coll: '<collection name>' }
 *
 * -- and a stale read of a lagging member is still checkable, which is the
 * part that took thinking about. A follower may hold FEWER documents than
 * the leader, or a whole generation from before a drop, so the count is not
 * an oracle. What is: every document must be self-consistent, must name the
 * collection it was read from, and the set must be a PREFIX -- ids
 * contiguous from 0 -- because the writer only ever appends id n after id
 * n-1. Lagging is legal; a gap is not, and bytes from another file are not.
 *
 * AND IT REFUSES TO PASS VACUOUSLY. A run that produced no install, or
 * whose installs never once had to wait for a reader, has not tested the
 * thing it is named for -- so it fails. `installs` comes from the members'
 * own stderr ("snapshot install adopted at index N"); `installDrains` comes
 * from `ping`, which counts installs that found a reader inside a view and
 * waited. Both have to move.
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
const COLLS = ['alpha', 'beta', 'gamma'];

function usage(msg) {
  if (msg) console.error(`error: ${msg}`);
  console.error(
    'usage: node test/soak-install.js [--seconds N] [--readers N] [--seed N]\n' +
    '                                 [--port N] [--readThreads N] [--churnMs N]\n' +
    '                                 [--pad BYTES] [--cap DOCS] [--quiet]');
  process.exit(2);
}

/*
 * `pad` is not padding for its own sake -- it is what makes the barrier
 * REACHABLE. A member refuses reads while an install is moving its state
 * machine (-66), so the only read that can still be in flight when adoption
 * runs is one submitted just before the install landed and still running.
 * With tiny documents a scan finishes in under a millisecond and that window
 * is almost never open: the first version of this file adopted 189 installs
 * and drained a reader for exactly ONE of them, which is a run that passes
 * without testing what it is named for. A padded document makes a scan tens
 * of milliseconds, which is wide enough to hit repeatedly.
 */
/*
 * A STRIDE, not an offset. Three members need six ports (client + peer
 * each), and `35000 + pid % 600` put consecutive runs 1 apart -- so two of
 * these at once collided and the second died with "Address already in use".
 * Catching an intermittent failure means running several at once, which is
 * exactly when that bites. Twenty apart leaves room for a member count to
 * grow, too.
 */
const opts = { seconds: 60, readers: 8, seed: 1, port: 35000 + (process.pid % 140) * 20,
               readThreads: 4, churnMs: 6000, pad: 400, cap: 4000, quiet: false };
for (let i = 2; i < process.argv.length; i++) {
  const a = process.argv[i];
  if (a === '--quiet') { opts.quiet = true; continue; }
  const key = a.replace(/^--/, '');
  if (!(key in opts) || a[0] !== '-') usage(`unknown option ${a}`);
  const v = Number(process.argv[++i]);
  const floor = key === 'readThreads' ? 0 : 1;
  if (!Number.isFinite(v) || v < floor) usage(`${a} needs a number of at least ${floor}`);
  opts[key] = v;
}

/** xorshift32: small, seeded, identical everywhere. */
function rng(seed) {
  let s = seed >>> 0 || 1;
  return () => {
    s ^= s << 13; s >>>= 0;
    s ^= s >> 17;
    s ^= s << 5;  s >>>= 0;
    return s / 0x100000000;
  };
}

const say = (...a) => { if (!opts.quiet) console.log(...a); };
const violations = [];
const note = (what) => {
  violations.push(what);
  console.error(`VIOLATION: ${what}`);
};

/*
 * WHAT EVERY LOOP IS WAITING FOR, so that a hang is a report rather than a
 * mystery. Each loop owns one slot and writes into it before every request.
 *
 * This is not defensive scaffolding: it is the instrument that found the bug
 * this file was written to look for. A run whose loops all check a deadline
 * can still fail to finish, because a request that is never answered is an
 * `await` that never returns -- and with `--idle-timeout` set high, as it is
 * here, nothing on either side ever times it out. What that looks like from
 * outside is a 90-second soak still running after 7 minutes with three
 * healthy members answering ping in 6ms, which says almost nothing. What it
 * looks like with these slots is the connection, the member and the exact
 * request that went unanswered.
 */
const slots = [];
const newSlot = (label) => {
  const s = { label, what: null, at: 0, done: 0 };
  slots.push(s);
  return s;
};
const stuckReport = () => slots
  .filter((s) => s.what)
  .map((s) => `    ${s.label}: waiting ${((Date.now() - s.at) / 1000).toFixed(1)}s` +
              ` on ${s.what} (${s.done} completed before it)`);

/*
 * Refusals this workload legitimately produces, BY CODE. A cluster being
 * deliberately broken produces more of them than soak.js does, and each one
 * is here because of what it means rather than to quieten the output:
 *
 *   -37 the collection was dropped between choosing it and reading it
 *   -49 compact refused: a cursor is scanning
 *   -56 / -57 createIndex or dropIndex this loop has already done
 *   -63 / -64 no leader, or this member is not it -- an election is running
 *             because the member we killed was holding one up
 *   -66 the state machine is moving under an install: the member says so
 *       rather than answering out of files it is about to replace
 *   -71 the database was dropped while a request was in flight
 */
const EXPECTED = new Set([-37, -49, -56, -57, -63, -64, -66, -71]);
const expected = (err) => EXPECTED.has(err?.code);

/*
 * Refusals TALLIED BY CODE, not counted in a lump. A run of this reports
 * more refusals than reads, and that is only reassuring if you can see that
 * they are -66 ("the state machine is moving under an install") rather than,
 * say, -37 for a collection nobody dropped. A lump total hides the
 * difference between a cluster being deliberately broken and a cluster that
 * is broken.
 */
const refusals = new Map();
const refused = (err) => {
  const k = err?.code ?? 'transport';
  refusals.set(k, (refusals.get(k) ?? 0) + 1);
};
const refusalLine = () => [...refusals.entries()]
  .sort((a, b) => b[1] - a[1])
  .map(([k, n]) => `${k}x${n}`)
  .join(' ');

/* The pad is not checked -- it is there to make a scan cost something, so
 * that a read is still in flight when an install adopts. Everything else in
 * the document is an oracle. */
const PAD = 'x'.repeat(opts.pad);
const docFor = (coll, n) => ({ n, echo: n, tag: `v${n}`, coll, pad: PAD });

/* A pattern that differs every call and matches the same documents, so
 * every read is a full scan AND a fresh compile -- the compile is the one
 * piece of process-global state a worker can reach. */
const regexFor = (k) => ({ tag: { $regex: `^v(?:[0-9]+|zzz${k})$` } });

/*
 * The oracle, weakened exactly as far as a lagging member requires and no
 * further. Not a count -- a follower legitimately holds less than the
 * leader, and after a drop it may briefly hold a whole earlier generation.
 */
function checkStale(coll, docs) {
  const ns = [];
  for (const d of docs) {
    if (d.coll !== coll) { note(`${coll}: a document from collection "${d.coll}" -- wrong file`); return; }
    if (d.echo !== d.n) { note(`${coll}: torn document, n=${d.n} echo=${d.echo}`); return; }
    if (d.tag !== `v${d.n}`) { note(`${coll}: torn document, n=${d.n} tag=${d.tag}`); return; }
    ns.push(d.n);
  }
  ns.sort((a, b) => a - b);
  /* A PREFIX, because the writer appends n only after n-1. Behind is fine;
   * a hole is a read that skipped an id it held. */
  for (let i = 0; i < ns.length; i++) {
    if (ns[i] !== i) { note(`${coll}: stale read skipped id ${i} but returned ${ns[i]}`); return; }
  }
}

const MEMBERS = [1, 2, 3].map((id) => ({
  id, port: opts.port + id, raftPort: opts.port + 10 + id,
  proc: null, dir: null, log: '', installs: 0,
  /* Counters survive a restart by being banked before the kill: the server
   * they came from is about to be a different process. */
  banked: { drainWaits: 0, drainedReads: 0, installDrains: 0, installDrainedReads: 0,
            movedReads: 0, reads: 0 }
}));

function argsFor(m) {
  const a = ['--port', String(m.port), '--max-clients', '32',
    '--raft', String(m.id), '--raft-port', String(m.raftPort),
    /* Four, so the base moves almost continuously and a member that was
     * away for a moment can no longer be caught up by AppendEntries. That
     * is the whole mechanism: without aggressive compaction there is
     * nothing to install. */
    '--snapshot-entries', '4',
    /* Wide, because this soak deliberately removes a member and loads the
     * machine: at the LAN default the cluster would spend the run electing,
     * which measures the election rather than the install. */
    '--election-timeout', '1500:3000', '--heartbeat', '300',
    /* Long enough that a member killed mid-scan is not also fighting the
     * idle sweep, which is a different test's subject. */
    '--idle-timeout', '300'];
  if (opts.readThreads > 0) {
    a.push('--read-threads', String(opts.readThreads), '--read-offload-min', '0');
  }
  for (const o of MEMBERS) if (o.id !== m.id) a.push('--peer', `${o.id}@127.0.0.1:${o.raftPort}`);
  return a;
}

async function startMember(m, freshDir) {
  if (freshDir || !m.dir) {
    if (m.dir) fs.rmSync(m.dir, { recursive: true, force: true });
    m.dir = fs.mkdtempSync(path.join(os.tmpdir(), `nisaba-inst${m.id}-`));
  }
  const proc = spawn(path.resolve(NATIVE), argsFor(m), {
    cwd: m.dir, stdio: ['ignore', 'pipe', 'pipe']
  });
  m.proc = proc;
  let ready = false;
  proc.stderr.on('data', (d) => {
    const text = String(d);
    m.log += text;
    if (text.includes('serving')) ready = true;
    /* THE PROOF OF HOW a member caught up. Counted here rather than
     * inferred from an index, because "it reached the leader's floor" is
     * also what AppendEntries does. */
    for (const _ of text.matchAll(/snapshot install adopted at index/g)) m.installs++;
    if (/AddressSanitizer|ThreadSanitizer|LeakSanitizer|runtime error/.test(text)) {
      note(`member ${m.id} sanitizer: ${text.trim().split('\n')[0]}`);
    }
    if (/halted|adoption failed|readers would not finish/.test(text)) {
      /*
       * With the member's own last words, not just the line that matched.
       * A halt prints WHICH entry it choked on (index, opcode, collection)
       * on the line before -- and reporting only the match threw exactly
       * that away, which cost one reproduction of twelve minutes.
       */
      const tail = m.log.trim().split('\n').slice(-14).join('\n      ');
      note(`member ${m.id} halted; its last words:\n      ${tail}`);
    }
  });
  const until = Date.now() + 30000;
  while (!ready && Date.now() < until) await new Promise((r) => setTimeout(r, 20));
  if (!ready) throw new Error(`member ${m.id} did not start: ${m.log.slice(-400)}`);
}

/** Bank what this incarnation counted, before it stops existing. */
async function bank(m) {
  try {
    const c = await connectServer(m.port, { keepAliveMs: 0 });
    const p = await c.ping();
    await c.close();
    for (const k of Object.keys(m.banked)) m.banked[k] += Number(p[k] ?? 0);
  } catch { /* already gone: what it counted since the last poll is lost,
               which undercounts and never overcounts */ }
}

const main = async () => {
  if (!fs.existsSync(NATIVE)) usage(`no server binary at ${NATIVE} -- ./build/build-server.sh --native`);

  const deadline = Date.now() + opts.seconds * 1000;
  const rand = rng(opts.seed);
  const counts = new Map(COLLS.map((c) => [c, 0]));
  const stats = { writes: 0, reads: 0, regexReads: 0, compacts: 0, drops: 0,
                  indexes: 0, cycles: 0, wipes: 0, reconnects: 0 };

  for (const m of MEMBERS) await startMember(m, true);

  /** Whoever will take a write; null if the cluster has no leader now. */
  const findLeader = async () => {
    for (const m of MEMBERS) {
      if (!m.proc) continue;
      try {
        const c = await connectServer(m.port, { keepAliveMs: 0 });
        const role = (await c.ping()).role;
        if (role === 'leader') return { m, c };
        await c.close();
      } catch { /* down, or mid-election */ }
    }
    return null;
  };

  let lead = null;
  for (let i = 0; i < 200 && !lead; i++) {
    lead = await findLeader();
    if (!lead) await new Promise((r) => setTimeout(r, 200));
  }
  if (!lead) throw new Error('no member would lead');

  say(`soak-install: ${opts.seconds}s, 3 members, ${opts.readers} readers,` +
      ` ${opts.readThreads} reader thread(s), churn every ${opts.churnMs}ms,` +
      ` seed ${opts.seed}, ports ${opts.port + 1}-${opts.port + 3}, ${NATIVE}`);

  const stop = () => Date.now() >= deadline || violations.length > 0;

  /* A long run has to say something before it ends -- and here it also has
   * to say whether the barrier under test is being REACHED, because a run
   * adopting installs that never overlap a reader is a run that will pass
   * without testing anything. */
  const heartbeat = opts.seconds > 90 && !opts.quiet ? setInterval(() => {
    const left = Math.round((deadline - Date.now()) / 1000);
    if (left < 30) return;
    const drains = MEMBERS.reduce((t, m) => t + m.banked.installDrains, 0);
    const inst = MEMBERS.reduce((t, m) => t + m.installs, 0);
    say(`  +${opts.seconds - left}s (${left}s left): ${stats.writes} writes,` +
        ` ${stats.reads} reads, ${stats.cycles} cycles, ${inst} installs,` +
        ` ${drains}+ install drains banked, ${stats.drops} drops`);
  }, 60000) : null;
  heartbeat?.unref?.();

  /*
   * ONE WRITER, on whichever member leads, and it owns `counts` -- so the
   * expected content of a collection is known to exactly one place and a
   * reader that disagrees is reporting a real disagreement. It re-finds the
   * leader whenever the one it had stops being one, which this soak causes
   * on purpose.
   */
  const wslot = newSlot('writer');
  const writing = (async () => {
    while (!stop()) {
      if (!lead) {
        wslot.what = 'finding a leader';
        wslot.at = Date.now();
        lead = await findLeader();
        wslot.what = null;
        if (!lead) { await new Promise((r) => setTimeout(r, 200)); continue; }
      }
      const coll = COLLS[(rand() * COLLS.length) | 0];
      const roll = rand();
      wslot.at = Date.now();
      try {
        const db = lead.c.db(DB);
        /*
         * A collection that is CAPPED rather than dropped constantly, which
         * is the opposite of soak.js's mix and is deliberate. A read has to
         * cost something for the install barrier to be reachable at all, and
         * a collection dropped every few seconds never holds more than a few
         * hundred documents -- the first version of this file kept them at
         * that size and drained a reader for 2 installs out of 168. Drops
         * still happen, rarely; they are soak.js's subject, not this one's.
         */
        if (roll < 0.80 && counts.get(coll) < opts.cap) {
          const at = counts.get(coll);
          wslot.what = `insertMany ${coll} at ${at} -> member ${lead.m.id}`;
          /* A batch, so the log advances fast enough for
           * --snapshot-entries 4 to keep compacting the base out from
           * under a member that was away for half a second. */
          const batch = Array.from({ length: 8 }, (_, k) => docFor(coll, at + k));
          await db.collection(coll).insertMany(batch);
          counts.set(coll, at + batch.length);
          stats.writes += batch.length;
        } else if (roll < 0.90) {
          wslot.what = `compact ${coll} -> member ${lead.m.id}`;
          await db.collection(coll).compact();
          stats.compacts++;
        } else if (roll < 0.94) {
          wslot.what = `createIndex ${coll} -> member ${lead.m.id}`;
          await db.collection(coll).createIndex({ n: 1 });
          stats.indexes++;
        } else if (roll < 0.98) {
          wslot.what = `dropIndex ${coll} -> member ${lead.m.id}`;
          await db.collection(coll).dropIndex('n_1');
          stats.indexes++;
        } else {
          wslot.what = `dropCollection ${coll} -> member ${lead.m.id}`;
          await db.dropCollection(coll);
          counts.set(coll, 0);
          stats.drops++;
        }
        wslot.done++;
      } catch (err) {
        if (err?.code === -63 || err?.code === -64 || typeof err?.code !== 'number') {
          /* Leadership moved, or the socket to it did -- which this soak
           * arranges. Let go and look again rather than treating a correct
           * refusal, or a deliberate kill, as a fault. */
          await lead.c.close().catch(() => {});
          lead = null;
          refused(err);
        } else if (!expected(err)) {
          note(`writer: [${err?.code}] ${err.message}`);
        } else {
          refused(err);
        }
      }
      wslot.what = null;
    }
    await lead?.c.close().catch(() => {});
  })();

  /*
   * READERS, spread over all three members, STALE. Stale is the point: a
   * follower answers it out of its own files with no barrier, which is both
   * how nisaba-web routes scan-heavy work and the only read a member being
   * caught up by an install ever has in flight.
   */
  const reading = Array.from({ length: opts.readers }, (_, k) => (async () => {
    let m = MEMBERS[k % MEMBERS.length], c = null;
    const stale = { stale: true };
    let pat = k * 1_000_000;
    const slot = newSlot(`reader ${k}`);
    while (!stop()) {
      if (!c) {
        try {
          c = await connectServer(m.port, { keepAliveMs: 0 });
        } catch {
          /* Its member is down -- this soak keeps killing them. Move to
           * another one rather than spinning on a closed port. */
          m = MEMBERS[(MEMBERS.indexOf(m) + 1) % MEMBERS.length];
          stats.reconnects++;
          await new Promise((r) => setTimeout(r, 50));
          continue;
        }
      }
      const coll = COLLS[(rand() * COLLS.length) | 0];
      slot.at = Date.now();
      try {
        const cl = c.db(DB).collection(coll);
        /*
         * Mostly count, sometimes find. Both are full scans on a worker --
         * the same path, the same view -- but a `find` of a capped
         * collection ships every document, which at `cap` x `pad` is
         * megabytes and would make this a benchmark of the socket. The
         * count does the scanning; the find does the checking.
         */
        if (rand() < 0.25) {
          slot.what = `stale find ${coll} on member ${m.id}`;
          const docs = await cl.find(regexFor(pat++), stale).toArray();
          checkStale(coll, docs);
        } else {
          slot.what = `stale count ${coll} on member ${m.id}`;
          const n = await cl.countDocuments(regexFor(pat++), stale);
          if (n < 0) note(`${coll}: countDocuments answered ${n}`);
        }
        slot.done++;
        stats.reads++;
        stats.regexReads++;
        /*
         * MOVE ABOUT. Readers start spread evenly, but a member that has
         * just been restarted has none of them pointed at it until one
         * happens to fail over -- and it is precisely the restarted member
         * that is about to be installed into. Rotating occasionally keeps
         * every member under read load within a second of coming back.
         */
        if (rand() < 0.02) {
          await c.close().catch(() => {});
          c = null;
          m = MEMBERS[(rand() * MEMBERS.length) | 0];
        }
      } catch (err) {
        if (typeof err?.code !== 'number') {
          /*
           * The member was killed under this request. NOT `code ===
           * undefined`: a socket error carries a STRING code -- ECONNRESET
           * when the peer went away mid-answer, EPIPE, ECONNREFUSED -- and
           * treating only the undefined case as transport meant this soak
           * reported its own deliberate kills as violations. The wire's
           * refusals are the numeric ones; everything else is the socket.
           */
          await c.close().catch(() => {});
          c = null;
          stats.reconnects++;
        } else if (!expected(err)) {
          note(`reader on member ${m.id}: [${err?.code}] ${err.message}`);
        } else {
          refused(err);
        }
      }
      slot.what = null;
    }
    await c?.close().catch(() => {});
  })());

  /*
   * THE CHURN. Kill a member that is not leading, then bring it back --
   * half the time onto an empty directory, which cannot be caught up by
   * AppendEntries at all and so must be installed into. Never the leader:
   * killing that measures elections, and a leader receives no installs.
   */
  const churning = (async () => {
    while (!stop()) {
      await new Promise((r) => setTimeout(r, opts.churnMs));
      if (stop()) break;
      const live = [];
      for (const m of MEMBERS) {
        if (!m.proc) continue;
        try {
          const c = await connectServer(m.port, { keepAliveMs: 0 });
          const role = (await c.ping()).role;
          await c.close();
          live.push({ m, role });
        } catch { /* not answering */ }
      }
      const victims = live.filter((x) => x.role !== 'leader');
      if (!victims.length || live.length < 3) continue;   // keep quorum
      const victim = victims[(rand() * victims.length) | 0].m;

      await bank(victim);
      victim.proc.kill();
      await new Promise((r) => victim.proc.once('exit', r));
      victim.proc = null;

      /*
       * Down just long enough for the leader to compact past it -- which at
       * --snapshot-entries 4 is a fraction of a second -- and no longer.
       * A member that has been away for ten seconds comes back nearly
       * empty, and stale reads against an empty member cost nothing and are
       * never still running when the install adopts. The interesting member
       * is the one that still holds thousands of documents AND needs an
       * install, which is the one that only just left.
       */
      await new Promise((r) => setTimeout(r, 250 + Math.floor(rand() * 350)));
      if (stop()) break;
      /*
       * Mostly NOT wiped, which is the opposite of what looks aggressive.
       * A wiped member comes back empty, so every stale read against it is
       * refused -37 until the install lands -- 54% of all read attempts, in
       * the run that taught this -- and an empty member has no view worth
       * draining. A member that kept its files still needs an install (the
       * base moved past it) and still holds thousands of documents to be
       * reading when the install arrives. Some wipes stay, because a full
       * install from nothing is a different amount of work to adopt.
       */
      const wipe = rand() < 0.25;
      if (wipe) stats.wipes++;
      await startMember(victim, wipe);
      stats.cycles++;
    }
  })();

  /*
   * THE WATCHDOG. Every loop above checks the deadline, so the run cannot
   * overrun -- unless a request is never answered, in which case its `await`
   * never returns and `Promise.all` below waits forever. That is not a
   * hypothetical: it is what happens, and a hang is the WORST way to report
   * it, because it looks like a slow machine.
   *
   * Fifteen seconds past the deadline every loop should have finished its
   * last request. Anything still waiting is named, with what it was waiting
   * for and how long, and the run fails as a violation rather than sitting
   * there. Also pings each member from a FRESH connection, because "the
   * server is wedged" and "one connection's answer was lost" look identical
   * from the client and are not the same bug at all.
   */
  const hung = new Promise((resolve) => {
    const t = setInterval(async () => {
      if (Date.now() < deadline + 15000) return;
      const stuck = stuckReport();
      if (!stuck.length) return;    // finishing normally; let Promise.all win
      clearInterval(t);
      note(`${stuck.length} loop(s) never got an answer:\n${stuck.join('\n')}`);
      for (const m of MEMBERS) {
        if (!m.proc) { console.error(`    member ${m.id}: not running`); continue; }
        try {
          const c = await connectServer(m.port, { keepAliveMs: 0 });
          const t0 = Date.now();
          const p = await c.ping();
          await c.close();
          console.error(`    member ${m.id}: a fresh connection pinged in` +
            ` ${Date.now() - t0}ms -- role ${p.role}, applied ${p.applied},` +
            ` commit ${p.commit}, moved ${p.movedReads}, drains ${p.drainWaits},` +
            ` installDrains ${p.installDrains}`);
        } catch (e) {
          console.error(`    member ${m.id}: would not ping: ${e.message}`);
        }
      }
      resolve('hung');
    }, 1000);
    t.unref?.();
  });

  await Promise.race([Promise.all([writing, ...reading, churning]), hung]);
  if (heartbeat) clearInterval(heartbeat);
  for (const m of MEMBERS) if (m.proc) await bank(m);

  const total = (k) => MEMBERS.reduce((n, m) => n + m.banked[k], 0);
  const installs = MEMBERS.reduce((n, m) => n + m.installs, 0);

  for (const m of MEMBERS) {
    m.proc?.kill();
    if (m.dir) fs.rmSync(m.dir, { recursive: true, force: true });
  }

  say(`done: ${stats.writes} writes, ${stats.reads} reads` +
      ` (${stats.regexReads} compiling a fresh $regex), ${stats.compacts} compacts,` +
      ` ${stats.indexes} index ops, ${stats.drops} drops`);
  say(`      ${stats.cycles} churn cycles (${stats.wipes} onto an empty directory),` +
      ` ${installs} installs adopted, ${stats.reconnects} reconnects`);
  say(`      correct refusals by code: ${refusalLine() || 'none'}`);
  say(`      ${total('movedReads')} reads moved to a worker,` +
      ` ${total('drainWaits')} drains (${total('drainedReads')} reads),` +
      ` of which ${total('installDrains')} were installs` +
      ` (${total('installDrainedReads')} reads)`);

  /*
   * VACUOUS PASSES ARE FAILURES. Each of these is a way for the run to have
   * been green without having tested what it is named for, and each has
   * happened at least once while this file was being written.
   */
  if (!installs) {
    note('no install was adopted -- the churn never got a member far enough behind');
  }
  if (opts.readThreads > 0 && !total('movedReads')) {
    note('no read reached a worker thread -- the stale reads were answered inline');
  }
  if (opts.readThreads > 0 && !total('installDrains')) {
    note('no install ever waited for a reader -- the barrier under test was never reached');
  }
  if (!stats.regexReads) {
    note('no read compiled a $regex -- the compile path went untested');
  }

  if (violations.length) {
    console.error(`\n${violations.length} violation(s) -- replay with --seed ${opts.seed}`);
    process.exit(1);
  }
  say('clean');
  /* The loops that were still awaiting an answer hold the event loop open,
   * so a hung run has to be told to stop rather than be waited on. */
  process.exit(0);
};

main().catch((err) => {
  console.error(err);
  for (const m of MEMBERS) {
    m.proc?.kill();
    if (m.dir) fs.rmSync(m.dir, { recursive: true, force: true });
  }
  process.exit(1);
});
