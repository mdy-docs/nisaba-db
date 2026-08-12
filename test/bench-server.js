/**
 * test/bench-server.js — throughput against a REAL server, across
 * sockets and callers.
 *
 *   node test/bench-server.js                 # solo member
 *   node test/bench-server.js --cluster 3     # a 3-member raft cluster
 *   node test/bench-server.js --seconds 8
 *
 * test/bench.js measures the engine in this process, one caller at a
 * time. It says nothing about the two axes that decide what a deployment
 * actually gets: how many CONNECTIONS the work arrives on, and how many
 * callers are outstanding on each. Those are the axes anything that
 * moves work off the serving thread will be judged on, so they need a
 * number before and after rather than an argument.
 *
 * WHAT THE AXES MEAN, because they are not interchangeable.
 * `server/main.c` allows ONE DEFERRED ANSWER per connection: `conn_serve`
 * breaks on `if (c->owed) break;` and the pollset stops asking for
 * POLLIN until that answer has gone out. A request answered INLINE never
 * sets `owed`, so inline work pipelines freely on one socket while
 * deferred work does not. On a replicated member:
 *
 *   - a WRITE always defers (it waits for its entry to commit), so one
 *     socket carries one write at a time;
 *   - a READ defers only while its quorum barrier is unproven, and
 *     concurrent barriers share a round -- so a pipeline of reads on one
 *     socket is usually answered inline and costs about one round for
 *     the batch rather than one each.
 *
 * That asymmetry is the whole reason this file reports the two
 * separately. The numbers gate nothing; they exist so a change can be
 * argued with evidence.
 *
 * THE LAST TWO AXES ARE ABOUT --read-threads, and they are two questions
 * rather than one. SCAN INTERFERENCE is what a single client's scan does
 * to everybody else's reads -- the measurement the whole reader-thread
 * milestone exists for. SCAN SCALING is whether more workers get more
 * scanning done, which is the only thing that justified N workers over
 * one. Each runs its own solo server per --read-threads value, because
 * the flag is per-process, and on a bigger collection than the axes
 * above, because a scan of 2,000 documents is a fraction of a
 * millisecond (--scandocs, default 50000).
 *
 * Read them together, because on a SHORT scan they trade against each
 * other. At 8,000 documents on six cores, going from 0 workers to 1 took
 * eight sockets of point reads from 26% of their idle rate to 102% -- and
 * took the scanner itself from 609 scans/s to 462: it gives up a quarter
 * of its own throughput to stop taking three quarters of everybody
 * else's, because the serving thread it was starving is now busy
 * answering them. At 50,000 -- the size the isolation problem was
 * measured at -- there is no trade to make: 4% held becomes 100-104%,
 * and the scanner stays at 87 scans/s either way.
 */
import { spawn, spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer } from '../src/db-server-client.js';

const BIN = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const DB = 'bench';

const opts = { seconds: 4, cluster: 1, docs: 2000, port: 38000 + (process.pid % 900),
               scandocs: 50000 };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in opts)) {
    console.error('usage: node test/bench-server.js [--seconds N] [--cluster 1|3]' +
                  ' [--docs N] [--port N] [--scandocs N]');
    process.exit(2);
  }
  opts[key] = Number(process.argv[++i]);
}

const dirs = [], procs = [];
const cleanup = () => {
  for (const p of procs) p.kill();
  for (const d of dirs) fs.rmSync(d, { recursive: true, force: true });
};

function start(args) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-bench-'));
  dirs.push(dir);
  const p = spawn(path.resolve(BIN), args, { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });
  procs.push(p);
  return new Promise((res, rej) => {
    const t = setTimeout(() => rej(new Error('server did not start')), 20000);
    p.stderr.on('data', (d) => { if (String(d).includes('serving')) { clearTimeout(t); res(p); } });
  });
}

/*
 * A solo server of its own, run and then thrown away. The scan axes below
 * need one per --read-threads value: the flag is per-process, and the point
 * of both is to compare a server that has workers against one that does
 * not, on this machine, while it is as loaded as the other run was.
 */
async function withServer(port, extra, body) {
  const before = { d: dirs.length, p: procs.length };
  await start(['--port', String(port), '--max-clients', '64', '--raft', '1', ...extra]);
  const proc = procs[procs.length - 1];
  try {
    return await body(port, proc);
  } finally {
    proc.kill();
    procs.splice(before.p, 1);
    for (const d of dirs.splice(before.d)) fs.rmSync(d, { recursive: true, force: true });
  }
}

/** Resident KB, which is what a deployment is billed for. */
function rssKb(pid) {
  const out = spawnSync('ps', ['-o', 'rss=', '-p', String(pid)], { encoding: 'utf8' });
  const kb = Number(String(out.stdout).trim());
  return Number.isFinite(kb) && kb > 0 ? kb : 0;
}

const main = async () => {
  if (!fs.existsSync(BIN)) {
    console.error(`no server at ${BIN} -- ./build/build-server.sh --native`);
    process.exit(2);
  }

  const members = Array.from({ length: opts.cluster }, (_, i) => ({
    id: i + 1, port: opts.port + i, raft: opts.port + 50 + i
  }));
  for (const m of members) {
    const args = ['--port', String(m.port), '--max-clients', '64'];
    if (opts.cluster > 1) {
      args.push('--raft', String(m.id), '--raft-port', String(m.raft),
        /* Wider than the LAN default: a benchmark saturates the machine,
         * and at 150:300 the cluster elects mid-run -- which measures
         * the election rather than the workload. */
        '--election-timeout', '900:1500', '--heartbeat', '300');
      for (const o of members) {
        if (o.id !== m.id) args.push('--peer', `${o.id}@127.0.0.1:${o.raft}`);
      }
    } else {
      args.push('--raft', '1');
    }
    await start(args);
  }

  /** Whichever member takes a write; on a cluster, the leader. */
  const findLeader = async () => {
    for (let i = 0; i < 200; i++) {
      for (const m of members) {
        let c = null;
        try {
          c = await connectServer(m.port);
          if (opts.cluster === 1 || (await c.ping()).role === 'leader') return m.port;
        } catch { /* not up yet */ }
        finally { await c?.close().catch(() => {}); }
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    throw new Error('no member would lead');
  };
  const port = await findLeader();

  // Seed, and keep the ids so reads are point lookups rather than scans.
  const ids = [];
  {
    const c = await connectServer(port);
    const coll = c.db(DB).collection('items');
    for (let n = 0; n < opts.docs; n += 100) {
      const r = await coll.insertMany(Array.from({ length: Math.min(100, opts.docs - n) },
        (_, k) => ({ n: n + k, pad: 'x'.repeat(80) })));
      ids.push(...Object.values(r.insertedIds));
    }
    await c.close();
  }
  const pick = () => ids[(Math.random() * ids.length) | 0];

  console.log(`nisaba-server bench  ${opts.cluster === 1 ? 'solo member' : `${opts.cluster}-member cluster`}` +
              `  docs=${ids.length}  ${opts.seconds}s per point  ${BIN}\n`);

  /** `callers` outstanding requests spread over `sockets` connections. */
  async function drive(sockets, callers, ms, op) {
    const cs = await Promise.all(Array.from({ length: sockets }, () => connectServer(port)));
    /* One collection for every socket, not one each: a session holds at
     * most DBS_MAX_COLLECTIONS (32) open, and sharing is the realistic
     * shape anyway -- a tenant's writers contend on the same tree. */
    const colls = cs.map((c) => c.db(DB).collection(op === 'write' ? 'writes' : 'items'));
    const stop = Date.now() + ms;
    let n = 0, refused = 0;
    await Promise.all(Array.from({ length: callers }, async (_, k) => {
      const coll = colls[k % sockets];
      while (Date.now() < stop) {
        try {
          if (op === 'write') await coll.insertOne({ k, n });
          else await coll.findOne({ _id: pick() });
          n++;
        } catch (err) {
          // An election mid-run refuses; counted and reported, not hidden.
          if (err?.code === -63 || err?.code === -64 || err?.code === -66) refused++;
          else throw err;
        }
      }
    }));
    await Promise.all(cs.map((c) => c.close().catch(() => {})));
    return { n, refused };
  }

  async function row(label, sockets, callers, op) {
    await drive(sockets, callers, 400, op);      // warm
    const t0 = Date.now();
    const { n, refused } = await drive(sockets, callers, opts.seconds * 1000, op);
    const rate = Math.round(n / ((Date.now() - t0) / 1000));
    console.log(`  ${label.padEnd(34)} ${String(rate).padStart(7)} ${op}s/s` +
                (refused ? `   (${refused} refused)` : ''));
    return rate;
  }

  console.log('READS -- 32 callers, spread over N sockets');
  for (const s of [1, 2, 8, 32]) await row(`${s} socket(s)`, s, 32, 'read');

  console.log('\nREADS -- one socket, N callers pipelined on it');
  for (const c of [1, 4, 16, 64]) await row(`${c} caller(s)`, 1, c, 'read');

  console.log('\nWRITES -- 32 callers, spread over N sockets');
  for (const s of [1, 2, 8, 32]) await row(`${s} socket(s)`, s, 32, 'write');

  console.log('\nWRITES -- one socket, N callers pipelined on it');
  for (const c of [1, 4, 16, 64]) await row(`${c} caller(s)`, 1, c, 'write');

  /*
   * ---- scans, and what they cost everybody else -------------------------
   *
   * The two axes --read-threads is judged on, and they are different
   * questions. INTERFERENCE is what one client's scan does to every other
   * client's latency, which is the measurement the whole milestone exists
   * for. SCALING is whether more workers get more scanning done, which is
   * the only thing that justified N workers over one.
   *
   * Each needs its own server, because the flag is per-process -- and its
   * own collection size, because a scan of 2,000 documents is a fraction
   * of a millisecond and neither axis has anything to measure on it.
   * --read-offload-min 0 so the routing floor never decides for us.
   */
  const cpus = os.availableParallelism?.() ?? os.cpus().length;
  const spare = cpus > 2 ? cpus - 2 : 1;
  const WORKERS = [0, 1, 2, 4, 8].filter((n) => n <= spare);

  /** `scanners` connections scanning `coll` flat out for `ms`. */
  async function scanFor(port, coll, scanners, ms) {
    const cs = await Promise.all(Array.from({ length: scanners }, () => connectServer(port)));
    const stop = Date.now() + ms;
    let n = 0;
    await Promise.all(cs.map(async (c) => {
      const cl = c.db(DB).collection(coll);
      // Matches nothing, so every scan walks the whole collection.
      while (Date.now() < stop) { await cl.countDocuments({ pad: 'zzzzzz' }); n++; }
    }));
    const moved = (await cs[0].ping()).movedReads;
    await Promise.all(cs.map((c) => c.close().catch(() => {})));
    return { n, moved };
  }

  /** Seeds `scandocs` documents big enough that a scan is milliseconds. */
  async function seedScan(port) {
    const c = await connectServer(port);
    const coll = c.db(DB).collection('scan');
    const ids = [];
    for (let n = 0; n < opts.scandocs; n += 200) {
      const r = await coll.insertMany(Array.from({ length: Math.min(200, opts.scandocs - n) },
        (_, k) => ({ n: n + k, pad: 'x'.repeat(80) })));
      ids.push(...Object.values(r.insertedIds));
    }
    await c.close();
    return ids;
  }

  console.log(`\nSCAN INTERFERENCE -- 8 sockets of _id reads, with and without` +
              ` one client scanning ${opts.scandocs} docs`);
  console.log('  workers      idle reads/s     while scanning        held');
  for (const w of WORKERS) {
    await withServer(opts.port + 20 + w, ['--read-threads', String(w),
                                          '--read-offload-min', '0'], async (port) => {
      const ids = await seedScan(port);
      const pickScan = () => ids[(Math.random() * ids.length) | 0];
      const points = async (ms) => {
        const cs = await Promise.all(Array.from({ length: 8 }, () => connectServer(port)));
        const stop = Date.now() + ms;
        let n = 0;
        await Promise.all(cs.map(async (c) => {
          const cl = c.db(DB).collection('scan');
          while (Date.now() < stop) { await cl.findOne({ _id: pickScan() }); n++; }
        }));
        await Promise.all(cs.map((c) => c.close().catch(() => {})));
        return n;
      };
      await points(400);                                       // warm
      const idle = await points(opts.seconds * 1000) / opts.seconds;

      let go = true;
      const scanner = await connectServer(port);
      const scanning = (async () => {
        const cl = scanner.db(DB).collection('scan');
        let did = 0;
        while (go) { await cl.countDocuments({ pad: 'zzzzzz' }); did++; }
        return did;
      })();
      const busy = await points(opts.seconds * 1000) / opts.seconds;
      go = false;
      const scans = await scanning;
      await scanner.close();

      console.log(`  ${String(w).padStart(7)} ${idle.toFixed(0).padStart(17)}` +
                  ` ${busy.toFixed(0).padStart(18)}` +
                  ` ${((busy / idle) * 100).toFixed(0).padStart(10)}%` +
                  `   (${(scans / opts.seconds).toFixed(1)} scans/s)`);
    });
  }

  /*
   * WRITE INTERFERENCE -- the axis the reader-thread milestone left open.
   * With scans offloaded, the remaining long thing on the serving thread is
   * a long WRITE: one `updateMany` over the whole collection, or the
   * backfill inside `createIndex`. Both hold the loop for their whole
   * duration, and a write cannot be moved to a read view, so this is the
   * number any design for them has to beat.
   *
   * Reader threads are held at `auto` and at 0, because they should make no
   * difference here: a point read is inline either way, so it waits behind
   * the write on the loop thread whatever the pool is doing.
   */
  console.log(`\nWRITE INTERFERENCE -- 8 sockets of _id reads, with and without` +
              ` one client writing over ${opts.scandocs} docs`);
  console.log('  workers  what              idle reads/s   while writing      held      the write');
  for (const w of [0, -1]) {
    const flag = w < 0 ? ['--read-threads', 'auto'] : ['--read-threads', '0'];
    await withServer(opts.port + 60 + (w < 0 ? 9 : 0), flag, async (port) => {
      const ids = await seedScan(port);
      const pick = () => ids[(Math.random() * ids.length) | 0];
      const points = async (ms) => {
        const cs = await Promise.all(Array.from({ length: 8 }, () => connectServer(port)));
        const stop = Date.now() + ms;
        let n = 0;
        await Promise.all(cs.map(async (c) => {
          const cl = c.db(DB).collection('scan');
          while (Date.now() < stop) { await cl.findOne({ _id: pick() }); n++; }
        }));
        await Promise.all(cs.map((c) => c.close().catch(() => {})));
        return n;
      };
      await points(400);
      const idle = await points(opts.seconds * 1000) / opts.seconds;

      /*
       * Two long writes, each measured over the same window: `updateMany`
       * touches every document, `createIndex` reads every document and
       * writes an index file (and is dropped again so the next round has
       * work to do). Reads run throughout, and the window is timed rather
       * than assumed -- a write that is REFUSED returns instantly, and
       * dividing by the intended seconds would report the readers as
       * blocked when they simply had nothing to be blocked by.
       */
      const WRITES = [
        ['updateMany', async (cl, round) => {
          await cl.updateMany({}, { $set: { touched: round } });
        }],
        ['createIndex', async (cl) => {
          const name = await cl.createIndex({ n: 1 });
          await cl.dropIndex(name);
        }]
      ];
      for (const [what, run] of WRITES) {
        const writer = await connectServer(port);
        const cl = writer.db(DB).collection('scan');
        const cs = await Promise.all(Array.from({ length: 8 }, () => connectServer(port)));
        let n = 0, go = true;
        const t0 = Date.now();
        const reading = Promise.all(cs.map(async (c) => {
          const rc = c.db(DB).collection('scan');
          while (go) { await rc.findOne({ _id: pick() }); n++; }
        }));
        const stop = t0 + opts.seconds * 1000;
        let rounds = 0, refused = null, wrote = 0;
        while (Date.now() < stop) {
          const w0 = Date.now();
          try { await run(cl, rounds); } catch (err) {
            /* "This op does not work at this size" IS the measurement, and
             * the window keeps running so the reads stay comparable. */
            refused ??= err.code ?? err.message;
            await new Promise((r) => setTimeout(r, 50));
            continue;
          }
          wrote += Date.now() - w0;
          rounds++;
        }
        go = false;
        await reading;
        const secs = (Date.now() - t0) / 1000;
        await Promise.all(cs.map((c) => c.close().catch(() => {})));
        await writer.close();
        const busy = n / secs;
        console.log(`  ${String(w < 0 ? 'auto' : w).padStart(7)}  ${what.padEnd(14)}` +
                    ` ${idle.toFixed(0).padStart(13)} ${busy.toFixed(0).padStart(15)}` +
                    ` ${((busy / idle) * 100).toFixed(0).padStart(8)}%` +
                    `   ${refused !== null
                          ? `REFUSED (${refused})${rounds ? ` after ${rounds}` : ''}`
                          : `${(wrote / Math.max(rounds, 1)).toFixed(0)}ms x${rounds}`}`);
      }
    });
  }

  console.log(`\nSCAN SCALING -- N sockets all scanning ${opts.scandocs} docs at once`);
  const SCANNERS = [1, 2, 4, 8];
  console.log('  workers ' + SCANNERS.map((s) => `${String(s).padStart(8)} scan`).join('') +
              '      rssMB');
  for (const w of WORKERS) {
    await withServer(opts.port + 40 + w, ['--read-threads', String(w),
                                          '--read-offload-min', '0'], async (port, proc) => {
      await seedScan(port);
      let line = `  ${String(w).padStart(7)} `, peak = 0;
      const sample = setInterval(() => { peak = Math.max(peak, rssKb(proc.pid)); }, 100);
      for (const s of SCANNERS) {
        await scanFor(port, 'scan', s, 400);                   // warm
        const t0 = Date.now();
        const { n, moved } = await scanFor(port, 'scan', s, opts.seconds * 1000);
        line += (n / ((Date.now() - t0) / 1000)).toFixed(1).padStart(13);
        /* A worker count that moved nothing is a run whose number means
         * something else entirely, so it is marked rather than averaged in. */
        if (w > 0 && !moved) line += '!';
      }
      clearInterval(sample);
      console.log(line + `   ${(peak / 1024).toFixed(1)}`);
    });
  }
  if (WORKERS.length < 5) {
    console.log(`  (${cpus} cpus, so --read-threads above ${spare} is lowered` +
                ' and was not run)');
  }
};

main().then(cleanup, (err) => { console.error(err); cleanup(); process.exit(1); });
