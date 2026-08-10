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
 */
import { spawn } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { connectServer } from '../src/db-server-client.js';

const BIN = process.env.NISABA_SERVER_BIN || 'build/lib/nisaba-server';
const DB = 'bench';

const opts = { seconds: 4, cluster: 1, docs: 2000, port: 38000 + (process.pid % 900) };
for (let i = 2; i < process.argv.length; i++) {
  const key = process.argv[i].replace(/^--/, '');
  if (!(key in opts)) {
    console.error('usage: node test/bench-server.js [--seconds N] [--cluster 1|3] [--docs N] [--port N]');
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
};

main().then(cleanup, (err) => { console.error(err); cleanup(); process.exit(1); });
