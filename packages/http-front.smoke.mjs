/**
 * The smallest proof the assembled http-front package works.
 * Lives beside the package (see http-front.build.mjs's header)
 * and runs against the assembled files inside it, after `npm run build`.
 *
 * Two layers, the same split every package smoke here makes:
 *
 *  - always: the closure is complete — the front end imports from the
 *    package alone, constructs, starts, and serves its banner. No
 *    server is needed for any of that: the front end dials members per
 *    request, which is itself part of the claim.
 *  - when wasm/lib/nisaba-server has been built: a LIVE round trip —
 *    spawn the server, put the front in front of it, insert and find
 *    over plain HTTP. Skipped otherwise, the same way the repo's own
 *    server tests skip.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');

const { DbHttpFront } = await import('./http-front/src/db-http-front.js');

/* The closure, without a server. */
const front = new DbHttpFront('127.0.0.1:1', { listenPort: 0 });
await front.start();
const { port: httpPort } = front.address();
const banner = await (await fetch(`http://127.0.0.1:${httpPort}/`)).text();
if (!banner.includes('nisaba HTTP front end')) throw new Error('the front end did not serve its banner');
const bad = await fetch(`http://127.0.0.1:${httpPort}/no/such/route/here/at/all`, { method: 'POST' });
if (bad.status !== 404) throw new Error('the URL grammar did not refuse a non-route');
await front.stop();
console.log('smoke ok: the closure imports, starts, and speaks its grammar');

/* The live half, against a built server. */
const NATIVE = path.join(repo, 'wasm', 'lib', 'nisaba-server');
if (!fs.existsSync(NATIVE)) {
  console.log('smoke skipped the live round-trip: wasm/lib/nisaba-server is not built ' +
              '(./wasm/build-server.sh --native)');
  process.exit(0);
}

const port = 47000 + (process.pid % 900);
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-http-front-pkg-'));
const proc = spawn(NATIVE, ['--port', String(port)], { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });

try {
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the server never started listening')), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve(); }
    });
  });
  const live = new DbHttpFront(`127.0.0.1:${port}`, { listenPort: 0 });
  await live.start();
  const base = `http://127.0.0.1:${live.address().port}`;
  const ins = await (await fetch(`${base}/db/smoke/users/insert`, {
    method: 'POST', body: JSON.stringify({ doc: { name: 'Ada', n: 1 } })
  })).json();
  if (!ins.ok || !ins.minted?.id?.$oid) throw new Error('insert over HTTP failed or minted no id');
  const found = await (await fetch(`${base}/db/smoke/users/findOne`, {
    method: 'POST', body: JSON.stringify({ filter: { name: 'Ada' } })
  })).json();
  if (found.doc?.n !== 1) throw new Error('findOne over HTTP failed');
  await live.stop();
  console.log('smoke ok: a live round-trip over HTTP to a real server');
} finally {
  proc.kill();
  fs.rmSync(dir, { recursive: true, force: true });
}
