/**
 * The smallest proof the assembled package works. Lives beside the
 * package (see client.build.mjs's header) and runs against the
 * assembled files inside it, after `npm run build`.
 *
 * Two layers, because the client's one dependency-free claim and its
 * whole purpose need different witnesses:
 *
 *  - always: the closure is complete — the client imports, and the codec
 *    it shipped round-trips the values the wire is made of (ObjectId,
 *    dates, binary) without reaching outside the package.
 *  - when wasm/lib/nisaba-server has been built: a LIVE round-trip —
 *    spawn the server on a temp root, insert, find, close. Skipped
 *    otherwise, the same way the repo's own server tests skip: `npm
 *    test` does not build the server.
 */
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');

const {
  connectServer, parseAddress, WIRE_OPS, ServerError,
  encode, decode, ObjectId
} = await import('./client/src/db-server-client.js');

/* The closure, without a socket. */
if (!WIRE_OPS.includes('find') || !WIRE_OPS.includes('bulkWrite')) {
  throw new Error('WIRE_OPS is not the wire\'s op list');
}
const addr = parseAddress('127.0.0.1:8097');
if (addr.host !== '127.0.0.1' || addr.port !== 8097) throw new Error('parseAddress failed');
const id = new ObjectId();
const back = decode(encode({ _id: id, when: new Date(0), n: 1 }));
if (String(back._id) !== String(id) || back.n !== 1) throw new Error('codec round-trip failed');
if (!(new ServerError(-41, 'x') instanceof Error)) throw new Error('ServerError is not an Error');
console.log('smoke ok: the closure imports and the codec round-trips');

/* The live half, against a built server. */
const NATIVE = path.join(repo, 'wasm', 'lib', 'nisaba-server');
if (!fs.existsSync(NATIVE)) {
  console.log('smoke skipped the live round-trip: wasm/lib/nisaba-server is not built ' +
              '(./wasm/build-server.sh --native)');
  process.exit(0);
}

const port = await new Promise((resolve) => {
  const s = net.createServer();
  s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
});
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-client-pkg-'));
const proc = spawn(NATIVE, ['--port', String(port)], { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });

try {
  let client = null;
  for (let i = 0; i < 40 && !client; i++) {
    client = await connectServer(`127.0.0.1:${port}`).catch(() => null);
    if (!client) await new Promise((r) => setTimeout(r, 250));
  }
  if (!client) throw new Error('the server never started listening');
  const users = client.db('smoke').collection('users');
  await users.insertOne({ name: 'Ada', n: 1 });
  if ((await users.findOne({ name: 'Ada' })).n !== 1) throw new Error('live round-trip failed');
  await client.close();
  console.log('smoke ok: a live round-trip over the socket');
} finally {
  proc.kill();
  fs.rmSync(dir, { recursive: true, force: true });
}
