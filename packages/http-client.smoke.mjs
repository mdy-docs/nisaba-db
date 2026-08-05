/**
 * The smallest proof the assembled http-client package works.
 * Lives beside the package (see http-client.build.mjs's header)
 * and runs against the assembled files inside it, after `npm run build`.
 *
 * Two layers, the same split every package smoke here makes:
 *
 *  - always: the closure is complete and PLATFORM-CLEAN — the client
 *    imports from the package alone, no file in the closure names a
 *    node: module (the browser claim, checked rather than asserted),
 *    addresses normalize, and Extended JSON round-trips the wire's
 *    types through the package's own copy.
 *  - when wasm/lib/nisaba-server has been built: a LIVE round trip —
 *    server, front end (from the repo; it is the thing this client
 *    talks to, not part of it), and the packaged client doing insert,
 *    find and a change stream. Skipped otherwise, the same way the
 *    repo's own server tests skip.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');
const pkg = path.join(here, 'http-client');

const { connectHttp, parseBaseUrl, ObjectId, ServerError, WIRE_OPS } =
  await import('./http-client/src/db-http-client.js');
const { fromExtendedJson, toExtendedJson } =
  await import('./http-client/src/extended-json.js');

/* The closure, without a network. */
for (const rel of ['src/db-http-client.js', 'src/extended-json.js', 'third_party/binjson/js/binjson.js']) {
  const text = fs.readFileSync(path.join(pkg, rel), 'utf8');
  if (/from\s+['"]node:/.test(text)) {
    throw new Error(`${rel} imports a node: module -- the browser claim is broken`);
  }
}
if (parseBaseUrl('127.0.0.1:8080') !== 'http://127.0.0.1:8080') throw new Error('parseBaseUrl failed');
if (!WIRE_OPS.includes('find') || !WIRE_OPS.includes('watch')) throw new Error('WIRE_OPS is not the wire\'s op list');
if (!(new ServerError(-41, 'x') instanceof Error)) throw new Error('ServerError is not an Error');
const id = new ObjectId();
const across = fromExtendedJson(JSON.parse(JSON.stringify(toExtendedJson({
  _id: id, when: new Date(0), bytes: new Uint8Array([1, 2, 255])
}))));
if (String(across._id) !== String(id) || across.when.getTime() !== 0 || across.bytes[2] !== 255) {
  throw new Error('Extended JSON did not round-trip the wire types');
}
console.log('smoke ok: the closure imports, is node:-free, and round-trips the wire types');

/* The live half, against a built server with the repo's front end in front. */
const NATIVE = path.join(repo, 'wasm', 'lib', 'nisaba-server');
if (!fs.existsSync(NATIVE)) {
  console.log('smoke skipped the live round-trip: wasm/lib/nisaba-server is not built ' +
              '(./wasm/build-server.sh --native)');
  process.exit(0);
}
const { DbHttpFront } = await import(path.join(repo, 'src', 'db-http-front.js'));

const port = 48000 + (process.pid % 900);
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-client-http-pkg-'));
const proc = spawn(NATIVE, ['--port', String(port)], { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });

try {
  await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error('the server never started listening')), 30000);
    proc.stderr.on('data', (d) => {
      if (String(d).includes('serving')) { clearTimeout(t); resolve(); }
    });
  });
  const front = new DbHttpFront(`127.0.0.1:${port}`, { listenPort: 0 });
  await front.start();

  const client = await connectHttp(`http://127.0.0.1:${front.address().port}`);
  const users = client.db('smoke').collection('users');
  const stream = users.watch();
  await stream.ready;
  const { insertedId } = await users.insertOne({ name: 'Ada', n: 1 });
  if (!(insertedId instanceof ObjectId)) throw new Error('insertOne minted no ObjectId');
  if ((await users.findOne({ name: 'Ada' })).n !== 1) throw new Error('live round-trip failed');
  const { value: change } = await stream.next();
  if (change.fullDocument?.name !== 'Ada') throw new Error('the change stream missed the insert');
  await stream.close();
  await client.close();
  await front.stop();
  console.log('smoke ok: a live round-trip, change stream included');
} finally {
  proc.kill();
  fs.rmSync(dir, { recursive: true, force: true });
}
