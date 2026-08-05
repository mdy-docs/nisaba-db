#!/usr/bin/env node
/**
 * smoke-server.mjs — boot a built nisaba-server artifact and prove it
 * over the wire: serve, connect, insert, read back, count, shut down.
 *
 * The release workflow's per-platform gate (.github/workflows/
 * release.yml): the C harness proves the engine, but only this proves
 * the ARTIFACT — the thing a user downloads — accepts a socket on the
 * OS it was built for. Runs anywhere Node does, Windows included; the
 * client it connects with is src/db-server-client.js, pure JS, no
 * install step.
 *
 *   node build/smoke-server.mjs <server-binary> [prefix args...]
 *   node build/smoke-server.mjs --wasmtime <wasmtime> <server.wasm>
 *
 * The first form appends `--port <ephemeral>` to whatever command it
 * is given and runs it in a fresh temp directory — so
 * `smoke-server.mjs arch -x86_64 /path/nisaba-server` exercises the
 * Intel slice of a universal binary under Rosetta, unchanged. The
 * second form supplies the invocation shape the wasip2 artifact needs
 * (`run -S inherit-network --dir <tmp>::.`), because the temp
 * directory is this script's to create and the --dir mapping has to
 * name it.
 */
import fs from 'node:fs';
import net from 'node:net';
import os from 'node:os';
import path from 'node:path';
import { spawn } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const { connectServer } = await import(
  pathToFileURL(path.join(repo, 'src', 'db-server-client.js')));

const argv = process.argv.slice(2);
if (argv.length === 0 || (argv[0] === '--wasmtime' && argv.length !== 3)) {
  console.error('usage: smoke-server.mjs <server-binary> [prefix args...]\n' +
                '       smoke-server.mjs --wasmtime <wasmtime> <server.wasm>');
  process.exit(2);
}

const port = await new Promise((resolve) => {
  const s = net.createServer();
  s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => resolve(p)); });
});
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-smoke-'));

/* The child runs in the temp directory, so anything that names a file
 * relative to HERE must become absolute before the cwd changes under
 * it. Only things that exist are touched: `arch` stays a bare word for
 * the PATH to find. */
const absolute = (a) => (fs.existsSync(a) ? path.resolve(a) : a);

let cmd, args;
if (argv[0] === '--wasmtime') {
  cmd = absolute(argv[1]);
  args = ['run', '-S', 'inherit-network', '--dir', `${dir}::.`,
          absolute(argv[2]), '--port', String(port)];
} else {
  [cmd, ...args] = argv.map(absolute);
  args.push('--port', String(port));
}

console.log(`smoke: ${cmd} ${args.join(' ')}`);
const proc = spawn(cmd, args, { cwd: dir, stdio: ['ignore', 'pipe', 'pipe'] });

/* Everything the server says is worth having in the log of a failed
 * run; "serving" within it is the boot gate, the same word every test
 * and script here waits for. */
let said = '';
const serving = new Promise((resolve, reject) => {
  const t = setTimeout(() => reject(new Error('the server never said "serving"')), 60000);
  proc.stderr.on('data', (d) => {
    said += String(d);
    process.stderr.write(d);
    if (said.includes('serving')) { clearTimeout(t); resolve(); }
  });
  proc.on('exit', (code) => reject(new Error(`the server exited (${code}) before serving`)));
  proc.on('error', reject);
});

let failed = null;
try {
  await serving;

  let client = null;
  for (let i = 0; i < 40 && !client; i++) {
    client = await connectServer(`127.0.0.1:${port}`).catch(() => null);
    if (!client) await new Promise((r) => setTimeout(r, 250));
  }
  if (!client) throw new Error('the server is serving but never accepted a connection');

  const users = client.db('smoke').collection('users');
  const { insertedId } = await users.insertOne({ name: 'Ada', n: 1 });
  const back = await users.findOne({ name: 'Ada' });
  if (!back || back.n !== 1 || String(back._id) !== String(insertedId)) {
    throw new Error('the round trip lost the document');
  }
  if (await users.countDocuments({}) !== 1) throw new Error('countDocuments disagrees');
  await client.close();
  console.log('smoke ok: served, connected, wrote, read back');
} catch (err) {
  failed = err;
} finally {
  proc.kill();
  await new Promise((resolve) => {
    const t = setTimeout(() => { proc.kill('SIGKILL'); resolve(); }, 5000);
    proc.on('exit', () => { clearTimeout(t); resolve(); });
  });
  try { fs.rmSync(dir, { recursive: true, force: true }); } catch { /* windows holds locks briefly */ }
}

if (failed) {
  console.error(`smoke FAILED: ${failed.message}`);
  process.exit(1);
}
