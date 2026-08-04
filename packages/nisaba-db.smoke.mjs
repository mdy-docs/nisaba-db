/**
 * The smallest proof the assembled package works: the wasm loads, a
 * database opens over the in-memory provider, and a round-trip lands.
 * Lives beside the package (see nisaba-db.build.mjs's header) and runs
 * against the assembled files inside it, after `npm run build`. The
 * Node provider is exercised too, against a temp directory, because
 * `./node` is the one entry with platform-specific imports.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const { ready } = await import('./nisaba-db/wasm/nisaba-wasm.js');
await ready();

const { connect, MemoryStorageProvider } = await import('./nisaba-db/src/db.js');
const db = await connect(new MemoryStorageProvider());
const users = await db.collection('users');
await users.insertOne({ name: 'Ada', n: 1 });
if ((await users.findOne({ name: 'Ada' })).n !== 1) throw new Error('memory round-trip failed');
await db.close();

const { connect: connectViaNode, NodeFSStorageProvider } = await import('./nisaba-db/src/db-node.js');
const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-pkg-'));
const ndb = await connectViaNode(new NodeFSStorageProvider(dir));
const things = await ndb.collection('things');
await things.insertOne({ n: 42 });
if ((await things.findOne({ n: 42 })) === null) throw new Error('node round-trip failed');
await ndb.close();
fs.rmSync(dir, { recursive: true, force: true });

console.log('smoke ok: memory and node providers both round-trip');
