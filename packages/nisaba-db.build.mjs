/**
 * Assemble the publishable nisaba-db package from the repository.
 *
 * Lives BESIDE the package, not in it: the package directory holds only
 * what ships (plus its package.json and README), and this script is the
 * monorepo's, reachable from the package's build/prepack scripts as
 * ../nisaba-db.build.mjs.
 *
 * Nothing in the package directory is a source of truth: every shipped
 * file is copied from the repo root at pack time (prepack runs this), so
 * the package can never drift from what the repository builds and tests.
 * The copied tree mirrors the repo's layout exactly, because the source
 * files import each other by relative path ('../src/nisaba-wasm.js',
 * '../third_party/binjson/js/binjson.js') and preserving the shape is
 * what lets them ship unmodified.
 *
 * The closure is the EMBEDDED database for both platforms -- one wasm
 * binary and one loader serve Node, the browser main thread, and
 * workers alike (the emscripten loader detects its environment); what
 * differs per platform is only the storage-provider entry point:
 *
 *   node      src/db-node.js         NodeFSStorageProvider (node:fs)
 *   browser   src/db.js              OPFSStorageProvider / MemoryStorageProvider
 *   multi-tab src/db-coordinator.js  connectShared, inside your worker
 *
 * The server, replication, and HTTP-front surfaces are deliberately not
 * here -- they are the repository package's; this one is the database
 * you embed.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');
const pkg = path.join(here, 'nisaba-db');

const CLOSURE = [
  // The core, and the module it re-exports from.
  'src/db.js',
  'src/db-remote.js',
  // Node's storage provider (imports node:fs; reached only via ./node).
  'src/db-node.js',
  // The browser's multi-tab coordination (runs inside a worker).
  'src/db-coordinator.js',
  // The engine wrapper and the engine.
  'src/nisaba-wasm.js',
  'build/lib/nisaba.wasm.mjs',
  'build/lib/nisaba.wasm',
  // The leaf modules the above import.
  'third_party/binjson/js/binjson.js',
  'third_party/binjson-structures/wasm/structures-core.js',
  // Types for each export.
  'types/nisaba.d.ts',
  'types/remote.d.ts',
  'types/node.d.ts',
  'types/coordinator.d.ts',
  'types/wasm.d.ts'
];

// The engine is a build artifact the repository does not commit; make it
// if it is not there, with the one script that owns how it is made.
if (!fs.existsSync(path.join(repo, 'build', 'lib', 'nisaba.wasm'))) {
  console.log('nisaba.wasm not built; running build/build-wasm.sh ...');
  execFileSync(path.join(repo, 'build', 'build-wasm.sh'), { cwd: repo, stdio: 'inherit' });
}

for (const rel of CLOSURE) {
  const from = path.join(repo, rel);
  const to = path.join(pkg, rel);
  if (!fs.existsSync(from)) {
    console.error(`missing from the repository: ${rel}`);
    process.exit(1);
  }
  fs.mkdirSync(path.dirname(to), { recursive: true });
  fs.copyFileSync(from, to);
}

console.log(`assembled ${CLOSURE.length} files into ${pkg}`);
