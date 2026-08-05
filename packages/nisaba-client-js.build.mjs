/**
 * Assemble the publishable nisaba-client-js package from the repository.
 *
 * Lives BESIDE the package, not in it, for the same reason
 * nisaba-db.build.mjs does: the package directory holds only what ships
 * (plus its package.json and README), and this script is the monorepo's,
 * reachable from the package's build/prepack scripts as
 * ../nisaba-client-js.build.mjs.
 *
 * Nothing in the package directory is a source of truth: every shipped
 * file is copied from the repo root at pack time (prepack runs this), so
 * the package can never drift from what the repository builds and tests.
 * The copied tree mirrors the repo's layout exactly, because
 * db-server-client.js imports binjson by relative path
 * ('../third_party/binjson/js/binjson.js') and preserving the shape is
 * what lets it ship unmodified.
 *
 * The closure is the CLIENT for the database server (server/main.c) and
 * nothing else: JavaScript to a TCP socket to binjson frames. No engine,
 * no WASM, no storage provider — which is the package's whole claim, and
 * why this list is four files. The embedded database is
 * packages/nisaba-db; the server, replication, and the HTTP front stay
 * the repository package's.
 *
 * types/nisaba.d.ts rides along because types/server-client.d.ts imports
 * Document/Filter/Update/ObjectId from it — it is the shared vocabulary
 * of the driver shape, and shipping it unmodified beats maintaining a
 * second copy that could disagree.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');
const pkg = path.join(here, 'nisaba-client-js');

const CLOSURE = [
  // The client: the whole runtime surface.
  'src/db-server-client.js',
  // The one module it imports: the pure-JS wire codec.
  'third_party/binjson/js/binjson.js',
  // Its types, and the type vocabulary they import from.
  'types/server-client.d.ts',
  'types/nisaba.d.ts'
];

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
