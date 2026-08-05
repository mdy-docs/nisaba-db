/**
 * Assemble the publishable http-client package (npm:
 * @mdy-docs/nisaba-client-http) from the repository.
 *
 * Lives BESIDE the package, not in it, for the same reason the other
 * assembly scripts do: the package directory holds only what ships
 * (plus its package.json and README), and this script is the
 * monorepo's, reachable from the package's build/prepack scripts as
 * ../http-client.build.mjs.
 *
 * Nothing in the package directory is a source of truth: every shipped
 * file is copied from the repo root at pack time (prepack runs this),
 * so the package can never drift from what the repository builds and
 * tests. The copied tree mirrors the repo's layout exactly, because
 * the sources import each other by relative path ('./extended-json.js',
 * '../third_party/binjson/js/binjson.js') and preserving the shape is
 * what lets them ship unmodified.
 *
 * The closure is the BROWSER-CAPABLE client for the HTTP front end
 * (db-http, @mdy-docs/nisaba-http-front): the TCP client's surface
 * spelled against fetch. Nothing in it touches a node: module — that
 * is the package's whole claim, and why it is the one client in the
 * family a web page can import. The TCP client for server-side
 * JavaScript is @mdy-docs/nisaba-client-js; the embedded database is
 * packages/nisaba-db.
 *
 * types/server-client.d.ts and types/nisaba.d.ts ride along because
 * types/http-client.d.ts imports the mirrored driver shapes from them
 * — a second copy of a mirrored type is how the mirror drifts.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');
const pkg = path.join(here, 'http-client');

const CLOSURE = [
  // The client: the whole runtime surface.
  'src/db-http-client.js',
  // What it stands on: the Extended JSON convention, and binjson for
  // ObjectId. Both pure JS; no node: modules anywhere in the closure.
  'src/extended-json.js',
  'third_party/binjson/js/binjson.js',
  // Its types, and the mirrored vocabulary they import from.
  'types/http-client.d.ts',
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
