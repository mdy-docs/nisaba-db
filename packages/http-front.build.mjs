/**
 * Assemble the publishable http-front package (npm:
 * @mdy-docs/nisaba-http-front) from the repository.
 *
 * Lives BESIDE the package, not in it, for the same reason the other
 * assembly scripts do: the package directory holds only what ships
 * (plus its package.json and README), and this script is the
 * monorepo's, reachable from the package's build/prepack scripts as
 * ../http-front.build.mjs.
 *
 * Nothing in the package directory is a source of truth: every shipped
 * file is copied from the repo root at pack time (prepack runs this),
 * so the package can never drift from what the repository builds and
 * tests. The copied tree mirrors the repo's layout exactly, because
 * the sources import each other by relative path ('../src/…',
 * '../third_party/binjson/js/binjson.js') and preserving the shape is
 * what lets them ship unmodified — bin/http-front.js included.
 *
 * The closure is the HTTP FRONT END as a deployable (Decision B of
 * docs/deployment-shapes.md): HTTP on one side, binjson frames over
 * TCP to a nisaba-server — or every member of a cluster of them — on
 * the other. No engine, no WASM; the TCP client it stands on ships
 * inside it (the same file @mdy-docs/nisaba-client-js publishes —
 * both are assembled from the one copy in src/, so they cannot
 * disagree). The server itself is a native binary, not an npm
 * package; the embedded database is packages/nisaba-db.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');
const pkg = path.join(here, 'http-front');

const CLOSURE = [
  // The front end, and the process wrapper that runs it as db-http.
  'src/db-http-front.js',
  'bin/http-front.js',
  // What it stands on: the server's own TCP client, the Extended JSON
  // convention, and the wire codec.
  'src/db-server-client.js',
  'src/extended-json.js',
  'third_party/binjson/js/binjson.js',
  // Its types.
  'types/http-front.d.ts'
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
