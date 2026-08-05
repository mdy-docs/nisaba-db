/**
 * Assemble the publishable backup-agent package (npm:
 * @mdy-docs/nisaba-backup) from the repository.
 *
 * Lives BESIDE the package, not in it, for the same reason the other
 * assembly scripts do: the package directory holds only what ships
 * (plus its package.json and README), and this script is the
 * monorepo's, reachable from the package's build/prepack scripts as
 * ../backup-agent.build.mjs.
 *
 * Nothing in the package directory is a source of truth: every shipped
 * file is copied from the repo root at pack time (prepack runs this),
 * so the package can never drift from what the repository builds and
 * tests. The copied tree mirrors the repo's layout exactly, because
 * the sources import each other by relative path ('./s3.js',
 * '../src/db-server-client.js') and preserving the shape is what lets
 * them ship unmodified — bin/backup.js included.
 *
 * The closure is the S3 BACKUP AGENT as a deployable and a library
 * (docs/s3-backup.md): BackupAgent / restoreFromS3 /
 * shipGenerationFromDir over the server's own TCP client, and the
 * zero-dependency S3 client they stand on (SigV4 over node:http; no
 * AWS SDK), exported at ./s3 for a caller that wants just that. No
 * engine, no WASM. The TCP client ships inside it, assembled from the
 * same src/ copy every package assembles from, so none of them can
 * disagree.
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));   // packages/
const repo = path.resolve(here, '..');
const pkg = path.join(here, 'backup-agent');

const CLOSURE = [
  // The agent, the S3 client it stands on, and the process wrapper
  // that runs them as db-backup.
  'src/db-backup.js',
  'src/s3.js',
  'bin/backup.js',
  // What the agent drives: the server's own TCP client, and the wire
  // codec beneath it.
  'src/db-server-client.js',
  'third_party/binjson/js/binjson.js',
  // Its types; s3.d.ts is imported by backup.d.ts.
  'types/backup.d.ts',
  'types/s3.d.ts'
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
