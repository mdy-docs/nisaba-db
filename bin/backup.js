#!/usr/bin/env node
/**
 * The S3 backup agent (src/db-backup.js) as a process: one member's
 * committed snapshot generations shipped to S3-compatible storage —
 * MinIO in development, S3 by dropping --s3-endpoint. No engine in this
 * process (docs/s3-backup.md).
 *
 *     AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
 *       db-backup watch \
 *         --target 127.0.0.1:8097 \
 *         --s3-endpoint http://127.0.0.1:9000 --s3-bucket nisaba-backups \
 *         --instance dev --keep 3
 *
 *     db-backup once --target 127.0.0.1:8097 --s3-bucket ... --instance dev
 */
import { connectServer } from '../src/db-server-client.js';
import { S3Client } from '../src/s3.js';
import { BackupAgent, restoreFromS3 } from '../src/db-backup.js';

function usage() {
  console.error(`Usage: db-backup <once|watch|restore> --s3-bucket name --instance name [options]

Commands:
  once    Take a snapshot on the member (idempotent when nothing changed),
          ship it, apply retention, exit. Needs --target.
  watch   Stay up: ship every generation the member commits (--every adds
          a wall-clock cadence), apply retention, until stopped. Needs
          --target.
  restore Download a generation into an EMPTY directory (--into), in the
          snapstore's own on-disk shape, manifest last. Then start a
          server on it: the startup adoption does the rest. The restored
          process is a NEW cluster of one -- never restore beside a live
          cluster.

Options:
  --target host:port     The ONE member this agent backs up. A follower is
                         fine -- its generation is a true prefix of history.
  --s3-bucket name       Created if absent.
  --s3-endpoint url      e.g. http://127.0.0.1:9000 (MinIO). Omit for AWS.
  --s3-region name       Default us-east-1.
  --s3-prefix p          Key prefix in front of the instance name.
  --instance name        Names this instance in the bucket. Generation
                         numbers are per-member: one member per instance
                         prefix, enforced.
  --keep n               Retention: keep the newest n generations in S3.
  --poll dur             watch: how often to ask the member (default 5s).
  --every dur            watch: also TAKE a snapshot this often -- cadence
                         decoupled from write volume.
  --no-snapshot          once: ship what is committed without taking one.
  --into dir             restore: the empty directory to restore into.
  --gen n                restore: a specific generation (default: newest
                         committed).

Durations: 30s, 5m, 1h (a bare number is seconds). Credentials ride
AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY. docs/s3-backup.md has the story.`);
  process.exit(1);
}

function duration(s) {
  const m = /^(\d+)(s|m|h)?$/.exec(s);
  if (!m) usage();
  return Number(m[1]) * { undefined: 1000, s: 1000, m: 60_000, h: 3_600_000 }[m[2]];
}

const argv = process.argv.slice(2);
const command = argv.shift();
if (command !== 'once' && command !== 'watch' && command !== 'restore') usage();

const opts = {
  region: 'us-east-1', prefix: null, keep: null,
  pollMs: 5000, everyMs: null, takeSnapshot: true
};
for (let i = 0; i < argv.length; i++) {
  const arg = argv[i];
  const next = () => { if (i + 1 >= argv.length) usage(); return argv[++i]; };
  if (arg === '--target') opts.target = next();
  else if (arg === '--s3-bucket') opts.bucket = next();
  else if (arg === '--s3-endpoint') opts.endpoint = next();
  else if (arg === '--s3-region') opts.region = next();
  else if (arg === '--s3-prefix') opts.prefix = next();
  else if (arg === '--instance') opts.instance = next();
  else if (arg === '--keep') opts.keep = Number(next());
  else if (arg === '--poll') opts.pollMs = duration(next());
  else if (arg === '--every') opts.everyMs = duration(next());
  else if (arg === '--no-snapshot') opts.takeSnapshot = false;
  else if (arg === '--into') opts.into = next();
  else if (arg === '--gen') opts.gen = Number(next());
  else usage();
}
if (!opts.bucket || !opts.instance) usage();
if (command === 'restore' ? !opts.into : !opts.target) usage();

const s3 = new S3Client({
  bucket: opts.bucket, endpoint: opts.endpoint, region: opts.region
});
const instance = opts.prefix ? `${opts.prefix}/${opts.instance}` : opts.instance;

if (command === 'restore') {
  const r = await restoreFromS3({
    s3, instance, into: opts.into, gen: opts.gen ?? null,
    log: (kind, detail) => console.error(`db-backup: ${kind} ${JSON.stringify(detail)}`)
  });
  console.error(`db-backup: restored gen ${r.gen} (boundary ${r.lastIncludedIndex}, ${r.files} files) into ${opts.into}

Next, from that directory, start a server on it:

    nisaba-server --port <port> --raft 1 --raft-port <raft-port>

The startup adoption restores every database from the generation and
bases a fresh log at index ${r.lastIncludedIndex}. The restored process
is a NEW cluster of one; grow it with --join from blank members. Never
start it beside the cluster the backup came from.`);
  process.exit(0);
}

await s3.createBucket();

const client = await connectServer(opts.target);
const agent = new BackupAgent({
  client, s3,
  instance,
  member: opts.target,
  log: (kind, detail) => console.error(`db-backup: ${kind} ${JSON.stringify(detail)}`)
});

try {
  if (command === 'once') {
    const r = await agent.once({ takeSnapshot: opts.takeSnapshot, keep: opts.keep });
    console.error(r.shipped
      ? `db-backup: shipped gen ${r.gen} (boundary ${r.boundary})`
      : r.absent
        ? 'db-backup: nothing to ship (no committed generation on the member)'
        : `db-backup: gen ${r.gen} is already in S3`);
  } else {
    const abort = new AbortController();
    process.on('SIGINT', () => abort.abort());
    process.on('SIGTERM', () => abort.abort());
    console.error(`db-backup: watching ${opts.target} (poll ${opts.pollMs}ms${opts.everyMs ? `, snapshot every ${opts.everyMs}ms` : ''})`);
    await agent.watch({
      pollMs: opts.pollMs, everyMs: opts.everyMs, keep: opts.keep,
      signal: abort.signal
    });
  }
} finally {
  await client.close().catch(() => {});
}
