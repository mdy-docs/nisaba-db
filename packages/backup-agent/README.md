# nisaba-backup

The S3 backup agent for the nisaba database server: a Node process (or
a library) beside the server — or beside a cluster of them — that
ships a member's committed snapshot generations to S3-compatible
object storage, prunes by retention, and restores a generation into an
empty directory for a fresh server to adopt.

**No engine in this package.** The agent's imports are the server's
own TCP client (which ships inside it) and an S3 client that is a page
of `node:crypto` — SigV4 over `node:http`, no AWS SDK. The artifact it
moves is the one the server's snapshot machinery already produces;
this agent invents nothing about what a consistent copy is.

## Running it

```sh
# development: MinIO
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  npx db-backup watch \
    --target 127.0.0.1:8097 \
    --s3-endpoint http://127.0.0.1:9000 --s3-bucket nisaba-backups \
    --instance dev --keep 3

# production: AWS, by dropping --s3-endpoint
npx db-backup once --target 127.0.0.1:8097 --s3-bucket my-backups --instance prod

# disaster recovery: a generation into an empty directory
npx db-backup restore --s3-bucket my-backups --instance prod --into ./restored
```

`once` takes a snapshot on the member (idempotent when nothing
changed), ships it, applies retention, exits. `watch` stays up and
ships every generation the member commits; `--every` adds a wall-clock
cadence on top. `restore` downloads a generation and tells you what to
do next: start a server on the directory — its startup adoption does
the rest. The restored process is a **new cluster of one**; never
start it beside the cluster the backup came from.

One agent backs up **one member** (`--target`); a follower is fine —
its generation is a true prefix of history. Generation numbers are
per-member, so each member gets its own `--instance` prefix, and the
agent enforces that against what the prefix already holds.

The S3 layout copies the disk's commit rule: files first,
`manifest.bj` last — a listing without a manifest never existed, and
pruning deletes the manifest first, so a half-pruned generation reads
as absent, never as intact. Every file is CRC-checked in transit, both
directions. The repository's `docs/s3-backup.md` has the whole story.

## As a library

```js
import { BackupAgent, restoreFromS3 } from '@mdy-docs/nisaba-backup';
import { S3Client } from '@mdy-docs/nisaba-backup/s3';
import { connectServer } from '@mdy-docs/nisaba-client-js';

const s3 = new S3Client({ bucket: 'nisaba-backups', endpoint: 'http://127.0.0.1:9000' });
const client = await connectServer('127.0.0.1:8097');
const agent = new BackupAgent({ client, s3, instance: 'dev', member: '127.0.0.1:8097' });
await agent.once({ keep: 3 });
```

`shipGenerationFromDir` covers the member with no client wire to ask —
a JS-hosted instance, or a stopped server's root. The `./s3` export is
the whole S3 client, usable on its own: five verbs, path-style
addressing, works against MinIO and AWS alike.

## Building this package

Every shipped file is copied from the repository at pack time
(`prepack` runs `../backup-agent.build.mjs` — the assembly script
lives beside this directory, not in it), so the package cannot drift
from what the repository builds and tests. From a repo checkout:

```sh
cd packages/backup-agent
npm run build      # assembles src/, bin/, types/, third_party/
npm test           # a smoke test; a live S3 round-trip if MinIO answers
npm pack           # the publishable tarball
```

## License

BSD-2-Clause.
