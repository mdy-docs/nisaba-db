# S3 backup and restore

Automatic backup of a nisaba server (or cluster) to S3-compatible
object storage — MinIO in development, S3 by dropping one flag. Built;
this document is the record of what exists and the decisions that
shaped it. The quick version:

```sh
# development storage (MinIO), once:
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  minio server ~/projects/minio-data --address :9000 --console-address :9001

# back a member up, continuously:
AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  db-backup watch \
    --target 127.0.0.1:8097 \
    --s3-endpoint http://127.0.0.1:9000 --s3-bucket nisaba-backups \
    --instance dev --keep 3

# disaster recovery, later:
db-backup restore --s3-bucket nisaba-backups --instance dev --into ./recovered
cd recovered && nisaba-server --port 8097 --raft 1
```

Credentials ride the standard AWS variables; omitting `--s3-endpoint`
targets AWS itself. Everything below explains why it has this shape.

## Where it lives, and why

Backup coordination is a **Node agent beside the cluster**
(`src/db-backup.js`, run as `db-backup`), talking to one member over
the existing client wire and to S3 over HTTP. Neither of the other
homes survived contact with the architecture:

- **Not the HTTP front end.** `db-http-front.js` holds sockets and no
  files; there may be several of them, none distinguished. A backup
  driven through it could only be a `find`-scan per collection — not a
  point-in-time image, and a reimplementation of consistency the
  snapshot machinery already owns. The logical path exists as
  `bin/db.js dump`/`restore` and stays what it is: a portability tool.
- **Not inside the C server.** S3 means HTTPS, TLS, SigV4 signing,
  retries. The server deliberately has none of that (deployment-shapes
  Decision B, and the `wip/http-transport` revert is the receipt). The
  server's contribution is three wire ops that expose the artifact it
  already produces.
- **Not a raw file copy.** One opener per directory is the invariant
  every shape enforces, and compaction/adoption delete and replace
  files — an external copier can capture a mixture no recovery ladder
  repairs. Live *database* files cannot be safely copied; the snapshot
  generation can, which is the whole design.

## The artifact

A Raft member past `--snapshot-entries` (or asked via the `snapshot`
op) writes a **generation**: every live database file streamed into an
immutable, CRC-manifested copy, stamped with the log boundary —

```
__snap__-<gen>-f0.bj …          one per live file, byte-copied, CRC'd
__snap__-<gen>.manifest.bj      binjson + CRC-32 trailer, written LAST
```

The manifest records `{ gen, lastIncludedIndex, lastIncludedTerm,
config: { live: [{role, name}] }, files: [{role, name, size, crc}] }`,
where `live` names are `"db/file"` — one generation carries **every
database in the root** through one boundary. Facts the design leans on:

- **Immutable once committed** (`snapstore.h`): reading one beside a
  live server needs no quiesce and no lock.
- **Manifest-last is the commit rule.** A generation without a valid
  manifest never existed. The S3 layout, the shipping order, the
  pruning order, and the restore order all copy this rule.
- **Roles are positional and unstable**; `config.live` is the meaning.
  Nothing anywhere interprets a generation except through its manifest.
- **Backup requires the log** (`--raft <id>`), because snapshots exist
  to compact it. A single non-replicated server that wants S3 backup
  runs as a cluster of one — that costs the log's fsync-per-commit and
  buys the entire snapshot/restore machinery, and it is the
  recommendation, not a workaround. A log-less server's options remain
  stop-and-copy or the logical `dump`.

## One snapshot, two hosts

A generation is the same artifact whether the member that wrote it is
the pure C server or the C/WASM engine hosted in Node — a requirement,
delivered, and what makes a mixed C/Node cluster a deployment choice:

- **The store is one implementation**: the JS `SnapshotStore` is C's
  `snapstore.h` compiled to WASM, so manifest encoding, the CRC commit
  rule, and the `sst_check_files` verify rule cannot disagree.
- **The prefix is one spelling**: `__snap__`, the C server's
  `REPLICA_SNAP_PREFIX`, adopted by the JS WAL host (`db-wal.js`
  migrates a legacy `__snap`-prefixed root whole at open, through the
  store's own install machinery) and pinned by tripwire tests in three
  places.
- **The scope is the instance**: `src/db-wal-instance.js` and
  `src/db-replicated-instance.js` are `db_instance.h` spoken by
  JavaScript — one root, many databases, one log whose entries carry
  the `{d, c}` envelope, generations with `"db/file"` live names.
  Cross-host tests prove a C server serves (and replays) a JS-written
  root and vice versa, and a mixed cluster converges with snapshot
  installs in both directions (`test/db.wal-instance.test.js`,
  `test/db.replicated-instance.test.js`).
- **How generation files are produced stays divergent, on purpose**:
  C byte-copies live files, the JS host compacts structures into the
  generation. Identity means same naming, schema, scope, and mutual
  adoptability — not byte-equal files; the manifest's per-file
  `size`/`crc` is the integrity contract either way.

## The wire ops

Three ops on the client wire (`docs/db-server.md` has the table):

| op | answer |
| --- | --- |
| `snapshot` | take a generation NOW; the manifest, under `snapshot:` |
| `latestSnapshot` | the committed generation's manifest |
| `readSnapshotFile` `{gen, role, offset}` | one ≤ 4 MB chunk: `{data, eof, size}` |

`readSnapshotFile` with `manifest: true` reads the manifest FILE raw —
CRC trailer and all — because a restore puts back the exact bytes whose
validity is the commit, and a re-encoding is exactly not that
(`client.readSnapshotManifest(gen)` on the JS side).

They are **per-member**, answered before the leader check: a
follower's generation is a true prefix of history, and backup reads
from one offload the leader. Two refusal codes carry the whole failure
model: `-72` (`DC_ERR_NO_SNAPSHOT_STORE`) — this server runs without a
log; the remedy is `--raft`, not a retry — and `-73`
(`DC_ERR_SNAPSHOT_GONE`) — nothing committed yet, or the named
generation was superseded and pruned; re-ask `latestSnapshot` and
restart. A prune mid-transfer is therefore a clean retry, never a pin:
a pinned generation is disk the operator cannot reclaim, held by a
caller that may never come back. `snapshot` runs exactly what the
`--snapshot-entries` trigger runs, refuses `-66` while an install is
moving the state machine, and is idempotent at an unchanged boundary.

## The agent

`db-backup` (`bin/backup.js` over `src/db-backup.js`). No engine in
the process: its imports are the server client and `src/s3.js` — a
zero-dependency SigV4 client (path-style addressing, five verbs plus
`createBucket`, internal list pagination, errors as `S3Error` with
S3's own `<Code>`).

**One member, explicit** (`--target`). The leader gives the freshest
boundary; a follower offloads read I/O and is at worst behind, never
inconsistent. The agent does not chase leadership — staleness here is
a cadence question, not a correctness one. The S3 prefix records which
member it holds (as object metadata on each manifest), and a run
pointed at a different member is stopped: generation numbers are
per-member, and an interleaved prefix would make the numbering lie.

**Files stream, and are verified by whoever sees the bytes.** The
producer CRCs as it reads (off the wire, or off disk) and the uploader
checks the size against what it actually sent — a corrupted transfer
therefore fails after a large file's bytes are in S3, which
manifest-last already makes harmless: a generation whose manifest never
lands never existed, and the abandoned upload is aborted rather than
left to be billed.

**The S3 layout copies the disk's commit rule:**

```
<prefix>/<instance>/gen-<N>/<role>.bj      the generation's files
<prefix>/<instance>/gen-<N>/manifest.bj    uploaded LAST — the commit point
```

`manifest.bj` is the member's manifest file, byte-identical; the
agent's facts (member, boundary, shipped-at) ride as S3 object
metadata, about the object and never in its bytes. Every file is
CRC-verified against the manifest in transit — bjfile's polynomial, a
transport check on top of the store's verify rule, not a fourth
opinion. Re-shipping an already-present generation is a head-check
no-op. Retention (`--keep N`) prunes oldest-first, manifest first, so
a half-pruned generation reads as absent, never as intact.

**Modes:**

- `once` — take a snapshot on the member (idempotent when nothing
  changed; `--no-snapshot` skips it), ship, prune, exit.
- `watch` — poll `ping` (its `base` moves exactly when a generation
  commits — the entries-driven cadence), ship what appears, prune;
  `--every <dur>` adds a wall-clock snapshot cadence on the side that
  has a clock, because the server deliberately does not.
- `restore` — below.

**A member with no client wire** — a JS-hosted instance
(`connectWalInstance`/`connectReplicatedInstance`), or any stopped
server's root — ships with `shipGenerationFromDir` (module API):
straight from the directory, newest manifest that *validates* (a torn
newest falls back to its predecessor, the store's own adoption rule),
live files never touched. Safe beside a live owner for the same reason
the wire ops are: the generation is immutable.

## Restore

Two different failures, and only one of them touches S3:

- **One member lost.** No S3 involved. Start a blank member with
  `--join <seed>`: the leader streams a snapshot install — existing,
  tested machinery. This is the everyday failure.
- **Everything lost.** `db-backup restore --into <empty-dir>` (a
  non-empty directory is refused; restore never merges) downloads the
  newest committed generation — or `--gen N` — with every file
  verified against the manifest, and writes the manifest **last**,
  byte-identical. Then start a server on the directory: the startup
  adoption (`restore_if_stale`) restores every live name from
  `config.live` and bases a fresh log at the boundary. An interrupted
  restore leaves files with no manifest — the crashed-attempt shape
  the store's own startup sweep already handles ("never existed").

The restored process is a **new cluster of one**; grow it with
`--join` from blank members. Restoring beside a still-live cluster is
refused by design, not by code: if a cluster is alive it needs no S3,
and a rewound member beside a live majority is how split brain is
manufactured.

A backup restores into **either host**: into a C server as above, or
into a JS-hosted instance via `restoreFromS3` →
`restoreLatestInstanceSnapshot` → `connectWalInstance`. Both
directions of the full round trip — backed up from a C member,
restored into a JS host, and the reverse — are tested against real
MinIO (`test/db.backup.test.js`, "one artifact, three hands").

## Against real AWS, as opposed to MinIO

MinIO proves the dialect — SigV4, path-style addressing, `ListObjectsV2`
paging, `x-amz-meta-*` round-tripping. Three things it cannot prove,
because a store on loopback with root credentials never does them, and
all three decide whether a backup happens at all:

- **Temporary credentials.** An instance role, an assumed role and a CI
  OIDC exchange all issue a *triple*, and AWS refuses the key pair
  without `x-amz-security-token`. `S3Client` takes `sessionToken` (or
  `AWS_SESSION_TOKEN`) and signs it — a token outside `SignedHeaders`
  is a signature over something other than what arrived. They are also
  **resolved per request and refreshed before they expire**
  (`src/aws-credentials.js`): explicit → environment → the container
  endpoint → IMDSv2. A client that captured one in its constructor
  would sign with it long after it died, which is a process that worked
  at deploy time and answers 403 the next morning.
- **Retry.** AWS answers `503 SlowDown` when a prefix is busy and bare
  `500`s transiently, and documents that clients back off and try
  again. Three attempts by default, exponential with jitter, on 429 and
  5xx and dropped connections. Never on a 4xx: that is an answer, not a
  hiccup, and retrying `AccessDenied` turns one misconfiguration into
  three requests per routine forever.
- **A socket that goes quiet.** `socketTimeoutMs` is *inactivity*, not
  a deadline: a multi-gigabyte PUT may take as long as the wire needs,
  but a connection that has said nothing for a minute is gone. Without
  it a half-open socket hangs the run that asked, and a scheduler with
  a no-overlap guard never becomes due again.

Two more things differ and are not the client's to fix. **The region is
required** when no endpoint is given, because it picks the host *and*
signs; a wrong one comes back as a malformed signature, so it is asked
for rather than guessed (S3's own `x-amz-bucket-region` is passed
through when it disagrees). And **the bucket is not created** unless
`db-backup --create-bucket` says so: it once was, on every run, which is
invisible against a MinIO holding root credentials and wrong everywhere
else — a fleet agent would need `s3:CreateBucket` to take a backup, and
an agent without it is refused on every attempt.

The IAM policy the agent actually needs is `ListBucket` on the bucket
plus `GetObject`/`PutObject`/`DeleteObject` on its keys. `ListBucket`
is not optional: without it S3 answers `403` rather than `404` for a
missing key, and "no manifest here" is how an uncommitted generation is
recognised.

`test/s3.resilience.test.js` covers all of this against a hand-written
S3 rather than MinIO — a real store will not answer `503` twice on
request, and those are exactly the paths a healthy store never takes.

## What this deliberately does not do

- **No continuous / point-in-time backup.** Granularity is a
  generation. The road to finer granularity is a learner-shaped agent
  on the peer wire (installs and log entries pushed to it, S3 as its
  disk) — the natural second version, deliberately not this one.
- **No restore into a live cluster**, stated above.
- **No backup of log-less servers.** The log is the consistency
  boundary; without it there is nothing consistent to ship.
- **No scheduling inside the server.** The engine runs no timers;
  wall-clock cadence belongs to the agent.
- ~~No multipart upload.~~ **Done.** `putObjectStream` takes an async
  iterable and holds one part, switching to a multipart upload only
  when the content outgrows one — so a generation file is never read
  whole and the ceiling is no longer the agent's RAM. Small files still
  take the single-PUT path, and are therefore still fully verified
  before a byte is stored. Measured: peak Buffer memory is flat in the
  file size (89 / 84 / 21 MiB for 96 / 192 / 384 MiB files) where
  accumulating cost 338 MiB for a 192 MiB file.
- **No encryption or auth invented here.** SigV4 and TLS to S3;
  encryption at rest is the bucket's policy; auth to the database
  remains the deployment perimeter's job, as everywhere in this
  repository.

## Still open

- Should `ping` answer the committed generation number, so a watcher
  can skip `latestSnapshot` entirely on idle polls? Cheap, but it
  widens `ping`; nothing hurts yet.
- The learner-shaped continuous backup, above.

## The record

Built in the order the plan set, each step tested before the next
(the tests named here are the proof, and they all run in `npm test`):

1. **Canonical prefix** — JS `SNAP_PREFIX` flipped to the C server's
   `__snap__`; legacy roots adopted whole at open; tripwire pins the
   spellings (`test/db.wal-snapshot.test.js`).
2. **The instance layer in the Node host** — `nisaba/wal-instance`,
   `nisaba/replicated-instance`; cross-host roots and the mixed
   C/Node cluster with installs both ways
   (`test/db.wal-instance.test.js`, `test/db.replicated-instance.test.js`).
3. **The wire ops** — `snapshot`/`latestSnapshot`/`readSnapshotFile`,
   kinds, refusal codes, client methods; native and wasip2
   (`test/db.server.test.js`).
4. **The S3 client** — `src/s3.js` against real MinIO (`test/s3.test.js`).
5. **The agent, shipping half** — byte-identical round trip, watch on
   the member's own cadence (`test/db.backup.test.js`).
6. **The restore half** — boot-and-serve from a restored root, the
   interrupted-restore shape swept, the manifest byte-identity that
   forced `manifest: true` onto the wire and the agent's facts into
   object metadata.
7. **The cross-host round trip** — both directions, plus
   `shipGenerationFromDir`.

Two real defects were found by this work and fixed where they lived:
the `encoding-type=url` listing dialect (`+` is a space) in the S3
client's tests, and — the significant one — a pre-existing pipelining
bug in `server/replica.c`: the deferred-answer paths treated the
connection's output buffer as per-request scratch and zeroed it, so an
answered request sharing a `read()` with a deferred write lost its
answer and every later answer on that connection paired one request
early. Found with a logging TCP proxy under the agent's
poll-while-writing traffic; reachable by the HTTP front's pooled
connections too; deferral now takes back only the bytes it appended
itself.
