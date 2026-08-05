# S3 backup and restore — the plan

Automatic backup of a nisaba server (or cluster) to S3-compatible object
storage, with MinIO as the development target. This document records the
design decisions and the implementation steps; nothing described here is
built yet, and where a behaviour still needs proving it says so rather
than describing an intention as though it were code.

## The three questions, answered first

**Where does backup coordinate — the HTTP front end or the C server?**
Neither, exactly. It is a **Node backup agent beside the cluster**,
talking to one member over the existing client wire, pushing to S3 over
HTTP. The C server's contribution is three new wire ops that expose the
snapshot artifact it already produces. The reasoning is below, but it is
the same split Decision B of [deployment-shapes.md](deployment-shapes.md)
already made: the C server owns the files and speaks frames; anything
that speaks HTTP — and S3 *is* an HTTP API, with TLS and SigV4 signing —
lives in a Node process next to it.

**How does backup interact with Raft?** It rides on it. A Raft member
past `--snapshot-entries` already snapshots itself into an immutable,
CRC-manifested **generation** stamped with `lastIncludedIndex` and
`lastIncludedTerm`, and compacts its log through that boundary
(`server/replica.c`, `snapshot_take`). That generation is a consistent
point-in-time image of the whole instance — every database, every
index, taken at a quiesced applied index. Backup is *shipping that
artifact to S3*, not inventing a second opinion about what a consistent
copy is. Snapshots are per-member and local ("Each member does this for
itself, as Raft members do; nothing about it is replicated" —
`server/replica.h`), so backing up the cluster means picking a member.

**How does restore work?** Two different failures, two different
answers, and only one of them touches S3:

- **One member lost.** No S3 involved. Start a blank member with
  `--join <seed>`: the leader notices its log position is below the
  compacted base and streams a snapshot install — existing, tested
  machinery (`test/db.server.test.js`, "a compacted leader installs a
  snapshot into a joiner"). This is the everyday failure and it already
  works.
- **Everything lost** (the root, the disk, the whole cluster). Download
  a generation from S3, assemble it into an empty root under the
  snapstore's own file names, and start the server: the startup
  adoption path (`restore_if_stale`) sees a committed generation whose
  boundary is past the log's base, deletes nothing (the root is empty),
  restores every file to its live name by the manifest's `config.live`
  map, and bases a fresh log at the boundary. The restored process is a
  **new cluster of one**; the others join blank and are caught up by
  the install path above.

---

## Why not the alternatives

**Not in the HTTP front end.** `db-http-front.js` holds sockets and no
files; there may be several of them, and none is distinguished. A
backup driven through it could only be a `find`-scan per collection —
which is not a point-in-time image (each cursor sees a different
moment), rebuilds indexes on restore instead of copying them, and
re-implements consistency the snapshot machinery already owns. The
logical path already exists as `bin/db.js dump`/`restore` and stays what
it is: a portability tool, not a backup.

**Not S3 inside the C server.** S3 means HTTPS, TLS, SigV4 request
signing, retries, multipart uploads. The server deliberately has none
of that — no HTTP ("and it is not getting any"), no TLS, and the one
revert on record (`wip/http-transport`) is the receipt for what
happens when a protocol stack is invited into `main.c`. The engine
reads no clock and opens no file; the transport holds sockets it can
`poll()`. An S3 client fits neither layer.

**Not a raw file copy from a cron job.** "One opener per directory" is
the invariant every shape enforces, and the storage-layout audit is
blunt about what a live copy risks: compaction and snapshot adoption
*delete and replace* files, so an external copier can capture a
post-flip catalog beside pre-flip files — a mixture no recovery ladder
repairs. A stopped server or a filesystem-level snapshot (ZFS/EBS) can
be copied; a live root cannot, and nothing should encourage it.

**Not (yet) a Raft learner whose disk is S3.** A backup agent could
join the cluster as a non-voting learner on the peer wire and receive
installs and log entries pushed to it — continuous backup with
point-in-time granularity. It is the natural *second* version of this
feature, and the peer wire already speaks everything it needs. Declined
for v1 because it makes the backup agent a cluster member (config
changes, membership visible to every tool), and because generation
shipping delivers the operator-visible value — "my data survives losing
every disk" — with a fraction of the machinery. Nothing in v1 blocks
it: the S3 layout below stores the boundary index precisely so a
log-tailing v2 can append to it.

---

## The artifact

What a committed generation is, today, on a member's disk:

```
__snap__-<gen>-f0.bj            one per live database file, byte-copied,
__snap__-<gen>-f1.bj            CRC'd during the copy, fsync'd
...
__snap__-<gen>.manifest.bj      written LAST — the commit point
```

The manifest is binjson + CRC-32 of exactly those bytes, and records:

```
{ gen, lastIncludedIndex, lastIncludedTerm,
  config: { live: [{role, name}] },        // role → "db/file" live name
  files:  [{role, name, size, crc}] }
```

Facts the design leans on, with their owners:

- **Immutable once committed.** The files are never appended or
  rewritten; a generation is superseded and pruned, never edited
  (`snapstore.h`). That is what makes reading it while the server runs
  safe — no quiesce, no lock, no race with writes.
- **Instance-wide.** `live` names are `"<db>/<file>"`; one generation
  carries every database in the root through one boundary.
- **Manifest-last is the commit rule.** A generation without a valid
  manifest "never existed". The S3 layout copies this rule verbatim.
- **Roles are positional and unstable** (`f0`, `f1`, … assigned by
  listing order at snapshot time). Tools use `config.live` to interpret
  a generation and never compare generations by role.
- **The verify rule has one owner** — `sst_check_files`
  (`snapstore.h`: "A follower deciding whether a transferred snapshot
  is intact is not a place for three opinions"). Restore ends by
  letting the server's own adoption verify; the agent's checks are
  transport integrity on top, not a fourth opinion.
- **One artifact for both hosts** is a requirement, not an
  aspiration — see the next section. Tooling interprets generations
  strictly through the manifest; nothing may hardcode a prefix or a
  role where the manifest can name the file instead.

**Backup therefore requires the log** (`--raft <id>`), because
snapshots exist to compact it. A server running without `--raft` has no
generations to ship; its options are stop-and-copy or the logical
`dump`, and this plan does not change that. A single non-replicated
server that wants S3 backup runs as a cluster of one — `--raft 1` with
itself as the only member — which costs it the log's fsync-per-commit
and buys it the entire snapshot/restore machinery. That trade is the
recommendation, not a workaround.

---

## One snapshot, two hosts

**Requirement: a generation is the same artifact whether the member
producing it is the pure C server or the C/WASM engine hosted in Node.**
Both hosts must be able to sit in one Raft cluster, install snapshots
into each other, and be backed up and restored by the same tooling —
a backup taken from a C member restores into a Node-hosted member and
vice versa.

Most of this is already true, because the layers below are single-owner:

- **The store is one implementation.** The JS `SnapshotStore` is C's
  `snapstore.h` compiled to WASM (`structures-core.js`: "this._ctx —
  the C store (snapstore.h)"), so manifest encoding, the CRC-32 commit
  rule, generation lifecycle, and the `sst_check_files` verify rule
  cannot disagree between hosts — there is nothing to keep in sync.
- **The Raft core is one implementation** (`raft_node.h`, phase 7c),
  and InstallSnapshot — serving, staging, verifying, adopting — is the
  node's on both hosts. The peer wire is byte-for-byte shared.

Three things diverge above those layers, and closing them is part of
this plan:

**1. The prefix.** C hardcodes `__snap__` (`REPLICA_SNAP_PREFIX`,
`server/replica.c`); the JS host defaults to `__snap`
(`SNAP_PREFIX`, `src/db-wal.js`) — and it is already a parameter
(`snapshotPrefix`), so this is a default, not a design. **Decision:
`__snap__` is canonical** (it is the form production servers have
written). **Built:** the JS default flipped, and `openWalStorage`
adopts a legacy root at open — the newest valid `__snap-` generation
is copied into a canonical one through the store's own install
machinery (CRC-verified per file, manifest-last commit, crash anywhere
leaves an adoptable root), the live log is moved onto its canonical
paired name, and nothing legacy survives the first open.
`restoreLatestSnapshot` falls back to the legacy store so disaster
recovery never requires a migration first. A tripwire test pins the JS
prefix to `REPLICA_SNAP_PREFIX` in `server/replica.c`, verbatim.

**2. The scope.** A C generation spans the **instance** — live names
are `"db/file"`, one log beside the database directories, the entry
envelope naming the target database (`db_instance.h`). The Node host
replicates **one database** — `WalDb` is one directory, bare live
names, the log inside it. This is the real gap, and it is wider than
snapshots: until the Node host is instance-shaped, a mixed C/Node
cluster is wire-compatible but not log-compatible, and "participate in
Raft replication together" is not yet true. **Decision: the Node host
grows an instance layer** — one log and one store at a root above the
database directories, entries carrying the same `db_instance.h`
envelope C already defines (adopted, not reinvented), generations with
`"db/file"` live names. `WalDb` stays what it is underneath, as the
per-database apply target.

**3. How generation files are produced.** C byte-copies live files
(`copy_file`); the JS host streams each structure through `compact()`
into the generation. Both produce valid, self-describing files that
adoption copies back by the manifest's `config.live` map. **Decision:
this stays divergent, on purpose.** Identity means same naming, same
manifest schema, same scope, mutually adoptable — not byte-equal
files. A backup tool that assumed byte equality across hosts would be
wrong even within one host (two snapshots of identical data differ),
and the manifest's per-file `size`/`crc` is the integrity contract
either way.

What proves the requirement, when built: a generation written by each
host adopted by the other at startup; a mixed cluster where a C leader
installs into a Node member and the reverse; and the S3 round-trip
crossing hosts — backed up from one, restored into the other.

---

## What gets built

### 1. Three ops on the client wire (C)

Added to `OP_NAMES` in `wasm/src/db_request.c` — the owner of the op
list — with the server-side handlers reading the snapstore. All three
are refused on a server without a log (no `--raft`), with a refusal
code saying exactly that.

| op | request | answer |
| --- | --- | --- |
| `snapshot` | `{}` | `{gen, lastIncludedIndex, lastIncludedTerm}` — take one NOW |
| `latestSnapshot` | `{}` | the committed manifest, as a document; a refusal when none exists yet |
| `readSnapshotFile` | `{gen, role, offset}` | `{data, eof}` — one bounded chunk (≤ 4 MB) of one generation file |

Notes that are decisions:

- `snapshot` runs exactly what the `--snapshot-entries` trigger runs
  (`snapshot_take` + log compaction), synchronously between poll-loop
  turns, stop-the-world — the cadence moves to the caller, the
  mechanism does not change. An agent that wants nightly backups
  decoupled from write volume calls this; one content with the
  entries-driven cadence never does.
- `readSnapshotFile` serves the **committed** generation only. If the
  member commits generation N+1 mid-transfer and prunes N, the next
  read of N is refused; the agent restarts from `latestSnapshot`. An
  accepted race, resolved by retry, not by a pin — a pinned generation
  is a disk the operator cannot reclaim, held by a client that may
  never come back (the same reasoning as cursor slots, resolved the
  opposite way because a backup restarts cheaply and a scan does not).
- The paired compacted log (`__snap__-log-<gen>.bj`) is **not** served
  in v1: it is live (appended to) after adoption, and a
  boundary-exact restore does not need it — `restore_if_stale` bases a
  fresh log at the boundary when the log ends below it. Serving a
  stable prefix of it is v2's point-in-time story.
- Like every op, these land in `src/db-server-client.js` in the same
  change (`latestSnapshot()`, `readSnapshotFile()`, `snapshot()` on the
  client), because a method without an op — or an op without a method —
  is a second opinion about what the server does.

### 2. A minimal S3 client (Node, zero dependencies)

`src/s3.js`: SigV4 signing over `node:http`/`node:https` — `PUT`,
`GET`, `HEAD`, `DELETE`, `ListObjectsV2`, bucket creation. Path-style
addressing (MinIO's default), region configurable (`us-east-1` for
MinIO). No AWS SDK: the repository ships zero runtime dependencies
everywhere else, SigV4 is ~150 lines of `node:crypto`, and the surface
needed is five verbs. Single-part uploads in v1 — a generation file
over 5 GB (the single-PUT ceiling) is the trigger for adding multipart,
and the failure until then is a loud refusal naming the limit, not a
silent truncation.

### 3. The backup agent (Node)

`src/db-backup.js`, run as `bin/backup.js` (`db-backup` alongside `db`
and `db-http` in the root package's `bin`). No engine in this process;
its imports are the server client and `src/s3.js`.

**Backup loop.** Connect to the configured member. Poll `ping` (it
already answers `base` — "base moves when a snapshot commits"); when
`base` passes the last generation shipped, or on `--every <interval>`
after calling `snapshot`:

1. `latestSnapshot` → the manifest.
2. Skip if that `gen` for that member already has a manifest in S3.
3. Upload every file: `readSnapshotFile` chunks streamed into S3 PUTs,
   size checked against the manifest's `size`, CRC-32 computed during
   the stream and checked against the manifest's `crc`.
4. Upload the manifest **last** — the commit point, same rule as disk.
5. Prune S3 by retention (`--keep N` generations), oldest first,
   manifest deleted first (so a half-pruned generation reads as absent,
   never as intact).

**Which member.** `--target host:port`, one member, explicit. The
leader gives the freshest boundary; a follower offloads the read I/O
and is at worst behind, never inconsistent — a follower's generation is
a true prefix of history. The agent does not chase leadership (it is
not the HTTP front; staleness here is a cadence question, not a
correctness one). Backing up "the cluster" is backing up one member.

**Restore command.** `db-backup restore --from s3://… --gen N --into
<empty-dir>` (newest committed generation when `--gen` is omitted —
"committed" meaning its manifest is present and valid):

1. Refuse a non-empty target directory. Restore never merges.
2. Download the manifest, verify its CRC.
3. Download every file to `__snap__-<gen>-<role>.bj`, verifying size
   and CRC against the manifest.
4. Write the manifest file last, byte-identical.
5. Print what the operator does next, exactly:
   `nisaba-server --raft 1 --raft-port <p> --port <p'>` from that
   directory — the startup adoption restores live names and bases a
   fresh log at the boundary — then grow the cluster with `--join`.

The restored member is a new cluster. The plan explicitly does **not**
support restoring "into" a live cluster: if a cluster is alive it needs
no S3, and a rewound member beside a live majority is how split brain
is manufactured. The doc the command prints says this too.

**S3 layout.**

```
s3://<bucket>/<prefix>/<instance>/gen-<N>/<role>.bj
s3://<bucket>/<prefix>/<instance>/gen-<N>/manifest.bj      ← written last
```

`<instance>` is operator-chosen (`--instance`), because nothing on the
wire names an instance and inventing an identity here would be a second
opinion about a fact the operator owns. `gen` numbers are per-member;
mixing members under one `<instance>` prefix is refused by the agent
(the manifest upload records `--target`, and a mismatch stops the run).

### 4. Configuration, and MinIO for development

All agent configuration is flags and environment, no config file:

```sh
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  minio server ~/projects/minio-data --address :9000 --console-address :9001

AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
  db-backup watch \
    --target 127.0.0.1:8097 \
    --s3-endpoint http://127.0.0.1:9000 --s3-bucket nisaba-backups \
    --instance dev --keep 3
```

Credentials ride the standard AWS variables so the same invocation
points at real S3 by dropping `--s3-endpoint`. TLS to S3 is
`node:https` doing its job; encryption at rest is the bucket's policy
(SSE), and auth *to the database* remains what it is everywhere in this
repository — the gateway's job, which is why the agent is expected to
sit on the same network the members do.

---

## What this deliberately does not do

- **No continuous / point-in-time backup.** v1's granularity is a
  generation. The road to finer granularity is the learner-shaped v2
  (log tailing to S3), not serving mutable log prefixes over ops.
- **No restore into a live cluster,** stated above.
- **No backup of log-less servers.** The log is the consistency
  boundary; without it there is nothing consistent to ship.
- **No scheduling inside the server.** The engine runs no timers; the
  `--snapshot-entries` trigger stays the only in-process cadence, and
  wall-clock cadence belongs to the agent, which has a clock.
- **No encryption or auth invented here.** SigV4 and TLS for S3;
  everything else is the deployment's perimeter.

## Open questions

1. **Should `readSnapshotFile` hold a read handle per connection** (a
   cursor-shaped resource, reclaimed with the connection) rather than
   open-per-chunk? Open-per-chunk is simpler and the files are
   immutable; measure before adding state.
2. **Refusal codes**: one new code ("no snapshot store on this server")
   or reuse of an existing -6x? Decided in `db_session.h` when the ops
   land, where the other codes live.
3. **Should `ping` answer the committed generation number** so the
   agent can skip `latestSnapshot` in the idle loop? Cheap, but it
   widens `ping`; decide when the loop exists.

## Implementation order

Each step lands tested before the next starts; every step is useful on
its own. Steps 1–2 are the host-convergence work from "One snapshot,
two hosts"; the S3 agent (steps 3–7) depends only on step 1, so the
two tracks can proceed in parallel after it.

1. ✅ **Canonical prefix — built.** JS `SNAP_PREFIX` flipped to
   `__snap__`; open adopts a legacy `__snap-` root whole (generation
   re-committed canonically via the store's install machinery, log
   moved to its canonical paired name, legacy files swept);
   `restoreLatestSnapshot` reads legacy roots directly. Tested in
   `test/db.wal-snapshot.test.js` ("canonical snapshot prefix"):
   fresh roots write canonical only; a legacy root with a post-boundary
   log suffix migrates with data, boundary, and replay intact;
   remigration is a fixpoint; restore works unmigrated; and a tripwire
   pins the prefix to `server/replica.c`'s `REPLICA_SNAP_PREFIX`.
   Cross-host root *opens* cannot be tested before step 2 — the two
   hosts still disagree about scope (instance vs. database), which is
   exactly what step 2 exists to close.
2. **The instance layer in the Node host.** One log and one store at a
   root above the database directories; entries carry the
   `db_instance.h` envelope; generations record `"db/file"` live
   names. This is the largest step and the only one that touches
   replication.

   **The storage half is built** — `src/db-wal-instance.js`
   (`nisaba/wal-instance`): `connectWalInstance` opens the C server's
   root shape (refusing a directory that is itself a database, the
   `root_holds_a_database` rule), `WalInstance` wraps every command in
   the `{ d, c }` envelope on one shared log and one serialize chain,
   `dropDatabase` is the logged `{ d, i: "drop" }` act,
   `snapshot()` writes one generation for the whole root with
   `"db/file"` live names, and `restoreLatestInstanceSnapshot` is the
   disaster path. Replay applies NORMAL entries only — NOOP/CONFIG are
   the Raft node's — and refuses an envelope-less log loudly rather
   than misapply it. Tested in `test/db.wal-instance.test.js`,
   including the cross-host round trips against the real native
   server: a JS-written root (generation, applied suffix, and a forged
   durable-but-unapplied entry) is served by the C server, **which
   replays the JS-built entry**; a C-written root — its log with
   noop/config entries, its `--snapshot-entries` generation — opens in
   the JS host, takes a JS write, and goes back to the C server, which
   carries on from there.

   **The replicated half is built too** — `src/db-replicated-instance.js`
   (`nisaba/replicated-instance`): `connectReplicatedInstance` is the
   JavaScript equivalent of `nisaba-server --raft <id>` — an
   InstanceStateMachine applying enveloped entries (the same code
   single-node recovery replays through), the file seam resolving
   `"db/file"` live names to per-database subproviders, and the install
   swap window closing every database, adopting, and removing scopes
   the generation does not hold (adopt_install's contract). Tested in
   `test/db.replicated-instance.test.js`: cross-database convergence,
   replicated dropDatabase, blank-member and stale-disk instance-wide
   installs on the simulator — and the **mixed C/Node cluster over the
   real peer wire**: a C leader replicating into a Node member and
   installing into a blank one, and a Node leader installing into a
   blank C member (`snapshot install adopted at index` on its stderr),
   which then takes leadership by transfer and serves both databases
   over its own client wire. A mixed cluster is now a deployment
   choice, not a feature. **Step 2 is done.**
3. ✅ **The three ops in C — built.** `snapshot`, `latestSnapshot` and
   `readSnapshotFile` are in `OP_NAMES` with their own request kind
   (`DBS_REQ_SNAPSHOT`, answered per-member before the leader check),
   implemented in `server/replica.c` against the committed generation,
   refused `-72` (`DC_ERR_NO_SNAPSHOT_STORE`) by any log-less path and
   `-73` (`DC_ERR_SNAPSHOT_GONE`) for a missing or superseded
   generation — the open refusal-code question is thereby answered.
   `snapshot` runs exactly the `--snapshot-entries` trigger, refuses
   `-66` mid-install, and is idempotent at an unchanged boundary. The
   client methods and `WIRE_OPS` entries landed in the same change
   (`src/db-server-client.js`, `types/server-client.d.ts`), and
   `docs/db-server.md` gained the ops. Tested in
   `test/db.server.test.js`, native and wasip2: all three refused
   without `--raft`; the manifest round-trip with `"db/file"` live
   names across two databases; chunked reads reassembling every
   generation file byte-for-byte (offsets honored, past-end refused);
   and the superseded-generation refusal mid-transfer.
4. ✅ **`src/s3.js` — built.** SigV4 over `node:http`/`https`,
   path-style, five verbs (`putObject`/`getObject`/`headObject`/
   `deleteObject`/`list`) plus `createBucket`; bodies are Buffers
   signed over the real payload hash; `list` pages internally with
   `encoding-type=url` (whose query-string dialect — `+` is a space —
   was found by the test, not the docs); every non-2xx is an `S3Error`
   carrying HTTP's status and S3's own `<Code>`. Tested in
   `test/s3.test.js` against MinIO when something answers at
   `NISABA_S3_TEST_ENDPOINT` (default `http://127.0.0.1:9000`),
   `describe.skipIf` otherwise: bucket idempotence, binary round-trip,
   awkward key names through signing AND listing, real pagination,
   prefix+delimiter, and the 404 error shape.
5. **`src/db-backup.js` + `bin/backup.js`, backup half.** Test: server
   `--raft 1 --snapshot-entries 8`, writes past the trigger, agent
   ships to MinIO, assert the S3 listing matches the manifest and the
   manifest is last.
6. **Restore half.** Test: wipe the root, `db-backup restore`, start
   the server on the restored directory, assert
   `snapshot install adopted at index N` on stderr, every document
   readable, and a second member joining blank converges. The
   mid-restore crash states (torn download, files-without-manifest)
   must land in the forged-state suite in `test/db.server.test.js` —
   they are exactly the directory shapes it already proves recoverable.
7. **Watch mode + retention.** Test: cadence on `base` movement, `--keep`
   pruning manifest-first. The cross-host S3 round-trip lands here:
   backed up from a C member, restored into a Node-hosted member, and
   the reverse (depends on step 2).
8. **Docs**: `db-server.md` gains the ops; this document's "plan"
   framing is replaced by what was built, as the other docs do.
