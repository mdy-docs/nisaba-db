# Next step: databases in the server

A work brief, written to be handed to someone who has not been following
the effort. It says what to build, what already exists so it is not built
twice, the shape the answer has to take and why, and what must still be
true afterwards.

Its first deliverable is a DECISION, in writing, before any of it is
implemented. That decision is in "The choice to make first" below.

## Where this sits

The library already has the MongoDB shape and it is easy to miss:
`connectClient(provider)` returns a `Client`, and `client.db(name)` calls
`provider.subProvider(name)` — a real OPFS subdirectory, a real
filesystem directory under Node, an independent file map in memory. Two
names never share a catalog or a collection file
(`wasm/nisaba-wasm.js`). `bin/db.js` names a database that way on every
local run.

**The C server does not have it.** `dbs_open` opens the preopen `"."` as
ONE database, and the wire's thirty-one ops carry `coll` with no `db`
beside it. So `nisaba-server` serves one database directory, and an
instance with several databases in it is several processes.

That is the gap this closes: **one executable, one root folder, database
folders under it, each with its own catalog and its own collection
files** — the same layout `Client.db(name)` already writes, served by the
same binary that already serves one of them.

**This is not tenancy.** Tenancy is a layer above this repository and
stays there. The retired `native-composition.md` conflated the two — it
asked for N independent Raft clusters seated in one process because
roadmap step 4 wanted one group per tenant — and reading it as "several
databases in one instance" is what made it look relevant. It was not the
same thing, and this brief is the thing that was actually wanted.

## Goal

`nisaba-server` serves an INSTANCE: one process, one root directory, many
databases beneath it.

**Done when** two databases in one server cannot see each other's
collections, `bin/db.js --server` names a database the way it already
does locally, and — with `--raft` — a write to either one goes through the
log and lands on every member.

## The choice to make first

**One log for the instance, or one log per database?** Everything else
here is mechanical; this is not. Decide it, write down why, and put that
paragraph in this file before writing code.

**(a) One log for the instance.** Entries carry the database as well as
the collection. One leader, one election, one member set, one failover
story for the whole executable. Raft does not change AT ALL —
`server/replica.c` and `server/peers.c` stand exactly as they are — and
what grows a database axis is the SESSION.

- Costs: one database's write rate is every database's, a slow apply
  blocks all of them, and no database can be placed on a different set of
  machines from its neighbours.

**(b) One log per database.** What `src/raft-host.js` does (one WalDb =
one log = one group) and what the retired brief specified: N nodes, N
logs, N leaders in one process, multiplexed over one transport with a
`{group, msg}` envelope and one tick loop, with idle groups quiesced.

- Costs: a seat, a group registry, the envelope in C, quiescence — and N
  leaders means a client has to find the leader PER DATABASE rather than
  per server, which changes the redirect that `--raft` just made real.

**The recommendation is (a)**, and the reason is that (b)'s justification
was tenancy: independently placed, mostly-idle databases, where
quiescence is the design rather than an optimization. With tenancy out of
scope, an always-on instance quiesces nothing and places nothing. (b) can
also be built on top of (a)'s naming later without redoing it; the
reverse is not true.

### A consequence worth seeing before you choose

`dbs_applied_floor` is the replay floor — the highest index this database
has applied, and the number a restarting replica resumes from. With ONE
log for the instance it becomes the floor across EVERY database, because
apply is strictly ordered across the whole log and the max is the applied
prefix only if the max is taken over everything the log wrote.

This is not hypothetical. That function has already shipped wrong once
and been fixed: it asked `dbs_collection` to open each collection while
passing NULL for the handle, which that function refuses outright, so the
floor was always zero — and a floor of zero replays a prefix that was
already applied, which hands a collection an applied index it has already
passed, which is refused, which is not a DETERMINISTIC refusal, so the
replica halts on the way up. It is the single most load-bearing number in
`server/replica.c` and it has to move with the log's shape rather than
after it.

## Do not build these — they exist

| Piece | Where |
| --- | --- |
| Database-name validation | `dc_check_db_name` (`wasm/include/db_validate.h`) — and a platform's extra rules compose on top of it, which the Node provider already does |
| The client shape, in JS | `Client` / `connectClient` (`wasm/nisaba-wasm.js`): one root provider, one real subdirectory per name |
| Naming files inside one database | `db_names.h` — collections, indexes, journals, compaction generations |
| A session over one database | `db_session.h`'s `dbs`; `dbs_open` takes a `bj_ns`, and a `bj_ns` is one directory |
| The client, when the wire grows a field | `src/db-server-client.js` — `WIRE_OPS` in one place |
| Everything replication does | `server/replica.c`, `server/peers.c` — untouched under (a) |

## What has to be built

1. **A database axis on the wire.** Two shapes: a `db` field on every op,
   or a session-level selection that later requests inherit.
   Recommendation: **a field**. A session-level "use" makes every
   request's meaning depend on an earlier one, and the server is
   deliberately stateless per request apart from cursors and change
   streams — which are the two things that would then have to remember
   which database they were opened against anyway. Whichever you pick,
   ONE place decides what a request naming no database means, and it
   should be a named default or an explicit refusal rather than "whatever
   was used last".
2. **One namespace per open database.** A `bj_ns` is one directory, so a
   second database is a second `openat` and a second `bjns_posix_open`.
   Bounded with an explicit refusal at the cap, like every other table in
   this server, because the worst case a reader has to reason about is
   that number times what one open database costs.
3. **Discovering what is there.** `listDatabases` needs a directory
   listing, and **`bj_ns` deliberately cannot produce one** — OPFS
   enumeration is asynchronous, so `bjns.h` passes listings in rather
   than offering them. Nothing in the server reads a directory today. So
   the server does it itself with POSIX/WASI, which is correct rather
   than a workaround: a browser will never run this, and that is exactly
   why it is not the namespace's job.
4. **The database axis through the session.** Cursors, change streams,
   the applied index, compaction and the orphan sweep are all keyed by
   collection name today; each becomes `(db, coll)`. Change streams are
   the one that fails silently rather than loudly: `dbs_watched` and
   `dbs_emit` match on the collection name alone, so two databases with a
   `users` in each would cross-deliver events.
5. **Whatever the choice above committed you to.** Under (a) that is the
   database in the logged command and the floor across every database.

## Invariants that must hold

- **One writer per ROOT.** The process owns the preopen and everything
  beneath it. That is the same rule as before with a wider scope, and it
  is still the whole answer to concurrent writers, because
  wasi-filesystem has no locking to arbitrate them with.
- **Two databases never share a file.** That is the guarantee
  `Client.db(name)` makes with a real subdirectory, and the server must
  make the same one. A prefix scheme inside one flat directory would be a
  second naming convention living beside `db_names.h`, and two naming
  conventions is one too many.
- **One owner per fact.** A database's name rules are
  `dc_check_db_name`'s; its file names are `db_names.h`'s; what a
  collection is called on the wire is `WIRE_OPS`'s.
- **Bounded, and it says so.**
- **A directory that is a database today keeps working.** This is not a
  migration, and an existing single-database directory must not be
  silently reinterpreted.
- **Nothing is dropped in silence.**
- **Falsify both ways.** Break the fix, watch the specific test fail,
  restore it, and say so in the commit message.

## Known hazards

- **The preopen changes meaning.** `"."` was a database and becomes a
  directory of databases. Decide explicitly what an existing directory
  full of `coll-*.bj` means when a server is pointed at it as a root, and
  say so in `docs/db-server.md` — guessing here silently opens the wrong
  files.
- **A directory listing is not a catalog.** A name on disk is not proof
  of a database, and a database created but never written to is a
  directory with nothing in it.
- **Under (a), one apply pump serves every database.** A deterministic
  failure in one is still a result, but a HALT in one halts all of them —
  which is an argument for (a) being honest about what it couples, not
  against it.
- **A change stream registered before the axis existed has no database.**
  If streams are persisted or resumed anywhere, that is a format
  question, not just a field.

## Ordering

Independent of
[`joining-a-native-cluster.md`](joining-a-native-cluster.md); both sit on
the server that exists. Doing this one first makes that one simpler,
because "is a join per instance or per database" only has to be asked if
this brief answers (b).

## Verification

```
./wasm/build-native.sh                 # ASan/UBSan
./wasm/build-server.sh --native        # and --wasip1, --wasip2
./wasm/build-wasm.sh && npx vitest run
```

Beside the suites in `test/db.server.test.js` that already spawn the
binary: two databases in one server that cannot see each other's
collections or each other's change events, `bin/db.js --server` naming
one of them, the JS engine reading the same root afterwards through
`connectClient`, and — with `--raft` — a write to each landing on all
three members.

## Out of scope

Tenancy, and everything that follows from it: per-database placement,
per-database member sets, quiescence. Cross-database transactions, of
which there are none anywhere in this repository — a `bulkWrite` is a
list of writes, not a transaction. The HTTP surface
([`http-front-end.md`](http-front-end.md)).
