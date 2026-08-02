# Next step: databases in the server

A work brief, written to be handed to someone who has not been following
the effort. It says what to build, what already exists so it is not built
twice, the shape the answer has to take and why, and what must still be
true afterwards.

Both of its design decisions are made and are recorded below with the
reasons: the client shape, and **one log per instance**. What is left is
implementation and the small choices inside it, which the relevant
section names as it goes.

## The shape, stated first

One connection to an INSTANCE; many databases reached over it, switched
between at will:

```js
const client = await MongoClient.connect("mongodb://localhost:27017");
const analyticsDb = client.db("analytics");
const billingDb   = client.db("billing");
```

That is the target for the native C server AND for the WASM engine in the
browser, and both halves of it matter:

- **`client.db(name)` is a client-side handle**, not a round trip and not
  a mode. Nothing is sent when you call it.
- **The CONNECTION is not stateful about which database.** Every
  operation names its own, which is the only reason two handles can share
  one connection pool and be switched between freely.

The second point decides something this brief would otherwise have had to
argue about, so it is settled here rather than below: **the database is a
field on every operation, not a session-level selection.** A "use"
request would make every later request's meaning depend on an earlier
one, which is exactly the thing `client.db()` is not.

## Where this sits

The library already has this shape in process, and it is easy to miss:
`connectClient(provider)` returns a `Client`, and `client.db(name)` calls
`provider.subProvider(name)` — a real OPFS subdirectory, a real
filesystem directory under Node, an independent file map in memory. Two
names never share a catalog or a collection file
(`wasm/nisaba-wasm.js`). `src/db.js` exports both, `bin/db.js` names a
database that way on every local run, and `src/db-worker.js` already
hands a browser page a `client.db(name)`.

So the browser is not waiting on a decision — it is waiting on the ops
the instance needs (listing databases, dropping one) and on the server
learning the same shape.

**The C server does not have it.** `dbs_open` opens the preopen `"."` as
ONE database; the wire's thirty-one ops carry `coll` with no `db` beside
it; and `connectServer(address)` returns a `Db` directly, with no client
above it. So `nisaba-server` serves one database directory, an instance
with several databases in it is several processes, and there is nowhere
for `client.db(name)` to exist.

That is the gap this closes: **one executable, one root folder, database
folders under it, each with its own catalog and its own collection
files** — the same layout `Client.db(name)` already writes, served by the
same binary that already serves one of them, over one connection.

**This is not tenancy.** Tenancy is a layer above this repository and
stays there. The retired `native-composition.md` conflated the two — it
asked for N independent Raft clusters seated in one process because
roadmap step 4 wanted one group per tenant — and reading it as "several
databases in one instance" is what made it look relevant. It was not the
same thing, and this brief is the thing that was actually wanted.

## Goal

`nisaba-server` serves an INSTANCE: one process, one root directory, many
databases beneath it.

**Done when** one connection to a server can hold `client.db("analytics")`
and `client.db("billing")` at the same time and neither can see the
other's collections or change events, `bin/db.js --server` names a
database the way it already does locally, the browser reaches the same
shape through the same `Client`, and — with `--raft` — a write to either
one goes through the log and lands on every member.

## The replication shape: ONE LOG PER INSTANCE

Decided. Entries carry the database as well as the collection; there is
one log, one leader, one election, one member set and one failover story
for the whole executable.

**Raft does not change at all.** `server/replica.c` and
`server/peers.c` stand exactly as they are. What grows a database axis is
the SESSION, which is the half of this brief that was always going to be
work.

The alternative was one log per database — what `src/raft-host.js` does
(one WalDb = one log = one group), and what the retired
`native-composition.md` specified: N nodes, N logs, N leaders in one
process, multiplexed with a `{group, msg}` envelope, idle groups
quiesced. Two reasons it is not this.

Its justification was **tenancy**: independently placed, mostly-idle
databases, where quiescence is the design rather than an optimization.
Tenancy is a layer above this repository. An always-on instance quiesces
nothing and places nothing, so the whole apparatus would be paid for and
unused.

And it fights the **connection shape** above. One connection holds many
databases, and under `--raft` only the leader takes a write. With one log
that is one refusal for the whole connection: this server leads, or that
one does. With a log per database, leadership is per database — one
socket could be leader for `analytics` and follower for `billing`, and a
caller would need a redirect table keyed by database and connections to
several servers at once to satisfy a single `client` object. Every
caller pays that, [`http-front-end.md`](http-front-end.md) included, for
a property that is out of scope.

**What this costs, accepted deliberately:** one database's write rate is
every database's, and one apply pump serves all of them — a deterministic
failure in one is still a result, but a HALT in one halts every database
in the instance. Per-database placement is not available and is not
planned.

**What it does not foreclose:** a log per database can be built on top of
this naming later without redoing it. The reverse is not true, which is
the other reason to start here.

### The consequence that follows immediately

`dbs_applied_floor` is the replay floor — the highest index this database
has applied, and the number a restarting replica resumes from. With one
log it becomes the floor across EVERY database, because apply is strictly
ordered across the whole log and the max is the applied prefix only if
the max is taken over everything the log wrote. A floor computed per
database, or over only the databases that happen to be open, is a
replica that replays entries it has already applied.

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
| The client shape, in process | `Client` / `connectClient` (`wasm/nisaba-wasm.js`): one root provider, one real subdirectory per name, `Db`s cached per name. Exported by `src/db.js`; `src/db-worker.js` already serves a browser page from it |
| One connection carrying everything | `Connection` (`src/db-server-client.js`) — one socket, answers in request order. `client.db(name)` needs no second one |
| Naming files inside one database | `db_names.h` — collections, indexes, journals, compaction generations |
| A session over one database | `db_session.h`'s `dbs`; `dbs_open` takes a `bj_ns`, and a `bj_ns` is one directory |
| The client, when the wire grows a field | `src/db-server-client.js` — `WIRE_OPS` in one place |
| Everything replication does | `server/replica.c`, `server/peers.c` — untouched: one log per instance is a session change, not a Raft one |

## What has to be built

1. **A `db` field on every operation.** Settled above, not open. What is
   still to decide is the small part: what a request naming no database
   means. A named default and an explicit refusal are both defensible;
   "whatever was used last" is not, because that is the connection state
   the shape exists to avoid. Decide it in ONE place —
   `wasm/src/db_request.c`'s op dispatch — and give the client the same
   answer.
2. **A client over the wire.** `connectServer(address)` returns a `Db`
   today, and every caller in the repository expects that
   (`test/db.server.test.js`, `bin/db.js --server`). The instance shape
   needs a client above it whose `db(name)` hands back a `Db` bound to
   the same `Connection` — the in-process `Client` with a socket where
   the provider is. Keep the single-database entry point: `connect` and
   `connectClient` already sit side by side in process, and the wire
   should mirror that rather than break its callers.
3. **One namespace per open database.** A `bj_ns` is one directory, so a
   second database is a second `openat` and a second `bjns_posix_open`.
   Bounded with an explicit refusal at the cap, like every other table in
   this server, because the worst case a reader has to reason about is
   that number times what one open database costs. And the cap is a
   ceiling on what is OPEN, not on what exists: a database nobody has
   asked for costs a directory and nothing else, and one that has gone
   quiet should be closeable — which the in-process `Client` never has to
   do, because it caches a `Db` per name forever and a page does not have
   a thousand of them.
4. **`listDatabases` and `dropDatabase`, which exist nowhere.** Neither
   name appears anywhere in this repository, in any commit, on any
   branch. It is an easy thing to misremember, because the COLLECTION
   pair is everywhere — `Db.listCollections` / `Db.dropCollection`, both
   on the wire, both in `bin/db.js`, both in `docs/db-example.js`. The
   database-level pair has never been written.

   They arrive here, and in BOTH halves at once: the wire ops and the
   `Client` methods, same names, same meanings, because a `Client` method
   that only works over a socket has missed the point of there being one
   `Client`.

   Neither is free at the provider layer, and that is the part to scope
   before starting. A provider today has `listFiles()`, which filters to
   `kind === 'file'` (OPFS) and `isFile()` (Node) and therefore cannot
   see a database at all, and `subProvider(name)`, which CREATES on
   demand and so cannot answer "does this one exist". There is
   `deleteFile(name)` and nothing that removes a subdirectory. So all
   three providers — OPFS, Node, memory — grow two capabilities, and
   `dropDatabase` has to decide what it means for the Node provider's
   per-directory lock and for a `Db` the `Client` still has cached.

   In C, listing needs a directory listing and **`bj_ns` deliberately
   cannot produce one** — OPFS enumeration is asynchronous, so `bjns.h`
   passes listings in rather than offering them. Nothing in the server
   reads a directory today; it will need POSIX/WASI `readdir`. That
   split is correct rather than a workaround, and it is exactly why
   listing is each host's own and not the namespace's.
5. **The database axis through the session.** Cursors, change streams,
   the applied index, compaction and the orphan sweep are all keyed by
   collection name today; each becomes `(db, coll)`. Change streams are
   the one that fails silently rather than loudly: `dbs_watched` and
   `dbs_emit` match on the collection name alone, so two databases with a
   `users` in each would cross-deliver events.
6. **The database in the logged command, and the floor across all of
   them.** `dc_wal_plan_build` names a collection; a command that does
   not also name its database is a command a replica cannot apply.
   `dbs_applied_floor` becomes the instance's, per the section above.
   Both are the whole of what one-log-per-instance costs in C.

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
- **The in-process `Client` caches a `Db` per name and never lets one
  go.** That is right for a browser page with three databases and wrong
  for a server with a thousand, which needs to close an idle one to get
  its file handles back. Do not copy the caching without deciding that.
- **`dropDatabase` removes a DIRECTORY, and `bj_ns` has no such verb.**
  `bj_ns.remove` names one file and MAY BE DEFERRED (`bjns.h`), which is
  why nothing may order a create against a remove. Dropping a whole
  database is the host's, and under `--raft` it is also a logged command
  that every replica must perform identically — decide what it means when
  a cursor is open on a collection inside it, the same way compaction
  already had to.

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
binary: **two databases held at once over ONE connection**, which is the
shape itself and the thing a per-connection "use" would fail — writing to
each in turn, interleaved, and neither seeing the other's collections or
change events. Then `bin/db.js --server` naming one of them, and the JS
engine reading the same root afterwards through `connectClient`, which is
the claim this repository rests on: three implementations, one set of
files. With `--raft`, a write to each landing on all three members.

The browser half has its own bar and it is the existing one: whatever
`Client` grows here is exercised in `test/db.*.test.js` under Node with
`MemoryStorageProvider` and `NodeFSStorageProvider`, because a `Client`
method that only works over a socket has missed the point.

## Out of scope

Tenancy, and everything that follows from it: per-database placement,
per-database member sets, quiescence. Cross-database transactions, of
which there are none anywhere in this repository — a `bulkWrite` is a
list of writes, not a transaction. The HTTP surface
([`http-front-end.md`](http-front-end.md)).
