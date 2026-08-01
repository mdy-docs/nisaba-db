# The database server

A process that holds one database directory and answers binjson frames
over a socket, with no JavaScript in it at all. It is the same C the
browser and Node builds link — `server/main.c` adds a `main()` and a
transport, and nothing else.

The deployment target is **`wasm32-wasip2`**, run by `wasmtime run`. The
native build exists so the same code runs wherever a `cc` does, and the
`wasm32-wasip1` build exists because it proves the transport does not
depend on sockets (preview1 has none).

## Building and running

```sh
./wasm/build-server.sh              # wasm32-wasip2  (sockets + --stdio)
./wasm/build-server.sh --native     # a native binary, same sources
./wasm/build-server.sh --wasip1     # wasm32-wasip1, --stdio only
```

The database directory is the working directory (native) or the preopen
mapped to `.` (WASI). One process per directory, for its whole lifetime:

```sh
wasmtime run -S inherit-network --dir ~/.nisaba/mydb::. \
  wasm/lib/nisaba-server-wasip2.wasm --port 8097

cd ~/.nisaba/mydb && nisaba-server --port 8097     # or natively
cd /tmp/brand-new && nisaba-server --port 8097     # an empty directory becomes a database
nisaba-server --stdio                              # frames on stdin/stdout
```

| Flag | |
| --- | --- |
| `--port N` | TCP listener on loopback (default 8097). Needs sockets: wasip2 or native |
| `--stdio` | frames on stdin/stdout. Every target, including wasip1 |
| `--order N` | B+ tree order the files were **written** with (default 32) |
| `--max-clients N` | connections held at once (default and ceiling 64) |
| `--idle-timeout N` | seconds of silence before a connection's slot is taken back (default 60; 0 disables) |

`--order` is not a preference. Open a tree with the wrong one and its
pages read as nonsense, so it has to match whatever created the files —
`bin/db.js --order N`, or a host passing `order` to `connect()`.

## Clients

`bin/db.js --server <host:port>` drives it with the same commands it uses
locally — see [`bin/db.md`](../bin/db.md).

`@mdy-docs/nisaba-db/server-client` is the JavaScript client
(`src/db-server-client.js`): a socket, the pure-JS binjson codec, and
nothing else. No WASM module, no `ready()`, no storage provider.

```js
import { connectServer } from '@mdy-docs/nisaba-db/server-client';

// Pings every 20s so the server's idle timeout does not take the slot
// back; `{ keepAliveMs: 0 }` turns that off. The timer is unref'd, so a
// script that connects, asks and finishes still exits on its own.
const db = await connectServer('127.0.0.1:8097');
const users = db.collection('users');
await users.insertOne({ name: 'Ada', team: 'core' });   // _id minted here
console.log(await users.find({ team: 'core' }).toArray());

// A large result, paged: one batch per round trip, and the cursor closes
// itself on the last one. `break` mid-scan closes it rather than leaving
// it held.
for await (const doc of users.find({}, { batchSize: 500 })) {
  process.stdout.write(doc.name + '\n');
}

await db.close();
```

## The wire

**Framing is the format's own.** A binjson value carries its total size in
its header, so a reader takes the header, asks `bj_value_size` how long
the value is, and reads the rest. There is no length prefix to disagree
about and no framing version.

A frame that cannot be *measured* ends the connection rather than
producing an error response: a reader that has lost the frame boundary
cannot resynchronise, and answering would be pretending it had. Every
other refusal is a response.

**One request object in, one response object out.** Twenty-two
operations — thirteen about a collection's documents, five about its
schema, two about a cursor, and two about neither: `listCollections` and
`ping`.

| Request | Response |
| --- | --- |
| `{op:'ping'}` | `{ok:true, pong:true}` |
| `{op:'listCollections'}` | `{ok:true, collections:[...]}` |
| `{op:'find', coll, filter, opts:{sort,projection,skip,limit,batchSize}}` | `{ok:true, docs:[...]}`, or with `batchSize`: `{ok:true, docs:[...], cursor}` |
| `{op:'getMore', cursor, opts:{batchSize}}` | `{ok:true, docs:[...], cursor}` |
| `{op:'closeCursor', cursor}` | `{ok:true, closed:true}` |
| `{op:'findOne', coll, filter}` | `{ok:true, found, doc}` |
| `{op:'count', coll, filter}` | `{ok:true, n}` |
| `{op:'distinct', coll, field, filter}` | `{ok:true, values:[...]}` |
| `{op:'insert', coll, doc, id}` | `{ok:true, result}` |
| `{op:'insertMany', coll, docs:[...], ordered}` | `{ok:true, result, attempted, upserted, errors}` |
| `{op:'bulkWrite', coll, writes:[...], ordered, now}` | `{ok:true, result, attempted, upserted, errors}` |
| `{op:'update'\|'updateMany', coll, filter, update, upsert, id, now}` | `{ok:true, result}` |
| `{op:'replace', coll, filter, doc, upsert, id}` | `{ok:true, result}` |
| `{op:'delete'\|'deleteMany', coll, filter}` | `{ok:true, result}` |
| `{op:'createCollection', coll}` | `{ok:true, created}` |
| `{op:'dropCollection', coll}` | `{ok:true, dropped}` |
| `{op:'createIndex', coll, keys, options}` | `{ok:true, name}` |
| `{op:'dropIndex', coll, index}` | `{ok:true, dropped:true}` |
| `{op:'listIndexes', coll}` | `{ok:true, indexes:[...]}` |
| `{op:'compact', coll}` | `{ok:true, result:{generation, bytesBefore, bytesAfter, bytesFreed}}` |

`result` is `{acknowledged, matchedCount, modifiedCount, deletedCount,
insertedCount, upsertedId}` for a single write, and
`{acknowledged, insertedCount, matchedCount, modifiedCount, deletedCount,
upsertedCount}` for a list of them. An upsert is counted once, as an
upsert: it is *applied* as an insert, and only the plan still knows which
it was.

**A list of writes is one round trip, and one loop — the server's.**
`insertMany` and `bulkWrite` are not the same operation. One list holds
documents and goes through a single `DC_WREQ_INSERT_MANY` plan; the other
holds writes of six different kinds, each planned and applied on its own.
What they share is how they can go wrong, so they answer in the same
shape:

```js
{ ok: true,
  result:    { acknowledged, insertedCount, matchedCount, modifiedCount,
               deletedCount, upsertedCount },
  attempted: 2,                                  // how many of the list ran
  upserted:  [ {index, id} ] | null,
  errors:    [ {index, code, msg} ] | null }
```

**A failed member is a result, not a refusal**, which is what makes
`ordered` mean anything: `false` attempts every member regardless of
earlier failures, `true` stops at the first. `attempted` is the one fact
a client cannot derive — with `ordered:true` "never tried" and "tried and
succeeded" are different answers — and everything attempted but not named
in `errors` succeeded. Inserted ids are absent and upserted ids are
present because an insert's id was chosen by whoever asked, while an
upsert's was resolved here.

In a host that shares a process with the engine, that loop is JavaScript's
(`wasm/include/db_bulk.h` says why, and it stays true there). Over a
socket the same loop would be N round trips — and a client with no engine
in it has no `dc_bulk_parse` to check a list of operations with. So the
list goes over whole and C runs it.

**The grammar is checked before any of the list runs.** Which operation
names exist and which fields each one needs is `dc_bulk_parse`'s
(`db_bulk.h`), and so are the wire's own rules — that a write which might
need an `_id` was given one, and that one which dates a field was given a
clock reading. A malformed list is refused entirely, with
`index` naming the operation that was wrong — the one refusal that names
a position, because a list of operations has positions. That ordering is
not tidiness: an unordered run is supposed to attempt every operation,
which it cannot do if operation seven is malformed in a way that only
surfaces once one through six have already landed.

A `bulkWrite` that inserts makes a missing collection, exactly as an
`insert` does; a `bulkWrite` of nothing but deletes and updates does not,
exactly as a `find` does not.

**Cursors page a scan, not a result.** `batchSize` on a find opens a
cursor: one batch comes back with an id, `getMore` asks for the next, and
`cursor` comes back **null** on the last batch — so a drained cursor
needs no `closeCursor` and costs no round trip to discover it is finished.
What the server holds between calls is a *position in a B+ tree scan*
(`dc_cursor_open`), not a materialised result, which is the difference
between paging a million documents and being sent a million documents.

A cursor belongs to the connection that opened it: another connection
asking for it gets `-46`, the same answer as for an id that never
existed, because telling those apart would tell a client about somebody
else's cursors. Cursors are released when drained, closed, when their
connection ends (however it ends), or when the server closes.

**A cursor is a snapshot**, which most databases will not give you for
free. It pins the root of the B+ tree it scans and walks nodes that
mutations never overwrite, so writes from other connections during a
scan are simply not seen, and every document is returned exactly once —
no missed documents, no duplicates, no read concern to ask for. MongoDB
makes no such promise without a snapshot read concern or a transaction:
a document can be missed or returned twice if a concurrent update moves
it in the index being walked.

The one operation that can break it is **compaction**, which rebuilds a
collection into fresh files and deletes the old ones. That is refused
while any cursor is open over a tree it would rebuild
(`DC_ERR_CURSORS_OPEN`, -49, before anything is written) — enforced in
`dc_compact_execute` rather than left to callers, because a cursor can
now outlive the request that made it.

**A sorted find cannot be batched** (`-48`). An arbitrary sort needs
every match before the first ordered result exists — the reason
`dc_cursor_open` has no sort parameter, and the reason the in-process
cursor refuses `next()` on a sorted find too. One rule, said once by each
layer, rather than a server that quietly materialises everything and
calls it a cursor. Ask without `batchSize`, or without `sort`.

**A refusal is a response.** Anything the request gets wrong — an unknown
op, a missing field, no such collection, a duplicate key — comes back as
`{ok:false, code, msg}` where `code` is a `DC_ERR_*` and `msg` is
`dc_strerror`'s text, the same sentence a native caller would get. The
connection survives it.

Two refusals are about the transport rather than a request, and both are
sent to a connection that is then closed: `code: -44` to one that arrived
when all `--max-clients` slots were taken, and `code: -45` to one whose
slot is being taken back after `--idle-timeout`. Both are in the same
shape as every other refusal, so a client reads them with the code it
already has — and both say what happened, rather than leaving a client to
infer it from a socket that closed.

**`listCollections` names no collection**, which is the question you ask
when you do not know what is there. The catalog's keys *are* the
collection names — there is no list kept beside them to fall out of step
— minus the format stamp, a reserved key no collection can be called.

**It can build a database, not just serve one.** Point the server at an
empty directory and it writes the catalog (and the format stamp) at
startup; `createCollection` makes a collection, and an `insert` into a
name that does not exist makes one too — the way it does in every other
host of this library, and in the database this is shaped after. A *read*
of a name that does not exist is refused (`-37`) rather than answered out
of a collection created on the reader's behalf: at that point it is far
more likely to be a typo than an intention.

`createIndex` plans the index — kind, name, files, all three decided by
`db_catalog.h`, as they are for every host — creates exactly those files,
**backfills it against every document already there**, and records the
definition in the catalog entry. A failed build (a missing field, an
unindexable value, a duplicate on a `unique` index) leaves the collection
without the index and the catalog untouched.

**Compaction is a request like any other.** `compact` rewrites a
collection's whole file set without its append-only history and adopts
the result — plan, stream, flip, reopen, delete — in one call
([`compaction.md`](compaction.md)). The browser needs an awaited
pre-open pass between the plan and the execute because OPFS opens are
promises; here `ns->open` really opens, so the two calls sit next to each
other with nothing between them. That difference, and only that
difference, is what the plan/execute split buys.

The session reopens the new generation for itself, so the next request is
answered from it without anyone reconnecting, and the old files are
deleted after the flip. Refused with `-49` while any cursor — anyone's —
is scanning that collection.

**Ids stay with the caller, and so does the clock.** `id` supplies the 12
bytes a write needs if it turns out to need one (an insert whose document
has no `_id`, an upsert that matched nothing). Generating one needs a
clock, which `wasm/include/db.h` keeps out of the engine deliberately, so
a write that needed an id and was not given one is refused rather than
given an id invented in C.

`now` is the same bargain: milliseconds, for an update carrying
`$currentDate`. That is not an operator the engine knows —
`upd_apply`'s table has no entry for it — because a host is supposed to
rewrite it into a concrete `$set` *before* proposing, so that what gets
written down is a date rather than a rule that would read a different
clock on replay (`db_wal.h`). This server is a host too, so it does that
rewrite, with `upd_resolve_current_date` (which also owns the rule that a
field cannot be both `$set` and dated) and with the caller's
milliseconds. An update that needed them and was not given them is
refused. A `bulkWrite` carries one reading for the whole list, so two
members dating the same field cannot disagree about when it was.

The date is therefore the *client's* clock, not the server's — the same
clock already embedded in every ObjectId this client mints. A deployment
that needs the database's own notion of now wants a gateway that stamps
it, not a clock inside the engine.

**Nothing is re-encoded on the way through.** Filters, documents and
updates are handed to the engine as the bytes they arrived as, and
results leave as the bytes the engine produced. Writes go through
`dc_wal_plan_build` + `dc_wal_apply` — the same path a replicated write
takes — so every mutation this serves is one a log could have carried.

Everything the server decides lives behind one function,
`dbs_handle(dbs*, req, req_len, dbuf *out)` (`wasm/include/db_session.h`),
which is why the protocol is tested in `test/native/main.c` over buffers
with no socket and no port.

## Invariants

- **One process per database directory.** The whole answer to concurrent
  writers: wasi-filesystem has no locking to arbitrate them with, so
  there is never more than one. The same rule OPFS enforces in the
  browser and `NodeFSStorageProvider`'s advisory lock enforces in Node.
- **Many connections, one at a time through the engine.** `poll()` over
  the listener and every accepted socket; whichever is ready is served,
  and `dbs_handle` runs to completion for one request before the next is
  looked at. There are no threads and there is no second engine, so the
  database sees the same serial stream it saw when there was one
  connection — what changed is who waits for whom. The sockets are
  non-blocking and a connection carries the bytes of a request that has
  only partly arrived and a response that has only partly gone out; a
  client that stops reading delays nobody but itself.
- **Bounded, and it says so.** `--max-clients` is a fixed table sized at
  startup, for the reason every other table here is bounded: a server
  that grows one per client has a failure mode nobody tests. Nothing is
  read from a client whose last answer has not gone out, so a pipelining
  client cannot make the server hold an unbounded number of answers for
  it either.
- **A slot has to be earned.** `--idle-timeout` closes a connection that
  has asked nothing for that long. It is aimed at the connection whose
  peer is *gone* — a crashed client, a dropped NAT mapping, a half-open
  socket — all of which look exactly like a quiet one to TCP, and all of
  which would otherwise hold a slot until the process restarts.
  `SO_KEEPALIVE` is not the answer: it defaults to hours, and the knobs
  that shorten it are per-OS and not reliably available through
  wasi-sockets. The timer measures **silence**, not connectedness — it is
  reset by a request and by its answer going out, so a client dribbling
  one byte at a time is closed like any other client that asked nothing.
  A client that wants to stay warm sends `{op:'ping'}`.
- **A clock is the transport's, not the engine's.** `server/main.c` reads
  `CLOCK_MONOTONIC`; nothing below it learns what time it is, which is
  why an insert's `_id` is still the caller's. Monotonic so that an NTP
  correction cannot take a connection's slot away, and a clock that
  cannot be read *stops* rather than jumping, so nothing times out.
- **The transport frames, it does not interpret.** `server/main.c` never
  reads a field of a request or a response.
- **Nothing is dropped in silence.** Every refusal is a distinct code
  with `dc_strerror` text.

## What it does not do yet

Stated here rather than discovered later.

- **Cursors are bounded and not timed out on their own.** Sixteen at
  once across all clients (`DBS_MAX_CURSORS`); the seventeenth is `-47`.
  An abandoned cursor is held until its connection ends, which the idle
  timeout bounds but does not target.
- **A sorted find still returns one frame.** Batching it is refused
  rather than faked, so a large sorted result is as large as it was.
- **No fairness between clients.** Ready connections are served in table
  order every time round the loop, so a client that always has a request
  waiting is always looked at before one further down. Nothing starves
  while requests are small; a stream of large ones from slot 0 would make
  slot 5 wait.
- **No database-wide `compact`.** `listCollections` makes it possible to
  build client-side, but the in-process `Db.compact()` takes
  `minBytes`/`factor`/`skipBusy` and this would not, so it would be a
  second, weaker thing wearing the same name. Compact a collection at a
  time.
- **No change streams, no `aggregate`, no `find-one-and-*` family, no
  `findByIndex`/`pruneExpired`.** Each is an op in
  `wasm/src/db_request.c` plus a method in the client — except `watch`,
  which also needs frames the client did not ask for, and this protocol
  has no shape for those. `dump` and `restore` both work today.
- **Compaction is per collection, and per request.** No `compact()`
  across a whole database (that needs collection listing), and no
  scheduler: the engine runs no timers, so *when* to compact stays with
  whoever is driving (`docs/compaction.md`).
- **No TLS, no auth, no tenants.** Loopback only. Those belong to the
  gateway in front, not to the database
  (`docs/replicaton-roadmap.md` step 4 records that boundary).

## Why not `wasi:http`

`wasmtime serve` **instantiates the component once per request** —
measured, with a counter in a static that read `1` on every request. For
a database that is disqualifying rather than inconvenient: no open
collections, no page cache, no handle table, and a fresh open on every
call. It also puts two writers on the same B+ tree files the moment two
requests overlap, and wasi-filesystem has no locking to stop them.

A long-lived `wasmtime run` process with ordinary BSD sockets has none of
those problems, and the runtime is not the expensive part: the same
90-test workload takes 0.02s native, 0.16s under wasmtime, 0.33s under
Node's WASI host — and startup amortises to nothing in a process that
stays up. What does not amortise is the I/O path, and that is where the
hosts really differ:

```
Node/browser:  hostio.c      EM_JS(bjio_js_read)  ->  wasm -> JS -> fs.readSync
WASI/native:   bjio_posix.c  pread(...)           ->  wasm -> syscall
```

Every B+ tree page read in a JS host crosses into JavaScript and back.
This path deletes that bridge rather than optimising it.

`wasi:http` still earns a place as a **stateless gateway in front** of
this, and for outgoing calls. It is not the database's own interface.

## Tests

`test/db.server.test.js` drives a real process over a pipe and over a
socket, twice over — native, and wasip2 under wasmtime — with a client
that shares no code with the server, against databases the JavaScript
implementation wrote. CI builds both artifacts and sets
`NISABA_SERVER_TESTS=required`, so those suites cannot quietly stop
running.
