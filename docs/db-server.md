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
nisaba-server --stdio                              # frames on stdin/stdout
```

| Flag | |
| --- | --- |
| `--port N` | TCP listener on loopback (default 8097). Needs sockets: wasip2 or native |
| `--stdio` | frames on stdin/stdout. Every target, including wasip1 |
| `--order N` | B+ tree order the files were **written** with (default 32) |
| `--max-clients N` | connections held at once (default and ceiling 64) |

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

const db = await connectServer('127.0.0.1:8097');
const users = db.collection('users');
await users.insertOne({ name: 'Ada', team: 'core' });   // _id minted here
console.log(await users.find({ team: 'core' }).toArray());
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

**One request object in, one response object out.** Ten operations:

| Request | Response |
| --- | --- |
| `{op:'find', coll, filter, opts:{sort,projection,skip,limit}}` | `{ok:true, docs:[...]}` |
| `{op:'findOne', coll, filter}` | `{ok:true, found, doc}` |
| `{op:'count', coll, filter}` | `{ok:true, n}` |
| `{op:'distinct', coll, field, filter}` | `{ok:true, values:[...]}` |
| `{op:'insert', coll, doc, id}` | `{ok:true, result}` |
| `{op:'update'\|'updateMany', coll, filter, update, upsert, id}` | `{ok:true, result}` |
| `{op:'replace', coll, filter, doc, upsert, id}` | `{ok:true, result}` |
| `{op:'delete'\|'deleteMany', coll, filter}` | `{ok:true, result}` |

`result` is `{acknowledged, matchedCount, modifiedCount, deletedCount,
insertedCount, upsertedId}`.

**A refusal is a response.** Anything the request gets wrong — an unknown
op, a missing field, no such collection, a duplicate key — comes back as
`{ok:false, code, msg}` where `code` is a `DC_ERR_*` and `msg` is
`dc_strerror`'s text, the same sentence a native caller would get. The
connection survives it.

One refusal is about the transport rather than a request, and arrives
before the client has asked anything: `code: -44`, sent to a connection
that arrived when all `--max-clients` slots were taken, which is then
closed. It is in the same shape as every other refusal, so a client reads
it with the code it already has.

**Ids stay with the caller.** `id` supplies the 12 bytes a write needs if
it turns out to need one (an insert whose document has no `_id`, an
upsert that matched nothing). Generating one needs a clock, which
`wasm/include/db.h` keeps out of the engine deliberately, so a write that
needed an id and was not given one is refused rather than given an id
invented in C.

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
- **The transport frames, it does not interpret.** `server/main.c` never
  reads a field of a request or a response.
- **Nothing is dropped in silence.** Every refusal is a distinct code
  with `dc_strerror` text.

## What it does not do yet

Stated here rather than discovered later.

- **No fairness between clients.** Ready connections are served in table
  order every time round the loop, so a client that always has a request
  waiting is always looked at before one further down. Nothing starves
  while requests are small; a stream of large ones from slot 0 would make
  slot 5 wait.
- **Ten operations.** No index management, compaction, change streams,
  collection listing, or the `find-one-and-*` family. Each is an op in
  `wasm/src/db_request.c` plus a method in the client.
- **No cursors.** A `find` returns every match in one frame.
- **No idle timeout.** A connection that says nothing holds its slot
  until the client goes away.
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
