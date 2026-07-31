# Next step: a database server, as a wasip2 command

A work brief. Unlike its siblings it opens with a decision already made
and the evidence that made it, because the alternatives were built and
measured rather than argued about.

## The decision

A long-lived server process: **one process per database directory**,
speaking **binjson over sockets**, built as a **wasm32-wasip2 command**
run by `wasmtime run`.

Not `wasi:http`. Not a fresh instance per request.

## The evidence, since it is short

Four things were built as throwaway spikes and measured. All of it is
reproducible in about ten minutes with the toolchain this repo already
pins.

**A wasi:http component in C works, and is the wrong shape.** wit-bindgen
generates C bindings for the `proxy` world, wasi-sdk 33's bundled
`wasm-component-ld` produces a component with `-mexec-model=reactor`, and
`wasmtime serve` routes to it. It can even reach the filesystem — the
`proxy` world imports no `wasi:filesystem`, but libc's file calls add the
imports and `wasmtime serve -S cli --dir` satisfies them.

But `wasmtime serve` **instantiates the component once per request**. A
counter in a static lives exactly one request:

```
in-memory counter = 1
in-memory counter = 1
in-memory counter = 1
```

For a database that is disqualifying, not inconvenient: no open
collections, no page cache, no handle table, and `Db.open()` on every
call. It also puts two writers on the same B+ tree files the moment two
requests overlap, and **wasi-filesystem has no locking** — no `flock`, no
advisory lock, nothing to check.

**A long-lived server on the same target works.** Ordinary BSD sockets,
an accept loop, a normal `main()`, `wasmtime run` instead of `serve`:

```
$ wasmtime run -S inherit-network --dir ./data::. tcpserver.wasm
listening on 127.0.0.1:8096
served 1 requests from one long-lived process
served 2 requests from one long-lived process
served 3 requests from one long-lived process
```

`socket`/`bind`/`listen`/`accept` link and run under wasip2. Under wasip1
they do not exist — `socket()` is not even declared.

**The engine already builds for the target.** All 42 sources, no changes,
`--target=wasm32-wasip2`:

```
1..90
# passed 90
```

**And the runtime is not the expensive part.** The same 90-test workload
(thousands of inserts, index builds, queries, reopens):

| Engine | Time | Bare startup |
| --- | --- | --- |
| native x86_64, no sanitizers | 0.02s | — |
| wasmtime, wasm32-wasip1 | 0.16s (0.13s precompiled) | <0.01s |
| Node's WASI host, same `.wasm` | 0.33s | 0.11s |

Startup amortises to nothing in a long-lived process. What does not
amortise is the I/O path, and that is where the hosts really differ:

```
Node/browser:  hostio.c      EM_JS(bjio_js_read)  ->  wasm -> JS -> fs.readSync
WASI/native:   bjio_posix.c  pread(...)           ->  wasm -> syscall
```

Every B+ tree page read in a JS host crosses into JavaScript and back.
The server path deletes that bridge rather than optimising it.

## Goal

A binary that serves one database directory over a socket, with no
JavaScript in the process, and a request/response layer that is testable
without a socket.

**Done when** `wasmtime run -S inherit-network --dir ./data::. nisaba.wasm`
serves inserts, finds, counts and index creation over binjson frames; the
same `dbs_handle` is driven directly by native tests with no I/O at all;
and `./wasm/build-native.sh` and the JS suite are unchanged and green.

## Shape

**One function, pure over buffers.** Everything the server decides lives
behind:

```c
int dbs_handle(dbs *s, const uint8_t *req, size_t req_len,
               uint8_t **res, size_t *res_len);
```

`main()` only frames: read a request, call this, write the response. That
split is the whole reason the transport can change later without the
protocol changing with it — sockets today, a preopened listener, a
`wasi:http` gateway in front, or a native binary, all the same function.
It is also what makes the protocol testable in `test/native/main.c`
alongside the other 90, under ASan/UBSan and on wasm32, with no process
and no port.

**binjson in, binjson out, self-delimiting.** `bj_value_size` computes a
value's total on-wire size from its header without decoding, so a reader
takes the header, learns the length, and reads the rest. No framing
format to invent and no length prefix to disagree about.

```
->  {op:"insert", coll:"users", doc:{name:"Ada", team:"core"}}
<-  {ok:1, id:ObjectId(...)}
->  {op:"find", coll:"users", filter:{team:"core"}, opts:{limit:10}}
<-  {ok:1, docs:[...]}
```

The read side needs no serialisation work at all: filters arrive as
binjson bytes and `dc_find` already **returns a binjson array of
documents**. Errors are `{ok:0, code:-N, msg:...}` with the text from
`dc_strerror`, which a native test already asserts is total.

**One process per database directory.** This is the invariant that makes
the concurrency question disappear rather than get checked for. It is not
new: `NodeFSStorageProvider` takes a per-directory advisory lock,
`db-coordinator.js` elects one tab to own the files, and OPFS enforces it
in the browser whether we like it or not. The server gets the same rule
from process ownership, which is the cheapest enforcement of the three.

**Writes go through the WAL grammar.** `dc_wal_plan` resolves a request
to id-targeted commands and `dc_wal_apply` performs one against an open
collection with no host. Using it means every mutation is replayable and
one step from replication, and it exercises the path a Raft seat will
need. The alternative — calling `dc_insert_one` directly — is simpler and
throws that away.

## Suggested staging

Each step should land green on its own.

1. **Collection by name, in C.** A session that opens a catalog through
   `bj_ns`, resolves a collection with `dc_catalog_open_plan`, opens each
   file in attach order and caches the result. This is the piece
   `install-snapshot-in-c.md` says it is blocked on and
   `completions-in-c.md` names as the reason its apply pump cannot move.
   Native tests, no server yet.
2. **`dbs_handle` and the request grammar.** Opcodes, dispatch, responses,
   refusals. Native tests driving it with binjson buffers.
3. **`main()` and the socket.** Accept loop, framing, one connection at a
   time to start. A `--wasip2` build target beside `--wasi`.
4. **A client.** The JS side already has `encode`/`decode`; `bin/db.js`
   gains a transport that talks to the socket instead of loading the
   module in-process, so the same CLI drives both.
5. **CI.** Build the wasip2 target and run a round trip against it.
   wasmtime is already installed there.

## What this costs the other targets

Nothing, which is the point of checking.

- **The engine is untouched.** It compiles for wasip2 today, unchanged.
  The server is two new files and a build target; no existing source
  changes to make it work.
- **The browser and Node builds do not see it.** `bjio_posix.c` is what a
  native or WASI target links *instead of* `hostio.c`; neither this
  library's `wasm/sources.txt` nor the browser build compiles it. The
  same exclusion already keeps the two apart.
- **It removes divergence rather than adding it.** The server path has no
  JavaScript at all, so the "logic in two places" risk this whole effort
  exists to close does not grow.
- **The one new pin** is wit-bindgen, and only if a `wasi:http` gateway
  is built later; the socket server needs nothing but the wasi-sdk
  already pinned.

## The browser question, asked honestly

It is worth asking whether multi-tab OPFS skewed the design, because it
is the most exotic constraint in the repo. The answer is: **partly, and
not in the way it looks.**

Two different constraints get conflated:

- **Exclusive sync access handles per file.** This is the multi-tab one.
  It produced `db-coordinator.js` (elect one tab, others RPC to it) and
  the handle-ownership discipline whose violation cost three red browser
  tests. But this is not a wart to be removed — it is the same
  **one writer per database directory** invariant the server now gets
  from process ownership and Node gets from an advisory lock. Multi-tab
  did not distort the design; it forced the invariant to be discovered
  early, in the environment that will not let you cheat.
- **Asynchronous opens.** This is the expensive one, and it is *not*
  multi-tab. OPFS `getFileHandle` and `createSyncAccessHandle` both
  return promises, and wasm cannot block on a promise without Asyncify or
  JSPI. That single fact forces the plan/execute split, `bj_ns.open`'s
  synchronous requirement, the pre-open scope table, and "effects cannot
  carry buffers". It would hold with exactly one tab open.

So: solving multi-tab differently would buy the removal of one JS file
and one discipline. It would not buy back plan/execute.

**What would actually simplify everything** is a different decision, and
it is a product decision rather than a technical one: make the browser a
**client** instead of a host. `src/db-remote.js` already exists, and the
parent project's gateway is already the intended deployment. If the
browser talked to a server instead of owning files, then every remaining
host is POSIX-shaped, `bj_ns.open` is just `openat`, plan/execute stops
being load-bearing, and C could own the file lifecycle end to end —
which is precisely what `install-snapshot-in-c.md` needs and cannot have.

That is a real trade, and it costs the thing this library is pitched on:
a local-first embedded database that works offline in a browser. **Do not
decide it as a side effect of building a server.** Decide it deliberately,
write down why, and if the answer is "keep the browser as a host", then
plan/execute is the price of that answer and is worth paying.

## Invariants

- **One writer per database directory.** Enforced by process ownership
  here, by an advisory lock in Node, by OPFS in the browser. Three
  enforcements of one rule; do not invent a fourth.
- **The transport frames, it does not interpret.** It has never read a
  field of a Raft message and must not start reading fields of a request
  either. Everything the server decides is behind `dbs_handle`.
- **Nothing is dropped in silence.** Every refusal is a distinct code
  with `dc_strerror` text, and the response says which.
- **All or nothing, and falsify both ways.** See
  `install-snapshot-in-c.md`'s invariants; they are the same.

## Verification

```
./wasm/build-native.sh                      # ASan/UBSan, including dbs_handle
./wasm/build-native.sh --wasi               # both WASI hosts
./wasm/build-wasm.sh && npx vitest run      # unchanged and green
wasmtime run -S inherit-network --dir ./data::. nisaba-wasip2.wasm
```

The server's own tests belong in `test/native/main.c` with the rest,
driving `dbs_handle` over buffers. A single round trip through a real
socket proves the wiring; it does not need to prove the protocol.

## Out of scope

Raft. A seat that hosts several groups is `native-composition.md`, and it
sits on top of this rather than beside it. Also out: `wasi:http` as the
database's own interface (it earns a place as a stateless gateway in
front, and for outgoing calls), TLS, auth, and anything about tenants —
`docs/replicaton-roadmap.md` step 4 records that boundary deliberately.
