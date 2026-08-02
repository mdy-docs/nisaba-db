# Deployment shapes

The five ways this database is meant to run. One document, because the
shapes share almost everything — the same C engine, the same file
format, the same operations — and differ only in *who owns the files*
and *who talks to whom*. Getting that difference written down in one
place is what stops each shape growing its own answer to a question the
others already answered.

Each shape says what runs where, what is built, and what is not. Where a
decision is genuinely open it says so, rather than describing an
intention as though it were code.

## At a glance

| # | Shape | State |
| --- | --- | --- |
| 1 | Browser, WASM, JS bridge | **Built.** The bridge is not minimal, and structurally cannot be — see below |
| 2 | Browser, many tabs, one owner | **Built.** Elected owner, others RPC to it |
| 3 | CLI, open then close | **Built**, as Node + WASM. A native CLI does not exist |
| 4 | One persistent server process | **Built**, speaking binjson frames over TCP. **No HTTP** |
| 5 | Several server processes, Raft | **Built in Node.** The C server cannot replicate |

The single largest gap is that **4 and 5 are two different programs
today**: shape 4 is `server/main.c`, a C process with no JavaScript in
it; shape 5 is a Node process hosting the WASM engine. They share the
engine and the format, not the executable. Whether they converge, and in
which direction, is the open question this document ends on.

---

## The parts, once

Every shape is assembled from these. Nothing below is per-shape.

```
   ┌──────────────────────────────────────────────────────────────┐
   │ C ENGINE                                     ~15,700 lines   │
   │   documents, queries, updates, indexes (equality/text/geo),  │
   │   the catalog, compaction, the WAL command grammar and its   │
   │   planner/applier, the Raft state machine                    │
   │                                                              │
   │   It opens no file. It reads no clock. It has no socket.     │
   └──────────────────────────────────────────────────────────────┘
              ▲                        ▲                    ▲
              │ wasm exports           │ dbs_handle         │ raft_node
   ┌──────────┴─────────┐   ┌──────────┴────────┐  ┌────────┴────────┐
   │ JS DRIVER          │   │ REQUEST GRAMMAR   │  │ RAFT HOST       │
   │ wasm/nisaba-wasm.js│   │ wasm/src/         │  │ src/raft.js     │
   │ Db · Collection ·  │   │   db_request.c    │  │ timers, socket, │
   │ ChangeStream       │   │   db_session.c    │  │ the outbox pump │
   │ + storage providers│   │ 31 ops over       │  │                 │
   │ + plan/execute     │   │ binjson objects   │  │                 │
   └────────────────────┘   └───────────────────┘  └─────────────────┘
        shapes 1, 2, 3, 5          shape 4              shape 5
```

**Two drivers, on purpose.** The same thirty-one operations exist twice:
once as `Collection`/`Db` in JavaScript, once as the request grammar in
C. That is duplication, and it is not an accident — see shape 1's
constraint, which forces it. It is the largest standing cost in the
codebase and is worth re-reading whenever it looks avoidable.

**One writer per database directory**, in every shape. The browser gets
it from OPFS's exclusive sync access handles, Node from an advisory
`.nisaba-lock`, the server from process ownership of its directory.
Three mechanisms, one invariant.

---

## 1. Browser, WASM, JS bridge

The database *in* the page: an OPFS-backed local store, the thing you
reach for instead of IndexedDB. No network, works offline.

```
   ┌─ browser tab ──────────────────────────────────────┐
   │  application code                                  │
   │        │  await users.insertOne({...})             │
   │        ▼                                           │
   │  ┌─ Worker ────────────────────────────────────┐   │
   │  │  wasm/nisaba-wasm.js   the JS bridge        │   │
   │  │      │                                      │   │
   │  │      │  1. ask C which files this needs     │   │
   │  │      │  2. await OPFS handles for them      │   │
   │  │      │  3. call C, which now cannot block   │   │
   │  │      ▼                                      │   │
   │  │  nisaba.wasm  (the C engine)                │   │
   │  │      │  read/write at offsets               │   │
   │  │      ▼                                      │   │
   │  │  OPFS sync access handles                   │   │
   │  └─────────────────────────────────────────────┘   │
   └────────────────────────────────────────────────────┘
```

**Built.** `connect(new OPFSStorageProvider(dir))` gives the full driver
surface. A Worker is not a design choice: `createSyncAccessHandle()` is
a blocking call the spec forbids on the main thread, and real Chromium
does not expose it there. `nisaba/remote` exists for a main thread that
only wants to marshal calls to that Worker.

### The bridge is not minimal, and cannot be

`wasm/nisaba-wasm.js` is ~3,900 lines: roughly 2,000 of codec, handle
table and glue, then `Collection` (~1,500), `Db` (~270) and the storage
providers (~150).

The reason is one fact. OPFS `getFileHandle()` and
`createSyncAccessHandle()` **return promises**, and WASM cannot block on
a promise without Asyncify or JSPI. So C can never open its own file.
Everything follows:

- `bj_ns.open` must be synchronous, so every file a call might touch is
  opened **before** the call — C plans the file set, JS executes the
  plan, C then runs unable to block.
- Anything that decides *which* files an operation needs therefore lives
  above that boundary — which is most of what a driver is.
- Hence the JS driver, and hence the second copy of the operation set in
  C for shape 4, which has no such constraint.

This is recorded as a settled decision in
[`steps/README.md`](steps/README.md): the browser stays a host, and
plan/execute is permanent. Every other host — Node, native, WASI — pays
a discipline it does not need so that this one can exist at all.

**Open question.** "Minimal JS bridge" as a goal is not currently met and
cannot be met while that constraint holds. If minimality matters more
than the browser-as-host, the lever is JSPI (Chrome 137+, not Safari) or
Asyncify (a size and speed cost across every host). Neither is planned.

**Verified by** `test/db.test.js` (166 cases, Node), plus real-browser
runs in CI for compaction and the coordinator via Playwright.

---

## 2. Browser, many tabs, one owner

The same database with several tabs open. OPFS sync access handles are
exclusive per file *origin-wide*, so "just open it in both" is not
available — one context opens the files and the rest talk to it.

```
   tab A ───┐                        ┌─── tab C
            │   navigator.locks      │
   ┌────────▼───────┐  ┌─────────────▼──┐  ┌────────────────┐
   │ Worker A       │  │ Worker B       │  │ Worker C       │
   │ connectShared  │  │ connectShared  │  │ connectShared  │
   │  ↳ FOLLOWER    │  │  ↳ LEADER      │  │  ↳ FOLLOWER    │
   │    proxies ────┼──┼─▶ real Db      │◀─┼──── proxies    │
   └────────────────┘  │    ↳ OPFS      │  └────────────────┘
        BroadcastChannel└────────────────┘
```

**Built** — `src/db-coordinator.js`, `connectShared(name, ...)`. Election
is `navigator.locks.request`: whoever's callback fires holds the lock and
is the leader until its context dies, at which point the lock releases
and another takes over. RPC between contexts is `BroadcastChannel`.

**Not a SharedWorker**, deliberately: iOS Safari has never supported one,
and this project already targets Safari 16.4+ for OPFS.

Note what this is *not*: it is not replication. There is one copy of the
data and one writer; the others are clients over `postMessage`. It is the
same "one writer per directory" invariant the other shapes get from a
lock or from owning a process.

**Verified by** `test/db-coordinator.test.js` (15 cases) and
`test/db-coordinator.browser.test.js` (6, in real Chromium — election and
handover cannot be faked in Node).

---

## 3. CLI, open then close

A shell tool: open the directory, do one thing, close.

```
   $ db mydb insert users '{"name":"Ada"}'
   ┌──────────────────────────────────────────┐
   │ node bin/db.js                           │
   │   nisaba-wasm.js  →  nisaba.wasm         │
   │   NodeFSStorageProvider  →  node:fs      │
   │   .nisaba-lock  (advisory, per directory)│
   └──────────────────────────────────────────┘
             ~/.nisaba/mydb/*.bj
```

**Built** — `bin/db.js`, documented in [`../bin/db.md`](../bin/db.md).
Collections, indexes, CRUD, dump/restore, compaction. `flush()` is a real
`fsyncSync`, and file creation fsyncs the parent directory too.

The same binary is also a *client*: `db --server host:port <command>` runs
every command against shape 4 instead, over the socket, with no engine in
the process.

**It is Node plus WASM, not a native executable.** `nisaba-server` is a
native binary, but it is a server: it has no CLI verbs, and `--stdio`
speaks the wire protocol rather than a command line.

**Open question.** Is a dependency-free native CLI wanted — `db` as a
single file with no Node — or is Node an acceptable requirement for the
shell tool? Nothing is blocked on this; it has simply never been asked.

---

## 4. One persistent server process

A long-lived process owning one database directory, answering clients
over a socket.

```
   ┌─ client ────────────┐        ┌─ client ────────────┐
   │ src/db-server-client│        │ bin/db.js --server  │
   │ no WASM, no engine  │        │ (same client)       │
   └──────────┬──────────┘        └──────────┬──────────┘
              │   binjson frames, TCP loopback           
              └──────────────┬───────────────┘
                  ┌──────────▼───────────────────────────┐
                  │ nisaba-server        server/main.c   │
                  │   poll() over the listener + clients │
                  │   dbs_handle: 31 ops, one at a time  │
                  │   cursors · change streams · sessions│
                  │   NO JavaScript in this process      │
                  └──────────┬───────────────────────────┘
                             │ pread/pwrite
                        one database directory
```

**Built**, and documented in [`db-server.md`](db-server.md). Three build
targets from the same sources: native, `wasm32-wasip1` (`--stdio` only —
preview1 has no sockets at all, which is what proves the transport is
separable), and `wasm32-wasip2` under `wasmtime`, the deployment target.

Thirty-one operations — everything the in-process `Collection`/`Db` have
except `storageEstimate`. Bounded connection table, idle reclamation,
paged cursors, change streams, compaction, DDL.

**Verified by** `test/db.server.test.js` (52 cases), run twice over in
CI — against the native binary and against the wasip2 command under
wasmtime — driven by the shipped client, not by test-local protocol code.

### What this shape does not have

- **No HTTP.** It speaks binjson frames on a TCP socket. An HTTP
  envelope was built (parser, listener, sessionless refusals, tests) and
  then **reverted at your instruction** — "http handling is just another
  moving piece we have to maintain". The work is preserved on the branch
  `wip/http-transport` and is recoverable in full.

  This contradicts the use case as stated ("provide a cli and http
  server"), and is the first question below.

- **No TLS, no auth, no tenants.** Loopback only. The README places
  REST/WebSocket gateways and the control plane in the parent project,
  not here.

- **One request at a time.** Many connections, one engine; `dbs_handle`
  runs to completion before the next request is looked at. Deliberate —
  it is what makes the database see the same serial stream it saw with
  one connection — but it is a throughput ceiling, not a detail.

---

## 5. Several server processes, replicated by Raft

A cluster: several processes each holding a copy, a leader accepting
writes, followers converging, failover when one dies.

```
   ┌─ node 1 (LEADER) ─────┐   ┌─ node 2 ──────┐   ┌─ node 3 ──────┐
   │ Raft: log, elections  │   │               │   │               │
   │ state machine (C)     │◀─▶│  AppendEntries│◀─▶│               │
   │ apply → the database  │   │  InstallSnaps │   │               │
   └───────────────────────┘   └───────────────┘   └───────────────┘
              ▲
              │  writes only to the leader
        clients / gateway
```

**Built — in Node.** Every piece exists and is tested:

| Piece | Where | Tests |
| --- | --- | --- |
| Raft state machine (roles, terms, timers, commit arithmetic, the two hot RPC handlers) | **C** — `wasm/include/raft_node.h` | via the host |
| Raft host (timers, sockets, the outbox pump) | `src/raft.js` | 26 |
| Many groups in one process | `src/raft-host.js` | 12 |
| Peer transport, TCP | `src/raft-transport-tcp.js` | 5 |
| Peer transport, HTTP | `src/raft-transport-http.js` | 5 |
| WAL + replicated commit engine | `src/db-wal.js`, `src/db-replicated.js` | 24 + 12 |
| Status + SSE observability | `src/raft-monitor.js` | ✓ |

Membership changes, join-via-any-node, learners that carry no quorum
weight until they catch up, snapshot install with CRC-verified staging,
leader failover, restart catch-up, blank-member bootstrap — all
implemented and tested. See [`clustering.md`](clustering.md) and
[`replicaton-roadmap.md`](replicaton-roadmap.md) step 5.

### The C server cannot do this

`server/main.c` mentions Raft **zero times**. The pieces are already in
the binary — `raft_core.c`, `raft_msg.c`, `raft_drive.c`, `raft_node.c`
and `entrylog.c` are all linked into `nisaba-server` — but nothing
constructs a node, and there is no log, no peer transport and no apply
pump in the process.

So the replicated deployment that exists today is **Node processes
hosting the WASM engine**, which is a different program from shape 4.

What is still missing on the C side, in dependency order — briefs in
[`steps/`](steps/):

1. **InstallSnapshot in C** (`steps/install-snapshot-in-c.md`) — the last
   Raft message kind a host must answer.
2. **Completions in C** (`steps/completions-in-c.md`) — answering a
   proposal without a promise. Small.
3. **Native composition** (`steps/native-composition.md`) — seating
   several Raft groups over sockets in one process, and deciding what is
   policy.

Two prerequisites *were* just closed and are worth noting, because they
were the reason this was blocked rather than merely unstarted:

- `dbs_apply` — a C process can now apply a committed entry of **any**
  kind. It needed a JavaScript host for the three DDL opcodes until now.
- DDL is a command in the C server, so a leader has something to send.

### And it has no client-facing HTTP either

`raft-transport-http.js` is **node-to-node**, not client-to-server. The
monitor serves `GET /status` and an SSE `/events` stream, which is
observability, not a data API. Same question as shape 4.

---

## Where the shapes disagree

Flagged rather than resolved. Each is a real fork, not a detail.

### A. Which program is "the server"?

Shapes 4 and 5 both say "run the db on the server", but today they are
different executables:

|  | shape 4 | shape 5 |
| --- | --- | --- |
| process | `nisaba-server` (C, wasip2/native) | Node + WASM |
| driver | C request grammar | JS `Collection`/`Db` |
| replication | none | full |
| client wire | binjson frames over TCP | in-process, or your own |

Two directions, and they are mutually exclusive in the medium term:

- **Raft moves into the C server.** Then one program covers 4 and 5, the
  C request grammar is the only server-side driver, and Node is needed
  for neither. Costs briefs 1–3, plus a log, a peer transport and an
  apply pump in C.
- **The persistent server is a Node process.** Then shape 5 already
  works, shape 4's job is the embedded/single-node case, and the C
  server's reason to exist narrows to "a database process with no
  JavaScript in it" — which is a real reason, but a smaller one than it
  looks today.

Everything in `steps/` assumes the first. Nothing has confirmed it.

### B. HTTP: where does it live?

The use cases ask for an HTTP server in both 4 and 5. Three places it
could be, and they are not equivalent:

1. **In the C server** — what was built and reverted. Costs an HTTP
   parser in C (~250 lines, isolated) plus session identity, since HTTP
   has no stable connection to hang a cursor on.
2. **In the parent project's gateway** — where the README already puts
   "REST/WebSocket gateways", talking to this over the frame protocol.
   Costs nothing here, and a gateway that knows every op and refusal code
   is a second thing that knows the wire.
3. **A Node process in front**, using `db-server-client.js`. Cheapest to
   build, and adds a hop and a process.

### C. "Minimal JS bridge"

Not true today and not reachable without JSPI or Asyncify (shape 1). If
the phrase means "no logic in JS beyond glue", the browser must stop
being a host — which was already asked and answered *both* in
`steps/README.md`. Worth re-confirming that the answer still stands, as
it is the reason the operation set exists twice.

### D. Native CLI

Shape 3 is Node + WASM. Whether a dependency-free native `db` is wanted
has never been decided (shape 3, above).
