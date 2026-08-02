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
| 4 | One persistent server process | **Built**, speaking binjson frames over TCP. HTTP goes in front, in Node |
| 5 | Several server processes, Raft | **Built in Node.** The C server cannot replicate — and now must |

The single largest gap is that **4 and 5 are two different programs
today**: shape 4 is `server/main.c`, a C process with no JavaScript in
it; shape 5 is a Node process hosting the WASM engine. They share the
engine and the format, not the executable.

**Decided: they converge on the C server** — Raft moves into it, and
HTTP goes in front of it as a separate Node process. See
[Decisions](#decisions) at the end, which is what the rest of this
document is here to justify.

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

**Two drivers.** The same thirty-one operations exist twice: once as
`Collection`/`Db` in JavaScript, once as the request grammar in C. Part
of that is forced and part is not; the audit under shape 1 measures
which, and the answer is 44/54.

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

### The bridge is not minimal

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
  above that boundary — opening a collection, building an index,
  compacting, dropping.

That is why a JS driver exists at all. It is NOT, it turns out, why the
whole driver is in JS — see the audit below, which was written after
this paragraph and corrects it.

This is recorded as a settled decision in
[`steps/README.md`](steps/README.md): the browser stays a host, and
plan/execute is permanent. Every other host — Node, native, WASI — pays
a discipline it does not need so that this one can exist at all.

### The audit: what is actually forced

The claim above — that the bridge's size follows from the async-open
constraint — was an assumption until it was measured. It is half true,
and the half that is false is the interesting one.

`wasm/nisaba-wasm.js` is 3,937 lines, and it is five things, not one:

| Part | Lines | |
| --- | ---: | --- |
| `Db` + `Collection` — the driver | 1,804 | the subject of this audit |
| Raft bindings (`RaftCore`, `raftDrive`, `raftMsg`) | ~550 | shape 5's, not shape 1's |
| Codec, handle bridge, memory helpers | ~450 | forced: it is the WASM boundary |
| Name/catalog/WAL/TTL bindings | ~480 | thin calls into C |
| Providers, `ChangeStream`, errors, `Client` | ~650 | JS-shaped API surface |

Within the 1,804-line driver, every method was classified by whether it
touches the storage provider:

```
   plan/execute, genuinely forced          794 lines   (44%)
     Db.open, Db.collection, Db.dropCollection, Db.compact,
     Db._sweepOrphans, Collection._open, Collection.compact,
     Collection.createIndex / _createTextIndex / _createGeoIndex,
     Collection.dropIndex, and the handle lifecycle

   pure marshal — JS value in, C call, JS value out
                                          983 lines   (54%)
     find, findOne, count, distinct, aggregate, explain,
     findByIndex, insert, insertMany, update, updateMany,
     replace, delete, deleteMany, the findOneAnd* three,
     bulkWrite, pruneExpired, listIndexes, watch
```

**The 983 do not open anything.** They run against a collection whose
files are already open, and every one of them is an operation
`wasm/src/db_request.c` already implements for shape 4. The async-open
constraint does not reach them: it forces *opening* to be planned in C
and executed in JS, and opening is the other 794 lines.

So the second driver is not forced. In principle the browser could hold a
`dbs` session — the C session layer — and those 983 lines could become
"build a request object, call `dbs_handle`, decode the answer", leaving
one owner for the operation set. What that would cost, honestly:

- Every call gains an encode/decode round trip where today it passes
  pointers. Cheap per call, not free.
- `find`'s 192 lines are the async-iterator and batching protocol, which
  survives either way — it would wrap `getMore` instead of a direct
  cursor.
- Change-stream fan-out to JS consumers survives too; only the event
  *derivation* moves, and C already has it (`dbs_emit`).
- DDL and compaction stay exactly as they are. They are the forced 794.

**Decided: audit first, then decide** — this is that audit. The number to
carry forward is that **~54% of the driver is duplication that could be
removed, and ~44% cannot be**. Nothing is scheduled on it yet.

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

- **No HTTP, and it is not getting any.** It speaks binjson frames on a
  TCP socket. An HTTP envelope was built here (parser, listener,
  sessionless refusals, tests) and reverted; the decision is that HTTP
  belongs in a Node process in front of this one, not in this file. The
  reverted work stays on `wip/http-transport` as a record of what the
  other path costs. See [Decisions](#decisions) B.

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

### The C server does this now

`server/main.c` was written mentioning Raft zero times, and that is no
longer where it stands. `nisaba-server --raft ID --raft-port N --peer
ID@HOST:PORT` is a cluster member: a log in the process, a node over it,
a peer transport speaking the same wire `src/raft-transport-tcp.js`
does, and an apply pump — with the one real design change, that a write
is PROPOSED rather than applied where it lands. Three of them elect a
leader, replicate, survive the leader dying and catch a restarted member
up, with no JavaScript in any of them
([`db-server.md`](db-server.md)).

So there are now two replicated deployments rather than one: **Node
processes hosting the WASM engine**, and **C processes hosting nothing**.

**A native cluster can be grown and shrunk without restarting it**,
which retired the brief for it. `nisaba-server --raft 4 --raft-port 9004
--join 127.0.0.1:9001` joins knowing one ADDRESS: it follows the
redirect to the leader, enters as a learner, is caught up from the log
and is promoted to a voter once its match index proves it current.
`--leave ID` removes one. Argv became a BOOTSTRAP and the log became the
member set, so a restart needs neither flag.

What is still missing on the C side — briefs in [`steps/`](steps/):

The log growing without bound, which is the real reason to want
compaction and a snapshot store in the process. A joiner does not need
them — nothing compacts, so an empty joiner is caught up by plain
AppendEntries — but a long-lived member's `__wal__.bj` is every write it
has ever taken. `steps/README.md` records it.

**The server holds an INSTANCE**, which retired the brief for it: one
root directory, a subdirectory per database, and one connection that
reaches all of them (`client.db(name)`, exactly as in process).
Replication follows the instance — one log, one leader, one member set
for the executable — so `server/replica.c` and `server/peers.c` were
untouched by it.

**`steps/native-composition.md` is retired, unbuilt**, because it asked
for a multi-tenant seat: N independent Raft groups in one process, with
quiescence, from roadmap step 4's "one group per tenant database".
Tenancy is a layer above this repository, and with it excluded almost
nothing of that brief survived. What it kept being mistaken for is
brief 1 above, which is a different and much cheaper thing — one cluster
serving several named databases rather than several clusters sharing a
process.

**Two of these are done and retired.** Completions in C: a proposal's
fate — applied, and still yours — is decided in the node
(`rn_await`/`rn_applied`/`RN_EFFECT_SETTLED`), and `src/raft.js` reads
the answer rather than re-deriving the term rule.

**InstallSnapshot in C is done** and its brief is retired. All five Raft
message kinds are the node's now: it serves an install, receives one,
verifies it and adopts it — the generation's files onto the live names,
its own log rebased onto the boundary — through a `bj_ns` it is handed
(`wasm/include/raft_node.h`). The JavaScript implementation went with
it; what remains on any host is opening a file, which is asynchronous in
a browser, and the close/reopen an adoption runs between.

Two prerequisites *were* just closed and are worth noting, because they
were the reason this was blocked rather than merely unstarted:

- `dbs_apply` — a C process can now apply a committed entry of **any**
  kind. It needed a JavaScript host for the three DDL opcodes until now.
- DDL is a command in the C server, so a leader has something to send.

### And it has no client-facing HTTP either

`raft-transport-http.js` is **node-to-node**, not client-to-server. The
monitor serves `GET /status` and an SSE `/events` stream, which is
observability, not a data API. Client-facing HTTP is the Node front end
of [Decisions](#decisions) B, and it sits in front of a cluster exactly
as it sits in front of a single server — it holds a socket to whichever
member is the leader.

---

## Decisions

Three of the four forks this document opened have been answered. They are
recorded here with what they commit us to, because each one makes some
future work obvious and some other work pointless.

### A. Raft moves into the C server ✅

**One program covers shapes 4 and 5.** `nisaba-server` grows a log, a
peer transport and an apply pump; the C request grammar becomes the only
server-side driver; Node is required for neither shape.

What this commits us to — the briefs in [`steps/`](steps/) were written
on this assumption and are now confirmed rather than speculative:

1. ~~`steps/install-snapshot-in-c.md`~~ — **done**, and retired: the last
   Raft message kind a host had to answer is the node's.
2. ~~`steps/completions-in-c.md`~~ — **done**, and retired: a proposal's
   fate is the node's answer.
3. ~~`steps/server-as-replica.md`~~ — **done**, and retired: one server
   process IS a cluster member, log and peer transport and apply pump.
4. ~~`steps/native-composition.md`~~ — **retired unbuilt**: it was a
   multi-tenant seat, and tenancy is not this repository's.

What it makes secondary: `src/raft.js`, `src/raft-host.js`,
`src/db-replicated.js` and the two peer transports remain correct,
tested, and the reference implementation — but they stop being the road
to production clustering and become the embedded-Node story.

### B. HTTP is a Node process in front ✅

Not in C, and not the parent project's problem. A thin HTTP front end in
this repo, over `src/db-server-client.js` — which already speaks all 31
operations with no engine in the process. Briefed in
[`steps/http-front-end.md`](steps/http-front-end.md).

```
   browser / REST client
          │  HTTP
          ▼
   ┌─ node http front ─────────────┐   new, this repo
   │  src/db-server-client.js      │   no WASM, no ready()
   └──────────┬────────────────────┘
              │  binjson frames over TCP
              ▼
   nisaba-server (C)                   unchanged
```

**The revert stands.** `server/main.c` stays frame-only for CLIENTS, and
the HTTP subset written for it stays on `wip/http-transport` as a record
of what that path costs — chiefly session identity, since HTTP has no stable
connection for a cursor or a change stream to belong to. A Node front end
has the same problem to solve and a much easier place to solve it: it
holds one real socket per session and can keep them.

The costs, stated plainly: a hop, a process, and Node back in a
deployment the C server had removed it from.

### C. "Minimal JS bridge": audited ✅

Measured rather than assumed — see "The audit: what is actually forced"
under shape 1. **44% of the driver is forced by the
async-open constraint; 54% is duplication that is not.** No work is
scheduled on the strength of it yet; the number now exists to decide
with.

### D. Native CLI — still open

Shape 3 is Node + WASM. Whether a dependency-free native `db` is wanted
has never been decided, and nothing is blocked on it.
