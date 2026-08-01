# Nisaba

> Nisaba (pronunciation: nee-SAH-bah)
> 
> Nisaba was the Sumerian goddess of writing, grain, and accounting — one of the oldest deities associated with record keeping and scribes. She was often depicted with a stylus and clay tablet, and scribes would invoke her name at the end of texts as a mark of accuracy and completeness.

A WASM/C, MongoDB-driver-shaped embedded document database: CRUD,
operator-aware query matching, `$regex` (ECMAScript-flavored, via
[regex-engine](https://github.com/mdy-docs/regex-engine)), update
operators, secondary/geo/text indexes, and change streams (`watch()`).

Built on [binjson](https://github.com/mdy-docs/binjson) (the wire
format and value types) and
[binjson-structures](https://github.com/mdy-docs/binjson-structures)
(the B+ tree primary store and secondary equality index, R-tree for geo
indexes, text index for `$text`-style search) — `nisaba`'s own C code
(`wasm/src/db.c`/`db_query.c`/`db_update.c`/`db_keyenc.c`/`db_wasm.c`, plus
`regex.c` as the `$regex` adapter over regex-engine) is the CRUD/query/
update layer built on top of those, not a data structure itself.

Split out from the parent `binjson` document-database project (currently
staged ahead of becoming its own git submodule/repo there — see that
project's `third_party/regex-engine` for the pattern this is following).
The cloud SaaS layer that runs this as a service (control plane,
REST/WebSocket gateways, a MongoDB-driver-shaped HTTP client) is **not**
part of this package — it stays in the parent project, built on top of
the `Db`/`Client` this package exports, the same way any other
application would consume it.

## Dependencies

This package's C sources call directly into binjson's encoder/builder
and binjson-structures' B+ tree/R-tree/text-index functions — real
compile-time and link-time dependencies, not just shared headers. Its
own standalone WASM build links this package's sources together with
checkouts of binjson, binjson-structures, and regex-engine into one
binary; its JS wrapper keeps its own self-contained copy of the codec
and tree-wrapper classes for the same reason (see the top comment of
`wasm/nisaba-wasm.js`).

Those checkouts are git submodules of *this* repo
(`third_party/binjson`, `third_party/binjson-structures`,
`third_party/regex-engine`), used only for this package's own
standalone build/test. A project that already depends on all of these
(like the parent `binjson` project itself) should supply its own single
copy of each to build against, rather than relying on these nested
copies, to avoid ending up with duplicate checkouts linked into the
same binary.

```
git submodule update --init
./wasm/build-wasm.sh
```

## Usage

```js
import { connect, MemoryStorageProvider } from '@mdy-docs/nisaba-db';

const db = await connect(new MemoryStorageProvider());
const users = await db.collection('users');

const { insertedId } = await users.insertOne({ name: 'Ada', team: 'core' });
await users.createIndex({ team: 1 });
const core = await users.find({ team: 'core' }).toArray();
```

In Node, persist to real files (fsync-backed, per-directory advisory
lock — no OPFS shim needed):

```js
import { connect, NodeFSStorageProvider } from '@mdy-docs/nisaba-db/node';

const db = await connect(new NodeFSStorageProvider('./data'));
```

`connect(provider)` opens a single database against a storage provider
(`MemoryStorageProvider` for in-memory/ephemeral use, `OPFSStorageProvider`
for real browser/Worker persistence, `NodeFSStorageProvider` for Node).
`connectClient(provider)` opens a `Client` with multiple independently
named databases, each its own isolated storage scope — see
`Client.db(name)`. TypeScript declarations ship with the package.

No `ready()` call is needed before `connect()` — it awaits the WASM
instantiation itself; only the WASM-backed low-level surface
(`@mdy-docs/nisaba-db/wasm`'s `encode`/`decode`, tree classes) needs an explicit
`await ready()` first.

One deliberate MongoDB deviation to know up front: **`_id` must be an
ObjectId** (scalar `_id`s — numbers, arbitrary strings, Dates — throw
`InvalidIdError`; the on-disk format keys everything by fixed 12-byte
OIDs). Keep natural keys in their own field with a unique index:
`createIndex({ email: 1 }, { unique: true })`.

### Entry points

| import | contents |
|---|---|
| `@mdy-docs/nisaba-db` | the full in-process database (browser Worker, or anywhere) |
| `@mdy-docs/nisaba-db/node` | ↑ plus `NodeFSStorageProvider` (imports `node:fs` — Node only) |
| `@mdy-docs/nisaba-db/remote` | WASM-free main-thread half: pure-JS codec + `createRemoteBridge` (~27 KB module graph) |
| `@mdy-docs/nisaba-db/server-client` | WASM-free client for a database *server* process (`server/main.c`) over a socket — `connectServer('host:port')`, twenty-four operations — CRUD, lists of writes, aggregation, query plans, paged cursors, indexes, compaction, listing — no engine in the client |
| `@mdy-docs/nisaba-db/coordinator` | `connectShared` — multi-tab sharing via leader election (Worker-side) |
| `@mdy-docs/nisaba-db/wasm` | everything, including the low-level tree/index classes |

### Non-goals

Deliberate scope limits, stated up front rather than discovered late:

- **Cross-collection transactions.** Every single write (including
  `updateMany`/`bulkWrite` sub-operations) is atomic and journaled;
  there is no multi-collection transaction and none planned.
- **Scalar `_id`s** — until a future format v2 (`docs/roadmap.md`): the
  on-disk format keys everything by fixed 12-byte ObjectIds.
- **Change-stream pipelines, `updateDescription`, resume tokens.**
  `watch()` delivers whole events, bounded-buffered; an overflowed
  consumer re-watches and re-reads current state.
- **Multi-writer across processes/origins.** One opener per database
  directory (enforced by OPFS handle exclusivity in browsers and the
  advisory lock in Node); multi-tab sharing is single-leader by design.
- **Full aggregation framework.** `aggregate()` is a documented small
  subset (`docs/db-api.md`); `$lookup`/`$unwind`/expression operators
  are out of scope.

## Documentation

- [`docs/db-api.md`](docs/db-api.md) — complete JS-facing API reference.
- [`docs/db-example.js`](docs/db-example.js) — a runnable, narrated tour of
  most of that API (`node docs/db-example.js`).
- [`docs/db-server.md`](docs/db-server.md) — the database as a *server*
  process: one directory, binjson over a socket, `wasm32-wasip2` or
  native, with no JavaScript in it. The wire, the invariants, and what it
  does not do yet.
- [`docs/db-plan.md`](docs/db-plan.md) — milestone-by-milestone design
  history and scope decisions (historical; see the note at its top).
- [`docs/textindex-atomicity.md`](docs/textindex-atomicity.md) — how the
  text index stays atomic across its backing files.
- [`docs/compaction.md`](docs/compaction.md) — how `compact()` reclaims
  the space append-only storage costs, via an atomic catalog-commit swap
  of a collection's whole file set.
- [`docs/format-compatibility.md`](docs/format-compatibility.md) — the
  on-disk format version stamp and the rules for ever changing it.
- [`docs/roadmap.md`](docs/roadmap.md) — honest appraisal of where the
  project stands and the prioritized productization plan (types, named
  errors, a native Node storage provider, CI, and the remaining known
  robustness holes).
