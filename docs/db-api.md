# Document database API reference

A MongoDB-driver-shaped document database, implemented in C/WASM
(`wasm/src/db*.c`, `wasm/include/db*.h`) with a thin JS bridge
(`wasm/nisaba-wasm.js`/`src/db.js`, plus `src/db-coordinator.js` for
multi-tab sharing). This document is a complete reference for the
JS-facing API; see `docs/db-plan.md` for the milestone-by-milestone
design history and exact scope decisions behind each feature.

Every document is a plain JS object; `_id` is an `ObjectId` (auto-assigned
on insert if omitted) unless you supply your own. `Date`/`ObjectId`/binary
(`Uint8Array`) values round-trip through the same binjson codec used
everywhere else in this package — no BSON, no JSON, no lossy conversion.

## Contents

- [Connecting](#connecting)
- [`Client` (multiple named databases)](#client-multiple-named-databases)
- [`Db`](#db)
- [`Collection`](#collection)
  - [Insert](#insert)
  - [Read](#read)
  - [Update / replace](#update--replace)
  - [Find-and-modify](#find-and-modify)
  - [Delete](#delete)
  - [Counting and distinct values](#counting-and-distinct-values)
  - [Aggregation (`aggregate`)](#aggregation-aggregate)
  - [Query plans (`explain`)](#query-plans-explain)
  - [`bulkWrite`](#bulkwrite)
  - [Indexes](#indexes)
  - [Change streams (`watch`)](#change-streams-watch)
  - [Compaction (`compact`)](#compaction-compact)
- [Query operators](#query-operators)
- [Update operators](#update-operators)
- [Multi-tab sharing (`connectShared`)](#multi-tab-sharing-connectshared)
- [Not implemented / explicit scope limits](#not-implemented--explicit-scope-limits)

## Connecting

```js
import { connect, MemoryStorageProvider, OPFSStorageProvider } from '@mdy-docs/nisaba-db';
// (from Node, `import { connect, NodeFSStorageProvider } from '@mdy-docs/nisaba-db/node'`)

// In-memory (tests, ephemeral use — data lives only for the process lifetime)
const db = await connect(new MemoryStorageProvider());

// OPFS-backed (browser or Worker only; durable across reloads)
const dir = await navigator.storage.getDirectory(); // or a subdirectory
const db2 = await connect(new OPFSStorageProvider(dir));
```

`connect(provider, options?)` returns an already-open `Db`. `options.order`
sets the underlying B+ tree order (default 32) — rarely needs changing.
`options.autoCompact` (`{ minBytes?, factor? }`) schedules a deferred
compaction sweep after every open — see
[Compaction](#compaction-compact).

For sharing one database across multiple browser tabs/workers, see
[Multi-tab sharing](#multi-tab-sharing-connectshared) instead of `connect`.
For more than one *named* database under one root, see `Client` below
instead of calling `connect` directly.

## `Client` (multiple named databases)

`connect(provider)` gives you exactly one database, rooted at `provider`
itself. `connectClient(provider)` gives you a `Client` that can open many
independently named databases underneath that same root — mirrors the
cloud service's `MongoClient.db(name)` (`docs/cloud-rest-api.md`), minus
the tenant/network layer: there, `db(name)` picks one of a *tenant's*
databases; here, `provider` already picks the root you want named
databases under.

```js
import { connectClient, OPFSStorageProvider, MemoryStorageProvider } from '@mdy-docs/nisaba-db';

const client = await connectClient(new OPFSStorageProvider(dir));
const app = await client.db('app');             // real OPFS subdirectory: dir/app/
const analytics = await client.db('analytics');  // dir/analytics/ -- a fully separate catalog + files

await app.collection('users').insertOne({ name: 'Ada' });
await analytics.collection('users').find({}).toArray(); // [] -- a different database entirely
```

Each `db(name)` is a genuinely isolated storage scope, not a namespace
prefix inside one shared catalog: `OPFSStorageProvider` creates (or
reuses) a real child directory per name; `MemoryStorageProvider` creates
an independent in-memory file map per name. Two different names never
share a catalog, collection files, or indexes. `name` has the same
constraints as a collection name (no `/`, no NUL byte).

| Method | Description |
|---|---|
| `client.db(name)` | Open (or create) a named database as `provider.subProvider(name)`. Returns the same cached `Db` instance for repeated calls with the same name. |
| `client.close()` | Close every database this client has opened. |

`bin/db.js` (the `db` CLI) and `index.html`'s
console bridge (`window.client`) both use this, one OPFS root per install/page, one
subdirectory per database name.

## `Db`

| Method | Description |
|---|---|
| `db.collection(name)` | Open (or create) a collection. Returns the same cached `Collection` instance for repeated calls with the same name. `name` may not contain `/` or a NUL byte. |
| `db.listCollections()` | Array of every collection name in this database. |
| `db.dropCollection(name)` | Delete a collection and all its indexes/files. Returns `false` if it didn't exist. |
| `db.compact(options?)` | Compact every collection — see [Compaction](#compaction-compact). `{ minBytes, factor }` skip collections not worth rewriting yet; `{ skipBusy }` skips ones with open cursors. |
| `db.storageEstimate()` | `navigator.storage.estimate()` where the platform provides it, else `null` — the early-warning knob for quota pressure. A write that does hit quota fails cleanly: the operation is rolled back in-process and the error carries the handle's `QuotaExceededError` as `cause`. |
| `db.close()` | Close every open collection (and their indexes) and the catalog file. |

## `Collection`

### Insert

```js
await users.insertOne({ name: 'Ada', team: 'core' });
// => { acknowledged: true, insertedId: ObjectId(...) }

await users.insertMany([{ name: 'Grace' }, { name: 'Linus' }], { ordered: true });
// => { acknowledged: true, insertedCount: 2, insertedIds: { 0: ObjectId(...), 1: ObjectId(...) } }
```

- `insertOne(doc)` — `_id` is generated client-side (`new ObjectId()`) if
  `doc._id` is absent.
- `insertMany(docs, { ordered = true })` — `ordered: true` (default) stops
  at the first failing document; `false` attempts every document
  regardless of earlier failures (the thrown error's `.result` still
  reflects everything that succeeded first).

### Read

```js
const doc = await users.findOne({ team: 'core' });        // one document or null
const thin = await users.findOne({ team: 'core' }, { projection: { name: 1 } });

const cursor = users.find({ team: 'core' })
  .sort({ age: -1 })       // { field: 1 | -1, ... }, compound sorts allowed
  .skip(10)
  .limit(20)
  .project({ name: 1, age: 1 }); // inclusion XOR exclusion, not both; _id defaults included

const docs = await cursor.toArray();
for await (const doc of users.find({ team: 'core' })) { /* ... */ }
```

- `findOne(filter = {}, options = {})` — first match or `null`. `options.projection` follows the same inclusion-XOR-exclusion rules as `find()`'s (`_id` defaults included).
- `find(filter = {}, options = {})` — returns a lazy cursor (not a
  promise). `options` can set `sort`/`skip`/`limit`/`projection` up front,
  or use the chainable `.sort()`/`.skip()`/`.limit()`/`.project()` —
  both forms set the same underlying state and can be mixed. Only
  `.toArray()`/iteration actually executes the query.
- `countDocuments(filter = {})` — exact count via a full scan (or an
  equality-index plan when the filter allows it).
- `estimatedDocumentCount()` — alias for `countDocuments({})` (an O(1)
  B+ tree size lookup either way in this implementation, unlike real
  MongoDB's metadata-estimate-vs-exact-scan distinction).
- `distinct(field, filter = {})` — unique values of `field` (dot-path
  supported) across every matching document; array field values are
  flattened, not returned as arrays-of-arrays.

### Update / replace

```js
await users.updateOne({ _id }, { $set: { team: 'kernel' }, $inc: { visits: 1 } });
await users.updateMany({ team: 'core' }, { $set: { onCall: true } });
await users.replaceOne({ _id }, { name: 'Ada Lovelace' }); // full replacement document
```

All three accept `{ upsert: false }` as a third argument. Return shape
(all three): `{ acknowledged: true, matchedCount, modifiedCount, upsertedId }`
(`upsertedId` is `null` unless an upsert inserted a new document).
`updateOne`/`updateMany` require the update document's top level to be
entirely `$`-prefixed operators (see [Update operators](#update-operators));
a plain field is rejected — use `replaceOne` for a full replacement.
`updateMany` does not detect no-op updates: `modifiedCount` always mirrors
`matchedCount`.

An upsert takes its `_id` from the write itself where the write says one:
a `replaceOne` replacement's own `_id` first, then an `_id` the filter
pins as a bare equality (`{ _id: x }` — not `{ _id: { $ne: y } }`, which
pins nothing, the same rule that governs which filter conditions seed the
new document). Only when neither says anything is an id generated. So

```js
await users.updateOne({ _id: x, team: 'core' }, { $set: { seen: true } }, { upsert: true });
```

creates `x`, and running it again matches rather than creating a second
document. A pinned `_id` that is not an ObjectId is rejected — the
on-disk format has no other kind of key.

### Find-and-modify

```js
const before = await users.findOneAndUpdate({ _id }, { $set: { team: 'kernel' } });
const after  = await users.findOneAndUpdate({ _id }, { $set: { team: 'kernel' } }, { returnDocument: 'after' });
const deleted = await users.findOneAndDelete({ _id });
```

- `findOneAndUpdate(filter, update, { upsert = false, returnDocument = 'before' })`
- `findOneAndReplace(filter, replacement, { upsert = false, returnDocument = 'before' })`
- `findOneAndDelete(filter = {})`

All three return the matched document (pre- or post-image per
`returnDocument`) or `null` if nothing matched and no upsert happened
(an upsert with `returnDocument: 'before'` also returns `null` — no prior
state exists to return, matching real MongoDB).

### Delete

```js
await users.deleteOne({ _id });
await users.deleteMany({ team: 'ghosts' });
```

Both return `{ acknowledged: true, deletedCount }`.

### Counting and distinct values

Covered above under [Read](#read): `countDocuments`, `estimatedDocumentCount`, `distinct`.

### Aggregation (`aggregate`)

A deliberately small pipeline subset, executed in JS over materialized
`find()` results — enough for the "group and summarize" reach-for-it
moments without duplicating the C query engine:

```js
const perTeam = await users.aggregate([
  { $match: { active: true } },              // leading $match: runs in the ENGINE
  { $group: { _id: '$team', n: { $count: {} }, avgAge: { $avg: '$age' } } },
  { $match: { n: { $gte: 3 } } },            // later $match: JS subset (see below)
  { $sort: { n: -1 } },
  { $limit: 10 }
]).toArray();
```

- **Stages**: `$match`, `$sort`, `$skip`, `$limit`, `$project`, `$group`,
  `$count`. Anything else (`$unwind`, `$lookup`, …) throws, naming the
  stage.
- **The leading `$match` is pushed down into `find()`** — full engine
  operator grammar (`$regex`, `$text`, `$near`, …) and index planning
  apply there. Later `$match` stages run over synthesized documents in
  JS and accept a documented subset: field conditions, `$eq`/`$ne`/
  `$gt`/`$gte`/`$lt`/`$lte`/`$in`/`$nin`/`$exists`, `$and`/`$or` — an
  unsupported operator throws rather than silently not-matching.
- **`$group`**: `_id` of `null`, `'$path'`, or a composite object;
  accumulators `$sum` (field or literal), `$avg`, `$min`, `$max`,
  `$first`, `$last`, `$push`, `$addToSet`, `$count`.
- **`$project`**: `1`/`0` inclusion XOR exclusion (same rules as
  `find()`'s projection) plus `'$path'` computed copies.
- Returns a cursor-like handle (`toArray()`, `next()`, `for await`,
  `close()`), executing once on first pull. Everything after the
  pushed-down `$match` is materialized in memory — this is a convenience
  layer, not a streaming engine.
- Works through `connectShared` (the leader executes the whole
  pipeline; one RPC).

### Query plans (`explain`)

```js
await users.explain({ team: 'core' });
// => { source: 'equality', index: 'teamIdx' }
await users.find({ age: { $gt: 30 } }).explain();
// => { source: 'scan', index: null }   <- the signal to add an index
```

Reports which candidate source the query dispatch would use for a
filter, **without executing it** — the WASM side (`dc_explain`) consults
the very planners the queries run, so the report cannot drift from
reality. `source` is one of:

| source | meaning |
|---|---|
| `'ids'` | `{_id: <ObjectId>}` point lookup on the primary tree |
| `'equality'` | equality index lookup (`index` names it) |
| `'text'` | `$text` via the text index |
| `'geo'` | `$near`/`$geoWithin` via the R-tree |
| `'scan'` | full collection scan |

The same plan serves `find()` (streaming and sorted), `findOne`,
`countDocuments`, `updateMany`/`deleteMany` and `distinct`. Note the
equality planner is deliberately conservative: any `$`-operator
condition (even on an indexed field) falls back to a scan today.

### `bulkWrite`

```js
await users.bulkWrite([
  { insertOne:  { document: { name: 'Ada' } } },
  { updateOne:  { filter: { name: 'Ada' }, update: { $set: { team: 'core' } }, upsert: false } },
  { updateMany: { filter: { team: 'core' }, update: { $set: { onCall: true } } } },
  { replaceOne: { filter: { name: 'Ghost' }, replacement: { name: 'New' }, upsert: true } },
  { deleteOne:  { filter: { name: 'Linus' } } },
  { deleteMany: { filter: { team: 'kernel' } } }
], { ordered: true });
```

Pure JS orchestration over the collection's own already-atomic methods —
each sub-operation is a complete atomic unit on its own, so no new C-level
transaction logic is needed. `ordered` (default `true`) stops at the first
failing operation; `false` attempts every operation and throws an
aggregate error afterward (with `.result` and `.writeErrors` on the thrown
error) if any failed. Result shape: `{ acknowledged, insertedCount,
matchedCount, modifiedCount, deletedCount, upsertedCount, insertedIds,
upsertedIds }`.

### Indexes

```js
await users.createIndex({ team: 1 });                                   // equality (ascending only)
await users.createIndex({ team: 1, level: 1 });                         // compound
await users.createIndex({ email: 1 }, { unique: true });
await users.createIndex({ email: 1 }, { sparse: true });
await users.createIndex({ email: 1 }, { partialFilterExpression: { active: true } });
await users.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600 }); // TTL, single-field only
await posts.createIndex({ body: 'text' });                              // one text index per collection
await places.createIndex({ location: '2dsphere' });                     // GeoJSON Point index

await users.dropIndex('team_1');
await users.listIndexes();          // [{ name, key, unique?, sparse?, partialFilterExpression?, expireAfterSeconds? }, ...]
await users.findByIndex('team_1', ['core']); // direct index lookup, bypassing the query planner
await users.pruneExpired();         // delete every document past its TTL cutoff; call this periodically yourself
```

- **Equality indexes**: fields must all be `1` (ascending) — descending
  (`-1`) is rejected. `options.name` defaults to `field1_1_field2_1...`.
  `createIndex` backfills existing documents and fails all-or-nothing if
  any document can't be indexed (unless `sparse` tolerates a missing
  field, or `partialFilterExpression` excludes the document).
  - `unique` — rejects a duplicate value on insert, `updateOne`/
    `updateMany`/`replaceOne`, and upsert; also rejected during
    `createIndex`'s backfill if pre-existing duplicates are found (the
    index is not created in that case).
  - `sparse` — a document missing the indexed field is simply not
    indexed (not an error).
  - `partialFilterExpression` — a filter document; only documents
    matching it are indexed (and, combined with `unique`, only *those*
    documents' values must be unique).
  - `expireAfterSeconds` — TTL index, single-field only, on a `Date`
    field. Nothing deletes automatically — call `pruneExpired()`
    yourself on whatever schedule you want (e.g. `setInterval`, or only
    from whichever tab currently holds coordinator leadership).
- **Text index** (`{ field: 'text' }`) — exactly one per collection.
  Powers the `$text` query operator (see below).
- **Geo index** (`{ field: '2dsphere' }`) — field holds a GeoJSON
  `{ type: 'Point', coordinates: [lng, lat] }`. Powers `$near`/
  `$geoWithin` (see below).
- `unique`/`sparse`/`partialFilterExpression`/`expireAfterSeconds` are
  only supported on equality indexes, not `text`/`2dsphere`.

### Change streams (`watch`)

```js
const stream = users.watch(); // pipeline argument not supported yet — see below

stream.on('change', (change) => console.log(change));
// or:
for await (const change of stream) console.log(change);

stream.close();
```

`watch(pipeline = [], options = {})` throws if `pipeline` is non-empty
(no `$match`/aggregation-stage filtering yet — filter inside your own
`on('change', cb)` instead). Returns a `ChangeStream`: both an
EventEmitter-lite (`.on('change', cb)`, `.off(cb)`) and an async iterator
(`for await`, or manual `.next()` → `{ value, done }`), plus `.close()`.

Backpressure: listeners get every event synchronously and nothing is
buffered for them; events are buffered only for the iterator side, up to
`options.maxBuffered` (default 4096) unconsumed events. At the bound the
stream closes itself and `next()`/`for await` reject with a
`ChangeStreamOverflowError` — never unbounded growth, never a silent
drop. There are no resume tokens (a deliberate non-goal): an overflowed
consumer re-`watch()`es and re-reads current state.

Change event shape:

```js
{
  operationType: 'insert' | 'update' | 'replace' | 'delete',
  ns: { coll: 'users' },
  documentKey: { _id },
  fullDocument: { ... }   // absent for 'delete'
}
```

Fired by `insertOne`/`insertMany`/`updateOne`/`updateMany`/`replaceOne`/
`deleteOne`/`deleteMany`/`findOneAndUpdate`/`findOneAndReplace`/
`findOneAndDelete`. Costs nothing when nothing is watching; some methods
(`updateOne`/`updateMany`/`deleteOne`/`deleteMany` with a filter that
doesn't already name `_id`) do one extra lookup per affected document, but
*only* when a watcher is actually registered. `updateMany` with active
watchers is `O(matched)` extra round trips — fine for an observability
feature, not meant for a hot path. No `updateDescription` (would need
diffing before/after images). `findOneAndUpdate`/`findOneAndReplace`
always report `'update'`/`'replace'`, never distinguishing an
upsert-triggered insert from a genuine match (unlike `updateOne`/
`updateMany`/`replaceOne`, which do for free via their own return value).

See `index.html` + `src/db-worker.js` for a live multi-tab demo (`npm run
dev`, then open http://localhost:8086/ in two or more browser tabs;
`npm run build` + `npm run preview` serves the production bundle the
same way).

### Compaction (`compact`)

Every backing file is append-only: updates and deletes append new tree
nodes and never reclaim old ones, so files grow monotonically with write
traffic, not with live data. `compact()` rewrites the collection's whole
file set (primary tree + every index + a fresh commit journal) as
minimal, fully-packed files and atomically swaps them in — the
append-only analog of MongoDB's `compact` command. See
`docs/compaction.md` for the design (atomic swap via one catalog commit,
crash windows, orphan sweeping).

```js
const stats = await users.compact();
// => { generation: 1, bytesBefore: 182_344, bytesAfter: 21_580, bytesFreed: 160_764 }

// Or every collection at once, optionally with cheap skip thresholds:
await db.compact();                             // unconditional
await db.compact({ minBytes: 1 << 20, factor: 4 }); // only what's worth doing

// Or let the engine sweep at its own natural moments (open, and -- under
// connectShared -- every leadership acquisition):
const db2 = await connect(provider, { autoCompact: { minBytes: 1 << 20, factor: 4 } });
await db2.autoCompacted; // optional: the deferred sweep's results
```

- **`collection.compact()`** rewrites unconditionally. Throws if the
  collection has open (unexhausted, unclosed) `find()` cursors — close
  them first. Any other operation issued while a `compact()` is in
  flight simply waits for it and then runs against the new generation —
  a brief queue, never an error. `watch()` streams stay attached across
  the swap.
- **`db.compact({ minBytes = 0, factor = 0, skipBusy = false })`** runs
  `collection.compact()` on each collection, skipping (result `null`)
  any whose file set is smaller than `minBytes` or hasn't grown to
  `factor ×` its size right after its previous compaction. `skipBusy`
  also skips, rather than throws on, collections with open cursors —
  for unattended sweeps. With thresholds set it is cheap to call
  eagerly — on a timer, the same host-driven convention as
  `pruneExpired()`. Returns `{ [collectionName]: stats | null }`.
- **`connect(provider, { autoCompact: { minBytes, factor } })`** builds
  the convention in: one deferred
  `db.compact({ minBytes, factor, skipBusy: true })` sweep fires after
  `open()` completes, without delaying `connect()`; `db.autoCompacted`
  resolves with its results (`null` after a failure, which warns;
  closing mid-sweep quietly abandons it). Under `connectShared` the
  options reach every newly elected leader's `connect()`, so a
  leadership handover re-runs the sweep.
- **Space**: while a compact runs, old and new file sets exist
  side-by-side, so peak usage is roughly *old + live* bytes.
- **History**: compaction destroys the old file's append-only history —
  B+ tree snapshots/`boundaries()` taken against the old files become
  invalid.
- Works through `connectShared` too (`SharedCollection.compact()` /
  `SharedDb.compact()` proxy to the leader), and from the CLI:
  `db <name> compact [coll]`.

## Query operators

Filters are plain objects; a field's value is either a literal (matched by
exact encoded-byte equality) or an "operator expression" — an object whose
keys are *all* `$`-prefixed. Multiple operators on one field are ANDed:
`{ age: { $gte: 18, $lt: 65 } }`. An unrecognized `$`-operator is a hard
error, never a silent no-op.

| Operator | Example | Notes |
|---|---|---|
| `$eq` | `{ age: { $eq: 36 } }` | Same as a bare literal. |
| `$ne` | `{ age: { $ne: 36 } }` | |
| `$gt` `$gte` `$lt` `$lte` | `{ age: { $gt: 18 } }` | Only same-domain ordering: number-vs-number, string-vs-string, Date-vs-Date. A cross-domain comparison never matches. |
| `$in` `$nin` | `{ team: { $in: ['core', 'kernel'] } }` | |
| `$exists` | `{ nickname: { $exists: true } }` | |
| `$not` | `{ age: { $not: { $gt: 65 } } }` | Requires an operator expression inside. |
| `$and` `$or` `$nor` | `{ $or: [{ team: 'core' }, { team: 'kernel' }] }` | Top-level only; each takes a non-empty array of filter objects. |
| `$size` | `{ tags: { $size: 2 } }` | Array's own element count. |
| `$all` | `{ tags: { $all: ['core', 'admin'] } }` | Every listed value must be present; an empty `$all` never matches. |
| `$type` | `{ value: { $type: 'string' } }` | String aliases: `string`, `number` (int or double), `int`, `double`, `bool`, `date`, `objectId`, `array`, `object`, `null` — not BSON's numeric type codes. |
| `$mod` | `{ count: { $mod: [4, 2] } }` | `[divisor, remainder]`, both truncated to integer. |
| `$elemMatch` | `{ scores: { $elemMatch: { $gt: 80, $lt: 90 } } }` | The one operator with element-*wide* AND semantics — every other operator matches if *any* element (or the whole array value) satisfies it independently; `$elemMatch`'s sub-query must hold entirely against *one* element. Sub-query can also be a plain query object for an array of subdocuments. |
| `$regex` / `$options` | `{ name: { $regex: '^A', $options: 'i' } }` | Operator-expression form only (no bare `RegExp` literal — binjson has no BSON-regex wire type). Small backtracking engine (`c/regex.c`): literals, `.`, `\d\D\w\W\s\S`, `[...]`/`[^...]` classes with ranges, `* + ? {n} {n,} {n,m}`, `^ $` anchors, `(...)` grouping (no capture), `\|` alternation, `i` flag. No non-greedy quantifiers, backreferences, lookaround, named/capturing groups, or other flags. |
| `$text` | `{ $text: { $search: 'fox' } }` | Requires a `text` index on the collection; combines with a residual filter on other fields. |
| `$near` | `{ location: { $near: { $geometry: { type: 'Point', coordinates: [lng, lat] }, $maxDistance: 1000 } } }` | Requires a `2dsphere` index on that field. Nearest-first; `$maxDistance` in km, optional. |
| `$geoWithin` | `{ location: { $geoWithin: { $box: [[minLng, minLat], [maxLng, maxLat]] } } }` or `{ $center: [[lng, lat], radiusKm] }` | Requires a `2dsphere` index. |

Dot-notation field paths (`"a.b.c"`) descend through nested objects only —
a path segment that would need to index into an array isn't supported.
A missing field never equality-matches a literal `null` (MongoDB's
null-matches-missing quirk isn't implemented), and full cross-BSON-type
ordering isn't implemented either — both documented, deliberate scope
limits.

## Update operators

Update documents passed to `updateOne`/`updateMany`/`findOneAndUpdate`
must be entirely `$`-prefixed operators at the top level; a field may be
targeted by at most one operator; `_id` may never be targeted. **Target
field names are top-level only — no dotted paths (`{$set: {"a.b": 1}}` is
rejected) and no positional array operators (`$`, `$[]`, `$[<id>]`) yet.**

| Operator | Example | Notes |
|---|---|---|
| `$set` | `{ $set: { team: 'kernel' } }` | Creates the field if absent. |
| `$unset` | `{ $unset: { nickname: '' } }` | No-op if the field is absent. |
| `$inc` | `{ $inc: { visits: 1 } }` | Field (if present) and operand must both be numbers; missing field seeds from 0. |
| `$mul` | `{ $mul: { price: 1.1 } }` | Like `$inc` but multiplies; missing field seeds at `0` (matches real MongoDB). |
| `$min` / `$max` | `{ $min: { score: 10 } }` | Keeps the smaller/larger value (same ordering as `$gt`/`$lt`); missing field is seeded directly; an incomparable pair is an error (unlike a filter, a value-producing op can't silently do nothing). |
| `$rename` | `{ $rename: { nick: 'nickname' } }` | Renaming a missing field is a total no-op — it does *not* touch a pre-existing destination field either. Destination can't collide with another operator's target or another rename's destination. |
| `$currentDate` | `{ $currentDate: { lastSeen: true } }` or `{ $type: 'date' }` | Resolved entirely client-side (in `wasm/nisaba-wasm.js`, rewritten into `$set` before crossing into C — only the JS host has a clock). `{$type: 'timestamp'}` isn't supported (no timestamp wire type). |
| `$setOnInsert` | `{ $setOnInsert: { createdAt: new Date() } }` | Applied only when `upsert: true` actually inserts a new document; a complete no-op on a normal matched update (still reserves the field against other operators). |
| `$addToSet` | `{ $addToSet: { tags: 'admin' } }` or `{ $addToSet: { tags: { $each: ['admin', 'root'] } } }` | Appends only if not already present (byte-equality); `$each` batch is deduped against existing elements and against itself. |
| `$push` | `{ $push: { tags: 'admin' } }` or `{ $push: { scores: { $each: [4, 2], $slice: -3, $sort: -1, $position: 0 } } }` | Modifier form triggered by an operand object containing `$each`. `$slice` keeps first N (positive) / last \|N\| (negative) after insertion. `$sort` (`1`/`-1` value comparison only — no document-key sort for subdocument arrays) overrides `$position` when both given. |
| `$pull` | `{ $pull: { scores: 5 } }` or `{ $pull: { scores: { $gt: 90 } } }` | Byte-equality by default, or an operator-expression condition (reuses the query matcher). |
| `$pullAll` | `{ $pullAll: { scores: [1, 2, 3] } }` | Removes every element byte-equal to *any* listed value — no operator-expression form. |
| `$pop` | `{ $pop: { queue: 1 } }` | `1` drops the last element, `-1` the first; no-op on a missing field, error if present but not an array. |
| `$bit` | `{ $bit: { flags: { and: 0b1100, or: 0b0001 } } }` | `and`/`or`/`xor`, chainable, applied in encounter order to an `INT` field (no bitwise ops on floats); missing field defaults to `0`. |

## Multi-tab sharing (`connectShared`)

`FileSystemSyncAccessHandle` (what every OPFS file here uses) takes an
exclusive, origin-wide lock per file — only one browser context can have a
collection's files open at once. `src/db-coordinator.js`'s
`connectShared` lets many tabs/workers share one logical database anyway.

```js
// Inside a dedicated Worker (required — see below)
import { connectShared } from '@mdy-docs/nisaba-db/coordinator';
import { OPFSStorageProvider } from '@mdy-docs/nisaba-db';

const dir = await navigator.storage.getDirectory();
const db = await connectShared('my-app', new OPFSStorageProvider(dir));
const users = await db.collection('users');
```

- Exactly one connecting context becomes the **leader** (elected via
  `navigator.locks.request` — no polling, no split-brain) and is the only
  one that actually opens the real `Db`/OPFS files; every other context
  gets a `SharedDb`/`SharedCollection` — the *same public API* as
  `Db`/`Collection` (including `watch()`; enforced by a reflection test) —
  that proxies calls over one `BroadcastChannel` per `dbName`, payloads
  binjson-encoded so `ObjectId`/`Date` survive the trip. Partial-failure
  details cross too: a follower's failed `insertMany`/`bulkWrite` carries
  the same `err.result`/`err.writeErrors` a local caller would see.
- One structural difference: a `SharedCollection.find()` cursor has the
  full cursor API, but materializes its complete result set with a single
  RPC on the first pull (`toArray()`/`next()`/iteration) instead of
  streaming batches — there is no batch protocol across the channel. A
  side effect is that `next()` works after `.sort()` here, which the real
  cursor's streaming path refuses.
- Must run inside a dedicated `Worker` (not the main thread) — same
  requirement OPFS access already has in this repo.
- If the leader's tab/worker closes, a queued follower is automatically
  granted leadership and picks up serving reads/writes, including data
  already written.
- `watch()` on a `SharedCollection` sees writes made through *any* tab
  sharing the database, not just the calling one — the leader
  rebroadcasts its real collection's change events to everyone.

## Not implemented / explicit scope limits

Tracked in detail in `docs/db-plan.md`; summarized here:

- **Dotted-path update targets** (`{$set: {"a.b": 1}}`) and **positional
  array operators** (`$`, `$[]`, `$[<id>]`) — update targets are
  top-level field names only.
- **Aggregation pipeline** (`$group`/`$project`/`$lookup`/etc.) —
  deliberately not implemented; would be the single biggest chunk of new
  execution code in the roadmap, and nothing else depends on it.
- **Full multi-document transactions** (`startSession`/
  `withTransaction()`) — only per-document-write atomicity exists: a
  cross-file commit journal ensures one document's primary-tree write and
  all its index updates land together across a crash, and a failed write
  (e.g. a document missing a non-sparse indexed field, or a unique-key
  conflict) rewinds every file in-process, so a rejection never leaves a
  half-applied document visible. Real sessions would need a journal
  spanning multiple collections at once plus a JS-side session object
  buffering operations until commit.
- **MongoDB's null-matches-missing quirk** and **full cross-BSON-type
  ordering** — a missing field never equality-matches a literal `null`;
  ordering comparisons only work within one domain (number/string/Date).
- **Descending (`-1`) index fields** — equality index fields must all be
  ascending (`1`).
- **`$match`/aggregation-stage filtering in `watch()`** and
  **`updateDescription`** in change events — see
  [Change streams](#change-streams-watch).
