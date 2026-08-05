# Roadmap: productization plan of attack

An honest appraisal of where nisaba stands (2026-07-16) and the ordered
work needed to make it easily usable, stable, and robust for strangers —
in the browser and directly from Node.js.

**Verdict:** the hard part is done unusually well. The durability core —
append-only files with CRC-trailer commits, the cross-file journal
(`engine/src/db.c`, magic+version+CRC), catalog-flip compaction
(`docs/compaction.md`), orphan sweeping, byte-level crash-window tests —
is more rigorous than most embedded databases ever get. What's missing is
almost entirely *productization*: the layer between "correct engine" and
"thing someone can npm-install and trust." Nothing below touches the
storage format or the core design; it is all surface, packaging, and the
last known concurrency holes.

What's already strong (don't relitigate): the crash-atomicity story and
its synthesized-crash-window test technique; leader election via
`navigator.locks` instead of SharedWorker (iOS Safari has none); the
single execution path for local and proxied calls (`executeOnRealDb`);
the entry-point split (`nisaba` full / `@mdy-docs/nisaba-db/remote` ~11 KB main-thread
/ `@mdy-docs/nisaba-db/coordinator` worker-side); the docs themselves.

---

## P0 — adoption blockers

**Status: all six items landed 2026-07-16.** Notes against the original
acceptance criteria: #3 resolved as the documented-restriction branch —
the spike confirmed the C core is OID-typed end to end (fixed 12-byte
primary keys, OID back-pointers in every index row, `{_id}` fast paths),
so scalar `_id`s are a format-v2 migration, now recorded below; scalar
`_id`s throw `InvalidIdError` pointing at the unique-index alternative.
#4 shipped with its own dedicated suite (`test/db.nodefs.test.js`)
rather than parametrizing the OPFS tests — parametrization is still
worth doing. #5: build/lib turned out to be gitignored (artifacts build at pack
time, never committed), so every CI job builds from the C sources with
a pinned emsdk before testing -- each run inherently verifies sources
and wrapper agree; no byte-diff gate (emcc output isn't reproducible
enough across hosts).

Everything an evaluator hits in the first ten minutes, plus the two
things that make Node a real target. Do these before publicizing
anything.

### 1. TypeScript declarations ✅

No `.d.ts`, no `types` field in package.json. For a MongoDB-shaped API
this is the single highest-leverage fix — the shapes are well-known and
the whole public surface is already enumerated in `docs/db-api.md`.

- Hand-write `types/nisaba.d.ts` (+ entries for `./remote`, `./wasm`,
  `./coordinator`); wire `"types"` into each `exports` condition.
- Cover: `connect`/`connectClient`/`connectShared` (incl. `order`,
  `autoCompact`), `Db`/`Client`/`Collection`/cursor/`ChangeStream`,
  storage providers, `ObjectId`/`Pointer`, `encode`/`decode`,
  `createRemoteBridge`.
- Acceptance: a TS consumer gets completions and correct return types
  for every documented API; `tsc --noEmit` passes on `docs/db-example.js`
  converted to TS as a fixture.

### 2. Named, coded errors ✅

`codeError()` (`src/nisaba-wasm.js`) produces bare `Error`s: `e.code`
is `undefined`, `e.name` is `"Error"`. Programmatic handling requires
message matching — we personally lost debugging time to the old
"builder state error" for exactly this reason. The ERR map already has
everything needed.

- Introduce a `NisabaError extends Error` with `code` (the C error
  code) and `name` per class: `DuplicateKeyError` (-10, -12),
  `MissingIndexedFieldError` (-13), `UnindexableValueError` (-14), etc.
- Keep messages identical (tests match on them); add `code`/`name`
  assertions to the suite.
- Coordinator note: `_handleRequest` flattens errors for the wire —
  carry `code`/`name` through the payload so followers rebuild the same
  shape (`db-coordinator.js` already rebuilds `result`/`writeErrors`).

### 3. Scalar `_id` support (or a loud, documented restriction) ✅ (restriction branch)

Verified empirically: `{ _id: 'user-42' }` and `{ _id: 1 }` both throw
(`toObjectId` requires ObjectId/24-hex). MongoDB accepts any scalar
`_id`, and natural keys are extremely common — this will be the #1
"it doesn't work" report.

- Preferred: allow string/number/Date `_id`s. The key encoder already
  handles exactly those types for secondary indexes (see the -14 error
  text), so the primary key can plausibly reuse `keyenc.c`'s
  ordered encoding. Audit every `_id` touchpoint: insert paths, filter
  fast-paths (`filter._id !== undefined`), change events' documentKey,
  the CLI's arg parsing, RPC codec round-trip.
- If that's deeper than it looks (primary tree keys are currently
  fixed-width OID bytes), then instead: document the restriction in the
  README's first code block and throw a purpose-built error naming it.
- Acceptance: either scalar `_id`s round-trip through insert/find/
  update/delete/watch/index paths with tests, or the restriction is
  impossible to miss.

### 4. `NodeFSStorageProvider` — make Node first-class ✅

Persistence in Node currently rides on `node-opfs`, a third-party OPFS
shim that isn't even a dependency (`bin/db.js` exits with "install
node-opfs"), and it roots data at `~/.node-opfs/` — a location users
don't choose.

- Implement `NodeFSStorageProvider(rootDir)` in its own entry
  (`@mdy-docs/nisaba-db/node`, so browser bundles never see `node:fs`): the provider
  interface is tiny — `openFile` returning the sync-access-handle shape
  (`read`/`write` at offsets, `getSize`, `truncate`, `flush`, `close`),
  `deleteFile`, `listFiles`, `subProvider` (subdirectory).
- `flush()` must be a real `fsync` — the whole durability story assumes
  flushed bytes stay put.
- **Exclusivity**: browser OPFS sync handles are exclusive per file;
  Node has no analog, so nothing stops two processes corrupting one
  directory. Take an advisory lock (`flock` via `fs-ext`, or an
  O_EXCL lockfile with stale-PID detection) on the root at open; fail
  loudly on contention.
- Rework `bin/db.js` onto it; drop the node-opfs requirement (keep it
  usable for tests). Document where data lives and how to choose.
- Acceptance: full existing suite green with the new provider swapped
  in (parametrize the OPFS-path tests); a second-process open fails
  with a clear error; `kill -9` mid-write recovers on reopen (the
  journal tests, but against real files).

### 5. CI ✅

220 node tests plus a browser suite exist, and nothing runs them on
commit — the browser suite isn't even installable here
(`@vitest/browser` was never added to the parent workspace). Nothing
else on this list is trustworthy without this.

- GitHub Actions: job 1 `npm test` (node suite; includes the node-opfs
  OPFS paths), job 2 `npm run test:browser` under Playwright Chromium
  (the real-OPFS compaction + coordinator handover tests), job 3 the
  WASM build (`./build/build-wasm.sh` with emsdk pinned) verifying the
  committed `build/lib/` artifacts match the sources.
- Acceptance: red PRs on any suite failure; the browser suite runs
  somewhere at last.

### 6. README/doc import hygiene ✅

README's usage block imports `./src/nisaba-wasm.js`; docs mix relative
paths. Teach the curated entries: `@mdy-docs/nisaba-db`, `@mdy-docs/nisaba-db/remote`,
`@mdy-docs/nisaba-db/coordinator` (and `@mdy-docs/nisaba-db/node` once #4 lands). Also correct
the `ready()` ceremony: `connect()` transitively awaits it
(`BPlusTree.open`), verified empirically — only bare `encode`/`decode`
before any open need it.

---

## P1 — the remaining known correctness/robustness holes

**Status: all six items landed 2026-07-16** (engine: `_inFlight` drain +
prototype wrapper, `cursorFinalizer`, bounded ChangeStream, `bridgeHandle`
+ `storageEstimate()`, format stamp; coordinator: `_rpcReplies` replay
cache). The quota work also fixed a real durability bug found while
writing the test: a QuotaExceededError thrown through the WASM frames
left a phantom document that the next successful write committed
durably — `bridgeHandle` now converts handle exceptions into the
bridge's short-write error contract so C rolls back in-process.

### 7. Leader-side RPC dedup (exactly-once for retried requests) ✅

`db-coordinator.js` retries a timed-out request once — but if the first
request actually executed and only the response was lost, a
non-idempotent write runs twice (the code's own comment at the retry
site acknowledges this). Standard fix: leader caches recent
`requestId → response` and replays instead of re-executing; a bounded
LRU (say, last 128) is plenty at BroadcastChannel scale.

### 8. Compact vs in-flight ops: the op counter ✅

The compaction gate queues operations *issued* mid-compact
(`_compacting`, inlined loop — see its doc comment for why inlined).
Remaining hole: an op whose body awaits internally (e.g. `deleteOne`
with watchers awaits a `findOne` between gate and mutation) can have a
compact *start* inside that window. Unreachable for a single awaiting
caller; reachable on a leader handling concurrent RPCs. Fix: ops
increment an in-flight counter after passing the gate (try/finally),
and `compact()` waits for it to drain before setting `_compacting`.
This closes the last known concurrency hole.

### 9. Quota exhaustion: test it and surface it ✅

The most likely real-world browser failure — OPFS `QuotaExceededError`
mid-multi-file write — has no test. Add a wrapping provider whose
handles throw after N bytes; assert in-process rollback + journaled
recovery leave the collection consistent. Expose a
`db.storageEstimate()` convenience (wraps `navigator.storage.estimate()`
where available) so hosts can warn before writes start failing.

### 10. Abandoned cursors ✅

An unexhausted, unclosed `find()` cursor blocks `compact()` forever and
pins WASM memory. Add a `FinalizationRegistry` safety net that frees
the WASM-side `dc_cursor` when the JS cursor is collected (belt), and
document `close()` as required for early-abandoned streaming cursors
(braces). autoCompact's `skipBusy` already tolerates them; this is
about not leaking forever.

### 11. ChangeStream backpressure ✅

`_emit` pushes to an unbounded `_queue` when a consumer is slower than
the write rate — and the coordinator rebroadcasts every change to every
tab. Pick and document a policy: bounded buffer (default a few
thousand) that on overflow closes the stream with a
`ChangeStreamOverflowError`. No silent unbounded growth, no silent
drops. (Resume tokens are a non-goal for now; say so.)

### 12. On-disk format compatibility contract ✅

The journal has magic+version and B+ tree metadata carries
`version: 1`, but nothing documents what v2 code does with v1 files or
vice versa. Write the one-page policy now (cheap; painful after the
first format change): every file self-identifies; newer readers open
older formats or migrate explicitly; older readers refuse loudly with
the version in the message. Add a version check at `Db.open()` with a
deliberate error, and a test opening a doctored future-version file.

---

## P2 — ecosystem polish (after P0/P1)

**Status: landed 2026-07-16** except scalar `_id`/format v2, which stays
the one open project. Notes: `explain()` is backed by a real C export
(`dc_explain`) consulting the same planners the queries run — and the
spike surfaced that `find({_id})` full-scanned while `findOne({_id})`
point-looked-up, so the `{_id}` fast path now covers the whole shared
dispatch (`gather_matches`/`dc_cursor_open`). `aggregate()` pushes a
leading `$match` into the engine and documents its JS-side subset.
`dump`/`restore` round-trip Extended JSON including index definitions.
`npm run bench` covers both providers.

- ✅ **`aggregate()` subset** — `$match`/`$sort`/`$skip`/`$limit`/
  `$project`/`$group` (basic accumulators), executable JS-side over
  `find()`; covers most reach-for-it moments without touching C.
- ✅ **`explain()`** — even `{ index: 'team_1' | null, scanned: n }` per
  query, so users can tell whether an index was used. The C dispatch
  already knows.
- ✅ **CLI dump/restore** — `db <name> dump > x.jsonl` /
  `restore < x.jsonl` (extended-JSON for ObjectId/Date). Doubles as the
  escape hatch for any future format migration.
- ✅ **Benchmarks** — a small tracked suite (insert/query/index/compact at
  1k/100k docs, node + browser) guarding the WASM-boundary cost from
  regressions.
- **Scalar `_id` support (format v2)** — the P0 #3 spike's finding:
  variable-length primary keys via `keyenc`'s ordered encoding, new
  index-row back-pointer format, migration per
  `docs/format-compatibility.md`'s rules. A real project; the format
  stamp machinery it needs now exists.
- ✅ **Non-goals to state in the README**: cross-collection transactions,
  watch() pipelines/`updateDescription` beyond current scope,
  multi-writer across origins/processes (single-leader by design).

---

## Suggested sequencing

Roughly dependency-ordered; items within a group are parallelizable.

1. **#5 CI** first — everything after it lands with a net.
2. **#2 errors + #6 docs** — small, immediate, unblock nothing but
   improve everything.
3. **#4 Node provider** then **#1 types** (types can then include
   `@mdy-docs/nisaba-db/node`).
4. **#3 scalar `_id`** — the one P0 with real design risk; spike the
   key-encoding question early in case it demotes to "document the
   restriction."
5. P1 in listed order (#7 and #8 are small and close known holes; #9
   and #12 add the missing failure-mode coverage).
