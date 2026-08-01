# Steps

One brief per piece of remaining work, each written to be handed to
someone who has not been following the effort. They say what to build,
what already exists so it is not built twice, the shape the answer has to
take and why, and what must still be true afterwards.

They are briefs, not specifications: where a design decision is genuinely
open, the brief says so and names the deliverable as "decide it, and
write down why" rather than guessing on the implementer's behalf.

## The remaining work, in dependency order

| # | Brief | What it unblocks |
| --- | --- | --- |
| 1 | [install-snapshot-in-c.md](install-snapshot-in-c.md) | The last Raft message kind a host must answer. Gives the node a file namespace, which everything below also wants. |
| 2 | [completions-in-c.md](completions-in-c.md) | Answering a proposal without a promise. Small; can go before or beside 1. |
| 3 | [native-composition.md](native-composition.md) | Seating several Raft groups over sockets, and deciding what is policy. Sits on top of the server; wants 1 and 2 done. |
| 4 | [read-semantics-and-change-streams.md](read-semantics-and-change-streams.md) | Roadmap step 6. Follower reads, and change streams that tail the log. Independent of the rest. |
| 5 | [crash-point-testing.md](crash-point-testing.md) | Roadmap step 7. Confidence in everything already built. Its install cases want 1 done; the rest do not wait. |
| 6 | [http-transport.md](http-transport.md) | The server speaking HTTP instead of raw frames, so a browser can be a client of it without a translating gateway. Follows from the browser decision below; independent of 1–5. |

1 and 2 are the C pushdown's remaining substance; 3 turns one server into
a cluster; 4 is a feature; 5 is the coverage all of it rests on; 6 is
what the browser decision below costs on the server side.

**The database server is built**, which is why there is no brief for it:
one process per database directory, binjson over sockets, as a
`wasm32-wasip2` command, documented in
[`docs/db-server.md`](../db-server.md). It delivered
`wasm/include/db_session.h` — resolving a collection by name in C — which
is the piece 1 and 2 were both blocked on, and it is what 3 sits on top
of. It has since grown a bounded `poll()` multiplexer with idle
reclamation, paged cursors, creation and schema, compaction and sweeps,
lists of writes, aggregation, and change streams — thirty-one
operations, which is everything the in-process `Collection` and `Db`
have except `storageEstimate` (a browser API, not a database one). What
it still does not do is listed there rather than here, because those are
properties of a thing that exists.

**The browser question is decided: BOTH.** The browser stays a *host* —
an OPFS-backed local database, the thing you reach for instead of
IndexedDB — *and* gains a client for talking to a database elsewhere
over REST/HTTP. It was posed as either/or below because one of the two
answers would have removed a large constraint from the C API; the answer
is that the constraint stays, and is paid for deliberately.

What that costs, stated plainly: **plan/execute is permanent.** OPFS
`getFileHandle` and `createSyncAccessHandle` both return promises and
wasm cannot block on one, so `bj_ns.open` must stay synchronous, the
pre-open scope table stays, and "effects cannot carry buffers" stays.
Every host that came later — Node, native, `wasm32-wasip2` — pays a
discipline it does not need, so that one host can exist at all. That is
the price of a local-first database that works offline in a browser,
which is what this library is pitched on, and it is worth paying.

What it does *not* cost: the client half needs nothing from C. The
server's own client (`src/db-server-client.js`) already demonstrates the
shape — a socket, the pure-JS codec, no WASM, no `ready()`, thirty-one
operations, one file. A REST/HTTP client is that file with `fetch` where
`net.connect` is, and the gateway in front is the parent project's
(`docs/replicaton-roadmap.md` step 4 records that boundary). The two
halves meet at the same `Db`/`Collection` shape, which is what makes
"both" cheap rather than two products.

The separation below is still worth keeping, because the two constraints
get conflated whenever this comes up:

- **Exclusive sync access handles per file** is the multi-tab one. It
  produced `src/db-coordinator.js` (elect one tab, others RPC to it) and
  the handle-ownership discipline. This is not a wart: it is the same
  **one writer per database directory** invariant the server gets from
  process ownership and Node gets from an advisory lock. Multi-tab forced
  it to be discovered early, in the environment that will not let you
  cheat.
- **Asynchronous opens** is the expensive one, and it is *not* multi-tab.
  OPFS `getFileHandle` and `createSyncAccessHandle` both return promises,
  and wasm cannot block on a promise without Asyncify or JSPI. That one
  fact forces the plan/execute split, `bj_ns.open`'s synchronous
  requirement, the pre-open scope table, and "effects cannot carry
  buffers". It would hold with exactly one tab open.

So solving multi-tab differently buys the removal of one JS file and one
discipline; it does not buy back plan/execute. Making the browser a
client *only* would have: every remaining host would be POSIX-shaped,
`bj_ns.open` would be `openat`, and C could own the file lifecycle end
to end — which is precisely what brief 1 wants and cannot have. That
trade was refused above, so brief 1 must work within plan/execute rather
than waiting for it to go away.

## Standing debts

Both are paid. Neither was a design question — known, diagnosed, and now
fixed.

- **The compaction handle leak.** `compact()` never closed the OPFS
  handles it pre-opened, so the adopt step could not re-open its own
  files and three browser tests were permanently red. It gives them back
  now, and `test/db.exclusive-handles.test.js` enforces the browser's
  one-handle-per-file rule in the Node suite, where that whole class of
  bug used to be invisible.
- **`--wasi` on a developer machine.** It ran only on a CI runner.
  `./wasm/get-wasi-sdk.sh` fetches the pinned toolchain for the host,
  `./wasm/build-native.sh --wasi` finds it with nothing exported, and CI
  installs it by running that same script — so the pin lives in one
  place (`wasm/build-common.sh`). It runs under wasmtime too, which
  immediately found a durability bug Node's WASI host had been passing
  over.

## Shared context every brief assumes

- `docs/replicaton-roadmap.md` — the replication plan and its recorded
  decisions, including the ones deliberately deferred.
- `docs/clustering.md` — how membership, join/leave and learners behave
  from the outside.
- `wasm/include/raft_node.h` — the seam the whole effort turns on: C
  decides, the host delivers, C consumes the answer. Its header explains
  why there are no promises in it.
- `third_party/binjson-structures/include/bjns.h` — the file-namespace
  discipline (C plans, the host opens, C executes) and the reason it is a
  discipline rather than a mechanism.

## House rules, stated once

Every brief repeats the ones it needs, but they are the same everywhere:

- **One owner per fact.** If two components could disagree, remove the
  opportunity rather than checking for it afterwards.
- **All or nothing.** A refusal leaves the previous state exactly as it
  was.
- **Nothing is dropped in silence.** Every refusal is a distinct code
  with text in `dc_strerror`, and a queue that can overflow says when it
  did.
- **Falsify both ways.** Break the fix, watch the specific test fail,
  restore it, and say so in the commit message.
- **The build is the test.** `./wasm/build-native.sh` (ASan/UBSan) and
  `./wasm/build-wasm.sh && npx vitest run` both stay green, and both
  should grow.
