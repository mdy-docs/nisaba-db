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
| 1 | [wasip2-database-server.md](wasip2-database-server.md) | A real server: one process per database directory, binjson over sockets, as a wasip2 command. Needs neither 2 nor 3, and delivers the collection-by-name resolution that 2 is blocked on. |
| 2 | [install-snapshot-in-c.md](install-snapshot-in-c.md) | The last Raft message kind a host must answer. Gives the node a file namespace, which everything below also wants. |
| 3 | [completions-in-c.md](completions-in-c.md) | Answering a proposal without a promise. Small; can go before or beside 2. |
| 4 | [native-composition.md](native-composition.md) | Seating several Raft groups over sockets, and deciding what is policy. Sits on top of 1; wants 2 and 3 done. |
| 5 | [read-semantics-and-change-streams.md](read-semantics-and-change-streams.md) | Roadmap step 6. Follower reads, and change streams that tail the log. Independent of the rest. |
| 6 | [crash-point-testing.md](crash-point-testing.md) | Roadmap step 7. Confidence in everything already built. Its install cases want 2 done; the rest do not wait. |

1 is the product shape, and its first step is the piece 2 and 3 are both
waiting on; 2 and 3 are the C pushdown's remaining substance; 4 turns one
server into a cluster; 5 is a feature; 6 is the coverage all of it rests
on.

**One decision is deliberately left open**, and 1 says why rather than
answering it: whether the browser stays a *host* (owning OPFS files) or
becomes a *client* of a server. It is a product decision, it is worth
making explicitly, and almost every awkward constraint in the C API
descends from the answer.

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
