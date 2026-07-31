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
| 3 | [native-composition.md](native-composition.md) | A server binary: seat, sockets, and deciding what is policy. Wants 1 and 2 done. |
| 4 | [read-semantics-and-change-streams.md](read-semantics-and-change-streams.md) | Roadmap step 6. Follower reads, and change streams that tail the log. Independent of 1-3. |
| 5 | [crash-point-testing.md](crash-point-testing.md) | Roadmap step 7. Confidence in everything already built. Its install cases want 1 done; the rest do not wait. |

1 and 2 are the C pushdown's remaining substance; 3 is what turns it into
a product; 4 is a feature; 5 is the coverage all of it rests on.

## Standing debts

Independent of the above. Not a design question; known, diagnosed and
written down rather than fixed on the spot.

| Brief | What it is |
| --- | --- |
| [wasi-locally.md](wasi-locally.md) | `--wasi` only runs on a CI runner today, so the one memory model resembling the shipped one is checked minutes-to-days after the code is written. Make it runnable on a developer machine at CI's pinned version. |

Do this first if you want the ground to stop moving: it restores a check.

The other debt — compaction leaking the OPFS handles it pre-opened, which
kept three browser tests red — is fixed: `compact()` gives them back, and
`test/db.exclusive-handles.test.js` now enforces the browser's
one-handle-per-file rule in the Node suite, where that whole class of bug
used to be invisible.

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
