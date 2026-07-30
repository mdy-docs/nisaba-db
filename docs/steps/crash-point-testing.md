# Next step: crash-point testing across the replicated write path

A work brief for step 7 of `docs/replicaton-roadmap.md`. The one item on
that list that is not a feature: it is the coverage the rest of the
replication work is resting on.

## Where this sits

The pieces exist separately and have never been pointed at each other:

- **A deterministic simulator.** `test/raft-harness.js` — virtual clock,
  seeded rng, in-memory network with delays, partitions and
  unreachable-peer failures. A failing schedule replays exactly.
- **Crash-window forging, by hand.** `test/db.wal.test.js` ("WAL: crash
  recovery", ~303) builds the window between `sync()` and apply by
  reaching into the log directly, then reopens and asserts the replay. It
  proves the technique; it does not systematically cover the boundaries.
- **The model to copy.** The submodule's
  `third_party/binjson-structures/test/entrylog.durability-wasm.test.js`
  and `durability-wasm.test.js` do byte-level crash-window testing over a
  `MemoryHandle`, which is what makes this cheap: no real files, no real
  clock, exact control of what reached "disk".
- **Who fsyncs, tested.** `test/db.durability.test.js` counts the flushes
  reaching the handle, because a durability regression fails no other
  test — the data still lands, it just stops being durable. That file's
  header is worth reading before writing any of this.

## Goal

Kill the process at every boundary in the replicated write path and prove
the invariant that boundary is supposed to protect.

**Done when** each boundary below has a test that crashes exactly there
and asserts what must still be true, and the suite runs in the simulator
so a failure replays deterministically.

## The boundaries

For one logged command on one node, in order:

1. **after append, before sync** — the entry is buffered, not durable. On
   restart it must be as if the write never happened. Nothing may have
   acked it.
2. **after sync, before apply** — durable and unapplied. This is the
   window `db.wal.test.js` forges today. On restart it must be replayed
   exactly once. (Roadmap step 1's guarantee: replay from
   `dc_applied_index + 1` is exact, never double-applying, because the
   cross-file journal rewinds every file to one consistent commit.)
3. **mid-apply, between the appliedIndex staging and the mutation's
   commit** — the pair that `dc_wal_apply` stages together. A crash here
   must leave the collection either fully holding the mutation and its
   index, or fully lacking both.
4. **mid-batch** — a group-committed batch, half applied. `db.wal.test.js`
   covers the resume; make it systematic across batch shapes.
5. **after apply, before the commit index is persisted** — the advisory
   marker rides the next sync (`elog_set_commit_index`). Re-deriving it
   on restart must not lose or over-claim committed entries.

For the replicated path, additionally:

6. **a follower crashing between accepting entries and acking** — the ack
   must mean what the leader thinks it means, so an unsynced entry must
   never have been acked.
7. **a leader crashing after committing but before applying** — a
   successor must commit the same prefix.
8. **mid-install** — a snapshot install interrupted at each of: staging
   chunks, after the manifest commits, mid-adopt (live files partly
   replaced), after adopt but before the log rebase. This one depends on
   `install-snapshot-in-c.md` landing first, and is the most valuable of
   the set: the adopt step is the only place in the system that replaces
   live files wholesale.

## Shape

`MemoryHandle` is the lever: it is the whole "disk", so a crash is
"stop calling into this handle, then reopen a node over the same bytes".
The harness already does the reopen half — `bootNode(id, ids, sim, net,
member.handle)` in `test/raft-harness.js` deliberately keeps the handle
alive across `stopNode` for exactly this. What is missing is the ability
to stop at a chosen instant rather than at a call boundary.

Two approaches, pick with an argument:

- **A wrapping handle** that fails or freezes after N writes/bytes, the
  way `test/db.quota.test.js` already wraps a provider to throw after N
  bytes. Systematic and cheap; enumerate N.
- **Explicit forging**, as `db.wal.test.js` does: construct the exact
  state a crash would have left. Precise and readable; more work per
  case, and it can encode a wrong belief about what a crash leaves.

The first is better for coverage, the second for the cases where the
interesting state is hard to reach. Both are legitimate; say which you
used where.

## Invariants under test

State them as assertions, not prose, so a failure names the rule:

- **Nothing acked is ever lost.** The chaos test in `test/raft.test.js`
  already asserts this shape across partitions ("every acknowledged write
  survives"); crash-stop needs the same assertion.
- **Nothing unacked is ever visible** as committed on any replica.
- **Replay is exactly-once.** Not idempotence by luck — the appliedIndex
  guard, tested by crashing between stage and commit.
- **Every replica ends identical.** The suite already has this comparison
  (`maps(cluster)` equality); crash-point tests should reuse it verbatim
  rather than inventing a second notion of "same".

## Verification

```
./wasm/build-wasm.sh && npx vitest run
./wasm/build-native.sh                     # ASan/UBSan
```

Multi-seed is the standard here: `test/raft.test.js`'s chaos test runs
seeds 21-24 and asserts convergence plus no lost acknowledged write. A
crash-point suite should do the same — a single seed proves one schedule,
and the whole value of the deterministic simulator is that a failing one
replays.

## A note on what this is for

Every other brief in this directory moves logic. This one buys
confidence in logic already moved, and it is the one whose absence is
invisible until a real deployment loses data. The roadmap has carried it
as "step 7, throughout" since before any of the C work started; it has
not been done, and the honest reason is that features are more
interesting. It should probably be done anyway, and before the system is
trusted with anything.

## Out of scope

Fault injection below the storage provider (torn sectors, lying fsync).
The layer boundary this repository tests to is the provider's contract:
`flush()` is a real fsync (`test/db.durability.test.js` proves who calls
it) and bytes that were flushed stay put. Testing a lying disk is a
different project.
