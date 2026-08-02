# Next step: read semantics, and change streams that tail the log

A work brief for step 6 of `docs/replicaton-roadmap.md`. Two things, in
one brief, because they are the same question asked twice: what may a
replica tell a reader, and how does a reader learn what changed.

Unlike its sibling briefs, most of this is DESIGN not yet decided. The
roadmap records the options and defers the choice; the first deliverable
is the choice, argued.

## Part one: what a follower may serve

Today: leader reads only. `docs/replicaton-roadmap.md` step 4 recorded
the decision as "leader reads by default; stale follower reads as a later
opt-in (readPreference-style); readIndex only if demand materializes."

What already exists to build on:

- **`hasQuorumContact(withinMs)`** (`src/raft.js`, backed by
  `rn_has_quorum_contact`). A leader that cannot prove it still reaches a
  quorum answers false. Its doc comment states the exact reason it
  exists: "a caller that reads the leader's local state without
  committing gets a stale answer presented as authoritative". That is the
  whole safety argument for leader reads, already implemented and tested
  (`test/raft.test.js`, "check-quorum").
- **`appliedIndex`** per collection (roadmap step 1), so a replica can
  say precisely how far behind it is.

The decision to make, with a recommendation expected:

1. **Stale-ok follower reads.** Cheapest. A read served by any replica,
   with its `appliedIndex` exposed so the caller can reason about
   staleness. Needs an opt-in on the read path (a `readPreference`-shaped
   option) and honest documentation that it is monotonic-per-replica and
   nothing more.
2. **Leader leases.** A leader that has heard from a quorum within a
   bounded window serves reads without a round trip. Needs a clock
   assumption, which this codebase has so far refused to make anywhere —
   note that `hasQuorumContact` deliberately takes the window from its
   CALLER rather than assuming one.
3. **ReadIndex.** Correct without clock assumptions, costs a round trip
   per read barrier. The roadmap says "only if demand materializes".

Whichever is chosen, the rule that decides whether a read is servable
belongs in C, for the reason every other rule did: two hosts must not
answer the same question differently. The *serving* stays where reads
already are.

## Part two: change streams, tailing the log

`watch()` exists and is entirely JS-side: an in-process hook firing after
each committed write, rebroadcast to other tabs by
`src/db-coordinator.js`. `docs/db-plan.md`'s milestone 13 says plainly
that real MongoDB tails the oplog and that there was "no analog" here,
and roadmap P1 #11 records that resume tokens are a non-goal *for now*.

Both premises have changed:

- The entry log IS the analog. Every mutation is a numbered, durable,
  ordered entry.
- The applier is in C (`dc_wal_apply`) and returns a result per command,
  so what a change event says can be derived from the log rather than
  observed from a side channel.

That makes a structurally better change stream possible:

- **Resumable.** The log index IS the resume token. A consumer that
  reconnects says "from index N" and gets everything after it, exactly
  once, in order.
- **Gap-free.** A stream that falls behind does not silently drop; it
  reads from the log at its own pace. The current design cannot do this —
  `ChangeStream` has a bounded queue and closes with
  `ChangeStreamOverflowError` on overflow, which is the honest answer
  available to an in-memory fan-out and not a good one for a consumer.
- **Cluster-wide.** A follower can serve a change stream, because it has
  the same log.

The bound to respect: a resumable stream is only resumable back to the
log's base index. Snapshots compact the log (`docs/compaction.md`,
roadmap step 3), so a consumer resuming from before the boundary must be
told it cannot be served rather than served a gap. That refusal is the
one new rule this part needs, and it belongs in C with the rest of the
log's rules.

## Goal

**Done when** the read policy is chosen and documented with its argument;
the rule that decides whether a given replica may serve a given read
lives in C; and `watch()` can resume from an index across a reconnect,
refusing explicitly when that index has been compacted away.

## Suggested staging

1. **Decide the read policy.** One page in
   `docs/replicaton-roadmap.md` step 6, replacing the deferral. Include
   the option not chosen and why.
2. **Implement the servability rule in C**, tested directly rather than
   inferred from cluster behaviour.
3. **Change streams from the log**, host-side reader, C-side rule for
   what is still readable.
4. **Retire or reframe `ChangeStreamOverflowError`** — a log-backed
   stream should not need it. If it survives, say what it now means.

## Invariants

- **A stale read must be labelled, not silent.** If a follower serves a
  read, the caller can tell how far behind it was. Anything else presents
  staleness as authority, which is exactly what `hasQuorumContact` exists
  to prevent on the leader.
- **A gap is an error, never a silence.** A consumer that cannot be
  served from its resume point is told so.
- **No new clock assumptions** without writing down what breaks if the
  clock is wrong. Nothing in this codebase currently assumes bounded
  clock skew; a lease would be the first, and that is a real design
  change, not an optimization.
- **The usual house rules** — one owner per rule, all or nothing,
  nothing dropped in silence, ordering is the contract, explicit errors
  with `dc_strerror` text, falsify both ways.

## Verification

```
./wasm/build-native.sh
./wasm/build-wasm.sh && npx vitest run
```

The simulator (`test/raft-harness.js`) is where the interesting tests
live: a follower serving a read at a known lag, a stream resuming across
a leader failover, a resume point that a snapshot has compacted away.
`test/db.test.js`'s existing `watch()` coverage is the compatibility bar
— whatever this becomes, the single-node behaviour those tests describe
should still hold or be deliberately, visibly changed.

## Out of scope

Offline writes. `docs/replicaton-roadmap.md` step 4 is explicit: they
will never fit Raft, and when wanted they arrive as a queued-replay layer
with an app-visible conflict policy on top. Do not let a read-preference
discussion drift into one.
