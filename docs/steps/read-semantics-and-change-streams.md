# Next step: read semantics, and change streams that tail the log

A work brief for step 6 of `docs/replicaton-roadmap.md`. Two things, in
one brief, because they are the same question asked twice: what may a
replica tell a reader, and how does a reader learn what changed.

Part one is DECIDED and is written below as a decision rather than a
question. Part two is still design.

## Part one: what a member may serve — DECIDED

**Every read is linearizable. Only the leader serves one, and it serves
it behind a real ReadIndex barrier. A member that cannot prove it leads
refuses rather than answering from what it happens to hold.**

### The finding that forced it

The starting assumption — that a Raft member is either in sync or out of
the cluster — is not true, and the code says so plainly:

- **Commit is a MAJORITY, per entry.** `raft_commit_candidate`
  (`wasm/src/raft_core.c`) sorts every match index descending and takes
  `all[quorum-1]`. Membership in "the quorum" is not a state a node has;
  it is recomputed per entry, and the majority that carries entry 5 need
  not be the one that carries entry 6.
- **Nothing ever ejects a slow member.** The only callers of
  `rn_change_membership` are `handle_join`, `handle_leave` and the
  auto-promotion path, which only ever ADDS voting. A member that is down
  for a week stays a voter with a stale match index. That is deliberate:
  automatic removal lets a network blip shrink the electorate, and a
  cluster that shrinks itself under partition can lose data.
- **Being in the committing majority is about DURABILITY, not
  application.** A follower acks only after `elog_sync`
  (`raft_core.c` — "durable BEFORE the ack"), so it can be one of the
  members that made an entry committable and still not be serving it.
- **So a healthy follower is always behind**, by one round trip plus up
  to one heartbeat (its `leaderCommit` is whatever the last AppendEntries
  said — `raft_follower_commit`) plus up to one apply tick. The leader
  answers a write only after IT applies, so at the instant a client is
  told "inserted", no follower holds it yet.

"In sync" is therefore not something a member can CHECK. It is a barrier
a read has to ESTABLISH. That rules out option 1 outright.

### What was actually shipped before this brief, and was wrong

Two things, and neither was written down anywhere:

- **Followers served reads, with no check at all.** `replica_submit`
  consulted `replica_is_leader` only for writes; a read fell through to
  the instance and was answered from local state. The cluster tests read
  from every member routinely. Staleness was unbounded — a partitioned
  follower answered forever and had no way to know it was partitioned.
- **The leader's reads were not safe either.** A deposed leader that has
  not noticed happily serves state it no longer owns. `hasQuorumContact`
  exists for exactly this and NOTHING in `server/` ever called it.

So this was never "should we add follower reads". It was "we have
unrestricted follower reads and no leader guarantee".

### Why leader-only, given the barrier

Under a linearizable rule a follower read costs a round trip to the
leader for the readIndex, plus the leader's own confirmation round, plus
a wait for the follower's apply pump. It is strictly slower than a leader
read AND still costs the leader a message — so **it does not scale
reads**. The only thing it buys is distributing the query WORK: CPU,
page cache, cursor state. Worth it for a half-second aggregation, pure
loss for a point lookup.

That is a real feature, but it is a different one, and it is not what
was asked for. Read SCALING comes from the asynchronous replica tier
recorded below, which is outside consensus by design. Serving reads from
consensus followers is deferred until there is a workload that wants
work-distribution without wanting staleness.

### Why a fresh round rather than a lease

`hasQuorumContact(withinMs)` is lease-flavoured: "has a quorum answered
within the last N ms". Using it as the confirmation would make this
option 2 with the clock assumption pushed onto the caller. Nothing in
this codebase assumes bounded clock skew anywhere, and a lease would be
the first. So the barrier sends a real heartbeat round and waits for a
quorum to answer it — correct under any skew, any GC pause, any
suspended VM. A lease can arrive later as a documented opt-in, on top of
a correct default.

### The shape

Section 6.4's ReadIndex, with the term-boundary no-op it requires already
in place (`become_leader` appends `EL_NOOP`, so a fresh leader knows its
full committed prefix before it can answer anything):

1. A request arrives. C classifies it: touches nothing, reads, or writes.
2. Not the leader → refuse, reads and writes alike, with the same
   `-63` + `leaderId` + `leader` address a write refusal already carries.
   `ping` is the one exemption: it is the connection keepalive and
   touches no state.
3. Leader, and a read → take `readIndex` = the current commit index, run
   the apply pump so the local state is at least that, execute the read,
   and HOLD the answer.
4. Confirm leadership: a heartbeat round to every voting peer, confirmed
   when a quorum has answered SINCE the barrier was taken. Concurrent
   reads share one round — a burst of sixty-four costs one, not
   sixty-four.
5. Confirmed → send the held answer. Deposed, or an election timeout
   passed without a quorum answering → refuse.

Executing before confirming is deliberate and is the cheaper half of the
standard argument: confirming a quorum still follows us at term T over a
window that STARTED at the barrier proves no later leader existed before
it, so everything committed before the barrier is in our commit index and
the state served is at least that. Serving something NEWER is allowed;
serving something older is not.

A group of one skips the whole thing: there is nobody to hear from, so
the barrier is satisfied at the moment it is taken and the read is
answered outright. An unreplicated server never enters this path at all.

### What it costs, accepted

- **A read is now a deferred answer**, like a replicated write, and rides
  the same machinery — which is also why a connection with a deferred
  answer outstanding is not read from: answers are paired with requests
  by arrival order, and a read that could be overtaken would be handed
  the wrong one.
- **`getMore` barriers too.** A cursor batch is a read and the rule is
  applied uniformly rather than weakened for paging. Paging a large
  result therefore costs one quorum round per batch. The alternative —
  linearizable at open, monotonic thereafter — is a weaker guarantee that
  would have to be argued and documented separately; if paging cost ever
  matters, that is the knob, and it is a deliberate downgrade rather than
  an optimization.
- **A partitioned leader now refuses reads** after one election timeout
  rather than answering from stale state. That is the point.

### Deliberately NOT in this decision

**Asynchronous read replicas**, which fall behind on purpose and are not
part of consensus: no vote, no quorum weight, no effect on commit. That
is where read scaling comes from, it is a separate tier with a separate
and explicitly non-linearizable contract, and it is a later brief. The
rule above must not be weakened to accommodate it — a member of the
consensus group serves current data or refuses, and a member of the async
tier is not a member of the consensus group.

### What still exists to build on

- **`appliedIndex`** per collection (roadmap step 1), so a replica can
  say precisely how far behind it is — which the async tier will need
  even though the consensus tier no longer does.
- **`hasQuorumContact`**, unused by the server, kept for the lease
  variant if it is ever wanted.

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

Part one is done when a follower refuses a read the way it refuses a
write, a leader answers one only after a quorum has confirmed it still
leads, a partitioned leader refuses instead of answering, and the rule
that decides all three lives in C.

Part two is done when `watch()` can resume from an index across a
reconnect, refusing explicitly when that index has been compacted away.

## Suggested staging

1. ~~**Decide the read policy.**~~ Decided, above.
2. **Implement it**: the request classifier and the barrier in C, tested
   directly rather than inferred from cluster behaviour.
3. **Change streams from the log**, host-side reader, C-side rule for
   what is still readable.
4. **Retire or reframe `ChangeStreamOverflowError`** — a log-backed
   stream should not need it. If it survives, say what it now means.

## Invariants

- **A read is current or it is refused.** No member of the consensus
  group answers from state it cannot prove is current. Staleness as a
  labelled option belongs to the async tier, which is not this.
- **Every registered wait terminates.** A barrier is confirmed, lost at
  step-down, or expired on a timer — never left outstanding. The client
  holding the read gets an answer in all three cases.
- **A gap is an error, never a silence.** A consumer that cannot be
  served from its resume point is told so.
- **No new clock assumptions** without writing down what breaks if the
  clock is wrong. Nothing in this codebase currently assumes bounded
  clock skew; a lease would be the first, and that is a real design
  change, not an optimization. The barrier's expiry timer is not one: it
  bounds a wait, it does not license an answer.
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
