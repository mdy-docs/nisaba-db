# Next step: joining a native cluster

A work brief, written to be handed to someone who has not been following
the effort. It says what to build, what already exists so it is not built
twice, the shape the answer has to take and why, and what must still be
true afterwards.

## Where this sits

`nisaba-server --raft ID --raft-port N --peer ID@HOST:PORT` is a Raft
member, and its member set is whatever the process was started with.
Growing a cluster therefore means restarting every member with a longer
`--peer` list — and every member has to be given the SAME list, because a
member missing from one node's list is a vote that node will never count.
That is the last thing about a native cluster that is administered by
argv rather than by the log.

**The node already does the hard half, and has never had a caller in C.**
`rn_handle` answers `join` and `leave` itself
(`wasm/src/raft_node.c`'s `handle_join` / `handle_leave`), which landed
before the server was a replica. What it decides:

- **Redirect** if this node is not the leader, carrying the leader's id
  and the address out of its member record — a joiner knows addresses and
  not ids, so an id alone would send it back to the seed it just asked.
- **Idempotence**: a re-join naming a record identical to what the log
  already says is answered with the current members and changes nothing,
  which is what makes a retried join harmless.
- **Busy** (`RAFT_ERR_BUSY`) while a change is in flight, which is "ask
  again", not a failure — changes serialize because the single-server
  safety argument rests on it.
- **A new member always enters as a LEARNER**, whatever it asked for, and
  the leader promotes it automatically once its match index proves it
  caught up (`RN_EFFECT_PROMOTE`).
- **The reply is DEFERRED** until the CONFIG entry lands, parked in the
  node — and `flush_pending` settles every parked requester on step-down
  too, so a joiner whose leader died gets a redirect rather than a hang.

`raft_node.h`'s own text is stale about this: it still says "Only the two
hot kinds are answered here (RequestVote, AppendEntries)". Fix that
sentence while you are in there — it is the reason this looked like
missing work rather than an unused seam.

## Goal

A `nisaba-server` that joins a running cluster knowing only a seed
ADDRESS, and leaves one cleanly.

**Done when** two members are running, a third started with
`--join HOST:PORT` and no `--peer` list becomes a voter and receives a
write made before it existed; when that third member is killed and
restarted it is still a voter; and `--leave` removes a member so that the
survivors' quorum arithmetic changes with it.

## Do not build these — they exist

| Piece | Where |
| --- | --- |
| The join/leave grammar | `raft_msg.h`: `rmsg_join_member`, `rmsg_leave_id`, `rmsg_build_membership_reply` |
| Answering a join or a leave | `rn_handle` → `handle_join` / `handle_leave` |
| Learner entry, and promotion on match index | the node; `RN_EFFECT_PROMOTE` |
| One change at a time | `rn_config_in_flight`, `RAFT_ERR_BUSY` — a safety rule, not a policy |
| Settling a parked requester at step-down | `flush_pending` |
| What a seed loop has to decide | `seedRequest` (`src/raft-host.js`): which reply retries, which follows a redirect, which gives up because a validation refusal will never heal |
| Frames, connections, redial, timeouts | `server/peers.h` |
| Addresses, in the member records | `server/replica.c`'s `put_member` already writes `host` and `port`; `rn_adopted` carries them back |

## What has to be built

1. **A one-shot call to a bare ADDRESS.** `peers_request` needs an id in
   the peer table, and a joiner has no ids — it has seeds. The shape is
   `src/raft-transport-tcp.js`'s `callAddress`: dial, send, await one
   reply, hang up. Explicitly not for steady-state traffic, which is what
   the pooled path is for.
2. **The seed loop, in C.** Try each seed, follow a redirect that names
   the leader's address, retry through an election or a change in
   flight, and stop on a validation refusal because it will never heal.
   Every one of those rules is already stated by the replies the node
   builds; reading them here must not become a second opinion about what
   they mean.
3. **The address table has to FOLLOW the membership.** `peers_add` is
   startup-only today. When a CONFIG entry applies the adopted set
   changes, and a member the transport has no address for is a member
   nothing can replicate to — the failure is silent and looks like a slow
   follower. `TcpRaftTransport` has `setPeer`/`removePeer` for exactly
   this. The addresses are already IN the records, so this is reading the
   node back rather than keeping a second table.
4. **A deferred reply needs a conversation that outlives its request.**
   This is the one real gap in what exists, and it is worth reading
   `flush_out` in `server/replica.c` before designing anything. A reply
   goes back on `r->answering`, which is set for the duration of one
   `rn_handle` call and cleared after it. `handle_join`'s answer is
   deferred — possibly by seconds, until the CONFIG entry commits and
   applies — so it arrives with no conversation open and today lands in
   the "a reply with no conversation to send it on" branch, which exists
   precisely so this would be loud rather than silent when it happened.
   What it needs is a correlation-id → conversation table with the same
   discipline as the pending-write table: bounded, refused explicitly
   when full, and **every entry terminates** — the node guarantees its
   half through `flush_pending`, and the transport must not lose the
   other.
5. **The flags.** `--join HOST:PORT`, repeatable for several seeds, and
   `--leave`. Decide what a member started with BOTH `--peer` and
   `--join` means — refuse it, or state which wins — rather than letting
   argv order decide.

## What this does NOT need, and why that is worth saying

**Snapshots.** It is natural to assume a joiner needs one, and today it
does not: nothing in the server compacts its log, so the leader's log
base stays at zero forever and a joiner with an empty log is caught up by
plain `AppendEntries` from index 1. That is O(the whole history) and it
is correct.

What makes snapshots mandatory is the log GROWING WITHOUT BOUND, which is
a real debt and a different one: a long-lived member's `__wal__.bj` is
every write it has ever taken. Paying it means compaction, then a
snapshot store in the process (`rn_set_snapstore` — the node already has
the `bj_ns` it needs), and then the log-naming rule that follows from it
("the store's paired log if a generation has been adopted, otherwise the
WAL", written down only in `src/db-wal.js`). **Do not let this brief grow
into that one.** Do note, in the commit, that a cluster which can be
joined has one more reason to want it.

## Invariants that must hold

- **A new member is a LEARNER.** Adding capacity must never thin the
  failure margin: a cluster of three that admitted a fourth as a voter
  immediately would need a quorum of three, from a member whose log is
  empty and which cannot help. When it is promoted is the node's
  decision, on match index. Do not add a host-side opinion about it.
- **One change at a time**, and busy means ask again.
- **Every registered wait terminates.** A joiner holding a request gets
  an answer when the change lands, when leadership moves, and when the
  node stops.
- **One owner per fact.** The member set and the addresses are the
  node's. This layer reads them back; it does not keep a copy that could
  disagree.
- **The transport frames, it does not interpret.** It has never read a
  field of a Raft message and must not start; the grammar is
  `raft_msg.h`'s.
- **Peers must be DIRECT addresses.** A load balancer in front of a
  member breaks node identity — `src/raft-transport-http.js` documents
  this and a native seed list has the same rule.
- **Nothing is dropped in silence.**
- **Falsify both ways.** Break the fix, watch the specific test fail,
  restore it, and say so in the commit message.

## Known hazards

- **A joiner is not a member yet, so it has no id to be addressed by.**
  The node queues its answer against peer 0 and the correlation id alone
  — `raft_node.h`'s outbox comment calls this out as its one deliberate
  exception. A transport that routed replies by peer id would lose it.
- **A retried join must stay harmless.** The node makes it so for an
  IDENTICAL record. A joiner that treats a timeout as failure and rejoins
  with a DIFFERENT address has made a real membership change, and that is
  the joiner's bug to not have.
- **A restarted member's set comes from its log, not from argv.**
  `replica_open` adopts the `--peer` list unconditionally today, which is
  correct while argv is the only source. Once membership is in the log,
  argv is a BOOTSTRAP and the log is the truth — a member restarted with
  a stale `--peer` list must not overwrite what the log says. Decide the
  precedence explicitly; it is the same class of mistake as a second
  address book.
- **`seedRequest`'s envelope carries a group.** It sends
  `{ group, msg }` because `RaftGroupHost` multiplexes; a native member
  has one group and no envelope. Do not copy the wrapper along with the
  logic: the frame's `env` is the message and nothing around it, which is
  what lets a Node member and a C member share a cluster today.
- **`--stdio` cannot join anything**, for the reason it cannot be a peer:
  there is no poll loop there to serve one with.

## Ordering

Nothing blocks it. The server holds an instance now, with ONE log for
every database in it, so there is a single member set and "is a join per
instance or per database" never arises.

## Verification

```
./wasm/build-native.sh                 # ASan/UBSan
./wasm/build-server.sh --native        # and --wasip1, --wasip2
./wasm/build-wasm.sh && npx vitest run
```

Beside the three-process cluster already in `test/db.server.test.js`:
boot two members, join a third knowing only a seed address, assert it
becomes a voter and holds a write made before it existed; restart it and
assert it is still a voter without a `--peer` list; make one leave and
assert the survivors' quorum changed with it. A JavaScript test driving C
processes is a CLIENT, which is what the wire is for.

## Out of scope

Snapshots and log compaction, for the reason above. Tenancy. Automatic
membership — deciding to add or remove a member is a deployment's job,
and a database that made that decision for itself would be a database
with an opinion about capacity.
