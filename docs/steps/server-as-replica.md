# Next step: `nisaba-server` becomes a cluster member

A work brief, written to be handed to someone who has not been following
the C-pushdown effort. It says what to build, what already exists so it
is not built twice, what shape the answer has to take and why, and what
must still be true afterwards.

## Where this sits

Every consensus decision is in C. The state machine, the safety rules,
the wire grammar, the leader's bookkeeping, all five message kinds
including the snapshot install, the applier, and — since the
completions work landed — what a proposal's fate is. A process can
run Raft with no JavaScript in it, and a native test proves that.

`server/main.c` mentions Raft **zero times**.

`raft_node.c`, `raft_core.c`, `raft_msg.c`, `raft_drive.c` and
`entrylog.c` are all linked into the binary already. Nothing constructs a
node. There is no log in the process, no peer socket, and no apply pump.
The server opens a `dbs`, reads a frame, calls `dbs_handle`, writes the
answer back — a single-writer database with a socket on it.

This brief is the missing middle: one process that is a REPLICA. Not
several groups, not a seat, not a fleet — that is
[`native-composition.md`](native-composition.md), and it sits on top of
this.

## Goal

`nisaba-server --raft` joins a cluster, replicates every write through
it, and serves reads from its own copy.

**Done when** three `nisaba-server` processes on one machine elect a
leader, replicate writes, survive the leader being killed, and catch a
restarted member up — with no JavaScript in any of them — and the
existing single-process mode is byte-for-byte unchanged when the flag is
absent.

## The fork this is really about

Read `db_session.h`'s comment on `dbs_handle` before designing anything.
The relevant sentence is about writes:

> Writes go through `dc_wal_plan_build` and `dc_wal_apply` — **the same
> path a replicated write takes**, so every mutation this serves is one a
> log could have carried.

So the command grammar a leader would send **already exists on both
sides**, and `dbs_apply` already performs a committed command of any
kind — documents and DDL alike — against a session. What `dbs_handle`
does today is plan and then immediately apply, in one call, because there
is nobody to ask.

A replica splits that in half:

```
   today            dbs_handle:  plan ──────────────► apply ──► answer

   replicated       dbs_handle:  plan ──► propose ──┐
                                                    │  (quorum, a while later)
                    apply pump:              ┌──────┘
                                             └────► dbs_apply ──► answer
```

**The two halves are already written.** The seam is deciding where the
request stops and how the answer finds its way back to a socket that has
been waiting.

That is the whole design problem, and it has a wrong answer: doing the
plan on the leader and shipping the REQUEST would let two replicas
resolve an upsert's id or `$currentDate` differently. Nondeterminism is
resolved at proposal time, once, by the node that took the request —
that is the rule `db_wal.h` and `src/db-wal.js` were both built to, and
it does not change here.

## What answers the client

`RN_EFFECT_SETTLED` carries `(index, kept)`. The server holds a table of
`index -> connection`, and when a settlement arrives it writes the
response frame to that socket. `kept == 0` is the honest refusal: a new
leader overwrote the entry, and the client must retry.

That table is the native counterpart of `_waiters` in `src/raft.js`, and
it is the reason the completions work came first. It is also bounded —
`RN_MAX_AWAIT` proposals in flight, refused explicitly when full, which
the connection table already has a shape for.

**A write on a follower is refused with the leader's address**, not
forwarded. The node already knows both (`rn_adopted` carries records,
addresses included) and already answers a join with that redirect. One
refusal shape, one place it is decided.

## Do not build these — they exist

| Piece | Where |
| --- | --- |
| Everything Raft decides | `wasm/include/raft_node.h` and what it sits on |
| Applying a committed command of any kind | `dbs_apply` (`wasm/include/db_session.h`) |
| Planning a write into a command | `dc_wal_plan_build` (`wasm/include/db_wal.h`) |
| The log, on a real file | `entrylog.h` over `bjio_posix.h` |
| Snapshot generations, naming, adoption | `snapstore.h`; the node drives it itself given a `bj_ns` |
| A `bj_ns` over the preopened directory | `bjns_posix_open` — `main()` already opens one |
| A poll loop with non-blocking sockets and idle reclamation | `serve_forever` in `server/main.c` |
| Framing | self-delimiting binjson; `frame_total` in the same file |

The snapshot half needs no new work at all: give the node the `bj_ns`
`main()` already has and an `sst` over the same directory, and it serves,
receives and adopts installs by itself. Under POSIX the plan beat is
pure bookkeeping — `openat` is synchronous — so `rn_install_plan` and
`rn_adopt_plan` are called and their names opened on demand.

**The adoption still needs the host's close and reopen.** `dbs_close`
then `dbs_open` around `rn_adopt`, with no request served in between.
That is `dc_compact_execute`'s bargain and it does not get cheaper here.

## What has to be built

1. **A log in the process.** Open `<prefix>-log-<gen>.bj` or the plain
   WAL through the namespace, exactly as `openWalStorage` decides it in
   JavaScript — the rule is "the store's paired log if a generation has
   been adopted, otherwise the WAL", and it is currently written down
   only in `src/db-wal.js`. Consider moving it to `snapstore.h`, where
   the other half of the naming already lives.
2. **A peer transport.** Length-prefixed binjson frames over TCP, the
   same shape `src/raft-transport-tcp.js` uses, because a native member
   and a Node member should be able to sit in one cluster. Its wire
   format is written down in that file's header and nowhere else.
3. **A tick in the poll loop.** `rn_tick(now)` on a deadline, alongside
   the client and peer fds. `poll()`'s timeout is the only clock here.
4. **The apply pump.** Drain `RN_EFFECT_COMMIT`, read entries with
   `elog_get_batch`, hand each to `dbs_apply`, call `rn_applied`. A
   deterministic failure is a RESULT (`dc_is_deterministic`); anything
   else halts this replica rather than letting it diverge.
5. **The propose fork in the request path.** The one real design change.

## Ordering

Nothing here needs `native-composition.md`, and doing this first is what
makes that brief answerable: it asks which of `RaftGroupHost`'s policies
belong in C, and a native server that has actually needed them is a much
better judge than a reading of the JavaScript.

## Invariants that must hold

- **One owner per fact.** The member set, the addresses, the quorum
  arithmetic and the completion rule are the node's. This layer reads
  them back rather than keeping a second copy — `src/raft.js` is the
  worked example of how much that removes.
- **Nondeterminism is resolved at proposal time**, by the node that took
  the request, never at apply. Two replicas that resolve an upsert id
  differently have forked.
- **A deterministic failure is an answer, not a halt.** Every replica
  computes the same duplicate-key refusal; only the apply pump can tell
  the difference, and `dc_is_deterministic` is how.
- **Every registered wait terminates.** A connection holding a request
  gets an answer at step-down, at halt and at close. The node settles its
  own (`RN_EFFECT_SETTLED`); the socket table must not lose one.
- **Bounded, and it says so.** Like every other table in the server: a
  fixed size, an explicit refusal when full.
- **The flag is off by default and changes nothing.** A directory served
  without `--raft` behaves exactly as it does today, including its file
  layout, so a single-process database can be joined to a cluster later
  rather than re-created.
- **Falsify both ways.** Break the fix, watch the specific test fail,
  restore it, and say so in the commit message.

## Known hazards

- **One process per directory is load-bearing.** wasi-filesystem has no
  locking; the server's answer to concurrent writers is that there is
  never more than one. A replica does not change that — the log and the
  snapshot store live in the same directory, and the same single process
  owns all of it.
- **`elog_get`'s payload is log-owned** and dies on the next operation on
  that log. Copy before applying. (This produced a double-free in a
  native test; ASan caught it.)
- **A `dbs` handle spans the adoption.** Every cursor and change stream
  it holds is invalidated by an install. The read gate is a browser
  concept; here the equivalent is refusing to adopt while a cursor is
  open, or closing them with a code that says why.
- **`--stdio` has no sockets.** `wasm32-wasip1` cannot join a cluster at
  all; the flag must refuse there rather than half-work.
- **Effects cannot carry buffers.** `(kind, arg, flag)` only. Names come
  back through a plan call.

## Verification

```
./wasm/build-native.sh                 # ASan/UBSan
./wasm/build-server.sh --native        # and --wasip1, --wasip2
./wasm/build-wasm.sh && npx vitest run
```

The three-process test belongs in `test/db.server.test.js`, beside the
ones that already spawn the binary: boot three, write, kill the leader,
write again, restart the dead one, and assert every replica agrees. A
JavaScript test harness driving C processes is not a contradiction — it
is a client, which is what the wire is for.

## Out of scope

Several groups in one process, the seat model, and the monitor
(`native-composition.md`). Client-facing HTTP
([`http-front-end.md`](http-front-end.md)). Follower read policy
(`read-semantics-and-change-streams.md`) — until it is decided, a
follower serves its own copy and says so, exactly as `ReplicatedDb` does.
