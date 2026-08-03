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
| 1 | [http-front-end.md](http-front-end.md) | Decision B: clients reach a cluster over HTTP, through a Node process that routes writes to the leader. |
| 2 | [read-semantics-and-change-streams.md](read-semantics-and-change-streams.md) | Roadmap step 6. **Part one is decided and built** (linearizable leader-only reads); what remains is change streams that tail the log. |
| 3 | [crash-point-testing.md](crash-point-testing.md) | Roadmap step 7. Confidence in everything already built. |

1 is how a client reaches a cluster; 2 is a feature; 3 is the coverage
all of it rests on.

**Read semantics are settled**, which is half of brief 2. Every read is
linearizable, the LEADER alone serves one, and it serves it behind a
real ReadIndex barrier — a fresh heartbeat round to a quorum, not a
lease, because nothing here assumes bounded clock skew and a lease would
be the first thing that did. A follower refuses a read with the same
`-63` it refuses a write with; a leader that cannot prove it still leads
refuses with `-66` rather than answering from a log a newer leader may
have moved past.

That closed two holes that had shipped without ever being written down:
reads were served by ANY member with no check at all, and
`hasQuorumContact` — written for precisely this — was never called by
the server. Read SCALING was never what follower reads would have
bought: under a linearizable rule a follower read still costs the leader
a round trip, so it distributes query work and nothing else. Scaling
comes from an asynchronous replica tier outside consensus, deferred and
explicitly non-linearizable.

**They are load-bearing rather than speculative.** They were written
assuming the C server would one day be the cluster member; that is
decided ([`../deployment-shapes.md`](../deployment-shapes.md),
Decision A). One program covers "a persistent server" and "a replicated
server".

**Six briefs retired recently, which is why they are not listed.**

**A native cluster can be JOINED**, which is why that brief is gone.
`nisaba-server --raft 4 --raft-port 9004 --join 127.0.0.1:9001` is a
member of a running cluster, knowing one ADDRESS and nothing else: it
follows the redirect to the leader, is admitted as a LEARNER, is caught
up by ordinary `AppendEntries`, and is promoted to a voter by the
leader's own bookkeeping once its match index proves it current.
`--leave ID` removes one and exits.

The node had answered `join` and `leave` since before the server was a
replica; what was missing was a caller. Three things had to be built and
one of them was a real gap: a one-shot call to a bare address
(`peers_call` — a joiner has seeds, not ids), the seed loop in C
(`server/join.c`), and **a conversation that outlives its request**. A
join's answer is a fact about a `CONFIG` entry that does not exist yet,
so `rn_handle` parks the requester and queues nothing; the reply comes
out of a later call. `server/replica.c` keeps a bounded correlation
table for it, keyed on ids it mints itself rather than the sender's,
because two joiners both send id 0.

**Argv is now a bootstrap and the log is the truth.** The last `CONFIG`
entry wins at startup and at every apply, so a member restarted with a
stale `--peer` list cannot overwrite what the cluster agreed and a
joined member needs no list at all — and the transport's address table
follows the adopted set rather than being a second copy of it. A member
whose address the transport lacks is a member nothing can replicate to,
and the failure is silent: it looks exactly like a slow follower.

Snapshots are still not needed for this and that is worth repeating:
nothing compacts the log, so the leader's base stays at zero and an
empty joiner is caught up from index 1. What makes them mandatory is the
log growing without bound — a standing debt, and a cluster that can be
joined has one more reason to want it paid.

**The server holds an INSTANCE**, which is why that brief is gone. One
process, one root directory, a subdirectory per database, and one
connection that reaches all of them:

```js
const client = await connectServer('127.0.0.1:8097');
const analytics = client.db('analytics');
const billing   = client.db('billing');
```

`client.db(name)` sends nothing — it is a handle, exactly as
`Client.db(name)` is in process — because the CONNECTION is not stateful
about which database. Every request names its own in a `db` field, which
is the only reason two handles can be held at once and interleaved, and
the reason there is no "use" op. A request naming none is refused rather
than given a default. `listDatabases` and `dropDatabase` arrived on the
wire and in `bin/db.js`, whose first word now means the same thing over
`--server` as it does locally.

**Replication follows the INSTANCE**: one log, one leader, one member
set, one failover story for the executable. A log entry gained an
envelope saying which database its command is for
(`wasm/include/db_instance.h`), and that is the whole of what
`server/replica.c` and `server/peers.c` had to learn — which is nothing;
they are untouched. The cost, accepted: one database's write rate is
every database's, and a halt in one halts all of them.

The three pieces that could not be `bj_ns`'s — opening a subdirectory,
listing one, removing one whole — are `server/root.c`, because a `bj_ns`
is one directory by construction and deliberately cannot enumerate. No
browser runs that file, which is the point.

**`native-composition.md` is gone, and not because it was built.** It
asked for a SEAT: N independent Raft groups in one process, each with its
own log, member set and leader, multiplexed over one transport with a
`{group, msg}` envelope, with idle groups quiesced. That came from
roadmap step 4 — one Raft group per tenant database, where mostly-idle
tenants make quiescence the design rather than an optimization — and
**tenancy is a layer above this repository**. With tenants excluded, an
always-on instance quiesces nothing and places nothing, and almost
nothing of the brief survived the constraint.

What it was mistaken for is the MongoDB shape — one connection to an
instance, `client.db(name)` switching between databases over it — which
is a different thing from a seat, and which is now BUILT (above). The
rest of the retired brief's inventory (the seed loop, the deferred
reply) went to the joining brief, and is built.

**The server is a cluster member**, which is why its brief is gone.
`nisaba-server --raft ID --raft-port N --peer ID@HOST:PORT` is a Raft
member with a log, a node, an apply pump and a peer transport: three
processes elect a leader, replicate every write through the log before
applying it, survive the leader being killed, and catch a restarted
member up — with no JavaScript in any of them
([`docs/db-server.md`](../db-server.md)). The peer wire is
`src/raft-transport-tcp.js`'s — a 4-byte length, then `{ t, id, env }`
with the message as opaque BINARY — so a member running in Node sits in
the same cluster as C members, which `test/db.server.test.js` proves
with two C members and one `RaftNode` that the quorum cannot do without.
Without the flag nothing changes, file layout included.

Two things the brief listed are deliberately NOT built, and both are
waiting on the same thing rather than on a decision:

- **Snapshots in the server.** The node serves and adopts an install by
  itself given a `bj_ns` and an `sst`, and it has the first. It does not
  have the second because nothing in the process compacts the log — so
  no generation exists to install, no peer can fall below the log's base,
  and `RN_EFFECT_NEEDS_SNAPSHOT` cannot fire. It is reported and refused
  rather than ignored if it ever does. The log-naming rule the brief
  asks for ("the store's paired log if a generation has been adopted,
  otherwise the WAL", written down only in `src/db-wal.js`) belongs with
  the compaction that needs it.
- ~~**Joining.**~~ Built, above. The reason given here when the brief was
  retired was wrong and is worth leaving on the record: a joiner does
  NOT need the snapshot half.

**Completions in C is done.** A proposal's fate — applied, and still
yours — is `rn_await` / `rn_applied` / `RN_EFFECT_SETTLED`, decided in
the node. `src/raft.js` reads the answer back rather than re-deriving
the term rule, which was the last piece of `propose()` a host had to
re-implement.

**InstallSnapshot in C is done**, which is why its brief is gone. The
node serves an install, receives one, verifies it and adopts it —
generation files onto the live names, its own log rebased onto the
boundary — through a `bj_ns` it is handed (`wasm/include/raft_node.h`).
That closed the last message kind a host had to answer, and it took the
JavaScript implementation with it: `src/raft.js` has no snapshotter, no
`rebaseLog` and no chunk loop, `ReplicatedDb` hands the node its files
instead of running the transfer, and the deterministic simulator drives
the C path under partitions and crashes. What is left on the host side
is what will never move — opening a file, which is asynchronous in a
browser, and the close/reopen an adoption runs between.

**HTTP is not one of these briefs, deliberately.** It goes in a Node
process in front of the server, over `src/db-server-client.js`, rather
than in `server/main.c` — the same document records that decision and
what it costs. The subset written for the C server is on the branch
`wip/http-transport`; nothing on `main` depends on it, and the brief for
the Node front end has not been written yet.

**The database server is built**, which is why there is no brief for it:
one process per database directory, binjson over sockets, as a
`wasm32-wasip2` command, documented in
[`docs/db-server.md`](../db-server.md). It delivered
`wasm/include/db_session.h` — resolving a collection by name in C — which
is what made the server a replica at all, and what every brief above
sits on. It has since grown a bounded `poll()` multiplexer with idle
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
to end — which is precisely what the C-pushdown effort wants and cannot
have. That trade was refused above, so every brief here works within
plan/execute rather than waiting for it to go away.

## Standing debts

**`dropDatabase` is not replicated.** It is answered outright by
whichever member takes it (`db_instance.c`'s `instance_op`), so on a
leader it removes a directory the followers keep — a divergence the log
never records and nothing detects. It is CLASSIFIED as a write, so a
follower now refuses it and only the leader can do it, which halves the
hole; the other half needs an instance-level log entry, because the
existing envelope (`{ d, c }`) carries a command for a database rather
than a command about one. Found while classifying ops for the read
decision; it arrived with the instance and was never covered.

**The log grows without bound.** A long-lived member's `__wal__.bj` is
every write it has ever taken. Paying it means compaction, then a
snapshot store in the process, then the log-naming rule that follows.
A cluster that can be joined has one more reason to want it.

The two below are paid. Neither was a design question — known,
diagnosed, and now fixed.

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
