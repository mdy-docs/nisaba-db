# Clustering: membership, addresses, join/leave

How nisaba-db Raft groups grow, shrink, and find each other. The design
goal is *"if you want more, add more"*: a new node needs three facts —
its id, its own listen address, and one seed address — and everything
else flows through the replicated log. (Replication roadmap step 5d;
the consensus layer itself is documented in `src/raft.js`.)

## The one design rule: the log is the address book

Cluster membership travels through the log as CONFIG entries whose
payload is a list of **member records**:

```js
{ members: [ { id: 1, host: "10.0.0.1", port: 7001 },
             { id: 2, host: "10.0.0.2", port: 7001 }, ... ] }
```

Not bare ids — records, with addresses. Every path by which a node can
learn membership funnels through one method (`RaftNode._setMembers`),
which fires the `onConfig` hook with the full record list:

- **bootstrap** — the `peers` option at construction,
- **apply** — a CONFIG entry committing (the etcd convention: adopted
  when applied, one change in flight at a time, committed under the
  *old* quorum),
- **restart** — the log is scanned for the last CONFIG entry, which
  wins over the static `peers` (so a stale boot config cannot regress
  a cluster),
- **snapshot install** — the leader's manifest carries the records, so
  a blank-disk member learns the whole cluster shape with its data.

`RaftGroupHost.addGroup` chains `onConfig` to `transport.setPeer`, so
**the peer table can never drift from the log** — there is no separate
address book to administer, back up, or reconcile. Addresses are only
ever added or updated by this sync, never removed: the transport is
shared by every group on a host, and another group may still talk to a
node one group dropped. Pruning dead addresses is host policy
(`transport.removePeer`).

Extra fields on a member record ride through untouched. `voting: false`
marks a **learner** — see below.

## Growing: join via any node

A joiner knows *addresses*, not node ids — so both reference transports
expose `callAddress(addr, envelope)` alongside the id-keyed `call`, and
`join`/`leave` ride the ordinary transport as messages any member can
receive. A non-leader's refusal includes the leader's id **and
address** (it knows both from the records); the leader upserts the
record via `changeMembership` and replies with the adopted member list
once the CONFIG entry commits.

The whole flow, using the `joinGroup` helper (`nisaba/raft-host`):

```js
import { RaftGroupHost, joinGroup } from "@mdy-docs/nisaba-db/raft-host";
import { TcpRaftTransport } from "@mdy-docs/nisaba-db/raft-tcp";
import { connectReplicated } from "@mdy-docs/nisaba-db/replicated";

const host = new RaftGroupHost({ transport: null });
const transport = new TcpRaftTransport({
  listenPort: PORT, peers: {},
  onMessage: (env) => host.handleEnvelope(env)
});
await transport.start();
host._transport = transport;

const me = { id: MY_ID, host: MY_HOST, port: PORT };
const members = await joinGroup(transport, "tenant-1", me, {
  seeds: [{ host: "10.0.0.1", port: 7001 }]   // any current member(s)
});

const rdb = await connectReplicated(provider, {
  id: MY_ID, peers: members,                   // records, straight through
  transport: host.groupTransport("tenant-1")
});
host.addGroup("tenant-1", rdb);                // syncs the peer table
host.start();
```

What happens underneath: `joinGroup` tries each seed, follows the
leader redirect, and retries through elections (`attempts`/`delayMs`
options). Once the leader commits the CONFIG entry, every member's
`onConfig` adds the newcomer's address; the leader starts replicating
to it immediately — ordinary AppendEntries if the log still reaches
back far enough, an automatic full snapshot install if not. Failed or
lost replies are safe: **re-joining with an identical record is
idempotent** and appends nothing.

**Bootstrapping the first node**: list *yourself, with your address*,
in `peers` — `peers: [{ id: 1, host, port }]`. That puts your address
into the first CONFIG entry a join creates, which is how the second
joiner's redirect (and every later member) learns where you live.

**Restarts need no join.** A node that has been a member boots from its
own disk: the log's CONFIG entry restores ids and addresses (even with
an empty bootstrap `peers`), and it rejoins replication as itself.

**Replacing a dead node** is either flavor: boot a blank disk with the
dead node's id (the leader detects the regression and snapshot-installs
it — no membership change at all), or `leaveGroup` the dead id and
`joinGroup` a fresh one.

## Learners: join safely, vote when ready

A **new member always enters as a learner** (`voting: false` on its
record — `join` forces this regardless of what the request claims). A
learner receives everything a voter does — AppendEntries, snapshot
installs, the whole log — but:

- it carries **no quorum weight**: commit arithmetic and election
  majorities run over voters only, so adding capacity never thins the
  failure margin while the newcomer is still syncing;
- it **never campaigns**: election timeouts are inert on a learner;
- it **refuses votes**, pre-vote and real, even if a stale-config
  candidate asks.

**Promotion is automatic.** On every replication success the leader
checks whether a learner's match index covers the commit index — "has
everything committed" — and if so proposes the same record with
`voting: true`. That CONFIG entry commits under the existing electorate
and, once applied, the ex-learner counts and campaigns like anyone
else. If another membership change is in flight at that moment, the
promotion simply retries on the next replication success. Promotion is
the only *automatic* membership change in the system, and it can only
ever widen the electorate with a replica proven current.

Practical consequences:

- The add-a-node timeline is: `joinGroup` → CONFIG(learner) commits
  under the old quorum → catch-up (AppendEntries or snapshot install)
  → CONFIG(voter) commits → full member. Both entries are visible in
  the log; `node.voters` vs `node.members` shows the split at runtime.
- A re-join of an existing member (address change, retry after a lost
  reply) keeps its current voting status — an established voter is not
  demoted by re-announcing itself.
- An id-only `changeMembership([1,2,3,9])` inherits the known records,
  flag included — you cannot accidentally promote a learner (or erase
  an address) with a bare-id call. Explicit demotion is possible by
  proposing a record with `voting: false`; a demoted leader steps down
  when the entry applies.
- The quorum-math caveat from the previous section is now closed: the
  brief thin-margin window no longer exists, because the newcomer only
  joins the electorate after it is provably caught up.

## Shrinking: leave

```js
await leaveGroup(transport, "tenant-1", 3, { seeds: [{ host, port }] });
```

From the departing node itself (graceful decommission) or from an admin
naming a dead node. The removed node steps down if it was leader, stops
campaigning once it applies its own removal, and never disrupts the
survivors even if it *doesn't* learn of the removal (a healthy leader
refuses pre-votes — see `src/raft.js`). Removing an already-absent id
is idempotent. Shut the process down afterwards; that part is yours.

## Semantics and honest caveats

- **One change at a time.** A second `changeMembership` while one is in
  flight is refused (`{ retry: true }` on the wire; `joinGroup`
  retries). This serialization is what makes single-server changes safe
  without joint consensus.
- **Quorum math moves only at promotion.** A joiner is a learner until
  caught up, so adding a member never thins the failure margin; the
  electorate grows only when the promotion CONFIG entry applies.
- **Full-set replacement, merge-protected.** CONFIG entries carry the
  complete member list. `changeMembership([1,2,3,4])` with bare ids
  inherits the known records, so addresses in the log can't be erased
  by an id-only call.
- **Addresses are as fresh as the last CONFIG entry.** A node that
  changes address rejoins with its new one (`join` upserts). Snapshot
  manifests carry the *current* set — exact whenever changes are
  committed and settled, which is the only time to snapshot anyway.
- **Join is an ops action, not a hot path.** `callAddress` is a
  one-shot unpooled request; steady-state traffic stays on the pooled
  id-keyed `call`.
