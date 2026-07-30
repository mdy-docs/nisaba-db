# Next step: native composition — a seat, a socket, and what is policy

A work brief. This one is deliberately less prescriptive than its
siblings, because the first half of the job is deciding what belongs in C
at all. Do that decision explicitly, in writing, before implementing.

## Where this sits

With the state machine, the applier and four of the five message kinds in
C, one process can run consensus without JavaScript. What it cannot yet
do is BE a server: something has to own the sockets, the clock, the group
registry and the lifecycle. In JavaScript that is `src/raft-host.js`
(`RaftGroupHost`), `src/raft-transport-tcp.js`, `src/raft-transport-http.js`
and `src/raft-monitor.js`.

None of that is consensus. Most of it is plumbing that a native binary
will simply write in its own idiom. But some of it is POLICY that took
real thought and has a wrong answer, and policy duplicated per host is
the thing this whole effort exists to stop.

## Goal

A native server binary that seats one or more Raft groups over real
sockets, with every non-obvious policy decision sourced from C rather
than reimplemented.

**Done when** a native binary runs a three-node cluster over localhost
sockets — elections, replication, a membership join, quiescence and wake
— with no JavaScript in the process, and `src/raft-host.js` reads the
same policy decisions from C rather than computing its own.

## The inventory, and the honest split

Read `src/raft-host.js` end to end first; its header explains why each
piece exists.

**Probably C's — a wrong answer here is a bug, not a preference:**

- **The idle-quiesce test.** `_shouldQuiesce` (`src/raft-host.js`, ~187):
  only a LEADER quiesces by idleness, and only a settled one —
  everything committed, applied, and on every follower. The comment
  records what happens otherwise: host-side idling of followers misreads
  leader silence as leader death and churns elections, which the
  simulator caught. That is a rule with a failure mode, stated once.
- **The seed loop.** `seedRequest` (`src/raft-host.js`, ~225): which
  reply means retry, which means follow a redirect, which means give up
  because a validation refusal will never heal. The node now produces
  those replies (`rmsg_build_membership_reply`); reading them should not
  be a second opinion.
- **Envelope framing.** `{group, msg}` is the multiplexing format. It is
  written down in `groupTransport` and `handleEnvelope` and nowhere else;
  a native host would invent its own and the two would not interoperate.

**Probably the host's — genuinely per-platform:**

- Sockets, listeners, connection pools, redial policy, timeouts.
- The tick loop (`setInterval` here; a poll loop or timerfd there).
- The group registry's storage; how a tenant maps to a group.
- The monitor's HTTP surface. `RaftNode.status()` already produces the
  JSON-able snapshot; serving it is not interesting.

**Deliberately undecided, and worth an explicit answer:** heartbeat
coalescing across groups. `src/raft-host.js` says it is left as a
transport concern because quiescence does the heavy lifting. If a native
seat with many tenants shows otherwise, that is a real design finding —
record it rather than quietly implementing it in one host.

## Ordering

This should come AFTER `install-snapshot-in-c.md` and
`completions-in-c.md`. A seat that cannot answer an InstallSnapshot, or
cannot tell a client its write finished, is not a server — and both of
those change the surface a seat sits on.

## Suggested staging

1. **Write the split down.** For each item above, decide C or host, and
   say why in one sentence. This document is the deliverable of step 1;
   the rest is implementation.
2. **Move what you decided is C's**, one piece at a time, with the JS
   host reading the decision back — the same pattern
   `_shouldQuiesce`-style logic already follows for membership.
3. **Build the native seat**: a `main.c` in `test/native/` or a new
   `bin/` target that opens sockets, drives `rn_tick`, drains the outbox
   to writes, feeds reads back through `rn_handle` / `rn_on_reply` /
   `rn_on_fail`. The delivery loop in
   `test/native/main.c`'s `three_nodes_elect_a_leader_and_commit_without_a_host_language`
   is already the skeleton — it is about twenty lines and swapping its
   for-loop for sockets is the whole difference.
4. **Prove it**: three processes (or three seats in one process over real
   sockets), a join, a leader failover, a quiesce/wake cycle.

## Invariants

- **One owner per policy.** If both the C seat and `RaftGroupHost` decide
  when to quiesce, they will eventually decide differently, and the
  symptom will be election churn under load in one deployment and not the
  other.
- **The transport frames, it does not interpret.** It has never read a
  field of a Raft message and must not start. The grammar is
  `raft_msg.h`'s.
- **Peers must be DIRECT addresses.** A load balancer breaks node
  identity; `src/raft-transport-http.js` documents this and a native
  transport must too.
- **Quiescence is a leader-first protocol.** The leader parks followers
  with a flagged final heartbeat BEFORE it stops beating; the flag is
  already carried by `rn_quiesce` + `rn_replicate`. Do not let a native
  seat park a follower directly.

## Verification

The bar is the one `test/raft-tcp.test.js` and `test/raft-http.test.js`
already meet: a real cluster over real sockets with real timers, not a
simulation. Add the native equivalent; keep the JS ones green.

```
./wasm/build-native.sh                     # ASan/UBSan
./wasm/build-wasm.sh && npx vitest run
```

## Out of scope

Everything above the seat: tenant→group provisioning, gateway
leader-routing, auth/TLS, ops. `docs/replicaton-roadmap.md` step 4
records that boundary deliberately — it is nisaba-web's, not this
repository's.
