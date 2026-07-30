# Next step: completions, so a proposal can be answered without a promise

A work brief. Smaller than the others, and worth doing early: it is the
last piece of `propose()` that a host has to re-implement, and it is a
correctness rule rather than plumbing.

## Where this sits

`rn_propose` (`wasm/include/raft_node.h`) appends an entry, syncs it,
replicates it and runs the commit check, then hands the index back. What
it does NOT do is tell anyone when that index has been applied — which is
the moment a client's write is actually finished.

`src/raft.js` fills that gap with `_waiters` and `_settleWaiters`. A
native host would have to fill it too, and the rule it would be
re-implementing is not obvious.

## The rule, and why it is not obvious

`_settleWaiters` does this for every waiter at or below `lastApplied`:

```js
if (this._log.termAt(w.index) === w.term) w.resolve({ index, term });
else w.reject(new NotLeaderError(this.leaderId));
```

The comparison is the whole point. An index being applied does not mean
YOUR entry was applied there: a new leader's conflicting entry can
overwrite an uncommitted one at the same index before it commits, and the
proposer must be told its write did not happen. Get this wrong in the
lenient direction and a client is told a write succeeded that no replica
holds.

The other half is the failure paths: `_rejectWaiters` fires when the node
steps down (through the ROLE effect), when the apply pump halts, and when
the node stops. A host that forgets one of those leaves a client hanging
forever on a write that can never complete.

Both are consensus-adjacent rules living in the host, which is the shape
everything else in this effort has been moving out of.

## Goal

The node tracks what it owes and reports completions; the host maps them
to whatever it answers with — a promise, a socket write, a callback.

**Done when** a native test proposes entries, drives them to applied, and
observes exactly the completions Raft's rules say are owed, including a
rejection for an entry overwritten by a new leader — with no JavaScript
in the process — and `src/raft.js` settles its promises from the node's
report rather than from its own copy of the rule.

## Shape

Effects carry `(kind, arg, flag)` and nothing else, which is enough here:
`arg` is the index, `flag` is whether it is OUR entry or was overwritten.
The host keeps the index-to-caller mapping it already has.

Sketch, to be argued with:

```c
/* The host owes an answer for this index at this term. */
int rn_await(raft_node *n, uint64_t index, uint64_t term);

/* RN_EFFECT_SETTLED: arg = index, flag = 1 kept / 0 overwritten. */
```

`rn_propose` and `rn_change_membership` could register the wait
themselves, which would remove the last reason for a caller to hold an
index at all.

Fired from wherever the node already learns that applied state moved:
today the host tells it (`lastApplied` is the host's, because the apply
pump is). See the dependency note below.

## Dependency worth deciding up front

The node does not know `lastApplied` — the apply pump is the host's,
because applying needs a state machine. Two options:

1. **The host tells it.** `rn_applied(n, index)` after each entry the
   pump applies; the node emits the completions that unlocks. Small,
   works today, keeps the pump where it is.
2. **The pump moves too.** `dc_wal_apply` already performs a command
   against an open collection, so C could drive the loop — but it would
   need to resolve a collection BY NAME, which needs a namespace, which
   is `install-snapshot-in-c.md`'s work.

(1) is the right first move and does not block (2). Say which you chose
and why.

## Invariants

- **The term check is the rule.** An entry at your index is not your
  entry. Preserve `termAt(index) == term` exactly; a completion reported
  as kept when the entry was overwritten is a lost write reported as a
  success.
- **Every registered wait terminates.** Step-down, halt and stop must
  each produce an answer for everything outstanding. A hung client is the
  failure mode this brief exists to prevent, not to relocate.
- **Bounded.** A fixed table like `pending` in `raft_node.c` (with an
  explicit refusal when full), not an unbounded one — and if it can
  overflow, it says so, the way `rn_effects_lost` does.
- **All-or-nothing, no silent drops, falsify both ways.** See
  `install-snapshot-in-c.md`'s invariants section; they are the same.

## Verification

```
./wasm/build-native.sh                     # ASan/UBSan
./wasm/build-wasm.sh && npx vitest run
```

The overwritten-entry case is the one to build deliberately: the
simulator already produces it in `test/raft.test.js` ("a minority leader
cannot commit; its uncommitted entry is discarded on heal"), and the
native suite should get a direct version that does not need a whole
cluster — propose at term N, truncate, append a different entry at the
same index at term N+1, apply, and check the completion says overwritten.

## Out of scope

Moving the apply pump (see the dependency note). The snapshot/namespace
work. Anything about transports.
