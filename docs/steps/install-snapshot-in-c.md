# Next step: InstallSnapshot into C, via a namespace on the Raft node

A work brief, written to be handed to someone who has not been following
the C-pushdown effort. It says what to build, what already exists so it
is not built twice, what shape the answer has to take and why, and what
must still be true afterwards.

## Where this sits

The Raft state machine now lives in C (`wasm/include/raft_node.h`, over
`raft_core.h`'s safety rules, `raft_msg.h`'s wire grammar and
`raft_drive.h`'s leader bookkeeping). `src/raft.js` is the HOST: it owns
the transport, the promises, the apply pump, and nothing that decides
anything. The applier moved too — `dc_wal_apply` (`wasm/include/db_wal.h`)
performs a committed command against a real collection, so an entry no
longer needs a JavaScript runtime to be applied.

Of the five Raft message kinds, `rn_handle` answers four. The fifth,
`RAFT_MSG_INSTALL_SNAPSHOT`, is refused — see the `else` arm of
`rn_handle` in `wasm/src/raft_node.c`. It is the last one, and this brief
is about it.

## Goal

Make the Raft node able to SERVE and RECEIVE a snapshot install itself, so
a node with no JavaScript runtime can bootstrap a lagging replica and be
bootstrapped by one.

**Done when** a native test runs an install between two `raft_node`s over
a real directory (`bjns_posix_open`) with no JavaScript in the process —
leader streams, follower stages, verifies, adopts, rebases its log — and
the existing JS suite passes unchanged.

## Why it is blocked today, precisely

`rn_new(self_id, elog *log)` is the node's entire relationship with
storage. An `elog` wraps a `bj_io`, which is an ALREADY-OPEN file. The
node can append to the log it was handed; it cannot create, open or
unlink anything.

The sharpest evidence is `rebaseLog` (a constructor option in
`src/raft.js`): after an install commits, the node needs a fresh log
based at the snapshot boundary, and EntryLog cannot rebase in place. That
is a host callback for one reason — the node needs a FILE and has no way
to ask for one.

An install's receive side is not one write. Per install, the adapter in
`src/db-replicated.js` (`_makeSnapshotter`, around line 220) does:

- `store.begin()` — stage a new generation
- `tx.createFile(role)` per role — create files BY NAME
- `handle.write(data, {at})` per chunk
- `tx.commit(...)` — write the manifest, which IS the commit point
- `_adoptInstalledSnapshot()` — close the inner Db, copy every generation
  file onto its live filename, reopen the database, repoint every cached
  collection

and then the node closes its log and calls `rebaseLog`. A multi-file,
multi-phase transaction ending in a whole-database swap.

## The constraint that shapes the whole design

Read `third_party/binjson-structures/include/bjns.h` before designing
anything. Opening a file in a browser is asynchronous (OPFS
`getFileHandle` and `createSyncAccessHandle` both return promises) and
WASM cannot block on a promise without Asyncify or JSPI, neither of which
exists natively or under WASI. Adopting either would give the server a
different control-flow model from the browser, which is the exact problem
this whole effort exists to remove.

The resolution is a discipline rather than a mechanism:

> **C plans, the host opens, C executes.**

Every file-touching operation splits into a pure call that returns the
names it will need, and a synchronous call that does the work over the
handles the host opened in between. `bj_ns.open` is REQUIRED to be
synchronous, and callers may only ask for names a preceding plan call
declared.

The precedent to copy is `dc_sweep_execute` / `dc_compact_execute`
(`wasm/include/db_catalog.h`, lines ~250-305) and their JS side in
`wasm/nisaba-wasm.js` (~3365-3395): pre-open every planned name into the
`bjnsScopes` table, then make ONE synchronous C call.
`dc_compact_execute`'s doc comment explains why the flip must be a single
call and why the host's gate is still needed around it. The install has
the identical shape and the same reasoning applies verbatim.

## Do not build these — they exist

| Piece | Where |
| --- | --- |
| Generation naming, manifest shape and validation, adoption, sweep policy | `third_party/binjson-structures/include/snapstore.h` — `sst_check_files`, `sst_manifest_encode`, `sst_next_gen`, `sst_data_name`, `sst_log_name`, `sst_scan`/`sst_try_manifest`/`sst_confirm` |
| The chunk walk (which file, which offset, which chunk carries the manifest, which ends the stream) | `raft_drive.h`'s `raft_chunk_next` |
| The term rules on the message | `rn_observe_leader`, `rn_step_down` |
| The namespace vtable and its adapters | `bj_ns`; `bjns_posix_open` (native, `bjio_posix.h`), `bjns_bridge.c` (browser, scope table) |

`snapstore.h` says of itself that it is NOT the store — it is every
DECISION the store makes. Subtract that, and what remains in JavaScript
is only the motions.

## What moves

**Send side.** `_sendSnapshot` in `src/raft.js` opens each generation file
through `snapshotter.openFile(role)` and reads chunks. With a namespace,
C reads them itself and queues `installSnapshot` messages through the
outbox, consuming replies via `rn_on_reply` like every other message.
Read-only, so none of the adopt problem — the easiest win, and it
exercises the seam end to end.

**Receive side.** `_onInstallSnapshot` in `src/raft.js` plus the adapter
in `src/db-replicated.js`: stage chunks into a new generation, verify the
staged bytes against the leader's manifest, write the manifest.

**Adopt.** `_adoptInstalledSnapshot` (`src/db-replicated.js`) and
`restoreFromStore` (`src/db-wal.js`, ~797): delete the live files, copy
the generation's onto the live names. The delete and copy are C's through
`bj_ns`. **Closing and reopening the database stays the host's** — it
rebuilds `dc_collection` handles and, in a browser, needs async opens. C
does its work BETWEEN the host's close and reopen, which is exactly
`dc_compact_execute`'s bargain.

## Suggested staging

Each step should land green on its own.

1. **`rn_set_ns(n, bj_ns *)`, optional.** Without a namespace the node
   refuses installs exactly as today and every existing host path keeps
   working. No behaviour change. A native test that the node resolves a
   name through it.
2. **Send side in C.** `RN_EFFECT_NEEDS_SNAPSHOT` stops being something
   the host acts on and becomes C queueing chunks. Retires
   `_sendSnapshot`.
3. **Receive-side staging.** A pure "which files will this install need"
   beat so the host can pre-open, then `rn_handle` stages chunks and
   verifies through `sst_check_files`.
4. **Adopt and log rebase.** One synchronous call between the host's close
   and reopen. `rebaseLog` disappears wherever a namespace is present.
5. **Retire the `snapshotter` object**, or keep it as the no-namespace
   fallback, and update `ReplicatedDb` and the test harness.

## The decision this brief deliberately does not make

Whether `test/raft-harness.js` (the deterministic simulator: `KvMachine`,
`KvSnapshotter`, `MemoryHandle`-backed logs) gets a namespace too.

If it does not, the JS suite stops covering the new path, which would be
the worst available outcome — that harness is where partitions, crashes
and multi-chunk installs are actually tested. `test/native/memfs.h`
already describes itself as "the seed of what becomes `bjns_mem.c` once
bj_ns lands"; a memory adapter is probably the answer for both the
harness and the native tests. Decide it explicitly and write down why.

## Invariants that must hold

These are the house rules the surrounding code was built to; breaking one
is a regression even if every test passes.

- **One owner per fact.** If two components could disagree about
  something, remove the opportunity rather than checking for it
  afterwards. Recent examples: the member records live in the node and
  the host reads them back; the node derives the sender of a message
  rather than being told.
- **All or nothing.** A refusal leaves the previous state exactly as it
  was — never a partial adoption. `rn_set_members` builds into scratch
  and commits in one `memcpy` for this reason.
- **Nothing is dropped in silence.** Every refusal is a distinct code
  with text in `dc_strerror` (`wasm/src/db_validate.c`) and an entry in
  the native strerror test. A queue that can overflow reports that it
  did (`rn_effects_lost`).
- **Ordering is the contract.** The manifest is written LAST; a failure
  anywhere must leave the old generation live and the new one merely
  orphaned for the next sweep.
- **Falsify both ways.** Break the fix, watch the specific test fail,
  restore it, and say so in the commit message. Every commit on this
  branch does this.
- **No new `int` or `double` for indices.** `uint64_t` in C, `double` at
  the JS glue, with the 2^53 ceiling documented as the glue's and not the
  logic's (see the note at the top of `raft_core.h`). Correlation ids
  were `int` and would have started refusing messages at 2^31.

## Known hazards

- **Apply-chain serialization.** The install commit and log swap run
  through `_applyChain` today so an in-flight apply loop can never
  observe the swap. Whatever replaces it must preserve that.
- **The read gate.** `_readGate` in `db-wal.js` / `db-replicated.js`
  holds async reads off collections whose WASM contexts are being freed
  mid-swap. Sync-shaped cursors across an install window are a documented
  open hazard (roadmap 5d) — do not make it worse.
- **`bj_ns.remove` may be DEFERRED** under the browser adapter. Never
  order a create against a remove; use `BJ_NS_TRUNC`.
- **Effects cannot carry buffers.** `(kind, arg, flag)` only. Names have
  to come back through a plan call, not the effect queue.
- **`elog_get`'s payload is log-owned** and dies on the next operation on
  that log. Copy it before doing anything else. (This produced a
  double-free in a native test; ASan caught it.)
- **Chunk-order validation** (`offset === written`) currently lives in
  the JS adapter. Keep it, in C.
- **A checksum mismatch must adopt nothing** and ask the leader to
  restart. A superseding manifest, or a real election, aborts a
  half-staged install.

## Verification

```
./wasm/build-native.sh                     # ASan/UBSan; 90 tests at the time of writing
./wasm/build-wasm.sh && npx vitest run     # 435 tests at the time of writing
```

Both must stay green, and both should grow. `./wasm/build-native.sh
--wasi` needs `$WASI_SDK` and is the end-to-end proof of the whole
effort; CI runs it.

The browser suite (`npm run test:browser`) has three PRE-EXISTING
failures in `test/db.compact.browser.test.js` — OPFS access-handle
exclusivity, verified unrelated to this work by stashing and re-running.
They are not this step's to fix.

## Out of scope

The completion queue that would replace `propose()`'s promise for a
native host, and native composition (`RaftGroupHost`, the TCP/HTTP
transports, `RaftMonitor`). Those are separate phases. Folding either
into this one makes the diff unreviewable.
