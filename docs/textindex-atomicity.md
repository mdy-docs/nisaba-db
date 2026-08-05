# TextIndex cross-tree atomicity

> Moved here from the parent `nisaba-web` repo's `docs/`. The paths below
> (`c/textindex.c`, `src/binjson-wasm.js`) reflect the layout at the time
> this was written and predate this package's own reorg (C moved under
> `wasm/`, wrapper renamed to `src/nisaba-wasm.js`) — the design/algorithm
> described is unchanged, only the file locations.

How the C/WASM text index makes `add` / `remove` / `clear` atomic across its
three backing files, and why the mechanism is a 96-byte journal rather than a
write-ahead log.

Implementation: `c/textindex.c` (journal section), `c/bplustree.c`
(`bpt_rewind`), glue in `c/textindex_wasm.c`, wrapper in
`src/binjson-wasm.js`. Crash-interleaving tests:
`test/textindex.atomic-wasm.test.js`.

## The problem

A text index spans three independent B+ tree files:

| tree              | key    | value                          |
|-------------------|--------|--------------------------------|
| `index`           | stem   | posting blocks (`{docId: tf}`) |
| `documentTerms`   | docId  | `{stem: tf}`                   |
| `documentLengths` | docId  | total term count               |

One logical operation touches all three. Worse, it is not three writes but
*many*: each posting block, term map, and length update is an individual
B+ tree commit. A crash anywhere in that sequence used to leave the index
internally inconsistent — postings referencing a document with no length
entry (skewing every idf denominator), a document half-removed, a term map
without its postings — and nothing detected or repaired it.

## The insight: append-only files can be rewound

Every tree file is append-only. A mutation appends new nodes plus a fresh
metadata record; nothing before the append is ever modified. Therefore:

> Truncating a tree file to any previous commit boundary restores *exactly*
> the state the tree had when that commit landed.

This turns multi-file rollback — normally the hard part of atomicity — into
three `truncate()` calls. There is no undo log to replay, no torn page to
repair. All the journal has to remember is **how long each file was** the
last time the index was consistent.

`bpt_rewind(t, len)` (in `bplustree.c`) implements the single-tree half:
it verifies that the bytes at `len - 135` are a valid metadata record (so
`len` really is a commit boundary), truncates the file, and reloads the
tree's in-memory state from that metadata. It is deliberately a public
B+ tree API — the same primitive gives historical snapshots (review item
§1.8) for free.

## The journal

A fourth, fixed-size file records one triple of file lengths per committed
operation. It uses two 48-byte slots written alternately (ping-pong):

```
offset  size  field
0       4     magic "TIXJ"
4       4     version (1), little-endian
8       8     transaction counter, little-endian
16      8     length of the index file
24      8     length of the documentTerms file
32      8     length of the documentLengths file
40      4     CRC-32 (zlib polynomial) over bytes [0, 40)
44      4     zero padding
```

Slot 0 lives at offset 0, slot 1 at offset 48. Transaction *n* writes slot
`n & 1` — transaction 1 goes to slot 0, so the very first write appends at
offset 0 of the empty file and no write ever leaves a gap. The file never
exceeds 96 bytes, so there is nothing to compact and no growth to manage.

The ping-pong layout is what makes the single unsynchronized write safe: a
torn slot write can only damage the slot *being replaced*, and the other
slot still holds the previous transaction. There is never a moment when
both recovery points are at risk.

## Commit protocol

`tix_add` / `tix_remove` / `tix_clear`, when given a journal:

1. Perform all tree writes exactly as before (each is its own durable
   B+ tree commit with a CRC trailer — see the bjfile durability design).
2. After everything landed, read both journal slots, pick
   `txn = newest + 1`, and write one 48-byte slot recording the three
   current file lengths.

The journal write is *last*. An operation is committed if and only if its
slot landed; everything before that is provisional by construction.

## Recovery

`tix_recover` runs right after the three trees are opened (each tree first
performs its own single-file torn-tail recovery). It reads both slots and
walks them newest-first:

1. **Newest slot satisfiable** (every tree file is at least as long as the
   slot records): rewind all three trees to the slot's lengths. In the
   common case the lengths match exactly and this is a no-op; if a crash
   interrupted a later operation, its partial writes sit *beyond* the
   recorded lengths and are truncated away — the whole operation vanishes.
2. **Newest slot unsatisfiable** (some tree is shorter than it records —
   the slot persisted but that tree's writes did not): fall back to the
   previous slot, which described the transaction before it.
3. **Neither slot satisfiable**: more data was lost than the journal can
   reconcile. Open refuses (`BJ_ERR_STATE`) rather than serving a silently
   inconsistent index; the host can rebuild the index from source data.
4. **Empty or absent journal**: no constraint. The trees are adopted as-is,
   which is both the pre-journal compatibility path and the reset story
   (below).

Why two slots are enough: under the tail-tear failure model the durability
layer already assumes (writes may be lost from the *end* of a file;
mid-file corruption makes the tree refuse to open on its own), a tree can
only be "behind" the newest slot by the writes of the newest transaction.
Transaction *n − 1* completed fully before slot *n* was ever written, so
every tree is at least as long as slot *n − 1* records — the fallback in
step 2 always has solid ground to land on. Case 3 only arises when whole
committed transactions were lost (e.g. a file restored from an older copy),
which is precisely when refusing is the right answer.

## What this does and does not guarantee

- **Atomicity across the three files** for every journaled operation: after
  any crash, the reopened index reflects a prefix of the committed
  operations, never a partial one.
- **No lost committed data**: recovery only truncates bytes *beyond* the
  newest satisfiable commit record — bytes no committed operation refers to.
- **Durability is still the host's business.** The journal write is
  synchronous but nothing calls `fsync`/`flush` per operation, matching the
  trees themselves. If the OS loses the tail of several files at once, the
  recovery ladder degrades exactly one rung at a time (that is the
  tail-tear model). Hosts that need commit-level durability should flush
  the three tree handles and then the journal handle, in that order.
- **One journal per set of tree files.** The lengths in a journal are
  meaningless for any other files. After compacting the trees into fresh
  files, start them with an empty journal — a stale journal paired with
  compacted (shorter) files fails safe: both slots are unsatisfiable and
  open refuses.

## API surface

C (`textindex.h`):

```c
int tix_recover(const bj_io *journal, bpt *index, bpt *doc_terms, bpt *doc_lengths);
int tix_add    (bpt *index, bpt *doc_terms, bpt *doc_lengths, const bj_io *journal, ...);
int tix_remove (bpt *index, bpt *doc_terms, bpt *doc_lengths, const bj_io *journal, ...);
int tix_clear  (bpt *index, bpt *doc_terms, bpt *doc_lengths, const bj_io *journal);
```

`journal == NULL` disables journaling entirely (legacy behavior; queries
never need it). The WASM glue (`tixw_*`) takes a registered host file
descriptor instead, with `-1` meaning no journal.

JS wrapper:

```js
const index = new TextIndex({
  trees: { index, documentTerms, documentLengths },
  journal: journalSyncAccessHandle   // optional
});
await index.open();   // runs tix_recover before the index is usable
```

The pure-JS `TextIndex` implementation has no journal support; the feature
is part of the C storage engine.

## Failure interleavings covered by tests

`test/textindex.atomic-wasm.test.js` simulates crashes byte-for-byte by
snapshotting all four files mid-sequence and restoring subsets:

| persisted at "crash"                  | recovery outcome                     |
|---------------------------------------|--------------------------------------|
| all tree writes, journal write lost   | operation rolled back whole          |
| some trees, others behind, no journal | operation rolled back whole          |
| journal slot only, no tree writes     | falls back to previous slot          |
| trees behind both slots               | open refuses (`BJ_ERR_STATE`)        |
| partial remove                        | remove rolled back whole             |
