# On-disk format compatibility

The contract for what happens when one version of nisaba opens files
written by another. Written while there is only one format version, on
purpose — this page is cheap now and expensive after the first change.

## The stamp

Every database carries a format version in its catalog under the
reserved key `__format__` — one stamp, whichever host wrote it:
`DB_FORMAT_KEY`/`DB_FORMAT_VERSION` in `src/nisaba-wasm.js` and
`DC_FORMAT_KEY`/`DC_FORMAT_VERSION` in `engine/include/db_names.h`, read
and written by the same gate on both sides (`Db.open()`; `check_format`
inside `dbs_open`). Enforced by `test/db.format.test.js` in process and by
*a format from the future* in `test/db.server.test.js` through the real
server:

- `Db.open()` stamps a fresh database with the current version.
- A database with **no stamp** predates the stamp mechanism and is by
  definition version 1 — unless it has no content at all, which makes it
  a fresh database of the current version. Either way it is stamped on
  open.
- A database stamped **at or below** the build's version opens normally,
  migrating first if it is below (see the bump rules below).
- The stamp's value is `{ v }`, plus **`migrating: true`** for as long as
  a migration is unfinished. That flag exists because the version alone
  cannot say: the stamp is raised *before* the first collection is
  converted (so no older build can misread what the migration writes),
  which means from that instant the version reads current whether one
  collection was converted or none. A crash in that window would
  otherwise leave v1-keyed collections in a database nothing would ever
  offer to convert again — their documents sitting under keys no reader
  derives any more, with no error to go with it. The flag is written
  durably with the raised version and cleared, durably, only when the
  last collection has flipped; every open in between resumes the
  migration, skipping the definitions already marked (see below).
- A database stamped **above** the build's version is refused before
  anything touches the files — in particular before the orphan sweep,
  which must never judge a future format's files by an old version's
  naming rules. Both hosts name both versions: the JS host in its error
  message, the C server as `found`/`understands` fields beside the coded
  `DC_ERR_FORMAT_NEWER` refusal (additive, the `respond_error_at`
  precedent) — the stamp is re-read after the refusal (`dbs_peek_format`)
  because a coded int cannot carry it, and an unreadable stamp omits
  `found` rather than inventing it.

  **That sentence is now load-bearing, and measured.** It cost nothing
  while the C server had no sweep. It has one (`dbi_sweep_all`, run before
  anything is served), so a future format's file — a name today's rules
  say belongs to nobody — would be deleted by an old build, silently, and
  the remains then correctly refused. `check_format` runs inside
  `dbs_open`, before `dbi_database` can sweep anything; open the gate and
  the sweep eats the file, which is what *a format from the future*
  (`db.server.test.js`) asserts by comparing the directory before and
  after a boot.

The stamp names the version of the *whole database layout*: catalog
entry shapes, file naming (`g<N>-` generations, `coll-`/`idx-`
prefixes), journal record layout (which has its own magic + version
field inside `DCTJ` records — `engine/src/db.c`), and the tree/index file
formats beneath (binjson-structures' own metadata carries a `version`
field per tree).

## Rules for bumping the version

`DB_FORMAT_VERSION` may only be bumped in a commit that also:

1. **Writes the migration story here.** Either the old layout opens
   unchanged under the new code (pure addition — prefer this), or
   `Db.open()` migrates it explicitly and atomically (the compaction
   machinery is the template: build new files, flip one catalog commit,
   sweep the old — `docs/compaction.md`).
2. **Keeps refusal loud in both directions.** Newer readers handle every
   older version (open or migrate — never guess). Older readers already
   refuse newer stamps by the check above; a change that older readers
   would *misread without noticing* (rather than refuse) must bump the
   version, even if it looks backward-compatible.
3. **Adds a doctored-stamp test** proving the new reader accepts each
   older version's fixture and refuses a version above its own.

Additions that do NOT need a bump: new optional fields in catalog
entries that old readers ignore and new readers default (the existing
convention — `gen`, `journal`, `compactedBytes` all arrived this way);
new file kinds that old readers' sweep patterns don't match.

**Worked example: the catalog's applied index (no bump).** A
`dropCollection` deletes the files that recorded what had been applied, so
the catalog records it too — the one structure a drop both keeps and
writes (`catalog_note_applied` in C, `Db.noteApplied` in JS). Judged
against the rules above:

- *No byte changed shape.* The applied index is per-tree metadata
  binjson-structures has always written for every tree; a number that was
  always zero on the catalog is now sometimes not.
- *An old reader does not misread it — it ignores it*, and computes the
  floor it always computed: a max over surviving collections. So an old
  build opening a new database is exactly as buggy as it was before (a
  drop can regress its floor and halt it), and no worse. Data is not
  misinterpreted, which is the line rule 2 draws.
- *A new reader on an old database* finds zero and falls back to the same
  max, which is what every pre-fix database looks like.

What that leaves is the risk a version stamp cannot catch: the two hosts
DISAGREEING. If one wrote the field and the other could not see it, the
floors would differ about which entries are applied and replay would
resume where the files are not. So it is tested in both directions, with
the log **compacted past the drop** so that the catalog is the only place
the answer exists — *the applied index a drop leaves in the catalog
crosses in both directions* (`db.wal-instance.test.js`). Without the
compaction the test would pass on a host that ignores the field, by
replaying the entries and arriving at the same number by accident.

Changes that DO need a bump: anything that alters the meaning of
existing bytes — key encodings, journal record layout, tree node
formats, the semantics of an existing catalog field, file-naming rules
the orphan sweep relies on.

## Version 2: scalar `_id`, and how a version 1 database becomes one

The first real bump, and the migration story rule 1 asks for.

**What changed.** An `_id` may now be an ObjectId, a string (no U+0000),
a finite number, or a Date — the domain the ordered key encoding can
order — where version 1 had only ObjectIds. Two forms carry an id: the
*value form* (the binjson scalar, as documents, the wire, index rows and
change events carry it) and the *key form* (the `keyenc` part —
order-preserving, and canonical in that `5` and `5.0` are one id). The
primary tree's key is the key form, where v1 used the raw twelve
ObjectId bytes; text-index refs are key-form hex; and the WAL and
catalog rows that carry an id carry any admissible scalar.

**What did not change, and why the migration is small.** Secondary index
files are *byte-identical* between the versions for the ObjectId ids a v1
database holds: their composite keys already ended in the tagged key
form, and their rows already held the id's value form. Only the primary
tree is re-keyed. So the migration is one compaction-shaped copy per
collection — `dc_migrate_execute`, which is `dc_compact_execute` with the
primary's rows re-keyed on the way out (`Collection._migrate()` on the JS
side, the same call through the browser's namespace adapter).

**How it runs.** Opening is the whole interface; there is no migrate
command, and it is eager rather than lazy, because a v1 key is bytes a v2
read would simply not find:

1. The stamp is raised to 2 **durably, first** — the fence — carrying
   `migrating: true` (see *The stamp* above for what that flag is for).
2. Each collection whose catalog definition lacks `keys: 2` gets one
   re-keyed generation, committed by the same catalog flip that commits
   any compaction, with `keys: 2` added in that flip.
3. The flag is cleared, durably, once every collection has flipped.

Each key is derived from its document's own `_id`, never transformed from
the old key, which makes the copy **idempotent**: running it over a tree
that is already v2 writes the same tree. A crash therefore cannot
double-migrate, and resuming needs no record of where it stopped beyond
which definitions are already marked. A collection created *by* a v2
build carries no marker and needs none — it is only ever consulted for
databases the flag or the version says are being converted.

Not covered, deliberately: geo indexes store fixed twelve-byte refs, so a
geo-indexed write of a document whose `_id` is not an ObjectId is refused
(`DC_ERR_UNSUPPORTED_ID`) rather than silently truncated. Removal still
works, so such a document can always be deleted.

The escape hatch is unchanged: dump and restore through the CLI.

Tested from both hosts against real v1 fixtures — a primary tree keyed
the way v1 keyed it — in `test/db.format.test.js` (in process, including
the interrupted-migration resume) and the C harness's *a version 1
database is re-keyed when it is opened* and *a migration interrupted
after the fence resumes at the next open*.

## What the stamp does not cover

The stamp is a **database's**. An instance root holds files that belong to
no database and carry no stamp: the entry log (`__wal__.bj`), the snapshot
generations and their manifests (`__snap__-*`), and the cluster identity
(`__group__.bj`). They are versioned by their own means or not at all:

- The log and the generation files carry binjson-structures' per-file
  metadata version, and a manifest is read field by field, so an unknown
  field is ignored rather than fatal.
- `__group__.bj` is a single number written once. An older build ignores
  the file; a newer one treats absent as "no identity", which is what
  every cluster that predates it looks like (`server/group.h`).
- **The orphan sweep never sees them**: it runs per database, over one
  database's directory, so a root file cannot be judged by a collection's
  naming rules however new it is.

The peer wire has its own compatibility story rather than a version, and
it is stated where it is relied on: an old peer that does not know a
message answers nothing, and *silence is not an answer* — the identity
check treats no reply as no information rather than as permission
(`docs/db-server.md`).

## The staged-build window (unstamped, deliberately)

The staged index build (docs/db-server.md) writes two things an older
engine does not know: the `building`/`cursor` fields on a catalog
definition, and the `indexBegin`/`indexChunk` log opcodes. They exist
only while a build is IN FLIGHT — the final chunk strips the fields,
leaving a definition byte-identical to one that was never staged, and
the opcodes compact out of the log with everything else.

This did not bump the format version on its own, and the trade was
stated rather than hidden: a version bump would refuse EVERY database the
new engine ever touched, forever, to protect against opening one during a
window that lasts seconds. Inside that window an older engine would
attach a half-built index as live (wrong reads) and refuse the chunk
entries at replay (`DC_ERR_WAL_UNKNOWN_OP`). The deployment rule that
covered it is the one nisaba-web already follows — one engine version per
fleet, and never downgrade a directory that crashed mid-DDL without
replaying it on the version that wrote it.

**Format 2 closed the window anyway.** Every database a v2 engine has
touched is stamped 2, so a v1 build refuses the whole database rather
than reaching an in-flight build's fields at all — in-flight build state
included, since the fence goes down before anything else is written.

## Escape hatch

A refused database is never modified, so downgrading the data is always
possible from the newer version's side: the CLI's dump/restore pair
(`db <name> dump > x.jsonl` / `restore < x.jsonl` — shipped, `docs/
roadmap.md` P2, which once listed it as planned). There is no
in-place downgrade path and none is planned: one direction of migration
is a maintained promise, two is a liability.
