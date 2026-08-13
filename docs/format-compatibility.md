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
  definition version 1; it is stamped on open and otherwise unchanged.
- A database stamped **at or below** the build's version opens normally
  (see the bump rules below).
- A database stamped **above** the build's version is refused before
  anything touches the files — in particular before the orphan sweep,
  which must never judge a future format's files by an old version's
  naming rules. The JS host's error names both versions; the C server
  answers `DC_ERR_FORMAT_NEWER`, whose text points here and does *not*
  carry the version it found (a coded error cannot; threading the number
  out of `check_format` is the change if that ever matters).

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

## Escape hatch

A refused database is never modified, so downgrading the data is always
possible from the newer version's side: the CLI's dump/restore pair
(`db <name> dump > x.jsonl` / `restore < x.jsonl` — shipped, `docs/
roadmap.md` P2, which once listed it as planned). There is no
in-place downgrade path and none is planned: one direction of migration
is a maintained promise, two is a liability.
