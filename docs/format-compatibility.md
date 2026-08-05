# On-disk format compatibility

The contract for what happens when one version of nisaba opens files
written by another. Written while there is only one format version, on
purpose — this page is cheap now and expensive after the first change.

## The stamp

Every database carries a format version in its catalog under the
reserved key `__format__` (`DB_FORMAT_KEY`/`DB_FORMAT_VERSION` in
`src/nisaba-wasm.js`; enforced by `test/db.format.test.js`):

- `Db.open()` stamps a fresh database with the current version.
- A database with **no stamp** predates the stamp mechanism and is by
  definition version 1; it is stamped on open and otherwise unchanged.
- A database stamped **at or below** the build's version opens normally
  (see the bump rules below).
- A database stamped **above** the build's version is refused with an
  error naming both versions, before anything touches the files — in
  particular before the orphan sweep, which must never judge a future
  format's files by an old version's naming rules.

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

Changes that DO need a bump: anything that alters the meaning of
existing bytes — key encodings, journal record layout, tree node
formats, the semantics of an existing catalog field, file-naming rules
the orphan sweep relies on.

## Escape hatch

A refused database is never modified, so downgrading the data is always
possible from the newer version's side (its CLI can export; a
dump/restore pair is planned — `docs/roadmap.md` P2). There is no
in-place downgrade path and none is planned: one direction of migration
is a maintained promise, two is a liability.
