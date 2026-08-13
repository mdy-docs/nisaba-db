/*
 * db_catalog.h — the catalog entry schema.
 *
 * A database's catalog is a B+ tree keyed by collection name. Each value
 * is one entry describing where that collection's bytes live:
 *
 *   { file, journal?, gen?, compactedBytes?,
 *     indexes: [ { name, kind, fields|field, file|files,
 *                  unique?, sparse?, partialFilterExpression?,
 *                  expireAfterSeconds? } ] }
 *
 * That shape was defined in three places at once in JavaScript, none of
 * which could consult the others: _persistIndexes wrote it, Collection
 * ._open parsed it, and listIndexes projected it into the driver's shape.
 * A field added to one and forgotten in another is a silently
 * half-persisted index.
 *
 * docs/format-compatibility.md is explicit that the format stamp covers
 * "catalog entry shapes", so this is format-owned data and belongs beside
 * the naming scheme (db_names.h) that decides what those files are called.
 *
 * The plan/execute split
 * ---------------------
 * dc_catalog_open_plan is the first of the plan functions described in
 * bjns.h: it is PURE -- no I/O at all -- and returns the complete list of
 * files the collection needs opened, with everything needed to attach each
 * one. The host opens exactly those names (asynchronously in a browser,
 * synchronously under WASI) and then does the attaching in one
 * uninterrupted pass.
 *
 * That the plan is pure is what makes it testable without a filesystem,
 * and what lets the same C serve a host that cannot open a file
 * synchronously.
 */
#ifndef DB_CATALOG_H
#define DB_CATALOG_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "bjns.h"
#include "bplustree.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Continuing the DC_ERR_* range (db_update.h ends at -28). */
#define DC_ERR_CATALOG_ENTRY         (-29)
#define DC_ERR_INDEX_OPTION_UNSUPPORTED (-30)
#define DC_ERR_TTL_NEEDS_SINGLE_FIELD   (-31)
/* A compaction was asked for while a cursor is scanning one of the trees
 * it would rebuild. See dc_compact_execute. */
#define DC_ERR_CURSORS_OPEN             (-49)

/* Index kinds, as they appear in a plan. Distinct from the `kind` STRING
 * stored in the catalog, which stays a string for readability and
 * backward compatibility -- a pre-milestone-6 entry has no kind at all
 * and means equality. */
typedef enum {
    DC_INDEX_EQUALITY = 0,
    DC_INDEX_TEXT     = 1,
    DC_INDEX_GEO      = 2
} dc_index_plan_kind;

/*
 * Turn one catalog entry into the plan for opening its collection.
 *
 * Appends a single binjson OBJECT to `out`:
 *
 *   { primary: <file>,
 *     journal: <file>,
 *     gen: <int>,
 *     indexes: [ { name, kind: <int>, files: [<file>, ...],
 *                  fields?: [<field>, ...],   // equality
 *                  field?:  <field>,          // text / geo
 *                  unique, sparse,
 *                  partialFilterExpression?, expireAfterSeconds? } ] }
 *
 * `kind` is a STRING here ("equality" / "text" / "geo") and an INT in
 * dc_catalog_open_plan's output, which is a different document for a
 * different reader. Read a stored entry with the plan's convention and
 * you get BJ_ERR_UNKNOWN_TYPE, which is how this note came to be
 * written.
 *
 * `files` is in ATTACH order, which for a text index is exactly the order
 * dc_collection_attach_text_index takes its three trees. That ordering is
 * the plan's job: a host that had to know it would be reimplementing part
 * of the schema.
 *
 * Two pieces of backward compatibility live here rather than in a host:
 * an entry with no `kind` is an equality index (pre-milestone-6), and an
 * entry with no `journal` uses the generation-0 journal name (written
 * before that field existed).
 *
 * `coll`/`coll_len` are needed only to derive that fallback journal name.
 * DC_ERR_CATALOG_ENTRY if the entry is not an object, has no `file`, or
 * carries an index definition this build cannot make sense of.
 */
int dc_catalog_open_plan(const uint8_t *entry, size_t entry_len,
                         const char *coll, size_t coll_len, dbuf *out);

/*
 * Project a catalog entry's indexes into the shape the MongoDB driver's
 * listIndexes returns: [{ name, key: {field: 1|'text'|'2dsphere', ...},
 * unique?, sparse?, partialFilterExpression?, expireAfterSeconds? }].
 *
 * The `key` reconstruction is why this belongs with the schema: turning
 * stored `fields` back into {field: 1} pairs, and a text/geo `field` into
 * {field: 'text'}/{field: '2dsphere'}, is the inverse of what createIndex
 * did, and the two have to agree.
 */
int dc_catalog_list_indexes(const uint8_t *entry, size_t entry_len, dbuf *out);

/*
 * Add or replace one index definition in `entry`, appending the updated
 * entry to `out`. `def` is a plan-shaped index definition -- the same
 * shape dc_catalog_open_plan emits -- and is converted to the stored form
 * here, which is the only place that conversion exists.
 *
 * Replacing rather than appending when the name already exists matches
 * createIndex's delete-then-create clean slate, and keeps the entry from
 * accumulating two definitions of one index if a caller retries.
 *
 * This is what retired _persistIndexes. That function rebuilt the whole
 * `indexes` array from JavaScript's in-memory Map on every change, which
 * made the Map the effective source of truth for on-disk data and put a
 * third copy of the schema in the writer. Updating the entry in place
 * means the entry is the source of truth, and the Map is just a cache of
 * the live handles.
 */
int dc_catalog_put_index(const uint8_t *entry, size_t entry_len,
                         const uint8_t *def, size_t def_len, dbuf *out);

/*
 * Remove the index named `name` from `entry`, appending the updated entry
 * to `out`. Removing an absent name is not an error: dropIndex has
 * already established the index exists, and a retry after a partial
 * failure must not be refused.
 */
int dc_catalog_drop_index(const uint8_t *entry, size_t entry_len,
                          const char *name, size_t name_len, dbuf *out);

/*
 * The staged-build fields of ONE stored definition: `building: true` and
 * the backfill `cursor` (the OID of the last document backfilled). They
 * live in the CATALOG deliberately -- the catalog stages an applied
 * index of its own (catalog_note_applied), so one bpt_add commits the
 * cursor and the replay guard atomically, which is the whole crash
 * story of a chunk: fully applied (guarded), or re-run from the old
 * cursor with every duplicate absorbed by the backfill's if-absent adds.
 *
 * `set` rewrites the named definition inside `entry`, appending the
 * updated entry to `out`. building=1 stores the flag and, when given,
 * the 12-byte cursor; building=0 strips both -- the COMMIT, so a
 * committed definition is byte-identical to one that was never staged
 * (the apply-equivalence oracle depends on exactly that). DC_ERR_NO_INDEX
 * when nothing in `entry` has that name.
 *
 * `get` reads the same state back: *found says the definition exists at
 * all; *building and the cursor say where its build stands.
 */
int dc_catalog_index_building_set(const uint8_t *entry, size_t entry_len,
                                  const char *name, size_t name_len,
                                  int building, const uint8_t *cursor,
                                  dbuf *out);
int dc_catalog_index_building_get(const uint8_t *entry, size_t entry_len,
                                  const char *name, size_t name_len,
                                  int *found, int *building,
                                  uint8_t cursor_out[12], int *has_cursor);

/*
 * Plan a NEW index from a createIndex call: what kind it is, what it will
 * be called, and which files it needs created. Appends one plan-shaped
 * definition to `out` -- the same shape dc_catalog_open_plan emits and
 * dc_catalog_put_index stores, so one shape flows all the way from create
 * through the catalog and back out at open.
 *
 * Pure: it names files, it does not create them. The caller creates
 * exactly the names in `files` and then attaches.
 *
 * The conventions gathered here were scattered through createIndex:
 *
 *   - a single field whose value is the STRING "text" or "2dsphere" is a
 *     special index, not an ascending one;
 *   - special indexes take none of unique / sparse /
 *     partialFilterExpression / expireAfterSeconds
 *     (DC_ERR_INDEX_OPTION_UNSUPPORTED);
 *   - expireAfterSeconds needs a single-field index
 *     (DC_ERR_TTL_NEEDS_SINGLE_FIELD);
 *   - the default name mirrors the driver's: "team_1", "team_1_age_1",
 *     "body_text", "location_2dsphere", overridable with options.name.
 *
 * Default naming in particular has to agree with dc_catalog_list_indexes'
 * reconstruction of `key`, which is the other half of the same
 * convention -- so they live in one file.
 */
int dc_index_create_plan(const uint8_t *keys, size_t keys_len,
                         const uint8_t *options, size_t options_len,
                         const char *coll, size_t coll_len, dbuf *out);

/* Is this key spec a text/2dsphere index? Asked by the dispatch that
 * chooses between the staged build (equality only) and the monolithic
 * one -- the same single-field-string convention dc_index_create_plan
 * applies, exposed so the chooser cannot re-derive it differently. */
int dc_index_keys_is_special(const uint8_t *keys, size_t keys_len);

/*
 * Decide which files the orphan sweep may delete.
 *
 * `catalog` is the whole catalog as an ARRAY of {key, value} pairs -- the
 * shape BPlusTree.toArray() produces -- and `names` is the directory
 * listing as a NUL-separated buffer. Appends the victims to `out` as a
 * binjson ARRAY of STRINGs.
 *
 * The listing is an INPUT rather than a callback for the reason bjns.h
 * gives: directory enumeration is asynchronous in OPFS, and a callback
 * would need a JS function pointer in the WASM table, which
 * -sALLOW_TABLE_GROWTH=0 forbids on purpose. So the host reads the
 * directory, C decides, and the host deletes.
 *
 * A file is a victim only if it is BOTH unreferenced by the catalog AND
 * one this layer could have created (dc_is_db_file). Both halves matter:
 * the first alone would delete a host's own files sitting in the same
 * directory, and the second alone would delete every live collection.
 *
 * The catalog file itself and the format-stamp row are never victims --
 * the stamp owns no files, and deleting the catalog would destroy the
 * database it is sweeping.
 */
int dc_sweep_plan(const uint8_t *catalog, size_t catalog_len,
                  const char *names, size_t names_len, dbuf *out);

/*
 * Every file one catalog entry lays claim to, as a binjson ARRAY of
 * STRINGs: the primary, the journal, and each index's file or files.
 *
 * This is dropCollection's plan -- the files to delete once the entry is
 * gone -- and it is the same computation the orphan sweep does per entry,
 * sharing one implementation. That sharing is the point: a file kind the
 * sweep knows about but drop does not becomes an orphan on every drop,
 * and a file kind drop knows about but the sweep does not gets deleted
 * from under a live collection.
 *
 * Applies the same generation-0 journal fallback as the open plan, for
 * entries written before that field existed.
 */
int dc_collection_files(const uint8_t *entry, size_t entry_len,
                        const char *coll, size_t coll_len, dbuf *out);

/*
 * Every file ONE STORED index definition claims, as a binjson ARRAY of
 * STRINGs: `file` for an equality or geo index, every role in `files` for
 * a text one. dropIndex's plan, and the same computation the sweep and
 * dropCollection do, from one implementation.
 *
 * It is here rather than in the caller because the STORED shape is not the
 * PLAN shape (put_stored_def says why): a plan says `files: [array]` for
 * every kind, the catalog says `file` or `files{}` depending on kind. Code
 * that reaches into a stored definition for a plan-shaped array finds
 * nothing on two kinds out of three -- which is precisely the bug this
 * function was extracted to fix, and it needed no crash to happen.
 */
int dc_index_files(const uint8_t *def, size_t def_len, dbuf *out);

/*
 * A fresh catalog entry for a collection that does not exist yet: just
 * the primary file name, derived from the naming scheme. Later fields
 * (journal, gen, compactedBytes, indexes) are added as they are earned,
 * which is what keeps an untouched collection's entry small and is why
 * every reader treats them as optional.
 */
int dc_catalog_new_entry(const char *coll, size_t coll_len, dbuf *out);

/*
 * Plan a compaction (docs/compaction.md).
 *
 * Appends one binjson OBJECT to `out`:
 *
 *   { gen: <int>,                 // the generation being built
 *     newEntry: { ... },          // the catalog entry to flip to
 *     build: [ { name, kind, files: [<new file>, ...] } ],
 *     oldFiles: [ <file>, ... ] } // to delete AFTER the flip
 *
 * `build` is in the same plan shape as everywhere else, and its entries
 * line up with newEntry's indexes: for each one the caller streams the
 * matching live structure into each named file, in order. The primary and
 * the journal are newEntry's own `file` and `journal`.
 *
 * Pure -- it names the whole new generation without creating any of it,
 * which is what lets the caller create exactly these files and delete
 * exactly `oldFiles`, with the single catalog write between them.
 *
 * Everything here was JavaScript spreading the old entry and patching
 * names into the copy: `{...entry, gen, file: collectionFileName(name,
 * gen), indexes: []}` and then `{...def, file}` per index. That is the
 * catalog schema being rewritten by hand at the one moment when getting
 * it wrong strands a whole generation -- so it belongs with the schema.
 */
int dc_compact_plan(const uint8_t *entry, size_t entry_len,
                    const char *coll, size_t coll_len, dbuf *out);

/*
 * Compute the orphan sweep AND carry it out, through `ns`.
 *
 * Same decision as dc_sweep_plan -- it calls it -- but C also does the
 * deleting, which makes this the first operation to drive a bj_ns rather
 * than hand names back for a host to act on. `*deleted` receives the
 * count.
 *
 * The two adapters differ in exactly the way bjns.h anticipates, and
 * neither difference is visible here: bjio_posix unlinks immediately,
 * while bjns_bridge queues the name for the host to drain once this
 * synchronous call returns. Deferring is safe because a sweep only ever
 * removes files the catalog already does not reference -- an undeleted
 * one is an orphan the next sweep collects, never a correctness problem.
 *
 * A single removal failing does not abort the sweep: the remaining
 * orphans are still worth collecting, and whatever refused to unlink will
 * be offered again next time.
 */
int dc_sweep_execute(bj_ns *ns, const uint8_t *catalog, size_t catalog_len,
                     const char *names, size_t names_len, uint32_t *deleted);

/* What kind of live structure a compaction source is. */
typedef enum { DC_SRC_BPT = 0, DC_SRC_RTREE = 1 } dc_source_kind;

/*
 * Build a compacted generation and flip the catalog to it, in ONE
 * synchronous call.
 *
 * `plan` is dc_compact_plan's output. `sources` are the live structures
 * to stream from, in the plan's build order: the primary first, then each
 * index's files (three for a text index, one otherwise). `catalog` is the
 * catalog tree; `coll` its key for this collection.
 *
 * Every destination is opened through `ns` -- which is why the caller
 * must have made every name in the plan resolvable first. Under the
 * browser adapter that means pre-opening them; under POSIX the adapter
 * just opens.
 *
 * Why one call. Between the last byte of the new generation and the
 * catalog write that adopts it, nothing may observe the collection
 * half-migrated. Spanning that window with awaits meant relying on the
 * JS-side gate to hold across every one of them; a synchronous call
 * cannot be interleaved at all.
 *
 * It does NOT make the gate unnecessary. The caller still awaits before
 * (pre-opening) and after (reopening its wrappers, deleting the old
 * files), so a concurrent operation must still be kept out across those.
 *
 * REFUSED WHILE A CURSOR IS OPEN over any tree it would rebuild
 * (DC_ERR_CURSORS_OPEN, before anything is written). A cursor iterates
 * nodes that mutations never overwrite, which is what makes it a
 * snapshot -- and a compaction is the one operation that takes those
 * nodes away. The caller drains or closes its cursors and asks again.
 *
 * Ordering is the contract: the flip is the LAST thing this does, so a
 * failure anywhere leaves the collection fully on the old generation with
 * the new files merely orphaned. The caller deletes them; a crash instead
 * leaves them for the next sweep.
 */
int dc_compact_execute(bj_ns *ns, bpt *catalog,
                       const char *coll, size_t coll_len,
                       const uint8_t *plan, size_t plan_len,
                       void *const *sources, const int *source_kinds,
                       uint32_t nsources, uint64_t *bytes_built);

#ifdef __cplusplus
}
#endif

#endif /* DB_CATALOG_H */
