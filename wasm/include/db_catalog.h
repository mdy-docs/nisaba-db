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

#ifdef __cplusplus
extern "C" {
#endif

/* Continuing the DC_ERR_* range (db_update.h ends at -28). */
#define DC_ERR_CATALOG_ENTRY         (-29)
#define DC_ERR_INDEX_OPTION_UNSUPPORTED (-30)
#define DC_ERR_TTL_NEEDS_SINGLE_FIELD   (-31)

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

#ifdef __cplusplus
}
#endif

#endif /* DB_CATALOG_H */
