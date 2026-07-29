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
#define DC_ERR_CATALOG_ENTRY (-29)

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

#ifdef __cplusplus
}
#endif

#endif /* DB_CATALOG_H */
