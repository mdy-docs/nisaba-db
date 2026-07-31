/*
 * db_names.h — the database's file-naming scheme and format stamp.
 *
 * These are the conventions that decide what a collection, index, journal
 * or catalog is *called* on disk. They were JavaScript
 * (wasm/nisaba-wasm.js) because of an argument recorded in
 * docs/db-plan.md: JS must compute a file name before it can open the
 * file, so the name cannot be learned from the catalog first. That is
 * true, and it is an argument for JS *asking* for a name -- not for JS
 * *owning* the scheme.
 *
 * It matters that C owns it, because the catalog, the compaction
 * generation flip and the orphan sweep all reason about these names, and
 * all three are moving into C. A host that cannot name a file cannot own
 * a catalog.
 *
 * docs/format-compatibility.md is the contract for changing any of this:
 * the stamp covers "catalog entry shapes, file naming (g<N> generations,
 * coll-/idx- prefixes), journal record layout ... and the tree/index file
 * formats beneath". DC_FORMAT_VERSION lived in JS while the format it
 * stamps was C's -- an inversion this fixes.
 *
 * Generations (docs/compaction.md). compact() rewrites a collection's
 * whole file set into fresh files carrying a `g<N>-` PREFIX, and records
 * the new names in the catalog -- the catalog entry, not this convention,
 * is what open() trusts. A prefix rather than a `.g<N>` suffix so a
 * generation can never collide with a gen-0 name: every gen-0 file starts
 * with `coll-`/`idx-` while every gen>0 file starts with `g<digits>-`,
 * and collection/index names may legally contain dots (a collection
 * literally named "users.g2" must not claim generation 2 of "users").
 * Generation 0 keeps the historical unprefixed names, so pre-compaction
 * databases open unchanged.
 */
#ifndef DB_NAMES_H
#define DB_NAMES_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

/* On-disk format version. Bump only with a written migration story --
 * see docs/format-compatibility.md's three conditions. */
#define DC_FORMAT_VERSION 1

/* The B+ tree order every collection, index and catalog in this format is
 * written with. A tree is opened with the order it was written with, so
 * this is a property of the format rather than a tuning knob -- and
 * DB_DEFAULT_ORDER in wasm/nisaba-wasm.js must agree with it, because a
 * JS host and a native one open the same files. */
#define DC_DEFAULT_ORDER 32

/* The catalog's own file, and the reserved catalog key holding the format
 * stamp. The latter is not a collection name and must never be usable as
 * one (dc_check_collection_name enforces that). */
#define DC_CATALOG_FILE "__catalog__.bj"
#define DC_FORMAT_KEY   "__format__"

/* The three files a text index is always made of, in attach order. */
typedef enum {
    DC_TEXT_ROLE_TERMS     = 0,   /* -terms.bj     */
    DC_TEXT_ROLE_DOCUMENTS = 1,   /* -documents.bj */
    DC_TEXT_ROLE_LENGTHS   = 2    /* -lengths.bj   */
} dc_text_role;

/*
 * Append a file name to `out`. Each returns BJ_OK, or BJ_ERR_OOM. `gen` 0
 * means the historical unprefixed name.
 *
 * Collection and index names are passed as explicit (ptr, len) pairs
 * rather than C strings: they come from user data and may contain any
 * byte except '/' and NUL (dc_check_collection_name).
 */
int dc_collection_file_name(dbuf *out, const char *coll, size_t coll_len, uint32_t gen);
int dc_index_file_name(dbuf *out, const char *coll, size_t coll_len,
                       const char *idx, size_t idx_len, uint32_t gen);
int dc_text_index_file_name(dbuf *out, const char *coll, size_t coll_len,
                            const char *idx, size_t idx_len, uint32_t gen,
                            dc_text_role role);
int dc_journal_file_name(dbuf *out, const char *coll, size_t coll_len, uint32_t gen);

/*
 * 1 if `name` is a file this layer could have created for itself -- any
 * generation of a collection, index or journal file. This is exactly what
 * the orphan sweep is allowed to delete when the catalog does not
 * reference it, so it deliberately does NOT match the catalog file, nor
 * anything a host happened to put in the same directory.
 *
 * Equivalent to the JS /^(?:g\d+-)?(?:coll|idx)-.*\.bj$/ it replaces.
 */
int dc_is_db_file(const char *name, size_t len);

#ifdef __cplusplus
}
#endif

#endif /* DB_NAMES_H */
