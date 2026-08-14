/*
 * db_validate.h — argument validation and the error-message table.
 *
 * Two things that were JavaScript for no reason beyond history.
 *
 * dc_strerror: the JS wrapper carried a literal map from error code to
 * message, with a comment reading "must match the BJ_ERR_* constants in
 * c/binjson.h" -- a hand-maintained second copy of C's own error
 * vocabulary, kept in sync by nothing. C owns the codes, so C owns the
 * words for them.
 *
 * The validators: name and key-spec rules that decide what the database
 * will accept. They belong with the naming scheme they guard (db_names.h)
 * and with the catalog that is about to move into C.
 *
 * What deliberately STAYS in JavaScript: the map from code to JS error
 * CLASS (DuplicateKeyError, MissingIndexedFieldError, ...). That is a
 * JavaScript type taxonomy, not a database rule, and C has no opinion
 * about it. So is the JS-type dispatch in `toId` -- C never sees a JS
 * value, only the bytes one encoded to.
 */
#ifndef DB_VALIDATE_H
#define DB_VALIDATE_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Validation codes, continuing db.h's DC_ERR_* range (which ends at -14).
 * Collection and database names get distinct codes rather than sharing one
 * because their messages differ, and callers match on those messages.
 */
#define DC_ERR_INVALID_COLLECTION_NAME (-15)
#define DC_ERR_INVALID_DB_NAME         (-16)
#define DC_ERR_RESERVED_NAME           (-17)
#define DC_ERR_EMPTY_KEY_SPEC          (-18)
#define DC_ERR_NON_ASCENDING_KEY       (-19)

/*
 * Is `code` a DETERMINISTIC command failure -- one every replica
 * applying the same command against the same state computes identically?
 *
 * A replicated state machine has to tell two kinds of failure apart. A
 * duplicate key is a RESULT: every replica reaches it, so the leader
 * reports it to the caller and the cluster carries on. An I/O error is
 * DIVERGENCE: this replica failed where others did not, so its apply
 * loop must stop rather than skip an entry and fork the state.
 *
 * src/db-replicated.js decided this with `err.name === 'Error' ||
 * err.cause`, which rests consensus safety on a JavaScript runtime
 * detail: any code path throwing a plain Error for a perfectly
 * deterministic reason halted the cluster, and a typed error raised by
 * an I/O path was swallowed as a result. The classification belongs with
 * the codes, which is here.
 *
 * The list is an ALLOWLIST and the default is 0. Getting this wrong in
 * the safe direction costs availability and a human notices; getting it
 * wrong the other way lets replicas diverge silently, which nobody
 * notices until the answers stop matching.
 */
int dc_is_deterministic(int code);

/*
 * Human-readable text for any BJ_ERR_* or DC_ERR_* code. Never NULL:
 * an unrecognized code yields a generic string naming it. The returned
 * pointer is static storage and outlives any caller.
 */
const char *dc_strerror(int code);

/*
 * A collection name must be non-empty and contain no '/' (it becomes a
 * path segment) and no NUL (it is stored as a length-counted string but
 * crosses hosts that treat NUL as a terminator). It must also not be the
 * reserved format-stamp key, which lives in the same catalog keyspace.
 */
int dc_check_collection_name(const char *name, size_t len);

/*
 * Same rules, minus the reserved-key one: a database name is a directory,
 * not a catalog key. Hosts may add their own rules on top -- the Node
 * provider additionally rejects '\\' and '..' because its names become
 * real filesystem path segments. That is a platform rule, correctly the
 * platform's, and it composes with this rather than replacing it.
 */
int dc_check_db_name(const char *name, size_t len);

/*
 * Validate a createIndex key spec -- a binjson OBJECT {field: 1, ...} with
 * at least one field and every value exactly INT 1 -- and append the field
 * names, in spec order, to `fields_out` as a binjson ARRAY of STRINGs.
 *
 * That array is exactly the shape dc_collection_add_index takes for its
 * `fields` argument, so validating and marshalling are one step instead of
 * a JS Object.keys() followed by a re-encode.
 *
 * Descending (-1) is rejected rather than supported because the ordered
 * key encoding (keyenc.h) has no per-field direction: a descending field
 * would need either a reversed encoding or a reversed scan, and reversing
 * results client-side already covers the single-field case.
 */
int dc_check_index_key_spec(const uint8_t *spec, size_t len, dbuf *fields_out);

#ifdef __cplusplus
}
#endif

#endif /* DB_VALIDATE_H */
