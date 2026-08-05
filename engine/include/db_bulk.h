/*
 * db_bulk.h — what a bulkWrite operation list is allowed to contain.
 *
 * bulkWrite itself is orchestration over a collection's own already-atomic
 * methods (docs/db-api.md: "no new C-level transaction logic is needed"),
 * and orchestration over async methods is JavaScript's job. So the loop
 * stays there -- deliberately, and it is now ONE loop shared by both
 * collection implementations rather than the two near-identical copies
 * that src/db-wal.js's own comment described as "Same loop as the inner
 * bulkWrite (src/nisaba-wasm.js)".
 *
 * What is NOT orchestration is the grammar: which operation names exist,
 * and which fields each one requires. That is a rule about what the
 * database accepts, it belongs with every other such rule, and the JS
 * never actually enforced it -- a missing `document` or `filter` reached
 * insertOne as `undefined` and failed somewhere further down with a
 * message about the wrong thing. dc_bulk_parse validates the whole list
 * up front and hands back one type code per operation.
 *
 * Validating the whole list before running any of it also makes the
 * `ordered` contract honest: an unordered bulkWrite is supposed to
 * attempt every operation, which it cannot do if operation seven is
 * malformed in a way that only surfaces once operations one through six
 * have already been applied.
 */
#ifndef DB_BULK_H
#define DB_BULK_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Continuing the DC_ERR_* range (db_validate.h ends at -19). */
#define DC_ERR_BULK_EMPTY         (-20)
#define DC_ERR_BULK_UNKNOWN_OP    (-21)
#define DC_ERR_BULK_MISSING_FIELD (-22)

/* Operation type codes, in the order dc_bulk_parse emits them. The JS
 * side dispatches on these rather than on strings. */
typedef enum {
    DC_BULK_INSERT_ONE  = 0,
    DC_BULK_UPDATE_ONE  = 1,
    DC_BULK_UPDATE_MANY = 2,
    DC_BULK_REPLACE_ONE = 3,
    DC_BULK_DELETE_ONE  = 4,
    DC_BULK_DELETE_MANY = 5
} dc_bulk_op;

/*
 * Validate `ops` -- a binjson ARRAY of single-key OBJECTs such as
 * {insertOne: {document: {...}}} -- and append one INT type code per
 * operation to `out` as a binjson ARRAY.
 *
 * Returns BJ_OK, or DC_ERR_BULK_EMPTY / DC_ERR_BULK_UNKNOWN_OP /
 * DC_ERR_BULK_MISSING_FIELD. On any rejection, `*bad_index` is the
 * zero-based position of the offending operation so the caller can name
 * it, and `out` is left untouched.
 */
int dc_bulk_parse(const uint8_t *ops, size_t len, dbuf *out, int *bad_index);

#ifdef __cplusplus
}
#endif

#endif /* DB_BULK_H */
