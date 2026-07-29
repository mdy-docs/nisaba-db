/*
 * db_update.h — apply MongoDB-style update operators to a document.
 *
 * An update document's top level must be entirely $-prefixed operators —
 * $set, $unset, $inc, $mul, $min, $max, $rename, $setOnInsert, $addToSet,
 * $push, $pull, $pullAll, $pop, $bit — each an OBJECT mapping a field name
 * to an operand; a plain field (a full replacement document) is rejected
 * (BJ_ERR_STATE), since that is replaceOne's job, not updateOne/
 * updateMany's, matching the modern MongoDB driver's own validation.
 * $currentDate is resolved into $set by upd_resolve_current_date (below)
 * before upd_apply ever sees an update, so the operator table here has no
 * entry for it. The clock stays a caller-supplied parameter -- WASM has no
 * portable one, the same reasoning that puts _id generation in the host --
 * but the REWRITE, including its collision rules against every other
 * operator, is update logic and lives here.
 *
 * Scope, deliberately conservative (matching how db_query.h and keyenc.h
 * scope their own first cuts):
 *   - Target field names are top-level only — no dotted paths yet, so
 *     {$set: {"a.b": 1}} is rejected rather than silently doing something
 *     the caller didn't ask for. Auto-vivifying intermediate objects for a
 *     dotted $set path is real MongoDB behavior, not implemented here.
 *     Positional array operators ($, $[], $[<id>]) are equally out of
 *     scope. Both are left for a dedicated follow-up milestone.
 *   - A field may be targeted by at most one operator per update (matches
 *     MongoDB's own "path collision" validation). $rename's *destination*
 *     name is checked against every other operator's target field name,
 *     and against every other $rename's destination, too -- but not
 *     against pre-existing fields of the document being updated (that
 *     collision is resolved dynamically at apply time: the destination
 *     field is simply overwritten, same as real MongoDB).
 *   - `_id` may never be targeted by any operator (matches MongoDB: the
 *     _id field cannot be modified by an update), including as a $rename
 *     source or destination.
 *   - $inc/$mul require the field (if present) and the operand to both be
 *     numbers (INT/FLOAT unified, as everywhere else in this codebase);
 *     the result is INT if it's a safe integer, else FLOAT — the same rule
 *     src/binjson-wasm.js's encoder uses for a plain JS number. $mul on a
 *     missing field seeds it at 0 (matches real MongoDB: multiplying by
 *     the implicit base 0).
 *   - $min/$max compare the field's current value (if present) against
 *     the operand using the same number/string/Date ordering db_query.h's
 *     $gt/$lt use; an incomparable pair (different domains, or a type
 *     ordering isn't implemented for) is BJ_ERR_STATE -- unlike a filter's
 *     "incomparable never matches", a value-producing operator can't
 *     silently do nothing. A missing field is seeded with the operand
 *     directly (no comparison).
 *   - $rename requires a BJ_TYPE_STRING operand (the destination field
 *     name), itself subject to the same non-dotted/non-_id rules as any
 *     target field name. Renaming a field that doesn't exist is a
 *     complete no-op -- it does not touch a pre-existing destination
 *     field either (matches real MongoDB).
 *   - $setOnInsert is only ever applied on dc_update_one/dc_update_many's
 *     upsert-and-inserted path (upd_apply's `is_insert` parameter); on a
 *     normal matched update it is a complete no-op, though it still
 *     participates in the one-operator-per-field collision check.
 *   - $addToSet appends its operand (or each element of a {$each: [...]}
 *     batch) only if not already byte-equal to an existing element (or to
 *     one just added earlier in the same batch); requires the field, if
 *     present, to be an ARRAY.
 *   - $push always appends (no modifiers), or, if the operand is an
 *     OBJECT containing a $each key, supports $each (an array of elements
 *     to insert), $slice (keep only the first/last N of the resulting
 *     array), $sort (1/-1 only -- no document-key sort form for arrays of
 *     subdocuments) and $position (insertion index; ignored when $sort is
 *     present, matching real MongoDB). Requires the field, if present, to
 *     be an ARRAY.
 *   - $pull removes every element *byte-equal* to its operand, or, if the
 *     operand is itself an operator expression (e.g. {$gt: 5}), every
 *     element matching that expression (reusing db_query.h's matcher).
 *     $pullAll removes every element byte-equal to *any* of its (array)
 *     operand's values -- no operator-expression form. Both require the
 *     field, if present, to be an ARRAY; both are a no-op if the field is
 *     absent.
 *   - $pop removes the last (operand 1) or first (operand -1) element of
 *     an ARRAY field; a no-op if the field is absent, BJ_ERR_STATE if
 *     present but not an ARRAY, a no-op if the array is already empty.
 *   - $bit applies one or more of `and`/`or`/`xor` (bitwise, in encounter
 *     order) to an INT field, defaulting the base to 0 if the field is
 *     absent; BJ_ERR_STATE if the field is present but not an INT (no
 *     bitwise operations on FLOAT).
 */
#ifndef DB_UPDATE_H
#define DB_UPDATE_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"

/* Continuing the DC_ERR_* range (db_agg.h ends at -26). */
#define DC_ERR_BAD_CURRENT_DATE      (-27)
#define DC_ERR_CURRENT_DATE_CONFLICT (-28)

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Apply `update` to `doc` (both binjson OBJECTs), producing a modified
 * copy through *out / *out_len (freshly malloc'd, caller frees). `doc`'s
 * `_id` field, and any field the update doesn't target, is copied through
 * unchanged. `is_insert` selects whether $setOnInsert fields are applied
 * (1, `doc` is a freshly-built upsert seed) or ignored entirely (0, a
 * normal matched-document update).
 */
int upd_apply(const uint8_t *doc, size_t doc_len,
              const uint8_t *update, size_t update_len,
              int is_insert,
              uint8_t **out, size_t *out_len);

/*
 * Validate `update`'s shape (the same checks upd_apply does before
 * touching any particular document) without needing a document at hand —
 * for validating an update spec up front, before scanning for matches.
 */
/*
 * Rewrite {$currentDate: {...}} into an equivalent {$set: {...}} carrying
 * `now_ms`, appending the resulting update document to `out`.
 *
 * Each $currentDate entry must be TRUE or {$type: "date"}
 * (DC_ERR_BAD_CURRENT_DATE otherwise), and its field must not already be
 * targeted by another operator or by an existing $set
 * (DC_ERR_CURRENT_DATE_CONFLICT) -- the same path-collision rule the rest
 * of this file enforces, which is exactly why the rewrite belongs here
 * rather than in a host that would have to restate it.
 *
 * An update with no $currentDate is copied through unchanged, so callers
 * can run everything through this unconditionally. It is also idempotent
 * on an already-resolved update, which is what lets the WAL pre-resolve a
 * command at proposal time and the apply path run the same helper again
 * without a second clock reading changing the outcome.
 */
int upd_resolve_current_date(const uint8_t *update, size_t update_len,
                             int64_t now_ms, dbuf *out);

int upd_validate(const uint8_t *update, size_t update_len);

#ifdef __cplusplus
}
#endif

#endif /* DB_UPDATE_H */
