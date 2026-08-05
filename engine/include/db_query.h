/*
 * db_query.h — filter matching, sorting, and projection for collection
 * queries (db.h). Operates purely on binjson bytes (documents, filters,
 * sort/projection specs); knows nothing about bpt or dc_collection.
 *
 * Filters are binjson OBJECTs. A top-level key is either a logical
 * operator ($and/$or/$nor, each taking a non-empty ARRAY of filter
 * OBJECTs) or a field path (dot-separated, e.g. "a.b.c" — descending
 * through nested OBJECTs only; a path segment that would need to index
 * into an ARRAY, or implicitly fan out over an array of subdocuments, is
 * not supported yet and simply resolves to "not found"). All top-level
 * entries are ANDed together, matching the reference's shallow-equality
 * behavior it replaces.
 *
 * A field's value is either a literal (matched by equality) or an
 * "operator expression" — an OBJECT whose keys are *all* $-prefixed
 * ($eq, $ne, $gt, $gte, $lt, $lte, $in, $nin, $exists, $not, $size, $all,
 * $type, $mod, $elemMatch, $regex + $options — milestone 11). Multiple
 * operators on one field are ANDed (e.g. {age: {$gte: 18, $lt: 65}}).
 * Any other $-prefixed key is an error (BJ_ERR_STATE) rather than a
 * silent no-op — an unrecognized operator must never be mistaken for
 * "matches everything".
 *
 * If a resolved field value is an ARRAY, every operator except $exists/
 * $size/$elemMatch matches if it holds for the array value itself *or*
 * for any one of its elements (one level deep — matching MongoDB's
 * default array-field behavior for a plain path). $size checks the
 * array's own element count (not its elements). $elemMatch is the one
 * operator with element-*wide*-AND semantics: its sub-query (an operator
 * expression for a scalar-element array, or a plain query object for an
 * array of subdocuments) must hold entirely against *one* element, unlike
 * every other operator's any-element-independently matching.
 *
 * $type accepts MongoDB-familiar string aliases ("string", "number" (INT
 * or FLOAT), "int", "double", "bool" (TRUE or FALSE), "date", "objectId",
 * "array", "object", "null") rather than BSON's numeric type codes — this
 * isn't BSON, and a string is the natural fit for a JS-facing API.
 *
 * $regex only supports the operator-expression form ({field: {$regex:
 * "pattern", $options: "flags"}}, both plain strings) — binjson has no
 * BSON-regex wire type, so a bare native-RegExp-literal filter value isn't
 * representable in the codec at all. The pattern itself is ECMAScript-
 * flavored (named groups, lookahead/lookbehind, backreferences, Unicode
 * property escapes — see regex.h and third_party/regex-engine/README.md),
 * but `$options` at this layer still only accepts `i`; other flags
 * (`m`/`s`/`u`/etc.) are valid pattern syntax to compile but have no way to
 * be requested here yet.
 *
 * Equality ($eq, and bare-literal matching) is exact encoded-byte
 * equality, same rationale as db.h: binjson's encoder is a deterministic
 * function of the JS value, so this reproduces MongoDB's embedded-
 * document/array exact-match semantics for free. A missing field never
 * equality-matches even a literal `null` (MongoDB's null-matches-missing
 * quirk is not implemented). Ordering ($gt/$gte/$lt/$lte/$mod's numeric
 * candidates) only compares same-domain values — number vs number (INT
 * and FLOAT unified, as everywhere else in this codebase), string vs
 * string (byte-wise), or (milestone 9, for TTL) Date vs Date — a cross-
 * domain comparison (or against an unsupported type) never matches;
 * MongoDB's full cross-BSON-type ordering is not implemented.
 */
#ifndef DB_QUERY_H
#define DB_QUERY_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Does `doc` (a binjson OBJECT) match `filter` (a binjson OBJECT)? */
int qry_matches(const uint8_t *doc, size_t doc_len,
                const uint8_t *filter, size_t filter_len, int *out_matches);

/*
 * Resolve dot-separated `path` (e.g. "a.b.c") against `doc`, descending
 * through nested OBJECTs only -- same rule qry_matches's field-path
 * handling uses (see this header's top comment). *found is 0 if any
 * segment is missing, or a non-terminal segment isn't itself an OBJECT.
 * Exposed for db.c's distinct(), which needs the same path resolution
 * qry_matches uses internally.
 */
int qry_resolve_path(const uint8_t *doc, size_t doc_len,
                     const uint8_t *path, uint32_t path_len,
                     const uint8_t **val_ptr, size_t *val_len, int *found);

/* A growable list of byte spans (views into someone else's buffer, not
 * owned) plus byte-equality comparison -- exposed for db.c's distinct(),
 * which needs the same "accumulate candidate values, dedup by exact
 * encoded-byte equality" shape qry_matches's own array-element candidate
 * list (db_query.c) already implements. */
typedef struct { const uint8_t *ptr; size_t len; } val_span;
typedef struct { val_span *items; uint32_t count, cap; } val_list;

int val_list_push(val_list *l, const uint8_t *ptr, size_t len);
void val_list_free(val_list *l);
int value_eq(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen);

/* -2 = incomparable (different domains, or not number/string/date), else
 * -1/0/1. See this header's top comment: only number-vs-number, string-
 * vs-string, or Date-vs-Date order. Exposed for db_update.c's $min/$max/
 * $push's $sort modifier. */
int qry_value_cmp(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen);

/* Evaluate operator-expression `expr` (e.g. {$gt: 5}) against a single
 * resolved value (not a field path) -- exposed for db_update.c's $pull
 * query-condition support ({$pull: {field: {$gt: 5}}}), reusing the same
 * per-key operator dispatch qry_matches's field conditions use. */
int qry_value_matches_expr(const uint8_t *value, size_t value_len,
                           const uint8_t *expr, size_t expr_len, int *out_match);

/*
 * True (via *is_expr) iff `value` is a binjson OBJECT whose keys are all
 * $-prefixed (an "operator expression", e.g. {$gt: 5}) rather than a
 * literal to match by equality. An empty object {} is a literal (matches
 * a field whose value is itself {}). Exposed so db.c's equality-index
 * planner can recognize the same bare-value/{$eq: v} shape qry_matches
 * treats as a plain equality condition.
 */
int qry_is_operator_expr(const uint8_t *value, size_t value_len, int *is_expr);

typedef struct {
    const uint8_t *sort;        /* binjson OBJECT {field: 1|-1, ...}, or NULL for none */
    uint32_t sort_len;
    int64_t skip;                 /* 0 = none */
    int64_t limit;                 /* 0 = unlimited */
    const uint8_t *projection;   /* binjson OBJECT {field: 0|1, ...}, or NULL for none */
    uint32_t projection_len;
} qry_options;

/* Validate opts->sort up front (BJ_ERR_STATE if any value isn't exactly 1
 * or -1) so a later qry_collect can't fail partway through a sort. Safe to
 * call with a NULL `opts` (no-op, BJ_OK). */
int qry_validate_options(const qry_options *opts);

typedef struct { const uint8_t *ptr; size_t len; } qry_doc;

/*
 * Apply opts->sort/skip/limit/projection (any/all may be absent: NULL
 * `opts`, no sort, skip 0, limit 0, no projection) to `docs` — already-
 * matched, already-collected documents the caller owns and keeps valid
 * for the duration of this call — and emit the result as a binjson ARRAY
 * through *out / *out_len (freshly malloc'd, caller frees). Does not take
 * ownership of `docs` or its entries.
 */
int qry_collect(const qry_doc *docs, size_t count, const qry_options *opts,
                uint8_t **out, size_t *out_len);

/*
 * Apply a single field-inclusion/exclusion projection spec (the same
 * shape qry_collect's opts->projection uses) to one document, emitting
 * the result through *out / *out_len (freshly malloc'd, caller frees).
 * Exposed for db.c's dc_cursor, which projects one document at a time as
 * it streams rather than batch-projecting an already-collected array the
 * way qry_collect does.
 */
int qry_project_one(const uint8_t *doc, size_t doc_len,
                    const uint8_t *proj, size_t proj_len,
                    uint8_t **out, size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* DB_QUERY_H */
