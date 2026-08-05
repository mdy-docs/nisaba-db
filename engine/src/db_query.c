/*
 * db_query.c — see db_query.h.
 */
#include "db_query.h"
#include "bjcursor.h"
#include "dbuf.h"
#include "regex.h"

#include <stdlib.h>
#include <string.h>

/* ---- field path resolution --------------------------------------------- */

/* Resolve dot-separated `path` against `doc`, descending through nested
 * OBJECTs only -- see db_query.h's top comment for the array/index limitation. */
int qry_resolve_path(const uint8_t *doc, size_t doc_len,
                     const uint8_t *path, uint32_t path_len,
                     const uint8_t **val_ptr, size_t *val_len, int *found) {
    uint32_t start = 0;
    const uint8_t *cur_obj = doc;
    size_t cur_len = doc_len;
    for (;;) {
        uint32_t dot = start;
        while (dot < path_len && path[dot] != '.') dot++;
        uint32_t seg_len = dot - start;

        const uint8_t *vp; size_t vl; int f;
        int e = obj_get_field(cur_obj, cur_len, path + start, seg_len, &vp, &vl, &f);
        if (e) return e;
        if (!f) { *found = 0; return BJ_OK; }

        if (dot >= path_len) {
            *val_ptr = vp; *val_len = vl; *found = 1;
            return BJ_OK;
        }
        if (vl < 1 || vp[0] != BJ_TYPE_OBJECT) { *found = 0; return BJ_OK; }
        cur_obj = vp; cur_len = vl;
        start = dot + 1;
    }
}

/* ---- candidate values (array element matching) -------------------------- */
/* val_span/val_list themselves are declared in db_query.h (also used by
 * db.c's distinct()). */

int val_list_push(val_list *l, const uint8_t *ptr, size_t len) {
    if (l->count == l->cap) {
        uint32_t ncap = l->cap ? l->cap * 2 : 4;
        val_span *nb = (val_span *)realloc(l->items, ncap * sizeof(val_span));
        if (!nb) return BJ_ERR_OOM;
        l->items = nb; l->cap = ncap;
    }
    l->items[l->count].ptr = ptr;
    l->items[l->count].len = len;
    l->count++;
    return BJ_OK;
}

void val_list_free(val_list *l) {
    free(l->items);
    l->items = NULL; l->count = l->cap = 0;
}

/* The value itself, plus (if it's an ARRAY) each element -- one level. */
static int expand_candidates(const uint8_t *val, size_t val_len, val_list *out) {
    int e = val_list_push(out, val, val_len);
    if (e) return e;
    if (val_len < 1 || val[0] != BJ_TYPE_ARRAY) return BJ_OK;

    cur c = { val, val_len, 0 };
    uint32_t count;
    e = array_begin(&c, &count);
    if (e) return e;
    for (uint32_t i = 0; i < count; i++) {
        size_t estart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        e = val_list_push(out, c.d + estart, c.pos - estart);
        if (e) return e;
    }
    return BJ_OK;
}

/* ---- value comparison ---------------------------------------------------- */

int value_eq(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen) {
    return alen == blen && (alen == 0 || memcmp(a, b, alen) == 0);
}

/* -2 = incomparable (different domains, or not number/string), else
 * -1/0/1. See db_query.h: only number-vs-number and string-vs-string order. */
int qry_value_cmp(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen) {
    if (alen < 1 || blen < 1) return -2;
    uint8_t ta = a[0], tb = b[0];
    int a_num = (ta == BJ_TYPE_INT || ta == BJ_TYPE_FLOAT);
    int b_num = (tb == BJ_TYPE_INT || tb == BJ_TYPE_FLOAT);
    if (a_num && b_num) {
        cur ca = { a, alen, 0 }; cur cb = { b, blen, 0 };
        double da, db;
        if (read_number(&ca, &da) || read_number(&cb, &db)) return -2;
        if (da < db) return -1;
        if (da > db) return 1;
        return 0;
    }
    if (ta == BJ_TYPE_STRING && tb == BJ_TYPE_STRING) {
        cur ca = { a, alen, 0 }; cur cb = { b, blen, 0 };
        const uint8_t *sa; uint32_t la; const uint8_t *sb; uint32_t lb;
        if (take_string(&ca, &sa, &la) || take_string(&cb, &sb, &lb)) return -2;
        uint32_t n = la < lb ? la : lb;
        int c = n ? memcmp(sa, sb, n) : 0;
        if (c < 0) return -1;
        if (c > 0) return 1;
        if (la < lb) return -1;
        if (la > lb) return 1;
        return 0;
    }
    /* Date-vs-Date only (a Date is 1 type byte + 8-byte LE millis-since-
     * epoch, c/binjson.c's bj_put_date) -- scoped narrowly as a milestone-9
     * TTL prerequisite ($lt/$gt against expireAfterSeconds cutoffs), not
     * the broader cross-BSON-type ordering db_query.h documents as
     * out of scope. */
    if (ta == BJ_TYPE_DATE && tb == BJ_TYPE_DATE) {
        if (alen != 9 || blen != 9) return -2;
        int64_t da = (int64_t)rdu64(a + 1), db = (int64_t)rdu64(b + 1);
        if (da < db) return -1;
        if (da > db) return 1;
        return 0;
    }
    return -2;
}

/* ---- operator-expression detection --------------------------------------- */

int qry_is_operator_expr(const uint8_t *value, size_t value_len, int *is_expr) {
    *is_expr = 0;
    if (value_len < 1 || value[0] != BJ_TYPE_OBJECT) return BJ_OK;
    cur c = { value, value_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;
    if (count == 0) return BJ_OK;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        e = skip_value(&c);
        if (e) return e;
        if (klen == 0 || kp[0] != '$') return BJ_OK;
    }
    *is_expr = 1;
    return BJ_OK;
}

/* ---- operators ------------------------------------------------------------ */

static int op_eq(const val_list *cands, const uint8_t *operand, size_t operand_len) {
    for (uint32_t i = 0; i < cands->count; i++) {
        if (value_eq(cands->items[i].ptr, cands->items[i].len, operand, operand_len)) return 1;
    }
    return 0;
}

static int op_in(const val_list *cands, const uint8_t *arr, size_t arr_len, int *out) {
    cur c = { arr, arr_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    for (uint32_t i = 0; i < count; i++) {
        size_t estart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        if (op_eq(cands, c.d + estart, c.pos - estart)) { *out = 1; return BJ_OK; }
    }
    *out = 0;
    return BJ_OK;
}

typedef enum { QRY_GT, QRY_GTE, QRY_LT, QRY_LTE } cmp_mode;

static int op_compare(const val_list *cands, const uint8_t *operand, size_t operand_len, cmp_mode mode) {
    for (uint32_t i = 0; i < cands->count; i++) {
        int c = qry_value_cmp(cands->items[i].ptr, cands->items[i].len, operand, operand_len);
        if (c == -2) continue;
        if (mode == QRY_GT && c > 0) return 1;
        if (mode == QRY_GTE && c >= 0) return 1;
        if (mode == QRY_LT && c < 0) return 1;
        if (mode == QRY_LTE && c <= 0) return 1;
    }
    return 0;
}

/* ---- filter evaluation ----------------------------------------------------- */

static int eval_filter(const uint8_t *doc, size_t doc_len,
                       const uint8_t *filter, size_t filter_len, int *out_match);

/* ---- $type ----------------------------------------------------------------- */

/* MongoDB-familiar string aliases, not BSON's numeric type codes (this
 * isn't BSON; a string is the natural fit for a JS-facing API). "number"/
 * "bool" each cover two BJ_TYPE_* tags, matching how JS treats every
 * number/boolean uniformly elsewhere in this codebase. */
typedef enum {
    TA_STRING, TA_NUMBER, TA_INT, TA_DOUBLE, TA_BOOL,
    TA_DATE, TA_OID, TA_ARRAY, TA_OBJECT, TA_NULL
} type_alias;

static int resolve_type_alias(const uint8_t *alias, uint32_t alen, type_alias *out) {
    if (alen == 6 && !memcmp(alias, "string", 6)) { *out = TA_STRING; return BJ_OK; }
    if (alen == 6 && !memcmp(alias, "number", 6)) { *out = TA_NUMBER; return BJ_OK; }
    if (alen == 3 && !memcmp(alias, "int", 3)) { *out = TA_INT; return BJ_OK; }
    if (alen == 6 && !memcmp(alias, "double", 6)) { *out = TA_DOUBLE; return BJ_OK; }
    if (alen == 4 && !memcmp(alias, "bool", 4)) { *out = TA_BOOL; return BJ_OK; }
    if (alen == 4 && !memcmp(alias, "date", 4)) { *out = TA_DATE; return BJ_OK; }
    if (alen == 8 && !memcmp(alias, "objectId", 8)) { *out = TA_OID; return BJ_OK; }
    if (alen == 5 && !memcmp(alias, "array", 5)) { *out = TA_ARRAY; return BJ_OK; }
    if (alen == 6 && !memcmp(alias, "object", 6)) { *out = TA_OBJECT; return BJ_OK; }
    if (alen == 4 && !memcmp(alias, "null", 4)) { *out = TA_NULL; return BJ_OK; }
    return BJ_ERR_STATE;
}
static int type_alias_matches_tag(type_alias ta, uint8_t tag) {
    switch (ta) {
    case TA_STRING: return tag == BJ_TYPE_STRING;
    case TA_NUMBER: return tag == BJ_TYPE_INT || tag == BJ_TYPE_FLOAT;
    case TA_INT:    return tag == BJ_TYPE_INT;
    case TA_DOUBLE: return tag == BJ_TYPE_FLOAT;
    case TA_BOOL:   return tag == BJ_TYPE_TRUE || tag == BJ_TYPE_FALSE;
    case TA_DATE:   return tag == BJ_TYPE_DATE;
    case TA_OID:    return tag == BJ_TYPE_OID;
    case TA_ARRAY:  return tag == BJ_TYPE_ARRAY;
    case TA_OBJECT: return tag == BJ_TYPE_OBJECT;
    case TA_NULL:   return tag == BJ_TYPE_NULL;
    }
    return 0;
}

/* ---- $regex/$options -------------------------------------------------------- */

/* $options is a modifier paired with $regex in the *same* operator-
 * expression object, not a standalone operator -- pre-scanned once so
 * $regex's handler (reached in per-key order below) already has the flags
 * regardless of which key came first. */
static int find_options(const uint8_t *expr, size_t expr_len,
                        const uint8_t **out_opts, uint32_t *out_opts_len, int *found_options) {
    cur c = { expr, expr_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;
    *out_opts = NULL; *out_opts_len = 0; *found_options = 0;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        if (klen == 8 && memcmp(kp, "$options", 8) == 0) {
            if (c.pos - vstart < 1 || c.d[vstart] != BJ_TYPE_STRING) return BJ_ERR_STATE;
            cur oc = { c.d + vstart, c.pos - vstart, 0 };
            e = take_string(&oc, out_opts, out_opts_len);
            if (e) return e;
            *found_options = 1;
        }
    }
    return BJ_OK;
}

/* Evaluate one operator-expression object (all keys $-prefixed) against a
 * field resolved to `found`/`cands`/`raw_vp`/`raw_vl`. `path`/`path_len` is
 * only needed for $not's recursive re-resolution (it re-evaluates the same
 * field). `raw_vp`/`raw_vl` (the field's own resolved span, NULL/0 if
 * `!found`) are only needed by $size/$elemMatch, which must distinguish
 * "the array itself" from `cands`' flattened whole-value-plus-elements
 * list every other operator uses for its any-element matching. */
static int eval_operator_expr(const uint8_t *doc, size_t doc_len,
                              const uint8_t *path, uint32_t path_len,
                              int found, const val_list *cands,
                              const uint8_t *raw_vp, size_t raw_vl,
                              const uint8_t *expr, size_t expr_len, int *out_match) {
    const uint8_t *opts = NULL; uint32_t opts_len = 0; int found_options = 0;
    int e = find_options(expr, expr_len, &opts, &opts_len, &found_options);
    if (e) return e;
    int saw_regex = 0;

    cur c = { expr, expr_len, 0 };
    uint32_t count;
    e = object_begin(&c, &count);
    if (e) return e;

    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        const uint8_t *operand = c.d + vstart;
        size_t operand_len = c.pos - vstart;

        int m;
        if (klen == 3 && memcmp(kp, "$eq", 3) == 0) {
            m = found && op_eq(cands, operand, operand_len);
        } else if (klen == 3 && memcmp(kp, "$ne", 3) == 0) {
            m = !(found && op_eq(cands, operand, operand_len));
        } else if (klen == 3 && memcmp(kp, "$gt", 3) == 0) {
            m = found && op_compare(cands, operand, operand_len, QRY_GT);
        } else if (klen == 4 && memcmp(kp, "$gte", 4) == 0) {
            m = found && op_compare(cands, operand, operand_len, QRY_GTE);
        } else if (klen == 3 && memcmp(kp, "$lt", 3) == 0) {
            m = found && op_compare(cands, operand, operand_len, QRY_LT);
        } else if (klen == 4 && memcmp(kp, "$lte", 4) == 0) {
            m = found && op_compare(cands, operand, operand_len, QRY_LTE);
        } else if (klen == 3 && memcmp(kp, "$in", 3) == 0) {
            int r = 0;
            e = found ? op_in(cands, operand, operand_len, &r) : BJ_OK;
            if (e) return e;
            m = found && r;
        } else if (klen == 4 && memcmp(kp, "$nin", 4) == 0) {
            int r = 0;
            e = found ? op_in(cands, operand, operand_len, &r) : BJ_OK;
            if (e) return e;
            m = !(found && r);
        } else if (klen == 7 && memcmp(kp, "$exists", 7) == 0) {
            int want;
            if (operand_len == 1 && operand[0] == BJ_TYPE_TRUE) want = 1;
            else if (operand_len == 1 && operand[0] == BJ_TYPE_FALSE) want = 0;
            else return BJ_ERR_STATE;
            m = (found == want);
        } else if (klen == 4 && memcmp(kp, "$not", 4) == 0) {
            int inner_is_expr = 0;
            e = qry_is_operator_expr(operand, operand_len, &inner_is_expr);
            if (e) return e;
            if (!inner_is_expr) return BJ_ERR_STATE; /* $not requires an operator expression */
            int inner_match = 0;
            e = eval_operator_expr(doc, doc_len, path, path_len, found, cands, raw_vp, raw_vl, operand, operand_len, &inner_match);
            if (e) return e;
            m = !inner_match;
        } else if (klen == 5 && memcmp(kp, "$size", 5) == 0) {
            cur oc = { operand, operand_len, 0 };
            double want;
            if (read_number(&oc, &want) || want < 0 || want != (double)(int64_t)want) return BJ_ERR_STATE;
            m = 0;
            if (found && raw_vl >= 1 && raw_vp[0] == BJ_TYPE_ARRAY) {
                cur ac = { raw_vp, raw_vl, 0 };
                uint32_t acount;
                e = array_begin(&ac, &acount);
                if (e) return e;
                m = (acount == (uint32_t)(int64_t)want);
            }
        } else if (klen == 4 && memcmp(kp, "$all", 4) == 0) {
            cur oc = { operand, operand_len, 0 };
            uint32_t ocount;
            e = array_begin(&oc, &ocount);
            if (e) return e;
            m = found && ocount > 0;
            for (uint32_t oi = 0; m && oi < ocount; oi++) {
                size_t ostart = oc.pos;
                e = skip_value(&oc);
                if (e) return e;
                if (!op_eq(cands, oc.d + ostart, oc.pos - ostart)) m = 0;
            }
        } else if (klen == 5 && memcmp(kp, "$type", 5) == 0) {
            if (operand_len < 1 || operand[0] != BJ_TYPE_STRING) return BJ_ERR_STATE;
            cur oc = { operand, operand_len, 0 };
            const uint8_t *alias; uint32_t alen;
            e = take_string(&oc, &alias, &alen);
            if (e) return e;
            type_alias ta;
            e = resolve_type_alias(alias, alen, &ta);
            if (e) return e;
            m = 0;
            for (uint32_t ci = 0; found && ci < cands->count; ci++) {
                if (cands->items[ci].len >= 1 && type_alias_matches_tag(ta, cands->items[ci].ptr[0])) { m = 1; break; }
            }
        } else if (klen == 4 && memcmp(kp, "$mod", 4) == 0) {
            cur oc = { operand, operand_len, 0 };
            uint32_t ocount;
            e = array_begin(&oc, &ocount);
            if (e) return e;
            if (ocount != 2) return BJ_ERR_STATE;
            size_t s0 = oc.pos; e = skip_value(&oc); if (e) return e;
            size_t s1 = oc.pos; e = skip_value(&oc); if (e) return e;
            cur dvc = { oc.d + s0, s1 - s0, 0 }; double divisor;
            cur rvc = { oc.d + s1, oc.pos - s1, 0 }; double remainder;
            if (read_number(&dvc, &divisor) || read_number(&rvc, &remainder)) return BJ_ERR_STATE;
            int64_t idiv = (int64_t)divisor, irem = (int64_t)remainder;
            if (idiv == 0) return BJ_ERR_STATE;
            m = 0;
            for (uint32_t ci = 0; found && ci < cands->count; ci++) {
                const uint8_t *cp = cands->items[ci].ptr; size_t cl = cands->items[ci].len;
                if (cl < 1 || (cp[0] != BJ_TYPE_INT && cp[0] != BJ_TYPE_FLOAT)) continue;
                cur cc = { cp, cl, 0 }; double v;
                if (read_number(&cc, &v)) continue;
                if ((int64_t)v % idiv == irem) { m = 1; break; }
            }
        } else if (klen == 10 && memcmp(kp, "$elemMatch", 10) == 0) {
            if (operand_len < 1 || operand[0] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;
            int sub_is_expr = 0;
            e = qry_is_operator_expr(operand, operand_len, &sub_is_expr);
            if (e) return e;
            m = 0;
            if (found && raw_vl >= 1 && raw_vp[0] == BJ_TYPE_ARRAY) {
                cur ac = { raw_vp, raw_vl, 0 };
                uint32_t acount;
                e = array_begin(&ac, &acount);
                if (e) return e;
                for (uint32_t ei = 0; !m && ei < acount; ei++) {
                    size_t estart = ac.pos;
                    e = skip_value(&ac);
                    if (e) return e;
                    const uint8_t *elem = ac.d + estart; size_t elen = ac.pos - estart;
                    if (sub_is_expr) {
                        val_list ecands; memset(&ecands, 0, sizeof(ecands));
                        e = expand_candidates(elem, elen, &ecands);
                        if (e) { val_list_free(&ecands); return e; }
                        int em = 0;
                        e = eval_operator_expr(doc, doc_len, path, path_len, 1, &ecands, elem, elen, operand, operand_len, &em);
                        val_list_free(&ecands);
                        if (e) return e;
                        if (em) m = 1;
                    } else if (elen >= 1 && elem[0] == BJ_TYPE_OBJECT) {
                        int em = 0;
                        e = eval_filter(elem, elen, operand, operand_len, &em);
                        if (e) return e;
                        if (em) m = 1;
                    }
                }
            }
        } else if (klen == 6 && memcmp(kp, "$regex", 6) == 0) {
            if (operand_len < 1 || operand[0] != BJ_TYPE_STRING) return BJ_ERR_STATE;
            cur oc = { operand, operand_len, 0 };
            const uint8_t *pat; uint32_t patlen;
            e = take_string(&oc, &pat, &patlen);
            if (e) return e;
            int ignorecase = 0;
            for (uint32_t fi = 0; fi < opts_len; fi++) {
                if (opts[fi] == 'i') ignorecase = 1;
                else return BJ_ERR_STATE; /* unsupported flag: hard error, not silently ignored */
            }
            m = 0;
            for (uint32_t ci = 0; found && ci < cands->count; ci++) {
                if (cands->items[ci].len < 1 || cands->items[ci].ptr[0] != BJ_TYPE_STRING) continue;
                cur sc = { cands->items[ci].ptr, cands->items[ci].len, 0 };
                const uint8_t *s; uint32_t slen;
                e = take_string(&sc, &s, &slen);
                if (e) return e;
                int one = 0;
                e = rx_match((const char *)pat, (int)patlen, ignorecase, (const char *)s, (int)slen, &one);
                if (e) return e;
                if (one) { m = 1; break; }
            }
            saw_regex = 1;
        } else if (klen == 8 && memcmp(kp, "$options", 8) == 0) {
            m = 1; /* neutral: already consumed by the find_options pre-scan */
        } else {
            /* Unrecognized operator: fail loudly rather than silently
             * matching everything -- see db_query.h. */
            return BJ_ERR_STATE;
        }

        if (!m) { *out_match = 0; return BJ_OK; }
    }
    if (found_options && !saw_regex) return BJ_ERR_STATE; /* $options requires a sibling $regex */
    *out_match = 1;
    return BJ_OK;
}

/* Evaluate operator-expression `expr` against a single resolved value (not
 * a field path) -- exposed for db_update.c's $pull query-condition support.
 * Reuses the same expand_candidates + eval_operator_expr machinery
 * $elemMatch's operator-expression branch already uses internally. */
int qry_value_matches_expr(const uint8_t *value, size_t value_len,
                           const uint8_t *expr, size_t expr_len, int *out_match) {
    val_list cands; memset(&cands, 0, sizeof(cands));
    int e = expand_candidates(value, value_len, &cands);
    if (e) { val_list_free(&cands); return e; }
    e = eval_operator_expr(NULL, 0, NULL, 0, 1, &cands, value, value_len, expr, expr_len, out_match);
    val_list_free(&cands);
    return e;
}

static int eval_field_condition(const uint8_t *doc, size_t doc_len,
                                const uint8_t *path, uint32_t path_len,
                                const uint8_t *cond, size_t cond_len, int *out_match) {
    const uint8_t *vp = NULL; size_t vl = 0; int found = 0;
    int e = qry_resolve_path(doc, doc_len, path, path_len, &vp, &vl, &found);
    if (e) return e;

    val_list cands; memset(&cands, 0, sizeof(cands));
    if (found) {
        e = expand_candidates(vp, vl, &cands);
        if (e) { val_list_free(&cands); return e; }
    }

    int is_expr = 0;
    e = qry_is_operator_expr(cond, cond_len, &is_expr);
    if (e) { val_list_free(&cands); return e; }

    if (!is_expr) {
        *out_match = found && op_eq(&cands, cond, cond_len);
        val_list_free(&cands);
        return BJ_OK;
    }

    e = eval_operator_expr(doc, doc_len, path, path_len, found, &cands, vp, vl, cond, cond_len, out_match);
    val_list_free(&cands);
    return e;
}

static int eval_logical_array(const uint8_t *doc, size_t doc_len,
                              const uint8_t *arr, size_t arr_len,
                              int require_all, int negate, int *out_match) {
    cur c = { arr, arr_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    if (count == 0) return BJ_ERR_STATE; /* $and/$or/$nor require a non-empty array */

    for (uint32_t i = 0; i < count; i++) {
        if (c.d[c.pos] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        int m = 0;
        e = eval_filter(doc, doc_len, c.d + vstart, c.pos - vstart, &m);
        if (e) return e;
        if (require_all && !m) { *out_match = negate; return BJ_OK; }
        if (!require_all && m) { *out_match = !negate; return BJ_OK; }
    }
    *out_match = require_all ? !negate : negate;
    return BJ_OK;
}

static int eval_filter(const uint8_t *doc, size_t doc_len,
                       const uint8_t *filter, size_t filter_len, int *out_match) {
    cur c = { filter, filter_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;

    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        const uint8_t *val = c.d + vstart;
        size_t val_len = c.pos - vstart;

        int m;
        if (klen == 4 && memcmp(kp, "$and", 4) == 0) {
            e = eval_logical_array(doc, doc_len, val, val_len, 1, 0, &m);
        } else if (klen == 3 && memcmp(kp, "$or", 3) == 0) {
            e = eval_logical_array(doc, doc_len, val, val_len, 0, 0, &m);
        } else if (klen == 4 && memcmp(kp, "$nor", 4) == 0) {
            e = eval_logical_array(doc, doc_len, val, val_len, 0, 1, &m);
        } else if (klen > 0 && kp[0] == '$') {
            return BJ_ERR_STATE; /* unrecognized top-level operator */
        } else {
            e = eval_field_condition(doc, doc_len, kp, klen, val, val_len, &m);
        }
        if (e) return e;
        if (!m) { *out_match = 0; return BJ_OK; }
    }
    *out_match = 1;
    return BJ_OK;
}

int qry_matches(const uint8_t *doc, size_t doc_len,
                const uint8_t *filter, size_t filter_len, int *out_matches) {
    return eval_filter(doc, doc_len, filter, filter_len, out_matches);
}

/* ---- sort ------------------------------------------------------------------ */

static int validate_sort_spec(const uint8_t *sort, size_t sort_len) {
    cur c = { sort, sort_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;
    if (count == 0) return BJ_ERR_STATE;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        cur vc = { c.d + vstart, c.pos - vstart, 0 };
        double d;
        e = read_number(&vc, &d);
        if (e || (d != 1.0 && d != -1.0)) return BJ_ERR_STATE;
    }
    return BJ_OK;
}

int qry_validate_options(const qry_options *opts) {
    if (!opts || !opts->sort || opts->sort_len == 0) return BJ_OK;
    return validate_sort_spec(opts->sort, opts->sort_len);
}

/* -1/0/1, per an already-validated sort spec. A field resolving for one
 * document but not the other sorts the resolving one after the missing one
 * in ascending order (missing sorts first); two present but mutually
 * incomparable values (e.g. number vs string) count as equal for that
 * field, falling through to the next sort key. */
static int compare_by_sort(const uint8_t *a, size_t alen, const uint8_t *b, size_t blen,
                           const uint8_t *sort, size_t sort_len) {
    cur c = { sort, sort_len, 0 };
    uint32_t count;
    /* validate_sort_spec ran before any comparison did, so none of these
     * can fail here. Checked anyway because a comparator has no error
     * channel to report it through, and comparing on an uninitialised
     * count or direction would answer the question wrongly rather than
     * not answer it: two documents nothing could be said about are equal,
     * which leaves the caller's order as it found it. */
    if (object_begin(&c, &count)) return 0;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        if (take_key(&c, &kp, &klen)) return 0;
        size_t vstart = c.pos;
        if (skip_value(&c)) return 0;
        cur vc = { c.d + vstart, c.pos - vstart, 0 };
        double dir = 1;
        if (read_number(&vc, &dir)) return 0;

        const uint8_t *av = NULL; size_t al = 0; int af = 0;
        const uint8_t *bv = NULL; size_t bl = 0; int bf = 0;
        qry_resolve_path(a, alen, kp, klen, &av, &al, &af);
        qry_resolve_path(b, blen, kp, klen, &bv, &bl, &bf);

        int cmp;
        if (!af && !bf) cmp = 0;
        else if (!af) cmp = -1;
        else if (!bf) cmp = 1;
        else {
            int vc2 = qry_value_cmp(av, al, bv, bl);
            cmp = (vc2 == -2) ? 0 : vc2;
        }
        if (cmp != 0) return dir < 0 ? -cmp : cmp;
    }
    return 0;
}

/* A qry_doc plus its position in the original (stable, pre-sort) order, so
 * ties break by original order -- a portable substitute for a stable sort
 * without relying on any particular sort implementation being stable. */
typedef struct { qry_doc doc; size_t orig_index; } sortable;

static int sortable_less(const sortable *a, const sortable *b,
                         const uint8_t *sort, size_t sort_len) {
    int c = compare_by_sort(a->doc.ptr, a->doc.len, b->doc.ptr, b->doc.len, sort, sort_len);
    if (c != 0) return c < 0;
    return a->orig_index < b->orig_index;
}

/* Portable (no qsort_r) bottom-up merge sort. */
static void merge_sort(sortable *arr, sortable *scratch, size_t n,
                       const uint8_t *sort, size_t sort_len) {
    for (size_t width = 1; width < n; width *= 2) {
        for (size_t lo = 0; lo < n; lo += 2 * width) {
            size_t mid = lo + width < n ? lo + width : n;
            size_t hi = lo + 2 * width < n ? lo + 2 * width : n;
            size_t i = lo, j = mid, k = lo;
            while (i < mid && j < hi) {
                scratch[k++] = sortable_less(&arr[i], &arr[j], sort, sort_len) ? arr[i++] : arr[j++];
            }
            while (i < mid) scratch[k++] = arr[i++];
            while (j < hi) scratch[k++] = arr[j++];
        }
        memcpy(arr, scratch, n * sizeof(sortable));
    }
}

/* ---- projection -------------------------------------------------------------- */

#define QRY_MAX_PROJECTED_FIELDS 128

typedef struct { const uint8_t *name; uint32_t name_len; } proj_field;

static int parse_projection(const uint8_t *proj, size_t proj_len,
                            proj_field *fields, uint32_t max_fields, uint32_t *out_count,
                            int *mode, int *id_included) {
    cur c = { proj, proj_len, 0 };
    uint32_t count;
    int e = object_begin(&c, &count);
    if (e) return e;

    *id_included = 1;
    int mode_set = 0;
    uint32_t n = 0;
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;

        int inc;
        uint8_t t = c.d[vstart];
        if (t == BJ_TYPE_TRUE) inc = 1;
        else if (t == BJ_TYPE_FALSE) inc = 0;
        else if (t == BJ_TYPE_INT || t == BJ_TYPE_FLOAT) {
            cur vc = { c.d + vstart, c.pos - vstart, 0 };
            double d;
            e = read_number(&vc, &d);
            if (e) return e;
            inc = (d != 0);
        } else return BJ_ERR_STATE;

        if (klen == 3 && memcmp(kp, "_id", 3) == 0) {
            *id_included = inc;
            continue;
        }
        if (!mode_set) { *mode = inc; mode_set = 1; }
        else if (*mode != inc) return BJ_ERR_STATE; /* mixed include/exclude */
        if (n >= max_fields) return BJ_ERR_STATE;
        fields[n].name = kp; fields[n].name_len = klen;
        n++;
    }
    if (!mode_set) *mode = 1; /* only _id in spec, or an empty spec -> inclusion */
    *out_count = n;
    return BJ_OK;
}

int qry_project_one(const uint8_t *doc, size_t doc_len,
                    const uint8_t *proj, size_t proj_len,
                    uint8_t **out, size_t *out_len) {
    proj_field fields[QRY_MAX_PROJECTED_FIELDS];
    uint32_t nfields = 0; int mode = 1; int id_included = 1;
    int e = parse_projection(proj, proj_len, fields, QRY_MAX_PROJECTED_FIELDS, &nfields, &mode, &id_included);
    if (e) return e;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_object(b);

    cur c = { doc, doc_len, 0 };
    uint32_t count;
    if (!e) e = object_begin(&c, &count);
    for (uint32_t i = 0; !e && i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) break;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) break;

        int keep;
        if (klen == 3 && memcmp(kp, "_id", 3) == 0) {
            keep = id_included;
        } else {
            int listed = 0;
            for (uint32_t j = 0; j < nfields; j++) {
                if (fields[j].name_len == klen && memcmp(fields[j].name, kp, klen) == 0) { listed = 1; break; }
            }
            keep = mode ? listed : !listed;
        }
        if (keep) {
            e = bj_put_key(b, kp, klen);
            if (!e) e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
        }
    }
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

/* ---- collect ------------------------------------------------------------------ */

int qry_collect(const qry_doc *docs, size_t count, const qry_options *opts,
                uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;

    size_t start = 0, end = count;

    sortable *order = NULL;
    if (opts && opts->sort && opts->sort_len) {
        order = (sortable *)malloc(count ? count * sizeof(sortable) : 1);
        sortable *scratch = (sortable *)malloc(count ? count * sizeof(sortable) : 1);
        if (!order || !scratch) { free(order); free(scratch); return BJ_ERR_OOM; }
        for (size_t i = 0; i < count; i++) { order[i].doc = docs[i]; order[i].orig_index = i; }
        merge_sort(order, scratch, count, opts->sort, opts->sort_len);
        free(scratch);
    }

    int64_t skip = opts ? opts->skip : 0;
    int64_t limit = opts ? opts->limit : 0;
    if (skip > 0) start = (size_t)skip < count ? (size_t)skip : count;
    if (limit > 0) {
        size_t lim_end = start + (size_t)limit;
        if (lim_end < end) end = lim_end;
    }

    bj_builder *b = bj_builder_new();
    if (!b) { free(order); return BJ_ERR_OOM; }
    int e = bj_begin_array(b);
    for (size_t i = start; !e && i < end; i++) {
        qry_doc d = order ? order[i].doc : docs[i];
        if (opts && opts->projection && opts->projection_len) {
            uint8_t *pj = NULL; size_t pjlen = 0;
            e = qry_project_one(d.ptr, d.len, opts->projection, opts->projection_len, &pj, &pjlen);
            if (!e) e = bj_put_raw(b, pj, (uint32_t)pjlen);
            free(pj);
        } else {
            e = bj_put_raw(b, d.ptr, (uint32_t)d.len);
        }
    }
    free(order);
    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}
