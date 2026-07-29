/*
 * db_update.c — see db_update.h.
 */
#include "db_update.h"
#include "db_query.h"
#include "bjcursor.h"
#include "dbuf.h"

#include <stdlib.h>
#include <string.h>

#define UPD_MAX_FIELDS 128

typedef enum {
    UPD_SET, UPD_UNSET, UPD_INC, UPD_MUL, UPD_MIN, UPD_MAX,
    UPD_RENAME, UPD_SET_ON_INSERT, UPD_ADD_TO_SET,
    UPD_PUSH, UPD_PULL, UPD_PULL_ALL, UPD_POP, UPD_BIT
} upd_kind;

typedef struct {
    const uint8_t *name; uint32_t name_len;
    upd_kind kind;
    const uint8_t *operand; size_t operand_len;
} pending_op;

/* True iff `operand` is a BJ_TYPE_OBJECT whose keys are exactly "and"/
 * "or"/"xor", each an INT value -- $bit's operand shape. */
static int validate_bit_operand(const uint8_t *operand, size_t operand_len) {
    if (operand_len < 1 || operand[0] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;
    cur c = { operand, operand_len, 0 };
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
        int recognized = (klen == 3 && memcmp(kp, "and", 3) == 0)
                       || (klen == 2 && memcmp(kp, "or", 2) == 0)
                       || (klen == 3 && memcmp(kp, "xor", 3) == 0);
        if (!recognized) return BJ_ERR_STATE;
        if (c.pos - vstart < 1 || c.d[vstart] != BJ_TYPE_INT) return BJ_ERR_STATE;
    }
    return BJ_OK;
}

/* Parse `update`'s operators into a flat (field, kind, operand) list. */
static int parse_update(const uint8_t *update, size_t update_len,
                        pending_op *ops, uint32_t max_ops, uint32_t *out_count) {
    cur c = { update, update_len, 0 };
    uint32_t opcount;
    int e = object_begin(&c, &opcount);
    if (e) return e;
    if (opcount == 0) return BJ_ERR_STATE;

    uint32_t n = 0;
    for (uint32_t i = 0; i < opcount; i++) {
        const uint8_t *kp; uint32_t klen;
        e = take_key(&c, &kp, &klen);
        if (e) return e;
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;

        upd_kind kind;
        if (klen == 4 && memcmp(kp, "$set", 4) == 0) kind = UPD_SET;
        else if (klen == 6 && memcmp(kp, "$unset", 6) == 0) kind = UPD_UNSET;
        else if (klen == 4 && memcmp(kp, "$inc", 4) == 0) kind = UPD_INC;
        else if (klen == 4 && memcmp(kp, "$mul", 4) == 0) kind = UPD_MUL;
        else if (klen == 4 && memcmp(kp, "$min", 4) == 0) kind = UPD_MIN;
        else if (klen == 4 && memcmp(kp, "$max", 4) == 0) kind = UPD_MAX;
        else if (klen == 7 && memcmp(kp, "$rename", 7) == 0) kind = UPD_RENAME;
        else if (klen == 12 && memcmp(kp, "$setOnInsert", 12) == 0) kind = UPD_SET_ON_INSERT;
        else if (klen == 9 && memcmp(kp, "$addToSet", 9) == 0) kind = UPD_ADD_TO_SET;
        else if (klen == 5 && memcmp(kp, "$push", 5) == 0) kind = UPD_PUSH;
        else if (klen == 5 && memcmp(kp, "$pull", 5) == 0) kind = UPD_PULL;
        else if (klen == 8 && memcmp(kp, "$pullAll", 8) == 0) kind = UPD_PULL_ALL;
        else if (klen == 4 && memcmp(kp, "$pop", 4) == 0) kind = UPD_POP;
        else if (klen == 4 && memcmp(kp, "$bit", 4) == 0) kind = UPD_BIT;
        else return BJ_ERR_STATE; /* unrecognized operator, or a plain field
                                    (a replacement document) -- use
                                    replaceOne for a full replacement. */

        const uint8_t *opval = c.d + vstart;
        size_t opval_len = c.pos - vstart;
        if (opval_len < 1 || opval[0] != BJ_TYPE_OBJECT) return BJ_ERR_STATE;

        cur fc = { opval, opval_len, 0 };
        uint32_t fcount;
        e = object_begin(&fc, &fcount);
        if (e) return e;
        for (uint32_t j = 0; j < fcount; j++) {
            const uint8_t *fkp; uint32_t fklen;
            e = take_key(&fc, &fkp, &fklen);
            if (e) return e;
            size_t fvstart = fc.pos;
            e = skip_value(&fc);
            if (e) return e;

            if (fklen == 3 && memcmp(fkp, "_id", 3) == 0) return BJ_ERR_STATE;
            for (uint32_t k = 0; k < fklen; k++) {
                /* No dotted paths yet -- reject rather than treat "a.b" as
                 * a literal top-level field name (see db_update.h). */
                if (fkp[k] == '.') return BJ_ERR_STATE;
            }
            for (uint32_t k = 0; k < n; k++) {
                if (ops[k].name_len == fklen && memcmp(ops[k].name, fkp, fklen) == 0) {
                    return BJ_ERR_STATE; /* field targeted by more than one operator */
                }
            }
            if (n >= max_ops) return BJ_ERR_STATE;

            const uint8_t *fopnd = fc.d + fvstart;
            size_t fopnd_len = fc.pos - fvstart;
            if (kind == UPD_RENAME) {
                if (fopnd_len < 1 || fopnd[0] != BJ_TYPE_STRING) return BJ_ERR_STATE;
            } else if (kind == UPD_POP) {
                cur pc = { fopnd, fopnd_len, 0 };
                double pv;
                if (read_number(&pc, &pv) || (pv != 1.0 && pv != -1.0)) return BJ_ERR_STATE;
            } else if (kind == UPD_PULL_ALL) {
                if (fopnd_len < 1 || fopnd[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
            } else if (kind == UPD_BIT) {
                e = validate_bit_operand(fopnd, fopnd_len);
                if (e) return e;
            }

            ops[n].name = fkp; ops[n].name_len = fklen;
            ops[n].kind = kind;
            ops[n].operand = fopnd; ops[n].operand_len = fopnd_len;
            n++;
        }
    }

    /* $rename's destination name: not dotted/_id, and doesn't collide
     * with any other operator's target field name or another rename's
     * own destination (old == new is allowed -- a harmless identity
     * rename). Collision with a pre-existing document field is not
     * checked here -- that's resolved dynamically at apply time (the
     * destination is simply overwritten, matching real MongoDB). */
    for (uint32_t k = 0; k < n; k++) {
        if (ops[k].kind != UPD_RENAME) continue;
        cur dc = { ops[k].operand, ops[k].operand_len, 0 };
        const uint8_t *dest; uint32_t destlen;
        e = take_string(&dc, &dest, &destlen);
        if (e) return e;
        if (destlen == 0) return BJ_ERR_STATE;
        if (destlen == 3 && memcmp(dest, "_id", 3) == 0) return BJ_ERR_STATE;
        for (uint32_t di = 0; di < destlen; di++) {
            if (dest[di] == '.') return BJ_ERR_STATE;
        }
        for (uint32_t j = 0; j < n; j++) {
            if (j == k) continue;
            if (ops[j].name_len == destlen && memcmp(ops[j].name, dest, destlen) == 0) return BJ_ERR_STATE;
            if (ops[j].kind == UPD_RENAME) {
                cur dc2 = { ops[j].operand, ops[j].operand_len, 0 };
                const uint8_t *dest2; uint32_t dest2len;
                e = take_string(&dc2, &dest2, &dest2len);
                if (e) return e;
                if (dest2len == destlen && memcmp(dest2, dest, destlen) == 0) return BJ_ERR_STATE;
            }
        }
    }

    *out_count = n;
    return BJ_OK;
}

int upd_validate(const uint8_t *update, size_t update_len) {
    pending_op ops[UPD_MAX_FIELDS];
    uint32_t n = 0;
    return parse_update(update, update_len, ops, UPD_MAX_FIELDS, &n);
}

/* Byte-equality scan over a val_list -- a local helper since db_query.c's
 * own op_eq is static there; value_eq itself is exposed via db_query.h. */
static int list_has_eq(const val_list *l, const uint8_t *v, size_t vlen) {
    for (uint32_t i = 0; i < l->count; i++) {
        if (value_eq(l->items[i].ptr, l->items[i].len, v, vlen)) return 1;
    }
    return 0;
}

/* cur_val/cur_val_len: NULL/0 if the field doesn't exist yet (treated as
 * 0). BJ_ERR_STATE if a present cur_val or the operand isn't a number. */
static int emit_incremented(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                            const uint8_t *operand, size_t operand_len) {
    double base = 0;
    if (cur_val) {
        if (cur_val_len < 1 || (cur_val[0] != BJ_TYPE_INT && cur_val[0] != BJ_TYPE_FLOAT)) return BJ_ERR_STATE;
        cur c = { cur_val, cur_val_len, 0 };
        int e = read_number(&c, &base);
        if (e) return e;
    }
    if (operand_len < 1 || (operand[0] != BJ_TYPE_INT && operand[0] != BJ_TYPE_FLOAT)) return BJ_ERR_STATE;
    cur oc = { operand, operand_len, 0 };
    double delta;
    int e = read_number(&oc, &delta);
    if (e) return e;

    double sum = base + delta;
    if (is_safe_int(sum)) return bj_put_int(b, (int64_t)sum);
    return bj_put_float(b, sum);
}

/* Like emit_incremented, but multiplies; a missing field seeds at base 0
 * (matches real MongoDB: multiplying by the implicit base 0 always gives
 * 0 for a newly-created field). */
static int emit_multiplied(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                           const uint8_t *operand, size_t operand_len) {
    double base = 0;
    if (cur_val) {
        if (cur_val_len < 1 || (cur_val[0] != BJ_TYPE_INT && cur_val[0] != BJ_TYPE_FLOAT)) return BJ_ERR_STATE;
        cur c = { cur_val, cur_val_len, 0 };
        int e = read_number(&c, &base);
        if (e) return e;
    }
    if (operand_len < 1 || (operand[0] != BJ_TYPE_INT && operand[0] != BJ_TYPE_FLOAT)) return BJ_ERR_STATE;
    cur oc = { operand, operand_len, 0 };
    double factor;
    int e = read_number(&oc, &factor);
    if (e) return e;

    double product = base * factor;
    if (is_safe_int(product)) return bj_put_int(b, (int64_t)product);
    return bj_put_float(b, product);
}

/* cur_val/cur_val_len: NULL/0 if the field doesn't exist yet (the operand
 * is seeded directly, no comparison). -2 (incomparable, per
 * qry_value_cmp) is BJ_ERR_STATE -- unlike a filter's "incomparable never
 * matches", a value-producing operator can't silently do nothing. */
static int emit_min_max(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                        const uint8_t *operand, size_t operand_len, int want_min) {
    if (!cur_val) return bj_put_raw(b, operand, (uint32_t)operand_len);
    int c = qry_value_cmp(cur_val, cur_val_len, operand, operand_len);
    if (c == -2) return BJ_ERR_STATE;
    int keep_cur = want_min ? (c <= 0) : (c >= 0);
    if (keep_cur) return bj_put_raw(b, cur_val, (uint32_t)cur_val_len);
    return bj_put_raw(b, operand, (uint32_t)operand_len);
}

/* cur_val/cur_val_len: NULL/0 if the field doesn't exist yet (a fresh
 * single-element array is created). BJ_ERR_STATE if a present cur_val
 * isn't an ARRAY. */
static int emit_pushed(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                       const uint8_t *operand, size_t operand_len);

/* $addToSet's operand is either a plain value (appended if absent) or
 * {$each: [...]} (each element appended if absent, in order, deduped
 * against both existing elements and ones just added earlier in this same
 * call). Only $each is recognized here -- $addToSet has no $slice/$sort/
 * $position modifiers. */
static int addtoset_each(const uint8_t *operand, size_t operand_len,
                         const uint8_t **each_arr, size_t *each_len, int *is_each) {
    *is_each = 0;
    if (operand_len < 1 || operand[0] != BJ_TYPE_OBJECT) return BJ_OK;
    cur c = { operand, operand_len, 0 };
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
        if (klen == 5 && memcmp(kp, "$each", 5) == 0) {
            if (c.pos - vstart < 1 || c.d[vstart] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
            *each_arr = c.d + vstart; *each_len = c.pos - vstart; *is_each = 1;
        }
    }
    if (*is_each && count != 1) return BJ_ERR_STATE; /* only key allowed is $each */
    return BJ_OK;
}

static int emit_added_to_set(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                             const uint8_t *operand, size_t operand_len) {
    const uint8_t *each_arr = NULL; size_t each_len = 0; int is_each = 0;
    int e = addtoset_each(operand, operand_len, &each_arr, &each_len, &is_each);
    if (e) return e;

    val_list existing; memset(&existing, 0, sizeof(existing));
    if (cur_val) {
        if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
        cur c = { cur_val, cur_val_len, 0 };
        uint32_t count;
        e = array_begin(&c, &count);
        if (e) { val_list_free(&existing); return e; }
        for (uint32_t i = 0; i < count; i++) {
            size_t vstart = c.pos;
            e = skip_value(&c);
            if (e) { val_list_free(&existing); return e; }
            e = val_list_push(&existing, c.d + vstart, c.pos - vstart);
            if (e) { val_list_free(&existing); return e; }
        }
    }

    e = bj_begin_array(b);
    for (uint32_t i = 0; !e && i < existing.count; i++) {
        e = bj_put_raw(b, existing.items[i].ptr, (uint32_t)existing.items[i].len);
    }

    val_list added; memset(&added, 0, sizeof(added));
    if (!e) {
        if (is_each) {
            cur ec = { each_arr, each_len, 0 };
            uint32_t ecount;
            e = array_begin(&ec, &ecount);
            for (uint32_t i = 0; !e && i < ecount; i++) {
                size_t vstart = ec.pos;
                e = skip_value(&ec);
                if (e) break;
                const uint8_t *cand = ec.d + vstart; size_t cand_len = ec.pos - vstart;
                if (!list_has_eq(&existing, cand, cand_len) && !list_has_eq(&added, cand, cand_len)) {
                    e = bj_put_raw(b, cand, (uint32_t)cand_len);
                    if (!e) e = val_list_push(&added, cand, cand_len);
                }
            }
        } else if (!list_has_eq(&existing, operand, operand_len)) {
            e = bj_put_raw(b, operand, (uint32_t)operand_len);
        }
    }
    val_list_free(&existing);
    val_list_free(&added);
    if (!e) e = bj_end_array(b);
    return e;
}

typedef struct {
    int has_each; const uint8_t *each_arr; size_t each_len;
    int has_slice; int64_t slice_n;
    int has_sort; int sort_dir;
    int has_position; int64_t position_n;
} push_mods;

/* True (via *is_mod) iff `operand` is the {$each: [...], ...} modifier
 * form; a plain OBJECT with none of $each/$slice/$sort/$position is a
 * literal value to append as a single element (matches real MongoDB's own
 * $each-triggers-modifier-form ambiguity). Any of $slice/$sort/$position
 * without a sibling $each is rejected. */
static int parse_push_mods(const uint8_t *operand, size_t operand_len, push_mods *pm, int *is_mod) {
    memset(pm, 0, sizeof(*pm));
    *is_mod = 0;
    if (operand_len < 1 || operand[0] != BJ_TYPE_OBJECT) return BJ_OK;
    cur c = { operand, operand_len, 0 };
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
        const uint8_t *vp = c.d + vstart; size_t vl = c.pos - vstart;
        if (klen == 5 && memcmp(kp, "$each", 5) == 0) {
            if (vl < 1 || vp[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
            pm->has_each = 1; pm->each_arr = vp; pm->each_len = vl;
        } else if (klen == 6 && memcmp(kp, "$slice", 6) == 0) {
            cur nc = { vp, vl, 0 }; double d;
            if (read_number(&nc, &d) || d != (double)(int64_t)d) return BJ_ERR_STATE;
            pm->has_slice = 1; pm->slice_n = (int64_t)d;
        } else if (klen == 5 && memcmp(kp, "$sort", 5) == 0) {
            cur nc = { vp, vl, 0 }; double d;
            if (read_number(&nc, &d) || (d != 1.0 && d != -1.0)) return BJ_ERR_STATE;
            pm->has_sort = 1; pm->sort_dir = (int)d;
        } else if (klen == 9 && memcmp(kp, "$position", 9) == 0) {
            cur nc = { vp, vl, 0 }; double d;
            if (read_number(&nc, &d) || d != (double)(int64_t)d) return BJ_ERR_STATE;
            pm->has_position = 1; pm->position_n = (int64_t)d;
        } else {
            return BJ_OK; /* an unrecognized key means this is a literal
                             object value to push, not the modifier form */
        }
    }
    if (!pm->has_each) {
        if (pm->has_slice || pm->has_sort || pm->has_position) return BJ_ERR_STATE; /* modifier without $each */
        return BJ_OK;
    }
    *is_mod = 1;
    return BJ_OK;
}

static int emit_pushed(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                       const uint8_t *operand, size_t operand_len) {
    push_mods pm; int is_mod = 0;
    int e = parse_push_mods(operand, operand_len, &pm, &is_mod);
    if (e) return e;

    if (!is_mod) {
        e = bj_begin_array(b);
        if (e) return e;
        if (cur_val) {
            if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
            cur c = { cur_val, cur_val_len, 0 };
            uint32_t count;
            e = array_begin(&c, &count);
            if (e) return e;
            for (uint32_t i = 0; i < count; i++) {
                size_t vstart = c.pos;
                e = skip_value(&c);
                if (e) return e;
                e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
                if (e) return e;
            }
        }
        e = bj_put_raw(b, operand, (uint32_t)operand_len);
        if (e) return e;
        return bj_end_array(b);
    }

    val_list elems; memset(&elems, 0, sizeof(elems));
    if (cur_val) {
        if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
        cur c = { cur_val, cur_val_len, 0 };
        uint32_t count;
        e = array_begin(&c, &count);
        if (e) { val_list_free(&elems); return e; }
        for (uint32_t i = 0; i < count; i++) {
            size_t vstart = c.pos;
            e = skip_value(&c);
            if (e) { val_list_free(&elems); return e; }
            e = val_list_push(&elems, c.d + vstart, c.pos - vstart);
            if (e) { val_list_free(&elems); return e; }
        }
    }

    val_list each; memset(&each, 0, sizeof(each));
    {
        cur ec = { pm.each_arr, pm.each_len, 0 };
        uint32_t ecount;
        e = array_begin(&ec, &ecount);
        for (uint32_t i = 0; !e && i < ecount; i++) {
            size_t vstart = ec.pos;
            e = skip_value(&ec);
            if (e) break;
            e = val_list_push(&each, ec.d + vstart, ec.pos - vstart);
        }
    }
    if (e) { val_list_free(&elems); val_list_free(&each); return e; }

    if (pm.has_sort) {
        /* $position is ignored when $sort is present (matches real
         * MongoDB): append at the end, then sort the whole array. */
        for (uint32_t i = 0; !e && i < each.count; i++) {
            e = val_list_push(&elems, each.items[i].ptr, each.items[i].len);
        }
    } else {
        int64_t at = (int64_t)elems.count;
        if (pm.has_position) {
            at = pm.position_n;
            if (at < 0) at = (int64_t)elems.count + at;
            if (at < 0) at = 0;
            if (at > (int64_t)elems.count) at = (int64_t)elems.count;
        }
        val_list spliced; memset(&spliced, 0, sizeof(spliced));
        for (int64_t i = 0; !e && i < at; i++) {
            e = val_list_push(&spliced, elems.items[i].ptr, elems.items[i].len);
        }
        for (uint32_t i = 0; !e && i < each.count; i++) {
            e = val_list_push(&spliced, each.items[i].ptr, each.items[i].len);
        }
        for (int64_t i = at; !e && i < (int64_t)elems.count; i++) {
            e = val_list_push(&spliced, elems.items[i].ptr, elems.items[i].len);
        }
        val_list_free(&elems);
        elems = spliced;
    }
    val_list_free(&each);
    if (e) { val_list_free(&elems); return e; }

    if (pm.has_sort) {
        /* Small insertion sort -- arrays here are expected to be short.
         * Incomparable pairs count as equal (matches compare_by_sort's
         * own convention in db_query.c), not an error. */
        for (uint32_t i = 1; i < elems.count; i++) {
            val_span key = elems.items[i];
            uint32_t j = i;
            while (j > 0) {
                int cc = qry_value_cmp(elems.items[j - 1].ptr, elems.items[j - 1].len, key.ptr, key.len);
                if (cc == -2) cc = 0;
                int out_of_order = pm.sort_dir < 0 ? (cc < 0) : (cc > 0);
                if (!out_of_order) break;
                elems.items[j] = elems.items[j - 1];
                j--;
            }
            elems.items[j] = key;
        }
    }

    uint32_t start = 0, end = elems.count;
    if (pm.has_slice) {
        if (pm.slice_n >= 0) {
            end = (uint32_t)pm.slice_n < elems.count ? (uint32_t)pm.slice_n : elems.count;
        } else {
            uint32_t keep = (uint32_t)(-pm.slice_n);
            start = keep < elems.count ? elems.count - keep : 0;
        }
    }

    e = bj_begin_array(b);
    for (uint32_t i = start; !e && i < end; i++) {
        e = bj_put_raw(b, elems.items[i].ptr, (uint32_t)elems.items[i].len);
    }
    val_list_free(&elems);
    if (!e) e = bj_end_array(b);
    return e;
}

/* cur_val must be a present ARRAY (upd_apply only calls this when the field
 * exists; $pull on a missing field is a no-op handled by the caller).
 * Removes every element byte-equal to `operand`, or, if `operand` is
 * itself an operator expression (e.g. {$gt: 5}), every element matching
 * that expression (reusing db_query.h's matcher). */
static int emit_pulled(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                       const uint8_t *operand, size_t operand_len) {
    if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
    int is_expr = 0;
    int e = qry_is_operator_expr(operand, operand_len, &is_expr);
    if (e) return e;

    cur c = { cur_val, cur_val_len, 0 };
    uint32_t count;
    e = array_begin(&c, &count);
    if (e) return e;
    e = bj_begin_array(b);
    if (e) return e;
    for (uint32_t i = 0; i < count; i++) {
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        size_t vlen = c.pos - vstart;
        int drop;
        if (is_expr) {
            int m = 0;
            e = qry_value_matches_expr(c.d + vstart, vlen, operand, operand_len, &m);
            if (e) return e;
            drop = m;
        } else {
            drop = (vlen == operand_len && (vlen == 0 || memcmp(c.d + vstart, operand, vlen) == 0));
        }
        if (drop) continue;
        e = bj_put_raw(b, c.d + vstart, (uint32_t)vlen);
        if (e) return e;
    }
    return bj_end_array(b);
}

/* cur_val must be a present ARRAY. Removes every element byte-equal to
 * *any* of `operand`'s (ARRAY) values -- no operator-expression form,
 * unlike $pull. */
static int emit_pulled_all(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                           const uint8_t *operand, size_t operand_len) {
    if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
    cur c = { cur_val, cur_val_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    e = bj_begin_array(b);
    if (e) return e;
    for (uint32_t i = 0; i < count; i++) {
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        size_t vlen = c.pos - vstart;

        cur oc = { operand, operand_len, 0 };
        uint32_t ocount;
        e = array_begin(&oc, &ocount);
        if (e) return e;
        int drop = 0;
        for (uint32_t oi = 0; !drop && oi < ocount; oi++) {
            size_t ostart = oc.pos;
            e = skip_value(&oc);
            if (e) return e;
            size_t olen = oc.pos - ostart;
            if (olen == vlen && (vlen == 0 || memcmp(oc.d + ostart, c.d + vstart, vlen) == 0)) drop = 1;
        }
        if (drop) continue;
        e = bj_put_raw(b, c.d + vstart, (uint32_t)vlen);
        if (e) return e;
    }
    return bj_end_array(b);
}

/* cur_val must be a present ARRAY. Drops the last (want_last) or first
 * element; a no-op on an already-empty array. */
static int emit_popped(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len, int want_last) {
    if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_ARRAY) return BJ_ERR_STATE;
    cur c = { cur_val, cur_val_len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    e = bj_begin_array(b);
    if (e) return e;
    if (count == 0) return bj_end_array(b);
    uint32_t skip_index = want_last ? count - 1 : 0;
    for (uint32_t i = 0; i < count; i++) {
        size_t vstart = c.pos;
        e = skip_value(&c);
        if (e) return e;
        if (i == skip_index) continue;
        e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
        if (e) return e;
    }
    return bj_end_array(b);
}

/* cur_val/cur_val_len: NULL/0 if the field doesn't exist yet (base 0).
 * `operand` is a validated {and:N, or:N, xor:N} object (parse_update
 * already checked keys/value types); applies each in encounter order to a
 * running int64_t. BJ_ERR_STATE if a present cur_val isn't an INT (no
 * bitwise ops on FLOAT). */
static int emit_bit(bj_builder *b, const uint8_t *cur_val, size_t cur_val_len,
                    const uint8_t *operand, size_t operand_len) {
    int64_t base = 0;
    if (cur_val) {
        if (cur_val_len < 1 || cur_val[0] != BJ_TYPE_INT) return BJ_ERR_STATE;
        cur c = { cur_val, cur_val_len, 0 };
        double d;
        int e = read_number(&c, &d);
        if (e) return e;
        base = (int64_t)d;
    }
    cur c = { operand, operand_len, 0 };
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
        cur vc = { c.d + vstart, c.pos - vstart, 0 };
        double d;
        e = read_number(&vc, &d);
        if (e) return e;
        int64_t opnd = (int64_t)d;
        if (klen == 3 && memcmp(kp, "and", 3) == 0) base &= opnd;
        else if (klen == 2 && memcmp(kp, "or", 2) == 0) base |= opnd;
        else base ^= opnd; /* "xor", the only remaining option validated in parse_update */
    }
    return bj_put_int(b, base);
}

int upd_apply(const uint8_t *doc, size_t doc_len,
             const uint8_t *update, size_t update_len,
             int is_insert,
             uint8_t **out, size_t *out_len) {
    *out = NULL; *out_len = 0;

    pending_op ops[UPD_MAX_FIELDS];
    uint32_t nops = 0;
    int e = parse_update(update, update_len, ops, UPD_MAX_FIELDS, &nops);
    if (e) return e;

    uint8_t applied[UPD_MAX_FIELDS];
    memset(applied, 0, sizeof(applied));

    /* $rename bookkeeping (indexed by op index, only meaningful for
     * kind == UPD_RENAME): the decoded destination name, whether the
     * source field is present in `doc` at all (determines both
     * suppression of a pre-existing destination field and whether the
     * trailing pass emits anything), and -- once the main scan reaches
     * the source key -- its captured value. */
    const uint8_t *dest_name[UPD_MAX_FIELDS]; uint32_t dest_name_len[UPD_MAX_FIELDS];
    uint8_t rename_active[UPD_MAX_FIELDS];
    const uint8_t *rename_val[UPD_MAX_FIELDS]; size_t rename_val_len[UPD_MAX_FIELDS];
    memset(rename_active, 0, sizeof(rename_active));
    for (uint32_t k = 0; k < nops; k++) {
        if (ops[k].kind != UPD_RENAME) continue;
        cur dc = { ops[k].operand, ops[k].operand_len, 0 };
        e = take_string(&dc, &dest_name[k], &dest_name_len[k]);
        if (e) return e;
        const uint8_t *vp; size_t vl; int found = 0;
        e = obj_get_field(doc, doc_len, ops[k].name, ops[k].name_len, &vp, &vl, &found);
        if (e) return e;
        rename_active[k] = (uint8_t)found;
    }

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
        const uint8_t *cur_val = c.d + vstart;
        size_t cur_val_len = c.pos - vstart;

        int op_index = -1;
        for (uint32_t k = 0; k < nops; k++) {
            if (ops[k].name_len == klen && memcmp(ops[k].name, kp, klen) == 0) { op_index = (int)k; break; }
        }
        if (op_index < 0) {
            /* Not directly targeted -- but suppress it if it's the
             * destination of an active rename (about to be overwritten). */
            int suppressed = 0;
            for (uint32_t k = 0; k < nops; k++) {
                if (ops[k].kind == UPD_RENAME && rename_active[k] &&
                    dest_name_len[k] == klen && memcmp(dest_name[k], kp, klen) == 0) {
                    suppressed = 1; break;
                }
            }
            if (suppressed) continue;
            e = bj_put_key(b, kp, klen);
            if (!e) e = bj_put_raw(b, cur_val, (uint32_t)cur_val_len);
            continue;
        }

        applied[op_index] = 1;
        const pending_op *op = &ops[op_index];
        if (op->kind == UPD_UNSET) continue; /* drop the field */
        if (op->kind == UPD_RENAME) {
            rename_val[op_index] = cur_val; rename_val_len[op_index] = cur_val_len;
            continue; /* emitted under the new name in the trailing pass */
        }
        if (op->kind == UPD_SET_ON_INSERT && !is_insert) {
            /* No-op on a normal matched update: pass the field through. */
            e = bj_put_key(b, kp, klen);
            if (!e) e = bj_put_raw(b, cur_val, (uint32_t)cur_val_len);
            continue;
        }

        e = bj_put_key(b, kp, klen);
        if (e) break;
        switch (op->kind) {
        case UPD_SET:
        case UPD_SET_ON_INSERT:
            e = bj_put_raw(b, op->operand, (uint32_t)op->operand_len);
            break;
        case UPD_INC:
            e = emit_incremented(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        case UPD_MUL:
            e = emit_multiplied(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        case UPD_MIN:
            e = emit_min_max(b, cur_val, cur_val_len, op->operand, op->operand_len, 1);
            break;
        case UPD_MAX:
            e = emit_min_max(b, cur_val, cur_val_len, op->operand, op->operand_len, 0);
            break;
        case UPD_ADD_TO_SET:
            e = emit_added_to_set(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        case UPD_PUSH:
            e = emit_pushed(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        case UPD_PULL:
            e = emit_pulled(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        case UPD_PULL_ALL:
            e = emit_pulled_all(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        case UPD_POP: {
            cur pc = { op->operand, op->operand_len, 0 };
            double pv = 0;
            read_number(&pc, &pv);
            e = emit_popped(b, cur_val, cur_val_len, pv > 0);
            break;
        }
        case UPD_BIT:
            e = emit_bit(b, cur_val, cur_val_len, op->operand, op->operand_len);
            break;
        default:
            e = BJ_ERR_STATE; /* unreachable: UNSET/RENAME handled above */
        }
    }

    /* Fields the update targets that the document doesn't have yet. */
    for (uint32_t k = 0; !e && k < nops; k++) {
        const pending_op *op = &ops[k];
        if (op->kind == UPD_RENAME) {
            if (!rename_active[k]) continue; /* source absent: total no-op */
            e = bj_put_key(b, dest_name[k], dest_name_len[k]);
            if (!e) e = bj_put_raw(b, rename_val[k], (uint32_t)rename_val_len[k]);
            continue;
        }
        if (applied[k]) continue;
        if (op->kind == UPD_UNSET || op->kind == UPD_PULL || op->kind == UPD_PULL_ALL || op->kind == UPD_POP) continue; /* no-op on a missing field */
        if (op->kind == UPD_SET_ON_INSERT && !is_insert) continue; /* never creates the field on a matched update */
        e = bj_put_key(b, op->name, op->name_len);
        if (e) break;
        switch (op->kind) {
        case UPD_SET:
        case UPD_SET_ON_INSERT:
            e = bj_put_raw(b, op->operand, (uint32_t)op->operand_len);
            break;
        case UPD_INC:
            e = emit_incremented(b, NULL, 0, op->operand, op->operand_len);
            break;
        case UPD_MUL:
            e = emit_multiplied(b, NULL, 0, op->operand, op->operand_len);
            break;
        case UPD_MIN:
        case UPD_MAX:
            e = emit_min_max(b, NULL, 0, op->operand, op->operand_len, op->kind == UPD_MIN);
            break;
        case UPD_ADD_TO_SET:
            e = emit_added_to_set(b, NULL, 0, op->operand, op->operand_len);
            break;
        case UPD_PUSH:
            e = emit_pushed(b, NULL, 0, op->operand, op->operand_len);
            break;
        case UPD_BIT:
            e = emit_bit(b, NULL, 0, op->operand, op->operand_len);
            break;
        default:
            e = BJ_ERR_STATE; /* unreachable: filtered out above */
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

/* ---- $currentDate ------------------------------------------------------
 *
 * See db_update.h. The rewrite is here rather than in the host because
 * its rules are this file's rules: a field targeted by $currentDate must
 * not be targeted by anything else, which is the same path-collision
 * invariant parse_update enforces for every other operator.
 */

/* Is `field` targeted by some operator other than $currentDate, or by an
 * existing $set? */
static int cd_targeted_elsewhere(const uint8_t *update, size_t update_len,
                                 const uint8_t *field, uint32_t field_len) {
    cur c = { update, update_len, 0 };
    uint32_t n;
    if (object_begin(&c, &n) != BJ_OK) return 0;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *kp; uint32_t klen;
        if (take_key(&c, &kp, &klen) != BJ_OK) return 0;
        size_t vstart = c.pos;
        if (skip_value(&c) != BJ_OK) return 0;
        if (klen == 12 && memcmp(kp, "$currentDate", 12) == 0) continue;
        if (klen < 1 || kp[0] != '$') continue;

        const uint8_t *ov = update + vstart;
        size_t ov_len = c.pos - vstart;
        if (ov_len < 1 || ov[0] != BJ_TYPE_OBJECT) continue;
        cur oc = { ov, ov_len, 0 };
        uint32_t on;
        if (object_begin(&oc, &on) != BJ_OK) continue;
        for (uint32_t j = 0; j < on; j++) {
            const uint8_t *fk; uint32_t fklen;
            if (take_key(&oc, &fk, &fklen) != BJ_OK) break;
            if (skip_value(&oc) != BJ_OK) break;
            if (fklen == field_len && memcmp(fk, field, field_len) == 0) return 1;
        }
    }
    return 0;
}

/* TRUE, or {$type: "date"}. Anything else is rejected rather than
 * silently treated as a timestamp request. */
static int cd_spec_is_valid(const uint8_t *v, size_t len) {
    if (len >= 1 && v[0] == BJ_TYPE_TRUE) return 1;
    if (len < 1 || v[0] != BJ_TYPE_OBJECT) return 0;
    const uint8_t *tv; size_t tlen; int found = 0;
    if (obj_get_field(v, len, (const uint8_t *)"$type", 5, &tv, &tlen, &found) != BJ_OK) return 0;
    if (!found) return 0;
    cur c = { tv, tlen, 0 };
    const uint8_t *sp; uint32_t slen;
    if (take_string(&c, &sp, &slen) != BJ_OK) return 0;
    return slen == 4 && memcmp(sp, "date", 4) == 0;
}

int upd_resolve_current_date(const uint8_t *update, size_t update_len,
                             int64_t now_ms, dbuf *out) {
    const uint8_t *cd; size_t cd_len; int has_cd = 0;
    int e = obj_get_field(update, update_len, (const uint8_t *)"$currentDate", 12,
                          &cd, &cd_len, &has_cd);
    if (e) return e;
    if (!has_cd) return dbuf_put(out, update, update_len);   /* unchanged */
    if (cd_len < 1 || cd[0] != BJ_TYPE_OBJECT) return DC_ERR_BAD_CURRENT_DATE;

    /* Validate every entry before emitting anything. */
    {
        cur c = { cd, cd_len, 0 };
        uint32_t n;
        if ((e = object_begin(&c, &n))) return e;
        for (uint32_t i = 0; i < n; i++) {
            const uint8_t *fk; uint32_t fklen;
            if ((e = take_key(&c, &fk, &fklen))) return e;
            size_t vstart = c.pos;
            if ((e = skip_value(&c))) return e;
            if (!cd_spec_is_valid(cd + vstart, c.pos - vstart))
                return DC_ERR_BAD_CURRENT_DATE;
            if (cd_targeted_elsewhere(update, update_len, fk, fklen))
                return DC_ERR_CURRENT_DATE_CONFLICT;
        }
    }

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);

    /* Every operator except $currentDate and $set, verbatim. */
    cur c = { update, update_len, 0 };
    uint32_t n;
    if ((e = object_begin(&c, &n))) { bj_builder_free(b); return e; }
    const uint8_t *existing_set = NULL; size_t existing_set_len = 0;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&c, &kp, &klen))) { bj_builder_free(b); return e; }
        size_t vstart = c.pos;
        if ((e = skip_value(&c))) { bj_builder_free(b); return e; }
        if (klen == 12 && memcmp(kp, "$currentDate", 12) == 0) continue;
        if (klen == 4 && memcmp(kp, "$set", 4) == 0) {
            existing_set = update + vstart;
            existing_set_len = c.pos - vstart;
            continue;
        }
        bj_put_key(b, kp, klen);
        bj_put_raw(b, update + vstart, (uint32_t)(c.pos - vstart));
    }

    /* One merged $set: the original entries, then the resolved dates. */
    bj_put_key(b, (const uint8_t *)"$set", 4);
    bj_begin_object(b);
    if (existing_set && existing_set_len >= 1 && existing_set[0] == BJ_TYPE_OBJECT) {
        cur sc = { existing_set, existing_set_len, 0 };
        uint32_t sn;
        if ((e = object_begin(&sc, &sn))) { bj_builder_free(b); return e; }
        for (uint32_t i = 0; i < sn; i++) {
            const uint8_t *sk; uint32_t sklen;
            if ((e = take_key(&sc, &sk, &sklen))) { bj_builder_free(b); return e; }
            size_t vstart = sc.pos;
            if ((e = skip_value(&sc))) { bj_builder_free(b); return e; }
            /* A $set entry colliding with a $currentDate field was already
             * refused above, so this cannot shadow a date. */
            bj_put_key(b, sk, sklen);
            bj_put_raw(b, existing_set + vstart, (uint32_t)(sc.pos - vstart));
        }
    }
    {
        cur cc = { cd, cd_len, 0 };
        uint32_t cn;
        if ((e = object_begin(&cc, &cn))) { bj_builder_free(b); return e; }
        for (uint32_t i = 0; i < cn; i++) {
            const uint8_t *fk; uint32_t fklen;
            if ((e = take_key(&cc, &fk, &fklen))) { bj_builder_free(b); return e; }
            if ((e = skip_value(&cc))) { bj_builder_free(b); return e; }
            bj_put_key(b, fk, fklen);
            bj_put_date(b, now_ms);
        }
    }
    bj_end_object(b);
    bj_end_object(b);

    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, len);
    bj_builder_free(b);
    return e;
}
