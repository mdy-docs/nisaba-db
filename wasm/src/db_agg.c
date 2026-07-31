/*
 * db_agg.c — see db_agg.h.
 *
 * Shape of the implementation: the pipeline carries a list of document
 * spans plus the buffers those spans point into. Stages that reorder or
 * filter only rebuild the span list; stages that synthesize documents
 * ($project via qry_collect, $group, $count) produce one new binjson
 * ARRAY buffer, which is adopted and re-split into spans for the next
 * stage. Buffers are freed when the pipeline ends, not when a stage does,
 * because spans from earlier stages may still point into them.
 */
#include "db_agg.h"
#include "db_query.h"
#include "bjcursor.h"
#include "dbuf.h"

#include <stdlib.h>
#include <string.h>

/* ---- pipeline state ---------------------------------------------------- */

typedef struct {
    uint8_t **bufs;    /* owned intermediate ARRAY buffers */
    size_t    nbufs, cbufs;
    qry_doc  *docs;    /* spans into bufs, or into the initial buffer */
    size_t    ndocs, cdocs;
} agg;

static void agg_free(agg *a) {
    for (size_t i = 0; i < a->nbufs; i++) free(a->bufs[i]);
    free(a->bufs);
    free(a->docs);
    memset(a, 0, sizeof(*a));
}

static int agg_keep_buf(agg *a, uint8_t *buf) {
    if (a->nbufs == a->cbufs) {
        size_t nc = a->cbufs ? a->cbufs * 2 : 4;
        uint8_t **nb = (uint8_t **)realloc(a->bufs, nc * sizeof(*nb));
        if (!nb) return BJ_ERR_OOM;
        a->bufs = nb; a->cbufs = nc;
    }
    a->bufs[a->nbufs++] = buf;
    return BJ_OK;
}

static int agg_push(agg *a, const uint8_t *ptr, size_t len) {
    if (a->ndocs == a->cdocs) {
        size_t nc = a->cdocs ? a->cdocs * 2 : 16;
        qry_doc *nd = (qry_doc *)realloc(a->docs, nc * sizeof(*nd));
        if (!nd) return BJ_ERR_OOM;
        a->docs = nd; a->cdocs = nc;
    }
    a->docs[a->ndocs].ptr = ptr;
    a->docs[a->ndocs].len = len;
    a->ndocs++;
    return BJ_OK;
}

/* Replace the span list with the elements of binjson ARRAY `arr`, whose
 * buffer the pipeline takes ownership of. */
static int agg_adopt_array(agg *a, uint8_t *arr, size_t arr_len) {
    int e = agg_keep_buf(a, arr);
    if (e) { free(arr); return e; }
    a->ndocs = 0;
    cur c = { arr, arr_len, 0 };
    uint32_t count;
    if ((e = array_begin(&c, &count))) return e;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if ((e = skip_value(&c))) return e;
        if ((e = agg_push(a, arr + start, c.pos - start))) return e;
    }
    return BJ_OK;
}

/* ---- expressions ------------------------------------------------------- */

/*
 * Evaluate a $group expression against one document: a STRING beginning
 * with '$' is a field path, anything else is a literal. Returns a span --
 * into the document for a path, into `expr` for a literal -- or found = 0
 * for a path that resolves to nothing.
 */
static int agg_expr(const uint8_t *doc, size_t doc_len,
                    const uint8_t *expr, size_t expr_len,
                    const uint8_t **out, size_t *out_len, int *found) {
    *found = 0;
    if (expr_len >= 1 && expr[0] == BJ_TYPE_STRING) {
        cur c = { expr, expr_len, 0 };
        const uint8_t *sp; uint32_t slen;
        if (take_string(&c, &sp, &slen) == BJ_OK && slen >= 1 && sp[0] == '$') {
            return qry_resolve_path(doc, doc_len, sp + 1, slen - 1, out, out_len, found);
        }
    }
    *out = expr; *out_len = expr_len; *found = 1;
    return BJ_OK;
}

/* Emit one value span, or NULL when it resolved to nothing. */
static void put_span_or_null(bj_builder *b, const uint8_t *p, size_t len, int found) {
    if (found) bj_put_raw(b, p, (uint32_t)len);
    else bj_put_null(b);
}

/* ---- $group ------------------------------------------------------------ */

typedef struct {
    dbuf     id;        /* the group's _id, encoded (owned)   */
    size_t  *members;   /* indexes into the stage's doc list  */
    size_t   n, cap;
} agg_group;

static int group_push_member(agg_group *g, size_t idx) {
    if (g->n == g->cap) {
        size_t nc = g->cap ? g->cap * 2 : 8;
        size_t *nm = (size_t *)realloc(g->members, nc * sizeof(*nm));
        if (!nm) return BJ_ERR_OOM;
        g->members = nm; g->cap = nc;
    }
    g->members[g->n++] = idx;
    return BJ_OK;
}

/* Accumulator opcodes, in AGG_ACCS order. */
typedef enum {
    ACC_SUM, ACC_AVG, ACC_MIN, ACC_MAX, ACC_FIRST, ACC_LAST,
    ACC_PUSH, ACC_ADD_TO_SET, ACC_COUNT, ACC_NONE
} acc_op;

static const struct { const char *name; acc_op op; } AGG_ACCS[] = {
    { "$sum", ACC_SUM }, { "$avg", ACC_AVG }, { "$min", ACC_MIN },
    { "$max", ACC_MAX }, { "$first", ACC_FIRST }, { "$last", ACC_LAST },
    { "$push", ACC_PUSH }, { "$addToSet", ACC_ADD_TO_SET }, { "$count", ACC_COUNT },
};
static const size_t ACC_COUNT_N = sizeof(AGG_ACCS) / sizeof(AGG_ACCS[0]);

static acc_op acc_lookup(const uint8_t *name, uint32_t len) {
    for (size_t i = 0; i < ACC_COUNT_N; i++) {
        if (len == strlen(AGG_ACCS[i].name) && memcmp(name, AGG_ACCS[i].name, len) == 0)
            return AGG_ACCS[i].op;
    }
    return ACC_NONE;
}

/* Numeric value of a span, or found = 0 when it isn't a number. */
static int span_number(const uint8_t *p, size_t len, double *out) {
    if (len < 1) return 0;
    if (p[0] != BJ_TYPE_INT && p[0] != BJ_TYPE_FLOAT) return 0;
    cur c = { p, len, 0 };
    return read_number(&c, out) == BJ_OK;
}

/* Build the _id value for one document from the $group _id expression. */
static int group_id_of(const uint8_t *doc, size_t doc_len,
                       const uint8_t *id_expr, size_t id_expr_len,
                       dbuf *out) {
    out->len = 0;
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;

    int e = BJ_OK;
    if (id_expr_len == 0 || (id_expr_len >= 1 && id_expr[0] == BJ_TYPE_NULL)) {
        bj_put_null(b);
    } else if (id_expr[0] == BJ_TYPE_OBJECT) {
        /* Composite id: {k: <expr>, ...} evaluated field by field. */
        cur c = { id_expr, id_expr_len, 0 };
        uint32_t n;
        if ((e = object_begin(&c, &n))) { bj_builder_free(b); return e; }
        bj_begin_object(b);
        for (uint32_t i = 0; i < n; i++) {
            const uint8_t *kp; uint32_t klen;
            if ((e = take_key(&c, &kp, &klen))) { bj_builder_free(b); return e; }
            size_t vstart = c.pos;
            if ((e = skip_value(&c))) { bj_builder_free(b); return e; }
            const uint8_t *vp; size_t vlen; int found;
            e = agg_expr(doc, doc_len, id_expr + vstart, c.pos - vstart, &vp, &vlen, &found);
            if (e) { bj_builder_free(b); return e; }
            bj_put_key(b, kp, klen);
            put_span_or_null(b, vp, vlen, found);
        }
        bj_end_object(b);
    } else {
        const uint8_t *vp; size_t vlen; int found;
        e = agg_expr(doc, doc_len, id_expr, id_expr_len, &vp, &vlen, &found);
        if (e) { bj_builder_free(b); return e; }
        put_span_or_null(b, vp, vlen, found);
    }

    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, len);
    bj_builder_free(b);
    return e;
}

/*
 * Emit one accumulator's value for one group.
 *
 * `expr` is the accumulator's argument, e.g. the "$amount" in
 * {$sum: "$amount"}.
 */
static int emit_accumulator(bj_builder *b, acc_op op,
                            const uint8_t *expr, size_t expr_len,
                            const agg *a, const agg_group *g) {
    switch (op) {
        case ACC_COUNT:
            bj_put_int(b, (int64_t)g->n);
            return BJ_OK;

        case ACC_SUM: {
            /* {$sum: 1} is a counter, matching MongoDB. */
            double lit;
            if (span_number(expr, expr_len, &lit)) {
                bj_put_float(b, lit * (double)g->n);
                return BJ_OK;
            }
            double total = 0;
            for (size_t i = 0; i < g->n; i++) {
                const qry_doc *d = &a->docs[g->members[i]];
                const uint8_t *vp; size_t vlen; int found; double v;
                int e = agg_expr(d->ptr, d->len, expr, expr_len, &vp, &vlen, &found);
                if (e) return e;
                if (found && span_number(vp, vlen, &v)) total += v;
            }
            bj_put_float(b, total);
            return BJ_OK;
        }

        case ACC_AVG: {
            double total = 0; size_t count = 0;
            for (size_t i = 0; i < g->n; i++) {
                const qry_doc *d = &a->docs[g->members[i]];
                const uint8_t *vp; size_t vlen; int found; double v;
                int e = agg_expr(d->ptr, d->len, expr, expr_len, &vp, &vlen, &found);
                if (e) return e;
                if (found && span_number(vp, vlen, &v)) { total += v; count++; }
            }
            if (count) bj_put_float(b, total / (double)count);
            else bj_put_null(b);
            return BJ_OK;
        }

        case ACC_MIN:
        case ACC_MAX: {
            const uint8_t *best = NULL; size_t best_len = 0;
            for (size_t i = 0; i < g->n; i++) {
                const qry_doc *d = &a->docs[g->members[i]];
                const uint8_t *vp; size_t vlen; int found;
                int e = agg_expr(d->ptr, d->len, expr, expr_len, &vp, &vlen, &found);
                if (e) return e;
                if (!found || vlen < 1 || vp[0] == BJ_TYPE_NULL) continue;
                if (!best) { best = vp; best_len = vlen; continue; }
                int cmp = qry_value_cmp(vp, vlen, best, best_len);
                if (cmp == -2) continue;   /* incomparable: keep the incumbent */
                if (op == ACC_MIN ? cmp < 0 : cmp > 0) { best = vp; best_len = vlen; }
            }
            if (best) bj_put_raw(b, best, (uint32_t)best_len);
            else bj_put_null(b);
            return BJ_OK;
        }

        case ACC_FIRST:
        case ACC_LAST: {
            if (!g->n) { bj_put_null(b); return BJ_OK; }
            const qry_doc *d = &a->docs[g->members[op == ACC_FIRST ? 0 : g->n - 1]];
            const uint8_t *vp; size_t vlen; int found;
            int e = agg_expr(d->ptr, d->len, expr, expr_len, &vp, &vlen, &found);
            if (e) return e;
            put_span_or_null(b, vp, vlen, found);
            return BJ_OK;
        }

        case ACC_PUSH:
        case ACC_ADD_TO_SET: {
            bj_begin_array(b);
            val_list seen = {0};
            int e = BJ_OK;
            for (size_t i = 0; i < g->n; i++) {
                const qry_doc *d = &a->docs[g->members[i]];
                const uint8_t *vp; size_t vlen; int found;
                if ((e = agg_expr(d->ptr, d->len, expr, expr_len, &vp, &vlen, &found))) break;
                if (!found) continue;    /* a missing field contributes nothing */
                if (op == ACC_ADD_TO_SET) {
                    /* Identity is exact encoded-byte equality -- the same
                     * rule every other equality in this codebase uses. */
                    int dup = 0;
                    for (uint32_t s = 0; s < seen.count; s++) {
                        if (value_eq(seen.items[s].ptr, seen.items[s].len, vp, vlen)) { dup = 1; break; }
                    }
                    if (dup) continue;
                    if ((e = val_list_push(&seen, vp, vlen))) break;
                }
                bj_put_raw(b, vp, (uint32_t)vlen);
            }
            val_list_free(&seen);
            if (e) return e;
            bj_end_array(b);
            return BJ_OK;
        }

        default:
            return DC_ERR_AGG_BAD_ACCUMULATOR;
    }
}

static int stage_group(agg *a, const uint8_t *spec, size_t spec_len) {
    cur c = { spec, spec_len, 0 };
    uint32_t nkeys;
    int e = object_begin(&c, &nkeys);
    if (e) return e;

    /* Locate _id (default null) and validate every accumulator up front. */
    const uint8_t *id_expr = NULL; size_t id_expr_len = 0;
    {
        int found = 0;
        e = obj_get_field(spec, spec_len, (const uint8_t *)"_id", 3,
                          &id_expr, &id_expr_len, &found);
        if (e) return e;
        if (!found) { id_expr = NULL; id_expr_len = 0; }
    }
    {
        cur scan = c;
        for (uint32_t i = 0; i < nkeys; i++) {
            const uint8_t *kp; uint32_t klen;
            if ((e = take_key(&scan, &kp, &klen))) return e;
            size_t vstart = scan.pos;
            if ((e = skip_value(&scan))) return e;
            if (klen == 3 && memcmp(kp, "_id", 3) == 0) continue;
            const uint8_t *acc = spec + vstart;
            size_t acc_len = scan.pos - vstart;
            cur ac = { acc, acc_len, 0 };
            uint32_t an;
            if (object_begin(&ac, &an) != BJ_OK || an != 1) return DC_ERR_AGG_BAD_ACCUMULATOR;
            const uint8_t *ap; uint32_t alen;
            if (take_key(&ac, &ap, &alen) != BJ_OK) return DC_ERR_AGG_BAD_ACCUMULATOR;
            if (acc_lookup(ap, alen) == ACC_NONE) return DC_ERR_AGG_BAD_ACCUMULATOR;
        }
    }

    /* Bucket documents by their encoded _id, preserving first-seen order
     * so $first/$last mean what they say. */
    agg_group *groups = NULL;
    size_t ngroups = 0, cgroups = 0;
    for (size_t d = 0; d < a->ndocs; d++) {
        dbuf id = {0};
        if ((e = group_id_of(a->docs[d].ptr, a->docs[d].len, id_expr, id_expr_len, &id))) {
            dbuf_free(&id);
            goto fail;
        }
        size_t at = ngroups;
        for (size_t g = 0; g < ngroups; g++) {
            if (value_eq(groups[g].id.data, groups[g].id.len, id.data, id.len)) { at = g; break; }
        }
        if (at == ngroups) {
            if (ngroups == cgroups) {
                size_t nc = cgroups ? cgroups * 2 : 8;
                agg_group *ng = (agg_group *)realloc(groups, nc * sizeof(*ng));
                if (!ng) { dbuf_free(&id); e = BJ_ERR_OOM; goto fail; }
                memset(ng + cgroups, 0, (nc - cgroups) * sizeof(*ng));
                groups = ng; cgroups = nc;
            }
            groups[ngroups].id = id;      /* group takes ownership */
            ngroups++;
        } else {
            dbuf_free(&id);
        }
        if ((e = group_push_member(&groups[at], d))) goto fail;
    }

    /* Emit one row per group. */
    bj_builder *b = bj_builder_new();
    if (!b) { e = BJ_ERR_OOM; goto fail; }
    bj_begin_array(b);
    for (size_t g = 0; g < ngroups; g++) {
        bj_begin_object(b);
        bj_put_key(b, (const uint8_t *)"_id", 3);
        bj_put_raw(b, groups[g].id.data, (uint32_t)groups[g].id.len);

        cur field = c;
        for (uint32_t i = 0; i < nkeys; i++) {
            const uint8_t *kp; uint32_t klen;
            if ((e = take_key(&field, &kp, &klen))) { bj_builder_free(b); goto fail; }
            size_t vstart = field.pos;
            if ((e = skip_value(&field))) { bj_builder_free(b); goto fail; }
            if (klen == 3 && memcmp(kp, "_id", 3) == 0) continue;

            const uint8_t *acc = spec + vstart;
            size_t acc_len = field.pos - vstart;
            cur ac = { acc, acc_len, 0 };
            uint32_t an;
            if ((e = object_begin(&ac, &an))) { bj_builder_free(b); goto fail; }
            const uint8_t *ap; uint32_t alen;
            if ((e = take_key(&ac, &ap, &alen))) { bj_builder_free(b); goto fail; }
            size_t estart = ac.pos;
            if ((e = skip_value(&ac))) { bj_builder_free(b); goto fail; }

            bj_put_key(b, kp, klen);
            e = emit_accumulator(b, acc_lookup(ap, alen), acc + estart, ac.pos - estart,
                                 a, &groups[g]);
            if (e) { bj_builder_free(b); goto fail; }
        }
        bj_end_object(b);
    }
    bj_end_array(b);

    if ((e = bj_builder_error(b))) { bj_builder_free(b); goto fail; }
    {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { bj_builder_free(b); e = BJ_ERR_STATE; goto fail; }
        uint8_t *owned; size_t owned_len;
        e = dbuf_dup(data, len, &owned, &owned_len);
        bj_builder_free(b);
        if (e) goto fail;
        e = agg_adopt_array(a, owned, owned_len);
    }

fail:
    for (size_t g = 0; g < ngroups; g++) {
        dbuf_free(&groups[g].id);
        free(groups[g].members);
    }
    free(groups);
    return e;
}

/* ---- other stages ------------------------------------------------------ */

/* $sort / $skip / $limit / $project all reduce to one qry_collect -- the
 * same path find() takes, rather than a second implementation of each. */
static int stage_collect(agg *a, const qry_options *opts) {
    uint8_t *out = NULL; size_t out_len = 0;
    int e = qry_validate_options(opts);
    if (e) return e;
    e = qry_collect(a->docs, a->ndocs, opts, &out, &out_len);
    if (e) return e;
    return agg_adopt_array(a, out, out_len);
}

static int stage_match(agg *a, const uint8_t *filter, size_t filter_len) {
    size_t kept = 0;
    for (size_t i = 0; i < a->ndocs; i++) {
        int m = 0;
        int e = qry_matches(a->docs[i].ptr, a->docs[i].len, filter, filter_len, &m);
        if (e) return e;
        if (m) a->docs[kept++] = a->docs[i];
    }
    a->ndocs = kept;
    return BJ_OK;
}

static int stage_count(agg *a, const uint8_t *name, size_t name_len) {
    if (name_len < 1 || name[0] != BJ_TYPE_STRING) return DC_ERR_AGG_BAD_STAGE;
    cur c = { name, name_len, 0 };
    const uint8_t *sp; uint32_t slen;
    int e = take_string(&c, &sp, &slen);
    if (e) return e;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_array(b);
    bj_begin_object(b);
    bj_put_key(b, sp, slen);
    bj_put_int(b, (int64_t)a->ndocs);
    bj_end_object(b);
    bj_end_array(b);
    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }

    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    uint8_t *owned; size_t owned_len;
    e = dbuf_dup(data, len, &owned, &owned_len);
    bj_builder_free(b);
    if (e) return e;
    return agg_adopt_array(a, owned, owned_len);
}

/*
 * $project.
 *
 * Not delegated to qry_collect's projection, which is find()'s: that one
 * knows 1/0 inclusion and exclusion only, while an aggregation $project
 * also takes a computed field -- {total: "$qty"} copies a value under a
 * new name. So this stage is its own implementation, and the only one in
 * the pipeline that is.
 *
 * Field order matches the JS it replaces: in inclusion mode _id comes
 * first (unless dropped), then the spec's fields in spec order; in
 * exclusion mode the document's own order survives.
 */
static int stage_project(agg *a, const uint8_t *spec, size_t spec_len) {
    /* Which mode, and is _id dropped? */
    int excluding = 0, drop_id = 0;
    {
        cur c = { spec, spec_len, 0 };
        uint32_t n;
        int e = object_begin(&c, &n);
        if (e) return e;
        for (uint32_t i = 0; i < n; i++) {
            const uint8_t *kp; uint32_t klen;
            if ((e = take_key(&c, &kp, &klen))) return e;
            size_t vstart = c.pos;
            if ((e = skip_value(&c))) return e;
            const uint8_t *v = spec + vstart;
            size_t vlen = c.pos - vstart;
            double d;
            int is_zero = (vlen >= 1 && v[0] == BJ_TYPE_FALSE) ||
                          (span_number(v, vlen, &d) && d == 0);
            if (klen == 3 && memcmp(kp, "_id", 3) == 0) { drop_id = is_zero; continue; }
            if (is_zero) excluding = 1;
        }
    }

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = BJ_OK;
    bj_begin_array(b);

    for (size_t d = 0; d < a->ndocs; d++) {
        const uint8_t *doc = a->docs[d].ptr;
        size_t doc_len = a->docs[d].len;
        bj_begin_object(b);

        if (excluding) {
            /* Copy every field the spec did not name. */
            cur dc = { doc, doc_len, 0 };
            uint32_t dn;
            if ((e = object_begin(&dc, &dn))) break;
            for (uint32_t i = 0; i < dn; i++) {
                const uint8_t *dk; uint32_t dklen;
                if ((e = take_key(&dc, &dk, &dklen))) goto done;
                size_t vstart = dc.pos;
                if ((e = skip_value(&dc))) goto done;
                if (dklen == 3 && memcmp(dk, "_id", 3) == 0 && drop_id) continue;

                int dropped = 0;
                cur sc2 = { spec, spec_len, 0 };
                uint32_t sn;
                if ((e = object_begin(&sc2, &sn))) goto done;
                for (uint32_t j = 0; j < sn && !dropped; j++) {
                    const uint8_t *sk; uint32_t sklen;
                    if (take_key(&sc2, &sk, &sklen) != BJ_OK) break;
                    if (skip_value(&sc2) != BJ_OK) break;
                    if (sklen == 3 && memcmp(sk, "_id", 3) == 0) continue;
                    if (sklen == dklen && memcmp(sk, dk, sklen) == 0) dropped = 1;
                }
                if (!dropped) {
                    bj_put_key(b, dk, dklen);
                    bj_put_raw(b, doc + vstart, (uint32_t)(dc.pos - vstart));
                }
            }
        } else {
            /* _id first, then the spec's fields in spec order. */
            if (!drop_id) {
                const uint8_t *vp; size_t vlen; int found = 0;
                if ((e = qry_resolve_path(doc, doc_len, (const uint8_t *)"_id", 3,
                                          &vp, &vlen, &found))) goto done;
                if (found) {
                    bj_put_key(b, (const uint8_t *)"_id", 3);
                    bj_put_raw(b, vp, (uint32_t)vlen);
                }
            }
            cur sc2 = { spec, spec_len, 0 };
            uint32_t sn;
            if ((e = object_begin(&sc2, &sn))) goto done;
            for (uint32_t j = 0; j < sn; j++) {
                const uint8_t *sk; uint32_t sklen;
                if ((e = take_key(&sc2, &sk, &sklen))) goto done;
                size_t vstart = sc2.pos;
                if ((e = skip_value(&sc2))) goto done;
                if (sklen == 3 && memcmp(sk, "_id", 3) == 0) continue;

                const uint8_t *sv = spec + vstart;
                size_t svlen = sc2.pos - vstart;
                double d1;
                int plain_include = (svlen >= 1 && sv[0] == BJ_TYPE_TRUE) ||
                                    (span_number(sv, svlen, &d1) && d1 == 1);

                const uint8_t *vp; size_t vlen; int found = 0;
                if (plain_include) {
                    e = qry_resolve_path(doc, doc_len, sk, sklen, &vp, &vlen, &found);
                } else {
                    e = agg_expr(doc, doc_len, sv, svlen, &vp, &vlen, &found);
                }
                if (e) goto done;
                /* A field that resolves to nothing is omitted, not null. */
                if (found) {
                    bj_put_key(b, sk, sklen);
                    bj_put_raw(b, vp, (uint32_t)vlen);
                }
            }
        }
        bj_end_object(b);
    }

done:
    if (e) { bj_builder_free(b); return e; }
    bj_end_array(b);
    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }
    {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
        uint8_t *owned; size_t owned_len;
        e = dbuf_dup(data, len, &owned, &owned_len);
        bj_builder_free(b);
        if (e) return e;
        return agg_adopt_array(a, owned, owned_len);
    }
}

/*
 * $project's inclusion-XOR-exclusion rule, checked before running so the
 * error names the stage.
 */
static int check_projection(const uint8_t *spec, size_t spec_len) {
    cur c = { spec, spec_len, 0 };
    uint32_t n;
    int e = object_begin(&c, &n);
    if (e) return e;
    int including = 0, excluding = 0;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&c, &kp, &klen))) return e;
        size_t vstart = c.pos;
        if ((e = skip_value(&c))) return e;
        if (klen == 3 && memcmp(kp, "_id", 3) == 0) continue;   /* _id is exempt */
        const uint8_t *v = spec + vstart;
        size_t vlen = c.pos - vstart;
        double d;
        int is_zero = (vlen >= 1 && v[0] == BJ_TYPE_FALSE) ||
                      (span_number(v, vlen, &d) && d == 0);
        if (is_zero) excluding = 1; else including = 1;
    }
    return (including && excluding) ? DC_ERR_AGG_PROJECT_MIXED : BJ_OK;
}

/* ---- driver ------------------------------------------------------------ */

/* Is `stage` a single-key OBJECT named `name`? Fills the spec span. */
static int stage_is(const uint8_t *stage, size_t stage_len, const char *name,
                    const uint8_t **spec, size_t *spec_len) {
    cur c = { stage, stage_len, 0 };
    uint32_t n;
    if (object_begin(&c, &n) != BJ_OK || n != 1) return 0;
    const uint8_t *kp; uint32_t klen;
    if (take_key(&c, &kp, &klen) != BJ_OK) return 0;
    if (klen != strlen(name) || memcmp(kp, name, klen) != 0) return 0;
    size_t start = c.pos;
    if (skip_value(&c) != BJ_OK) return 0;
    *spec = stage + start;
    *spec_len = c.pos - start;
    return 1;
}

int dc_aggregate(dc_collection *c, const uint8_t *stages, size_t stages_len,
                 int *bad_stage, uint8_t **out, size_t *out_len) {
    *bad_stage = -1;
    cur sc = { stages, stages_len, 0 };
    uint32_t nstages;
    int e = array_begin(&sc, &nstages);
    if (e) return e;

    /* Record each stage's span up front so the driver can index them. */
    qry_doc *stage_spans = NULL;
    if (nstages) {
        stage_spans = (qry_doc *)calloc(nstages, sizeof(*stage_spans));
        if (!stage_spans) return BJ_ERR_OOM;
    }
    for (uint32_t i = 0; i < nstages; i++) {
        size_t start = sc.pos;
        if ((e = skip_value(&sc))) { free(stage_spans); return e; }
        stage_spans[i].ptr = stages + start;
        stage_spans[i].len = sc.pos - start;
    }

    /* Leading-$match pushdown: it becomes the scan's filter, so the
     * planner and any index can serve it. */
    uint32_t first = 0;
    const uint8_t *pushdown = NULL; size_t pushdown_len = 0;
    if (nstages > 0 && stage_is(stage_spans[0].ptr, stage_spans[0].len, "$match",
                                &pushdown, &pushdown_len)) {
        first = 1;
    }

    static const uint8_t EMPTY_FILTER[] = { BJ_TYPE_OBJECT, 0,0,0,0, 0,0,0,0 };
    uint8_t *materialized = NULL; size_t materialized_len = 0;
    e = dc_find(c, pushdown ? pushdown : EMPTY_FILTER,
                (uint32_t)(pushdown ? pushdown_len : sizeof(EMPTY_FILTER)),
                NULL, &materialized, &materialized_len);
    if (e) { free(stage_spans); return e; }

    agg a;
    memset(&a, 0, sizeof(a));
    e = agg_adopt_array(&a, materialized, materialized_len);

    for (uint32_t i = first; !e && i < nstages; i++) {
        *bad_stage = (int)i;
        const uint8_t *stage = stage_spans[i].ptr;
        size_t stage_len = stage_spans[i].len;
        const uint8_t *spec; size_t spec_len;
        qry_options opts;
        memset(&opts, 0, sizeof(opts));

        if (stage_is(stage, stage_len, "$match", &spec, &spec_len)) {
            e = stage_match(&a, spec, spec_len);
        } else if (stage_is(stage, stage_len, "$sort", &spec, &spec_len)) {
            opts.sort = spec; opts.sort_len = (uint32_t)spec_len;
            e = stage_collect(&a, &opts);
        } else if (stage_is(stage, stage_len, "$skip", &spec, &spec_len)) {
            double n;
            if (!span_number(spec, spec_len, &n) || n < 0) { e = DC_ERR_AGG_BAD_STAGE; break; }
            opts.skip = (int64_t)n;
            e = stage_collect(&a, &opts);
        } else if (stage_is(stage, stage_len, "$limit", &spec, &spec_len)) {
            double n;
            if (!span_number(spec, spec_len, &n) || n < 0) { e = DC_ERR_AGG_BAD_STAGE; break; }
            /* qry_options treats limit 0 as unlimited, but {$limit: 0} in
             * a pipeline means an empty result. */
            if (n == 0) { a.ndocs = 0; continue; }
            opts.limit = (int64_t)n;
            e = stage_collect(&a, &opts);
        } else if (stage_is(stage, stage_len, "$project", &spec, &spec_len)) {
            if ((e = check_projection(spec, spec_len))) break;
            e = stage_project(&a, spec, spec_len);
        } else if (stage_is(stage, stage_len, "$group", &spec, &spec_len)) {
            e = stage_group(&a, spec, spec_len);
        } else if (stage_is(stage, stage_len, "$count", &spec, &spec_len)) {
            e = stage_count(&a, spec, spec_len);
        } else {
            /* Either not a single-key object at all, or a stage we don't
             * implement -- distinguished so the message can say which. */
            cur probe = { stage, stage_len, 0 };
            uint32_t n;
            e = (object_begin(&probe, &n) == BJ_OK && n == 1)
                ? DC_ERR_AGG_UNKNOWN_STAGE : DC_ERR_AGG_BAD_STAGE;
        }
    }

    if (!e) { *bad_stage = -1; e = qry_collect(a.docs, a.ndocs, NULL, out, out_len); }
    agg_free(&a);
    free(stage_spans);
    return e;
}
