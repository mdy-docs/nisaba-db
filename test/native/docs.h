/*
 * docs.h — binjson document construction and inspection for the native
 * harness.
 *
 * The dc_* API takes and returns pre-encoded binjson bytes, so a test
 * that says what it means needs a terse way to spell {_id: ..., team:
 * "core"} and to read one field back out. That is all this is.
 *
 * Buffers returned by doc_* are owned by the doc and released by
 * doc_free. Nothing here is fast; it is called a few hundred times per
 * run.
 */
#ifndef DOCS_H
#define DOCS_H

#include "binjson.h"

#include <stdlib.h>
#include <string.h>

/* ---- building --------------------------------------------------------- */

typedef struct { bj_builder *b; } doc;

static inline doc *doc_new(void) {
    doc *d = (doc *)calloc(1, sizeof(doc));
    if (!d) return NULL;
    d->b = bj_builder_new();
    if (!d->b) { free(d); return NULL; }
    bj_begin_object(d->b);
    return d;
}

static inline void doc_free(doc *d) {
    if (!d) return;
    bj_builder_free(d->b);
    free(d);
}

static inline void doc_key(doc *d, const char *k) {
    bj_put_key(d->b, (const uint8_t *)k, (uint32_t)strlen(k));
}
static inline void doc_str(doc *d, const char *k, const char *v) {
    doc_key(d, k);
    bj_put_string(d->b, (const uint8_t *)v, (uint32_t)strlen(v));
}
static inline void doc_int(doc *d, const char *k, int64_t v) {
    doc_key(d, k);
    bj_put_int(d->b, v);
}
static inline void doc_oid(doc *d, const char *k, const uint8_t id[12]) {
    doc_key(d, k);
    bj_put_oid(d->b, id);
}
static inline void doc_null(doc *d, const char *k) {
    doc_key(d, k);
    bj_put_null(d->b);
}
/* Nested object/array: caller balances with doc_end_obj/doc_end_arr. */
static inline void doc_begin_obj(doc *d, const char *k) { doc_key(d, k); bj_begin_object(d->b); }
static inline void doc_end_obj(doc *d) { bj_end_object(d->b); }
static inline void doc_begin_arr(doc *d, const char *k) { doc_key(d, k); bj_begin_array(d->b); }
static inline void doc_end_arr(doc *d) { bj_end_array(d->b); }

/* Close the top-level object and hand back its bytes (owned by `d`). */
static inline const uint8_t *doc_done(doc *d, uint32_t *len) {
    bj_end_object(d->b);
    size_t n = 0;
    const uint8_t *p = bj_builder_data(d->b, &n);
    *len = (uint32_t)n;
    return p;
}

/* A deterministic, valid 12-byte ObjectId. Distinct `n` give distinct
 * ids that sort in `n` order, which several tests rely on. */
static inline void mk_oid(uint8_t out[12], uint32_t n) {
    memset(out, 0, 12);
    out[8]  = (uint8_t)(n >> 24);
    out[9]  = (uint8_t)(n >> 16);
    out[10] = (uint8_t)(n >> 8);
    out[11] = (uint8_t)n;
}

/* ---- inspection ------------------------------------------------------- */

/*
 * bj_decode calls every visitor callback unconditionally -- it does not
 * NULL-check a single one (see bj_decode_value in binjson.c). A visitor
 * with an unset slot is therefore not "a visitor that ignores that type",
 * it is a segfault waiting for the first document that contains one.
 *
 * So: never build a bj_visitor by hand here. Start from vis_defaults(),
 * which fills all fourteen with no-ops, then override what you need.
 */
static void nop_null(void *c)                              { (void)c; }
static void nop_bool(void *c, int t)                       { (void)c; (void)t; }
static void nop_num(void *c, double v)                     { (void)c; (void)v; }
static void nop_bytes(void *c, const uint8_t *b, uint32_t n) { (void)c; (void)b; (void)n; }
static void nop_oid(void *c, const uint8_t *b)             { (void)c; (void)b; }
static void nop_count(void *c, uint32_t n)                 { (void)c; (void)n; }
static void nop_void(void *c)                              { (void)c; }

static inline bj_visitor vis_defaults(void *ctx) {
    bj_visitor v;
    v.on_null         = nop_null;
    v.on_bool         = nop_bool;
    v.on_int          = nop_num;
    v.on_float        = nop_num;
    v.on_string       = nop_bytes;
    v.on_binary       = nop_bytes;
    v.on_oid          = nop_oid;
    v.on_date         = nop_num;
    v.on_pointer      = nop_num;
    v.on_array_begin  = nop_count;
    v.on_array_end    = nop_void;
    v.on_object_begin = nop_count;
    v.on_key          = nop_bytes;
    v.on_object_end   = nop_void;
    v.ctx             = ctx;
    return v;
}

/*
 * Read one top-level STRING field out of an encoded document. Copies at
 * most cap-1 bytes into `out` and NUL-terminates. Returns 1 if the field
 * was present and a string, else 0 (and `out` becomes "").
 *
 * Depth tracking matters: a nested object may repeat the key, and only
 * the top-level one counts.
 */
typedef struct {
    const char *want;
    char       *out;
    size_t      cap;
    int         depth;      /* 0 = inside the top-level object */
    int         armed;      /* next value is the one we want */
    int         found;
} field_probe;

static void fp_key(void *ctx, const uint8_t *k, uint32_t n) {
    field_probe *p = (field_probe *)ctx;
    p->armed = (p->depth == 1 && strlen(p->want) == n &&
                memcmp(p->want, k, n) == 0);
}
static void fp_string(void *ctx, const uint8_t *s, uint32_t n) {
    field_probe *p = (field_probe *)ctx;
    if (!p->armed) return;
    size_t take = n < p->cap - 1 ? n : p->cap - 1;
    memcpy(p->out, s, take);
    p->out[take] = '\0';
    p->found = 1;
    p->armed = 0;
}
static void fp_obj_begin(void *ctx, uint32_t c) { (void)c; ((field_probe *)ctx)->depth++; }
static void fp_obj_end(void *ctx)               { ((field_probe *)ctx)->depth--; }
static void fp_arr_begin(void *ctx, uint32_t c) { (void)c; ((field_probe *)ctx)->depth++; }
static void fp_arr_end(void *ctx)               { ((field_probe *)ctx)->depth--; }
/* Any non-string value clears the arm so a later string can't be mistaken
 * for the field's value. */
static void fp_null(void *ctx)                          { ((field_probe *)ctx)->armed = 0; }
static void fp_bool(void *ctx, int t)                   { (void)t; ((field_probe *)ctx)->armed = 0; }
static void fp_num(void *ctx, double v)                 { (void)v; ((field_probe *)ctx)->armed = 0; }
static void fp_bin(void *ctx, const uint8_t *b, uint32_t n) { (void)b; (void)n; ((field_probe *)ctx)->armed = 0; }
static void fp_oid(void *ctx, const uint8_t *b)         { (void)b; ((field_probe *)ctx)->armed = 0; }

static inline int doc_get_str(const uint8_t *buf, size_t len,
                              const char *key, char *out, size_t cap) {
    field_probe p;
    memset(&p, 0, sizeof(p));
    p.want = key; p.out = out; p.cap = cap;
    out[0] = '\0';

    bj_visitor v = vis_defaults(&p);
    v.on_key = fp_key;
    v.on_string = fp_string;
    v.on_object_begin = fp_obj_begin;
    v.on_object_end = fp_obj_end;
    v.on_array_begin = fp_arr_begin;
    v.on_array_end = fp_arr_end;
    /* Any non-string value must clear the arm, or a later string could be
     * mistaken for this field's value. */
    v.on_null = fp_null;
    v.on_bool = fp_bool;
    v.on_int = fp_num;
    v.on_float = fp_num;
    v.on_date = fp_num;
    v.on_pointer = fp_num;
    v.on_binary = fp_bin;
    v.on_oid = fp_oid;

    if (bj_decode(buf, len, &v, NULL) != BJ_OK) return 0;
    return p.found;
}

/* Number of elements in a top-level binjson ARRAY. */
typedef struct { int depth; uint32_t count; } arr_probe;
static void ap_arr_begin(void *ctx, uint32_t c) {
    arr_probe *p = (arr_probe *)ctx;
    if (p->depth == 0) p->count = c;
    p->depth++;
}
static void ap_arr_end(void *ctx)               { ((arr_probe *)ctx)->depth--; }
static void ap_obj_begin(void *ctx, uint32_t c) { (void)c; ((arr_probe *)ctx)->depth++; }
static void ap_obj_end(void *ctx)               { ((arr_probe *)ctx)->depth--; }

static inline long arr_count(const uint8_t *buf, size_t len) {
    arr_probe p;
    memset(&p, 0, sizeof(p));
    bj_visitor v = vis_defaults(&p);
    v.on_array_begin = ap_arr_begin;
    v.on_array_end = ap_arr_end;
    v.on_object_begin = ap_obj_begin;
    v.on_object_end = ap_obj_end;
    if (bj_decode(buf, len, &v, NULL) != BJ_OK) return -1;
    return (long)p.count;
}

#endif /* DOCS_H */
