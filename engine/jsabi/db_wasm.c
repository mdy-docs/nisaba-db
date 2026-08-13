/*
 * db_wasm.c — Emscripten glue over the document-collection ops in db.c.
 *
 * A collection is opened once via dcw_collection_open(primaryTreeCtx) —
 * primary and index trees themselves are opened via the existing
 * bptw_create/bptw_open (bplustree_wasm.c) — and freed with
 * dcw_collection_free. Filters, documents, replacements and index field/
 * value lists cross the bridge as pre-encoded binjson bytes (ptr+len) the
 * JS side already produces via encode(); dc_find_one/dc_find/
 * dc_collection_find_by_index results land in a reusable per-collection
 * output slot (dcw_out_new/dcw_out_free, mirroring textindex_wasm.c's
 * tixw_out) read via dcw_out_ptr/dcw_out_len.
 *
 * Memory: heap growth may swap HEAPU8's ArrayBuffer, so JS must re-read
 * HEAPU8 after any call before touching a returned pointer.
 */
#include "db.h"
#include "dbuf.h"
#include "hostio.h"
#include "binjson.h"   /* the backfill_step result object */

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { uint8_t *buf; size_t len; } dcw_out;

EMSCRIPTEN_KEEPALIVE dcw_out *dcw_out_new(void) {
    return (dcw_out *)calloc(1, sizeof(dcw_out));
}
EMSCRIPTEN_KEEPALIVE void dcw_out_free(dcw_out *o) {
    if (!o) return;
    free(o->buf);
    free(o);
}
static void reset_out(dcw_out *o) { free(o->buf); o->buf = NULL; o->len = 0; }

EMSCRIPTEN_KEEPALIVE dc_collection *dcw_collection_open(bpt *primary) {
    return dc_collection_open(primary);
}
EMSCRIPTEN_KEEPALIVE void dcw_collection_free(dc_collection *c) {
    dc_collection_free(c);
}

/* Enable the collection's cross-file commit journal and reconcile it against
 * the primary tree + every currently attached index (call once, after every
 * attach_* call for this collection). `jfd < 0` disables journaling. Mirrors
 * tixw_recover in textindex_wasm.c. */
EMSCRIPTEN_KEEPALIVE int dcw_collection_recover(dc_collection *c, int jfd) {
    if (jfd < 0) return dc_collection_recover(c, NULL);
    bj_io jio = bjio_host(jfd);
    return dc_collection_recover(c, &jio);
}

/* Replicated-log integration -- see dc_applied_index/dc_set_applied_index
 * in db.h. Indexes cross the bridge as doubles, same as elw/bptw. */
EMSCRIPTEN_KEEPALIVE double dcw_applied_index(dc_collection *c) {
    return (double)dc_applied_index(c);
}
EMSCRIPTEN_KEEPALIVE int dcw_set_applied_index(dc_collection *c, double index) {
    return dc_set_applied_index(c, (uint64_t)index);
}

/* `partial_filter_len == 0` means "no partial filter" (partial_filter may
 * then be any pointer, including NULL/0 -- dc_collection_attach_index only
 * consults it when the length is positive). */
EMSCRIPTEN_KEEPALIVE int dcw_collection_attach_index(dc_collection *c,
        const char *name, int name_len, bpt *index_tree,
        const uint8_t *fields, int fields_len,
        int unique, int sparse,
        const uint8_t *partial_filter, int partial_filter_len) {
    return dc_collection_attach_index(c, name, name_len, index_tree, fields, (uint32_t)fields_len,
                                      unique, sparse, partial_filter, (uint32_t)partial_filter_len);
}
EMSCRIPTEN_KEEPALIVE int dcw_collection_add_index(dc_collection *c,
        const char *name, int name_len, bpt *index_tree,
        const uint8_t *fields, int fields_len,
        int unique, int sparse,
        const uint8_t *partial_filter, int partial_filter_len) {
    return dc_collection_add_index(c, name, name_len, index_tree, fields, (uint32_t)fields_len,
                                   unique, sparse, partial_filter, (uint32_t)partial_filter_len);
}
EMSCRIPTEN_KEEPALIVE int dcw_collection_attach_text_index(dc_collection *c,
        const char *name, int name_len,
        bpt *tix_index, bpt *tix_doc_terms, bpt *tix_doc_lengths,
        const char *field, int field_len) {
    return dc_collection_attach_text_index(c, name, name_len, tix_index, tix_doc_terms, tix_doc_lengths, field, field_len);
}
EMSCRIPTEN_KEEPALIVE int dcw_collection_add_text_index(dc_collection *c,
        const char *name, int name_len,
        bpt *tix_index, bpt *tix_doc_terms, bpt *tix_doc_lengths,
        const char *field, int field_len) {
    return dc_collection_add_text_index(c, name, name_len, tix_index, tix_doc_terms, tix_doc_lengths, field, field_len);
}

EMSCRIPTEN_KEEPALIVE int dcw_collection_attach_geo_index(dc_collection *c,
        const char *name, int name_len, rtree *rt,
        const char *field, int field_len) {
    return dc_collection_attach_geo_index(c, name, name_len, rt, field, field_len);
}
EMSCRIPTEN_KEEPALIVE int dcw_collection_add_geo_index(dc_collection *c,
        const char *name, int name_len, rtree *rt,
        const char *field, int field_len) {
    return dc_collection_add_geo_index(c, name, name_len, rt, field, field_len);
}

EMSCRIPTEN_KEEPALIVE int dcw_collection_remove_index(dc_collection *c,
        const char *name, int name_len) {
    return dc_collection_remove_index(c, name, name_len);
}

/* The staged build's collection-level halves (db.h). `add_index_staged`
 * attaches EMPTY and BUILDING -- the chunks backfill; `backfill_step`
 * advances one, answering {advanced, done, last?} through the out slot
 * so the JS applier can record the cursor exactly as the C session
 * does. */
EMSCRIPTEN_KEEPALIVE int dcw_collection_add_index_staged(dc_collection *c,
        const char *name, int name_len, bpt *index_tree,
        const uint8_t *fields, int fields_len,
        int unique, int sparse,
        const uint8_t *partial_filter, int partial_filter_len) {
    return dc_collection_add_index_staged(c, name, name_len, index_tree,
                                          fields, (uint32_t)fields_len,
                                          unique, sparse,
                                          partial_filter, (uint32_t)partial_filter_len);
}
EMSCRIPTEN_KEEPALIVE int dcw_collection_index_set_building(dc_collection *c,
        const char *name, int name_len, int building) {
    return dc_collection_index_set_building(c, name, name_len, building);
}
EMSCRIPTEN_KEEPALIVE int dcw_collection_index_is_building(dc_collection *c,
        const char *name, int name_len) {
    return dc_collection_index_is_building(c, name, name_len);
}
EMSCRIPTEN_KEEPALIVE int dcw_backfill_step(dcw_out *o, dc_collection *c,
        const char *name, int name_len,
        const uint8_t *after_id, int k) {
    reset_out(o);
    uint8_t last[12];
    uint32_t advanced = 0;
    int done = 0;
    int e = dc_collection_backfill_step(c, name, name_len,
                                        after_id, (uint32_t)k,
                                        last, &advanced, &done);
    if (e) return e;
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"advanced", 8);
    if (!e) e = bj_put_int(b, (int64_t)advanced);
    if (!e) e = bj_put_key(b, (const uint8_t *)"done", 4);
    if (!e) e = bj_put_bool(b, done);
    if (!e && advanced) {
        e = bj_put_key(b, (const uint8_t *)"last", 4);
        if (!e) e = bj_put_oid(b, last);
    }
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        if (!d) e = BJ_ERR_STATE;
        else {
            o->buf = (uint8_t *)malloc(n ? n : 1);
            if (!o->buf) e = BJ_ERR_OOM;
            else { memcpy(o->buf, d, n); o->len = n; }
        }
    }
    bj_builder_free(b);
    return e;
}

EMSCRIPTEN_KEEPALIVE int dcw_find_by_index(dcw_out *o, dc_collection *c,
        const char *name, int name_len, const uint8_t *values, int values_len) {
    reset_out(o);
    return dc_collection_find_by_index(c, name, name_len, values, (uint32_t)values_len, &o->buf, &o->len);
}

EMSCRIPTEN_KEEPALIVE int dcw_insert_one(dc_collection *c, const uint8_t *doc, int doc_len) {
    return dc_insert_one(c, doc, (uint32_t)doc_len);
}

/* `docs` is a binjson ARRAY; the out slot receives a binjson ARRAY of one
 * INT error code per attempted document (see dc_insert_many). Returns 0 on
 * success (regardless of individual document outcomes, which JS inspects
 * in the out slot), negative if the result array itself couldn't be built. */
EMSCRIPTEN_KEEPALIVE int dcw_insert_many(dcw_out *o, dc_collection *c,
        const uint8_t *docs, int docs_len, int ordered) {
    reset_out(o);
    return dc_insert_many(c, docs, (uint32_t)docs_len, ordered, &o->buf, &o->len);
}

/* `projection` may be NULL (with length 0) for "none", same convention as
 * dcw_find's. Returns 1 if found (document in the out slot), 0 if not,
 * negative on error. */
EMSCRIPTEN_KEEPALIVE int dcw_find_one(dcw_out *o, dc_collection *c,
                                      const uint8_t *filter, int filter_len,
                                      const uint8_t *projection, int projection_len) {
    reset_out(o);
    int found = 0;
    int e = dc_find_one(c, filter, (uint32_t)filter_len,
                        projection_len > 0 ? projection : NULL,
                        projection_len > 0 ? (uint32_t)projection_len : 0,
                        &found, &o->buf, &o->len);
    if (e) return e;
    return found ? 1 : 0;
}

/* `sort`/`projection` may be NULL (with length 0) for "none"; skip/limit
 * cross the bridge as doubles (like every other count/size value here) and
 * 0 means "no skip" / "no limit" -- see db_query.h. */
EMSCRIPTEN_KEEPALIVE int dcw_find(dcw_out *o, dc_collection *c,
                                  const uint8_t *filter, int filter_len,
                                  const uint8_t *sort, int sort_len,
                                  double skip, double limit,
                                  const uint8_t *projection, int projection_len) {
    reset_out(o);
    qry_options opts;
    opts.sort = sort_len > 0 ? sort : NULL;
    opts.sort_len = sort_len > 0 ? (uint32_t)sort_len : 0;
    opts.skip = (int64_t)skip;
    opts.limit = (int64_t)limit;
    opts.projection = projection_len > 0 ? projection : NULL;
    opts.projection_len = projection_len > 0 ? (uint32_t)projection_len : 0;
    return dc_find(c, filter, (uint32_t)filter_len, &opts, &o->buf, &o->len);
}

/*
 * Resumable cursor over dc_find's same match set, minus sort support (see
 * db.h's dc_cursor comment) -- lets JS pull results in bounded-memory
 * batches across multiple separate calls instead of dc_find's single
 * fully-materializing one. skip/limit/projection follow dcw_find's own
 * bridge conventions (0/NULL = none). Returns NULL on failure, with the
 * specific error code written through *err_out -- unlike dc_find, which
 * returns the error code directly, this needs the return slot for the
 * cursor pointer itself. */
EMSCRIPTEN_KEEPALIVE dc_cursor *dcw_cursor_open(dc_collection *c,
                                                const uint8_t *filter, int filter_len,
                                                double skip, double limit,
                                                const uint8_t *projection, int projection_len,
                                                int *err_out) {
    dc_cursor *cur = NULL;
    int e = dc_cursor_open(c, filter, (uint32_t)filter_len,
                           projection_len > 0 ? projection : NULL,
                           projection_len > 0 ? (uint32_t)projection_len : 0,
                           (int64_t)skip, (int64_t)limit, &cur);
    *err_out = e;
    return cur;
}

/* Writes the next up-to-max_count matches into `o` and whether the cursor is now exhausted through *done_out. */
EMSCRIPTEN_KEEPALIVE int dcw_cursor_next_batch(dcw_out *o, dc_cursor *cur, int max_count, int *done_out) {
    reset_out(o);
    int done = 0;
    int e = dc_cursor_next_batch(cur, (uint32_t)max_count, &o->buf, &o->len, &done);
    *done_out = done;
    return e;
}

EMSCRIPTEN_KEEPALIVE void dcw_cursor_close(dc_cursor *cur) {
    dc_cursor_close(cur);
}

/* Returns 1 if deleted, 0 if not found, negative on error. */
EMSCRIPTEN_KEEPALIVE int dcw_delete_one(dc_collection *c, const uint8_t *filter, int filter_len) {
    int deleted = 0;
    int e = dc_delete_one(c, filter, (uint32_t)filter_len, &deleted);
    if (e) return e;
    return deleted ? 1 : 0;
}

/* Returns the number deleted, or a negative error code (mirrors dcw_count's
 * double-return-for-int64 pattern). */
/* `want_ids` asks for every removed document's _id alongside the count --
 * what a delete change-event needs, and what the host otherwise had to
 * pre-read with a projected find(). */
EMSCRIPTEN_KEEPALIVE double dcw_delete_many(dcw_out *o, dc_collection *c,
        const uint8_t *filter, int filter_len, int want_ids) {
    reset_out(o);
    int64_t n = 0;
    uint8_t *ids = NULL; size_t ids_len = 0;
    int e = dc_delete_many(c, filter, (uint32_t)filter_len, &n,
                           want_ids ? &ids : NULL, want_ids ? &ids_len : NULL);
    if (e) { free(ids); return (double)e; }
    if (ids) {
        int de = dbuf_dup(ids, ids_len, &o->buf, &o->len);
        free(ids);
        if (de) return (double)de;
    }
    return (double)n;
}

/* Returns 1 if *out holds a document (the pre-image), 0 if not, negative on
 * error. */
EMSCRIPTEN_KEEPALIVE int dcw_find_one_and_delete(dcw_out *o, dc_collection *c,
        const uint8_t *filter, int filter_len) {
    reset_out(o);
    int found = 0;
    int e = dc_find_one_and_delete(c, filter, (uint32_t)filter_len, &found, &o->buf, &o->len);
    if (e) return e;
    return found ? 1 : 0;
}

/* Returns 0 (no match, no upsert), 1 (matched and replaced), 2 (upserted),
 * or a negative error. */
/* `upserted_id_out` (12 writable bytes) receives the id an upsert
 * actually used, which is not always `default_id` -- see dc_replace_one.
 * The host reads it only when the return value is 2. */
EMSCRIPTEN_KEEPALIVE int dcw_replace_one(dc_collection *c,
        const uint8_t *filter, int filter_len,
        const uint8_t *replacement, int replacement_len,
        const uint8_t *default_id, int upsert, uint8_t *upserted_id_out) {
    int result = 0;
    int e = dc_replace_one(c, filter, (uint32_t)filter_len,
                           replacement, (uint32_t)replacement_len,
                           default_id, upsert, &result, upserted_id_out);
    if (e) return e;
    return result;
}

/* Returns 1 if *out holds a document (pre- or post-image per `return_new`),
 * 0 if not, negative on error -- see dc_find_one_and_replace's doc comment
 * for exactly when. */
EMSCRIPTEN_KEEPALIVE int dcw_find_one_and_replace(dcw_out *o, dc_collection *c,
        const uint8_t *filter, int filter_len,
        const uint8_t *replacement, int replacement_len,
        const uint8_t *default_id, int upsert, int return_new) {
    reset_out(o);
    int found = 0;
    int e = dc_find_one_and_replace(c, filter, (uint32_t)filter_len,
                                    replacement, (uint32_t)replacement_len,
                                    default_id, upsert, return_new, &found, &o->buf, &o->len);
    if (e) return e;
    return found ? 1 : 0;
}

/* Returns 0 (no match, no upsert), 1 (matched and updated), 2 (upserted),
 * or a negative error. */
/* `upserted_id_out`: see dcw_replace_one. */
EMSCRIPTEN_KEEPALIVE int dcw_update_one(dc_collection *c,
        const uint8_t *filter, int filter_len,
        const uint8_t *update, int update_len,
        const uint8_t *default_id, int upsert, uint8_t *upserted_id_out) {
    int result = 0;
    int e = dc_update_one(c, filter, (uint32_t)filter_len,
                          update, (uint32_t)update_len,
                          default_id, upsert, &result, upserted_id_out);
    if (e) return e;
    return result;
}

/* Returns 1 if *out holds a document (pre- or post-image per `return_new`),
 * 0 if not, negative on error -- see dc_find_one_and_update's doc comment
 * for exactly when. */
EMSCRIPTEN_KEEPALIVE int dcw_find_one_and_update(dcw_out *o, dc_collection *c,
        const uint8_t *filter, int filter_len,
        const uint8_t *update, int update_len,
        const uint8_t *default_id, int upsert, int return_new) {
    reset_out(o);
    int found = 0;
    int e = dc_find_one_and_update(c, filter, (uint32_t)filter_len,
                                   update, (uint32_t)update_len,
                                   default_id, upsert, return_new, &found, &o->buf, &o->len);
    if (e) return e;
    return found ? 1 : 0;
}

/* Writes a binjson OBJECT { matchedCount: number, upserted: bool } into the
 * out slot; matchedCount is 0 whenever upserted is true. Returns 0 on
 * success, negative on error. */
/* `want_images` asks for every post-image alongside the counts -- what a
 * change-stream consumer needs, and what the host otherwise had to
 * re-read one query per matched document. */
EMSCRIPTEN_KEEPALIVE int dcw_update_many(dcw_out *o, dc_collection *c,
        const uint8_t *filter, int filter_len,
        const uint8_t *update, int update_len,
        const uint8_t *default_id, int upsert, int want_images) {
    reset_out(o);
    int64_t matched = 0; int upserted = 0;
    uint8_t upserted_id[12];
    uint8_t *images = NULL; size_t images_len = 0;
    int e = dc_update_many(c, filter, (uint32_t)filter_len,
                           update, (uint32_t)update_len,
                           default_id, upsert, &matched, &upserted, upserted_id,
                           want_images ? &images : NULL,
                           want_images ? &images_len : NULL);
    if (e) { free(images); return e; }

    bj_builder *b = bj_builder_new();
    if (!b) { free(images); return BJ_ERR_OOM; }
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"matchedCount", 12);
    if (!e) e = bj_put_int(b, matched);
    if (!e) e = bj_put_key(b, (const uint8_t *)"upserted", 8);
    if (!e) e = bj_put_bool(b, upserted);
    /* The id the upsert actually used -- rides in the result object this
     * call already builds, rather than through an out-pointer the way the
     * single-document forms need. */
    if (!e && upserted) {
        e = bj_put_key(b, (const uint8_t *)"upsertedId", 10);
        if (!e) e = bj_put_oid(b, upserted_id);
    }
    if (!e && images) {
        e = bj_put_key(b, (const uint8_t *)"documents", 9);
        if (!e) e = bj_put_raw(b, images, (uint32_t)images_len);
    }
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, &o->buf, &o->len);
    }
    bj_builder_free(b);
    free(images);
    return e;
}

EMSCRIPTEN_KEEPALIVE double dcw_count(dc_collection *c, const uint8_t *filter, int filter_len) {
    int64_t n = 0;
    int e = dc_count(c, filter, (uint32_t)filter_len, &n);
    return e ? (double)e : (double)n;
}

/* The out slot receives {source: <string -- see dc_explain_source>,
 * index: <string|null>}, which is the driver-facing shape as it stands:
 * the kind's NAME is C's, so that this host and the server report one
 * plan the same way. Returns 0 on success, negative on error. */
EMSCRIPTEN_KEEPALIVE int dcw_explain(dcw_out *o, dc_collection *c,
        const uint8_t *filter, int filter_len) {
    reset_out(o);
    int kind = 0; uint8_t *name = NULL; size_t name_len = 0;
    int e = dc_explain(c, filter, (uint32_t)filter_len, &kind, &name, &name_len);
    if (e) return e;
    const char *source = dc_explain_source(kind);
    bj_builder *b = bj_builder_new();
    if (!b) { free(name); return BJ_ERR_OOM; }
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"source", 6);
    if (!e) e = bj_put_string(b, (const uint8_t *)source, (uint32_t)strlen(source));
    if (!e) e = bj_put_key(b, (const uint8_t *)"index", 5);
    if (!e) e = name ? bj_put_string(b, name, (uint32_t)name_len) : bj_put_null(b);
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(p, n, &o->buf, &o->len);
    }
    bj_builder_free(b);
    free(name);
    return e;
}

/* The out slot receives a binjson ARRAY of unique values. Returns 0 on
 * success, negative on error. */
EMSCRIPTEN_KEEPALIVE int dcw_distinct(dcw_out *o, dc_collection *c,
        const char *field, int field_len,
        const uint8_t *filter, int filter_len) {
    reset_out(o);
    return dc_distinct(c, field, field_len, filter, (uint32_t)filter_len, &o->buf, &o->len);
}

EMSCRIPTEN_KEEPALIVE const uint8_t *dcw_out_ptr(dcw_out *o) { return o->buf; }
/* Length of the slot's last output, or BJ_ERR_INT_RANGE if it cannot cross
 * the boundary as an int (>= 2 GB) instead of a silently truncated number. */
EMSCRIPTEN_KEEPALIVE int dcw_out_len(dcw_out *o) {
    return o->len > INT_MAX ? BJ_ERR_INT_RANGE : (int)o->len;
}
