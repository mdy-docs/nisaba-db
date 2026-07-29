/*
 * db_catalog_wasm.c — Emscripten glue over db_catalog.h.
 *
 * Both entry points are pure: they read an encoded catalog entry and
 * produce an encoded result, touching no file. That is what makes
 * dc_catalog_open_plan usable from a host whose file opens are
 * asynchronous -- the plan is computed before any of them happen.
 */
#include "db_catalog.h"
#include "dbuf.h"

#include <limits.h>
#include <stdlib.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf buf; } catw;

EMSCRIPTEN_KEEPALIVE catw *catw_new(void) { return (catw *)calloc(1, sizeof(catw)); }

EMSCRIPTEN_KEEPALIVE void catw_free(catw *c) {
    if (!c) return;
    dbuf_free(&c->buf);
    free(c);
}

EMSCRIPTEN_KEEPALIVE int catw_open_plan(catw *w, const uint8_t *entry, int entry_len,
                                        const char *coll, int coll_len) {
    if (entry_len < 0 || coll_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_catalog_open_plan(entry, (size_t)entry_len, coll, (size_t)coll_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_list_indexes(catw *w, const uint8_t *entry, int entry_len) {
    if (entry_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_catalog_list_indexes(entry, (size_t)entry_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_put_index(catw *w, const uint8_t *entry, int entry_len,
                                        const uint8_t *def, int def_len) {
    if (entry_len < 0 || def_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_catalog_put_index(entry, (size_t)entry_len, def, (size_t)def_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_drop_index(catw *w, const uint8_t *entry, int entry_len,
                                         const char *name, int name_len) {
    if (entry_len < 0 || name_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_catalog_drop_index(entry, (size_t)entry_len, name, (size_t)name_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_create_plan(catw *w, const uint8_t *keys, int keys_len,
                                          const uint8_t *options, int options_len,
                                          const char *coll, int coll_len) {
    if (keys_len < 0 || options_len < 0 || coll_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_index_create_plan(keys, (size_t)keys_len, options, (size_t)options_len,
                                coll, (size_t)coll_len, &w->buf);
}

/* `names` is the directory listing as a NUL-separated buffer -- see
 * dc_sweep_plan and bjns.h on why a listing is an input rather than a
 * callback. */
EMSCRIPTEN_KEEPALIVE int catw_sweep_plan(catw *w, const uint8_t *catalog, int catalog_len,
                                         const char *names, int names_len) {
    if (catalog_len < 0 || names_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_sweep_plan(catalog, (size_t)catalog_len, names, (size_t)names_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_collection_files(catw *w, const uint8_t *entry, int entry_len,
                                               const char *coll, int coll_len) {
    if (entry_len < 0 || coll_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_collection_files(entry, (size_t)entry_len, coll, (size_t)coll_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_new_entry(catw *w, const char *coll, int coll_len) {
    if (coll_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_catalog_new_entry(coll, (size_t)coll_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_compact_plan(catw *w, const uint8_t *entry, int entry_len,
                                           const char *coll, int coll_len) {
    if (entry_len < 0 || coll_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_compact_plan(entry, (size_t)entry_len, coll, (size_t)coll_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE const uint8_t *catw_ptr(const catw *w) { return w->buf.data; }

EMSCRIPTEN_KEEPALIVE int catw_len(const catw *w) {
    return w->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)w->buf.len;
}
