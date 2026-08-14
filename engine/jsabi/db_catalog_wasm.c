/*
 * db_catalog_wasm.c — Emscripten glue over db_catalog.h.
 *
 * Both entry points are pure: they read an encoded catalog entry and
 * produce an encoded result, touching no file. That is what makes
 * dc_catalog_open_plan usable from a host whose file opens are
 * asynchronous -- the plan is computed before any of them happen.
 */
#include "db_catalog.h"
#include "binjson.h"
#include "bjns_bridge.h"
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

/* The staged-build fields of one stored definition (db_catalog.h).
 * `set` rewrites the entry; `get` answers {found, building, cursor?} --
 * both pure buffer transforms, like everything else in this file. */
EMSCRIPTEN_KEEPALIVE int catw_index_building_set(catw *w, const uint8_t *entry, int entry_len,
                                                 const char *name, int name_len,
                                                 int building,
                                                 const uint8_t *cursor, int cursor_len) {
    if (entry_len < 0 || name_len < 0 || cursor_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    return dc_catalog_index_building_set(entry, (size_t)entry_len, name, (size_t)name_len,
                                         building, cursor, (uint32_t)cursor_len, &w->buf);
}

EMSCRIPTEN_KEEPALIVE int catw_index_building_get(catw *w, const uint8_t *entry, int entry_len,
                                                 const char *name, int name_len) {
    if (entry_len < 0 || name_len < 0) return BJ_ERR_RANGE;
    w->buf.len = 0;
    int found = 0, building = 0, has_cursor = 0;
    dbuf cursor = {0};
    int e = dc_catalog_index_building_get(entry, (size_t)entry_len, name, (size_t)name_len,
                                          &found, &building, &cursor, &has_cursor);
    if (e) { dbuf_free(&cursor); return e; }
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"found", 5);
    if (!e) e = bj_put_bool(b, found);
    if (!e) e = bj_put_key(b, (const uint8_t *)"building", 8);
    if (!e) e = bj_put_bool(b, building);
    if (!e && has_cursor) {
        e = bj_put_key(b, (const uint8_t *)"cursor", 6);
        if (!e) e = bj_put_raw(b, cursor.data, (uint32_t)cursor.len);
    }
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        e = d ? dbuf_put(&w->buf, d, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    dbuf_free(&cursor);
    return e;
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

/*
 * Sweep through the browser's bj_ns adapter. `scope` identifies the
 * host's pre-opened-name table; removals are queued into
 * Module.bjnsPending[scope] for the host to drain once this returns,
 * because OPFS cannot unlink synchronously.
 *
 * This is the first call to drive a bj_ns rather than return names, so it
 * is also what proves the adapter works at all.
 */
EMSCRIPTEN_KEEPALIVE int catw_sweep_execute(int scope,
                                            const uint8_t *catalog, int catalog_len,
                                            const char *names, int names_len) {
    if (catalog_len < 0 || names_len < 0) return BJ_ERR_RANGE;
    bj_ns ns;
    int e = bjns_bridge_open(scope, &ns);
    if (e) return e;
    uint32_t deleted = 0;
    e = dc_sweep_execute(&ns, catalog, (size_t)catalog_len, names, (size_t)names_len, &deleted);
    bjns_bridge_free(&ns);
    return e ? e : (int)deleted;
}

/*
 * Build and flip, through the browser's namespace adapter. `sources` and
 * `kinds` are heap arrays of `nsources` i32s the caller filled with the
 * live structures' context pointers and their kinds, in the plan's build
 * order.
 *
 * Returns the bytes built (>= 0) or a negative error code -- the count
 * doubles as the success value because the caller wants it for the
 * growth baseline and there is nothing else to report.
 */
EMSCRIPTEN_KEEPALIVE double catw_compact_execute(int scope, bpt *catalog,
                                                 const char *coll, int coll_len,
                                                 const uint8_t *plan, int plan_len,
                                                 void *const *sources,
                                                 const int *kinds, int nsources) {
    if (coll_len < 0 || plan_len < 0 || nsources < 0) return BJ_ERR_RANGE;
    bj_ns ns;
    int e = bjns_bridge_open(scope, &ns);
    if (e) return e;
    uint64_t built = 0;
    e = dc_compact_execute(&ns, catalog, coll, (size_t)coll_len,
                           plan, (size_t)plan_len, sources, kinds,
                           (uint32_t)nsources, &built);
    bjns_bridge_free(&ns);
    return e ? (double)e : (double)built;
}

/*
 * The format-v2 migration, through the same adapter and the same plan:
 * dc_migrate_execute IS dc_compact_execute with the primary copy re-keyed
 * from each document's _id (docs/format-compatibility.md). A browser
 * holding v1 databases in OPFS has no dump/restore escape hatch and no
 * server to migrate them for it, so the in-place path has to exist on
 * this side of the bridge too.
 */
EMSCRIPTEN_KEEPALIVE double catw_migrate_execute(int scope, bpt *catalog,
                                                 const char *coll, int coll_len,
                                                 const uint8_t *plan, int plan_len,
                                                 void *const *sources,
                                                 const int *kinds, int nsources) {
    if (coll_len < 0 || plan_len < 0 || nsources < 0) return BJ_ERR_RANGE;
    bj_ns ns;
    int e = bjns_bridge_open(scope, &ns);
    if (e) return e;
    uint64_t built = 0;
    e = dc_migrate_execute(&ns, catalog, coll, (size_t)coll_len,
                           plan, (size_t)plan_len, sources, kinds,
                           (uint32_t)nsources, &built);
    bjns_bridge_free(&ns);
    return e ? (double)e : (double)built;
}

EMSCRIPTEN_KEEPALIVE const uint8_t *catw_ptr(const catw *w) { return w->buf.data; }

EMSCRIPTEN_KEEPALIVE int catw_len(const catw *w) {
    return w->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)w->buf.len;
}
