/*
 * db_currentdate_wasm.c — Emscripten glue over upd_resolve_current_date.
 *
 * Separate from db_wasm.c because this is the one update-layer entry
 * point the host calls on its own, ahead of a write, rather than as part
 * of one: the WAL resolves $currentDate at proposal time so the logged
 * command carries a concrete Date rather than a rule that would read a
 * different clock on replay.
 */
#include "db_update.h"
#include "dbuf.h"

#include <limits.h>
#include <stdlib.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf buf; } cdw;

EMSCRIPTEN_KEEPALIVE cdw *cdw_new(void) { return (cdw *)calloc(1, sizeof(cdw)); }

EMSCRIPTEN_KEEPALIVE void cdw_free(cdw *c) {
    if (!c) return;
    dbuf_free(&c->buf);
    free(c);
}

EMSCRIPTEN_KEEPALIVE int cdw_resolve(cdw *c, const uint8_t *update, int len, double now_ms) {
    if (len < 0) return BJ_ERR_RANGE;
    c->buf.len = 0;
    return upd_resolve_current_date(update, (size_t)len, (int64_t)now_ms, &c->buf);
}

EMSCRIPTEN_KEEPALIVE const uint8_t *cdw_ptr(const cdw *c) { return c->buf.data; }

EMSCRIPTEN_KEEPALIVE int cdw_len(const cdw *c) {
    return c->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)c->buf.len;
}
