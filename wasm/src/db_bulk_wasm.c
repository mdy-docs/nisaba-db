/*
 * db_bulk_wasm.c — Emscripten glue over db_bulk.h.
 *
 * The failing operation's index rides in the builder's own slot rather
 * than a caller-allocated scratch pointer: it is wanted only on the error
 * path, where there is no result to read anyway, so bkw_bad_index is a
 * plain accessor instead of an out-param JS has to malloc and free around
 * every successful call.
 */
#include "db_bulk.h"
#include "dbuf.h"

#include <limits.h>
#include <stdlib.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf buf; int bad_index; } bkw;

EMSCRIPTEN_KEEPALIVE bkw *bkw_new(void) { return (bkw *)calloc(1, sizeof(bkw)); }

EMSCRIPTEN_KEEPALIVE void bkw_free(bkw *k) {
    if (!k) return;
    dbuf_free(&k->buf);
    free(k);
}

EMSCRIPTEN_KEEPALIVE int bkw_parse(bkw *k, const uint8_t *ops, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    k->buf.len = 0;
    k->bad_index = -1;
    return dc_bulk_parse(ops, (size_t)len, &k->buf, &k->bad_index);
}

/* -1 when the last parse succeeded. */
EMSCRIPTEN_KEEPALIVE int bkw_bad_index(const bkw *k) { return k->bad_index; }

EMSCRIPTEN_KEEPALIVE const uint8_t *bkw_ptr(const bkw *k) { return k->buf.data; }

EMSCRIPTEN_KEEPALIVE int bkw_len(const bkw *k) {
    return k->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)k->buf.len;
}
