/*
 * db_ttl_wasm.c — Emscripten glue over db_ttl.h.
 *
 * One call does both steps, because a caller never wants the cutoff
 * without the filter: JS passes the index's field and expireAfterSeconds
 * plus its own clock reading, and gets back the encoded filter to hand to
 * deleteMany (directly, or through the WAL's logged path).
 *
 * now_ms and the cutoff cross as doubles -- the standing JS-bridge
 * convention for epoch milliseconds, which stay exact well past any date
 * a TTL index will see (2^53 ms is year 285616). The internal API in
 * db_ttl.h uses int64_t, as everything below the bridge does.
 */
#include "db_ttl.h"
#include "dbuf.h"

#include <limits.h>
#include <stdlib.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf buf; } ttlw;

EMSCRIPTEN_KEEPALIVE ttlw *ttlw_new(void) { return (ttlw *)calloc(1, sizeof(ttlw)); }

EMSCRIPTEN_KEEPALIVE void ttlw_free(ttlw *t) {
    if (!t) return;
    dbuf_free(&t->buf);
    free(t);
}

EMSCRIPTEN_KEEPALIVE int ttlw_filter(ttlw *t, const char *field, int field_len,
                                     double now_ms, double expire_after_seconds) {
    if (field_len < 0) return BJ_ERR_RANGE;
    t->buf.len = 0;
    int64_t cutoff;
    int e = dc_ttl_cutoff_ms((int64_t)now_ms, expire_after_seconds, &cutoff);
    if (e) return e;
    return dc_ttl_filter(&t->buf, field, (size_t)field_len, cutoff);
}

EMSCRIPTEN_KEEPALIVE const uint8_t *ttlw_ptr(const ttlw *t) { return t->buf.data; }

EMSCRIPTEN_KEEPALIVE int ttlw_len(const ttlw *t) {
    return t->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)t->buf.len;
}
