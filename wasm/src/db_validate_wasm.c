/*
 * db_validate_wasm.c — Emscripten glue over db_validate.h.
 *
 * dvw_strerror returns a pointer to static C storage, so unlike the name
 * and key-spec calls it needs no out slot -- but JS has no NUL-scanning
 * helper (only HEAPU8 is exported), so dvw_strerror_len reports the
 * length and JS slices exactly that. Two calls, no scanning, no
 * allocation.
 */
#include "db_validate.h"
#include "dbuf.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

EMSCRIPTEN_KEEPALIVE const char *dvw_strerror(int code) { return dc_strerror(code); }

/* Is this a deterministic command failure (a result every replica
 * computes) rather than divergence (this replica alone failing)? See
 * db_validate.h -- a replicated apply loop rests on the distinction. */
EMSCRIPTEN_KEEPALIVE int dvw_is_deterministic(int code) { return dc_is_deterministic(code); }

EMSCRIPTEN_KEEPALIVE int dvw_strerror_len(int code) {
    return (int)strlen(dc_strerror(code));
}

EMSCRIPTEN_KEEPALIVE int dvw_check_collection_name(const char *name, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    return dc_check_collection_name(name, (size_t)len);
}

EMSCRIPTEN_KEEPALIVE int dvw_check_db_name(const char *name, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    return dc_check_db_name(name, (size_t)len);
}

/* The key-spec check also produces the field-name array, so it needs an
 * out slot. One per module, reused, like every other builder here. */
typedef struct { dbuf buf; } dvw;

EMSCRIPTEN_KEEPALIVE dvw *dvw_new(void) { return (dvw *)calloc(1, sizeof(dvw)); }

EMSCRIPTEN_KEEPALIVE void dvw_free(dvw *v) {
    if (!v) return;
    dbuf_free(&v->buf);
    free(v);
}

EMSCRIPTEN_KEEPALIVE int dvw_check_index_key_spec(dvw *v, const uint8_t *spec, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    v->buf.len = 0;
    return dc_check_index_key_spec(spec, (size_t)len, &v->buf);
}

EMSCRIPTEN_KEEPALIVE const uint8_t *dvw_ptr(const dvw *v) { return v->buf.data; }

EMSCRIPTEN_KEEPALIVE int dvw_len(const dvw *v) {
    return v->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)v->buf.len;
}
