/*
 * db_names_wasm.c — Emscripten glue over db_names.h.
 *
 * One reused buffer per module (the JS side allocates a single dnw and
 * keeps it), same shape and same reasoning as keyenc_wasm.c's qkw and the
 * dcw_out slot: a name is built by appending parts, and rebuilding it into
 * a buffer whose capacity survives beats a malloc per name.
 *
 * The constants are handed back through the same buffer rather than as C
 * string pointers, so JS needs no NUL-scanning helper and there is exactly
 * one way to read a result out of this file.
 *
 * Memory: heap growth may swap HEAPU8's ArrayBuffer, so JS must re-read
 * HEAPU8 after any call before touching a pointer returned by dnw_ptr.
 */
#include "db_names.h"
#include "dbuf.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf buf; } dnw;

EMSCRIPTEN_KEEPALIVE dnw *dnw_new(void) { return (dnw *)calloc(1, sizeof(dnw)); }

EMSCRIPTEN_KEEPALIVE void dnw_free(dnw *d) {
    if (!d) return;
    dbuf_free(&d->buf);
    free(d);
}

static int reset(dnw *d) { d->buf.len = 0; return BJ_OK; }

EMSCRIPTEN_KEEPALIVE int dnw_collection_file(dnw *d, const char *coll, int coll_len, int gen) {
    if (coll_len < 0 || gen < 0) return BJ_ERR_RANGE;
    reset(d);
    return dc_collection_file_name(&d->buf, coll, (size_t)coll_len, (uint32_t)gen);
}

EMSCRIPTEN_KEEPALIVE int dnw_index_file(dnw *d, const char *coll, int coll_len,
                                        const char *idx, int idx_len, int gen) {
    if (coll_len < 0 || idx_len < 0 || gen < 0) return BJ_ERR_RANGE;
    reset(d);
    return dc_index_file_name(&d->buf, coll, (size_t)coll_len, idx, (size_t)idx_len, (uint32_t)gen);
}

EMSCRIPTEN_KEEPALIVE int dnw_text_index_file(dnw *d, const char *coll, int coll_len,
                                             const char *idx, int idx_len, int gen, int role) {
    if (coll_len < 0 || idx_len < 0 || gen < 0) return BJ_ERR_RANGE;
    if (role < DC_TEXT_ROLE_TERMS || role > DC_TEXT_ROLE_LENGTHS) return BJ_ERR_RANGE;
    reset(d);
    return dc_text_index_file_name(&d->buf, coll, (size_t)coll_len, idx, (size_t)idx_len,
                                   (uint32_t)gen, (dc_text_role)role);
}

EMSCRIPTEN_KEEPALIVE int dnw_journal_file(dnw *d, const char *coll, int coll_len, int gen) {
    if (coll_len < 0 || gen < 0) return BJ_ERR_RANGE;
    reset(d);
    return dc_journal_file_name(&d->buf, coll, (size_t)coll_len, (uint32_t)gen);
}

EMSCRIPTEN_KEEPALIVE int dnw_catalog_file(dnw *d) {
    reset(d);
    return dbuf_put(&d->buf, (const uint8_t *)DC_CATALOG_FILE, strlen(DC_CATALOG_FILE));
}

EMSCRIPTEN_KEEPALIVE int dnw_format_key(dnw *d) {
    reset(d);
    return dbuf_put(&d->buf, (const uint8_t *)DC_FORMAT_KEY, strlen(DC_FORMAT_KEY));
}

EMSCRIPTEN_KEEPALIVE int dnw_format_version(void) { return DC_FORMAT_VERSION; }

EMSCRIPTEN_KEEPALIVE int dnw_is_db_file(const char *name, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    return dc_is_db_file(name, (size_t)len);
}

EMSCRIPTEN_KEEPALIVE const uint8_t *dnw_ptr(const dnw *d) { return d->buf.data; }

EMSCRIPTEN_KEEPALIVE int dnw_len(const dnw *d) {
    return d->buf.len > (size_t)INT_MAX ? BJ_ERR_INT_RANGE : (int)d->buf.len;
}
