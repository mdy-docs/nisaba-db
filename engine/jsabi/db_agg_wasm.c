/*
 * db_agg_wasm.c — Emscripten glue over db_agg.h.
 *
 * One call runs the whole pipeline, including the leading-$match
 * pushdown, so JS no longer decides which stage the engine gets to serve.
 * The result lands in the collection's existing dcw_out slot, like every
 * other result-producing collection call.
 */
#include "db_agg.h"
#include "db.h"

#include <limits.h>
#include <stdlib.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

/* Mirrors the dcw_out layout in db_wasm.c -- same slot, same accessors
 * (dcw_out_ptr / dcw_out_len), so JS reads an aggregate result exactly as
 * it reads a find result. */
typedef struct { uint8_t *buf; size_t len; } dcw_out;

/* `bad_stage` is a caller-allocated 4-byte slot, the same out-param shape
 * dcw_cursor_open's error slot uses. On a stage error it receives that
 * stage's index; the caller holds the pipeline and can name it. */
EMSCRIPTEN_KEEPALIVE int dcw_aggregate(dcw_out *o, dc_collection *c,
                                       const uint8_t *stages, int stages_len,
                                       int *bad_stage) {
    if (stages_len < 0) return BJ_ERR_RANGE;
    free(o->buf);
    o->buf = NULL;
    o->len = 0;
    return dc_aggregate(c, stages, (size_t)stages_len, bad_stage, &o->buf, &o->len);
}
