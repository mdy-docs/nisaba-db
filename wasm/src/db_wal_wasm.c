/*
 * db_wal_wasm.c — Emscripten glue over db_wal.h.
 *
 * The plan handle crosses the bridge as an opaque pointer and the host
 * reads it out one command at a time (walw_cmd_ptr/_len), rather than the
 * dcw_out convention of one concatenated result buffer. Deliberate: each
 * command becomes its OWN log entry, so the host needs the boundaries,
 * and a buffer of concatenated commands would only mean re-deriving the
 * spans on the far side of the bridge.
 *
 * Nothing here reads a clock or generates an id -- the host passes the
 * one id a plan might need (walw_plan's `default_id`), as everywhere
 * else in this codebase.
 */
#include "db_wal.h"

#include <stdlib.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

/*
 * Plan a request. Returns the plan pointer as a handle, or 0; the error
 * code, when there is one, lands in *rc_out (a plain int out-param
 * because a null handle alone cannot distinguish "OOM" from "malformed
 * update"). The caller must walw_plan_free the handle -- including when
 * the outcome is DC_PLAN_NOTHING, which is a successful plan of zero
 * commands, not a failure.
 */
EMSCRIPTEN_KEEPALIVE dc_wal_plan *walw_plan(dc_collection *c,
                                            const char *coll, int coll_len,
                                            int req,
                                            const uint8_t *a, int a_len,
                                            const uint8_t *b, int b_len,
                                            int upsert, const uint8_t *default_id,
                                            int *rc_out) {
    if (coll_len < 0 || a_len < 0 || b_len < 0) { *rc_out = BJ_ERR_RANGE; return NULL; }
    dc_wal_plan *p = NULL;
    *rc_out = dc_wal_plan_build(c, coll, (uint32_t)coll_len, req,
                          a, (uint32_t)a_len, b, (uint32_t)b_len,
                          upsert, default_id, &p);
    return p;
}

EMSCRIPTEN_KEEPALIVE void walw_plan_free(dc_wal_plan *p) { dc_wal_plan_free(p); }

EMSCRIPTEN_KEEPALIVE int walw_outcome(dc_wal_plan *p) { return dc_wal_plan_outcome(p); }
EMSCRIPTEN_KEEPALIVE int walw_count(dc_wal_plan *p) { return (int)dc_wal_plan_count(p); }

EMSCRIPTEN_KEEPALIVE const uint8_t *walw_cmd_ptr(dc_wal_plan *p, int i) {
    uint32_t len;
    return dc_wal_plan_cmd(p, (uint32_t)i, &len);
}
EMSCRIPTEN_KEEPALIVE int walw_cmd_len(dc_wal_plan *p, int i) {
    uint32_t len;
    dc_wal_plan_cmd(p, (uint32_t)i, &len);
    return (int)len;
}

EMSCRIPTEN_KEEPALIVE const uint8_t *walw_preimage_ptr(dc_wal_plan *p) {
    uint32_t len;
    return dc_wal_plan_preimage(p, &len);
}
EMSCRIPTEN_KEEPALIVE int walw_preimage_len(dc_wal_plan *p) {
    uint32_t len;
    dc_wal_plan_preimage(p, &len);
    return (int)len;
}

/* 12 raw id bytes, or 0 when the plan resolved no single document. */
EMSCRIPTEN_KEEPALIVE const uint8_t *walw_target_id(dc_wal_plan *p) {
    return dc_wal_plan_target_id(p);
}

/*
 * Validate a logged command and return its opcode (>= 0) or a negative
 * error.
 *
 * dc_wal_parse also reports the collection name, and this does not pass
 * it on: a JS host decodes the payload for the command's values anyway,
 * so it already has the name, and bridging a span it would ignore is
 * arity spent on nothing. The C-level span exists for a host that does
 * NOT decode -- the native applier Phase 7 needs.
 */
EMSCRIPTEN_KEEPALIVE int walw_parse(const uint8_t *buf, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    int op = -1;
    const uint8_t *coll; uint32_t coll_len;
    int e = dc_wal_parse(buf, (uint32_t)len, &op, &coll, &coll_len);
    return e ? e : op;
}

/*
 * Apply one logged command to an open collection.
 *
 * The result buffer is per-call and owned by the caller, not the usual
 * shared scratch: applies run inside the apply pump while other calls
 * (a follower's read, another collection's apply) can interleave between
 * this returning and the host reading it, and a shared slot would hand
 * back whichever landed last. The host frees it with walw_result_free.
 */
typedef struct { dbuf out; } walw_result;

EMSCRIPTEN_KEEPALIVE walw_result *walw_apply(dc_collection *c, double index,
                                             const uint8_t *cmd, int len, int *rc_out) {
    if (len < 0) { *rc_out = BJ_ERR_RANGE; return NULL; }
    walw_result *r = (walw_result *)calloc(1, sizeof(walw_result));
    if (!r) { *rc_out = BJ_ERR_OOM; return NULL; }
    int e = dc_wal_apply(c, (uint64_t)index, cmd, (uint32_t)len, &r->out);
    *rc_out = e;
    if (e) { dbuf_free(&r->out); free(r); return NULL; }
    return r;
}

EMSCRIPTEN_KEEPALIVE const uint8_t *walw_result_ptr(const walw_result *r) {
    return r ? r->out.data : NULL;
}
EMSCRIPTEN_KEEPALIVE int walw_result_len(const walw_result *r) {
    return r ? (int)r->out.len : 0;
}
EMSCRIPTEN_KEEPALIVE void walw_result_free(walw_result *r) {
    if (!r) return;
    dbuf_free(&r->out);
    free(r);
}

/* Does walw_apply drive this opcode, or is it the namespace owner's? */
EMSCRIPTEN_KEEPALIVE int walw_is_document(int op) { return dc_wal_is_document(op); }
