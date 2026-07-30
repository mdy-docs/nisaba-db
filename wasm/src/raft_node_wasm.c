/*
 * raft_node_wasm.c — Emscripten glue over raft_node.h.
 *
 * Almost everything crosses as scalars, because almost everything IS a
 * scalar: an id, an index, a correlation number. The two exceptions are
 * the member set going in and the outbox bytes coming out, and both are
 * already byte buffers on the other side of the bridge.
 *
 * There is no context object here. raft_node is itself the handle, and
 * the outbox lives inside it -- so unlike rcw/rmw, the host holds one
 * pointer per node rather than one shared scratch. That is not
 * incidental: a shared scratch would make two nodes in one process (the
 * simulated cluster every Raft test runs) trample each other.
 *
 * Indices and terms become doubles, exact to 2^53 -- the ceiling
 * entrylog.h's glue has always had. raft_node.h's uint64_t is what a
 * native host gets.
 */
#include "raft_node.h"

#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

/* ---- lifecycle ---------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE raft_node *rnw_new(double self_id, elog *log) {
    return rn_new((uint64_t)self_id, log);
}
EMSCRIPTEN_KEEPALIVE void rnw_free(raft_node *n) { rn_free(n); }
EMSCRIPTEN_KEEPALIVE void rnw_set_log(raft_node *n, elog *log) { rn_set_log(n, log); }

EMSCRIPTEN_KEEPALIVE int rnw_set_members(raft_node *n, const uint8_t *members, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    return rn_set_members(n, members, (uint32_t)len);
}

EMSCRIPTEN_KEEPALIVE int rnw_max_peers(void) { return (int)rn_max_peers(); }

EMSCRIPTEN_KEEPALIVE void rnw_set_timing(raft_node *n, double min_election,
                                         double max_election, double heartbeat) {
    rn_set_timing(n, (int64_t)min_election, (int64_t)max_election, (int64_t)heartbeat);
}
EMSCRIPTEN_KEEPALIVE void rnw_set_limits(raft_node *n, int max_batch_bytes) {
    rn_set_limits(n, max_batch_bytes < 0 ? 0 : (uint32_t)max_batch_bytes);
}

EMSCRIPTEN_KEEPALIVE void rnw_start(raft_node *n, double now, double random01) {
    rn_start(n, (int64_t)now, random01);
}
EMSCRIPTEN_KEEPALIVE void rnw_stop(raft_node *n) { rn_stop(n); }

/* ---- clock -------------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE int rnw_tick(raft_node *n, double now, double random01) {
    return rn_tick(n, (int64_t)now, random01);
}
EMSCRIPTEN_KEEPALIVE void rnw_quiesce(raft_node *n) { rn_quiesce(n); }
EMSCRIPTEN_KEEPALIVE void rnw_wake(raft_node *n, double now, double random01) {
    rn_wake(n, (int64_t)now, random01);
}

/* ---- messages ----------------------------------------------------------- */

/* Correlation ids cross as doubles, exact to 2^53 -- the ceiling every
 * index in this layer already has. As ints they capped at 2^31, where
 * the negative guard below would have started refusing every message a
 * long-lived node sent. */
EMSCRIPTEN_KEEPALIVE int rnw_handle(raft_node *n, double corr,
                                    const uint8_t *msg, int len, double random01) {
    if (len < 0 || corr < 0) return BJ_ERR_RANGE;
    return rn_handle(n, (uint64_t)corr, msg, (uint32_t)len, random01);
}

EMSCRIPTEN_KEEPALIVE int rnw_on_reply(raft_node *n, double corr,
                                      const uint8_t *reply, int len, double random01) {
    if (len < 0 || corr < 0) return BJ_ERR_RANGE;
    return rn_on_reply(n, (uint64_t)corr, reply, (uint32_t)len, random01);
}

EMSCRIPTEN_KEEPALIVE int rnw_on_fail(raft_node *n, double corr) {
    if (corr < 0) return BJ_ERR_RANGE;
    return rn_on_fail(n, (uint64_t)corr);
}

/* ---- outbox ------------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE int    rnw_out_count(const raft_node *n) { return (int)rn_out_count(n); }
EMSCRIPTEN_KEEPALIVE double rnw_out_peer(const raft_node *n, int i) {
    return (double)rn_out_peer(n, (uint32_t)i);
}
EMSCRIPTEN_KEEPALIVE double rnw_out_corr(const raft_node *n, int i) {
    return (double)rn_out_corr(n, (uint32_t)i);
}
EMSCRIPTEN_KEEPALIVE int rnw_out_is_reply(const raft_node *n, int i) {
    return rn_out_is_reply(n, (uint32_t)i);
}
EMSCRIPTEN_KEEPALIVE const uint8_t *rnw_out_ptr(const raft_node *n, int i) {
    uint32_t len = 0;
    return rn_out_bytes(n, (uint32_t)i, &len);
}
EMSCRIPTEN_KEEPALIVE int rnw_out_len(const raft_node *n, int i) {
    uint32_t len = 0;
    rn_out_bytes(n, (uint32_t)i, &len);
    return (int)len;
}
EMSCRIPTEN_KEEPALIVE void rnw_out_clear(raft_node *n) { rn_out_clear(n); }

/* ---- effects ------------------------------------------------------------ */

EMSCRIPTEN_KEEPALIVE int rnw_effect_count(const raft_node *n) {
    return (int)rn_effect_count(n);
}
EMSCRIPTEN_KEEPALIVE int rnw_effect_kind(const raft_node *n, int i) {
    return rn_effect_kind_at(n, (uint32_t)i);
}
EMSCRIPTEN_KEEPALIVE double rnw_effect_arg(const raft_node *n, int i) {
    return (double)rn_effect_arg(n, (uint32_t)i);
}
EMSCRIPTEN_KEEPALIVE int rnw_effect_flag(const raft_node *n, int i) {
    return rn_effect_flag(n, (uint32_t)i);
}
EMSCRIPTEN_KEEPALIVE void rnw_effects_clear(raft_node *n) { rn_effects_clear(n); }
EMSCRIPTEN_KEEPALIVE int rnw_effects_lost(const raft_node *n) { return rn_effects_lost(n); }

/* ---- accessors ---------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE int    rnw_role(const raft_node *n) { return rn_role(n); }
EMSCRIPTEN_KEEPALIVE double rnw_leader_id(const raft_node *n) { return (double)rn_leader_id(n); }
EMSCRIPTEN_KEEPALIVE double rnw_commit_index(const raft_node *n) {
    return (double)rn_commit_index(n);
}
EMSCRIPTEN_KEEPALIVE double rnw_match(const raft_node *n, double peer) {
    return (double)rn_match(n, (uint64_t)peer);
}
EMSCRIPTEN_KEEPALIVE double rnw_next(const raft_node *n, double peer) {
    return (double)rn_next(n, (uint64_t)peer);
}
EMSCRIPTEN_KEEPALIVE double rnw_inflight(const raft_node *n, double peer) {
    return (double)rn_inflight(n, (uint64_t)peer);
}
EMSCRIPTEN_KEEPALIVE int rnw_quorum(const raft_node *n) { return (int)rn_quorum(n); }
EMSCRIPTEN_KEEPALIVE int rnw_is_quiesced(const raft_node *n) { return rn_is_quiesced(n); }
EMSCRIPTEN_KEEPALIVE int rnw_has_quorum_contact(const raft_node *n, double within_ms) {
    return rn_has_quorum_contact(n, (int64_t)within_ms);
}
EMSCRIPTEN_KEEPALIVE int rnw_replicate(raft_node *n, double peer) {
    return rn_replicate(n, (uint64_t)peer);
}
EMSCRIPTEN_KEEPALIVE int rnw_installed(raft_node *n, double peer, double boundary) {
    return rn_installed(n, (uint64_t)peer, (uint64_t)boundary);
}

/* ---- what the host still owns ------------------------------------------- */

/* The index the entry landed at comes back through `out` (one f64 slot),
 * because the return value is the error code. */
EMSCRIPTEN_KEEPALIVE int rnw_propose(raft_node *n, int type, const uint8_t *payload,
                                     int len, double *out) {
    if (len < 0) return BJ_ERR_RANGE;
    uint64_t at = 0;
    int e = rn_propose(n, type, payload, (uint32_t)len, &at);
    if (out) *out = (double)at;
    return e;
}

EMSCRIPTEN_KEEPALIVE void rnw_seed_commit(raft_node *n, double index) {
    rn_seed_commit(n, (uint64_t)index);
}
EMSCRIPTEN_KEEPALIVE int rnw_campaign(raft_node *n, double random01) {
    return rn_campaign(n, random01);
}
EMSCRIPTEN_KEEPALIVE int rnw_observe_leader(raft_node *n, double term, double leader_id,
                                            double random01) {
    return rn_observe_leader(n, (uint64_t)term, (uint64_t)leader_id, random01);
}
EMSCRIPTEN_KEEPALIVE int rnw_step_down(raft_node *n, double term, double random01) {
    return rn_step_down(n, (uint64_t)term, random01);
}
