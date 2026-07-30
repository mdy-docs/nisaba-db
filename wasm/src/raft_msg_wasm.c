/*
 * raft_msg_wasm.c — Emscripten glue over raft_msg.h.
 *
 * One context carries two buffers: the reply the handler produced and
 * the effects the caller must adopt. Two, not one, because the reply
 * goes back on the wire byte-for-byte while the effects are read as
 * fields -- concatenating them would only mean splitting them again.
 *
 * The message crosses as the raw bytes it arrived as. That is the point
 * of the whole file: an AppendEntries batch reaches the conflict rule
 * without ever being decoded into JavaScript objects and re-encoded.
 */
#include "raft_msg.h"

#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf reply; dbuf eff; } rmw;

EMSCRIPTEN_KEEPALIVE rmw *rmw_new(void) { return (rmw *)calloc(1, sizeof(rmw)); }
EMSCRIPTEN_KEEPALIVE void rmw_free(rmw *w) {
    if (!w) return;
    dbuf_free(&w->reply);
    dbuf_free(&w->eff);
    free(w);
}
EMSCRIPTEN_KEEPALIVE const uint8_t *rmw_reply_ptr(rmw *w) { return w->reply.data; }
EMSCRIPTEN_KEEPALIVE int rmw_reply_len(rmw *w) { return (int)w->reply.len; }
EMSCRIPTEN_KEEPALIVE const uint8_t *rmw_eff_ptr(rmw *w) { return w->eff.data; }
EMSCRIPTEN_KEEPALIVE int rmw_eff_len(rmw *w) { return (int)w->eff.len; }

EMSCRIPTEN_KEEPALIVE int rmw_kind(const uint8_t *msg, int len) {
    if (len < 0) return BJ_ERR_RANGE;
    int kind = -1;
    int e = rmsg_kind(msg, (uint32_t)len, &kind);
    return e ? e : kind;
}

static int put_key(bj_builder *b, const char *k) {
    return bj_put_key(b, (const uint8_t *)k, (uint32_t)strlen(k));
}

static int encode_effect(rmw *w, const raft_msg_effect *eff) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    put_key(b, "becameFollower");   bj_put_bool(b, eff->became_follower);
    put_key(b, "leaderId");         bj_put_int(b, (int64_t)eff->new_leader_id);
    put_key(b, "commitIndex");      bj_put_int(b, (int64_t)eff->new_commit_index);
    put_key(b, "touchedLeader");    bj_put_bool(b, eff->touched_leader);
    put_key(b, "lastLeaderContact"); bj_put_int(b, eff->new_last_leader_contact);
    put_key(b, "resetTimer");       bj_put_bool(b, eff->reset_election_timer);
    put_key(b, "quiesce");          bj_put_bool(b, eff->quiesce);
    put_key(b, "grantedVote");      bj_put_bool(b, eff->granted_vote);
    put_key(b, "truncatedFrom");    bj_put_int(b, (int64_t)eff->truncated_from);
    put_key(b, "matchIndex");       bj_put_int(b, (int64_t)eff->match_index);
    put_key(b, "success");          bj_put_bool(b, eff->success);
    bj_end_object(b);

    int e = bj_builder_error(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = BJ_ERR_STATE;
        else e = dbuf_put(&w->eff, p, n);
    }
    bj_builder_free(b);
    return e;
}

/*
 * `state` is the caller's volatile node state, packed as f64s in the
 * order below -- a fixed layout rather than a binjson object because it
 * is nine numbers on the per-heartbeat path, and the JS side names the
 * slots in one place.
 *
 *   0 selfId  1 isFollower  2 isLeader  3 selfIsVoter  4 leaderId
 *   5 commitIndex  6 now  7 lastLeaderContact  8 minElectionTimeout
 */
static void unpack(const double *s, raft_msg_state *st) {
    st->self_id             = (uint64_t)s[0];
    st->is_follower         = s[1] != 0;
    st->is_leader           = s[2] != 0;
    st->self_is_voter       = s[3] != 0;
    st->leader_id           = (uint64_t)s[4];
    st->commit_index        = (uint64_t)s[5];
    st->now                 = (int64_t)s[6];
    st->last_leader_contact = (int64_t)s[7];
    st->min_election_timeout = (int64_t)s[8];
}

EMSCRIPTEN_KEEPALIVE int rmw_request_vote(rmw *w, elog *log, const double *state,
                                          const uint8_t *msg, int len) {
    w->reply.len = 0;
    w->eff.len = 0;
    if (len < 0) return BJ_ERR_RANGE;
    raft_msg_state st;
    unpack(state, &st);
    raft_msg_effect eff;
    int e = rmsg_handle_request_vote(log, &st, msg, (uint32_t)len, &eff, &w->reply);
    if (e) return e;
    return encode_effect(w, &eff);
}

EMSCRIPTEN_KEEPALIVE int rmw_append_entries(rmw *w, elog *log, const double *state,
                                            const uint8_t *msg, int len) {
    w->reply.len = 0;
    w->eff.len = 0;
    if (len < 0) return BJ_ERR_RANGE;
    raft_msg_state st;
    unpack(state, &st);
    raft_msg_effect eff;
    int e = rmsg_handle_append_entries(log, &st, msg, (uint32_t)len, &eff, &w->reply);
    if (e) return e;
    return encode_effect(w, &eff);
}

EMSCRIPTEN_KEEPALIVE int rmw_build_request_vote(rmw *w, double term, double candidate_id,
                                                double last_log_index, double last_log_term,
                                                int pre_vote) {
    w->reply.len = 0;
    return rmsg_build_request_vote((uint64_t)term, (uint64_t)candidate_id,
                                   (uint64_t)last_log_index, (uint64_t)last_log_term,
                                   pre_vote, &w->reply);
}

/* The entry count lands in the effect buffer (as `matchIndex`, which is
 * what the leader will do with it) so one call answers both questions. */
EMSCRIPTEN_KEEPALIVE int rmw_build_append_entries(rmw *w, elog *log, double term,
                                                  double leader_id, double next_index,
                                                  double prev_log_term, double leader_commit,
                                                  int max_bytes, int quiesce) {
    w->reply.len = 0;
    w->eff.len = 0;
    if (max_bytes < 0) return BJ_ERR_RANGE;
    uint32_t count = 0;
    int e = rmsg_build_append_entries(log, (uint64_t)term, (uint64_t)leader_id,
                                      (uint64_t)next_index, (uint64_t)prev_log_term,
                                      (uint64_t)leader_commit, (size_t)max_bytes,
                                      quiesce, &count, &w->reply);
    if (e) return e;
    raft_msg_effect eff;
    memset(&eff, 0, sizeof(eff));
    eff.match_index = (uint64_t)next_index - 1 + count;
    return encode_effect(w, &eff);
}
