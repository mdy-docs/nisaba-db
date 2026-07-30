/*
 * raft_core_wasm.c — Emscripten glue over raft_core.h.
 *
 * The two big decisions (vote, append) cross as binjson objects rather
 * than long argument lists: raft_vote_in has fifteen fields, and a
 * positional bridge for fifteen numbers is a transposition bug waiting
 * for its moment. Everything smaller crosses as scalars.
 *
 * Indices and terms become doubles here -- exact to 2^53, the ceiling
 * entrylog.h's own glue has always had. raft_core.h's uint64_t is what a
 * native host gets; this is where the browser's limit lives.
 *
 * Memory: heap growth may swap HEAPU8's ArrayBuffer, so JS must re-read
 * HEAPU8 after any call before touching a returned pointer.
 */
#include "raft_core.h"
#include "bjcursor.h"

#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

typedef struct { dbuf out; } rcw;

EMSCRIPTEN_KEEPALIVE rcw *rcw_new(void) { return (rcw *)calloc(1, sizeof(rcw)); }
EMSCRIPTEN_KEEPALIVE void rcw_free(rcw *w) {
    if (!w) return;
    dbuf_free(&w->out);
    free(w);
}
EMSCRIPTEN_KEEPALIVE const uint8_t *rcw_out_ptr(rcw *w) { return w->out.data; }
EMSCRIPTEN_KEEPALIVE int rcw_out_len(rcw *w) { return (int)w->out.len; }

/* ---- reading the decision inputs --------------------------------------- */

static uint64_t u64_field(const uint8_t *o, size_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found) != BJ_OK)
        return 0;
    if (!found) return 0;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) != BJ_OK || d < 0) return 0;
    return (uint64_t)d;
}

static int64_t i64_field(const uint8_t *o, size_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found) != BJ_OK)
        return 0;
    if (!found) return 0;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) != BJ_OK) return 0;
    return (int64_t)d;
}

/* Absent or false is 0; anything else is 1. */
static int flag_field(const uint8_t *o, size_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found) != BJ_OK)
        return 0;
    if (!found || vlen < 1) return 0;
    if (v[0] == BJ_TYPE_TRUE) return 1;
    if (v[0] == BJ_TYPE_FALSE || v[0] == BJ_TYPE_NULL) return 0;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) == BJ_OK) return d != 0;
    return 1;
}

static int emit(rcw *w, bj_builder *b) {
    int e = bj_builder_error(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = BJ_ERR_STATE;
        else e = dbuf_put(&w->out, p, n);
    }
    bj_builder_free(b);
    return e;
}

/* ---- decisions ---------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE int rcw_decide_vote(rcw *w, const uint8_t *in, int len) {
    w->out.len = 0;
    if (len < 0) return BJ_ERR_RANGE;
    size_t n = (size_t)len;

    raft_vote_in v;
    memset(&v, 0, sizeof(v));
    v.msg_term            = u64_field(in, n, "msgTerm");
    v.candidate_id        = u64_field(in, n, "candidateId");
    v.last_log_index      = u64_field(in, n, "lastLogIndex");
    v.last_log_term       = u64_field(in, n, "lastLogTerm");
    v.pre_vote            = flag_field(in, n, "preVote");
    v.current_term        = u64_field(in, n, "currentTerm");
    v.voted_for           = u64_field(in, n, "votedFor");
    v.our_last_index      = u64_field(in, n, "ourLastIndex");
    v.our_last_term       = u64_field(in, n, "ourLastTerm");
    v.self_is_voter       = flag_field(in, n, "selfIsVoter");
    v.is_leader           = flag_field(in, n, "isLeader");
    v.leader_id           = u64_field(in, n, "leaderId");
    v.now                 = i64_field(in, n, "now");
    v.last_leader_contact = i64_field(in, n, "lastLeaderContact");
    v.min_election_timeout = i64_field(in, n, "minElectionTimeout");

    raft_vote_out o;
    raft_decide_vote(&v, &o);

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"grant", 5);            bj_put_bool(b, o.grant);
    bj_put_key(b, (const uint8_t *)"replyTerm", 9);        bj_put_int(b, (int64_t)o.reply_term);
    bj_put_key(b, (const uint8_t *)"stepDown", 8);         bj_put_bool(b, o.step_down);
    bj_put_key(b, (const uint8_t *)"stepDownTerm", 12);    bj_put_int(b, (int64_t)o.step_down_term);
    bj_put_key(b, (const uint8_t *)"persist", 7);          bj_put_bool(b, o.persist);
    bj_put_key(b, (const uint8_t *)"persistTerm", 11);     bj_put_int(b, (int64_t)o.persist_term);
    bj_put_key(b, (const uint8_t *)"persistVotedFor", 15); bj_put_int(b, (int64_t)o.persist_voted_for);
    bj_put_key(b, (const uint8_t *)"resetTimer", 10);      bj_put_bool(b, o.reset_election_timer);
    bj_end_object(b);
    return emit(w, b);
}

EMSCRIPTEN_KEEPALIVE int rcw_decide_append(rcw *w, const uint8_t *in, int len) {
    w->out.len = 0;
    if (len < 0) return BJ_ERR_RANGE;
    size_t n = (size_t)len;

    raft_append_in a;
    memset(&a, 0, sizeof(a));
    a.msg_term       = u64_field(in, n, "msgTerm");
    a.leader_id      = u64_field(in, n, "leaderId");
    a.prev_log_index = u64_field(in, n, "prevLogIndex");
    a.prev_log_term  = u64_field(in, n, "prevLogTerm");
    a.entry_count    = (uint32_t)u64_field(in, n, "entryCount");
    a.current_term   = u64_field(in, n, "currentTerm");
    a.is_follower    = flag_field(in, n, "isFollower");
    a.our_base_index = u64_field(in, n, "ourBaseIndex");
    a.our_last_index = u64_field(in, n, "ourLastIndex");
    a.our_prev_term  = u64_field(in, n, "ourPrevTerm");

    raft_append_out o;
    raft_decide_append(&a, &o);

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"stale", 5);            bj_put_bool(b, o.stale);
    bj_put_key(b, (const uint8_t *)"success", 7);          bj_put_bool(b, o.success);
    bj_put_key(b, (const uint8_t *)"replyTerm", 9);        bj_put_int(b, (int64_t)o.reply_term);
    bj_put_key(b, (const uint8_t *)"stepDown", 8);         bj_put_bool(b, o.step_down);
    bj_put_key(b, (const uint8_t *)"stepDownTerm", 12);    bj_put_int(b, (int64_t)o.step_down_term);
    bj_put_key(b, (const uint8_t *)"stepDownLeader", 14);  bj_put_int(b, (int64_t)o.step_down_leader);
    bj_put_key(b, (const uint8_t *)"hasHint", 7);          bj_put_bool(b, o.has_hint);
    bj_put_key(b, (const uint8_t *)"hintIndex", 9);        bj_put_int(b, (int64_t)o.hint_index);
    bj_put_key(b, (const uint8_t *)"matchIndex", 10);      bj_put_int(b, (int64_t)o.match_index);
    bj_end_object(b);
    return emit(w, b);
}

/* Returns the index truncation started at (0 = none), or a negative
 * error. The log is this node's own open EntryLog handle. */
EMSCRIPTEN_KEEPALIVE double rcw_append_entries(elog *log, const uint8_t *entries, int len) {
    if (len < 0) return (double)BJ_ERR_RANGE;
    uint64_t truncated = 0;
    int e = raft_append_entries_to_log(log, entries, (uint32_t)len, &truncated);
    return e ? (double)e : (double)truncated;
}

/* The follower's new commit index, or -1 if it does not advance. */
EMSCRIPTEN_KEEPALIVE double rcw_follower_commit(double leader_commit, double our_commit,
                                                double our_last_index) {
    uint64_t out = 0;
    if (!raft_follower_commit((uint64_t)leader_commit, (uint64_t)our_commit,
                              (uint64_t)our_last_index, &out)) return -1;
    return (double)out;
}

/* The highest index a quorum holds, or -1. `matches` is a heap array of
 * f64, one per VOTING peer. */
EMSCRIPTEN_KEEPALIVE double rcw_commit_candidate(double leader_last, const double *matches,
                                                 int n, int quorum) {
    if (n < 0 || quorum < 0) return -1;
    uint64_t *m = NULL;
    if (n > 0) {
        m = (uint64_t *)malloc((size_t)n * sizeof(uint64_t));
        if (!m) return -1;
        for (int i = 0; i < n; i++) m[i] = matches[i] > 0 ? (uint64_t)matches[i] : 0;
    }
    uint64_t out = 0;
    uint32_t ok = raft_commit_candidate((uint64_t)leader_last, m, (uint32_t)n, (uint32_t)quorum, &out);
    free(m);
    return ok ? (double)out : -1;
}

EMSCRIPTEN_KEEPALIVE int rcw_may_commit(double candidate, double commit_index, double base_index,
                                        double term_at_candidate, double current_term) {
    return raft_may_commit((uint64_t)candidate, (uint64_t)commit_index, (uint64_t)base_index,
                           (uint64_t)term_at_candidate, (uint64_t)current_term);
}

EMSCRIPTEN_KEEPALIVE int rcw_quorum(int voter_count) {
    return voter_count < 0 ? 1 : (int)raft_quorum((uint32_t)voter_count);
}

/* { next, match, regressed } into the out buffer. */
EMSCRIPTEN_KEEPALIVE int rcw_backoff(rcw *w, double hint, int have_hint,
                                     double next, double match) {
    w->out.len = 0;
    raft_backoff_out o;
    raft_backoff(hint > 0 ? (uint64_t)hint : 0, have_hint,
                 next > 0 ? (uint64_t)next : 0, match > 0 ? (uint64_t)match : 0, &o);
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"next", 4);      bj_put_int(b, (int64_t)o.next);
    bj_put_key(b, (const uint8_t *)"match", 5);     bj_put_int(b, (int64_t)o.match);
    bj_put_key(b, (const uint8_t *)"regressed", 9); bj_put_bool(b, o.match_regressed);
    bj_end_object(b);
    return emit(w, b);
}

/* ---- membership ---------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE int rcw_members_adopt(rcw *w, const uint8_t *members, int len, double self_id) {
    w->out.len = 0;
    if (len < 0) return BJ_ERR_RANGE;
    return raft_members_adopt(members, (uint32_t)len, (uint64_t)self_id, &w->out);
}

EMSCRIPTEN_KEEPALIVE int rcw_members_merge(rcw *w, const uint8_t *input, int input_len,
                                           const uint8_t *known, int known_len) {
    w->out.len = 0;
    if (input_len < 0 || known_len < 0) return BJ_ERR_RANGE;
    return raft_members_merge(input, (uint32_t)input_len, known, (uint32_t)known_len, &w->out);
}
