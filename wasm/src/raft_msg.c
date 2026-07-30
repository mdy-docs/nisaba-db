/*
 * raft_msg.c — see raft_msg.h.
 */
#include "raft_msg.h"
#include "bjcursor.h"

#include <stdlib.h>
#include <string.h>

/* The wire spelling of each kind. The only place these strings exist. */
static const char *const KIND_NAME[] = {
    "requestVote", "appendEntries", "installSnapshot", "join", "leave",
    "timeoutNow"
};
#define KIND_COUNT ((int)(sizeof(KIND_NAME) / sizeof(KIND_NAME[0])))

/* ---- reading ----------------------------------------------------------- */

static int str_field(const uint8_t *o, size_t len, const char *key,
                     const uint8_t **p, uint32_t *n) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found))
        return RAFT_ERR_MESSAGE;
    if (!found || vlen < 5 || v[0] != BJ_TYPE_STRING) return RAFT_ERR_MESSAGE;
    uint32_t sl = rdu32(v + 1);
    if ((size_t)sl + 5 != vlen) return RAFT_ERR_MESSAGE;
    *p = v + 5;
    *n = sl;
    return BJ_OK;
}

/* A number field. Absent is 0 -- every numeric field in this grammar has
 * 0 as its "not present" meaning (term 0 is before any election, index 0
 * is before any entry, id 0 is nobody). */
static uint64_t num_field(const uint8_t *o, size_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found))
        return 0;
    if (!found) return 0;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) != BJ_OK || d < 0) return 0;
    return (uint64_t)d;
}

static int bool_field(const uint8_t *o, size_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found))
        return 0;
    return found && vlen >= 1 && v[0] == BJ_TYPE_TRUE;
}

/* The `entries` ARRAY, as the span it occupies in the message -- which is
 * the point: it goes to the conflict rule without being copied, decoded
 * or re-encoded. */
static int entries_field(const uint8_t *o, size_t len,
                         const uint8_t **p, uint32_t *n, uint32_t *count) {
    const uint8_t *v; size_t vlen; int found = 0;
    *p = NULL; *n = 0; *count = 0;
    if (obj_get_field(o, len, (const uint8_t *)"entries", 7, &v, &vlen, &found))
        return RAFT_ERR_MESSAGE;
    if (!found) return BJ_OK;                       /* a bare heartbeat */
    cur c = { v, vlen, 0 };
    if (array_begin(&c, count) != BJ_OK) return RAFT_ERR_MESSAGE;
    *p = v;
    *n = (uint32_t)vlen;
    return BJ_OK;
}

int rmsg_kind(const uint8_t *msg, uint32_t len, int *kind_out) {
    *kind_out = -1;
    const uint8_t *k; uint32_t klen;
    int e = str_field(msg, len, "kind", &k, &klen);
    if (e) return e;
    for (int i = 0; i < KIND_COUNT; i++) {
        if ((uint32_t)strlen(KIND_NAME[i]) == klen && memcmp(k, KIND_NAME[i], klen) == 0) {
            *kind_out = i;
            return BJ_OK;
        }
    }
    return RAFT_ERR_MESSAGE;
}

/* ---- replies ------------------------------------------------------------ */

static int finish(bj_builder *b, dbuf *out) {
    int e = bj_builder_error(b);
    if (!e) {
        size_t n; const uint8_t *p = bj_builder_data(b, &n);
        if (!p) e = BJ_ERR_STATE;
        else e = dbuf_put(out, p, n);
    }
    bj_builder_free(b);
    return e;
}

static int put_key(bj_builder *b, const char *k) {
    return bj_put_key(b, (const uint8_t *)k, (uint32_t)strlen(k));
}

/* ---- RequestVote -------------------------------------------------------- */

int rmsg_handle_request_vote(elog *log, const raft_msg_state *st,
                             const uint8_t *msg, uint32_t len,
                             raft_msg_effect *eff, dbuf *reply) {
    memset(eff, 0, sizeof(*eff));

    int kind = -1;
    int e = rmsg_kind(msg, len, &kind);
    if (e) return e;
    if (kind != RAFT_MSG_REQUEST_VOTE) return RAFT_ERR_MESSAGE;

    raft_vote_in in;
    memset(&in, 0, sizeof(in));
    in.msg_term             = num_field(msg, len, "term");
    in.candidate_id         = num_field(msg, len, "candidateId");
    in.last_log_index       = num_field(msg, len, "lastLogIndex");
    in.last_log_term        = num_field(msg, len, "lastLogTerm");
    in.pre_vote             = bool_field(msg, len, "preVote");
    in.current_term         = elog_current_term(log);
    in.voted_for            = elog_voted_for(log);
    in.our_last_index       = elog_last_index(log);
    in.our_last_term        = elog_last_term(log);
    in.self_is_voter        = st->self_is_voter;
    in.is_leader            = st->is_leader;
    in.leader_id            = st->leader_id;
    in.now                  = st->now;
    in.last_leader_contact  = st->last_leader_contact;
    in.min_election_timeout = st->min_election_timeout;

    if (in.candidate_id == 0) return RAFT_ERR_MESSAGE;

    raft_vote_out d;
    raft_decide_vote(&in, &d);

    /*
     * In this order, and before the reply is built: a higher term makes
     * us a follower of nobody, then the vote commits. elog_set_hard_state
     * fsyncs, so by the time the reply exists the promise it carries is
     * already on the disk.
     */
    if (d.step_down) {
        e = elog_set_hard_state(log, d.step_down_term, EL_VOTED_NONE);
        if (e) return e;
        eff->became_follower = 1;
        eff->new_leader_id = 0;
    }
    if (d.persist) {
        e = elog_set_hard_state(log, d.persist_term, d.persist_voted_for);
        if (e) return e;
    }
    eff->reset_election_timer = d.reset_election_timer;
    eff->granted_vote = d.grant;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    put_key(b, "term");        bj_put_int(b, (int64_t)elog_current_term(log));
    put_key(b, "voteGranted"); bj_put_bool(b, d.grant);
    bj_end_object(b);
    return finish(b, reply);
}

/* ---- AppendEntries ------------------------------------------------------ */

int rmsg_handle_append_entries(elog *log, const raft_msg_state *st,
                               const uint8_t *msg, uint32_t len,
                               raft_msg_effect *eff, dbuf *reply) {
    memset(eff, 0, sizeof(*eff));

    int kind = -1;
    int e = rmsg_kind(msg, len, &kind);
    if (e) return e;
    if (kind != RAFT_MSG_APPEND_ENTRIES) return RAFT_ERR_MESSAGE;

    const uint8_t *entries; uint32_t entries_len, entry_count;
    e = entries_field(msg, len, &entries, &entries_len, &entry_count);
    if (e) return e;

    uint64_t prev_index = num_field(msg, len, "prevLogIndex");
    uint64_t leader_id  = num_field(msg, len, "leaderId");
    if (leader_id == 0) return RAFT_ERR_MESSAGE;

    raft_append_in in;
    memset(&in, 0, sizeof(in));
    in.msg_term       = num_field(msg, len, "term");
    in.leader_id      = leader_id;
    in.prev_log_index = prev_index;
    in.prev_log_term  = num_field(msg, len, "prevLogTerm");
    in.entry_count    = entry_count;
    in.current_term   = elog_current_term(log);
    in.is_follower    = st->is_follower;
    in.our_base_index = elog_base_index(log);
    in.our_last_index = elog_last_index(log);
    if (prev_index >= in.our_base_index && prev_index <= in.our_last_index) {
        uint64_t t = 0;
        /* Below the base the log cannot answer; the decision does not
         * consult the term in that case. */
        if (elog_term_at(log, prev_index, &t) == BJ_OK) in.our_prev_term = t;
    }

    raft_append_out d;
    raft_decide_append(&in, &d);

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;

    /* A stale leader gets our term and nothing else -- no step-down, no
     * refreshed election timer. It is not our leader. */
    if (d.stale) {
        bj_begin_object(b);
        put_key(b, "term");    bj_put_int(b, (int64_t)d.reply_term);
        put_key(b, "success"); bj_put_bool(b, 0);
        bj_end_object(b);
        return finish(b, reply);
    }

    if (d.step_down) {
        if (d.step_down_term > in.current_term) {
            e = elog_set_hard_state(log, d.step_down_term, EL_VOTED_NONE);
            if (e) { bj_builder_free(b); return e; }
        }
        eff->became_follower = 1;
    }
    /* Whether or not we stepped down, this is our leader now and it is
     * alive: the election countdown restarts even for a rejection. */
    eff->new_leader_id = leader_id;
    eff->touched_leader = 1;
    eff->new_last_leader_contact = st->now;
    eff->reset_election_timer = 1;

    if (!d.success) {
        bj_begin_object(b);
        put_key(b, "term");      bj_put_int(b, (int64_t)elog_current_term(log));
        put_key(b, "success");   bj_put_bool(b, 0);
        if (d.has_hint) { put_key(b, "hintIndex"); bj_put_int(b, (int64_t)d.hint_index); }
        bj_end_object(b);
        return finish(b, reply);
    }

    /* The entries go to the conflict rule as the span they arrived in.
     * This is the whole reason the handler lives here. */
    if (entries && entry_count) {
        uint64_t truncated = 0;
        e = raft_append_entries_to_log(log, entries, entries_len, &truncated);
        if (e) { bj_builder_free(b); return e; }
        eff->truncated_from = truncated;
    }

    uint64_t advanced = 0;
    if (raft_follower_commit(num_field(msg, len, "leaderCommit"),
                             st->commit_index, elog_last_index(log), &advanced)) {
        e = elog_set_commit_index(log, advanced);   /* advisory; rides the next sync */
        if (e) { bj_builder_free(b); return e; }
        eff->new_commit_index = advanced;
    }
    eff->quiesce = bool_field(msg, len, "quiesce");
    eff->match_index = d.match_index;
    eff->success = 1;

    bj_begin_object(b);
    put_key(b, "term");       bj_put_int(b, (int64_t)elog_current_term(log));
    put_key(b, "success");    bj_put_bool(b, 1);
    put_key(b, "matchIndex"); bj_put_int(b, (int64_t)d.match_index);
    bj_end_object(b);
    return finish(b, reply);
}

/* ---- building requests -------------------------------------------------- */

int rmsg_build_request_vote(uint64_t term, uint64_t candidate_id,
                            uint64_t last_log_index, uint64_t last_log_term,
                            int pre_vote, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    put_key(b, "kind");
    bj_put_string(b, (const uint8_t *)KIND_NAME[RAFT_MSG_REQUEST_VOTE],
                  (uint32_t)strlen(KIND_NAME[RAFT_MSG_REQUEST_VOTE]));
    put_key(b, "term");         bj_put_int(b, (int64_t)term);
    put_key(b, "candidateId");  bj_put_int(b, (int64_t)candidate_id);
    put_key(b, "lastLogIndex"); bj_put_int(b, (int64_t)last_log_index);
    put_key(b, "lastLogTerm");  bj_put_int(b, (int64_t)last_log_term);
    put_key(b, "preVote");      bj_put_bool(b, pre_vote);
    bj_end_object(b);
    return finish(b, out);
}

int rmsg_build_append_entries(elog *log, uint64_t term, uint64_t leader_id,
                              uint64_t next_index, uint64_t prev_log_term,
                              uint64_t leader_commit, size_t max_bytes,
                              int quiesce, uint32_t *out_count, dbuf *out) {
    *out_count = 0;

    /* The batch comes straight out of the log's own reader and is copied
     * once, into the message. Nothing decodes it on the way. */
    int count = 0;
    const uint8_t *batch = NULL;
    size_t batch_len = 0;
    if (next_index <= elog_last_index(log)) {
        int e = elog_get_batch(log, next_index, max_bytes, &count, &batch, &batch_len);
        if (e) return e;
    }

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    put_key(b, "kind");
    bj_put_string(b, (const uint8_t *)KIND_NAME[RAFT_MSG_APPEND_ENTRIES],
                  (uint32_t)strlen(KIND_NAME[RAFT_MSG_APPEND_ENTRIES]));
    put_key(b, "term");          bj_put_int(b, (int64_t)term);
    put_key(b, "leaderId");      bj_put_int(b, (int64_t)leader_id);
    put_key(b, "prevLogIndex");  bj_put_int(b, (int64_t)(next_index - 1));
    put_key(b, "prevLogTerm");   bj_put_int(b, (int64_t)prev_log_term);
    put_key(b, "leaderCommit");  bj_put_int(b, (int64_t)leader_commit);
    put_key(b, "entries");
    if (batch && batch_len) bj_put_raw(b, batch, (uint32_t)batch_len);
    else { bj_begin_array(b); bj_end_array(b); }
    if (quiesce) { put_key(b, "quiesce"); bj_put_bool(b, 1); }
    bj_end_object(b);

    *out_count = (uint32_t)count;
    return finish(b, out);
}
