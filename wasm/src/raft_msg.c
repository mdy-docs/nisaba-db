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

int rmsg_sender(const uint8_t *msg, uint32_t len, uint64_t *out) {
    *out = 0;
    int kind = -1;
    int e = rmsg_kind(msg, len, &kind);
    if (e) return e;
    uint64_t id = 0;
    switch (kind) {
        case RAFT_MSG_REQUEST_VOTE:     id = num_field(msg, len, "candidateId"); break;
        case RAFT_MSG_APPEND_ENTRIES:
        case RAFT_MSG_INSTALL_SNAPSHOT:
        case RAFT_MSG_TIMEOUT_NOW:      id = num_field(msg, len, "leaderId");    break;
        default: return RAFT_ERR_MESSAGE;   /* join/leave: not from a member */
    }
    /* 0 is "nobody" everywhere in this grammar, so a message that fails
     * to name its sender is one nobody can answer. */
    if (id == 0) return RAFT_ERR_MESSAGE;
    *out = id;
    return BJ_OK;
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

/* ---- InstallSnapshot ---------------------------------------------------- */

/* A field's whole encoded span, or nothing. Unlike the readers above
 * this judges nothing about the type: the manifest is an object and the
 * chunk is binary, and both travel to their consumer as they arrived. */
static void raw_field(const uint8_t *o, size_t len, const char *key,
                      const uint8_t **p, uint32_t *n) {
    const uint8_t *v; size_t vlen; int found = 0;
    *p = NULL; *n = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found))
        return;
    if (!found) return;
    *p = v;
    *n = (uint32_t)vlen;
}

int rmsg_install_read(const uint8_t *msg, uint32_t len, raft_install *out) {
    if (!msg || !out) return BJ_ERR_STATE;
    memset(out, 0, sizeof *out);

    int kind = -1;
    int e = rmsg_kind(msg, len, &kind);
    if (e) return e;
    if (kind != RAFT_MSG_INSTALL_SNAPSHOT) return RAFT_ERR_MESSAGE;

    out->term                = num_field(msg, len, "term");
    out->last_included_index = num_field(msg, len, "lastIncludedIndex");
    out->last_included_term  = num_field(msg, len, "lastIncludedTerm");
    out->offset              = num_field(msg, len, "offset");
    out->done                = bool_field(msg, len, "done");

    /* The sender, by the same rule every other kind is read by -- and it
     * is a refusal rather than a 0, because a chunk nobody can be said to
     * have sent is a chunk nobody can answer. */
    e = rmsg_sender(msg, len, &out->leader_id);
    if (e) return e;

    /* `role` is a string, or null/absent for a chunk that names no file.
     * str_field refuses a null, which is exactly the ambiguity that
     * matters here, so absence is checked first. */
    {
        const uint8_t *v; size_t vlen; int found = 0;
        if (obj_get_field(msg, len, (const uint8_t *)"role", 4, &v, &vlen, &found))
            return RAFT_ERR_MESSAGE;
        if (found && vlen >= 1 && v[0] != BJ_TYPE_NULL) {
            const uint8_t *s; uint32_t slen;
            if ((e = str_field(msg, len, "role", &s, &slen))) return e;
            out->role = (const char *)s;
            out->role_len = slen;
        }
    }

    /* The chunk itself: BINARY, and handed on as the span it occupies so
     * nothing copies it on the way to the file it belongs in. */
    {
        const uint8_t *v; size_t vlen; int found = 0;
        if (obj_get_field(msg, len, (const uint8_t *)"data", 4, &v, &vlen, &found))
            return RAFT_ERR_MESSAGE;
        if (found && vlen >= 1 && v[0] != BJ_TYPE_NULL) {
            if (vlen < 5 || v[0] != BJ_TYPE_BINARY) return RAFT_ERR_MESSAGE;
            uint32_t n = rdu32(v + 1);
            if ((size_t)n + 5 != vlen) return RAFT_ERR_MESSAGE;
            out->data = v + 5;
            out->data_len = n;
        }
    }

    raw_field(msg, len, "manifest", &out->manifest, &out->manifest_len);
    /* A manifest that is present but null is no manifest -- the same
     * distinction `role` makes, and the JS encoder emits null for an
     * absent one in some paths. */
    if (out->manifest && out->manifest_len >= 1 && out->manifest[0] == BJ_TYPE_NULL) {
        out->manifest = NULL;
        out->manifest_len = 0;
    }
    return BJ_OK;
}

int rmsg_build_install_snapshot(uint64_t term, uint64_t leader_id,
                                uint64_t last_included_index,
                                uint64_t last_included_term,
                                const char *role, uint32_t role_len,
                                uint64_t offset,
                                const uint8_t *data, uint32_t data_len,
                                int done,
                                const uint8_t *manifest, uint32_t manifest_len,
                                dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    put_key(b, "kind");
    bj_put_string(b, (const uint8_t *)KIND_NAME[RAFT_MSG_INSTALL_SNAPSHOT],
                  (uint32_t)strlen(KIND_NAME[RAFT_MSG_INSTALL_SNAPSHOT]));
    put_key(b, "term");              bj_put_int(b, (int64_t)term);
    put_key(b, "leaderId");          bj_put_int(b, (int64_t)leader_id);
    put_key(b, "lastIncludedIndex"); bj_put_int(b, (int64_t)last_included_index);
    put_key(b, "lastIncludedTerm");  bj_put_int(b, (int64_t)last_included_term);
    /* Written even when there is none: a receiver reads `role` to decide
     * whether this chunk belongs in a file, and an absent field and an
     * explicit null would be two spellings of one answer. */
    put_key(b, "role");
    if (role) bj_put_string(b, (const uint8_t *)role, role_len);
    else      bj_put_null(b);
    put_key(b, "offset");            bj_put_int(b, (int64_t)offset);
    put_key(b, "data");              bj_put_binary(b, data, data_len);
    put_key(b, "done");              bj_put_bool(b, done);
    if (manifest && manifest_len) {
        put_key(b, "manifest");
        bj_put_raw(b, manifest, manifest_len);
    }
    bj_end_object(b);
    return finish(b, out);
}

int rmsg_build_install_reply(uint64_t term, int ok, int restart, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = put_key(b, "term");
    if (!e) e = bj_put_int(b, (int64_t)term);
    if (!e) e = put_key(b, "success");
    if (!e) e = bj_put_bool(b, ok);
    if (!e && !ok && restart) {
        e = put_key(b, "restart");
        if (!e) e = bj_put_bool(b, 1);
    }
    if (!e) e = bj_end_object(b);
    return e ? (bj_builder_free(b), e) : finish(b, out);
}

/* ---- TimeoutNow, join and leave: the replies ----------------------------- */

int rmsg_build_ack(uint64_t term, int ok, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = put_key(b, "term");
    if (!e) e = bj_put_int(b, (int64_t)term);
    if (!e) e = put_key(b, "ok");
    if (!e) e = bj_put_bool(b, ok);
    if (!e) e = bj_end_object(b);
    return e ? (bj_builder_free(b), e) : finish(b, out);
}

int rmsg_build_membership_reply(int ok, const uint8_t *members, uint32_t members_len,
                                const char *error, int retry,
                                uint64_t leader_id, const uint8_t *leader_record,
                                uint32_t leader_record_len, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = put_key(b, "ok");
    if (!e) e = bj_put_bool(b, ok);

    if (ok) {
        if (!e && members) {
            e = put_key(b, "members");
            if (!e) e = bj_put_raw(b, members, members_len);
        }
    } else if (error) {
        if (!e) e = put_key(b, "error");
        if (!e) e = bj_put_string(b, (const uint8_t *)error, (uint32_t)strlen(error));
    } else if (retry) {
        if (!e) e = put_key(b, "retry");
        if (!e) e = bj_put_bool(b, 1);
    } else {
        /* A redirect. The address travels WITH the id: a joiner knows
         * addresses, not ids, so an id alone would send it back to the
         * seed it just asked. */
        if (!e) e = put_key(b, "leaderId");
        if (!e) e = bj_put_int(b, (int64_t)leader_id);
        if (!e) e = put_key(b, "leaderAddress");
        if (leader_record) {
            const uint8_t *h; size_t hlen; int found = 0;
            if (!e && obj_get_field(leader_record, leader_record_len,
                                    (const uint8_t *)"host", 4, &h, &hlen, &found) == BJ_OK && found) {
                e = bj_begin_object(b);
                if (!e) e = put_key(b, "host");
                if (!e) e = bj_put_raw(b, h, (uint32_t)hlen);
                const uint8_t *p; size_t plen; int pfound = 0;
                if (!e && obj_get_field(leader_record, leader_record_len,
                                        (const uint8_t *)"port", 4, &p, &plen, &pfound) == BJ_OK && pfound) {
                    e = put_key(b, "port");
                    if (!e) e = bj_put_raw(b, p, (uint32_t)plen);
                }
                if (!e) e = bj_end_object(b);
            } else if (!e) {
                e = bj_put_null(b);
            }
        } else if (!e) {
            e = bj_put_null(b);
        }
    }
    if (!e) e = bj_end_object(b);
    return e ? (bj_builder_free(b), e) : finish(b, out);
}

int rmsg_term(const uint8_t *msg, uint32_t len, uint64_t *term) {
    *term = num_field(msg, len, "term");
    return BJ_OK;
}

int rmsg_join_member(const uint8_t *msg, uint32_t len,
                     const uint8_t **record, uint32_t *record_len, uint64_t *id) {
    const uint8_t *v; size_t vlen; int found = 0;
    *record = NULL; *record_len = 0; *id = 0;
    if (obj_get_field(msg, len, (const uint8_t *)"member", 6, &v, &vlen, &found))
        return RAFT_ERR_MESSAGE;
    if (!found || vlen < 1 || v[0] != BJ_TYPE_OBJECT) return RAFT_ERR_MESSAGE;
    /* An id it can be addressed by is the one field a join cannot omit;
     * raft_core.h's adopt rules validate the rest when it is proposed. */
    *id = num_field(v, vlen, "id");
    if (*id == 0) return RAFT_ERR_MESSAGE;
    *record = v;
    *record_len = (uint32_t)vlen;
    return BJ_OK;
}

/* One field of a member record, as the encoded span it occupies -- so two
 * records can be compared on it without decoding either. */
int rmsg_record_field(const uint8_t *record, uint32_t len, const char *key,
                      const uint8_t **v, uint32_t *vlen) {
    const uint8_t *p; size_t plen; int found = 0;
    *v = NULL; *vlen = 0;
    if (obj_get_field(record, len, (const uint8_t *)key, (uint32_t)strlen(key), &p, &plen, &found))
        return RAFT_ERR_MESSAGE;
    if (!found) return BJ_OK;
    *v = p;
    *vlen = (uint32_t)plen;
    return BJ_OK;
}

int rmsg_leave_id(const uint8_t *msg, uint32_t len, uint64_t *id) {
    *id = num_field(msg, len, "id");
    return *id ? BJ_OK : RAFT_ERR_MESSAGE;
}
