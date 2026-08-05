/*
 * raft_core.c — see raft_core.h. Every function here is a transcription
 * of a rule in src/raft.js, which is itself a transcription of the paper;
 * the comments name the section so a reader can check both.
 */
#include "raft_core.h"
#include "bjcursor.h"

#include <stdlib.h>
#include <string.h>

/* ---- membership -------------------------------------------------------- */

/* One member record, spanning into the caller's buffer. */
typedef struct {
    uint64_t       id;
    const uint8_t *rec;      /* the whole encoded record, or NULL for a
                              * bare id (nothing to copy through) */
    uint32_t       rec_len;
    int            voting;   /* 0 only when `voting: false` is present */
} member;

static int read_id(const uint8_t *rec, size_t len, uint64_t *out) {
    const uint8_t *v; size_t vlen; int found = 0;
    int e = obj_get_field(rec, len, (const uint8_t *)"id", 2, &v, &vlen, &found);
    if (e || !found) return RAFT_ERR_MEMBER;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) != BJ_OK) return RAFT_ERR_MEMBER;
    if (!(d >= 1) || d != (double)(uint64_t)d) return RAFT_ERR_MEMBER;
    *out = (uint64_t)d;
    return BJ_OK;
}

/* `voting: false` makes a learner; anything else (absent, true) a voter.
 * Absence means voter by construction -- the bootstrap set and explicit
 * changeMembership records get the franchise, only an explicit flag
 * withholds it. */
static int read_voting(const uint8_t *rec, size_t len) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(rec, len, (const uint8_t *)"voting", 6, &v, &vlen, &found) != BJ_OK) return 1;
    if (!found || vlen < 1) return 1;
    return v[0] == BJ_TYPE_FALSE ? 0 : 1;
}

/*
 * Read a member ARRAY into `out` (caller frees). Each element is either
 * an OBJECT record or a bare INT id.
 */
static int read_members(const uint8_t *buf, uint32_t len, member **out, uint32_t *n_out) {
    *out = NULL; *n_out = 0;
    cur c = { buf, len, 0 };
    uint32_t count;
    if (array_begin(&c, &count) != BJ_OK) return RAFT_ERR_MEMBER;

    member *ms = (member *)calloc(count ? count : 1, sizeof(member));
    if (!ms) return BJ_ERR_OOM;

    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) { free(ms); return RAFT_ERR_MEMBER; }
        const uint8_t *v = c.d + start;
        size_t vlen = c.pos - start;
        if (vlen < 1) { free(ms); return RAFT_ERR_MEMBER; }

        if (v[0] == BJ_TYPE_OBJECT) {
            int e = read_id(v, vlen, &ms[i].id);
            if (e) { free(ms); return e; }
            ms[i].rec = v;
            ms[i].rec_len = (uint32_t)vlen;
            ms[i].voting = read_voting(v, vlen);
        } else {
            cur ic = { v, vlen, 0 };
            double d;
            if (read_number(&ic, &d) != BJ_OK) { free(ms); return RAFT_ERR_MEMBER; }
            if (!(d >= 1) || d != (double)(uint64_t)d) { free(ms); return RAFT_ERR_MEMBER; }
            ms[i].id = (uint64_t)d;
            ms[i].rec = NULL;
            ms[i].voting = 1;
        }
    }
    *out = ms;
    *n_out = count;
    return BJ_OK;
}

/* Insertion sort by id, then drop duplicates keeping the LAST -- the same
 * thing a Map keyed by id does, which is how the JS built its set. */
static uint32_t sort_unique(member *ms, uint32_t n) {
    for (uint32_t i = 1; i < n; i++) {
        member key = ms[i];
        uint32_t j = i;
        while (j > 0 && ms[j - 1].id > key.id) { ms[j] = ms[j - 1]; j--; }
        ms[j] = key;
    }
    uint32_t w = 0;
    for (uint32_t i = 0; i < n; i++) {
        if (w > 0 && ms[w - 1].id == ms[i].id) ms[w - 1] = ms[i];
        else ms[w++] = ms[i];
    }
    return w;
}

static int put_record(bj_builder *b, const member *m) {
    if (m->rec) return bj_put_raw(b, m->rec, m->rec_len);
    /* A bare id becomes the record {id}. */
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"id", 2);
    if (!e) e = bj_put_int(b, (int64_t)m->id);
    if (!e) e = bj_end_object(b);
    return e;
}

int raft_members_adopt(const uint8_t *members, uint32_t len, uint64_t self_id, dbuf *out) {
    member *ms = NULL; uint32_t n = 0;
    int e = read_members(members, len, &ms, &n);
    if (e) return e;
    n = sort_unique(ms, n);

    bj_builder *b = bj_builder_new();
    if (!b) { free(ms); return BJ_ERR_OOM; }
    e = bj_begin_object(b);

    if (!e) e = bj_put_key(b, (const uint8_t *)"members", 7);
    if (!e) e = bj_begin_array(b);
    for (uint32_t i = 0; !e && i < n; i++) e = put_record(b, &ms[i]);
    if (!e) e = bj_end_array(b);

    if (!e) e = bj_put_key(b, (const uint8_t *)"voters", 6);
    if (!e) e = bj_begin_array(b);
    for (uint32_t i = 0; !e && i < n; i++) {
        if (ms[i].voting) e = bj_put_int(b, (int64_t)ms[i].id);
    }
    if (!e) e = bj_end_array(b);

    if (!e) e = bj_put_key(b, (const uint8_t *)"peers", 5);
    if (!e) e = bj_begin_array(b);
    for (uint32_t i = 0; !e && i < n; i++) {
        if (ms[i].id != self_id) e = bj_put_int(b, (int64_t)ms[i].id);
    }
    if (!e) e = bj_end_array(b);

    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t sz; const uint8_t *p = bj_builder_data(b, &sz);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_put(out, p, sz);
    }
    bj_builder_free(b);
    free(ms);
    return e;
}

/*
 * Copy `rec`'s fields into the builder, then any field of `known` that
 * `rec` does not have -- the JS `Object.assign(record, {...known,
 * ...record})`, which is "inherit what you did not say".
 */
static int merge_record(bj_builder *b, const member *rec, const member *known) {
    if (!known || !known->rec) return put_record(b, rec);
    if (!rec->rec) return bj_put_raw(b, known->rec, known->rec_len);

    int e = bj_begin_object(b);
    cur c = { rec->rec, rec->rec_len, 0 };
    uint32_t count;
    if (object_begin(&c, &count) != BJ_OK) return RAFT_ERR_MEMBER;
    for (uint32_t i = 0; !e && i < count; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&c, &kp, &klen))) break;
        size_t start = c.pos;
        if ((e = skip_value(&c))) break;
        e = bj_put_key(b, kp, klen);
        if (!e) e = bj_put_raw(b, c.d + start, (uint32_t)(c.pos - start));
    }
    if (e) return e;

    cur kc = { known->rec, known->rec_len, 0 };
    uint32_t kcount;
    if (object_begin(&kc, &kcount) != BJ_OK) return RAFT_ERR_MEMBER;
    for (uint32_t i = 0; !e && i < kcount; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&kc, &kp, &klen))) break;
        size_t start = kc.pos;
        if ((e = skip_value(&kc))) break;
        const uint8_t *dup; size_t duplen; int found = 0;
        if (obj_get_field(rec->rec, rec->rec_len, kp, klen, &dup, &duplen, &found) != BJ_OK) continue;
        if (found) continue;
        e = bj_put_key(b, kp, klen);
        if (!e) e = bj_put_raw(b, kc.d + start, (uint32_t)(kc.pos - start));
    }
    if (!e) e = bj_end_object(b);
    return e;
}

int raft_members_merge(const uint8_t *input, uint32_t input_len,
                       const uint8_t *known, uint32_t known_len, dbuf *out) {
    member *in = NULL; uint32_t n_in = 0;
    int e = read_members(input, input_len, &in, &n_in);
    if (e) return e;
    n_in = sort_unique(in, n_in);
    if (n_in == 0) { free(in); return RAFT_ERR_MEMBER; }

    member *kn = NULL; uint32_t n_kn = 0;
    if (known_len) {
        e = read_members(known, known_len, &kn, &n_kn);
        if (e) { free(in); return e; }
        n_kn = sort_unique(kn, n_kn);
    }

    bj_builder *b = bj_builder_new();
    if (!b) { free(kn); free(in); return BJ_ERR_OOM; }
    e = bj_begin_array(b);
    for (uint32_t i = 0; !e && i < n_in; i++) {
        /* Only an address-less record inherits: one that states a host is
         * making a statement, and re-announcing an address is exactly how
         * a restarted node corrects the log. */
        int has_host = 0;
        if (in[i].rec) {
            const uint8_t *v; size_t vlen;
            if (obj_get_field(in[i].rec, in[i].rec_len, (const uint8_t *)"host", 4,
                              &v, &vlen, &has_host) != BJ_OK) has_host = 0;
        }
        const member *match = NULL;
        if (!has_host) {
            for (uint32_t k = 0; k < n_kn; k++) if (kn[k].id == in[i].id) { match = &kn[k]; break; }
        }
        e = merge_record(b, &in[i], match);
    }
    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t sz; const uint8_t *p = bj_builder_data(b, &sz);
        if (!p) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_put(out, p, sz);
    }
    bj_builder_free(b);
    free(kn);
    free(in);
    return e;
}

uint32_t raft_quorum(uint32_t voter_count) { return voter_count / 2u + 1u; }

/* ---- §5.2, §5.4.1: granting a vote ------------------------------------- */

void raft_decide_vote(const raft_vote_in *in, raft_vote_out *out) {
    memset(out, 0, sizeof(*out));
    out->reply_term = in->current_term;

    /* A candidate from the past learns our term and nothing else. */
    if (in->msg_term < in->current_term) return;

    /* Learners and removed nodes hold no franchise. A candidate only
     * solicits voters, but a straggler with a stale config may still
     * ask -- and answering would let a node nobody counts decide an
     * election. */
    if (!in->self_is_voter) return;

    /* §5.4.1: at least as up to date as ours, by (term, index). */
    int up_to_date = in->last_log_term > in->our_last_term ||
        (in->last_log_term == in->our_last_term && in->last_log_index >= in->our_last_index);

    if (in->pre_vote) {
        /* "Would I vote for you?" -- persists nothing and bumps no term,
         * so an isolated node rejoining cannot dethrone a live leader by
         * arriving with a higher term.
         *
         * Stickiness: refuse while a leader is being heard from. A
         * healthy leader IS that leader and always refuses; the case this
         * exists for is a removed-but-unaware member whose log is up to
         * date pre-voting at the still-working leader. */
        int sticky = in->is_leader ||
            (in->leader_id != 0 &&
             in->now - in->last_leader_contact < in->min_election_timeout);
        out->grant = !sticky && up_to_date;
        return;
    }

    uint64_t term = in->current_term;
    uint64_t voted_for = in->voted_for;
    if (in->msg_term > term) {
        /* A higher term makes us a follower of nobody, and clears the
         * vote: a new term is a new election. */
        out->step_down = 1;
        out->step_down_term = in->msg_term;
        term = in->msg_term;
        voted_for = 0;
    }
    out->reply_term = term;

    if ((voted_for == 0 || voted_for == in->candidate_id) && up_to_date) {
        if (voted_for != in->candidate_id) {
            out->persist = 1;
            out->persist_term = term;
            out->persist_voted_for = in->candidate_id;
        }
        out->grant = 1;
        /* Having just endorsed somebody, do not immediately campaign
         * against them. */
        out->reset_election_timer = 1;
    }
}

/* ---- §5.3: accepting entries ------------------------------------------- */

void raft_decide_append(const raft_append_in *in, raft_append_out *out) {
    memset(out, 0, sizeof(*out));
    out->reply_term = in->current_term;

    if (in->msg_term < in->current_term) { out->stale = 1; return; }

    /* A leader at our term or above is THE leader: a candidate that
     * receives this concedes, and a leader that does has been superseded. */
    if (in->msg_term > in->current_term || !in->is_follower) {
        out->step_down = 1;
        out->step_down_term = in->msg_term;
        out->step_down_leader = in->leader_id;
    }
    out->reply_term = in->msg_term > in->current_term ? in->msg_term : in->current_term;

    /* The consistency check, and where to resume when it fails. Each hint
     * is the first index the leader could usefully send, so a rejection
     * costs one round trip rather than one per entry. */
    if (in->prev_log_index < in->our_base_index) {
        /* Offering entries our snapshot already covers. */
        out->has_hint = 1;
        out->hint_index = in->our_base_index + 1;
        return;
    }
    if (in->prev_log_index > in->our_last_index) {
        out->has_hint = 1;
        out->hint_index = in->our_last_index + 1;
        return;
    }
    if (in->our_prev_term != in->prev_log_term) {
        out->has_hint = 1;
        out->hint_index = in->prev_log_index;
        return;
    }

    out->success = 1;
    out->match_index = in->prev_log_index + in->entry_count;
}

int raft_append_entries_to_log(elog *log, const uint8_t *entries, uint32_t len,
                               uint64_t *truncated_from) {
    *truncated_from = 0;
    cur c = { entries, len, 0 };
    uint32_t count;
    if (array_begin(&c, &count) != BJ_OK) return BJ_ERR_VERIFY;

    int appended = 0;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) return BJ_ERR_VERIFY;
        const uint8_t *e = c.d + start;
        size_t elen = c.pos - start;

        const uint8_t *v; size_t vlen; int found = 0;
        uint64_t index, term; int type = 0;
        cur nc;
        double d;

        if (obj_get_field(e, elen, (const uint8_t *)"index", 5, &v, &vlen, &found) || !found)
            return BJ_ERR_VERIFY;
        nc = (cur){ v, vlen, 0 };
        if (read_number(&nc, &d) != BJ_OK || d < 0) return BJ_ERR_VERIFY;
        index = (uint64_t)d;

        if (obj_get_field(e, elen, (const uint8_t *)"term", 4, &v, &vlen, &found) || !found)
            return BJ_ERR_VERIFY;
        nc = (cur){ v, vlen, 0 };
        if (read_number(&nc, &d) != BJ_OK || d < 0) return BJ_ERR_VERIFY;
        term = (uint64_t)d;

        if (obj_get_field(e, elen, (const uint8_t *)"type", 4, &v, &vlen, &found) == BJ_OK && found) {
            nc = (cur){ v, vlen, 0 };
            if (read_number(&nc, &d) != BJ_OK) return BJ_ERR_VERIFY;
            type = (int)d;
        }

        const uint8_t *payload = NULL; uint32_t payload_len = 0;
        if (obj_get_field(e, elen, (const uint8_t *)"payload", 7, &v, &vlen, &found) == BJ_OK && found) {
            if (vlen >= 5 && v[0] == BJ_TYPE_BINARY) {
                payload_len = rdu32(v + 1);
                if ((size_t)payload_len + 5 != vlen) return BJ_ERR_VERIFY;
                payload = v + 5;
            } else if (vlen >= 1 && v[0] == BJ_TYPE_NULL) {
                payload = NULL;
            } else {
                return BJ_ERR_VERIFY;
            }
        }

        if (index <= elog_last_index(log)) {
            uint64_t have = 0;
            int rc = elog_term_at(log, index, &have);
            if (rc == BJ_OK && have == term) continue;   /* already ours */
            /* Ours disagrees: discard our suffix from here. Raft
             * guarantees this index is above the commit index, and
             * EntryLog's truncate guard is that guarantee made
             * mechanical -- a refusal here means the invariant broke
             * upstream, so it propagates rather than being absorbed. */
            rc = elog_truncate_from(log, index);
            if (rc) return rc;
            if (*truncated_from == 0) *truncated_from = index;
        }
        uint64_t assigned = 0;
        int rc = elog_append(log, term, type, payload, payload_len, &assigned);
        if (rc) return rc;
        appended = 1;
    }
    /* Durable BEFORE the ack: the leader counts this follower toward a
     * quorum on the strength of the reply. */
    return appended ? elog_sync(log) : BJ_OK;
}

int raft_follower_commit(uint64_t leader_commit, uint64_t our_commit,
                         uint64_t our_last_index, uint64_t *out) {
    if (leader_commit <= our_commit) return 0;
    /* Never past what we hold: the leader may have committed entries it
     * has not sent us yet. */
    *out = leader_commit < our_last_index ? leader_commit : our_last_index;
    return *out > our_commit;
}

/* ---- §5.4.2: what a leader may commit ---------------------------------- */

uint32_t raft_commit_candidate(uint64_t leader_last, const uint64_t *matches, uint32_t n,
                               uint32_t quorum, uint64_t *out) {
    if (quorum == 0) return 0;
    /* The leader is always a voter and always holds its own last index. */
    uint32_t total = n + 1;
    if (quorum > total) return 0;

    uint64_t *all = (uint64_t *)malloc((size_t)total * sizeof(uint64_t));
    if (!all) return 0;
    all[0] = leader_last;
    for (uint32_t i = 0; i < n; i++) all[i + 1] = matches[i];

    /* Descending; all[quorum-1] is the highest index that `quorum` nodes
     * have reached. Insertion sort over a cluster-sized array. */
    for (uint32_t i = 1; i < total; i++) {
        uint64_t key = all[i];
        uint32_t j = i;
        while (j > 0 && all[j - 1] < key) { all[j] = all[j - 1]; j--; }
        all[j] = key;
    }
    *out = all[quorum - 1];
    free(all);
    return 1;
}

int raft_may_commit(uint64_t candidate, uint64_t commit_index, uint64_t base_index,
                    uint64_t term_at_candidate, uint64_t current_term) {
    if (candidate <= commit_index) return 0;
    if (candidate <= base_index) return 0;
    /* §5.4.2, the figure-8 rule: only an entry of the CURRENT term
     * commits by counting replicas. Earlier terms ride along once a
     * current-term entry above them commits. Without this a new leader
     * can count an old entry to a majority, commit it, and then lose it
     * to a node that never held it. */
    return term_at_candidate == current_term;
}

/* ---- leader backoff ----------------------------------------------------- */

void raft_backoff(uint64_t hint, int have_hint, uint64_t next, uint64_t match,
                  raft_backoff_out *out) {
    uint64_t decrement = next > 1 ? next - 1 : 1;
    uint64_t target = have_hint ? hint : decrement;
    /* Never forward of a plain decrement: a hint is help, not authority. */
    if (target > decrement) target = decrement;
    if (target < 1) target = 1;

    out->next = target;
    out->match = match;
    out->match_regressed = 0;
    if (target <= match) {
        /* Only a follower that lost its disk can rewind below what we saw
         * it hold. Drop match with it, or the leader will count a node
         * with nothing toward a quorum. */
        out->match = target - 1;
        out->match_regressed = 1;
    }
}
