/* raft_node.c — see raft_node.h. */
#include "raft_node.h"

#include "raft_core.h"
#include "raft_drive.h"
#include "raft_msg.h"
#include "bjcursor.h"

#include <stdlib.h>
#include <string.h>

#define RN_MAX_PEERS 64
#define RN_MAX_OUT   256

/*
 * The effect queue is sized from the peer cap, not guessed.
 *
 * Three of the six kinds are ACTIONABLE and per-peer -- NEEDS_SNAPSHOT,
 * PROMOTE, REACHABLE -- and emit() coalesces those by (kind, peer), so
 * however many calls a host makes between drains, they can only occupy
 * three slots per peer. COMMIT coalesces to its highest index. What is
 * left is the narrative: ROLE, ELECTION, TRUNCATED, which append,
 * because collapsing a role trail loses the transition it exists to
 * report.
 *
 * So the queue cannot overflow within any sequence of calls that a host
 * drains between, which is the contract raft_node.h states. The static
 * assertion below is the arithmetic; rn_effects_lost is the belt for the
 * host that ignores the contract, or for a future emit site that breaks
 * the premise. It was 64 -- smaller than the 64 NEEDS_SNAPSHOT a single
 * tick can raise on a full cluster.
 */
#define RN_MAX_EFF   (RN_MAX_PEERS * 3 + 16)

typedef struct {
    uint64_t id;
    int      voting;
    uint64_t next;       /* next index to send                        */
    uint64_t match;      /* highest index known replicated            */
    int64_t  ack_at;     /* when it last answered without deposing us */
    int      reachable;  /* edge-triggered; -1 = never tried          */
    uint32_t inflight;   /* correlation id outstanding, 0 = none      */
} rn_peer;

typedef struct {
    uint64_t peer;
    uint32_t corr;
    int      is_reply;
    dbuf     bytes;
} rn_out;

typedef struct {
    int      kind;
    uint64_t arg;
    int      flag;
} rn_eff;

struct raft_node {
    uint64_t self_id;
    elog    *log;

    int      role;
    uint64_t leader_id;
    uint64_t commit_index;
    int      running;
    int      quiesced;

    rn_peer  peers[RN_MAX_PEERS];
    uint32_t npeers;      /* excludes self                              */
    int      self_voting;
    uint32_t voter_count; /* including self when self_voting             */

    int64_t  now;
    int64_t  election_deadline;
    int64_t  heartbeat_due;
    int64_t  last_leader_contact;
    int64_t  leader_at;
    int64_t  min_election, max_election, heartbeat_ms;
    uint32_t max_batch_bytes;

    raft_round round;
    int        round_live;
    /* The correlation ids this round's requests went out with. A vote
     * reply is not tracked per peer (the round tallies them), so without
     * this a straggling reply from the PRE-VOTE round lands in the real
     * round that followed it -- and since a pre-voter grants freely, two
     * candidates can both reach a quorum in one term. Corr ids are issued
     * in order, so the round owns a contiguous range. */
    uint32_t   round_corr_lo, round_corr_hi;

    uint32_t next_corr;

    rn_out   out[RN_MAX_OUT];
    uint32_t nout;
    rn_eff   eff[RN_MAX_EFF];
    uint32_t neff;
    /* Sticky: this node once had something to say and no room to say it.
     * Never cleared -- the host's view of it is incomplete from that
     * moment on, and the only safe reading is that it stays so. */
    int      effects_lost;
};

/* The coalescing argument above, as arithmetic the compiler checks:
 * three actionable slots per peer, plus headroom for the transitions
 * one call can raise. If a new per-peer effect kind arrives, this fails
 * here rather than in production. */
_Static_assert(RN_MAX_EFF >= RN_MAX_PEERS * 3 + 8,
               "effect queue must hold every peer's actionable effects at once");

/* ---- small helpers ------------------------------------------------------ */

static rn_peer *peer_of(raft_node *n, uint64_t id) {
    for (uint32_t i = 0; i < n->npeers; i++)
        if (n->peers[i].id == id) return &n->peers[i];
    return NULL;
}

/*
 * Queue one effect, coalescing what can be coalesced.
 *
 * The per-peer kinds say "this peer needs something"; saying it twice
 * asks the host for the same idempotent act twice, so the newer report
 * replaces the older in place. COMMIT keeps its highest index, since the
 * host reads the real commit index off the node anyway and the argument
 * is only a nudge. Everything else is a transition and appends.
 *
 * Dropping was the old behaviour when the queue filled, and dropping is
 * exactly what must not happen: a lost COMMIT stalls the apply pump
 * until the next one, a lost ROLE leaves the host's idea of this node's
 * role behind, and neither is visible to anyone. If it ever fills now,
 * the node records that it failed to speak, and the host halts on it.
 */
static void emit(raft_node *n, int kind, uint64_t arg, int flag) {
    if (kind == RN_EFFECT_NEEDS_SNAPSHOT || kind == RN_EFFECT_PROMOTE ||
        kind == RN_EFFECT_REACHABLE) {
        for (uint32_t i = 0; i < n->neff; i++) {
            if (n->eff[i].kind != kind || n->eff[i].arg != arg) continue;
            n->eff[i].flag = flag;   /* the newer report wins */
            return;
        }
    } else if (kind == RN_EFFECT_COMMIT) {
        for (uint32_t i = 0; i < n->neff; i++) {
            if (n->eff[i].kind != kind) continue;
            if (arg > n->eff[i].arg) n->eff[i].arg = arg;
            return;
        }
    }
    if (n->neff >= RN_MAX_EFF) { n->effects_lost = 1; return; }
    n->eff[n->neff].kind = kind;
    n->eff[n->neff].arg  = arg;
    n->eff[n->neff].flag = flag;
    n->neff++;
}

/* Queue one message. Takes the bytes by copy: the caller's builder is
 * usually freed before the host drains. */
static int queue(raft_node *n, uint64_t peer, uint32_t corr, int is_reply,
                 const uint8_t *bytes, size_t len) {
    if (n->nout >= RN_MAX_OUT) return BJ_ERR_RANGE;
    rn_out *o = &n->out[n->nout];
    o->peer = peer;
    o->corr = corr;
    o->is_reply = is_reply;
    o->bytes.len = 0;
    int e = dbuf_put(&o->bytes, bytes, len);
    if (e) return e;
    n->nout++;
    return BJ_OK;
}

static uint32_t fresh_corr(raft_node *n) {
    if (++n->next_corr == 0) n->next_corr = 1;   /* 0 means "none" */
    return n->next_corr;
}

static void set_role(raft_node *n, int role) {
    if (n->role == role) return;
    n->role = role;
    emit(n, RN_EFFECT_ROLE, (uint64_t)role, 0);
}

static void arm_election(raft_node *n, double random01) {
    if (n->quiesced || !n->running) { n->election_deadline = INT64_MAX; return; }
    int64_t span = n->max_election - n->min_election;
    if (span < 0) span = 0;
    n->election_deadline = n->now + n->min_election + (int64_t)(random01 * (double)span);
}

/* ---- lifecycle ---------------------------------------------------------- */

raft_node *rn_new(uint64_t self_id, elog *log) {
    if (!log || self_id == 0) return NULL;
    raft_node *n = (raft_node *)calloc(1, sizeof(raft_node));
    if (!n) return NULL;
    n->self_id = self_id;
    n->log = log;
    n->role = RAFT_FOLLOWER;
    n->self_voting = 1;
    n->voter_count = 1;
    n->min_election = 150;
    n->max_election = 300;
    n->heartbeat_ms = 50;
    n->max_batch_bytes = 65536;
    n->election_deadline = INT64_MAX;
    n->last_leader_contact = INT64_MIN;
    return n;
}

void rn_free(raft_node *n) {
    if (!n) return;
    for (uint32_t i = 0; i < RN_MAX_OUT; i++) dbuf_free(&n->out[i].bytes);
    free(n);
}

void rn_set_log(raft_node *n, elog *log) {
    if (log) n->log = log;
}

void rn_set_timing(raft_node *n, int64_t min_election, int64_t max_election,
                   int64_t heartbeat) {
    n->min_election = min_election;
    n->max_election = max_election < min_election ? min_election : max_election;
    n->heartbeat_ms = heartbeat;
}

void rn_set_limits(raft_node *n, uint32_t max_batch_bytes) {
    if (max_batch_bytes) n->max_batch_bytes = max_batch_bytes;
}

void rn_start(raft_node *n, int64_t now, double random01) {
    n->running = 1;
    n->now = now;
    arm_election(n, random01);
}

void rn_stop(raft_node *n) {
    n->running = 0;
    n->election_deadline = INT64_MAX;
}

/* ---- membership --------------------------------------------------------- */

/*
 * Adopt a member set, or adopt NONE of it.
 *
 * Everything is built into scratch and validated before a single field
 * of the node changes, so a refusal leaves the previous set exactly as
 * it was. That is not tidiness: a node that half-adopts has a peer list,
 * a voter count and a quorum that describe three different clusters, and
 * every safety argument in raft_core.h is stated over one cluster.
 *
 * Every parse failure is returned rather than broken out of. This used
 * to truncate at RN_MAX_PEERS and stop at the first unreadable element,
 * both silently, both leaving the host's own member list -- derived from
 * the same raft_members_adopt -- describing a cluster this node was not
 * actually replicating to.
 */
int rn_set_members(raft_node *n, const uint8_t *members, uint32_t len) {
    dbuf adopted = {0};
    int e = raft_members_adopt(members, len, n->self_id, &adopted);
    if (e) { dbuf_free(&adopted); return e; }

    /*
     * raft_members_adopt hands back { members, voters, peers }. The
     * cursors of a peer that survives the change survive with it: a
     * membership edit is not a reason to re-send a follower its whole
     * log. New peers start where a fresh leader would put them.
     */
    rn_peer next[RN_MAX_PEERS];
    uint32_t nnext = 0;
    int self_voting = 0;
    uint32_t voter_count = 0;

    const uint8_t *v; size_t vlen; int found = 0;
    uint64_t last = elog_last_index(n->log);
    uint32_t cnt = 0;

    e = obj_get_field(adopted.data, adopted.len, (const uint8_t *)"peers", 5, &v, &vlen, &found);
    if (e || !found) { dbuf_free(&adopted); return e ? e : RAFT_ERR_MEMBER; }
    {
        cur c = { v, vlen, 0 };
        if (array_begin(&c, &cnt) != BJ_OK) { dbuf_free(&adopted); return RAFT_ERR_MEMBER; }
        /* Refused whole, never trimmed to fit: a cluster this build
         * cannot represent is a cluster it must not pretend to. */
        if (cnt > RN_MAX_PEERS) { dbuf_free(&adopted); return RAFT_ERR_CAPACITY; }
        for (uint32_t i = 0; i < cnt; i++) {
            double d;
            if (read_number(&c, &d) != BJ_OK) { dbuf_free(&adopted); return RAFT_ERR_MEMBER; }
            rn_peer *p = &next[nnext++];
            memset(p, 0, sizeof(*p));
            p->id = (uint64_t)d;
            p->reachable = -1;
            p->ack_at = INT64_MIN;   /* never heard from; see check-quorum */
            p->next = last + 1;
            for (uint32_t j = 0; j < n->npeers; j++) {
                if (n->peers[j].id != p->id) continue;
                p->next = n->peers[j].next;
                p->match = n->peers[j].match;
                p->ack_at = n->peers[j].ack_at;
                p->reachable = n->peers[j].reachable;
                p->inflight = n->peers[j].inflight;
                break;
            }
        }
    }

    found = 0;
    e = obj_get_field(adopted.data, adopted.len, (const uint8_t *)"voters", 6, &v, &vlen, &found);
    if (e || !found) { dbuf_free(&adopted); return e ? e : RAFT_ERR_MEMBER; }
    {
        cur c = { v, vlen, 0 };
        if (array_begin(&c, &cnt) != BJ_OK) { dbuf_free(&adopted); return RAFT_ERR_MEMBER; }
        for (uint32_t i = 0; i < cnt; i++) {
            double d;
            if (read_number(&c, &d) != BJ_OK) { dbuf_free(&adopted); return RAFT_ERR_MEMBER; }
            uint64_t id = (uint64_t)d;
            voter_count++;
            if (id == n->self_id) { self_voting = 1; continue; }
            /* A voter who is not a member: the two lists contradict each
             * other, and counting it would put a node in the quorum
             * arithmetic that has no cursor to replicate to. */
            rn_peer *p = NULL;
            for (uint32_t j = 0; j < nnext; j++) if (next[j].id == id) { p = &next[j]; break; }
            if (!p) { dbuf_free(&adopted); return RAFT_ERR_MEMBER; }
            p->voting = 1;
        }
    }

    /* Nothing above touched the node. Commit the whole set at once. */
    if (nnext) memcpy(n->peers, next, sizeof(rn_peer) * nnext);
    n->npeers = nnext;
    n->self_voting = self_voting;
    n->voter_count = voter_count;

    dbuf_free(&adopted);
    return BJ_OK;
}

uint32_t rn_max_peers(void) { return RN_MAX_PEERS; }

uint32_t rn_quorum(const raft_node *n) { return raft_quorum(n->voter_count); }

/* ---- role transitions --------------------------------------------------- */

static void become_follower(raft_node *n, uint64_t term, uint64_t leader_id,
                            double random01) {
    if (term > elog_current_term(n->log)) elog_set_hard_state(n->log, term, 0);
    n->leader_id = leader_id;
    n->round_live = 0;
    set_role(n, RAFT_FOLLOWER);
    arm_election(n, random01);
}

static int replicate_to(raft_node *n, rn_peer *p);
static void advance_commit(raft_node *n);

/* Replicate to everyone, keeping the FIRST failure. A message that could
 * not be queued is a message the host will never send, so the caller
 * has to hear about it -- these loops used to discard it, which turned a
 * full outbox into a leader that silently stopped talking to some of its
 * followers. */
static int replicate_to_all(raft_node *n) {
    int first = BJ_OK;
    for (uint32_t i = 0; i < n->npeers; i++) {
        int e = replicate_to(n, &n->peers[i]);
        if (e && !first) first = e;
    }
    return first;
}

static int become_leader(raft_node *n) {
    n->leader_id = n->self_id;
    n->leader_at = n->now;
    n->round_live = 0;
    uint64_t last = elog_last_index(n->log);
    for (uint32_t i = 0; i < n->npeers; i++) {
        n->peers[i].next = last + 1;
        n->peers[i].match = 0;
        /* Acks from a previous term say nothing about this one. */
        n->peers[i].ack_at = INT64_MIN;
        n->peers[i].inflight = 0;
    }
    set_role(n, RAFT_LEADER);

    /*
     * The term-boundary no-op (section 5.4.2). Only a current-term entry
     * commits by counting replicas, so without this an idle leader can
     * never commit the tail it inherited.
     */
    uint64_t at = 0;
    int e = elog_append(n->log, elog_current_term(n->log), EL_NOOP,
                        (const uint8_t *)"", 0, &at);
    (void)at;
    if (e) return e;
    e = elog_sync(n->log);
    if (e) return e;

    n->heartbeat_due = n->now;
    e = replicate_to_all(n);
    if (e) return e;

    /* A single-voter group has nobody to hear from, so nothing will ever
     * arrive to trigger this: it commits the moment it appends. Without
     * this line such a group elects a leader and then commits nothing,
     * forever -- which is how the JS found it too (src/raft.js's
     * _becomeLeader ends with the same call, commented "single-node
     * clusters"). */
    advance_commit(n);
    return BJ_OK;
}

/* ---- elections ---------------------------------------------------------- */

static int start_election(raft_node *n, int pre_vote, double random01) {
    if (n->role == RAFT_LEADER) return BJ_OK;
    uint64_t term = elog_current_term(n->log) + 1;

    if (!pre_vote) {
        int e = elog_set_hard_state(n->log, term, n->self_id);
        if (e) return e;
        n->leader_id = 0;
        set_role(n, RAFT_CANDIDATE);
    }
    arm_election(n, random01);
    emit(n, RN_EFFECT_ELECTION, term, pre_vote);

    uint32_t quorum = rn_quorum(n);
    raft_round_begin(&n->round, term, quorum, pre_vote);
    n->round_live = 1;
    n->round_corr_lo = n->round_corr_hi = 0;

    if (quorum <= 1) {
        n->round_live = 0;
        return pre_vote ? start_election(n, 0, random01) : become_leader(n);
    }

    dbuf msg = {0};
    int e = rmsg_build_request_vote(term, n->self_id, elog_last_index(n->log),
                                    elog_last_term(n->log), pre_vote, &msg);
    if (!e) {
        for (uint32_t i = 0; i < n->npeers && !e; i++) {
            if (!n->peers[i].voting) continue;
            uint32_t corr = fresh_corr(n);
            if (!n->round_corr_lo) n->round_corr_lo = corr;
            n->round_corr_hi = corr;
            e = queue(n, n->peers[i].id, corr, 0, msg.data, msg.len);
        }
    }
    dbuf_free(&msg);
    return e;
}

/* ---- replication -------------------------------------------------------- */

static int replicate_to(raft_node *n, rn_peer *p) {
    if (n->role != RAFT_LEADER || p->inflight) return BJ_OK;

    uint64_t base = elog_base_index(n->log);
    int action = raft_repl_decide(p->next, base, 0 /* the host serves snapshots */);
    if (action != RAFT_REPL_APPEND) {
        emit(n, RN_EFFECT_NEEDS_SNAPSHOT, p->id, 0);
        return BJ_OK;
    }

    uint64_t term = elog_current_term(n->log);
    uint64_t prev_term = 0;
    if (p->next - 1 > base) {
        int e = elog_term_at(n->log, p->next - 1, &prev_term);
        if (e) prev_term = 0;
    } else if (p->next - 1 == base) {
        prev_term = elog_base_term(n->log);
    }

    dbuf msg = {0};
    uint32_t count = 0;
    int e = rmsg_build_append_entries(n->log, term, n->self_id, p->next, prev_term,
                                      n->commit_index, n->max_batch_bytes,
                                      n->quiesced, &count, &msg);
    if (!e) {
        uint32_t corr = fresh_corr(n);
        e = queue(n, p->id, corr, 0, msg.data, msg.len);
        if (!e) p->inflight = corr;
    }
    dbuf_free(&msg);
    return e;
}

int rn_replicate(raft_node *n, uint64_t peer) {
    rn_peer *p = peer_of(n, peer);
    if (!p) return RAFT_ERR_PEER;
    return replicate_to(n, p);
}

int rn_installed(raft_node *n, uint64_t peer, uint64_t boundary) {
    rn_peer *p = peer_of(n, peer);
    if (!p) return RAFT_ERR_PEER;
    raft_repl_installed(boundary, &p->match, &p->next);
    /* The install may have carried this peer past what a quorum needed. */
    advance_commit(n);
    return BJ_OK;
}

/* The highest index a quorum holds, if the figure-8 rule lets it commit. */
static void advance_commit(raft_node *n) {
    if (n->role != RAFT_LEADER) return;
    uint64_t matches[RN_MAX_PEERS];
    uint32_t m = 0;
    for (uint32_t i = 0; i < n->npeers; i++)
        if (n->peers[i].voting) matches[m++] = n->peers[i].match;

    uint64_t last = elog_last_index(n->log);
    uint64_t candidate = 0;
    if (!raft_commit_candidate(last, matches, m, rn_quorum(n), &candidate)) return;
    if (candidate <= n->commit_index) return;

    uint64_t base = elog_base_index(n->log);
    uint64_t term_at = 0;
    if (candidate > base && candidate <= last) elog_term_at(n->log, candidate, &term_at);
    if (!raft_may_commit(candidate, n->commit_index, base, term_at,
                         elog_current_term(n->log))) return;
    n->commit_index = candidate;
    /* Advisory: it rides the next sync, and a restart re-derives what it
     * cannot prove. Without it a rebooted node waits for a heartbeat
     * before it may replay its own committed prefix. */
    elog_set_commit_index(n->log, candidate);
    emit(n, RN_EFFECT_COMMIT, candidate, 0);
}

/* ---- the clock ---------------------------------------------------------- */

int rn_tick(raft_node *n, int64_t now, double random01) {
    if (!n->running) return BJ_OK;
    n->now = now;
    if (n->quiesced) return BJ_OK;

    if (n->role == RAFT_LEADER) {
        if (now >= n->heartbeat_due) {
            n->heartbeat_due = now + n->heartbeat_ms;
            return replicate_to_all(n);
        }
        return BJ_OK;
    }

    if (now >= n->election_deadline) {
        /* Pre-vote first: a straw poll persists nothing, so a node that
         * cannot win does not bump the term and disturb a live leader. */
        if (!n->self_voting) { arm_election(n, random01); return BJ_OK; }
        return start_election(n, 1, random01);
    }
    return BJ_OK;
}

void rn_quiesce(raft_node *n) {
    n->quiesced = 1;
    n->election_deadline = INT64_MAX;
}

void rn_wake(raft_node *n, int64_t now, double random01) {
    n->quiesced = 0;
    n->now = now;
    arm_election(n, random01);
}

/* ---- incoming ----------------------------------------------------------- */

static void fill_state(const raft_node *n, raft_msg_state *st) {
    st->self_id             = n->self_id;
    st->is_follower         = n->role == RAFT_FOLLOWER;
    st->is_leader           = n->role == RAFT_LEADER;
    st->self_is_voter       = n->self_voting;
    st->leader_id           = n->leader_id;
    st->commit_index        = n->commit_index;
    st->now                 = n->now;
    st->last_leader_contact = n->last_leader_contact;
    st->min_election_timeout= n->min_election;
}

static void adopt(raft_node *n, const raft_msg_effect *eff, double random01) {
    if (eff->became_follower) become_follower(n, elog_current_term(n->log),
                                              eff->new_leader_id, random01);
    if (eff->touched_leader) {
        n->leader_id = eff->new_leader_id;
        n->last_leader_contact = eff->new_last_leader_contact;
    }
    if (eff->reset_election_timer) arm_election(n, random01);
    if (eff->truncated_from) emit(n, RN_EFFECT_TRUNCATED, eff->truncated_from, 0);
    if (eff->new_commit_index > n->commit_index) {
        n->commit_index = eff->new_commit_index;
        emit(n, RN_EFFECT_COMMIT, n->commit_index, 0);
    }
    if (eff->quiesce) rn_quiesce(n);
}

int rn_handle(raft_node *n, uint64_t from, uint32_t corr,
              const uint8_t *msg, uint32_t len, double random01) {
    int kind = -1;
    int e = rmsg_kind(msg, len, &kind);
    if (e) return e;

    raft_msg_state st;
    fill_state(n, &st);
    raft_msg_effect eff;
    memset(&eff, 0, sizeof(eff));
    dbuf reply = {0};

    if (kind == RAFT_MSG_REQUEST_VOTE) {
        e = rmsg_handle_request_vote(n->log, &st, msg, len, &eff, &reply);
    } else if (kind == RAFT_MSG_APPEND_ENTRIES) {
        e = rmsg_handle_append_entries(n->log, &st, msg, len, &eff, &reply);
    } else {
        /* Everything else -- install, join, leave, timeoutNow -- needs
         * host resources (files, a promise, a membership proposal), so
         * it is the host's to answer. See raft_node.h. */
        dbuf_free(&reply);
        return RAFT_ERR_MESSAGE;
    }
    if (!e) {
        adopt(n, &eff, random01);
        e = queue(n, from, corr, 1, reply.data, reply.len);
    }
    dbuf_free(&reply);
    return e;
}

/* ---- replies ------------------------------------------------------------ */

/* Find the peer whose outstanding request carries `corr`. */
static rn_peer *peer_by_corr(raft_node *n, uint32_t corr) {
    for (uint32_t i = 0; i < n->npeers; i++)
        if (n->peers[i].inflight == corr) return &n->peers[i];
    return NULL;
}

static int on_vote_reply(raft_node *n, uint32_t corr, const uint8_t *reply,
                         uint32_t len, double random01) {
    if (!n->round_live) return BJ_OK;
    /* From a round that is over -- most sharply, the pre-vote round this
     * very campaign grew out of, whose grants would otherwise elect us
     * on a straw poll. */
    if (corr < n->round_corr_lo || corr > n->round_corr_hi) return BJ_OK;

    const uint8_t *v; size_t vlen; int found = 0;
    double term = 0;
    int granted = 0;
    if (obj_get_field(reply, len, (const uint8_t *)"term", 4, &v, &vlen, &found) == BJ_OK && found) {
        cur c = { v, vlen, 0 };
        read_number(&c, &term);
    }
    found = 0;
    if (obj_get_field(reply, len, (const uint8_t *)"voteGranted", 11, &v, &vlen, &found) == BJ_OK && found) {
        cur c = { v, vlen, 0 };
        read_bool(&c, &granted);
    }

    uint64_t step = 0;
    int action = raft_round_on_reply(&n->round, (uint64_t)term, granted,
                                     elog_current_term(n->log),
                                     n->role == RAFT_LEADER,
                                     n->role == RAFT_CANDIDATE, &step);
    switch (action) {
        case RAFT_ROUND_STEP_DOWN:
            n->round_live = 0;
            become_follower(n, step, 0, random01);
            return BJ_OK;
        case RAFT_ROUND_WON: {
            int pre = n->round.pre_vote;
            n->round_live = 0;
            return pre ? start_election(n, 0, random01) : become_leader(n);
        }
        case RAFT_ROUND_IGNORE:
            n->round_live = 0;
            return BJ_OK;
        default:
            return BJ_OK;
    }
}

static int on_append_reply(raft_node *n, rn_peer *p, const uint8_t *reply, uint32_t len,
                           double random01) {
    uint64_t term = elog_current_term(n->log);
    const uint8_t *v; size_t vlen; int found = 0;
    double rterm = 0, match = 0, hint = 0;
    int success = 0, have_hint = 0;

    if (obj_get_field(reply, len, (const uint8_t *)"term", 4, &v, &vlen, &found) == BJ_OK && found) {
        cur c = { v, vlen, 0 }; read_number(&c, &rterm);
    }
    found = 0;
    if (obj_get_field(reply, len, (const uint8_t *)"success", 7, &v, &vlen, &found) == BJ_OK && found) {
        cur c = { v, vlen, 0 }; read_bool(&c, &success);
    }
    found = 0;
    if (obj_get_field(reply, len, (const uint8_t *)"matchIndex", 10, &v, &vlen, &found) == BJ_OK && found) {
        cur c = { v, vlen, 0 }; read_number(&c, &match);
    }
    found = 0;
    if (obj_get_field(reply, len, (const uint8_t *)"hintIndex", 9, &v, &vlen, &found) == BJ_OK && found) {
        cur c = { v, vlen, 0 };
        if (read_number(&c, &hint) == BJ_OK) have_hint = 1;
    }

    if (n->role != RAFT_LEADER) return BJ_OK;
    if ((uint64_t)rterm > term) { become_follower(n, (uint64_t)rterm, 0, random01); return BJ_OK; }

    /* Any reply that did not depose us proves the peer is alive and still
     * accepts our leadership -- a log-conflict rejection counts as much
     * as a success for check-quorum. */
    p->ack_at = n->now;
    if (p->reachable != 1) { p->reachable = 1; emit(n, RN_EFFECT_REACHABLE, p->id, 1); }

    if (success) {
        if ((uint64_t)match > p->match) {
            p->match = (uint64_t)match;
            p->next = p->match + 1;
            advance_commit(n);
        }
        if (!p->voting && n->commit_index && p->match >= n->commit_index)
            emit(n, RN_EFFECT_PROMOTE, p->id, 0);
        if (p->next <= elog_last_index(n->log)) return replicate_to(n, p);
        return BJ_OK;
    }

    raft_backoff_out b;
    raft_backoff((uint64_t)hint, have_hint, p->next, p->match, &b);
    p->next = b.next;
    p->match = b.match;
    return replicate_to(n, p);
}

int rn_on_reply(raft_node *n, uint32_t corr, const uint8_t *reply, uint32_t len,
                double random01) {
    rn_peer *p = peer_by_corr(n, corr);
    if (p) {
        p->inflight = 0;
        return on_append_reply(n, p, reply, len, random01);
    }
    /* Not an append: the only other thing we send is a vote request, and
     * those are not tracked per peer because the round tallies them --
     * it checks the correlation id against its own range instead. */
    return on_vote_reply(n, corr, reply, len, random01);
}

int rn_on_fail(raft_node *n, uint32_t corr) {
    rn_peer *p = peer_by_corr(n, corr);
    if (!p) return BJ_OK;   /* a vote request nobody answered; the timer retries */
    p->inflight = 0;
    if (p->reachable != 0) { p->reachable = 0; emit(n, RN_EFFECT_REACHABLE, p->id, 0); }
    return BJ_OK;
}

/* ---- outbox and effects ------------------------------------------------- */

uint32_t rn_out_count(const raft_node *n) { return n->nout; }
uint64_t rn_out_peer(const raft_node *n, uint32_t i) { return i < n->nout ? n->out[i].peer : 0; }
uint32_t rn_out_corr(const raft_node *n, uint32_t i) { return i < n->nout ? n->out[i].corr : 0; }
int      rn_out_is_reply(const raft_node *n, uint32_t i) { return i < n->nout ? n->out[i].is_reply : 0; }

const uint8_t *rn_out_bytes(const raft_node *n, uint32_t i, uint32_t *len) {
    if (i >= n->nout) { *len = 0; return NULL; }
    *len = (uint32_t)n->out[i].bytes.len;
    return n->out[i].bytes.data;
}

void rn_out_clear(raft_node *n) {
    for (uint32_t i = 0; i < n->nout; i++) n->out[i].bytes.len = 0;
    n->nout = 0;
}

uint32_t rn_effect_count(const raft_node *n) { return n->neff; }
int      rn_effect_kind_at(const raft_node *n, uint32_t i) { return i < n->neff ? n->eff[i].kind : -1; }
uint64_t rn_effect_arg(const raft_node *n, uint32_t i) { return i < n->neff ? n->eff[i].arg : 0; }
int      rn_effect_flag(const raft_node *n, uint32_t i) { return i < n->neff ? n->eff[i].flag : 0; }
void     rn_effects_clear(raft_node *n) { n->neff = 0; }
/* Deliberately NOT cleared by rn_effects_clear: once something was lost,
 * the host's picture of this node has a hole in it that draining cannot
 * fill. */
int      rn_effects_lost(const raft_node *n) { return n->effects_lost; }

/* ---- accessors ---------------------------------------------------------- */

int      rn_role(const raft_node *n) { return n->role; }
uint64_t rn_leader_id(const raft_node *n) { return n->leader_id; }
uint64_t rn_commit_index(const raft_node *n) { return n->commit_index; }

uint64_t rn_match(const raft_node *n, uint64_t peer) {
    const rn_peer *p = peer_of((raft_node *)n, peer);
    return p ? p->match : 0;
}
uint64_t rn_next(const raft_node *n, uint64_t peer) {
    const rn_peer *p = peer_of((raft_node *)n, peer);
    return p ? p->next : 0;
}
uint32_t rn_inflight(const raft_node *n, uint64_t peer) {
    const rn_peer *p = peer_of((raft_node *)n, peer);
    return p ? p->inflight : 0;
}

int rn_is_quiesced(const raft_node *n) { return n->quiesced; }

/* ---- what the host still owns ------------------------------------------- */

int rn_propose(raft_node *n, int type, const uint8_t *payload, uint32_t len,
               uint64_t *out_index) {
    if (n->role != RAFT_LEADER) return BJ_ERR_STATE;

    uint64_t at = 0;
    int e = elog_append(n->log, elog_current_term(n->log), type, payload, len, &at);
    if (e) return e;
    /* Durable before it counts toward anything: the leader counting
     * itself is the same ack a follower gives, and it must mean the same
     * thing. */
    e = elog_sync(n->log);
    if (e) return e;
    if (out_index) *out_index = at;

    e = replicate_to_all(n);
    advance_commit(n);   /* single-voter groups: see become_leader */
    return e;
}

void rn_seed_commit(raft_node *n, uint64_t index) {
    if (index > n->commit_index) n->commit_index = index;
}

int rn_campaign(raft_node *n, double random01) {
    if (!n->running || n->role == RAFT_LEADER || !n->self_voting) return BJ_OK;
    n->quiesced = 0;
    return start_election(n, 0, random01);
}

int rn_observe_leader(raft_node *n, uint64_t term, uint64_t leader_id, double random01) {
    uint64_t current = elog_current_term(n->log);
    if (term < current) return 0;
    if (term > current || n->role != RAFT_FOLLOWER) {
        become_follower(n, term, leader_id, random01);
    }
    n->leader_id = leader_id;
    n->last_leader_contact = n->now;
    arm_election(n, random01);
    return 1;
}

int rn_step_down(raft_node *n, uint64_t term, double random01) {
    if (term < elog_current_term(n->log)) return 0;
    if (term == elog_current_term(n->log) && n->role == RAFT_FOLLOWER) return 0;
    become_follower(n, term, 0, random01);
    return 1;
}

int rn_has_quorum_contact(const raft_node *n, int64_t within_ms) {
    if (n->role != RAFT_LEADER) return 1;
    uint32_t live = n->self_voting ? 1 : 0;
    for (uint32_t i = 0; i < n->npeers; i++) {
        if (!n->peers[i].voting) continue;
        /* Never answered us this term: silence is not contact, however
         * recently the term began (the arithmetic would overflow here
         * anyway). */
        if (n->peers[i].ack_at == INT64_MIN) continue;
        if (n->now - n->peers[i].ack_at <= within_ms) live++;
    }
    return live >= rn_quorum(n);
}
