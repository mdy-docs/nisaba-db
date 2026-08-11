/* raft_node.c — see raft_node.h. */
#include "raft_node.h"

#include "raft_core.h"
#include "raft_drive.h"
#include "raft_msg.h"
#include "bjcursor.h"
#include "bjfile.h"   /* bjfile_crc32: a staged file is checksummed as it lands */

#include <stdlib.h>
#include <string.h>

#define RN_MAX_PEERS 64
#define RN_MAX_OUT   256
#define RN_MAX_PENDING 16

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
/*
 * How many proposals this node will owe an answer for at once.
 *
 * Bounded on purpose, like every other table here: a leader that accepts
 * unbounded work in flight fails by growing until something else does,
 * which is the failure nobody can attribute. Full is RAFT_ERR_CAPACITY
 * at rn_propose, where a caller is still standing and can back off.
 */
/*
 * Read barriers outstanding at once. One per client connection is the
 * bound the server imposes -- a connection is served one request at a
 * time and is not read from while it owes an answer -- so this matches
 * MAX_CLIENTS and DBS_MAX_INFLIGHT, and a further one is refused rather
 * than queued. They all share one heartbeat round, so the number costs
 * bookkeeping and not messages.
 */
#define RN_MAX_READS 64

#define RN_MAX_AWAIT 256

/* ... and the queue has to hold the settlements they turn into. One
 * apply pass can finish every one of them, and unlike the per-peer
 * kinds a settlement cannot coalesce: two indices are two answers to two
 * different clients. So the term above is added rather than assumed to
 * fit -- the arithmetic is the contract. */
#define RN_MAX_EFF   (RN_MAX_PEERS * 3 + 16 + RN_MAX_AWAIT)

/* Bytes per snapshot chunk -- the same figure src/raft.js defaults its
 * snapshotChunkBytes to, so a transfer is cut the same way whichever
 * host is serving it. */
#define RN_DEFAULT_CHUNK (64u * 1024u)

typedef struct {
    uint64_t id;
    int      voting;
    uint64_t next;       /* next index to send                        */
    uint64_t match;      /* highest index known replicated            */
    int64_t  ack_at;     /* when it last answered without deposing us */
    int      reachable;  /* edge-triggered; -1 = never tried          */
    uint64_t inflight;   /* correlation id outstanding, 0 = none      */
    /*
     * When the request now outstanding was SENT, and how far back an
     * answer to it proves this peer still follows us.
     *
     * A read barrier turns on this distinction and nothing else does.
     * An ack proves the peer had not moved to a higher term at the
     * moment it PROCESSED the request -- which is at or after the send,
     * and says nothing about any earlier instant. So a barrier taken at
     * T is covered by an ack only if the request it answers went out at
     * or after T; `ack_at` (when the answer arrived) is the wrong stamp
     * and would confirm a barrier with evidence predating it.
     */
    int64_t  sent_at;
    int64_t  ack_covers;

    /*
     * A snapshot transfer in flight to this peer. `installing` is what
     * tells rn_on_reply which kind of reply it is holding -- the
     * correlation id alone cannot say, and reading an install's answer
     * as an AppendEntries' would advance a match index on the strength
     * of a chunk.
     *
     * The cursor is the chunk walk's (raft_drive.h), carried back from
     * the last chunk rather than derived: offset + len cannot tell "sent
     * the empty file" from "have not", which is an infinite loop on any
     * snapshot whose last file is empty.
     */
    int      installing;
    uint64_t install_term;   /* our term when it started; a change voids it */
    uint32_t cursor_file;
    uint64_t cursor_offset;
    int      chunk_done;     /* the chunk outstanding was the last one      */
} rn_peer;

typedef struct {
    uint64_t peer;
    uint64_t corr;
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
    /* The cluster's durable identity, or 0 for a node that has never been
     * given one. Reported, never interpreted here: what a mismatch means
     * is the host's business (server/replica.c). */
    uint64_t group;
    elog    *log;

    /*
     * Serving snapshots (raft_node.h). Both borrowed; both NULL means
     * the host serves them and this node only says who needs one.
     *
     * The manifest and the file sizes are cached per GENERATION rather
     * than per peer: three followers bootstrapping at once are three
     * cursors over one snapshot, and re-encoding the manifest for each
     * chunk of each of them would be the only expensive thing in the
     * transfer.
     */
    bj_ns   *ns;
    sst     *store;
    uint32_t chunk_bytes;
    uint64_t snap_gen;         /* 0 = nothing cached                     */
    uint64_t snap_index, snap_term;
    dbuf     snap_manifest;    /* exactly what goes on the wire          */
    uint64_t snap_sizes[RN_MAX_SNAP_FILES];
    uint32_t snap_nfiles;

    /*
     * An install being STAGED into a new generation. The leader's
     * manifest is kept verbatim because it is what the staged bytes are
     * checked against at the end -- by sst_check_files, which is the one
     * place that rule lives (the JS store's verify(), the replicated
     * install path and the Raft harness each used to have a copy).
     *
     * `written` is per file and is also the chunk-order check: a chunk
     * whose offset is not exactly what has been written is a stream that
     * lost or reordered one, and staging it would produce a file that
     * passes no checksum but might pass a size.
     */
    struct {
        int      active;
        uint64_t gen;
        uint64_t index, term;
        dbuf     manifest;
        uint32_t nfiles;
        uint64_t written[RN_MAX_SNAP_FILES];
        uint32_t crc[RN_MAX_SNAP_FILES];
    } recv;

    /*
     * What a committed install still owes: putting its files onto the
     * live names. Kept across the host's close/reopen, because that is
     * precisely the window adoption runs in.
     *
     * `members` is stashed here rather than re-read from the store,
     * because the store's manifest does not carry it -- the member set
     * rides the INSTALL, so a bootstrapped node whose log holds no
     * CONFIG history learns the cluster's shape from the snapshot that
     * built it.
     */
    struct {
        int      pending;
        uint64_t gen, index, term;
        dbuf     members;
    } adopt;
    /* A log this node created for itself in rn_adopt, and therefore
     * frees. The one rn_new was given is the caller's, always.
     *
     * The io is kept because elog_free does not close one -- it holds a
     * COPY of the vtable, not the handle behind it -- so a rebase that
     * dropped it would leak a descriptor per install.
     */
    int    owns_log;
    bj_io  own_io;

    int      role;
    uint64_t leader_id;
    uint64_t commit_index;
    int      running;
    int      quiesced;

    rn_peer  peers[RN_MAX_PEERS];
    uint32_t npeers;      /* excludes self                              */
    int      self_voting;
    /* Set by a host that booted this member with an EMPTY log beside peers
     * that have one: it may replicate and be installed into, but it may
     * not vote or campaign until it holds something to vote WITH. See
     * rn_hold_vote_while_blank. */
    int      blank_hold;
    uint32_t voter_count; /* including self when self_voting             */
    /* The adopted set as raft_members_adopt normalized it: the member
     * RECORDS (ids, addresses, voting flags), the voter ids and the peer
     * ids, in one binjson object. Kept rather than derived twice: a host
     * that ran the same normalization on its own side would be a second
     * place the cluster's shape is written down. */
    dbuf     adopted;

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
    uint64_t   round_corr_lo, round_corr_hi;

    uint64_t next_corr;

    rn_out   out[RN_MAX_OUT];
    uint32_t nout;
    rn_eff   eff[RN_MAX_EFF];
    uint32_t neff;
    /* Sticky: this node once had something to say and no room to say it.
     * Never cleared -- the host's view of it is incomplete from that
     * moment on, and the only safe reading is that it stays so. */
    int      effects_lost;

    /*
     * Membership orchestration. One change may be in flight at a time --
     * the single-server-change safety argument rests on changes
     * serializing -- and `pending` holds the join/leave requests waiting
     * on the one in flight, so each can be answered when it lands.
     *
     * A request that cannot be answered NOW is the only thing in this
     * file that does not reply within its own rn_handle: the answer is a
     * fact about a log entry that has not committed yet. The host holds
     * the promise (raft_node.h's division), and the reply reaches it
     * through the outbox like every other message.
     */
    int      config_in_flight;
    struct { uint64_t peer, corr; } pending[RN_MAX_PENDING];
    uint32_t npending;

    /*
     * Leadership transfer (section 3.10), rn_transfer's state: the
     * target it named, the deadline it disarms at, and the TimeoutNow's
     * correlation id once one is out. corr 0 while armed means "not
     * sent, or send it again" -- a refused or lost TimeoutNow clears it
     * so the next ack decides afresh.
     */
    uint64_t transfer_target;
    uint64_t transfer_corr;
    int64_t  transfer_deadline;

    /*
     * Read barriers. `started` is the instant the barrier was taken, and
     * confirming it means a quorum of voters has since been shown to
     * still follow us (rn_peer's ack_covers). `state` is 0 waiting,
     * 1 confirmed, -1 lost -- and an entry stays until the host releases
     * it, because the host is what turns a state into an answer.
     */
    struct {
        int      used;
        int      state;
        uint64_t token;
        int64_t  started;
    } reads[RN_MAX_READS];
    uint64_t read_seq;

    /*
     * Proposals this node owes an answer for. The TERM is what makes it
     * a table rather than a counter: an index alone cannot tell a
     * committed entry from one a new leader wrote over.
     */
    struct { uint64_t index, term; } await[RN_MAX_AWAIT];
    uint32_t nawait;
};

/* The coalescing argument above, as arithmetic the compiler checks:
 * three actionable slots per peer, plus headroom for the transitions
 * one call can raise. If a new per-peer effect kind arrives, this fails
 * here rather than in production. */
_Static_assert(RN_MAX_EFF >= RN_MAX_PEERS * 3 + 8 + RN_MAX_AWAIT,
               "effect queue must hold every peer's actionable effects and "
               "every completion one apply pass can produce, at once");

/* ---- small helpers ------------------------------------------------------ */

/* Answers everyone waiting on a membership change; see its definition. */
static void flush_pending(raft_node *n, int ok);
static void lose_reads(raft_node *n);
static void settle_all_lost(raft_node *n);
static int  votes_now(const raft_node *n);

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
static int queue(raft_node *n, uint64_t peer, uint64_t corr, int is_reply,
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

/* Correlation ids are issued, never reused: a uint64 counter cannot
 * come back around to an id still outstanding, so no reply can ever be
 * attributed to a request that is not the one it answers. (The JS glue
 * carries these as doubles, exact to 2^53 -- the ceiling every index in
 * this layer already has, and it lives in the glue, not here.) */
static uint64_t fresh_corr(raft_node *n) {
    return ++n->next_corr;   /* 0 stays "none" */
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
    n->min_election = RN_DEFAULT_MIN_ELECTION;
    n->max_election = RN_DEFAULT_MAX_ELECTION;
    n->heartbeat_ms = RN_DEFAULT_HEARTBEAT;
    n->max_batch_bytes = 65536;
    n->chunk_bytes = RN_DEFAULT_CHUNK;
    n->election_deadline = INT64_MAX;
    n->last_leader_contact = INT64_MIN;
    return n;
}

void rn_free(raft_node *n) {
    if (!n) return;
    for (uint32_t i = 0; i < RN_MAX_OUT; i++) dbuf_free(&n->out[i].bytes);
    dbuf_free(&n->adopted);
    dbuf_free(&n->snap_manifest);
    dbuf_free(&n->recv.manifest);
    dbuf_free(&n->adopt.members);
    /* A log this node rebased for itself, and the handle under it. The
     * one it was GIVEN is the caller's and is never freed here --
     * rn_new's contract. */
    if (n->owns_log) {
        elog_free(n->log);
        if (n->own_io.close) n->own_io.close(n->own_io.ctx);
    }
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
    /* A stopped node answers nothing further, so anything it still owes
     * is owed now or never (raft_node.h's "every registered wait
     * terminates"). The host drains after this call like any other. */
    settle_all_lost(n);
    lose_reads(n);
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

    /* Nothing above touched the node. Commit the whole set at once --
     * the cursors, the arithmetic, and the records themselves, which the
     * node keeps so nobody has to normalize them a second time. */
    if (nnext) memcpy(n->peers, next, sizeof(rn_peer) * nnext);
    n->npeers = nnext;
    n->self_voting = self_voting;
    n->voter_count = voter_count;
    dbuf_free(&n->adopted);
    n->adopted = adopted;          /* moved, not copied */

    /* A set is in force, so no change is in flight, and anyone waiting on
     * one has their answer. */
    n->config_in_flight = 0;
    flush_pending(n, 1);
    return BJ_OK;
}

const uint8_t *rn_adopted(const raft_node *n, uint32_t *len) {
    *len = (uint32_t)n->adopted.len;
    return n->adopted.data;
}

/* The `members` ARRAY inside the adopted set -- the records themselves,
 * which is what a membership answer carries and what a redirect reads an
 * address out of. */
static const uint8_t *members_span(const raft_node *n, uint32_t *len) {
    const uint8_t *v; size_t vlen; int found = 0;
    *len = 0;
    if (!n->adopted.len) return NULL;
    if (obj_get_field(n->adopted.data, n->adopted.len,
                      (const uint8_t *)"members", 7, &v, &vlen, &found) || !found) return NULL;
    *len = (uint32_t)vlen;
    return v;
}

/* The record for `id`, or NULL. */
static const uint8_t *record_of(const raft_node *n, uint64_t id, uint32_t *len) {
    uint32_t mlen = 0;
    const uint8_t *m = members_span(n, &mlen);
    *len = 0;
    if (!m) return NULL;
    cur c = { m, mlen, 0 };
    uint32_t count;
    if (array_begin(&c, &count) != BJ_OK) return NULL;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) return NULL;
        const uint8_t *rec = m + start;
        uint32_t rlen = (uint32_t)(c.pos - start);
        const uint8_t *idv; uint32_t idlen;
        if (rmsg_record_field(rec, rlen, "id", &idv, &idlen) || !idv) continue;
        cur ic = { idv, idlen, 0 };
        double d;
        if (read_number(&ic, &d) != BJ_OK) continue;
        if ((uint64_t)d == id) { *len = rlen; return rec; }
    }
    return NULL;
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
    /* Whatever membership change this node was carrying, it is not the
     * one who can finish it now. Everyone waiting gets a redirect rather
     * than a promise nobody will keep -- and the same goes for every
     * ordinary proposal in flight: a node that has stopped leading can
     * promise nothing about entries it has not applied. */
    n->config_in_flight = 0;
    flush_pending(n, 0);
    settle_all_lost(n);
    /* A read this node was proving is a read it can no longer prove. */
    lose_reads(n);
    /* A transfer that was armed has arrived at its destination state:
     * leadership left this node. However that happened -- the target's
     * election is the expected way -- it is what the caller of
     * rn_transfer was waiting to hear. */
    if (n->transfer_target) {
        emit(n, RN_EFFECT_TRANSFER, n->transfer_target, 1);
        n->transfer_target = 0;
        n->transfer_corr = 0;
    }
}

static int replicate_to(raft_node *n, rn_peer *p);
static void advance_commit(raft_node *n);

/* ---- read barriers (section 6.4) ---------------------------------------- */

/*
 * Has a quorum of voters been shown to still follow us since `since`?
 *
 * Self counts when self is a voter -- we follow ourselves by
 * construction -- which is exactly what lets a single-voter group
 * confirm a barrier without sending anything.
 *
 * `ack_covers` and not `ack_at`: the question is not when an answer
 * ARRIVED but how far back it vouches for, which is when the request it
 * answers went out.
 */
static int quorum_covers(const raft_node *n, int64_t since) {
    uint32_t live = n->self_voting ? 1u : 0u;
    for (uint32_t i = 0; i < n->npeers; i++) {
        if (!n->peers[i].voting) continue;
        if (n->peers[i].ack_covers >= since) live++;
    }
    return live >= rn_quorum(n);
}

/*
 * Confirm what can be confirmed, and ask again wherever that is what is
 * missing.
 *
 * The nudge is not an optimization at the margin: without it a barrier
 * waits for the heartbeat timer, which puts up to a heartbeat interval
 * on EVERY read. A peer with a request already in flight is skipped by
 * replicate_to, and its answer will cover a send that predates the
 * barrier -- so it takes the reply, and then this, to close it.
 */
static int check_reads(raft_node *n) {
    int waiting = 0;
    int64_t oldest = 0;
    for (uint32_t i = 0; i < RN_MAX_READS; i++) {
        if (!n->reads[i].used || n->reads[i].state) continue;
        if (quorum_covers(n, n->reads[i].started)) { n->reads[i].state = 1; continue; }
        if (!waiting || n->reads[i].started < oldest) oldest = n->reads[i].started;
        waiting = 1;
    }
    if (!waiting || n->role != RAFT_LEADER) return BJ_OK;

    int first = BJ_OK;
    for (uint32_t i = 0; i < n->npeers; i++) {
        rn_peer *p = &n->peers[i];
        if (!p->voting || p->inflight || p->ack_covers >= oldest) continue;
        int e = replicate_to(n, p);
        if (e && !first) first = e;
    }
    return first;
}

/* Nobody is left holding a read. Step-down and stop settle every
 * barrier the same way every other registered wait here is settled. */
static void lose_reads(raft_node *n) {
    for (uint32_t i = 0; i < RN_MAX_READS; i++)
        if (n->reads[i].used && !n->reads[i].state) n->reads[i].state = -1;
}

/*
 * A barrier a quorum never answered. Raft does not depose a leader that
 * has lost contact -- it just stops being able to commit -- so without
 * this a partitioned leader holds a read forever. An election timeout is
 * the bound because by then a reachable majority has had time to elect
 * somebody else.
 */
static void expire_reads(raft_node *n) {
    for (uint32_t i = 0; i < RN_MAX_READS; i++) {
        if (!n->reads[i].used || n->reads[i].state) continue;
        if (n->now - n->reads[i].started >= n->max_election) n->reads[i].state = -1;
    }
}

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
        n->peers[i].sent_at = INT64_MIN;
        n->peers[i].ack_covers = INT64_MIN;
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
            uint64_t corr = fresh_corr(n);
            if (!n->round_corr_lo) n->round_corr_lo = corr;
            n->round_corr_hi = corr;
            e = queue(n, n->peers[i].id, corr, 0, msg.data, msg.len);
        }
    }
    dbuf_free(&msg);
    return e;
}

/* ---- serving a snapshot ------------------------------------------------- */

void rn_set_group(raft_node *n, uint64_t group) { if (n) n->group = group; }
uint64_t rn_group(const raft_node *n) { return n ? n->group : 0; }

void rn_set_ns(raft_node *n, bj_ns *ns)          { if (n) n->ns = ns; }
bj_ns *rn_ns(const raft_node *n)                 { return n ? n->ns : NULL; }
void rn_set_snapstore(raft_node *n, sst *store)  { if (n) { n->store = store; n->snap_gen = 0; } }
void rn_set_chunk_bytes(raft_node *n, uint32_t b) { if (n) n->chunk_bytes = b ? b : RN_DEFAULT_CHUNK; }

int rn_serves_snapshots(const raft_node *n) {
    return n && n->ns && n->store && sst_has_latest(n->store);
}

/* A number out of a record, or 0. */
static uint64_t rec_num(const uint8_t *o, size_t len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found))
        return 0;
    if (!found) return 0;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d) != BJ_OK || d < 0) return 0;
    return (uint64_t)d;
}

/* A field's whole encoded span, or NULL. */
static const uint8_t *rec_raw(const uint8_t *o, size_t len, const char *key, uint32_t *n_out) {
    const uint8_t *v; size_t vlen; int found = 0;
    *n_out = 0;
    if (obj_get_field(o, len, (const uint8_t *)key, (uint32_t)strlen(key), &v, &vlen, &found))
        return NULL;
    if (!found) return NULL;
    *n_out = (uint32_t)vlen;
    return v;
}

/*
 * Cache what a transfer of the store's latest generation needs: the
 * manifest exactly as it goes on the wire, and the file sizes the chunk
 * walk works over.
 *
 * The manifest is a PROJECTION of the store's own -- {config, files:
 * [{role, size, crc}], members} -- and not the store's manifest itself,
 * which also carries each file's NAME. The receiver names its own files;
 * telling it ours would be telling it something it must not use.
 *
 * `members` comes from this node's adopted set, not from a caller. A
 * bootstrapped follower's log holds no CONFIG history, so the install is
 * the only thing that can tell it the cluster's shape, and the node is
 * the one place that shape is written down.
 */
static int snap_refresh(raft_node *n) {
    if (!n->store || !sst_has_latest(n->store)) return RAFT_ERR_CAPACITY;
    uint64_t gen = sst_latest_gen(n->store);
    if (n->snap_gen == gen && n->snap_manifest.len) return BJ_OK;

    dbuf latest = {0};
    int has = 0;
    int e = sst_latest(n->store, &latest, &has);
    if (e || !has) { dbuf_free(&latest); return e ? e : RAFT_ERR_CAPACITY; }

    uint64_t index = rec_num(latest.data, latest.len, "lastIncludedIndex");
    uint64_t term  = rec_num(latest.data, latest.len, "lastIncludedTerm");
    uint32_t config_len = 0;
    const uint8_t *config = rec_raw(latest.data, latest.len, "config", &config_len);
    uint32_t files_len = 0;
    const uint8_t *files = rec_raw(latest.data, latest.len, "files", &files_len);

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&latest); return BJ_ERR_OOM; }
    uint32_t nfiles = 0;
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"config", 6);
    if (!e) e = config ? bj_put_raw(b, config, config_len) : bj_put_null(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"files", 5);
    if (!e) e = bj_begin_array(b);
    if (!e && files) {
        cur c = { files, files_len, 0 };
        uint32_t count = 0;
        e = array_begin(&c, &count);
        for (uint32_t i = 0; !e && i < count; i++) {
            size_t start = c.pos;
            e = skip_value(&c);
            if (e) break;
            const uint8_t *el = c.d + start;
            size_t el_len = c.pos - start;
            if (nfiles >= RN_MAX_SNAP_FILES) { e = RAFT_ERR_CAPACITY; break; }

            uint32_t role_len = 0;
            const uint8_t *role = rec_raw(el, el_len, "role", &role_len);
            uint32_t size_len = 0, crc_len = 0;
            const uint8_t *size = rec_raw(el, el_len, "size", &size_len);
            const uint8_t *crc  = rec_raw(el, el_len, "crc",  &crc_len);
            if (!role || !size) { e = RAFT_ERR_MESSAGE; break; }

            n->snap_sizes[nfiles++] = rec_num(el, el_len, "size");

            e = bj_begin_object(b);
            if (!e) e = bj_put_key(b, (const uint8_t *)"role", 4);
            if (!e) e = bj_put_raw(b, role, role_len);
            if (!e) e = bj_put_key(b, (const uint8_t *)"size", 4);
            if (!e) e = bj_put_raw(b, size, size_len);
            if (!e && crc) {
                e = bj_put_key(b, (const uint8_t *)"crc", 3);
                if (!e) e = bj_put_raw(b, crc, crc_len);
            }
            if (!e) e = bj_end_object(b);
        }
    }
    if (!e) e = bj_end_array(b);
    if (!e) {
        uint32_t mlen = 0;
        const uint8_t *m = members_span(n, &mlen);
        if (m) {
            e = bj_put_key(b, (const uint8_t *)"members", 7);
            if (!e) e = bj_put_raw(b, m, mlen);
        }
    }
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        if (!d) e = BJ_ERR_STATE;
        else {
            n->snap_manifest.len = 0;
            e = dbuf_put(&n->snap_manifest, d, len);
        }
    }
    bj_builder_free(b);
    dbuf_free(&latest);
    if (e) { n->snap_gen = 0; n->snap_manifest.len = 0; return e; }

    n->snap_gen = gen;
    n->snap_index = index;
    n->snap_term = term;
    n->snap_nfiles = nfiles;
    return BJ_OK;
}

/* The role of file `i` in the cached manifest, as a span into it. */
static const char *snap_role(const raft_node *n, uint32_t i, uint32_t *len) {
    *len = 0;
    uint32_t files_len = 0;
    const uint8_t *files = rec_raw(n->snap_manifest.data, n->snap_manifest.len,
                                   "files", &files_len);
    if (!files) return NULL;
    cur c = { files, files_len, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count) || i >= count) return NULL;
    for (uint32_t k = 0; k <= i; k++) {
        size_t start = c.pos;
        if (skip_value(&c)) return NULL;
        if (k != i) continue;
        const uint8_t *el = c.d + start;
        size_t el_len = c.pos - start;
        uint32_t rlen = 0;
        const uint8_t *r = rec_raw(el, el_len, "role", &rlen);
        /* A STRING: tag, u32 length, bytes. */
        if (!r || rlen < 5 || r[0] != BJ_TYPE_STRING) return NULL;
        *len = rdu32(r + 1);
        if ((size_t)*len + 5 != rlen) { *len = 0; return NULL; }
        return (const char *)(r + 5);
    }
    return NULL;
}

/*
 * Queue the next chunk to `p`, or finish the transfer.
 *
 * One chunk is one message with one correlation id, and the reply
 * arrives through rn_on_reply like any other. That is the whole
 * difference from the JavaScript version, which awaited each chunk
 * inside a loop: a loop cannot survive a leader stepping down mid-
 * transfer, so every iteration re-checked the role, the term and the
 * running flag by hand.
 */
static int install_send(raft_node *n, rn_peer *p) {
    if (n->role != RAFT_LEADER || !p->installing) return BJ_OK;

    raft_chunk ch;
    if (!raft_chunk_next(n->snap_sizes, n->snap_nfiles, n->chunk_bytes,
                         p->cursor_file, p->cursor_offset, &ch)) {
        /* The walk is exhausted without a `done` chunk having been
         * acknowledged. Nothing to do but stop; the next heartbeat
         * starts over. */
        p->installing = 0;
        return BJ_OK;
    }

    uint8_t *buf = NULL;
    uint32_t role_len = 0;
    const char *role = NULL;
    int e = BJ_OK;

    if (ch.len || n->snap_nfiles) role = snap_role(n, ch.file_index, &role_len);
    if (ch.len) {
        if (!role) return RAFT_ERR_MESSAGE;
        dbuf name = {0};
        e = sst_data_name(n->store, n->snap_gen, role, role_len, &name);
        if (e) { dbuf_free(&name); return e; }
        bj_io io;
        e = n->ns->open(n->ns->ctx, (const char *)name.data, (uint32_t)name.len, 0, &io);
        dbuf_free(&name);
        if (e) return e;
        buf = (uint8_t *)malloc(ch.len);
        if (!buf) { if (io.close) io.close(io.ctx); return BJ_ERR_OOM; }
        int64_t got = io.read(io.ctx, ch.offset, buf, ch.len);
        if (io.close) io.close(io.ctx);
        /* A short read means the file is not the size the manifest
         * claims, which is a snapshot this leader cannot serve rather
         * than a chunk to send anyway. */
        if (got < 0 || (uint32_t)got != ch.len) { free(buf); return SST_ERR_CHECKSUM; }
    }

    dbuf msg = {0};
    e = rmsg_build_install_snapshot(elog_current_term(n->log), n->self_id,
                                    n->snap_index, n->snap_term,
                                    role, role_len, ch.offset,
                                    buf, ch.len, ch.is_done,
                                    ch.is_first ? n->snap_manifest.data : NULL,
                                    ch.is_first ? (uint32_t)n->snap_manifest.len : 0,
                                    &msg);
    free(buf);
    if (!e) {
        uint64_t corr = fresh_corr(n);
        e = queue(n, p->id, corr, 0, msg.data, msg.len);
        if (!e) {
            p->inflight = corr;
            p->sent_at = n->now;
            p->chunk_done = ch.is_done;
            p->cursor_file = ch.next_file;
            p->cursor_offset = ch.next_offset;
        }
    }
    dbuf_free(&msg);
    return e;
}

/* Begin one, from the top. Restarting is always safe: the receiver
 * supersedes whatever it had staged when a manifest arrives. */
static int install_start(raft_node *n, rn_peer *p) {
    int e = snap_refresh(n);
    if (e) return e;
    p->installing = 1;
    p->install_term = elog_current_term(n->log);
    p->cursor_file = 0;
    p->cursor_offset = 0;
    p->chunk_done = 0;
    return install_send(n, p);
}

/*
 * A chunk's answer. The term rules are the same ones every reply obeys;
 * what is different is that success means "send the next one" rather
 * than "advance the match index" -- until the chunk that carried `done`
 * is acknowledged, at which point the peer stands at the boundary and
 * ordinary replication resumes.
 */
static int on_install_reply(raft_node *n, rn_peer *p,
                            const uint8_t *reply, uint32_t len, double random01) {
    int was_done = p->chunk_done;
    p->chunk_done = 0;

    if (p->reachable != 1) { p->reachable = 1; emit(n, RN_EFFECT_REACHABLE, p->id, 0); }
    p->ack_at = n->now;

    uint64_t their_term = 0;
    if (rmsg_term(reply, len, &their_term) == BJ_OK &&
        their_term > elog_current_term(n->log)) {
        p->installing = 0;
        return rn_step_down(n, their_term, random01);
    }
    /* An election, or a step down and back up, happened under this
     * transfer. Its chunks were addressed from a term we no longer hold,
     * so the peer is entitled to have refused them; start over. */
    if (n->role != RAFT_LEADER || p->install_term != elog_current_term(n->log)) {
        p->installing = 0;
        return BJ_OK;
    }

    const uint8_t *v; size_t vlen; int found = 0;
    int ok = 0;
    if (!obj_get_field(reply, len, (const uint8_t *)"success", 7, &v, &vlen, &found))
        ok = found && vlen >= 1 && v[0] == BJ_TYPE_TRUE;

    if (!ok) {
        /* Refused, restart asked for, or a reply we cannot read: all the
         * same answer, because the only remedy for any of them is to
         * begin again from the manifest. The next heartbeat does. */
        p->installing = 0;
        return BJ_OK;
    }
    if (!was_done) return install_send(n, p);

    p->installing = 0;
    int e = rn_installed(n, p->id, n->snap_index);
    if (!e) e = replicate_to(n, p);   /* whatever it is short of, now */
    return e;
}

/* ---- receiving a snapshot ----------------------------------------------- */

/*
 * Walk a manifest's `files`, handing each entry's span to `visit` with
 * its index. Stops at the first non-zero return, which is the visitor's
 * to interpret -- snapstore.h has the same shape internally, for the
 * same reason: a manifest is walked from four places and a fourth copy
 * of the walk is a fourth place to get the array's shape wrong.
 */
typedef int (*mf_visit_fn)(void *ctx, uint32_t i, const uint8_t *el, size_t el_len);

static int manifest_files(const uint8_t *manifest, uint32_t len,
                          mf_visit_fn visit, void *ctx) {
    uint32_t files_len = 0;
    const uint8_t *files = rec_raw(manifest, len, "files", &files_len);
    if (!files) return RAFT_ERR_MESSAGE;
    cur c = { files, files_len, 0 };
    uint32_t count = 0;
    int e = array_begin(&c, &count);
    if (e) return RAFT_ERR_MESSAGE;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if (skip_value(&c)) return RAFT_ERR_MESSAGE;
        e = visit(ctx, i, c.d + start, c.pos - start);
        if (e) return e;
    }
    return BJ_OK;
}

/* The role of entry `el`, as the string it holds. */
static const char *entry_role(const uint8_t *el, size_t el_len, uint32_t *len) {
    uint32_t rlen = 0;
    const uint8_t *r = rec_raw(el, el_len, "role", &rlen);
    *len = 0;
    if (!r || rlen < 5 || r[0] != BJ_TYPE_STRING) return NULL;
    uint32_t n = rdu32(r + 1);
    if ((size_t)n + 5 != rlen) return NULL;
    *len = n;
    return (const char *)(r + 5);
}

typedef struct { const char *role; uint32_t role_len; int found; uint32_t index; } find_ctx;

static int find_role(void *vctx, uint32_t i, const uint8_t *el, size_t el_len) {
    find_ctx *f = (find_ctx *)vctx;
    uint32_t rlen = 0;
    const char *r = entry_role(el, el_len, &rlen);
    if (r && rlen == f->role_len && memcmp(r, f->role, rlen) == 0) {
        f->found = 1;
        f->index = i;
        return 1;    /* stop */
    }
    return 0;
}

/* Which file in the staged manifest a role is, or 0. */
static int recv_index_of(const raft_node *n, const char *role, uint32_t role_len,
                         uint32_t *out) {
    find_ctx f = { role, role_len, 0, 0 };
    manifest_files(n->recv.manifest.data, (uint32_t)n->recv.manifest.len, find_role, &f);
    if (!f.found) return RAFT_ERR_MESSAGE;
    *out = f.index;
    return BJ_OK;
}

static int count_files(void *vctx, uint32_t i, const uint8_t *el, size_t el_len) {
    (void)el; (void)el_len;
    *(uint32_t *)vctx = i + 1;
    return 0;
}

/* Forget a staging attempt. The files it made are left on disk with no
 * manifest naming them: an orphan the next sweep collects, which is the
 * ordering snapstore.h's commit protocol depends on -- the manifest is
 * written LAST, so anything without one never existed. */
static void install_abort(raft_node *n) {
    n->recv.active = 0;
    n->recv.gen = 0;
    n->recv.nfiles = 0;
    n->recv.manifest.len = 0;
}

/* The name of generation `gen`'s file for `el`'s role, appended to `out`
 * with its NUL. */
typedef struct { raft_node *n; uint64_t gen; dbuf *out; } name_ctx;

static int append_name(void *vctx, uint32_t i, const uint8_t *el, size_t el_len) {
    name_ctx *c = (name_ctx *)vctx;
    (void)i;
    uint32_t rlen = 0;
    const char *r = entry_role(el, el_len, &rlen);
    if (!r) return RAFT_ERR_MESSAGE;
    dbuf name = {0};
    int e = sst_data_name(c->n->store, c->gen, r, rlen, &name);
    if (!e) e = dbuf_put(c->out, name.data, name.len);
    if (!e) e = dbuf_put(c->out, (const uint8_t *)"", 1);
    dbuf_free(&name);
    return e;
}

int rn_install_plan(raft_node *n, const uint8_t *msg, uint32_t len, dbuf *out) {
    if (!n || !msg || !out) return BJ_ERR_STATE;
    if (!n->ns || !n->store) return BJ_OK;      /* this node will refuse it */

    raft_install in;
    if (rmsg_install_read(msg, len, &in) != BJ_OK) return BJ_OK;

    uint64_t gen;
    int e = BJ_OK;
    if (in.manifest) {
        /* A manifest supersedes whatever was staged, so the generation
         * is the next one -- computed here and again in the handler,
         * from the same store, which is why they agree. */
        gen = sst_next_gen(n->store);
        name_ctx c = { n, gen, out };
        e = manifest_files(in.manifest, in.manifest_len, append_name, &c);
        if (e) return e;
    } else {
        if (!n->recv.active) return BJ_OK;      /* answered `restart` */
        gen = n->recv.gen;
        if (in.role) {
            name_ctx c = { n, gen, out };
            dbuf name = {0};
            e = sst_data_name(n->store, gen, in.role, in.role_len, &name);
            if (!e) e = dbuf_put(out, name.data, name.len);
            if (!e) e = dbuf_put(out, (const uint8_t *)"", 1);
            dbuf_free(&name);
            (void)c;
            if (e) return e;
        }
    }
    /* The manifest file is written when the last chunk lands, which may
     * be this one -- including the first, for a one-chunk install. */
    if (in.done) {
        dbuf name = {0};
        e = sst_manifest_name(n->store, gen, &name);
        if (!e) e = dbuf_put(out, name.data, name.len);
        if (!e) e = dbuf_put(out, (const uint8_t *)"", 1);
        dbuf_free(&name);
    }
    return e;
}

int      rn_installing(const raft_node *n)       { return n && n->recv.active; }
uint64_t rn_install_boundary(const raft_node *n) { return n ? n->recv.index : 0; }

/* Create every file the manifest names, so one that never receives a
 * byte still exists: an absent file and an empty one are different
 * things to sst_check_files. */
static int create_staged(void *vctx, uint32_t i, const uint8_t *el, size_t el_len) {
    name_ctx *c = (name_ctx *)vctx;
    (void)i;
    uint32_t rlen = 0;
    const char *r = entry_role(el, el_len, &rlen);
    if (!r) return RAFT_ERR_MESSAGE;
    dbuf name = {0};
    int e = sst_data_name(c->n->store, c->gen, r, rlen, &name);
    if (!e) {
        bj_io io;
        e = c->n->ns->open(c->n->ns->ctx, (const char *)name.data, (uint32_t)name.len,
                           BJ_NS_CREATE | BJ_NS_TRUNC, &io);
        if (!e && io.close) io.close(io.ctx);
    }
    dbuf_free(&name);
    return e;
}

/* Every staged file as {role, size, crc} -- what sst_check_files
 * compares against the leader's manifest -- and, with `names`, the
 * {role, name, size, crc} the store's own manifest records. */
typedef struct { raft_node *n; bj_builder *b; int names; int e; } actual_ctx;

static int put_actual(void *vctx, uint32_t i, const uint8_t *el, size_t el_len) {
    actual_ctx *a = (actual_ctx *)vctx;
    uint32_t rlen = 0;
    const char *r = entry_role(el, el_len, &rlen);
    if (!r || i >= RN_MAX_SNAP_FILES) return RAFT_ERR_MESSAGE;
    int e = bj_begin_object(a->b);
    if (!e) e = bj_put_key(a->b, (const uint8_t *)"role", 4);
    if (!e) e = bj_put_string(a->b, (const uint8_t *)r, rlen);
    if (!e && a->names) {
        dbuf name = {0};
        e = sst_data_name(a->n->store, a->n->recv.gen, r, rlen, &name);
        if (!e) e = bj_put_key(a->b, (const uint8_t *)"name", 4);
        if (!e) e = bj_put_string(a->b, name.data, (uint32_t)name.len);
        dbuf_free(&name);
    }
    if (!e) e = bj_put_key(a->b, (const uint8_t *)"size", 4);
    if (!e) e = bj_put_int(a->b, (int64_t)a->n->recv.written[i]);
    if (!e) e = bj_put_key(a->b, (const uint8_t *)"crc", 3);
    if (!e) e = bj_put_int(a->b, (int64_t)a->n->recv.crc[i]);
    if (!e) e = bj_end_object(a->b);
    return e;
}

static int staged_files(raft_node *n, int names, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    actual_ctx a = { n, b, names, 0 };
    int e = bj_begin_array(b);
    if (!e) e = manifest_files(n->recv.manifest.data, (uint32_t)n->recv.manifest.len,
                               put_actual, &a);
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        if (!d) e = BJ_ERR_STATE;
        else e = dbuf_put(out, d, len);
    }
    bj_builder_free(b);
    return e;
}

/*
 * The last chunk landed: check the staged bytes against the leader's
 * manifest, and if they are what it said, write OUR manifest -- which is
 * the commit, and the only irreversible step in the whole transfer.
 *
 * A mismatch adopts nothing. The staged files stay on disk with no
 * manifest naming them, which makes them orphans rather than a
 * generation, and the leader is asked to start over.
 */
static int install_commit(raft_node *n) {
    dbuf actual = {0};
    int e = staged_files(n, 0, &actual);
    if (!e) {
        const uint8_t *bad = NULL; uint32_t bad_len = 0;
        e = sst_check_files(n->recv.manifest.data, (uint32_t)n->recv.manifest.len,
                            actual.data, (uint32_t)actual.len, &bad, &bad_len);
    }
    dbuf_free(&actual);
    if (e) return e;

    dbuf files = {0};
    e = staged_files(n, 1, &files);
    if (e) { dbuf_free(&files); return e; }

    uint32_t config_len = 0;
    const uint8_t *config = rec_raw(n->recv.manifest.data, (uint32_t)n->recv.manifest.len,
                                    "config", &config_len);
    /* An absent config and an explicit null are the same thing to the
     * store, and the leader sends the latter. */
    if (config && config_len && config[0] == BJ_TYPE_NULL) { config = NULL; config_len = 0; }

    dbuf manifest = {0};
    e = sst_manifest_encode(n->recv.index, n->recv.term, config, config_len,
                            files.data, (uint32_t)files.len, &manifest);
    dbuf_free(&files);
    if (!e) {
        dbuf name = {0};
        e = sst_manifest_name(n->store, n->recv.gen, &name);
        if (!e) {
            bj_io io;
            e = n->ns->open(n->ns->ctx, (const char *)name.data, (uint32_t)name.len,
                            BJ_NS_CREATE | BJ_NS_TRUNC, &io);
            if (!e) {
                e = io.write(io.ctx, 0, manifest.data, (uint32_t)manifest.len);
                /* Durable before it counts: a manifest in a buffer is a
                 * generation that adopts after a crash and has no files. */
                if (!e && io.sync) e = io.sync(io.ctx);
                if (io.close) io.close(io.ctx);
            }
        }
        dbuf_free(&name);
    }
    if (!e) {
        /* The store learns about it so its next generation number does
         * not hand this one out twice. The previous generation's files
         * are NOT deleted here: the live database is still using them
         * until the host adopts, which is the next effect's business. */
        dbuf sweep = {0};
        e = sst_adopt_committed(n->store, n->recv.gen, manifest.data,
                                (uint32_t)manifest.len, &sweep);
        dbuf_free(&sweep);
        n->snap_gen = 0;    /* what we would serve has changed */
    }
    dbuf_free(&manifest);
    return e;
}

/*
 * One chunk. The term rules are rn_observe_leader's, exactly as they
 * were when this handler lived in the host -- what has moved is the
 * staging, which needed files.
 */
static int handle_install(raft_node *n, uint64_t corr,
                          const uint8_t *msg, uint32_t len, double random01) {
    raft_install in;
    int e = rmsg_install_read(msg, len, &in);
    if (e) return e;

    uint64_t term = elog_current_term(n->log);
    if (!rn_observe_leader(n, in.term, in.leader_id, random01)) {
        dbuf reply = {0};
        e = rmsg_build_install_reply(term, 0, 0, &reply);
        if (!e) e = queue(n, in.leader_id, corr, 1, reply.data, reply.len);
        dbuf_free(&reply);
        return e;
    }
    term = elog_current_term(n->log);

    int ok = 0, restart = 0;
    if (in.manifest) {
        install_abort(n);
        uint32_t nfiles = 0;
        e = manifest_files(in.manifest, in.manifest_len, count_files, &nfiles);
        if (!e && nfiles > RN_MAX_SNAP_FILES) e = RAFT_ERR_CAPACITY;
        if (!e) {
            n->recv.manifest.len = 0;
            e = dbuf_put(&n->recv.manifest, in.manifest, in.manifest_len);
        }
        if (!e) {
            n->recv.gen = sst_next_gen(n->store);
            name_ctx c = { n, n->recv.gen, NULL };
            e = manifest_files(in.manifest, in.manifest_len, create_staged, &c);
        }
        if (!e) {
            n->recv.active = 1;
            n->recv.index = in.last_included_index;
            n->recv.term = in.last_included_term;
            n->recv.nfiles = nfiles;
            memset(n->recv.written, 0, sizeof n->recv.written);
            memset(n->recv.crc, 0, sizeof n->recv.crc);
        } else {
            install_abort(n);
        }
    }

    if (!e && (!n->recv.active ||
               n->recv.index != in.last_included_index ||
               n->recv.term != in.last_included_term)) {
        /* A chunk for an install we never started -- the leader
         * restarted, or we abandoned this one. Ask for the manifest. */
        restart = 1;
    } else if (!e) {
        uint32_t idx = 0;
        if (in.role && recv_index_of(n, in.role, in.role_len, &idx) != BJ_OK) {
            restart = 1;
        } else if (in.role && in.offset != n->recv.written[idx]) {
            /* A stream that lost or reordered a chunk. Staging it would
             * make a file that passes a size check and no checksum. */
            install_abort(n);
            restart = 1;
        } else if (in.role && in.data_len) {
            dbuf name = {0};
            e = sst_data_name(n->store, n->recv.gen, in.role, in.role_len, &name);
            if (!e) {
                bj_io io;
                e = n->ns->open(n->ns->ctx, (const char *)name.data, (uint32_t)name.len,
                                BJ_NS_CREATE, &io);
                if (!e) {
                    e = io.write(io.ctx, in.offset, in.data, in.data_len);
                    if (!e && io.sync) e = io.sync(io.ctx);
                    if (io.close) io.close(io.ctx);
                }
            }
            dbuf_free(&name);
            if (!e) {
                n->recv.written[idx] += in.data_len;
                n->recv.crc[idx] = bjfile_crc32(n->recv.crc[idx], in.data, in.data_len);
            }
        }
        if (!e && !restart) {
            ok = 1;
            if (in.done) {
                int ce = install_commit(n);
                if (ce) {
                    /* Verification failed, or the commit could not be
                     * written. Nothing was adopted; start over. */
                    install_abort(n);
                    ok = 0;
                    restart = 1;
                } else {
                    /* What adoption still owes, recorded before the
                     * staging state is cleared: the generation to put
                     * onto the live names, and the member set that rode
                     * the install (the store's manifest does not carry
                     * one). */
                    n->adopt.pending = 1;
                    n->adopt.gen = n->recv.gen;
                    n->adopt.index = n->recv.index;
                    n->adopt.term = n->recv.term;
                    n->adopt.members.len = 0;
                    {
                        uint32_t mlen = 0;
                        const uint8_t *m = rec_raw(n->recv.manifest.data,
                                                   (uint32_t)n->recv.manifest.len,
                                                   "members", &mlen);
                        if (m && mlen) dbuf_put(&n->adopt.members, m, mlen);
                    }
                    uint64_t boundary = n->recv.index;
                    install_abort(n);
                    emit(n, RN_EFFECT_INSTALLED, boundary, 0);
                }
            }
        }
    }
    if (e) return e;

    dbuf reply = {0};
    e = rmsg_build_install_reply(term, ok, restart, &reply);
    if (!e) e = queue(n, in.leader_id, corr, 1, reply.data, reply.len);
    dbuf_free(&reply);
    return e;
}

/* ---- adopting one ------------------------------------------------------- */

elog    *rn_log(const raft_node *n)            { return n ? n->log : NULL; }
int      rn_adopt_pending(const raft_node *n)  { return n && n->adopt.pending; }
uint64_t rn_adopt_boundary(const raft_node *n) { return n ? n->adopt.index : 0; }

/* Each {role, name} of the adopted generation's `config.live` -- the map
 * from a snapshot's roles back to the file names the database opens. */
typedef int (*live_visit_fn)(void *ctx, const char *role, uint32_t role_len,
                             const char *name, uint32_t name_len);

static int live_each(const uint8_t *latest, uint32_t latest_len,
                     live_visit_fn visit, void *ctx) {
    uint32_t clen = 0;
    const uint8_t *config = rec_raw(latest, latest_len, "config", &clen);
    if (!config) return RAFT_ERR_MESSAGE;
    uint32_t llen = 0;
    const uint8_t *live = rec_raw(config, clen, "live", &llen);
    if (!live) return RAFT_ERR_MESSAGE;

    cur c = { live, llen, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count)) return RAFT_ERR_MESSAGE;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if (skip_value(&c)) return RAFT_ERR_MESSAGE;
        const uint8_t *el = c.d + start;
        size_t el_len = c.pos - start;
        uint32_t rlen = 0, nlen = 0;
        const char *role = entry_role(el, el_len, &rlen);
        const uint8_t *nv = rec_raw(el, el_len, "name", &nlen);
        if (!role || !nv || nlen < 5 || nv[0] != BJ_TYPE_STRING) return RAFT_ERR_MESSAGE;
        uint32_t n_str = rdu32(nv + 1);
        if ((size_t)n_str + 5 != nlen) return RAFT_ERR_MESSAGE;
        int e = visit(ctx, role, rlen, (const char *)(nv + 5), n_str);
        if (e) return e;
    }
    return BJ_OK;
}

typedef struct { raft_node *n; dbuf *out; int e; } plan_ctx;

static int put_name(dbuf *out, const uint8_t *name, size_t len) {
    int e = dbuf_put(out, name, len);
    if (!e) e = dbuf_put(out, (const uint8_t *)"", 1);
    return e;
}

static int plan_live(void *vctx, const char *role, uint32_t role_len,
                     const char *name, uint32_t name_len) {
    plan_ctx *p = (plan_ctx *)vctx;
    dbuf gen_name = {0};
    int e = sst_data_name(p->n->store, p->n->adopt.gen, role, role_len, &gen_name);
    if (!e) e = put_name(p->out, gen_name.data, gen_name.len);
    dbuf_free(&gen_name);
    if (!e) e = put_name(p->out, (const uint8_t *)name, name_len);
    return e;
}

int rn_adopt_plan(raft_node *n, dbuf *out) {
    if (!n || !out) return BJ_ERR_STATE;
    if (!n->adopt.pending || !n->store) return BJ_OK;

    dbuf latest = {0};
    int has = 0;
    int e = sst_latest(n->store, &latest, &has);
    if (!e && has) {
        plan_ctx ctx = { n, out, 0 };
        e = live_each(latest.data, (uint32_t)latest.len, plan_live, &ctx);
    }
    dbuf_free(&latest);
    if (e) return e;

    /* The log this adoption will create. Named by the store, like every
     * other file in a generation. */
    dbuf log_name = {0};
    e = sst_log_name(n->store, n->adopt.gen, &log_name);
    if (!e) e = put_name(out, log_name.data, log_name.len);
    dbuf_free(&log_name);
    return e;
}

/* Copy one generation file onto the live name the manifest maps it to.
 * A chunk at a time, so a large collection is not a large allocation. */
typedef struct { raft_node *n; uint8_t *buf; uint32_t cap; } copy_ctx;

static int copy_live(void *vctx, const char *role, uint32_t role_len,
                     const char *name, uint32_t name_len) {
    copy_ctx *c = (copy_ctx *)vctx;
    bj_ns *ns = c->n->ns;
    dbuf gen_name = {0};
    int e = sst_data_name(c->n->store, c->n->adopt.gen, role, role_len, &gen_name);
    if (e) { dbuf_free(&gen_name); return e; }

    bj_io src, dst;
    e = ns->open(ns->ctx, (const char *)gen_name.data, (uint32_t)gen_name.len, 0, &src);
    dbuf_free(&gen_name);
    if (e) return e;
    /* TRUNC, never remove-then-create: a deferred remove could land
     * after the create and take the restored file with it (bjns.h). */
    e = ns->open(ns->ctx, name, name_len, BJ_NS_CREATE | BJ_NS_TRUNC, &dst);
    if (e) { if (src.close) src.close(src.ctx); return e; }

    uint64_t total = src.size(src.ctx), at = 0;
    while (!e && at < total) {
        uint32_t want = (uint32_t)((total - at > c->cap) ? c->cap : (total - at));
        int64_t got = src.read(src.ctx, at, c->buf, want);
        if (got <= 0) { e = (int)(got < 0 ? got : BJ_ERR_EOF); break; }
        e = dst.write(dst.ctx, at, c->buf, (uint32_t)got);
        at += (uint64_t)got;
    }
    /* Durable before the log is rebased onto it: a restored file still
     * in a buffer is a snapshot the next crash does not have. */
    if (!e && dst.sync) e = dst.sync(dst.ctx);
    if (dst.close) dst.close(dst.ctx);
    if (src.close) src.close(src.ctx);
    return e;
}

/* Is `name` one of the live names the generation restores? */
typedef struct { const char *name; uint32_t len; int found; } is_live_ctx;

static int is_live(void *vctx, const char *role, uint32_t role_len,
                   const char *name, uint32_t name_len) {
    is_live_ctx *c = (is_live_ctx *)vctx;
    (void)role; (void)role_len;
    if (name_len == c->len && memcmp(name, c->name, name_len) == 0) {
        c->found = 1;
        return 1;
    }
    return 0;
}

void rn_swap_log(raft_node *n, elog *fresh, const bj_io *io, elog **old) {
    if (!n || !fresh || !old) return;
    /* A swap replaces a log this node already owned -- a prior install's
     * -- and that one is ours to close rather than the caller's to be
     * handed back twice. The caller only ever gets the log it lent us. */
    if (n->owns_log) {
        elog_free(n->log);
        if (n->own_io.close) n->own_io.close(n->own_io.ctx);
        *old = NULL;
    } else {
        *old = n->log;
    }
    n->log = fresh;
    if (io) n->own_io = *io;
    else    memset(&n->own_io, 0, sizeof n->own_io);
    n->owns_log = 1;
}

int rn_adopt(raft_node *n, const char *victims, size_t victims_len, elog **old) {
    if (!n || !old) return BJ_ERR_STATE;
    *old = NULL;
    if (!n->adopt.pending) return BJ_ERR_STATE;
    if (!n->ns || !n->store) return BJ_ERR_STATE;

    dbuf latest = {0};
    int has = 0;
    int e = sst_latest(n->store, &latest, &has);
    if (!e && !has) e = RAFT_ERR_CAPACITY;
    if (e) { dbuf_free(&latest); return e; }

    /* The hard state has to outlive the log that holds it: a fresh log
     * starts at term 0 having voted for nobody, and a node that forgot
     * its vote can vote twice in one term. */
    uint64_t hard_term = elog_current_term(n->log);
    uint64_t hard_voted = elog_voted_for(n->log);

    /* Stale live files first -- the ones this generation does not
     * restore. A journal left behind is the sharp one: recovery would
     * replay it over a restored file and rewind it. */
    for (size_t at = 0; at < victims_len; ) {
        size_t end = at;
        while (end < victims_len && victims[end] != '\0') end++;
        if (end > at) {
            is_live_ctx c = { victims + at, (uint32_t)(end - at), 0 };
            live_each(latest.data, (uint32_t)latest.len, is_live, &c);
            if (!c.found)
                n->ns->remove(n->ns->ctx, victims + at, (uint32_t)(end - at));
        }
        at = end + 1;
    }

    /* Then the restore itself. */
    {
        static const uint32_t CHUNK = 64u * 1024u;
        copy_ctx c = { n, (uint8_t *)malloc(CHUNK), CHUNK };
        if (!c.buf) e = BJ_ERR_OOM;
        else e = live_each(latest.data, (uint32_t)latest.len, copy_live, &c);
        free(c.buf);
    }
    dbuf_free(&latest);
    if (e) return e;

    /* And the log, LAST. Until this line the node still describes the
     * state it had; after it, the one it was sent. */
    dbuf log_name = {0};
    e = sst_log_name(n->store, n->adopt.gen, &log_name);
    elog *fresh = NULL;
    bj_io io;
    memset(&io, 0, sizeof io);
    if (!e) {
        e = n->ns->open(n->ns->ctx, (const char *)log_name.data, (uint32_t)log_name.len,
                        BJ_NS_CREATE | BJ_NS_TRUNC, &io);
        if (!e) {
            /* elog_create_at copies the vtable, so this stack io is
             * safe to pass -- but the HANDLE behind it is not closed by
             * elog_free, which is why it is kept below. */
            fresh = elog_create_at(&io, n->adopt.index, n->adopt.term);
            if (!fresh) e = BJ_ERR_OOM;
        }
    }
    dbuf_free(&log_name);
    if (e) {
        if (io.close) io.close(io.ctx);
        return e;
    }

    if (hard_term > 0) e = elog_set_hard_state(fresh, hard_term, hard_voted);
    if (e) {
        elog_free(fresh);
        if (io.close) io.close(io.ctx);
        return e;
    }

    rn_swap_log(n, fresh, &io, old);

    /* Where this node now stands. The member set rode the install
     * because a bootstrapped log has no CONFIG history to derive it
     * from; it goes through rn_set_members like every other adoption,
     * which is what keeps the voter list and the peer cursors together. */
    rn_seed_commit(n, n->adopt.index);
    if (n->adopt.members.len)
        rn_set_members(n, n->adopt.members.data, (uint32_t)n->adopt.members.len);

    n->adopt.pending = 0;
    n->adopt.members.len = 0;
    return BJ_OK;
}

/* ---- replication -------------------------------------------------------- */

static int replicate_to(raft_node *n, rn_peer *p) {
    if (n->role != RAFT_LEADER || p->inflight) return BJ_OK;

    uint64_t base = elog_base_index(n->log);
    /* Whether a snapshot can be served is now a fact about this node
     * rather than an assumption: with a namespace and a store it serves
     * its own, and without them it still says who needs one and the host
     * serves it (raft_node.h). */
    int action = raft_repl_decide(p->next, base, rn_serves_snapshots(n));
    if (action == RAFT_REPL_SNAPSHOT) {
        int e = install_start(n, p);
        if (!e) return BJ_OK;
        /* Could not read our own snapshot. Say so the way we would have
         * without a store at all, so a host that can serve one still
         * can, rather than leaving the peer parked in silence. */
        p->installing = 0;
        emit(n, RN_EFFECT_NEEDS_SNAPSHOT, p->id, 0);
        return BJ_OK;
    }
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
        uint64_t corr = fresh_corr(n);
        e = queue(n, p->id, corr, 0, msg.data, msg.len);
        if (!e) { p->inflight = corr; p->sent_at = n->now; }
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

/*
 * The transfer's trigger, re-checked wherever the target's match index
 * can have moved and on every leader tick: once the target holds
 * everything this log has, it is as up to date as a successor can be,
 * so tell it to stand NOW. One TimeoutNow is out at a time (the corr
 * says so); a refusal or a lost send clears the corr, and the acks the
 * heartbeats keep producing decide again.
 */
static int maybe_send_timeout_now(raft_node *n) {
    if (!n->transfer_target || n->transfer_corr || n->role != RAFT_LEADER)
        return BJ_OK;
    rn_peer *p = peer_of(n, n->transfer_target);
    if (!p || p->match < elog_last_index(n->log)) return BJ_OK;
    dbuf msg = {0};
    int e = rmsg_build_timeout_now(elog_current_term(n->log), n->self_id, &msg);
    if (e) { dbuf_free(&msg); return e; }
    uint64_t corr = fresh_corr(n);
    e = queue(n, n->transfer_target, corr, 0, msg.data, msg.len);
    dbuf_free(&msg);
    if (e) return e;
    n->transfer_corr = corr;
    return BJ_OK;
}

int rn_transfer(raft_node *n, uint64_t target) {
    if (!n || !n->running || n->role != RAFT_LEADER) return BJ_ERR_STATE;
    if (n->transfer_target) return RAFT_ERR_BUSY;
    rn_peer *p = peer_of(n, target);
    if (!p || !p->voting) return RAFT_ERR_PEER;
    n->transfer_target = target;
    n->transfer_corr = 0;
    n->transfer_deadline = n->now + 2 * n->max_election;
    /* A target already current is told NOW rather than on the next ack;
     * one that is behind gets the entries that close the gap, and every
     * ack of those re-checks. */
    int e = maybe_send_timeout_now(n);
    if (e) return e;
    return n->transfer_corr ? BJ_OK : replicate_to(n, p);
}

uint64_t rn_transfer_target(const raft_node *n) {
    return n ? n->transfer_target : 0;
}

int rn_tick(raft_node *n, int64_t now, double random01) {
    if (!n->running) return BJ_OK;
    n->now = now;
    if (n->quiesced) return BJ_OK;

    if (n->role == RAFT_LEADER) {
        /* Reads first: a barrier nobody answered has to end, and one a
         * heartbeat is about to confirm should not wait a further tick
         * to be noticed. */
        expire_reads(n);
        int r = check_reads(n);
        if (n->transfer_target) {
            if (now >= n->transfer_deadline) {
                /* Still leading past the deadline: the target is down,
                 * unreachable or refusing. Disarmed and said out loud,
                 * so a host that was fencing proposals lifts the fence
                 * and the caller learns a retry is safe. */
                emit(n, RN_EFFECT_TRANSFER, n->transfer_target, 0);
                n->transfer_target = 0;
                n->transfer_corr = 0;
            } else {
                int e = maybe_send_timeout_now(n);
                if (e) return e;
            }
        }
        if (now >= n->heartbeat_due) {
            n->heartbeat_due = now + n->heartbeat_ms;
            int e = replicate_to_all(n);
            return e ? e : r;
        }
        return r;
    }

    if (now >= n->election_deadline) {
        /* Pre-vote first: a straw poll persists nothing, so a node that
         * cannot win does not bump the term and disturb a live leader. */
        if (!votes_now(n)) { arm_election(n, random01); return BJ_OK; }
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

/*
 * MAY THIS MEMBER VOTE, RIGHT NOW?
 *
 * `self_voting` is the cluster's answer: what the member set says. This
 * adds the member's own, and it is temporary: a node that came up with an
 * EMPTY log beside peers that have one holds no franchise until it has
 * received something. Both halves of that are load-bearing:
 *
 *   NOT CAMPAIGNING is what stops two wiped members electing each other
 *   and handing a survivor their empty log to adopt -- measured, before
 *   this existed, against a member holding 7,319 committed entries.
 *
 *   NOT GRANTING is what stops one wiped member's vote completing a
 *   majority for a candidate whose log is missing entries that member's
 *   own acknowledgement helped commit.
 *
 * It clears by itself, on the only evidence that matters: entries. Once
 * the log is non-empty this member has heard from the leader of its term,
 * its state is that leader's, and comparing logs means something again.
 * The host also spends the term durably before starting (see
 * server/replica.c), so a restart cannot hand the vote back inside the
 * term the member woke into.
 */
static int votes_now(const raft_node *n) {
    if (!n->self_voting) return 0;
    return !(n->blank_hold && elog_last_index(n->log) == 0);
}

void rn_hold_vote_while_blank(raft_node *n, int hold) {
    if (n) n->blank_hold = hold ? 1 : 0;
}

int rn_vote_held(const raft_node *n) {
    return n && n->self_voting && !votes_now(n);
}

static void fill_state(const raft_node *n, raft_msg_state *st) {
    st->self_id             = n->self_id;
    st->is_follower         = n->role == RAFT_FOLLOWER;
    st->is_leader           = n->role == RAFT_LEADER;
    st->self_is_voter       = votes_now(n);
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

/* ---- the three kinds a node answers without touching its log ------------ */

/* Queue one reply, taking ownership of nothing. */
static int reply_with(raft_node *n, uint64_t peer, uint64_t corr, dbuf *r) {
    int e = queue(n, peer, corr, 1, r->data, r->len);
    dbuf_free(r);
    return e;
}

/* A redirect: who leads, and where to find them. */
static int reply_redirect(raft_node *n, uint64_t peer, uint64_t corr) {
    uint32_t llen = 0;
    const uint8_t *leader = n->leader_id ? record_of(n, n->leader_id, &llen) : NULL;
    dbuf r = {0};
    int e = rmsg_build_membership_reply(0, NULL, 0, NULL, 0, n->leader_id, leader, llen, &r);
    if (e) { dbuf_free(&r); return e; }
    return reply_with(n, peer, corr, &r);
}

/* `{ok:true, members}` -- the current set, which is the answer to a
 * request that asks for a change already in force. */
static int reply_members(raft_node *n, uint64_t peer, uint64_t corr) {
    uint32_t mlen = 0;
    const uint8_t *members = members_span(n, &mlen);
    dbuf r = {0};
    int e = rmsg_build_membership_reply(1, members, mlen, NULL, 0, 0, NULL, 0, &r);
    if (e) { dbuf_free(&r); return e; }
    return reply_with(n, peer, corr, &r);
}

static int reply_error(raft_node *n, uint64_t peer, uint64_t corr,
                       const char *error, int retry) {
    dbuf r = {0};
    int e = rmsg_build_membership_reply(0, NULL, 0, error, retry, 0, NULL, 0, &r);
    if (e) { dbuf_free(&r); return e; }
    return reply_with(n, peer, corr, &r);
}

/* Park a requester until the change it asked for lands. */
static int defer(raft_node *n, uint64_t peer, uint64_t corr) {
    if (n->npending >= RN_MAX_PENDING) {
        return reply_error(n, peer, corr, NULL, 1);   /* busy; retry */
    }
    n->pending[n->npending].peer = peer;
    n->pending[n->npending].corr = corr;
    n->npending++;
    return BJ_OK;
}

/*
 * An identity question: every durable fact that bears on "does this
 * cluster already exist", and nothing else.
 *
 * ANSWERED BY ANY MEMBER, IN ANY ROLE, AT ANY TIME. That is the point of
 * it, and the reason it is not a join and not a vote: what the asker
 * needs to know is whether a group is already here, and a leaderless
 * moment is not an answer to that question. Every field below comes from
 * this node's own log or its adopted set, so an election in progress
 * changes none of them.
 *
 * It touches nothing -- no term, no vote, no timer. A node being asked
 * who it is has not been contacted by a leader and must not act as
 * though it had.
 */
static int handle_identity(raft_node *n, uint64_t corr, const uint8_t *msg,
                           uint32_t len) {
    uint64_t asking = 0;
    if (rmsg_identity_asking(msg, len, &asking)) return RAFT_ERR_MESSAGE;
    /* Self counts: a member set of one is still a member set, and a node
     * asking about its own id wants to know if the cluster has it. */
    const int is_member = asking != 0 &&
                          (asking == n->self_id || peer_of(n, asking) != NULL);
    uint32_t mlen = 0;
    const uint8_t *members = members_span(n, &mlen);
    dbuf r = {0};
    int e = rmsg_build_identity_reply(n->group, elog_base_index(n->log),
                                      elog_last_index(n->log),
                                      elog_current_term(n->log), is_member,
                                      members, mlen, &r);
    if (e) { dbuf_free(&r); return e; }
    /* On the correlation id alone, like a join: whoever is asking may
     * have no id in this cluster at all. */
    return reply_with(n, 0, corr, &r);
}

/*
 * A join: the applicant's record, upserted into the member set.
 *
 * A NEW member always enters as a learner, whatever it asked for --
 * adding capacity must never thin the failure margin, and the leader
 * promotes it automatically once its match index proves it caught up. A
 * re-join of an EXISTING member keeps whatever status it already has: an
 * established voter is not demoted by re-announcing itself.
 */
static int handle_join(raft_node *n, uint64_t corr, const uint8_t *msg, uint32_t len) {
    const uint8_t *rec; uint32_t rlen; uint64_t id = 0;
    if (rmsg_join_member(msg, len, &rec, &rlen, &id)) {
        return reply_error(n, 0, corr, "join requires member { id, host, port }", 0);
    }
    /* The applicant is not a member yet, so it has no id to address a
     * reply to -- the reply rides back on the correlation id alone, and
     * the host returns it to whoever asked. */
    if (n->role != RAFT_LEADER) return reply_redirect(n, 0, corr);

    uint32_t elen = 0;
    const uint8_t *existing = record_of(n, id, &elen);
    if (existing) {
        /* Identical to what the log already says: nothing to change, and
         * saying so is what makes a retried join harmless. */
        const uint8_t *h1, *h2, *p1, *p2; uint32_t h1l, h2l, p1l, p2l;
        rmsg_record_field(existing, elen, "host", &h1, &h1l);
        rmsg_record_field(rec, rlen, "host", &h2, &h2l);
        rmsg_record_field(existing, elen, "port", &p1, &p1l);
        rmsg_record_field(rec, rlen, "port", &p2, &p2l);
        if (h1l == h2l && p1l == p2l &&
            (!h1l || memcmp(h1, h2, h1l) == 0) && (!p1l || memcmp(p1, p2, p1l) == 0)) {
            return reply_members(n, 0, corr);
        }
    }
    if (n->config_in_flight) return reply_error(n, 0, corr, NULL, 1);

    /* Everyone else, plus the applicant. */
    int voting = 0;
    if (existing) {
        const uint8_t *v; uint32_t vlen;
        rmsg_record_field(existing, elen, "voting", &v, &vlen);
        voting = !(vlen && v[0] == BJ_TYPE_FALSE);
    }
    uint32_t mlen = 0;
    const uint8_t *members = members_span(n, &mlen);
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);
    if (members) {
        cur c = { members, mlen, 0 };
        uint32_t count;
        if (!e && array_begin(&c, &count) == BJ_OK) {
            for (uint32_t i = 0; i < count && !e; i++) {
                size_t start = c.pos;
                if (skip_value(&c) != BJ_OK) { e = RAFT_ERR_MEMBER; break; }
                const uint8_t *r2 = members + start;
                uint32_t r2l = (uint32_t)(c.pos - start);
                const uint8_t *idv; uint32_t idl;
                if (!rmsg_record_field(r2, r2l, "id", &idv, &idl) && idv) {
                    cur ic = { idv, idl, 0 };
                    double d;
                    if (read_number(&ic, &d) == BJ_OK && (uint64_t)d == id) continue;
                }
                e = bj_put_raw(b, r2, r2l);
            }
        }
    }
    if (!e) e = rmsg_record_with_voting(rec, rlen, voting, b);
    if (!e) e = bj_end_array(b);
    uint64_t at = 0;
    if (!e) {
        size_t nlen; const uint8_t *next = bj_builder_data(b, &nlen);
        e = next ? rn_change_membership(n, next, (uint32_t)nlen, &at) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    if (e == RAFT_ERR_BUSY) return reply_error(n, 0, corr, NULL, 1);
    if (e) return reply_error(n, 0, corr, "membership change refused", 0);
    return defer(n, 0, corr);
}

/* A leave: the id, removed. Same redirect and idempotence rules. */
static int handle_leave(raft_node *n, uint64_t corr, const uint8_t *msg, uint32_t len) {
    uint64_t id = 0;
    if (rmsg_leave_id(msg, len, &id)) {
        return reply_error(n, 0, corr, "leave requires a member id", 0);
    }
    if (n->role != RAFT_LEADER) return reply_redirect(n, 0, corr);

    uint32_t elen = 0;
    if (!record_of(n, id, &elen)) return reply_members(n, 0, corr);  /* already gone */
    if (n->config_in_flight) return reply_error(n, 0, corr, NULL, 1);

    uint32_t mlen = 0;
    const uint8_t *members = members_span(n, &mlen);
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);
    cur c = { members, mlen, 0 };
    uint32_t count;
    if (!e && array_begin(&c, &count) == BJ_OK) {
        for (uint32_t i = 0; i < count && !e; i++) {
            size_t start = c.pos;
            if (skip_value(&c) != BJ_OK) { e = RAFT_ERR_MEMBER; break; }
            const uint8_t *r2 = members + start;
            uint32_t r2l = (uint32_t)(c.pos - start);
            const uint8_t *idv; uint32_t idl;
            if (!rmsg_record_field(r2, r2l, "id", &idv, &idl) && idv) {
                cur ic = { idv, idl, 0 };
                double d;
                if (read_number(&ic, &d) == BJ_OK && (uint64_t)d == id) continue;
            }
            e = bj_put_raw(b, r2, r2l);
        }
    }
    if (!e) e = bj_end_array(b);
    uint64_t at = 0;
    if (!e) {
        size_t nlen; const uint8_t *next = bj_builder_data(b, &nlen);
        e = next ? rn_change_membership(n, next, (uint32_t)nlen, &at) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    if (e == RAFT_ERR_BUSY) return reply_error(n, 0, corr, NULL, 1);
    if (e) return reply_error(n, 0, corr, "membership change refused", 0);
    return defer(n, 0, corr);
}

/*
 * TimeoutNow (section 3.10): the transferring leader certifies we are
 * fully caught up and asks us to stand NOW -- a real election, skipping
 * pre-vote, whose leader stickiness exists precisely to block challengers
 * while that leader still lives. Refused when stale-termed, when we
 * already lead, or when we hold no franchise (a learner cannot win the
 * election this would start).
 */
static int handle_timeout_now(raft_node *n, uint64_t peer, uint64_t corr,
                              const uint8_t *msg, uint32_t len, double random01) {
    uint64_t term = 0;
    rmsg_term(msg, len, &term);
    int ok = term >= elog_current_term(n->log) &&
             n->role != RAFT_LEADER && votes_now(n);
    int e = ok ? rn_campaign(n, random01) : BJ_OK;
    if (e) return e;
    dbuf r = {0};
    e = rmsg_build_ack(elog_current_term(n->log), ok, &r);
    if (e) { dbuf_free(&r); return e; }
    return reply_with(n, peer, corr, &r);
}

int rn_handle(raft_node *n, uint64_t corr,
              const uint8_t *msg, uint32_t len, double random01) {
    int kind = -1;
    int e = rmsg_kind(msg, len, &kind);
    if (e) return e;

    /* Who to answer comes from the message, not from the caller. A host
     * that had to name the sender could name it wrongly -- or, as the
     * JS one did, not know it at all and pass 0, which then rode into
     * the outbox and broke this file's own "every entry is addressed"
     * invariant on every inbound message. */
    /*
     * join and leave come from OUTSIDE the cluster -- an applicant has
     * no id yet, and an admin removing a dead node is not a member --
     * so they name no sender and are answered on the correlation id
     * alone. Everything else must say who it is.
     */
    if (kind == RAFT_MSG_JOIN)  return handle_join(n, corr, msg, len);
    if (kind == RAFT_MSG_LEAVE) return handle_leave(n, corr, msg, len);
    /* Also from outside, and from a process that may never become a
     * member at all: it is asking whether it should even start. */
    if (kind == RAFT_MSG_IDENTITY) return handle_identity(n, corr, msg, len);

    uint64_t from = 0;
    e = rmsg_sender(msg, len, &from);
    if (e) return e;

    if (kind == RAFT_MSG_TIMEOUT_NOW) {
        return handle_timeout_now(n, from, corr, msg, len, random01);
    }

    raft_msg_state st;
    fill_state(n, &st);
    raft_msg_effect eff;
    memset(&eff, 0, sizeof(eff));
    dbuf reply = {0};

    if (kind == RAFT_MSG_REQUEST_VOTE) {
        e = rmsg_handle_request_vote(n->log, &st, msg, len, &eff, &reply);
    } else if (kind == RAFT_MSG_APPEND_ENTRIES) {
        e = rmsg_handle_append_entries(n->log, &st, msg, len, &eff, &reply);
    } else if (kind == RAFT_MSG_INSTALL_SNAPSHOT && n->ns && n->store) {
        /* Answered here now: the files it writes go through the
         * namespace this node was given, over names the host pre-opened
         * from rn_install_plan. */
        dbuf_free(&reply);
        return handle_install(n, corr, msg, len, random01);
    } else {
        /* An install with no namespace to write through. Refused
         * exactly as before, so a host that serves them itself is
         * untouched by any of this. */
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
static rn_peer *peer_by_corr(raft_node *n, uint64_t corr) {
    for (uint32_t i = 0; i < n->npeers; i++)
        if (n->peers[i].inflight == corr) return &n->peers[i];
    return NULL;
}

static int on_vote_reply(raft_node *n, uint64_t corr, const uint8_t *reply,
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
        /* This ack may be the one that proves the transfer target
         * current (the trigger is its own no-op for every other peer). */
        if (n->transfer_target == p->id) {
            int te = maybe_send_timeout_now(n);
            if (te) return te;
        }
        if (p->next <= elog_last_index(n->log)) return replicate_to(n, p);
        return BJ_OK;
    }

    raft_backoff_out b;
    raft_backoff((uint64_t)hint, have_hint, p->next, p->match, &b);
    p->next = b.next;
    p->match = b.match;
    return replicate_to(n, p);
}

int rn_on_reply(raft_node *n, uint64_t corr, const uint8_t *reply, uint32_t len,
                double random01) {
    if (n->transfer_corr && corr == n->transfer_corr) {
        /* The TimeoutNow's ack, {term, ok}. ok means an election is
         * coming and the RequestVote at term+1 finishes this, so the
         * corr STAYS SET -- re-sending against a target whose election
         * is in flight would bump terms for nothing. A refusal clears
         * it, and the next ack decides again. A higher term deposes us
         * like any reply's would -- which is itself the transfer
         * arriving (become_follower reports it). */
        const uint8_t *v; size_t vlen; int found = 0;
        int ok = 0;
        if (obj_get_field(reply, len, (const uint8_t *)"ok", 2, &v, &vlen, &found) == BJ_OK && found) {
            cur c = { v, vlen, 0 };
            read_bool(&c, &ok);
        }
        uint64_t term = 0;
        rmsg_term(reply, len, &term);
        if (term > elog_current_term(n->log)) {
            become_follower(n, term, 0, random01);
            return BJ_OK;
        }
        if (!ok) n->transfer_corr = 0;
        return BJ_OK;
    }
    rn_peer *p = peer_by_corr(n, corr);
    if (p) {
        p->inflight = 0;
        /* Whatever this answer turns out to say, it answers a request
         * sent at `sent_at` -- which is how far back it can vouch for
         * this peer still following us. Recorded before the handlers,
         * which may step us down and clear everything. */
        p->ack_covers = p->sent_at;
        /* Which kind of reply this is cannot be read off the correlation
         * id -- the peer's own state is what says so. Reading an
         * install's answer as an AppendEntries' would advance a match
         * index on the strength of a chunk. */
        int e = p->installing ? on_install_reply(n, p, reply, len, random01)
                              : on_append_reply(n, p, reply, len, random01);
        /* This answer may be the one a read was waiting for. Checked
         * here rather than inside the handlers so there is one place
         * where "a peer spoke" becomes "a barrier is confirmed". */
        int c = check_reads(n);
        return e ? e : c;
    }
    /* Not an append: the only other thing we send is a vote request, and
     * those are not tracked per peer because the round tallies them --
     * it checks the correlation id against its own range instead. */
    return on_vote_reply(n, corr, reply, len, random01);
}

int rn_on_fail(raft_node *n, uint64_t corr) {
    if (n->transfer_corr && corr == n->transfer_corr) {
        /* Nobody answered the TimeoutNow. Cleared, so the next ack that
         * proves the target alive and current sends another. */
        n->transfer_corr = 0;
        return BJ_OK;
    }
    rn_peer *p = peer_by_corr(n, corr);
    if (!p) return BJ_OK;   /* a vote request nobody answered; the timer retries */
    p->inflight = 0;
    /* A transfer whose chunk never arrived is a transfer to abandon: the
     * next heartbeat decides again, from the manifest. */
    p->installing = 0;
    p->chunk_done = 0;
    if (p->reachable != 0) { p->reachable = 0; emit(n, RN_EFFECT_REACHABLE, p->id, 0); }
    return BJ_OK;
}

/* ---- outbox and effects ------------------------------------------------- */

uint32_t rn_out_count(const raft_node *n) { return n->nout; }
uint64_t rn_out_peer(const raft_node *n, uint32_t i) { return i < n->nout ? n->out[i].peer : 0; }
uint64_t rn_out_corr(const raft_node *n, uint32_t i) { return i < n->nout ? n->out[i].corr : 0; }
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
uint64_t rn_inflight(const raft_node *n, uint64_t peer) {
    const rn_peer *p = peer_of((raft_node *)n, peer);
    return p ? p->inflight : 0;
}

int rn_is_quiesced(const raft_node *n) { return n->quiesced; }

/* ---- membership orchestration ------------------------------------------- */

int rn_propose(raft_node *n, int type, const uint8_t *payload, uint32_t len,
               uint64_t *out_index);

/*
 * Answer everyone waiting on the change that just landed.
 *
 * `ok` with the adopted records if it landed; a redirect if this node
 * stopped being the one who could land it. Nobody is left holding a
 * promise that will never settle -- which for a joiner means a retry
 * against the new leader rather than a hang until its transport gives up.
 */
static void flush_pending(raft_node *n, int ok) {
    if (!n->npending) return;
    uint32_t mlen = 0;
    const uint8_t *members = members_span(n, &mlen);
    uint32_t llen = 0;
    const uint8_t *leader = n->leader_id ? record_of(n, n->leader_id, &llen) : NULL;

    for (uint32_t i = 0; i < n->npending; i++) {
        dbuf reply = {0};
        int e = rmsg_build_membership_reply(ok, members, mlen, NULL, 0,
                                            n->leader_id, leader, llen, &reply);
        if (!e) queue(n, n->pending[i].peer, n->pending[i].corr, 1, reply.data, reply.len);
        dbuf_free(&reply);
    }
    n->npending = 0;
}

int rn_change_membership(raft_node *n, const uint8_t *members, uint32_t len,
                         uint64_t *out_index) {
    if (n->role != RAFT_LEADER) return BJ_ERR_STATE;
    if (n->config_in_flight) return RAFT_ERR_BUSY;

    /* Merge with what the log already carries, so an id-only proposal
     * cannot erase an address (raft_core.h's members_merge). */
    uint32_t known_len = 0;
    const uint8_t *known = members_span(n, &known_len);
    dbuf merged = {0};
    int e = raft_members_merge(members, len, known, known_len, &merged);
    if (e) { dbuf_free(&merged); return e; }

    /*
     * Refuse an oversized set HERE, before it is proposed. Committed, it
     * would be refused at APPLY by every replica alike, where the only
     * honest response left is to halt -- so the check belongs where a
     * caller is still standing.
     */
    {
        dbuf probe = {0};
        e = raft_members_adopt(merged.data, (uint32_t)merged.len, n->self_id, &probe);
        if (!e) {
            const uint8_t *v; size_t vlen; int found = 0;
            uint32_t count = 0;
            if (!obj_get_field(probe.data, probe.len, (const uint8_t *)"peers", 5,
                               &v, &vlen, &found) && found) {
                cur c = { v, vlen, 0 };
                if (array_begin(&c, &count) != BJ_OK) count = 0;
            }
            if (count > RN_MAX_PEERS) e = RAFT_ERR_CAPACITY;
        }
        dbuf_free(&probe);
        if (e) { dbuf_free(&merged); return e; }
    }

    /* The CONFIG entry's payload: { members: [records] }. */
    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&merged); return BJ_ERR_OOM; }
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"members", 7);
    if (!e) e = bj_put_raw(b, merged.data, (uint32_t)merged.len);
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t plen; const uint8_t *payload = bj_builder_data(b, &plen);
        if (!payload) e = BJ_ERR_STATE;
        else e = rn_propose(n, EL_CONFIG, payload, (uint32_t)plen, out_index);
    }
    bj_builder_free(b);
    dbuf_free(&merged);
    if (!e) n->config_in_flight = 1;
    return e;
}

int rn_config_in_flight(const raft_node *n) { return n->config_in_flight; }

/* ---- what the host still owns ------------------------------------------- */

/* ---- completions -------------------------------------------------------- */

int rn_await(raft_node *n, uint64_t index, uint64_t term) {
    if (!n) return BJ_ERR_STATE;
    if (n->nawait >= RN_MAX_AWAIT) return RAFT_ERR_CAPACITY;
    n->await[n->nawait].index = index;
    n->await[n->nawait].term  = term;
    n->nawait++;
    return BJ_OK;
}

uint32_t rn_awaiting(const raft_node *n) { return n ? n->nawait : 0; }
uint32_t rn_max_await(void) { return RN_MAX_AWAIT; }

/*
 * Answer everything at or below `floor`, and keep the rest.
 *
 * `kept` is read from the LOG, not assumed from the fact that the index
 * applied: an entry at your index is not your entry. Compacted away is
 * the same answer as overwritten -- a log based past the index cannot
 * vouch for what was there, and vouching anyway is precisely the lenient
 * direction that turns a lost write into a reported success.
 */
static void settle_through(raft_node *n, uint64_t floor) {
    uint32_t keep = 0;
    for (uint32_t i = 0; i < n->nawait; i++) {
        uint64_t index = n->await[i].index;
        if (index > floor) { n->await[keep++] = n->await[i]; continue; }
        uint64_t at_term = 0;
        int kept = elog_term_at(n->log, index, &at_term) == BJ_OK &&
                   at_term == n->await[i].term;
        emit(n, RN_EFFECT_SETTLED, index, kept);
    }
    n->nawait = keep;
}

/* Nobody is left holding a request this node can no longer finish. A
 * step-down or a stop makes every outstanding proposal unanswerable
 * here, whatever its index says. */
static void settle_all_lost(raft_node *n) {
    for (uint32_t i = 0; i < n->nawait; i++)
        emit(n, RN_EFFECT_SETTLED, n->await[i].index, 0);
    n->nawait = 0;
}

void rn_applied(raft_node *n, uint64_t index) {
    if (n) settle_through(n, index);
}

int rn_propose(raft_node *n, int type, const uint8_t *payload, uint32_t len,
               uint64_t *out_index) {
    if (n->role != RAFT_LEADER) return BJ_ERR_STATE;
    /* Before the append, not after: an entry in the log that nobody will
     * ever be told the fate of is worse than a proposal refused while
     * its caller is still standing. */
    if (n->nawait >= RN_MAX_AWAIT) return RAFT_ERR_CAPACITY;

    uint64_t term = elog_current_term(n->log);
    uint64_t at = 0;
    int e = elog_append(n->log, term, type, payload, len, &at);
    if (e) return e;
    rn_await(n, at, term);
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

int rn_read_barrier(raft_node *n, uint64_t *token, uint64_t *read_index) {
    if (!n || !token || !read_index) return BJ_ERR_STATE;
    *token = 0;
    *read_index = 0;
    /* A node that does not lead cannot say what is committed, so there
     * is nothing to take a barrier against. The host refuses with a
     * routing error the caller can act on. */
    if (!n->running || n->role != RAFT_LEADER) return BJ_ERR_STATE;

    int slot = -1;
    for (uint32_t i = 0; i < RN_MAX_READS; i++)
        if (!n->reads[i].used) { slot = (int)i; break; }
    if (slot < 0) return RAFT_ERR_CAPACITY;

    n->reads[slot].used = 1;
    n->reads[slot].state = 0;
    n->reads[slot].token = ++n->read_seq;
    n->reads[slot].started = n->now;
    *token = n->reads[slot].token;
    /*
     * The commit index AS OF NOW. Everything committed before this
     * barrier is at or below it, which is what the confirmation will
     * prove -- so a caller serving state at or above this index has
     * served every write that finished before it asked.
     */
    *read_index = n->commit_index;
    return check_reads(n);
}

int rn_read_state(const raft_node *n, uint64_t token) {
    if (!n || !token) return -1;
    for (uint32_t i = 0; i < RN_MAX_READS; i++)
        if (n->reads[i].used && n->reads[i].token == token) return n->reads[i].state;
    /* Released, or never issued. Lost is the safe answer either way: the
     * caller refuses rather than serving on the strength of a barrier
     * nobody is holding. */
    return -1;
}

void rn_read_release(raft_node *n, uint64_t token) {
    if (!n || !token) return;
    for (uint32_t i = 0; i < RN_MAX_READS; i++) {
        if (!n->reads[i].used || n->reads[i].token != token) continue;
        memset(&n->reads[i], 0, sizeof n->reads[i]);
        return;
    }
}

uint32_t rn_reads_outstanding(const raft_node *n) {
    uint32_t c = 0;
    if (!n) return 0;
    for (uint32_t i = 0; i < RN_MAX_READS; i++) if (n->reads[i].used) c++;
    return c;
}

void rn_seed_commit(raft_node *n, uint64_t index) {
    if (index > n->commit_index) n->commit_index = index;
}

int rn_campaign(raft_node *n, double random01) {
    if (!n->running || n->role == RAFT_LEADER || !votes_now(n)) return BJ_OK;
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
