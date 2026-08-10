/* server/replica.c — see replica.h. */
#include "replica.h"

#include "raft_node.h"
#include "raft_msg.h"      /* the member-record grammar; this file reads it */
#include "db_validate.h"   /* dc_strerror, and the two routing codes */
#include "entrylog.h"
#include "snapstore.h"     /* generations and the log-naming rule */
#include "bjfile.h"        /* bjfile_crc32, the store's checksum */
#include "binjson.h"
#include "bjcursor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The log, when no snapshot generation has been adopted. The same name
 * src/db-wal.js opens, so a database written by one host is a database
 * the other can serve. */
#define REPLICA_WAL "__wal__.bj"

/* The snapshot store's file prefix: "__snap__-<gen>-<role>.bj" and kin,
 * beside the log and the database directories in the root. Double
 * underscores for the same reason the log has them -- root_list refuses
 * to present these to a client as databases (they are files, and it
 * lists directories), and a person listing the directory should see at
 * a glance which files are the instance's own. */
#define REPLICA_SNAP_PREFIX "__snap__"

/* Copy buffer for streaming a file into or out of a generation. */
#define REPLICA_COPY_BYTES (64u * 1024u)

/*
 * Writes waiting on the log at once. One per connection is the shape the
 * transport already has -- a connection is served one request at a time
 * and is not read from while it owes bytes -- so this matches the
 * session's own bound, and a request over it is refused rather than
 * queued.
 */
#define REPLICA_MAX_PENDING DBS_MAX_INFLIGHT

/* How much of the log the pump reads per pass. */
#define REPLICA_APPLY_BYTES (256u * 1024u)

/*
 * Peer requests whose ANSWER has not been built yet.
 *
 * Nearly every message is answered inside the rn_handle that took it, so
 * nearly every entry here lives for the length of one call. A JOIN or a
 * LEAVE is the exception and the reason this is a table at all: its
 * answer is a fact about a CONFIG entry that does not exist yet, so the
 * node parks the requester and the reply arrives later -- when that
 * entry applies, or when this node stops leading. The conversation it
 * came in on has to outlive the call, or the answer is built and lost.
 *
 * Bigger than the node's own parking bay (RN_MAX_PENDING, 16), so the
 * two cannot disagree about who is waiting: the node settles every one
 * of its entries, and each settlement needs a conversation here to go
 * out on. The spare slots are the in-flight ordinary requests, which
 * come and go within a call.
 */
#define REPLICA_MAX_CONV 24

/*
 * One client's request, somewhere between taken and answered.
 *
 * TWO KINDS SHARE THIS TABLE, because they end the same way: an answer
 * that was not ready when the request arrived, delivered later through
 * replica_ready.
 *
 * A WRITE is waiting on the log. `last` is the index whose settlement
 * finishes the batch in flight; entries commit, apply and settle in
 * index order, so the last one standing in for all of them is not an
 * optimization -- an earlier entry of the same batch cannot survive a
 * truncation the later one did not.
 *
 * A READ (`barrier` non-zero) has already been PERFORMED and is waiting
 * on the proof that it may be shown: a quorum confirming this node still
 * leads. Executing before confirming is deliberate -- see
 * replica_submit -- so what is parked here is the finished answer.
 */
typedef struct {
    int      used;
    uint64_t client;
    uint64_t token;             /* dbs_step's */
    uint64_t barrier;           /* rn_read_barrier's; 0 = this is a write */
    uint64_t   *indices;        /* what the batch in flight committed at */
    dbuf       *results;        /* and what applying each one produced */
    dbs_result *view;           /* the same, in the shape dbs_step takes */
    uint32_t n, cap, at;
    uint64_t last;
    int      done;              /* the answer is built and waiting */
    dbuf     answer;
} pending;

/*
 * One peer request, from the moment it arrives to the moment its answer
 * goes out.
 *
 * `mine` is the correlation id rn_handle was GIVEN, and it is minted
 * here rather than taken off the wire. Two peers -- or two joiners, both
 * of which send id 0, because a one-shot call to an address has nothing
 * to correlate against -- can perfectly well use the same id at the same
 * time, and a table keyed by theirs would answer one of them on the
 * other's socket. `theirs` is what goes back on the wire, because that
 * is what the sender is matching on. (src/raft.js mints its own the same
 * way, for the same reason.)
 */
typedef struct {
    int      used;
    uint64_t mine;
    uint64_t theirs;
    uint64_t from;              /* the conversation, peers.h's */
} conversation;

struct replica {
    bj_ns     *ns;              /* borrowed */
    dbi       *inst;            /* borrowed */
    peers     *px;              /* borrowed; NULL is a group of one */
    root_state *rt;             /* borrowed; the directory listings */
    bj_io      log_io;          /* meaningful only while host_owns_log */
    elog      *log;             /* mirror of rn_log once the node owns it */
    int        host_owns_log;   /* 0 after an adopt or a local compaction */
    raft_node *node;
    sst       *store;           /* owned; the snapshot store's policy */
    uint64_t   snap_every;      /* applied entries between snapshots; 0 = never */
    uint64_t   applied;
    uint64_t   rnd;             /* the election-timeout draw's state */
    int        tick_ms;         /* the clock slice, derived from the heartbeat */
    uint64_t   self_id;
    uint64_t   next_corr;       /* inbound ids, minted here */
    uint64_t   config_index;    /* the CONFIG entry in force */
    int        said_no_snapshot;
    int        said_no_conversation;
    int        said_no_address;
    int        said_snap_failed;
    conversation conv[REPLICA_MAX_CONV];
    pending    waiting[REPLICA_MAX_PENDING];
    /* The client waiting on a transferLeadership, if any: the node owns
     * the transfer itself (rn_transfer), this is only where its answer
     * goes. NULL when nobody asked -- or the asker hung up, in which
     * case leadership moves anyway and the answer has nowhere to go,
     * exactly like a proposer's (replica_drop_client). */
    pending   *transfer_waiter;

    /*
     * READ ROUTING. `read_threads` is how many reader threads were asked
     * for; `read_min_docs` is the collection size below which no read is
     * long enough to be worth moving (db_session.h's dbs_read_is_long).
     *
     * The two counters are how a test -- and an operator -- sees the
     * routing decision without measuring time. Reported by `ping`, beside
     * the log's bounds and for the same reason: a decision nobody can
     * observe is a decision nobody can check.
     *
     * The decision is only ASKED when read_threads > 0. Sizing a read
     * costs a collection resolve and a dc_explain, and a server that was
     * never asked for reader threads must pay exactly what it paid
     * before -- so with the default of 0, this whole mechanism is a
     * comparison against zero.
     */
    int        read_threads;
    int64_t    read_min_docs;
    uint64_t   reads_long;      /* judged worth moving off this thread */
    uint64_t   reads_short;     /* judged cheaper to answer here */
};

static conversation *conv_open(replica *r, uint64_t theirs, uint64_t from) {
    for (int i = 0; i < REPLICA_MAX_CONV; i++) {
        if (r->conv[i].used) continue;
        r->conv[i].used = 1;
        r->conv[i].mine = ++r->next_corr;
        r->conv[i].theirs = theirs;
        r->conv[i].from = from;
        return &r->conv[i];
    }
    return NULL;
}

static conversation *conv_find(replica *r, uint64_t mine) {
    for (int i = 0; i < REPLICA_MAX_CONV; i++)
        if (r->conv[i].used && r->conv[i].mine == mine) return &r->conv[i];
    return NULL;
}

static void conv_close(conversation *c) { memset(c, 0, sizeof *c); }

/*
 * A DIFFERENT election timeout per member.
 *
 * raft_node.h takes `random01` as an argument rather than drawing one --
 * a node that reads its own random source is one the simulator cannot
 * replay, and test/raft-harness.js's determinism is the biggest asset
 * the port has. So the draw is the host's, and it has to be a real draw:
 * three members that all pass 0.5 arm the same deadline, campaign in the
 * same millisecond, split the vote, and do it again. A group of one
 * never noticed, which is exactly why this arrived with the peers.
 *
 * xorshift64*, seeded from the node id and the clock it started on.
 * Nothing here is secret -- what is needed is that two members disagree,
 * not that nobody can predict them -- and a clock in the TRANSPORT is
 * already allowed (server/main.c says why; db.h keeps them out of the
 * engine, and nothing below this file learns what time it is).
 */
static double rnd01(replica *r) {
    uint64_t x = r->rnd;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    r->rnd = x;
    return (double)((x * 0x2545F4914F6CDD1DULL) >> 11) / 9007199254740992.0;
}

static void pending_release(pending *p) {
    for (uint32_t i = 0; i < p->cap; i++) dbuf_free(&p->results[i]);
    free(p->results);
    free(p->view);
    free(p->indices);
    dbuf_free(&p->answer);
    memset(p, 0, sizeof *p);
}

/*
 * The request waiting on `index`, and where to put what applying it
 * produces. NULL when nobody local proposed it -- a follower's entries,
 * or a leader's whose client has gone.
 */
static pending *pending_for_index(replica *r, uint64_t index, dbuf **into) {
    for (int i = 0; i < REPLICA_MAX_PENDING; i++) {
        pending *p = &r->waiting[i];
        if (!p->used || p->done || p->barrier) continue;
        for (uint32_t k = 0; k < p->n; k++) {
            if (p->indices[k] != index) continue;
            p->at = k;
            p->results[k].len = 0;
            *into = &p->results[k];
            return p;
        }
    }
    return NULL;
}

static pending *pending_find(replica *r, uint64_t last) {
    for (int i = 0; i < REPLICA_MAX_PENDING; i++)
        if (r->waiting[i].used && !r->waiting[i].done && !r->waiting[i].barrier &&
            r->waiting[i].last == last)
            return &r->waiting[i];
    return NULL;
}

/* ---- the member set -----------------------------------------------------
 *
 * ONE OWNER. The set, the voter list and the addresses are all the
 * node's -- rn_adopted is where they are written down, normalized once,
 * and everything here reads them back rather than keeping a copy. A
 * second address book would disagree with the log the first time a
 * member moved, and the disagreement would be invisible: a member with a
 * stale address is a member that silently never receives an entry.
 */

/* The `members` ARRAY inside the adopted set: the records themselves. */
static const uint8_t *members_of(const replica *r, uint32_t *len) {
    uint32_t alen = 0;
    const uint8_t *adopted = rn_adopted(r->node, &alen);
    *len = 0;
    if (!adopted) return NULL;
    const uint8_t *ms; size_t mslen; int found = 0;
    if (obj_get_field(adopted, alen, (const uint8_t *)"members", 7,
                      &ms, &mslen, &found) != BJ_OK || !found) return NULL;
    *len = (uint32_t)mslen;
    return ms;
}

static uint64_t record_id(const uint8_t *rec, uint32_t len) {
    const uint8_t *v; uint32_t vlen;
    if (rmsg_record_field(rec, len, "id", &v, &vlen) != BJ_OK || !v) return 0;
    cur c = { v, vlen, 0 };
    uint64_t id = 0;
    return read_u64(&c, &id) == BJ_OK ? id : 0;
}

/* The host and port a record carries, if it carries both. */
static int record_address(const uint8_t *rec, uint32_t len,
                          char *host, size_t cap, int *port) {
    const uint8_t *v; uint32_t vlen;
    if (rmsg_record_field(rec, len, "host", &v, &vlen) != BJ_OK || !v) return -1;
    cur hc = { v, vlen, 0 };
    const uint8_t *s; uint32_t slen;
    if (take_string(&hc, &s, &slen) != BJ_OK || (size_t)slen >= cap) return -1;
    memcpy(host, s, slen);
    host[slen] = '\0';

    if (rmsg_record_field(rec, len, "port", &v, &vlen) != BJ_OK || !v) return -1;
    cur pc = { v, vlen, 0 };
    uint64_t p = 0;
    if (read_u64(&pc, &p) != BJ_OK || !p || p > 65535) return -1;
    *port = (int)p;
    return 0;
}

/*
 * The transport's address table, made to say what the member set says.
 *
 * This is the step whose absence is silent. A member added by a CONFIG
 * entry that the transport has no address for is a member the leader
 * cannot reach: peers_request answers "not a member here", the node is
 * told the request failed, and the picture from outside is a follower
 * that is simply always behind.
 */
static int sync_peers(replica *r) {
    if (!r->px) return BJ_OK;
    uint32_t mlen = 0;
    const uint8_t *ms = members_of(r, &mlen);
    if (!ms) return BJ_OK;

    uint64_t named[PEERS_MAX + 1];
    uint32_t n_named = 0;
    cur c = { ms, mlen, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count) != BJ_OK) return RAFT_ERR_MEMBER;

    int e = BJ_OK;
    for (uint32_t i = 0; i < count && !e; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) return RAFT_ERR_MEMBER;
        const uint8_t *rec = c.d + start;
        uint32_t rlen = (uint32_t)(c.pos - start);
        uint64_t id = record_id(rec, rlen);
        if (!id || id == r->self_id) continue;
        if (n_named < sizeof named / sizeof named[0]) named[n_named++] = id;

        char host[PEERS_HOST_MAX];
        int port = 0;
        if (record_address(rec, rlen, host, sizeof host, &port) != 0) {
            /* A member nothing can reach. Not fatal -- the rest of the
             * cluster still works -- but never silent, because from the
             * outside it is indistinguishable from a slow follower. */
            if (!r->said_no_address) {
                r->said_no_address = 1;
                fprintf(stderr, "replica: member %llu has no address in the log;"
                                " nothing can replicate to it\n",
                        (unsigned long long)id);
                fflush(stderr);
            }
            continue;
        }
        e = peers_set(r->px, id, host, port);
        if (e) {
            /*
             * The transport holds fewer members than the node will
             * accept (PEERS_MAX bounds a pollfd array; rn_max_peers is
             * larger), so a set that fits there can still not fit here.
             * It reaches the apply pump as an error and halts this
             * member -- which is the only honest answer to a committed
             * entry it cannot carry out -- so it says which member it
             * ran out of room at rather than only that it stopped.
             */
            fprintf(stderr, "replica: no room for member %llu: this build holds"
                            " %d other members\n",
                    (unsigned long long)id, PEERS_MAX);
            fflush(stderr);
        }
    }

    /* And nobody the set does not name. A departed member left behind
     * here is a socket redialled forever. */
    for (uint32_t i = 0; !e && i < peers_count(r->px); ) {
        uint64_t id = peers_id_at(r->px, i);
        int keep = 0;
        for (uint32_t k = 0; k < n_named; k++) if (named[k] == id) { keep = 1; break; }
        if (keep) { i++; continue; }
        e = peers_remove(r->px, id);   /* it compacts: do not advance */
    }
    return e;
}

/* Whether the set in force still counts this node -- read off the
 * node's own normalized voter list rather than re-derived, which is the
 * same rule the addresses follow. */
static int self_is_voter(const replica *r) {
    uint32_t alen = 0;
    const uint8_t *adopted = rn_adopted(r->node, &alen);
    if (!adopted) return 1;
    const uint8_t *vs; size_t vslen; int found = 0;
    if (obj_get_field(adopted, alen, (const uint8_t *)"voters", 6,
                      &vs, &vslen, &found) != BJ_OK || !found) return 1;
    cur c = { vs, vslen, 0 };
    uint32_t n = 0;
    if (array_begin(&c, &n) != BJ_OK) return 1;
    for (uint32_t i = 0; i < n; i++) {
        uint64_t id = 0;
        if (read_u64(&c, &id) != BJ_OK) return 1;
        if (id == r->self_id) return 1;
    }
    return 0;
}

/*
 * A CONFIG entry taking effect: `{ members: [records] }`.
 *
 * A set the node REFUSES (malformed, or larger than this build holds)
 * reaches the apply pump as an error, and the pump halts on it. That is
 * the honest answer rather than a lenient one: every replica refuses the
 * same entry for the same reason, so the cluster stops together instead
 * of one member quietly replicating to a different membership than the
 * rest. rn_change_membership refuses such a set where a caller is still
 * standing, so reaching here means it arrived some other way.
 */
static int adopt_config(replica *r, const uint8_t *payload, uint32_t len,
                        uint64_t index) {
    const uint8_t *ms; size_t mslen; int found = 0;
    if (obj_get_field(payload, len, (const uint8_t *)"members", 7,
                      &ms, &mslen, &found) != BJ_OK || !found) return RAFT_ERR_MEMBER;
    int e = rn_set_members(r->node, ms, (uint32_t)mslen);
    if (e) return e;
    r->config_index = index;

    /*
     * Say that this entry COMMITTED before deciding whether we are still
     * one of the members it names. Membership takes effect at APPLY
     * here, and a follower holding the entry keeps the old set until it
     * learns the entry committed -- which only the leader can tell it.
     *
     * A leader applying its OWN removal used to step down first and
     * replicate second (at the call site below, under a
     * replica_is_leader guard that the step-down had just falsified), so
     * the second half never ran: it left carrying the commit index. The
     * survivors kept a configuration that still listed the departed
     * leader, and where that configuration's quorum needed it -- the
     * two-voter shrink, the commonest shape there is -- they could not
     * elect anyone to tell them otherwise. The group was lost, not slow,
     * while the caller of the leave had been told it succeeded, because
     * it had: the entry committed. Raft's "the leader steps down once
     * C_new is committed" describes the moment AFTER the cluster has
     * been told.
     *
     * Against the peer table as it stands -- the set BEFORE this change,
     * since sync_peers has not run yet -- which is what we want twice
     * over: it is a superset of the new members, so a member this entry
     * REMOVES also hears that it committed, applies its own removal, and
     * stops campaigning against a cluster that has moved on.
     */
    if (rn_role(r->node) == RAFT_LEADER && r->px)
        for (uint32_t i = 0; i < peers_count(r->px); i++)
            rn_replicate(r->node, peers_id_at(r->px, i));

    /*
     * Applied our own removal, or our own demotion to learner. A node
     * that goes on leading a cluster it is not in would be counting a
     * vote nobody else counts.
     */
    if (!self_is_voter(r) && rn_role(r->node) != RAFT_FOLLOWER)
        rn_step_down(r->node, elog_current_term(r->log), rnd01(r));
    return sync_peers(r);
}

/*
 * The last CONFIG entry in the log, adopted over whatever argv said.
 *
 * THE LOG IS THE TRUTH AND ARGV IS A BOOTSTRAP. Once membership is
 * written down, a member restarted with a stale --peer list must not
 * overwrite what the cluster agreed -- and a member that joined has no
 * --peer list at all, so its own log is the only place its cluster is
 * described.
 *
 * Every entry rather than the committed prefix, and the LAST one wins:
 * a config takes effect when it is appended, not when it commits (the
 * paper's rule, and src/raft.js's restart scan does exactly this). The
 * state machine's applied floor may already sit past a CONFIG entry it
 * never recorded, so the apply pump alone cannot be relied on to find
 * them.
 */
static int adopt_from_log(replica *r) {
    uint64_t last = elog_last_index(r->log);
    uint64_t found = 0;
    for (uint64_t i = elog_base_index(r->log) + 1; i <= last; i++) {
        uint64_t term = 0;
        int type = 0;
        const uint8_t *p = NULL;
        size_t plen = 0;
        if (elog_get(r->log, i, &term, &type, &p, &plen) != BJ_OK) return BJ_ERR_STATE;
        if (type == EL_CONFIG) found = i;
    }
    if (!found) return BJ_OK;

    uint64_t term = 0;
    int type = 0;
    const uint8_t *p = NULL;
    size_t plen = 0;
    int e = elog_get(r->log, found, &term, &type, &p, &plen);
    if (e) return e;
    /* The log owns that pointer only until the next operation on the
     * log, and adopting is not one -- but nothing downstream promises
     * that, so it is copied. */
    dbuf payload = {0};
    e = dbuf_put(&payload, p, plen);
    if (!e) e = adopt_config(r, payload.data, (uint32_t)payload.len, found);
    dbuf_free(&payload);
    return e;
}

/*
 * A learner the node says has caught up: the same set with that one
 * record's voting flag lifted.
 *
 * THE DECISION IS NOT HERE. The node raised the effect, on match index,
 * and it raises it again on the next successful append while the peer is
 * still a learner -- so a moment when the change cannot be made (another
 * one in flight, leadership just lost) needs no retry of its own. This
 * only carries out what was decided, which is why it can only ever WIDEN
 * the electorate, and only with a replica proven current.
 *
 * Explicitly voting:true rather than dropping the key: raft_members_merge
 * fills an absent field from the record it already knows, so an omission
 * would re-inherit the voting:false being lifted.
 */
static int promote(replica *r, uint64_t peer) {
    if (!replica_is_leader(r) || rn_config_in_flight(r->node)) return BJ_OK;
    uint32_t mlen = 0;
    const uint8_t *ms = members_of(r, &mlen);
    if (!ms) return BJ_OK;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    cur c = { ms, mlen, 0 };
    uint32_t count = 0;
    int e = array_begin(&c, &count);
    if (!e) e = bj_begin_array(b);
    for (uint32_t i = 0; i < count && !e; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) { e = RAFT_ERR_MEMBER; break; }
        const uint8_t *rec = c.d + start;
        uint32_t rlen = (uint32_t)(c.pos - start);
        e = record_id(rec, rlen) == peer ? rmsg_record_with_voting(rec, rlen, 1, b)
                                         : bj_put_raw(b, rec, rlen);
    }
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        uint64_t at = 0;
        e = d ? rn_change_membership(r->node, d, (uint32_t)len, &at) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    /* Busy, or no longer the leader: both are "not now", and the node
     * will say so again while it still holds. */
    if (e == RAFT_ERR_BUSY || e == BJ_ERR_STATE) e = BJ_OK;
    return e;
}

/* ---- opening ------------------------------------------------------------ */

/* One member record: { id, host?, port? }. The address goes in because
 * the log is where a cluster's shape is written down (raft_core.h says
 * so) -- a separate address book would be a second copy of it, and the
 * two would disagree the first time a member moved. */
static int put_member(bj_builder *b, uint64_t id, const char *host, int port) {
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"id", 2);
    if (!e) e = bj_put_int(b, (int64_t)id);
    if (host && port > 0) {
        if (!e) e = bj_put_key(b, (const uint8_t *)"host", 4);
        if (!e) e = bj_put_string(b, (const uint8_t *)host, (uint32_t)strlen(host));
        if (!e) e = bj_put_key(b, (const uint8_t *)"port", 4);
        if (!e) e = bj_put_int(b, port);
    }
    if (!e) e = bj_end_object(b);
    return e;
}

/*
 * The log, read the way a resumed change stream needs it (db_session.h's
 * dbs_log): the base below which entries are gone, the applied floor
 * replay may run to, and one entry's command for one database. The
 * replica owns the log and the instance hands these to every database it
 * opens -- the session decides what to replay, this side only delivers
 * the bytes, which is the same split as everywhere else.
 */
static uint64_t log_base(void *ctx)  { return elog_base_index(((replica *)ctx)->log); }
static uint64_t log_floor(void *ctx) { return ((replica *)ctx)->applied; }

static int log_entry(void *ctx, const char *db, size_t db_len, uint64_t index,
                     dbuf *cmd) {
    replica *r = (replica *)ctx;
    uint64_t term = 0;
    int type = 0;
    const uint8_t *p = NULL;
    size_t plen = 0;
    int e = elog_get(r->log, index, &term, &type, &p, &plen);
    if (e) return e;
    if (type != EL_NORMAL) return BJ_OK;   /* config, no-op: no command */
    const uint8_t *c = NULL; size_t clen = 0;
    e = dbi_entry_cmd(p, (uint32_t)plen, db, db_len, &c, &clen);
    if (e || !c) return e;
    /* Copied out: the log owns `p` and it dies on the next log call. */
    return dbuf_put(cmd, c, clen);
}

/* ---- snapshots: local compaction, and the log-naming rule ---------------
 *
 * See replica.h's header. Everything below is the HOST half of the
 * snapshot story: the node already serves an install from a committed
 * generation and adopts one it receives (raft_node.h); what nothing did
 * until here was MAKE a generation, which is the act that bounds the
 * log and arms all of it.
 */

/* Copy `src` to `dst` through the namespace, checksumming and counting
 * as it goes -- the same chunked walk rn_adopt's restore makes. */
static int copy_file(bj_ns *ns, const char *src, uint32_t src_len,
                     const char *dst, uint32_t dst_len,
                     uint64_t *size_out, uint32_t *crc_out) {
    bj_io in, dout;
    int e = ns->open(ns->ctx, src, src_len, 0, &in);
    if (e) return e;
    e = ns->open(ns->ctx, dst, dst_len, BJ_NS_CREATE | BJ_NS_TRUNC, &dout);
    if (e) { if (in.close) in.close(in.ctx); return e; }
    uint8_t *buf = (uint8_t *)malloc(REPLICA_COPY_BYTES);
    if (!buf) e = BJ_ERR_OOM;
    uint64_t total = in.size(in.ctx), at = 0;
    uint32_t crc = 0;
    while (!e && at < total) {
        uint32_t want = (uint32_t)((total - at > REPLICA_COPY_BYTES)
                                       ? REPLICA_COPY_BYTES : (total - at));
        int64_t got = in.read(in.ctx, at, buf, want);
        if (got <= 0) { e = got < 0 ? (int)got : BJ_ERR_EOF; break; }
        crc = bjfile_crc32(crc, buf, (size_t)got);
        e = dout.write(dout.ctx, at, buf, (uint32_t)got);
        at += (uint64_t)got;
    }
    free(buf);
    /* Durable before anything depends on it: a generation file still in
     * a buffer is a snapshot the next crash does not have. */
    if (!e && dout.sync) e = dout.sync(dout.ctx);
    if (dout.close) dout.close(dout.ctx);
    if (in.close) in.close(in.ctx);
    if (!e) { *size_out = total; *crc_out = crc; }
    return e;
}

/* Remove every NUL-separated name in `plan`, best-effort: a name that
 * fails to unlink is swept at the next open, and nothing adopted can be
 * lost by failing to delete what superseded it. */
static void remove_all(bj_ns *ns, const uint8_t *plan, size_t len) {
    for (size_t at = 0; at < len; ) {
        size_t end = at;
        while (end < len && plan[end] != '\0') end++;
        if (end > at) ns->remove(ns->ctx, (const char *)plan + at, (uint32_t)(end - at));
        at = end + 1;
    }
}

/*
 * Compact the log through `boundary` into the store's paired log for
 * `gen`: a fresh log based at the boundary, carrying the suffix and the
 * hard state forward, made durable, then handed to the node -- which
 * takes ownership, exactly as it does for an install's rebased log.
 */
static int compact_log_into(replica *r, uint64_t gen, uint64_t boundary,
                            uint64_t bterm) {
    dbuf name = {0};
    int e = sst_log_name(r->store, gen, &name);
    if (e) { dbuf_free(&name); return e; }
    bj_io io;
    e = r->ns->open(r->ns->ctx, (const char *)name.data, (uint32_t)name.len,
                    BJ_NS_CREATE | BJ_NS_TRUNC, &io);
    dbuf_free(&name);
    if (e) return e;
    elog *fresh = elog_create_at(&io, boundary, bterm);
    if (!fresh) { if (io.close) io.close(io.ctx); return BJ_ERR_OOM; }

    uint64_t hard_term = elog_current_term(r->log);
    if (hard_term > 0) e = elog_set_hard_state(fresh, hard_term, elog_voted_for(r->log));

    /* The suffix: everything the boundary does not cover. Entries commit
     * and apply in index order, so what a pending write is waiting on is
     * either at or below the boundary (settled, by definition of
     * applied) or carried forward here, indexes unchanged. */
    uint64_t last = elog_last_index(r->log);
    for (uint64_t i = boundary + 1; !e && i <= last; i++) {
        uint64_t term = 0;
        int type = 0;
        const uint8_t *p = NULL;
        size_t plen = 0;
        e = elog_get(r->log, i, &term, &type, &p, &plen);
        if (!e) {
            uint64_t at = 0;
            e = elog_append(fresh, term, type, p, (uint32_t)plen, &at);
        }
    }
    if (!e) {
        uint64_t commit = elog_commit_index(r->log);
        if (commit > last) commit = last;
        if (commit > boundary) e = elog_set_commit_index(fresh, commit);
    }
    if (!e) e = elog_sync(fresh);
    if (e) {
        elog_free(fresh);
        if (io.close) io.close(io.ctx);
        return e;
    }

    elog *old = NULL;
    rn_swap_log(r->node, fresh, &io, &old);
    if (old) {
        /* The node handed back the log we lent it; the fresh one is its
         * own now, io and all. */
        elog_free(old);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        memset(&r->log_io, 0, sizeof r->log_io);
    }
    r->log = fresh;
    r->host_owns_log = 0;
    return BJ_OK;
}

/*
 * One local snapshot: every database's files stream into a generation,
 * the manifest commits at the applied boundary, the log is compacted
 * through it, and everything superseded is swept. A failure part-way
 * leaves partial generation files with no manifest -- a generation that
 * never existed, swept at the next open -- and the log untouched.
 *
 * Runs synchronously between turns of the transport's loop, so nothing
 * applies or is served mid-copy; the files it reads are exactly the
 * state at `applied`. The pause is the cost, and it is the same bargain
 * WalDb.snapshot() struck under its write chain.
 */
static int snapshot_take(replica *r) {
    uint64_t boundary = r->applied;
    uint64_t bterm = 0;
    int e = elog_term_at(r->log, boundary, &bterm);
    if (e) return e;
    uint64_t gen = sst_next_gen(r->store);

    dbuf dbs = {0};
    e = dbi_list(r->inst, &dbs);
    if (e) { dbuf_free(&dbs); return e; }

    bj_builder *files = bj_builder_new();
    bj_builder *live = bj_builder_new();
    if (!files || !live) {
        bj_builder_free(files); bj_builder_free(live);
        dbuf_free(&dbs);
        return BJ_ERR_OOM;
    }
    e = bj_begin_array(files);
    if (!e) e = bj_begin_array(live);

    dbuf names = {0}, gen_name = {0}, live_name = {0};
    uint32_t nrole = 0;
    cur c = { dbs.data, dbs.len, 0 };
    uint32_t ndb = 0;
    if (!e) e = array_begin(&c, &ndb);
    for (uint32_t i = 0; !e && i < ndb; i++) {
        const uint8_t *dn; uint32_t dlen;
        e = take_string(&c, &dn, &dlen);
        if (e) break;
        names.len = 0;
        e = root_list_files(r->rt, (const char *)dn, dlen, &names);
        for (size_t at = 0; !e && at < names.len; ) {
            size_t end = at;
            while (end < names.len && names.data[end] != '\0') end++;
            if (end == at) { at = end + 1; continue; }
            /* The node's transfer works over a fixed array of files
             * (RN_MAX_SNAP_FILES); a generation it could never serve is
             * refused here, whole, rather than committed and stuck. */
            if (nrole >= RN_MAX_SNAP_FILES) { e = RAFT_ERR_CAPACITY; break; }
            char role[16];
            int rlen = snprintf(role, sizeof role, "f%u", nrole);

            gen_name.len = 0;
            e = sst_data_name(r->store, gen, role, (uint32_t)rlen, &gen_name);
            if (e) break;
            live_name.len = 0;
            e = dbuf_put(&live_name, dn, dlen);
            if (!e) e = dbuf_put(&live_name, (const uint8_t *)"/", 1);
            if (!e) e = dbuf_put(&live_name, names.data + at, end - at);
            if (e) break;

            uint64_t size = 0;
            uint32_t crc = 0;
            e = copy_file(r->ns, (const char *)live_name.data, (uint32_t)live_name.len,
                          (const char *)gen_name.data, (uint32_t)gen_name.len,
                          &size, &crc);
            if (e) break;

            if (!e) e = bj_begin_object(files);
            if (!e) e = bj_put_key(files, (const uint8_t *)"role", 4);
            if (!e) e = bj_put_string(files, (const uint8_t *)role, (uint32_t)rlen);
            if (!e) e = bj_put_key(files, (const uint8_t *)"name", 4);
            if (!e) e = bj_put_string(files, gen_name.data, (uint32_t)gen_name.len);
            if (!e) e = bj_put_key(files, (const uint8_t *)"size", 4);
            if (!e) e = bj_put_int(files, (int64_t)size);
            if (!e) e = bj_put_key(files, (const uint8_t *)"crc", 3);
            if (!e) e = bj_put_int(files, (int64_t)crc);
            if (!e) e = bj_end_object(files);

            if (!e) e = bj_begin_object(live);
            if (!e) e = bj_put_key(live, (const uint8_t *)"role", 4);
            if (!e) e = bj_put_string(live, (const uint8_t *)role, (uint32_t)rlen);
            if (!e) e = bj_put_key(live, (const uint8_t *)"name", 4);
            if (!e) e = bj_put_string(live, live_name.data, (uint32_t)live_name.len);
            if (!e) e = bj_end_object(live);

            nrole++;
            at = end + 1;
        }
    }
    dbuf_free(&names);
    dbuf_free(&gen_name);
    dbuf_free(&live_name);
    dbuf_free(&dbs);
    if (!e) e = bj_end_array(files);
    if (!e) e = bj_end_array(live);

    /* config: { live: [...] } -- the map from roles back to the names
     * the database opens, which is what an adoption (here or on a peer
     * that was sent this generation) restores by. */
    dbuf manifest = {0};
    if (!e) {
        size_t flen = 0, llen = 0;
        const uint8_t *fd = bj_builder_data(files, &flen);
        const uint8_t *ld = bj_builder_data(live, &llen);
        bj_builder *cfg = bj_builder_new();
        if (!fd || !ld || !cfg) e = BJ_ERR_OOM;
        if (!e) e = bj_begin_object(cfg);
        if (!e) e = bj_put_key(cfg, (const uint8_t *)"live", 4);
        if (!e) e = bj_put_raw(cfg, ld, (uint32_t)llen);
        if (!e) e = bj_end_object(cfg);
        if (!e) {
            size_t clen = 0;
            const uint8_t *cd = bj_builder_data(cfg, &clen);
            if (!cd) e = BJ_ERR_STATE;
            else e = sst_manifest_encode(boundary, bterm, cd, (uint32_t)clen,
                                         fd, (uint32_t)flen, &manifest);
        }
        bj_builder_free(cfg);
    }
    bj_builder_free(files);
    bj_builder_free(live);
    if (e) { dbuf_free(&manifest); return e; }

    /* Writing the manifest IS the commit. */
    dbuf mname = {0};
    e = sst_manifest_name(r->store, gen, &mname);
    if (!e) {
        bj_io io;
        e = r->ns->open(r->ns->ctx, (const char *)mname.data, (uint32_t)mname.len,
                        BJ_NS_CREATE | BJ_NS_TRUNC, &io);
        if (!e) {
            e = io.write(io.ctx, 0, manifest.data, (uint32_t)manifest.len);
            if (!e && io.sync) e = io.sync(io.ctx);
            if (io.close) io.close(io.ctx);
        }
    }
    dbuf_free(&mname);

    dbuf sweep = {0};
    if (!e) e = sst_adopt_committed(r->store, gen, manifest.data,
                                    (uint32_t)manifest.len, &sweep);
    dbuf_free(&manifest);

    /* The log, through the boundary. After this line the old entries are
     * the store's business, and a resume from below the boundary is
     * -68 -- which until this file existed was a rule nothing could
     * reach. */
    if (!e) e = compact_log_into(r, gen, boundary, bterm);

    if (!e) {
        /* Superseded: the previous generation's files, every older store
         * log, and the legacy log the store's now replaces. */
        remove_all(r->ns, sweep.data, sweep.len);
        r->ns->remove(r->ns->ctx, REPLICA_WAL, (uint32_t)strlen(REPLICA_WAL));
        dbuf listing = {0}, keep = {0}, prune = {0};
        if (root_list_files(r->rt, NULL, 0, &listing) == BJ_OK &&
            sst_log_name(r->store, gen, &keep) == BJ_OK &&
            sst_prune_logs_plan(r->store, listing.data, (uint32_t)listing.len,
                                (const char *)keep.data, (uint32_t)keep.len,
                                &prune) == BJ_OK) {
            remove_all(r->ns, prune.data, prune.len);
        }
        dbuf_free(&listing); dbuf_free(&keep); dbuf_free(&prune);
        fprintf(stderr, "nisaba: snapshot %llu at index %llu (%u files); log compacted\n",
                (unsigned long long)gen, (unsigned long long)boundary, nrole);
        fflush(stderr);
    }
    dbuf_free(&sweep);
    return e;
}

/* Scan the root for the store's generations and adopt the newest valid
 * one -- the plan/open/execute trampoline snapstore.h describes, walked
 * here with synchronous opens. A store with nothing to adopt is the
 * normal state of a member that has never compacted. */
static int store_scan_adopt(replica *r) {
    dbuf listing = {0};
    int e = root_list_files(r->rt, NULL, 0, &listing);
    if (!e) e = sst_scan(r->store, listing.data, (uint32_t)listing.len);
    if (e) { dbuf_free(&listing); return e; }

    uint32_t nc = sst_candidate_count(r->store);
    for (uint32_t i = 0; i < nc; i++) {
        uint32_t mlen = 0;
        const char *mname = sst_candidate_manifest(r->store, i, &mlen);
        if (!mname) continue;
        bj_io io;
        if (r->ns->open(r->ns->ctx, mname, mlen, 0, &io)) continue;
        uint64_t sz = io.size(io.ctx);
        uint8_t *bytes = (sz > 0 && sz < (16u << 20)) ? (uint8_t *)malloc((size_t)sz) : NULL;
        int ok = bytes && io.read(io.ctx, 0, bytes, (uint32_t)sz) == (int64_t)sz;
        if (io.close) io.close(io.ctx);
        int te = ok ? sst_try_manifest(r->store, i, bytes, (uint32_t)sz) : SST_ERR_MANIFEST;
        free(bytes);
        if (te != BJ_OK) continue;

        uint32_t np = sst_pending_count(r->store);
        if (np > RN_MAX_SNAP_FILES) continue;
        double sizes[RN_MAX_SNAP_FILES];
        for (uint32_t k = 0; k < np; k++) {
            uint32_t nlen = 0;
            const char *pn = sst_pending_name(r->store, k, &nlen);
            bj_io fio;
            sizes[k] = -1;
            if (pn && r->ns->open(r->ns->ctx, pn, nlen, 0, &fio) == BJ_OK) {
                sizes[k] = (double)fio.size(fio.ctx);
                if (fio.close) fio.close(fio.ctx);
            }
        }
        if (sst_confirm(r->store, sizes, np) == BJ_OK) break;
    }
    dbuf_free(&listing);
    return BJ_OK;
}

/* The log-naming rule: the store's newest log that OPENS -- a crash
 * mid-compaction leaves a torn newest file, and its predecessor is only
 * ever deleted once a successor is durable -- and the legacy __wal__.bj
 * when the store offers none. */
static int open_best_log(replica *r) {
    dbuf listing = {0}, cands = {0};
    int e = root_list_files(r->rt, NULL, 0, &listing);
    if (!e) e = sst_log_candidates(r->store, listing.data, (uint32_t)listing.len, &cands);
    dbuf_free(&listing);
    if (e) { dbuf_free(&cands); return e; }

    for (size_t at = 0; at < cands.len && !r->log; ) {
        size_t end = at;
        while (end < cands.len && cands.data[end] != '\0') end++;
        if (end > at) {
            bj_io io;
            if (r->ns->open(r->ns->ctx, (const char *)cands.data + at,
                            (uint32_t)(end - at), 0, &io) == BJ_OK) {
                elog *lg = io.size(io.ctx) > 0 ? elog_open(&io) : NULL;
                if (lg) {
                    r->log = lg;
                    r->log_io = io;
                    r->host_owns_log = 1;
                } else if (io.close) {
                    io.close(io.ctx);
                }
            }
        }
        at = end + 1;
    }
    dbuf_free(&cands);
    if (r->log) return BJ_OK;

    /* An empty file is a new log; anything else is one to recover. Both
     * are entrylog.h's call, not this file's guess about its format. */
    e = r->ns->open(r->ns->ctx, REPLICA_WAL, (uint32_t)strlen(REPLICA_WAL),
                    BJ_NS_CREATE, &r->log_io);
    if (e) return e;
    r->log = (r->log_io.size(r->log_io.ctx) > 0) ? elog_open(&r->log_io)
                                                 : elog_create(&r->log_io);
    if (!r->log) {
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        return BJ_ERR_STATE;
    }
    r->host_owns_log = 1;
    return BJ_OK;
}

/* Copy each of the adopted generation's files onto the live name its
 * config.live maps it to. The node has its own copy of this walk
 * (raft_node.c's live_each); this one exists because a restore at
 * STARTUP runs before there is a node to ask. */
static int restore_live_files(replica *r, const dbuf *latest) {
    uint64_t gen = sst_latest_gen(r->store);
    const uint8_t *config; size_t clen; int f = 0;
    int e = obj_get_field(latest->data, latest->len, (const uint8_t *)"config", 6,
                          &config, &clen, &f);
    if (e || !f) return SST_ERR_MANIFEST;
    const uint8_t *live; size_t llen;
    e = obj_get_field(config, clen, (const uint8_t *)"live", 4, &live, &llen, &f);
    if (e || !f) return SST_ERR_MANIFEST;
    cur c = { live, llen, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count)) return SST_ERR_MANIFEST;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if (skip_value(&c)) return SST_ERR_MANIFEST;
        const uint8_t *nv; size_t nlen; int nf = 0;
        if (obj_get_field(c.d + start, c.pos - start, (const uint8_t *)"name", 4,
                          &nv, &nlen, &nf) || !nf) return SST_ERR_MANIFEST;
        cur nc = { nv, nlen, 0 };
        const uint8_t *s; uint32_t slen;
        if (take_string(&nc, &s, &slen)) return SST_ERR_MANIFEST;
        /* The generation file for this entry is named by ROLE; read it
         * back the same way. */
        const uint8_t *rv; size_t rlen2; int rf = 0;
        if (obj_get_field(c.d + start, c.pos - start, (const uint8_t *)"role", 4,
                          &rv, &rlen2, &rf) || !rf) return SST_ERR_MANIFEST;
        cur rc = { rv, rlen2, 0 };
        const uint8_t *rs; uint32_t rslen;
        if (take_string(&rc, &rs, &rslen)) return SST_ERR_MANIFEST;
        dbuf gname = {0};
        e = sst_data_name(r->store, gen, (const char *)rs, rslen, &gname);
        if (!e) {
            uint64_t sz; uint32_t crc;
            e = copy_file(r->ns, (const char *)gname.data, (uint32_t)gname.len,
                          (const char *)s, slen, &sz, &crc);
        }
        dbuf_free(&gname);
        if (e) return e;
    }
    return BJ_OK;
}

/*
 * A committed generation whose boundary is past the log's base is an
 * adoption that never finished: an install (or a local compaction)
 * crashed between its manifest committing and its log moving. Restoring
 * is safe in both cases -- the generation is the state at its boundary,
 * and any live progress past it is replayed from the log's suffix by
 * the ordinary recovery the apply pump runs (replay from the applied
 * floor is exact; db_wal.h's contract). A log whose entries end BELOW
 * the boundary describes a state the snapshot has superseded whole, so
 * it is replaced by a fresh one based there, exactly as an adoption
 * would have.
 */
static int restore_if_stale(replica *r) {
    if (!sst_has_latest(r->store)) return BJ_OK;
    dbuf latest = {0};
    int has = 0;
    int e = sst_latest(r->store, &latest, &has);
    if (e || !has) { dbuf_free(&latest); return e; }

    uint64_t boundary = 0, bterm = 0;
    {
        const uint8_t *v; size_t vlen; int f = 0;
        if (!obj_get_field(latest.data, latest.len,
                           (const uint8_t *)"lastIncludedIndex", 17, &v, &vlen, &f) && f) {
            cur vc = { v, vlen, 0 };
            (void)read_u64(&vc, &boundary);
        }
        if (!obj_get_field(latest.data, latest.len,
                           (const uint8_t *)"lastIncludedTerm", 16, &v, &vlen, &f) && f) {
            cur vc = { v, vlen, 0 };
            (void)read_u64(&vc, &bterm);
        }
    }
    if (!boundary || boundary <= elog_base_index(r->log)) {
        dbuf_free(&latest);
        return BJ_OK;
    }

    fprintf(stderr, "nisaba: restoring snapshot at index %llu (an adoption did not"
                    " finish before the last crash)\n",
            (unsigned long long)boundary);
    fflush(stderr);

    /* Stale files that the generation does not restore -- a journal left
     * behind would rewind a restored file on the next recovery. Every
     * current database file goes; the copies below bring back the ones
     * the snapshot holds. */
    dbuf dbs = {0};
    e = dbi_list(r->inst, &dbs);
    if (!e) {
        cur c = { dbs.data, dbs.len, 0 };
        uint32_t ndb = 0;
        if (!array_begin(&c, &ndb)) {
            dbuf names = {0}, victim = {0};
            for (uint32_t i = 0; !e && i < ndb; i++) {
                const uint8_t *dn; uint32_t dlen;
                if (take_string(&c, &dn, &dlen)) break;
                names.len = 0;
                if (root_list_files(r->rt, (const char *)dn, dlen, &names)) continue;
                for (size_t at = 0; at < names.len; ) {
                    size_t end = at;
                    while (end < names.len && names.data[end] != '\0') end++;
                    if (end > at) {
                        victim.len = 0;
                        if (!dbuf_put(&victim, dn, dlen) &&
                            !dbuf_put(&victim, (const uint8_t *)"/", 1) &&
                            !dbuf_put(&victim, names.data + at, end - at)) {
                            r->ns->remove(r->ns->ctx, (const char *)victim.data,
                                          (uint32_t)victim.len);
                        }
                    }
                    at = end + 1;
                }
            }
            dbuf_free(&names);
            dbuf_free(&victim);
        }
    }
    dbuf_free(&dbs);

    if (!e) e = restore_live_files(r, &latest);
    dbuf_free(&latest);
    if (e) return e;

    if (elog_last_index(r->log) < boundary) {
        /* The log ends below the boundary: nothing in it survives the
         * snapshot. A fresh one, based there, carrying the hard state. */
        uint64_t gen = sst_latest_gen(r->store);
        dbuf name = {0};
        e = sst_log_name(r->store, gen, &name);
        bj_io io;
        if (!e) e = r->ns->open(r->ns->ctx, (const char *)name.data,
                                (uint32_t)name.len, BJ_NS_CREATE | BJ_NS_TRUNC, &io);
        dbuf_free(&name);
        if (e) return e;
        elog *fresh = elog_create_at(&io, boundary, bterm);
        if (!fresh) { if (io.close) io.close(io.ctx); return BJ_ERR_OOM; }
        /* The old log's hard state can sit BELOW the boundary's term --
         * this member crashed before it ever appended an entry of the
         * term that produced the snapshot -- and a log based at term T
         * refuses a hard state under T. The manifest is newer knowledge:
         * raise the term to it, and drop the vote, which belonged to the
         * old term (raising can never double a vote; keeping the old
         * term could refuse the log its own base). */
        uint64_t hard_term = elog_current_term(r->log);
        uint64_t voted = elog_voted_for(r->log);
        if (hard_term < bterm) { hard_term = bterm; voted = 0; }
        if (hard_term > 0) e = elog_set_hard_state(fresh, hard_term, voted);
        if (!e) e = elog_sync(fresh);
        if (e) {
            elog_free(fresh);
            if (io.close) io.close(io.ctx);
            return e;
        }
        elog_free(r->log);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        r->log = fresh;
        r->log_io = io;
        r->host_owns_log = 1;
    }
    return BJ_OK;
}

/* Crashed attempts and superseded generations, swept at open the way
 * the store's contract promises. Best-effort throughout. */
static void startup_sweep(replica *r) {
    dbuf plan = {0};
    if (sst_sweep_plan(r->store, &plan) == BJ_OK)
        remove_all(r->ns, plan.data, plan.len);
    dbuf_free(&plan);
    if (!sst_has_latest(r->store)) return;
    /* A store log is in use only when a generation was adopted; every
     * other store log -- and the legacy file, if the chosen log is the
     * store's -- is history. The prune keeps whichever log is open. */
    dbuf listing = {0}, keep = {0}, prune = {0};
    if (root_list_files(r->rt, NULL, 0, &listing) == BJ_OK &&
        sst_log_name(r->store, sst_latest_gen(r->store), &keep) == BJ_OK &&
        sst_prune_logs_plan(r->store, listing.data, (uint32_t)listing.len,
                            (const char *)keep.data, (uint32_t)keep.len,
                            &prune) == BJ_OK) {
        remove_all(r->ns, prune.data, prune.len);
    }
    dbuf_free(&listing); dbuf_free(&keep); dbuf_free(&prune);
}

/* ---- the clock ---------------------------------------------------------- */

/*
 * A tick every heartbeat is enough and is what the JavaScript host does
 * (RaftGroupHost's tickMs). Asking the node for its exact next deadline
 * would be a second copy of the timer arithmetic; a slice is the
 * transport's own business and cannot be wrong, only coarse.
 *
 * DERIVED FROM THE HEARTBEAT, not fixed: coarse is only harmless while
 * the slice is well under the interval it is meant to deliver. A 20ms
 * slice cannot deliver a 10ms heartbeat at all -- it would round every
 * beat up and quietly halve the leader's contact rate -- and against a
 * 500ms heartbeat it is 25 wakeups spent finding nothing to do. Half the
 * heartbeat is the most that can never round a beat away; 20ms is the
 * floor, because below that the wakeups cost more than the precision is
 * worth.
 */
#define REPLICA_TICK_MS 20

static int tick_for(int64_t heartbeat_ms) {
    int64_t half = heartbeat_ms / 2;
    if (half < REPLICA_TICK_MS) return REPLICA_TICK_MS;
    return half > INT32_MAX ? INT32_MAX : (int)half;
}

void replica_set_read_offload(replica *r, int threads, int64_t min_docs) {
    if (!r) return;
    if (threads >= 0) r->read_threads = threads;
    if (min_docs >= 0) r->read_min_docs = min_docs;
}

void replica_set_max_batch(replica *r, uint32_t bytes) {
    /* rn_set_limits keeps its default on 0, so this needs no policy of
     * its own -- one place decides what 0 means. */
    if (r && r->node) rn_set_limits(r->node, bytes);
}

void replica_timing_resolve(replica_timing *tm) {
    if (!tm) return;
    int given_min = tm->min_election_ms > 0;
    if (!given_min) tm->min_election_ms = RN_DEFAULT_MIN_ELECTION;
    if (tm->max_election_ms <= 0) {
        /* Twice the minimum when a minimum was given and no maximum: the
         * default pair's own ratio, so widening the floor widens the
         * spread with it instead of collapsing it to nothing. */
        tm->max_election_ms = given_min ? tm->min_election_ms * 2
                                        : RN_DEFAULT_MAX_ELECTION;
    }
    if (tm->heartbeat_ms <= 0) tm->heartbeat_ms = RN_DEFAULT_HEARTBEAT;
}

int replica_open(bj_ns *ns, dbi *inst, uint64_t self_id, peers *px,
                 const uint8_t *members, uint32_t members_len,
                 uint64_t now, root_state *rt, uint64_t snapshot_entries,
                 const replica_timing *tm, replica **out) {
    if (!ns || !inst || !out || !self_id) return BJ_ERR_STATE;
    if (px && peers_count(px) > rn_max_peers()) return RAFT_ERR_CAPACITY;
    *out = NULL;
    replica *r = (replica *)calloc(1, sizeof *r);
    if (!r) return BJ_ERR_OOM;
    r->ns = ns;
    r->inst = inst;
    r->px = px;
    r->rt = rt;
    r->snap_every = snapshot_entries;
    r->self_id = self_id;
    /* No reader threads unless asked for, and a floor that puts the
     * smallest moved read at roughly a quarter of a millisecond of
     * scanning -- measured at ~0.22us per document, so a thousand of them
     * is about the point where a queue hop stops being the bigger cost. */
    r->read_threads = 0;
    r->read_min_docs = REPLICA_READ_MIN_DOCS;
    /* Non-zero, or xorshift stays at zero forever and every member draws
     * the same nothing -- which is the bug this exists to avoid. */
    r->rnd = (self_id * 0x9E3779B97F4A7C15ULL) ^ (now + 0x100000001B3ULL);
    if (!r->rnd) r->rnd = 0x9E3779B97F4A7C15ULL;

    r->store = sst_new(REPLICA_SNAP_PREFIX, (uint32_t)strlen(REPLICA_SNAP_PREFIX));
    if (!r->store) { free(r); return BJ_ERR_OOM; }

    /* The store first, then the log, then the reconciliation between
     * them -- the order IS the naming rule replica.h states. All of it
     * before the node exists, and before any database opens: a restore
     * replaces the very files a database handle would be positioned in. */
    int e = store_scan_adopt(r);
    if (!e) e = open_best_log(r);
    if (!e) e = restore_if_stale(r);
    if (e) {
        if (r->log) elog_free(r->log);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        sst_free(r->store);
        free(r);
        return e;
    }
    startup_sweep(r);

    r->node = rn_new(self_id, r->log);
    if (!r->node) {
        elog_free(r->log);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        sst_free(r->store);
        free(r);
        return BJ_ERR_OOM;
    }
    rn_set_ns(r->node, ns);
    rn_set_snapstore(r->node, r->store);

    /*
     * The clock, before the timers are armed. Each field falls back on
     * its own, so a caller that only widens the election timeout keeps
     * the default heartbeat -- and the tick follows the heartbeat
     * whichever way it came.
     */
    {
        replica_timing clock = tm ? *tm : (replica_timing){0};
        replica_timing_resolve(&clock);
        rn_set_timing(r->node, clock.min_election_ms, clock.max_election_ms,
                      clock.heartbeat_ms);
        r->tick_ms = tick_for(clock.heartbeat_ms);
    }

    /*
     * The group to BOOTSTRAP with: the set a join came back with when
     * there is one, and otherwise the process's own argv. Without either
     * that is one member -- it elects itself, commits by counting only
     * itself, and every message it would send has nobody to send it to.
     *
     * A --peer list has to be the same on every member, because a member
     * missing from one node's list is a vote that node will never count.
     * That is the cost of bootstrapping by argv, and the reason it stops
     * mattering the moment the log has a CONFIG entry of its own.
     */
    if (members && members_len) {
        e = rn_set_members(r->node, members, members_len);
    } else {
        bj_builder *b = bj_builder_new();
        if (!b) { replica_close(r); return BJ_ERR_OOM; }
        e = bj_begin_array(b);
        if (!e) e = put_member(b, self_id, peers_self_host(px), peers_self_port(px));
        for (uint32_t i = 0; !e && px && i < peers_count(px); i++)
            e = put_member(b, peers_id_at(px, i), peers_host_at(px, i), peers_port_at(px, i));
        if (!e) e = bj_end_array(b);
        if (!e) e = bj_builder_error(b);
        if (!e) {
            size_t len = 0;
            const uint8_t *d = bj_builder_data(b, &len);
            e = d ? rn_set_members(r->node, d, (uint32_t)len) : BJ_ERR_STATE;
        }
        bj_builder_free(b);
    }
    if (e) { replica_close(r); return e; }

    /*
     * ...and then the log, which outranks it. Before the timers are
     * armed, because what this finds decides whether this node is a
     * voter at all -- and a member that campaigned on a bootstrap set of
     * one before reading its own log would elect itself into a cluster
     * that already has a leader.
     */
    e = adopt_from_log(r);
    if (!e) e = sync_peers(r);      /* the bootstrap set's addresses, if the log had none */
    if (e) { replica_close(r); return e; }

    /*
     * Where the database already is. Both numbers are the host's to
     * know: C starts at zero and would replay a prefix that is already
     * applied.
     *
     * The FLOOR is the database's, not the log's -- the log's commit
     * marker is advisory and rides the next sync, so it can sit behind
     * what was applied, and resuming from it would perform committed
     * commands a second time. An insert would come back a duplicate,
     * which is survivable; an $inc would silently count twice, which is
     * not. Anything committed above the floor is replayed by the pump on
     * the first tick, which is what makes a crash between commit and
     * apply a non-event.
     */
    r->applied = dbi_applied_floor(inst);
    /*
     * The floor is a MAX over databases, and a replicated dropDatabase
     * can remove the database that held it -- after which the surviving
     * ones report something older. Below the log's base that is not a
     * replay plan, it is a trap: the entries between are compacted away,
     * and the snapshot the base came from IS the state at the base
     * (the drop applied before the boundary was taken, or the install
     * adopted it whole). Above the base the regression is legal and the
     * replay is the answer: re-applying recreates the dropped database
     * entry by entry and the drop entry removes it again.
     */
    if (r->applied < elog_base_index(r->log))
        r->applied = elog_base_index(r->log);
    rn_seed_commit(r->node, r->applied > elog_commit_index(r->log)
                                ? r->applied : elog_commit_index(r->log));

    /*
     * Running, on THIS clock. Starting the node at zero and then ticking
     * it with a monotonic reading would make its first tick look like a
     * jump of however long the machine has been up -- which on a native
     * host elects it instantly, by accident, and hides the fact that the
     * election timer was never really running. (It did. wasip2, whose
     * guest clock starts near zero, is where it showed.)
     */
    rn_start(r->node, (int64_t)now, rnd01(r));

    /*
     * A group of one stands for election NOW rather than counting down
     * to it. There is nobody to disturb -- leader stickiness and the
     * pre-vote round exist to protect a live leader from a challenger,
     * and there is neither -- and the alternative is a server that
     * refuses writes for a random fraction of a second after it starts,
     * which is a startup race every client would have to know about.
     *
     * Only for one voter. A restarting MEMBER of a real cluster must
     * count down like everyone else: campaigning on startup is how a
     * rolling restart becomes an election storm.
     */
    if (rn_quorum(r->node) == 1) {
        e = rn_campaign(r->node, rnd01(r));
        if (!e) e = rn_tick(r->node, (int64_t)now, rnd01(r));
        rn_out_clear(r->node);
        rn_effects_clear(r->node);
        if (e) { replica_close(r); return e; }
    }

    /* From here every database this instance opens can replay the log,
     * which is what makes a watch on any of them resumable. */
    {
        dbs_log reader = { r, log_base, log_floor, log_entry };
        e = dbi_set_log(inst, &reader);
        if (e) { replica_close(r); return e; }
    }

    *out = r;
    return BJ_OK;
}

void replica_close(replica *r) {
    if (!r) return;
    /* The instance outlives this call in some teardowns, and a reader
     * whose ctx has been freed must not be reachable from it. */
    if (r->inst) dbi_set_log(r->inst, NULL);
    for (int i = 0; i < REPLICA_MAX_PENDING; i++)
        if (r->waiting[i].used) pending_release(&r->waiting[i]);
    /* The node first: a log it rebased for itself (an install, a local
     * compaction) is its own and rn_free closes it. The one WE opened is
     * ours -- elog_free does not close the handle behind the io; the one
     * that opened it closes it, which is this. */
    if (r->node) rn_free(r->node);
    if (r->host_owns_log) {
        if (r->log) elog_free(r->log);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
    }
    sst_free(r->store);
    free(r);
}

int      replica_is_leader(const replica *r) { return r && rn_role(r->node) == RAFT_LEADER; }
uint64_t replica_leader_id(const replica *r) { return r ? rn_leader_id(r->node) : 0; }

int replica_adopt_pending(const replica *r) {
    return r && r->node && rn_adopt_pending(r->node);
}

int replica_adopt(replica *r, const char *victims, size_t victims_len) {
    if (!r) return BJ_ERR_STATE;
    uint64_t boundary = rn_adopt_boundary(r->node);
    elog *old = NULL;
    int e = rn_adopt(r->node, victims, victims_len, &old);
    if (e) return e;
    if (old) {
        /* The log we lent came back; the node owns its rebased one. */
        elog_free(old);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        memset(&r->log_io, 0, sizeof r->log_io);
    }
    r->host_owns_log = 0;
    r->log = rn_log(r->node);
    r->applied = boundary;

    /* Whatever was waiting is waiting on an instance that no longer
     * exists. By the time a member is adopting an install it has been a
     * follower for a while -- reads and writes are refused, so this is
     * empty in every ordinary run -- but "ordinarily empty" is not a
     * contract, and a dangling token would be. */
    for (int i = 0; i < REPLICA_MAX_PENDING; i++)
        if (r->waiting[i].used) pending_release(&r->waiting[i]);

    /* The housekeeping local compaction does after its own swap: the
     * legacy log is superseded by the store's, older store logs by the
     * adopted generation's, and older generations by this one. */
    r->ns->remove(r->ns->ctx, REPLICA_WAL, (uint32_t)strlen(REPLICA_WAL));
    dbuf listing = {0}, keep = {0}, prune = {0}, sweep = {0};
    if (root_list_files(r->rt, NULL, 0, &listing) == BJ_OK &&
        sst_log_name(r->store, sst_latest_gen(r->store), &keep) == BJ_OK &&
        sst_prune_logs_plan(r->store, listing.data, (uint32_t)listing.len,
                            (const char *)keep.data, (uint32_t)keep.len,
                            &prune) == BJ_OK) {
        remove_all(r->ns, prune.data, prune.len);
    }
    if (sst_sweep_plan(r->store, &sweep) == BJ_OK)
        remove_all(r->ns, sweep.data, sweep.len);
    dbuf_free(&listing); dbuf_free(&keep); dbuf_free(&prune); dbuf_free(&sweep);

    fprintf(stderr, "nisaba: snapshot install adopted at index %llu\n",
            (unsigned long long)boundary);
    fflush(stderr);
    return BJ_OK;
}

void replica_set_instance(replica *r, dbi *inst) {
    if (!r) return;
    r->inst = inst;
    if (inst) {
        dbs_log reader = { r, log_base, log_floor, log_entry };
        dbi_set_log(inst, &reader);
    }
}

uint32_t replica_peer_count(const replica *r) {
    if (!r) return 0;
    uint32_t mlen = 0;
    const uint8_t *ms = members_of(r, &mlen);
    if (!ms) return 0;
    cur c = { ms, mlen, 0 };
    uint32_t n = 0;
    if (array_begin(&c, &n) != BJ_OK) return 0;
    uint32_t others = 0;
    for (uint32_t i = 0; i < n; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) return others;
        if (record_id(c.d + start, (uint32_t)(c.pos - start)) != r->self_id) others++;
    }
    return others;
}

int replica_wait_ms(const replica *r, uint64_t now) {
    (void)now;
    if (!r) return REPLICA_TICK_MS;
    /*
     * WORK THAT IS READY NOW IS NOT WORK TO SLEEP ON.
     *
     * Committed entries this member has not applied yet are the answer
     * to somebody's write, sitting one apply away. The transport learns
     * about them by ticking -- so if the loop sleeps first, that write
     * waits a whole tick interval for nothing.
     *
     * On a cluster it never showed: the acks that MOVED the commit index
     * are themselves peer traffic, so poll() is already awake and the
     * next pass applies immediately. A GROUP OF ONE has no peers and no
     * traffic. It commits inside rn_propose -- there is nobody to hear
     * from -- and then the loop, with nothing to wake it, sleeps the
     * full tick before noticing. Every write on a single-member cluster
     * cost one tick: 27ms at the default heartbeat, measured, and it
     * tracked the tick exactly when the heartbeat was changed (a 400ms
     * heartbeat made writes 202ms). That is ~37 writes/s where the same
     * member unlogged does thousands.
     *
     * `dbi_stream_pending` right above this in server/main.c's wait
     * computation is the same rule for change events, for the same
     * reason: something is deliverable because state changed rather than
     * because a socket did.
     *
     * It cannot spin: apply_committed either advances `applied` or
     * halts the replica outright.
     */
    if (r->applied < rn_commit_index(r->node)) return 0;
    return r->tick_ms;
}

/* ---- the apply pump ----------------------------------------------------- */

/*
 * Everything committed and not yet applied, in log order.
 *
 * A deterministic failure is a RESULT -- every replica applying the same
 * prefix reaches the same verdict -- so it is reported to whoever
 * proposed it and the pump carries on. Anything else is this replica
 * having diverged from the ones that did not hit it, and the honest
 * thing is to stop.
 *
 * rn_applied is called for EVERY entry, including the ones with nothing
 * to apply (a leader's NOOP, a CONFIG): the node settles what that
 * unlocks, and an index skipped here is a client waiting forever.
 */
static int apply_committed(replica *r) {
    dbuf payload = {0};
    int e = BJ_OK;
    while (r->applied < rn_commit_index(r->node)) {
        uint64_t index = r->applied + 1;
        uint64_t term = 0;
        int type = 0;
        const uint8_t *p = NULL;
        size_t plen = 0;
        e = elog_get(r->log, index, &term, &type, &p, &plen);
        if (e) break;
        /* The log owns that pointer and it dies on the next operation on
         * this log -- and dbs_apply can be one. */
        payload.len = 0;
        e = dbuf_put(&payload, p, plen);
        if (e) break;

        /*
         * A membership change taking effect. The index guard skips one
         * OLDER than the set in force -- the startup scan adopts the
         * last CONFIG in the log, which may sit above the applied floor,
         * and replaying an earlier one would regress the cluster's shape.
         */
        if (type == EL_CONFIG && index >= r->config_index) {
            e = adopt_config(r, payload.data, (uint32_t)payload.len, index);
            if (e) break;
            /* Whoever just ARRIVED has to be caught up, and the entry
             * that put them here is already behind us: this runs against
             * the peer table sync_peers has now widened, where
             * adopt_config's own pass ran against the set as it stood.
             * A peer in both is asked twice and replicates once
             * (rn_replicate skips one with a request already in
             * flight). */
            if (replica_is_leader(r))
                for (uint32_t i = 0; i < peers_count(r->px); i++)
                    rn_replicate(r->node, peers_id_at(r->px, i));
        }

        if (type == EL_NORMAL) {
            /* Into whoever proposed it, if that was a client of this
             * process -- because the response is built from exactly what
             * applying produced, and this is the only place it happens.
             * A follower's pump writes into a scratch buffer instead;
             * nobody local is waiting on it. */
            dbuf  scratch = {0};
            dbuf *into = &scratch;
            pending *p = pending_for_index(r, index, &into);
            int rc = dbi_apply(r->inst, index, payload.data, (uint32_t)payload.len, into);
            if (p) p->view[p->at].rc = rc;
            dbuf_free(&scratch);
            if (rc && !dc_is_deterministic(rc)) { e = rc; break; }
        }
        r->applied = index;
        rn_applied(r->node, index);
    }
    dbuf_free(&payload);
    return e;
}

/* ---- proposing ---------------------------------------------------------- */

/* Append every command of `cmds` (a binjson ARRAY) to the log, recording
 * the index each took. All or nothing: a half-proposed batch is a
 * request that can never be completed. */
static int propose_batch(replica *r, pending *p, const dbuf *cmds) {
    cur c = { cmds->data, cmds->len, 0 };
    uint32_t count = 0;
    int e = array_begin(&c, &count);
    if (e) return e;
    /*
     * Refused WHOLE, before anything is appended. rn_propose refuses the
     * entry that hits the await cap -- not the entries before it, which
     * are already in the log and commit anyway. Discovering that mid-way
     * left 256 of a client's 300 documents landing behind a dropped
     * connection, which violates all-or-nothing in the worst direction:
     * silently. So the capacity question is asked once, up front, for
     * the batch as a whole, counting what other in-flight requests
     * already hold.
     */
    if (count > rn_max_await() - rn_awaiting(r->node))
        return DC_ERR_BATCH_TOO_LARGE;
    if (count > p->cap) {
        uint64_t *ix = (uint64_t *)realloc(p->indices, count * sizeof *ix);
        if (!ix) return BJ_ERR_OOM;
        p->indices = ix;
        dbuf *rs = (dbuf *)realloc(p->results, count * sizeof *rs);
        if (!rs) return BJ_ERR_OOM;
        memset(rs + p->cap, 0, (count - p->cap) * sizeof *rs);
        p->results = rs;
        dbs_result *v = (dbs_result *)realloc(p->view, count * sizeof *v);
        if (!v) return BJ_ERR_OOM;
        p->view = v;
        p->cap = count;
    }
    p->n = 0;
    for (uint32_t i = 0; i < count; i++) {
        size_t start = c.pos;
        if ((e = skip_value(&c))) return e;
        uint64_t at = 0;
        e = rn_propose(r->node, EL_NORMAL, c.d + start, (uint32_t)(c.pos - start), &at);
        if (e) return e;
        p->indices[p->n++] = at;
        p->last = at;
    }
    /* A plan with no commands would leave nothing to settle on, and the
     * request would wait for an index that will never arrive. */
    return p->n ? BJ_OK : BJ_ERR_STATE;
}

/* ---- the outbox ---------------------------------------------------------
 *
 * Everything the node wants said, said. Requests go to the peer they are
 * addressed to; a reply goes back on the CONVERSATION its request
 * arrived on, looked up by the correlation id this file minted for it.
 *
 * Looked up rather than remembered from the call, because a reply is not
 * necessarily the answer to the message being handled right now: a join
 * parked seconds ago is settled by the CONFIG entry applying, in a call
 * that is handling nothing at all.
 *
 * A peer that cannot be reached is failed HERE rather than left: the
 * node will not replace a request it still believes is in flight, so a
 * correlation id that produces no answer and no failure is that peer's
 * cursor wedged for good. The failures are collected first and delivered
 * after, because rn_on_fail mutates the node and the outbox this is
 * walking is only valid until it does.
 */
static int flush_out(replica *r) {
    uint32_t n = rn_out_count(r->node);
    if (!r->px) { rn_out_clear(r->node); return BJ_OK; }

    dbuf unreachable = {0};
    int e = BJ_OK;
    for (uint32_t i = 0; i < n && !e; i++) {
        uint32_t len = 0;
        const uint8_t *msg = rn_out_bytes(r->node, i, &len);
        uint64_t corr = rn_out_corr(r->node, i);
        if (!msg) continue;
        if (rn_out_is_reply(r->node, i)) {
            conversation *c = conv_find(r, corr);
            if (c) {
                e = peers_answer(r->px, c->from, c->theirs, msg, len);
                conv_close(c);
            } else if (!r->said_no_conversation) {
                /* Unreachable by construction -- every id the node can
                 * reply on was minted by conv_open, and a conversation is
                 * closed only by the reply that ends it -- and said out
                 * loud anyway, because the only thing worse than a peer
                 * waiting for an answer that was built is nobody knowing
                 * it was. */
                r->said_no_conversation = 1;
                fprintf(stderr, "replica: a reply with no conversation to send it on\n");
                fflush(stderr);
            }
            continue;
        }
        int rc = peers_request(r->px, rn_out_peer(r->node, i), corr, msg, len);
        if (rc < 0) { e = rc; break; }
        if (rc > 0) e = dbuf_put(&unreachable, (const uint8_t *)&corr, sizeof corr);
    }
    rn_out_clear(r->node);
    for (size_t i = 0; i + sizeof(uint64_t) <= unreachable.len; i += sizeof(uint64_t)) {
        uint64_t corr;
        memcpy(&corr, unreachable.data + i, sizeof corr);
        rn_on_fail(r->node, corr);
    }
    dbuf_free(&unreachable);
    return e;
}

/* Everything the peers have said. */
static int serve_peers(replica *r) {
    if (!r->px) return BJ_OK;
    for (;;) {
        peer_event ev;
        int have = 0;
        int e = peers_next(r->px, &ev, &have);
        if (e) return e;
        if (!have) return BJ_OK;

        if (ev.kind == PEER_EV_REQUEST) {
            conversation *c = conv_open(r, ev.corr, ev.from);
            if (!c) {
                /* Every slot is a question this node still owes an
                 * answer for. Refused explicitly, like every other
                 * bounded table here -- and retryable, which is what a
                 * seed loop makes of it (server/join.h). */
                peers_reject(r->px, ev.from, ev.corr, "too many peer requests at once");
                continue;
            }
            e = rn_handle(r->node, c->mine, ev.bytes, ev.len, rnd01(r));
            if (e) {
                /* A kind this node cannot answer -- an install with no
                 * store to put it in. Refused out loud rather than
                 * dropped, so the sender learns now instead of at its own
                 * timeout, with that peer idle until then. */
                peers_reject(r->px, ev.from, ev.corr, dc_strerror(e));
                conv_close(c);
            }
            e = flush_out(r);
            if (e) return e;
            continue;
        }

        if (ev.kind == PEER_EV_REPLY) rn_on_reply(r->node, ev.corr, ev.bytes, ev.len, rnd01(r));
        else                          rn_on_fail(r->node, ev.corr);
        /* RAFT_ERR_PEER from either is not an error the host acts on: it
         * means the round that id belonged to is over. */
        e = flush_out(r);
        if (e) return e;
    }
}

/*
 * The leader's member record, as a span into the node's own adopted set.
 * That set is where the addresses live (rn_adopted), so a refusal can
 * name where to go instead of merely naming who -- an id alone would
 * send a client back to whichever member it just asked. NULL while there
 * is no leader, which is an election in progress and a "try again".
 */
static const uint8_t *member_record(const replica *r, uint64_t want, uint32_t *len) {
    if (!want) return NULL;
    uint32_t alen = 0;
    const uint8_t *adopted = rn_adopted(r->node, &alen);
    if (!adopted) return NULL;

    const uint8_t *ms; size_t mslen; int found = 0;
    if (obj_get_field(adopted, alen, (const uint8_t *)"members", 7,
                      &ms, &mslen, &found) != BJ_OK || !found) return NULL;

    cur c = { ms, mslen, 0 };
    uint32_t n = 0;
    if (array_begin(&c, &n) != BJ_OK) return NULL;
    for (uint32_t i = 0; i < n; i++) {
        size_t start = c.pos;
        if (skip_value(&c) != BJ_OK) return NULL;
        const uint8_t *rec = c.d + start;
        uint32_t rlen = (uint32_t)(c.pos - start);
        const uint8_t *idv; size_t idlen; int has = 0;
        if (obj_get_field(rec, rlen, (const uint8_t *)"id", 2, &idv, &idlen, &has) != BJ_OK ||
            !has) continue;
        cur ic = { idv, idlen, 0 };
        uint64_t id = 0;
        if (read_u64(&ic, &id) != BJ_OK || id != want) continue;
        *len = rlen;
        return rec;
    }
    return NULL;
}

/* Build a refusal into `out`, naming `toward` as where to go instead --
 * usually the leader, but a refusal fenced by a transfer names the
 * TARGET, because that is where leadership is headed. */
static int refuse_toward(const replica *r, int code, uint64_t toward, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    const char *msg = dc_strerror(code);
    uint32_t rlen = 0;
    const uint8_t *rec = member_record(r, toward, &rlen);
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 0);
    if (!e) e = bj_put_key(b, (const uint8_t *)"code", 4);
    if (!e) e = bj_put_int(b, code);
    if (!e) e = bj_put_key(b, (const uint8_t *)"msg", 3);
    if (!e) e = bj_put_string(b, (const uint8_t *)msg, (uint32_t)strlen(msg));
    if (!e) e = bj_put_key(b, (const uint8_t *)"leaderId", 8);
    if (!e) e = bj_put_int(b, (int64_t)toward);
    if (!e && rec) e = bj_put_key(b, (const uint8_t *)"leader", 6);
    if (!e && rec) e = bj_put_raw(b, rec, rlen);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        e = d ? dbuf_put(out, d, len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/* The common shape: the refusal points at the leader. */
static int refuse(const replica *r, int code, dbuf *out) {
    return refuse_toward(r, code, replica_leader_id(r), out);
}

/*
 * A read, behind the proof that it may be shown (raft_node.h's read
 * barriers, section 6.4).
 *
 *   0  answered outright -- a group of one, which has nobody to hear
 *      from, so the barrier is satisfied the moment it is taken
 *   1  performed, and held until a quorum confirms
 *  <0  the transport's problem
 *
 * THE ORDER IS TAKE, APPLY, PERFORM, HOLD, and each step is where it is
 * for a reason.
 *
 * The barrier is taken FIRST, so `read_index` is the commit index as of
 * the instant the read arrived and the confirmation covers a window
 * beginning there. Confirming that a quorum still follows us over that
 * window proves no later leader existed before it, so everything
 * committed before the read asked is at or below `read_index`.
 *
 * The pump runs before the read is performed, so what it reads is at or
 * above `read_index`. It is normally a no-op -- the transport's loop
 * applies before it serves a client -- but resting on that ordering
 * rather than stating it here would make this correct by accident.
 *
 * The read is performed BEFORE the confirmation arrives, which is the
 * cheap half of the argument and is safe: the state served is at or
 * above `read_index`, and serving something NEWER than the barrier
 * requires is allowed. Only serving something older is not.
 */
/*
 * Did the client declare this read stale-tolerant? A top-level
 * `stale: true` on a read request. Anything else -- absent, false,
 * unreadable -- is the default, which is the strong one.
 */
static int req_reads_stale(const uint8_t *req, size_t len) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(req, len, (const uint8_t *)"stale", 5, &v, &vlen, &found)) return 0;
    if (!found) return 0;
    cur c = { v, vlen, 0 };
    int b = 0;
    return read_bool(&c, &b) == BJ_OK && b;
}

/*
 * The floor the client put under this read: `after: <n>`, a log index
 * it has already been told about (the `commit` a write answered with).
 * 0 -- absent, unreadable, or genuinely zero -- is no floor at all.
 */
static uint64_t req_read_after(const uint8_t *req, size_t len) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(req, len, (const uint8_t *)"after", 5, &v, &vlen, &found)) return 0;
    if (!found) return 0;
    cur c = { v, vlen, 0 };
    uint64_t at = 0;
    return read_u64(&c, &at) == BJ_OK ? at : 0;
}

/*
 * A read the client declared stale-tolerant, served from this member's
 * own applied state, whatever its role.
 *
 * The refusal below ("staleness presented as authority") is about a
 * follower answering as if it were current when nobody asked it to be.
 * This is the other case: the CLIENT signed the waiver, in the request,
 * where it travels with the read -- so what is served is exactly what
 * was asked for. The staleness is bounded by replication lag (the next
 * heartbeat, on a healthy cluster), and nothing here promises tighter:
 * no barrier, no proof, no read-your-writes.
 *
 * What this buys is the two things a leader-only read path cannot have:
 * every member's CPU serves reads instead of one in three, and a read
 * near a far follower streams its documents locally instead of across
 * the WAN. It is also why there is no role check at all -- a leader
 * serving a stale read just skips the barrier, and a member mid-election
 * keeps answering, which makes flagged reads AVAILABLE through the very
 * window where linearizable ones wait.
 *
 * The pump still runs first: "stale" means behind the cluster, never
 * behind this member's own committed log.
 *
 * READ-YOUR-WRITES, when the client asks for it. `after: <n>` is a log
 * index the client has already been told about -- the `commit` some
 * write answered with -- and this member refuses (-76) rather than
 * serve state older than it. That turns "eventually consistent" into
 * "monotonic, from a floor the client chose": a client that writes and
 * then reads never sees its own write vanish, wherever the read lands.
 * The refusal is cheap to act on -- another member may already be past
 * that index, and this one will be within about a heartbeat -- which is
 * why it is its own code rather than -63 or -66.
 */
/*
 * Would this read be better performed somewhere other than here?
 *
 * Asked in ONE place so the two read paths -- linearizable and stale --
 * cannot come to different conclusions about the same request, and so the
 * counters mean one thing. Only asked when reader threads were asked for:
 * sizing a read costs a collection resolve and a dc_explain, which a
 * server running the default of none must not pay.
 *
 * Nothing acts on the answer yet. It is recorded so the decision can be
 * checked -- by a test, and by an operator through `ping` -- while the
 * server is still answering every read on this thread, which is the only
 * moment at which the routing can be judged apart from the threading.
 */
static int read_is_long(replica *r, const uint8_t *req, size_t len) {
    if (!r || r->read_threads <= 0) return 0;

    /*
     * Counted only if the router had a CHOICE about it, so that
     * longReads + shortReads is the number of decisions rather than the
     * number of read requests. `getMore`, `watch`, `listCollections` and a
     * batched `find` can go nowhere at all -- a cursor and a stream belong
     * to the session -- and recording those as "short" would report a read
     * that was never in question as one judged cheap. Two different facts,
     * and an operator watching these numbers to decide whether reader
     * threads are earning their keep needs them apart.
     *
     * dbs_read_is_long asks this again for its own reasons (it will not
     * perform what it cannot). The check is cheap and the two have
     * different jobs: this one gates counting, that one gates answering.
     */
    if (!dbs_request_is_bare(req, len)) return 0;

    int lng = dbi_read_is_long(r->inst, req, len, r->read_min_docs);
    if (lng) r->reads_long++; else r->reads_short++;
    return lng;
}

static int submit_read_stale(replica *r, uint64_t client,
                             const uint8_t *req, size_t len, dbuf *out) {
    int e = apply_committed(r);
    if (e) return e;
    /* After the pump, so the floor is tested against everything this
     * member has actually got -- not against a number that was already
     * stale when the request arrived. */
    if (r->applied < req_read_after(req, len)) {
        e = refuse(r, DC_ERR_BEHIND, out);
        return e ? e : 0;
    }
    (void)read_is_long(r, req, len);

    dbuf cmds = {0};
    uint64_t token = 0;
    e = dbi_propose(r->inst, client, req, len, &token, &cmds, out);
    dbuf_free(&cmds);
    if (e || token) {
        /* A read plans nothing; see submit_read. */
        if (token) dbi_abandon(r->inst, token);
        return e ? e : BJ_ERR_STATE;
    }
    return 0;
}

static int submit_read(replica *r, pending *p, uint64_t client,
                       const uint8_t *req, size_t len, dbuf *out) {
    /*
     * `out` is the CONNECTION's output buffer, not this request's: with
     * pipelined requests it can already hold answers built this batch
     * and not yet flushed (server/main.c's conn_serve loop). Everything
     * below may only APPEND to it, and a deferral may only take back
     * what it appended itself -- zeroing it wholesale was how a ping's
     * answer vanished and every later answer paired one request early.
     */
    size_t mark = out->len;
    uint64_t barrier = 0, read_index = 0;
    int e = rn_read_barrier(r->node, &barrier, &read_index);
    if (e) {
        /* Not the leader is BJ_ERR_STATE and is caught before this is
         * called; what is left is a table full of reads this node has
         * not finished proving. Refused explicitly, like every other
         * bounded table here. */
        e = refuse(r, DC_ERR_TOO_MANY_CLIENTS, out);
        return e ? e : 0;
    }

    e = apply_committed(r);
    if (e) { rn_read_release(r->node, barrier); return e; }
    if (r->applied < read_index) {
        /* Unreachable: everything at or below the commit index is in the
         * log and the pump just ran. Said out loud rather than serving
         * from state that is demonstrably too old. */
        rn_read_release(r->node, barrier);
        e = refuse(r, DC_ERR_NOT_CURRENT, out);
        return e ? e : 0;
    }

    (void)read_is_long(r, req, len);

    dbuf cmds = {0};
    uint64_t token = 0;
    e = dbi_propose(r->inst, client, req, len, &token, &cmds, out);
    dbuf_free(&cmds);
    if (e || token) {
        /* A read plans nothing. A token here means the classifier and
         * the session disagree about what this op does, which is the one
         * thing that must never be true (db_session.h says why). */
        if (token) dbi_abandon(r->inst, token);
        rn_read_release(r->node, barrier);
        return e ? e : BJ_ERR_STATE;
    }

    if (rn_read_state(r->node, barrier) > 0) {
        rn_read_release(r->node, barrier);
        return 0;                       /* nothing to prove; already proved */
    }

    p->used = 1;
    p->client = client;
    p->barrier = barrier;
    p->done = 0;
    /* Only THIS request's answer defers -- the bytes past the mark.
     * Anything before it belongs to earlier pipelined requests and goes
     * out on the ordinary flush, in order. */
    e = dbuf_put(&p->answer, out->data + mark, out->len - mark);
    if (e) { pending_release(p); rn_read_release(r->node, barrier); return e; }
    out->len = mark;                    /* it goes out through replica_ready */
    /* The heartbeats the barrier queued go NOW: a read that waited a
     * tick for its own proof to start would add the tick interval to
     * every read in the cluster. */
    e = flush_out(r);
    return e ? e : 1;
}

/*
 * What this member is, and how far it has got.
 *
 * `ping` is the one thing a follower still answers once reads belong to
 * the leader, so it is where a follower says so -- otherwise a member
 * that is not the leader is a black box over the wire, and a cluster
 * nobody can watch replicate is one nobody can operate. It reports and
 * promises nothing: `applied` is this member's own floor at the moment
 * it was asked, which is exactly the number that must not be used to
 * serve a read nobody declared stale-tolerant.
 *
 * A server without --raft never reaches here and answers the plain
 * { ok, pong } it always did.
 */
static int replica_status(const replica *r, dbuf *out) {
    static const char *const ROLE[] = { "follower", "candidate", "leader" };
    int role = rn_role(r->node);
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_put_key(b, (const uint8_t *)"pong", 4);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_put_key(b, (const uint8_t *)"role", 4);
    if (!e) {
        const char *name = (role >= 0 && role <= 2) ? ROLE[role] : "unknown";
        e = bj_put_string(b, (const uint8_t *)name, (uint32_t)strlen(name));
    }
    if (!e) e = bj_put_key(b, (const uint8_t *)"leaderId", 8);
    if (!e) e = bj_put_int(b, (int64_t)replica_leader_id(r));
    if (!e) e = bj_put_key(b, (const uint8_t *)"applied", 7);
    if (!e) e = bj_put_int(b, (int64_t)r->applied);
    if (!e) e = bj_put_key(b, (const uint8_t *)"commit", 6);
    if (!e) e = bj_put_int(b, (int64_t)rn_commit_index(r->node));
    /* The log's bounds -- how an operator (and a test) sees compaction
     * happen: `base` moves when a snapshot commits, and `last - base` is
     * what the next one will cover. */
    if (!e) e = bj_put_key(b, (const uint8_t *)"base", 4);
    if (!e) e = bj_put_int(b, (int64_t)elog_base_index(r->log));
    if (!e) e = bj_put_key(b, (const uint8_t *)"last", 4);
    if (!e) e = bj_put_int(b, (int64_t)elog_last_index(r->log));
    /* Read routing, for the same reason as the log's bounds above: a
     * decision nobody can observe is a decision nobody can check. Both are
     * 0 on a server that was not asked for reader threads, because it does
     * not ask the question -- and they count DECISIONS, so a read that
     * could never be moved anywhere is in neither. */
    if (!e) e = bj_put_key(b, (const uint8_t *)"readThreads", 11);
    if (!e) e = bj_put_int(b, (int64_t)r->read_threads);
    if (!e) e = bj_put_key(b, (const uint8_t *)"longReads", 9);
    if (!e) e = bj_put_int(b, (int64_t)r->reads_long);
    if (!e) e = bj_put_key(b, (const uint8_t *)"shortReads", 10);
    if (!e) e = bj_put_int(b, (int64_t)r->reads_short);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        e = d ? dbuf_put(out, d, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/* ---- the snapshot ops (docs/s3-backup.md step 3) ------------------------
 *
 * snapshot, latestSnapshot, readSnapshotFile: the client wire's window
 * onto the store this file already keeps. PER-MEMBER, deliberately --
 * they are answered before the leader check, because a follower's
 * generation is a true prefix of history and serving a backup from one
 * offloads the leader. Everything served is the COMMITTED generation:
 * immutable, so no quiesce and no lock; a generation superseded and
 * pruned mid-transfer refuses further reads (DC_ERR_SNAPSHOT_GONE) and
 * the caller restarts from latestSnapshot -- a retry, not a pin,
 * because a pinned generation is disk an operator cannot reclaim, held
 * by a caller that may never come back.
 */

/* One bounded chunk of a generation file per request: big enough to
 * amortize the round trip, small enough that a response frame never
 * balloons (the client bounds responses at 64 MB). */
#define REPLICA_READ_MAX (4u * 1024 * 1024)

static int req_op_is(const uint8_t *req, size_t len, const char *name) {
    const uint8_t *v; size_t vlen; int f = 0;
    if (obj_get_field(req, len, (const uint8_t *)"op", 2, &v, &vlen, &f) || !f) return 0;
    cur c = { v, vlen, 0 };
    const uint8_t *s; uint32_t slen;
    if (take_string(&c, &s, &slen)) return 0;
    return slen == (uint32_t)strlen(name) && memcmp(s, name, slen) == 0;
}

static int req_field_u64(const uint8_t *req, size_t len, const char *key,
                         uint64_t *out_v, int *found) {
    const uint8_t *v; size_t vlen; int f = 0;
    *found = 0;
    if (obj_get_field(req, len, (const uint8_t *)key, (uint32_t)strlen(key),
                      &v, &vlen, &f) || !f) return BJ_OK;
    cur c = { v, vlen, 0 };
    if (read_u64(&c, out_v)) return DC_ERR_REQ_MALFORMED;
    *found = 1;
    return BJ_OK;
}

/* {ok:true, snapshot:<the manifest record, verbatim>} -- gen, boundary,
 * config.live and the file list, spliced raw so the record's shape has
 * exactly one owner (snapstore.h). */
static int snapshot_answer_latest(replica *r, dbuf *out) {
    dbuf rec = {0};
    int has = 0;
    int e = sst_latest(r->store, &rec, &has);
    if (e) { dbuf_free(&rec); return e; }
    if (!has) {
        dbuf_free(&rec);
        return refuse(r, DC_ERR_SNAPSHOT_GONE, out);
    }
    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&rec); return BJ_ERR_OOM; }
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_put_key(b, (const uint8_t *)"snapshot", 8);
    if (!e) e = bj_put_raw(b, rec.data, (uint32_t)rec.len);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        e = d ? dbuf_put(out, d, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    dbuf_free(&rec);
    return e;
}

/* Take one NOW: exactly what the --snapshot-entries trigger runs
 * (snapshot_take + log compaction), with the cadence moved to the
 * caller. Refused while an install is moving the state machine (-66,
 * retryable: the moment passes); idempotent when nothing has been
 * applied since the last boundary -- the committed generation already
 * IS the snapshot of this state, so it is the answer. */
static int snapshot_take_now(replica *r, dbuf *out) {
    if (rn_adopt_pending(r->node) || rn_installing(r->node))
        return refuse(r, DC_ERR_NOT_CURRENT, out);
    if (r->applied == 0)
        return refuse(r, DC_ERR_SNAPSHOT_GONE, out);
    if (r->applied == elog_base_index(r->log) && sst_has_latest(r->store))
        return snapshot_answer_latest(r, out);
    int se = snapshot_take(r);
    if (se) return refuse(r, se, out);
    return snapshot_answer_latest(r, out);
}

/* {gen, role, offset?} -> {ok:true, data, eof, size}: one bounded chunk
 * of one committed generation file, read straight off the store's
 * immutable copy. `gen` must be the committed generation -- naming it
 * is what makes a prune mid-transfer a clean refusal instead of a file
 * that changes identity between chunks. {gen, manifest:true, offset?}
 * reads the MANIFEST file itself, raw -- CRC trailer and all -- because
 * a restore must put back the exact bytes whose validity is the commit
 * (snapstore.h), and a re-encoding is exactly not that. */
static int snapshot_read_file(replica *r, const uint8_t *req, size_t len, dbuf *out) {
    if (!sst_has_latest(r->store))
        return refuse(r, DC_ERR_SNAPSHOT_GONE, out);

    uint64_t gen = 0, offset = 0;
    int found = 0;
    int e = req_field_u64(req, len, "gen", &gen, &found);
    if (e) return refuse(r, e, out);
    if (!found) return refuse(r, DC_ERR_REQ_MISSING_FIELD, out);
    e = req_field_u64(req, len, "offset", &offset, &found);
    if (e) return refuse(r, e, out);

    int manifest = 0;
    {
        const uint8_t *mv; size_t mvlen;
        found = 0;
        if (obj_get_field(req, len, (const uint8_t *)"manifest", 8, &mv, &mvlen, &found))
            return refuse(r, DC_ERR_REQ_MALFORMED, out);
        if (found) {
            cur mc = { mv, mvlen, 0 };
            if (read_bool(&mc, &manifest))
                return refuse(r, DC_ERR_REQ_MALFORMED, out);
        }
    }

    const uint8_t *role = NULL; uint32_t role_len = 0;
    if (!manifest) {
        const uint8_t *rv; size_t rvlen;
        found = 0;
        if (obj_get_field(req, len, (const uint8_t *)"role", 4, &rv, &rvlen, &found))
            return refuse(r, DC_ERR_REQ_MALFORMED, out);
        if (!found) return refuse(r, DC_ERR_REQ_MISSING_FIELD, out);
        cur rc = { rv, rvlen, 0 };
        if (take_string(&rc, &role, &role_len))
            return refuse(r, DC_ERR_REQ_MALFORMED, out);
    }

    if (gen != sst_latest_gen(r->store))
        return refuse(r, DC_ERR_SNAPSHOT_GONE, out);

    dbuf name = {0};
    e = manifest
        ? sst_manifest_name(r->store, gen, &name)
        : sst_data_name(r->store, gen, (const char *)role, role_len, &name);
    if (e) { dbuf_free(&name); return refuse(r, DC_ERR_REQ_MALFORMED, out); }

    bj_io io;
    e = r->ns->open(r->ns->ctx, (const char *)name.data, (uint32_t)name.len, 0, &io);
    dbuf_free(&name);
    /* Absent: a role the manifest never named, or a generation pruned
     * beneath the caller between its chunks. Same remedy either way. */
    if (e) return refuse(r, DC_ERR_SNAPSHOT_GONE, out);

    uint64_t total = io.size(io.ctx);
    if (offset > total) {
        if (io.close) io.close(io.ctx);
        return refuse(r, DC_ERR_REQ_MALFORMED, out);
    }
    uint32_t want = (uint32_t)((total - offset > REPLICA_READ_MAX)
                                   ? REPLICA_READ_MAX : (total - offset));
    uint8_t *buf = (uint8_t *)malloc(want ? want : 1);
    if (!buf) { if (io.close) io.close(io.ctx); return BJ_ERR_OOM; }
    uint32_t at = 0;
    while (!e && at < want) {
        int64_t got = io.read(io.ctx, offset + at, buf + at, want - at);
        if (got <= 0) { e = got < 0 ? (int)got : BJ_ERR_EOF; break; }
        at += (uint32_t)got;
    }
    if (io.close) io.close(io.ctx);
    if (e) { free(buf); return e; }   /* an I/O failure here is the server's trouble, not a refusal */

    bj_builder *b = bj_builder_new();
    if (!b) { free(buf); return BJ_ERR_OOM; }
    e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_put_key(b, (const uint8_t *)"data", 4);
    if (!e) e = bj_put_binary(b, buf, want);
    if (!e) e = bj_put_key(b, (const uint8_t *)"eof", 3);
    if (!e) e = bj_put_bool(b, offset + want >= total);
    if (!e) e = bj_put_key(b, (const uint8_t *)"size", 4);
    if (!e) e = bj_put_int(b, (int64_t)total);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        e = d ? dbuf_put(out, d, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    free(buf);
    return e;
}

/* ---- transferLeadership (raft_node.h's rn_transfer) ---------------------
 *
 * The client wire's handle on the section 3.10 flow: the leader brings
 * `to` fully up to date, tells it to stand NOW, and the answer comes
 * back once leadership has actually moved -- or the node gives up at
 * its deadline and says so (-75). The node owns the whole of that; this
 * side parses the request, holds the asker, and fences new reads and
 * writes toward the target while leadership is in the air.
 */

static int answer_ok(dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        e = d ? dbuf_put(out, d, len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

static int transfer_submit(replica *r, pending *p, uint64_t client,
                           const uint8_t *req, size_t len, dbuf *out) {
    if (!replica_is_leader(r)) {
        int e = refuse(r, DC_ERR_NOT_LEADER, out);
        return e ? e : 0;
    }

    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(req, len, (const uint8_t *)"to", 2, &v, &vlen, &found)) {
        int e = refuse(r, DC_ERR_REQ_MALFORMED, out);
        return e ? e : 0;
    }
    if (!found) {
        int e = refuse(r, DC_ERR_REQ_MISSING_FIELD, out);
        return e ? e : 0;
    }
    cur c = { v, vlen, 0 };
    uint64_t to = 0;
    if (read_u64(&c, &to) != BJ_OK || !to) {
        int e = refuse(r, DC_ERR_REQ_MALFORMED, out);
        return e ? e : 0;
    }

    /* Transfer to self: leadership is already exactly there. */
    if (to == r->self_id) {
        int e = answer_ok(out);
        return e ? e : 0;
    }

    /* One at a time, the serialization rule membership changes follow.
     * The refusal points at the TARGET already in flight: that is where
     * leadership is headed, and where the asker should look. */
    if (rn_transfer_target(r->node)) {
        int e = refuse_toward(r, DC_ERR_NOT_LEADER, rn_transfer_target(r->node), out);
        return e ? e : 0;
    }

    int e = rn_transfer(r->node, to);
    if (e == RAFT_ERR_PEER || e == RAFT_ERR_BUSY) {
        e = refuse(r, e, out);
        return e ? e : 0;
    }
    if (e == BJ_ERR_STATE) {    /* lost leadership between the checks */
        e = refuse(r, DC_ERR_NOT_LEADER, out);
        return e ? e : 0;
    }
    if (e) return e;

    p->used = 1;
    p->client = client;
    p->token = 0;
    p->done = 0;
    r->transfer_waiter = p;
    /* A target that was already current has a TimeoutNow in the outbox
     * NOW; one that was not has the entries that close its gap. */
    e = flush_out(r);
    return e ? e : 1;
}

int replica_submit(replica *r, uint64_t client, const uint8_t *req, size_t len,
                   dbuf *out) {
    if (!r) return BJ_ERR_STATE;

    pending *p = NULL;
    for (int i = 0; i < REPLICA_MAX_PENDING; i++)
        if (!r->waiting[i].used) { p = &r->waiting[i]; break; }
    if (!p) { int e = refuse(r, DC_ERR_TOO_MANY_CLIENTS, out); return e ? e : 0; }

    /*
     * What the request DOES, before any of it is done. Both branches
     * below need to know, and asking first is what lets a follower
     * refuse without touching anything: it used to plan the write, find
     * out it could not take it, and abandon the plan -- which had
     * already CREATED the database directory the request named. A member
     * that cannot take a write has no business making a directory for
     * one.
     */
    int kind = DBS_REQ_NONE;
    dbi_request_kind(req, len, &kind);

    if (kind == DBS_REQ_STATUS) { int e = replica_status(r, out); return e ? e : 0; }

    /* Per-member, before the leader check (db_session.h says why). */
    if (kind == DBS_REQ_SNAPSHOT) {
        int e;
        if (req_op_is(req, len, "snapshot"))            e = snapshot_take_now(r, out);
        else if (req_op_is(req, len, "latestSnapshot")) e = snapshot_answer_latest(r, out);
        else                                            e = snapshot_read_file(r, req, len, out);
        return e ? e : 0;
    }

    if (kind == DBS_REQ_TRANSFER) return transfer_submit(r, p, client, req, len, out);

    /*
     * Before the role check AND the transfer fence: a stale read takes
     * no barrier and proposes nothing, so neither gate protects
     * anything on its path -- and staying open through elections and
     * transfers is half of what the flag is for.
     */
    if (kind == DBS_REQ_READ && req_reads_stale(req, len))
        return submit_read_stale(r, client, req, len, out);

    if (kind == DBS_REQ_READ || kind == DBS_REQ_WRITE) {
        if (!replica_is_leader(r)) {
            /*
             * Reads as well as writes. A follower is behind by at least
             * a round trip and cannot tell by how much, so an answer
             * from it is staleness presented as authority
             * (docs/replicaton-roadmap.md, the step 6 decision).
             * `stale: true` is the client waiving exactly that, and it
             * was routed above before this check.
             */
            int e = refuse(r, DC_ERR_NOT_LEADER, out);
            return e ? e : 0;
        }
        if (rn_transfer_target(r->node)) {
            /*
             * Leadership is in the air (transfer_submit): a write taken
             * now would be proposed by a node doing its best to stop
             * leading, and a read's barrier proven by a quorum it is
             * abdicating. Refused toward the TARGET, so rerouting
             * clients land where leadership is headed -- and if the
             * transfer times out instead, the fence lifts by itself.
             */
            int e = refuse_toward(r, DC_ERR_NOT_LEADER,
                                  rn_transfer_target(r->node), out);
            return e ? e : 0;
        }
    }
    if (kind == DBS_REQ_READ) return submit_read(r, p, client, req, len, out);

    dbuf cmds = {0};
    uint64_t token = 0;
    int e = dbi_propose(r->inst, client, req, len, &token, &cmds, out);
    if (e) { dbuf_free(&cmds); return e; }
    if (!token) { dbuf_free(&cmds); return 0; }   /* a ping, or a refusal */

    p->used = 1;
    p->client = client;
    p->token = token;
    p->done = 0;
    e = propose_batch(r, p, &cmds);
    dbuf_free(&cmds);
    if (e) {
        dbi_abandon(r->inst, token);
        pending_release(p);
        /* A batch the node cannot track is a REFUSAL, not a transport
         * failure: nothing was appended, the previous state stands
         * exactly as it was, and the connection survives to be told so. */
        if (e == DC_ERR_BATCH_TOO_LARGE) {
            int re = refuse(r, e, out);
            return re ? re : 0;
        }
        return e;
    }
    /* Nothing of this request's is in `out` (dbi_propose writes there
     * only when it answers outright, and then token is 0) -- and what IS
     * there belongs to earlier pipelined requests, so it is left alone:
     * the zeroing that used to be here ate a ping's answer whenever one
     * shared a read() with a write, and every later answer on the
     * connection paired one request early. The write's own answer comes
     * later, through replica_ready. */
    /* The AppendEntries the proposal queued goes NOW, not on the next
     * tick: a write that waits a tick for its own replication to start
     * has added the tick interval to every write in the cluster. */
    e = flush_out(r);
    return e ? e : 1;
}

/*
 * One batch has finished. Either the request wants another trip to the
 * log -- a bulkWrite planning its next operation against what this one
 * just did -- or its answer is ready.
 */
/*
 * Stamp a finished write's answer with the log index its entries
 * reached: `at: <n>`, spliced in beside whatever the operation itself
 * answered.
 *
 * NOT `commit`, which `ping` already uses for a different fact -- that
 * member's own commit index. One name for two quantities is how a
 * client ends up putting a floor under a read because it once said
 * hello.
 *
 * THIS IS THE HALF OF READ-YOUR-WRITES THE SERVER OWES. A follower
 * serving a stale read has no way to know which writes the asking
 * client has already been told about -- so the client is told, in the
 * one place it cannot miss, and hands the number back on the reads
 * that must not appear to go backwards (`after`, submit_read_stale).
 *
 * Rebuilt rather than appended to: a binjson object carries its field
 * count in its header, so a field cannot be bolted onto the end of an
 * encoded one. Every field is copied through by raw bytes, which
 * neither parses nor re-encodes the values the operation produced --
 * an answer holding a document must come back byte-identical.
 *
 * Only the replicated path reaches here. An unreplicated server has no
 * log index to report and no follower that could be behind one, so its
 * answers keep exactly the shape they always had.
 */
static int stamp_at(dbuf *answer, uint64_t at) {
    cur c = { answer->data, answer->len, 0 };
    uint32_t count = 0;
    if (object_begin(&c, &count) != BJ_OK) return BJ_OK;   /* not an object: leave it */

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    for (uint32_t i = 0; i < count && !e; i++) {
        const uint8_t *kp; uint32_t klen;
        if (take_key(&c, &kp, &klen) != BJ_OK) { e = BJ_ERR_STATE; break; }
        size_t vstart = c.pos;
        if (skip_value(&c) != BJ_OK) { e = BJ_ERR_STATE; break; }
        /* An answer that already says `at` is not overwritten twice:
         * the first stamp is the one that is true. */
        if (klen == 2 && memcmp(kp, "at", 2) == 0) { bj_builder_free(b); return BJ_OK; }
        e = bj_put_key(b, kp, klen);
        if (!e) e = bj_put_raw(b, c.d + vstart, (uint32_t)(c.pos - vstart));
    }
    if (!e) e = bj_put_key(b, (const uint8_t *)"at", 2);
    if (!e) e = bj_put_int(b, (int64_t)at);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        if (!d) e = BJ_ERR_STATE;
        else { answer->len = 0; e = dbuf_put(answer, d, len); }
    }
    bj_builder_free(b);
    return e;
}

static int advance(replica *r, pending *p) {
    dbuf cmds = {0};
    uint64_t next = 0;
    p->answer.len = 0;
    for (uint32_t i = 0; i < p->n; i++) {
        p->view[i].data = p->results[i].data;
        p->view[i].len = (uint32_t)p->results[i].len;
    }
    int e = dbi_step(r->inst, p->token, p->indices, p->view, p->n, &next, &cmds, &p->answer);
    if (e) { dbuf_free(&cmds); return e; }
    if (!next) {
        dbuf_free(&cmds);
        /* Stamped even when the operation itself REFUSED (a duplicate
         * key, say): the entries were proposed and the log did move, so
         * a later read that must not go backwards has to clear this
         * index too. */
        e = stamp_at(&p->answer, p->last);
        p->done = 1;
        return e;
    }

    p->token = next;
    e = propose_batch(r, p, &cmds);
    dbuf_free(&cmds);
    if (e == DC_ERR_BATCH_TOO_LARGE) {
        /* Mid-request -- a bulkWrite whose next OPERATION plans more
         * entries than the node can track. The operations already
         * applied stay applied, exactly as they do for any mid-list
         * failure; what this one cannot do is report them, because a
         * refusal has no result to carry them in. The refusal names the
         * rule, and the caller re-reads to learn where the list got to. */
        dbi_abandon(r->inst, p->token);
        p->answer.len = 0;
        e = refuse(r, DC_ERR_BATCH_TOO_LARGE, &p->answer);
        p->done = 1;
        return e;
    }
    if (e) dbi_abandon(r->inst, p->token);
    return e;
}

/* A write whose entry a different leader wrote over. It did not happen
 * and no replica holds it, so the client is told exactly that. */
static int lost(replica *r, pending *p) {
    dbi_abandon(r->inst, p->token);
    p->answer.len = 0;
    int e = refuse(r, DC_ERR_WRITE_LOST, &p->answer);
    p->done = 1;
    return e;
}

int replica_tick(replica *r, uint64_t now) {
    if (!r) return BJ_ERR_STATE;

    /*
     * The compaction trigger: enough APPLIED entries since the log's
     * base, and no adoption in flight (an install about to move the
     * whole state machine makes a snapshot of the old one a wasted
     * generation). Host-driven, matching every other policy in this
     * repository -- the engine runs no timers, so WHEN to compact
     * belongs to the side that has a clock, which is this one. A
     * failure says so once rather than every tick, and keeps trying:
     * a full disk that empties should not need a restart to notice.
     */
    if (r->snap_every && !rn_adopt_pending(r->node) && !rn_installing(r->node) &&
        r->applied > elog_base_index(r->log) &&
        r->applied - elog_base_index(r->log) >= r->snap_every) {
        int se = snapshot_take(r);
        if (se && !r->said_snap_failed) {
            r->said_snap_failed = 1;
            fprintf(stderr, "replica: snapshot failed: %s\n", dc_strerror(se));
            fflush(stderr);
        }
        if (!se) r->said_snap_failed = 0;
    }

    int e = rn_tick(r->node, (int64_t)now, rnd01(r));
    if (e) return e;
    e = flush_out(r);            /* heartbeats, and any election it started */
    if (e) return e;
    e = serve_peers(r);          /* and everything the peers have said back */
    if (e) return e;

    /* Twice around: applying raises settlements, and a settlement can
     * propose the next batch of a stepped request, which commits (in a
     * group of one) and needs applying in its turn. */
    for (int round = 0; round < 2; round++) {
        e = apply_committed(r);
        if (e) return e;

        uint32_t n = rn_effect_count(r->node);
        for (uint32_t i = 0; i < n; i++) {
            int kind = rn_effect_kind_at(r->node, i);
            if (kind == RN_EFFECT_NEEDS_SNAPSHOT) {
                /* The log DOES compact now (snapshot_take, above), so a
                 * peer below the base is an expected state -- and the
                 * node serves it an install from the store, so this
                 * effect fires only when there is no adopted generation
                 * to serve FROM (a crash swept it, a disk lost it).
                 * That peer is stuck until a snapshot commits, and it
                 * is said out loud rather than ignored. */
                if (!r->said_no_snapshot) {
                    r->said_no_snapshot = 1;
                    fprintf(stderr, "replica: peer %llu needs a snapshot and this build"
                                    " serves none\n",
                            (unsigned long long)rn_effect_arg(r->node, i));
                    fflush(stderr);
                }
                continue;
            }
            if (kind == RN_EFFECT_PROMOTE) {
                e = promote(r, rn_effect_arg(r->node, i));
                if (e) return e;
                continue;
            }
            if (kind == RN_EFFECT_TRANSFER) {
                /* The transfer ended: leadership left (flag 1) or the
                 * node gave up at its deadline (flag 0). Either way the
                 * asker -- if it is still on the wire -- gets its
                 * answer, and the fence is already down (the node
                 * disarmed before emitting). */
                pending *tp = r->transfer_waiter;
                r->transfer_waiter = NULL;
                if (!tp) continue;
                tp->answer.len = 0;
                e = rn_effect_flag(r->node, i)
                        ? answer_ok(&tp->answer)
                        : refuse(r, DC_ERR_TRANSFER_FAILED, &tp->answer);
                if (e) return e;
                tp->done = 1;
                continue;
            }
            if (kind != RN_EFFECT_SETTLED) continue;
            pending *p = pending_find(r, rn_effect_arg(r->node, i));
            if (!p) continue;   /* nobody is waiting on it any more */
            e = rn_effect_flag(r->node, i) ? advance(r, p) : lost(r, p);
            if (e) return e;
        }
        rn_effects_clear(r->node);
        /* A settlement can propose the next batch of a stepped request,
         * which is an AppendEntries with somewhere to go. */
        e = flush_out(r);
        if (e) return e;
    }

    /*
     * And the reads waiting to be shown. Every barrier reaches one of
     * three ends -- confirmed, lost with the leadership, or expired
     * because a quorum went quiet -- so a client holding a read is never
     * holding it forever, which is the whole of what this table owes.
     */
    for (int i = 0; i < REPLICA_MAX_PENDING; i++) {
        pending *p = &r->waiting[i];
        if (!p->used || p->done || !p->barrier) continue;
        int st = rn_read_state(r->node, p->barrier);
        if (!st) continue;
        if (st < 0) {
            /* The answer was built; it just cannot be shown to be
             * current, and showing it anyway is the one thing a read
             * must not do. */
            p->answer.len = 0;
            e = refuse(r, DC_ERR_NOT_CURRENT, &p->answer);
            if (e) return e;
        }
        rn_read_release(r->node, p->barrier);
        p->barrier = 0;
        p->done = 1;
    }

    /*
     * An effect this node had no room to report. There is no way to
     * recover what was not said and every kind here is actionable, so
     * carrying on would mean acting on a picture known to be incomplete
     * (raft_node.h's rn_effects_lost says exactly this).
     */
    return rn_effects_lost(r->node) ? BJ_ERR_STATE : BJ_OK;
}

int replica_ready(replica *r, uint64_t *client, dbuf *out, int *have) {
    *have = 0;
    if (!r) return BJ_ERR_STATE;
    for (int i = 0; i < REPLICA_MAX_PENDING; i++) {
        pending *p = &r->waiting[i];
        if (!p->used || !p->done) continue;
        int e = dbuf_put(out, p->answer.data, p->answer.len);
        if (e) return e;
        *client = p->client;
        *have = 1;
        pending_release(p);
        return BJ_OK;
    }
    return BJ_OK;
}

void replica_drop_client(replica *r, uint64_t client) {
    if (!r) return;
    for (int i = 0; i < REPLICA_MAX_PENDING; i++) {
        pending *p = &r->waiting[i];
        if (!p->used || p->client != client) continue;
        /* A read holds a barrier and no plan; a write holds a plan and
         * no barrier; a transfer waiter holds neither -- the transfer
         * itself is the node's and proceeds, its answer now has nowhere
         * to go. Whatever was held is given back. */
        if (r->transfer_waiter == p) r->transfer_waiter = NULL;
        if (p->barrier) rn_read_release(r->node, p->barrier);
        else if (!p->done && p->token) dbi_abandon(r->inst, p->token);
        pending_release(p);
    }
}
