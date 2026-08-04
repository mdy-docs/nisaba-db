/* server/replica.c — see replica.h. */
#include "replica.h"

#include "raft_node.h"
#include "raft_msg.h"      /* the member-record grammar; this file reads it */
#include "db_validate.h"   /* dc_strerror, and the two routing codes */
#include "entrylog.h"
#include "binjson.h"
#include "bjcursor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* The log, when no snapshot generation has been adopted. The same name
 * src/db-wal.js opens, so a database written by one host is a database
 * the other can serve. */
#define REPLICA_WAL "__wal__.bj"

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
    bj_io      log_io;
    elog      *log;
    raft_node *node;
    uint64_t   applied;
    uint64_t   rnd;             /* the election-timeout draw's state */
    uint64_t   self_id;
    uint64_t   next_corr;       /* inbound ids, minted here */
    uint64_t   config_index;    /* the CONFIG entry in force */
    int        said_no_snapshot;
    int        said_no_conversation;
    int        said_no_address;
    conversation conv[REPLICA_MAX_CONV];
    pending    waiting[REPLICA_MAX_PENDING];
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
     * Applied our own removal, or our own demotion to learner. As leader
     * we committed the entry first, so the new set has it -- and a node
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

int replica_open(bj_ns *ns, dbi *inst, uint64_t self_id, peers *px,
                 const uint8_t *members, uint32_t members_len,
                 uint64_t now, replica **out) {
    if (!ns || !inst || !out || !self_id) return BJ_ERR_STATE;
    if (px && peers_count(px) > rn_max_peers()) return RAFT_ERR_CAPACITY;
    *out = NULL;
    replica *r = (replica *)calloc(1, sizeof *r);
    if (!r) return BJ_ERR_OOM;
    r->ns = ns;
    r->inst = inst;
    r->px = px;
    r->self_id = self_id;
    /* Non-zero, or xorshift stays at zero forever and every member draws
     * the same nothing -- which is the bug this exists to avoid. */
    r->rnd = (self_id * 0x9E3779B97F4A7C15ULL) ^ (now + 0x100000001B3ULL);
    if (!r->rnd) r->rnd = 0x9E3779B97F4A7C15ULL;

    int e = ns->open(ns->ctx, REPLICA_WAL, (uint32_t)strlen(REPLICA_WAL),
                     BJ_NS_CREATE, &r->log_io);
    if (e) { free(r); return e; }

    /* An empty file is a new log; anything else is one to recover. Both
     * are entrylog.h's call, not this file's guess about its format. */
    r->log = (r->log_io.size(r->log_io.ctx) > 0) ? elog_open(&r->log_io)
                                                 : elog_create(&r->log_io);
    if (!r->log) {
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        free(r);
        return BJ_ERR_STATE;
    }

    r->node = rn_new(self_id, r->log);
    if (!r->node) {
        elog_free(r->log);
        if (r->log_io.close) r->log_io.close(r->log_io.ctx);
        free(r);
        return BJ_ERR_OOM;
    }
    rn_set_ns(r->node, ns);

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
    if (r->node) rn_free(r->node);
    if (r->log) elog_free(r->log);
    /* elog_free does not close the handle behind the io; the one that
     * opened it closes it, which is this. */
    if (r->log_io.close) r->log_io.close(r->log_io.ctx);
    free(r);
}

int      replica_is_leader(const replica *r) { return r && rn_role(r->node) == RAFT_LEADER; }
uint64_t replica_leader_id(const replica *r) { return r ? rn_leader_id(r->node) : 0; }

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

/* ---- the clock ---------------------------------------------------------- */

/*
 * A tick every heartbeat is enough and is what the JavaScript host does
 * (RaftGroupHost's tickMs). Asking the node for its exact next deadline
 * would be a second copy of the timer arithmetic; a fixed slice is the
 * transport's own business and cannot be wrong, only coarse.
 */
#define REPLICA_TICK_MS 20

int replica_wait_ms(const replica *r, uint64_t now) {
    (void)r; (void)now;
    return REPLICA_TICK_MS;
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
            /* Whoever just arrived has to be caught up, and the entry
             * that put them here is already behind us. */
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
static const uint8_t *leader_record(const replica *r, uint32_t *len) {
    uint64_t want = replica_leader_id(r);
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

/* Build a refusal into `out`, with the leader's id and record when there
 * is one. */
static int refuse(const replica *r, int code, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    const char *msg = dc_strerror(code);
    uint32_t rlen = 0;
    const uint8_t *rec = leader_record(r, &rlen);
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 0);
    if (!e) e = bj_put_key(b, (const uint8_t *)"code", 4);
    if (!e) e = bj_put_int(b, code);
    if (!e) e = bj_put_key(b, (const uint8_t *)"msg", 3);
    if (!e) e = bj_put_string(b, (const uint8_t *)msg, (uint32_t)strlen(msg));
    if (!e) e = bj_put_key(b, (const uint8_t *)"leaderId", 8);
    if (!e) e = bj_put_int(b, (int64_t)replica_leader_id(r));
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
static int submit_read(replica *r, pending *p, uint64_t client,
                       const uint8_t *req, size_t len, dbuf *out) {
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
        out->len = 0;
        e = refuse(r, DC_ERR_NOT_CURRENT, out);
        return e ? e : 0;
    }

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
    e = dbuf_put(&p->answer, out->data, out->len);
    if (e) { pending_release(p); rn_read_release(r->node, barrier); return e; }
    out->len = 0;                       /* it goes out through replica_ready */
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
 * serve a read.
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

    if ((kind == DBS_REQ_READ || kind == DBS_REQ_WRITE) && !replica_is_leader(r)) {
        /*
         * Reads as well as writes. A follower is behind by at least a
         * round trip and cannot tell by how much, so an answer from it
         * is staleness presented as authority
         * (docs/replicaton-roadmap.md, the step 6 decision).
         */
        int e = refuse(r, DC_ERR_NOT_LEADER, out);
        return e ? e : 0;
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
        return e;
    }
    out->len = 0;   /* the answer comes later, through replica_ready */
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
    if (!next) { dbuf_free(&cmds); p->done = 1; return BJ_OK; }

    p->token = next;
    e = propose_batch(r, p, &cmds);
    dbuf_free(&cmds);
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
                /* Nothing here compacts the log, so no peer can fall
                 * below its base and this cannot fire. If it ever does,
                 * that peer is stuck and no message will unstick it --
                 * so it is said out loud rather than ignored. */
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
         * no barrier. Both have to be given back. */
        if (p->barrier) rn_read_release(r->node, p->barrier);
        else if (!p->done) dbi_abandon(r->inst, p->token);
        pending_release(p);
    }
}
