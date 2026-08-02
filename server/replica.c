/* server/replica.c — see replica.h. */
#include "replica.h"

#include "raft_node.h"
#include "db_validate.h"   /* dc_strerror, and the two routing codes */
#include "entrylog.h"
#include "binjson.h"
#include "bjcursor.h"

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
 * One client's write, somewhere between planned and answered.
 *
 * `last` is the index whose settlement finishes the batch in flight.
 * Entries commit, apply and settle in index order, so the last one
 * standing in for all of them is not an optimization: an earlier entry
 * of the same batch cannot survive a truncation the later one did not.
 */
typedef struct {
    int      used;
    uint64_t client;
    uint64_t token;             /* dbs_step's */
    uint64_t   *indices;        /* what the batch in flight committed at */
    dbuf       *results;        /* and what applying each one produced */
    dbs_result *view;           /* the same, in the shape dbs_step takes */
    uint32_t n, cap, at;
    uint64_t last;
    int      done;              /* the answer is built and waiting */
    dbuf     answer;
} pending;

struct replica {
    bj_ns     *ns;              /* borrowed */
    dbs       *s;               /* borrowed */
    bj_io      log_io;
    elog      *log;
    raft_node *node;
    uint64_t   applied;
    pending    waiting[REPLICA_MAX_PENDING];
};

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
        if (!p->used || p->done) continue;
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
        if (r->waiting[i].used && !r->waiting[i].done && r->waiting[i].last == last)
            return &r->waiting[i];
    return NULL;
}

/* ---- opening ------------------------------------------------------------ */

int replica_open(bj_ns *ns, dbs *s, uint64_t self_id, uint64_t now, replica **out) {
    if (!ns || !s || !out || !self_id) return BJ_ERR_STATE;
    *out = NULL;
    replica *r = (replica *)calloc(1, sizeof *r);
    if (!r) return BJ_ERR_OOM;
    r->ns = ns;
    r->s = s;

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

    /* A group of one. It elects itself, commits by counting only itself,
     * and every message it would send has nobody to send it to -- which
     * is a whole replica minus other replicas, and exactly the shape
     * peers get added to. */
    bj_builder *b = bj_builder_new();
    if (!b) { replica_close(r); return BJ_ERR_OOM; }
    e = bj_begin_array(b);
    if (!e) e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"id", 2);
    if (!e) e = bj_put_int(b, (int64_t)self_id);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        e = d ? rn_set_members(r->node, d, (uint32_t)len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
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
    r->applied = dbs_applied_floor(s);
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
    rn_start(r->node, (int64_t)now, 0.5);

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
        e = rn_campaign(r->node, 0.5);
        if (!e) e = rn_tick(r->node, (int64_t)now, 0.5);
        rn_out_clear(r->node);
        rn_effects_clear(r->node);
        if (e) { replica_close(r); return e; }
    }

    *out = r;
    return BJ_OK;
}

void replica_close(replica *r) {
    if (!r) return;
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

        if (type == EL_NORMAL) {
            /* Into whoever proposed it, if that was a client of this
             * process -- because the response is built from exactly what
             * applying produced, and this is the only place it happens.
             * A follower's pump writes into a scratch buffer instead;
             * nobody local is waiting on it. */
            dbuf  scratch = {0};
            dbuf *into = &scratch;
            pending *p = pending_for_index(r, index, &into);
            int rc = dbs_apply(r->s, index, payload.data, (uint32_t)payload.len, into);
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

/* Build a refusal into `out`, with the leader's id when there is one. */
static int refuse(const replica *r, int code, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    const char *msg = dc_strerror(code);
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 0);
    if (!e) e = bj_put_key(b, (const uint8_t *)"code", 4);
    if (!e) e = bj_put_int(b, code);
    if (!e) e = bj_put_key(b, (const uint8_t *)"msg", 3);
    if (!e) e = bj_put_string(b, (const uint8_t *)msg, (uint32_t)strlen(msg));
    if (!e) e = bj_put_key(b, (const uint8_t *)"leaderId", 8);
    if (!e) e = bj_put_int(b, (int64_t)replica_leader_id(r));
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

int replica_submit(replica *r, uint64_t client, const uint8_t *req, size_t len,
                   dbuf *out) {
    if (!r) return BJ_ERR_STATE;

    pending *p = NULL;
    for (int i = 0; i < REPLICA_MAX_PENDING; i++)
        if (!r->waiting[i].used) { p = &r->waiting[i]; break; }
    if (!p) { int e = refuse(r, DC_ERR_TOO_MANY_CLIENTS, out); return e ? e : 0; }

    dbuf cmds = {0};
    uint64_t token = 0;
    int e = dbs_propose(r->s, client, req, len, &token, &cmds, out);
    if (e) { dbuf_free(&cmds); return e; }
    if (!token) { dbuf_free(&cmds); return 0; }   /* a read, a ping, a refusal */

    /* It plans, and only then do we find out it is a write -- which a
     * follower cannot take. Nothing has been applied, so abandoning the
     * plan leaves the database exactly as it was. */
    if (!replica_is_leader(r)) {
        dbs_abandon(r->s, token);
        dbuf_free(&cmds);
        out->len = 0;
        e = refuse(r, DC_ERR_NOT_LEADER, out);
        return e ? e : 0;
    }

    p->used = 1;
    p->client = client;
    p->token = token;
    p->done = 0;
    e = propose_batch(r, p, &cmds);
    dbuf_free(&cmds);
    if (e) {
        dbs_abandon(r->s, token);
        pending_release(p);
        return e;
    }
    out->len = 0;   /* the answer comes later, through replica_ready */
    return 1;
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
    int e = dbs_step(r->s, p->token, p->indices, p->view, p->n, &next, &cmds, &p->answer);
    if (e) { dbuf_free(&cmds); return e; }
    if (!next) { dbuf_free(&cmds); p->done = 1; return BJ_OK; }

    p->token = next;
    e = propose_batch(r, p, &cmds);
    dbuf_free(&cmds);
    if (e) dbs_abandon(r->s, p->token);
    return e;
}

/* A write whose entry a different leader wrote over. It did not happen
 * and no replica holds it, so the client is told exactly that. */
static int lost(replica *r, pending *p) {
    dbs_abandon(r->s, p->token);
    p->answer.len = 0;
    int e = refuse(r, DC_ERR_WRITE_LOST, &p->answer);
    p->done = 1;
    return e;
}

int replica_tick(replica *r, uint64_t now) {
    if (!r) return BJ_ERR_STATE;
    int e = rn_tick(r->node, (int64_t)now, 0.5);
    if (e) return e;

    /* Twice around: applying raises settlements, and a settlement can
     * propose the next batch of a stepped request, which commits (in a
     * group of one) and needs applying in its turn. */
    for (int round = 0; round < 2; round++) {
        e = apply_committed(r);
        if (e) return e;

        uint32_t n = rn_effect_count(r->node);
        for (uint32_t i = 0; i < n; i++) {
            if (rn_effect_kind_at(r->node, i) != RN_EFFECT_SETTLED) continue;
            pending *p = pending_find(r, rn_effect_arg(r->node, i));
            if (!p) continue;   /* nobody is waiting on it any more */
            e = rn_effect_flag(r->node, i) ? advance(r, p) : lost(r, p);
            if (e) return e;
        }
        rn_effects_clear(r->node);
        /* The outbox is emptied because a group of one still queues its
         * own replies; with peers, this is where they go on the wire. */
        rn_out_clear(r->node);
    }
    return BJ_OK;
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
        if (!p->done) dbs_abandon(r->s, p->token);
        pending_release(p);
    }
}
