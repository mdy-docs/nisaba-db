/* db_instance.c — see db_instance.h. */
#include "db_instance.h"

#include "db_validate.h"
#include "db_names.h"   /* DC_FORMAT_VERSION, for the -55 refusal */
#include "db_wal.h"     /* DC_ERR_WAL_MISSING_FIELD: a malformed entry */
#include "bjcursor.h"

#include <stdlib.h>
#include <string.h>

/*
 * A token names a REQUEST IN FLIGHT, and the sessions each mint their
 * own from 1. The slot goes in the top bits so a token names its session
 * as well as its request, which is what lets dbs_step find the one that
 * planned it without a second table to keep in step.
 *
 * A session with work in flight used to be provably never closed, so a
 * slot could not mean one database at propose and another at step. A
 * replicated dropDatabase broke the proof: the drop is a committed log
 * entry, and applying it MUST close the session whatever is in flight
 * there -- every replica applies the same prefix, and one that deferred
 * the close would diverge from the ones that did not. So the token now
 * carries the slot's GENERATION, bumped at every close, and a step
 * whose generation is stale is answered DC_ERR_DB_DROPPED rather than
 * stepped into whichever database occupies the slot now.
 */
#define TOKEN_SLOT(t)          ((uint32_t)((t) >> 48) - 1u)
#define TOKEN_GEN(t)           ((uint32_t)((t) >> 40) & 0xffu)
#define TOKEN_INNER(t)         ((t) & 0xffffffffffULL)
#define TOKEN_MAKE(slot,gen,i) ((((uint64_t)(slot) + 1u) << 48) | \
                                (((uint64_t)(gen) & 0xffu) << 40) | (i))

/* The token of an INSTANCE act -- a request whose one log entry is about
 * a database rather than for one. No slot, no session, no state: the
 * apply's result is the whole answer, so the token needs no identity. */
#define TOKEN_INSTANCE     (~0ULL)

typedef struct {
    int      used;
    char     name[DBI_NAME_MAX];
    uint32_t name_len;
    bj_ns    ns;
    dbs     *s;
} dbi_slot;

struct dbi {
    dbi_root  root;
    int       order;
    dbi_slot  db[DBI_MAX_DATABASES];
    /* Each slot's generation, OUTSIDE the slot: slot_close wipes the
     * slot, and the generation's whole job is to survive that. */
    uint32_t  gen[DBI_MAX_DATABASES];
    /* One counter for every cursor and every stream in the process. */
    uint64_t  next_id;
    /* The entry log's reader (dbi_set_log), handed to every database
     * this instance opens so a watch can resume. Zeroed = no log. */
    dbs_log   log;
    /* Files the orphan sweep has collected, over this instance's life.
     * Normally zero -- a number here says an earlier run of the process
     * was interrupted part-way through a DDL op, which is worth saying
     * out loud rather than silently tidying. */
    uint64_t  swept;
};

/* ---- opening ------------------------------------------------------------ */

int dbi_open(const dbi_root *root, int order, dbi **out) {
    if (!root || !root->open_ns || !root->close_ns || !root->list ||
        !root->remove || !out) return BJ_ERR_STATE;
    *out = NULL;
    dbi *i = (dbi *)calloc(1, sizeof *i);
    if (!i) return BJ_ERR_OOM;
    i->root = *root;
    i->order = order;
    *out = i;
    return BJ_OK;
}

static void slot_close(dbi *i, dbi_slot *d) {
    if (!d->used) return;
    /* The session first: it holds trees over files in this namespace. */
    dbs_close(d->s);
    i->root.close_ns(i->root.ctx, &d->ns);
    i->gen[d - i->db]++;   /* every token minted against this occupancy is dead */
    memset(d, 0, sizeof *d);
}

void dbi_close(dbi *i) {
    if (!i) return;
    for (int k = 0; k < DBI_MAX_DATABASES; k++) slot_close(i, &i->db[k]);
    free(i);
}

int dbi_open_count(const dbi *i) {
    if (!i) return 0;
    int n = 0;
    for (int k = 0; k < DBI_MAX_DATABASES; k++) if (i->db[k].used) n++;
    return n;
}

uint64_t dbi_swept(const dbi *i) { return i ? i->swept : 0; }

static dbi_slot *find(dbi *i, const char *name, size_t len) {
    for (int k = 0; k < DBI_MAX_DATABASES; k++) {
        dbi_slot *d = &i->db[k];
        if (d->used && d->name_len == len && memcmp(d->name, name, len) == 0) return d;
    }
    return NULL;
}

int dbi_database(dbi *i, const char *name, size_t len, int create, dbs **out) {
    if (!i || !name || !out) return BJ_ERR_STATE;
    *out = NULL;
    int e = dc_check_db_name(name, len);
    if (e) return e;
    if (len >= DBI_NAME_MAX) return DC_ERR_INVALID_DB_NAME;

    dbi_slot *d = find(i, name, len);
    if (d) { *out = d->s; return BJ_OK; }

    int slot = -1;
    for (int k = 0; k < DBI_MAX_DATABASES; k++) if (!i->db[k].used) { slot = k; break; }
    if (slot < 0) return DC_ERR_TOO_MANY_DATABASES;
    d = &i->db[slot];

    /*
     * THE NAMESPACE GOES IN THE SLOT FIRST, and that is not tidiness:
     * dbs_open BORROWS the bj_ns it is given and keeps the pointer for
     * the session's whole life (db_session.h says so). Opening into a
     * local and handing over its address leaves every file operation
     * reading a stack frame that returned -- which works for exactly as
     * long as nothing has overwritten it, and then does not.
     *
     * The slot is free until `used` is set, so a failure below rewinds
     * it and the instance is left exactly as it was.
     */
    memset(&d->ns, 0, sizeof d->ns);
    e = i->root.open_ns(i->root.ctx, name, (uint32_t)len, create, &d->ns);
    if (e) return e;

    dbs *s = NULL;
    e = dbs_open(&d->ns, i->order, create, &s);
    if (e) {
        i->root.close_ns(i->root.ctx, &d->ns);
        memset(d, 0, sizeof *d);
        return e;
    }

    d->used = 1;
    memcpy(d->name, name, len);
    d->name_len = (uint32_t)len;
    d->s = s;
    dbs_set_id_source(s, &i->next_id);

    /*
     * THE ORPHAN SWEEP, HERE AND NOWHERE ELSE. Every DDL op leaves files
     * nothing references if it is interrupted, and each of them says so as
     * "an orphan the sweep collects" -- so somebody has to sweep, or a
     * crash inside a compaction costs a second copy of the collection for
     * ever. This is the one moment it is safe: the session exists, its
     * catalog is readable, and nothing else holds it yet, so no cursor is
     * positioned in a file and no compaction is part-way through writing
     * one.
     *
     * A failure to sweep is NOT a failure to open. Space is not
     * correctness: a database that cannot be swept still serves every
     * request correctly, and refusing to open it over housekeeping would
     * turn a wasted megabyte into an outage.
     */
    if (i->root.list_files) {
        dbuf names = {0};
        if (i->root.list_files(i->root.ctx, name, (uint32_t)len, &names) == BJ_OK) {
            uint32_t gone = 0;
            (void)dbs_sweep_orphans(s, (const char *)names.data, names.len, &gone);
            i->swept += gone;
        }
        dbuf_free(&names);
    }
    /* The instance's log reader reaches every database it opens, present
     * and future, under that database's own name -- registration happens
     * here so a database opened lazily (an apply creating one, a client
     * naming a new one) is never the one that cannot resume a watch. */
    if (i->log.entry) {
        e = dbs_set_log(s, &i->log, name, len);
        if (e) { slot_close(i, d); return e; }
    }
    *out = s;
    return BJ_OK;
}

int dbi_set_log(dbi *i, const dbs_log *log) {
    if (!i) return BJ_ERR_STATE;
    if (log) i->log = *log;
    else     memset(&i->log, 0, sizeof i->log);
    for (int k = 0; k < DBI_MAX_DATABASES; k++) {
        dbi_slot *d = &i->db[k];
        if (!d->used) continue;
        int e = dbs_set_log(d->s, log, d->name, d->name_len);
        if (e) return e;
    }
    return BJ_OK;
}

int dbi_list(dbi *i, dbuf *out) {
    if (!i || !out) return BJ_ERR_STATE;
    return i->root.list(i->root.ctx, out);
}

int dbi_drop(dbi *i, const char *name, size_t len, int *dropped) {
    if (!i || !name || !dropped) return BJ_ERR_STATE;
    *dropped = 0;
    int e = dc_check_db_name(name, len);
    if (e) return e;
    if (len >= DBI_NAME_MAX) return DC_ERR_INVALID_DB_NAME;

    /* Closed before its files go. A session holds a catalog tree and
     * open collection files pointing into storage that is about to stop
     * existing, and a tree reading a file that no longer has a name is
     * not a diagnosable state -- it is a database that reads fine and
     * persists nothing. */
    dbi_slot *d = find(i, name, len);
    if (d) slot_close(i, d);
    return i->root.remove(i->root.ctx, name, (uint32_t)len, dropped);
}

/* ---- reading the request ------------------------------------------------ */

static int req_str(const uint8_t *req, size_t len, const char *key,
                   const uint8_t **s, uint32_t *slen, int *found) {
    const uint8_t *v; size_t vlen;
    int e = obj_get_field(req, len, (const uint8_t *)key, (uint32_t)strlen(key),
                          &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    return take_string(&c, s, slen);
}

/*
 * The three ops that name no database.
 *
 * Their spellings live here rather than in db_request.c's OP_NAMES table
 * for a reason that is not laziness: two of them are about the INSTANCE
 * and no single database could answer them, and the third is answered
 * before anything at all is resolved. db_request.c owns the spelling of
 * every op that is about a collection; this file owns the three that are
 * not. `ping` appears in both, which is the one duplication, and it is
 * four characters against a routing API that neither layer wants.
 */
static int op_is(const uint8_t *req, size_t len, const char *name) {
    const uint8_t *o; uint32_t olen; int found = 0;
    if (req_str(req, len, "op", &o, &olen, &found) != BJ_OK || !found) return 0;
    return olen == (uint32_t)strlen(name) && memcmp(o, name, olen) == 0;
}

/* { ok:true, <key>: <value spliced raw, or null> } */
static int respond_raw(dbuf *out, const char *key, const uint8_t *v, size_t vlen) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
    if (!e) e = vlen ? bj_put_raw(b, v, (uint32_t)vlen) : bj_put_null(b);
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

static int respond_flag(dbuf *out, const char *key, int value) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"ok", 2);
    if (!e) e = bj_put_bool(b, 1);
    if (!e) e = bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
    if (!e) e = bj_put_bool(b, value);
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

/*
 * Answer an instance op, or say this is not one.
 *
 *   0  not an instance op; the caller routes it to a database
 *   1  answered, and `out` holds the response
 *  <0  no response could be built
 */
static int instance_op(dbi *i, const uint8_t *req, size_t len, dbuf *out) {
    if (op_is(req, len, "ping")) {
        int e = respond_flag(out, "pong", 1);
        return e ? e : 1;
    }
    if (op_is(req, len, "listDatabases")) {
        dbuf names = {0};
        int e = dbi_list(i, &names);
        if (e) { dbuf_free(&names); e = dbs_refusal(e, out); return e ? e : 1; }
        e = respond_raw(out, "databases", names.data, names.len);
        dbuf_free(&names);
        return e ? e : 1;
    }
    if (op_is(req, len, "transferLeadership")) {
        /* Replication's, like the snapshot ops below: server/replica.c
         * answers it before routing ever reaches here, on the leader.
         * An instance asked directly has no log, no peers and no
         * leadership to move, and the refusal says so. */
        int e = dbs_refusal(DC_ERR_NOT_REPLICATED, out);
        return e ? e : 1;
    }
    if (op_is(req, len, "snapshot") || op_is(req, len, "latestSnapshot") ||
        op_is(req, len, "readSnapshotFile")) {
        /* Instance-level, but not this layer's: the snapshot store
         * belongs to the replicated transport (server/replica.c), which
         * answers these before routing ever reaches here. An instance
         * asked directly is an instance with no log, and the refusal
         * says so -- these ops name no database, so letting them fall
         * through would refuse them for the wrong reason. */
        int e = dbs_refusal(DC_ERR_NO_SNAPSHOT_STORE, out);
        return e ? e : 1;
    }
    if (op_is(req, len, "dropDatabase")) {
        const uint8_t *name; uint32_t nlen; int found = 0;
        int e = req_str(req, len, "db", &name, &nlen, &found);
        if (e || !found) {
            e = dbs_refusal(e ? DC_ERR_REQ_MALFORMED : DC_ERR_REQ_MISSING_FIELD, out);
            return e ? e : 1;
        }
        int dropped = 0;
        e = dbi_drop(i, (const char *)name, nlen, &dropped);
        if (e) { e = dbs_refusal(e, out); return e ? e : 1; }
        e = respond_flag(out, "dropped", dropped);
        return e ? e : 1;
    }
    return 0;
}

void dbi_request_kind(const uint8_t *req, size_t req_len, int *kind) {
    *kind = DBS_REQ_NONE;
    if (!req) return;
    /* The instance's own ops, which never reach a session. */
    if (op_is(req, req_len, "ping")) { *kind = DBS_REQ_STATUS; return; }
    if (op_is(req, req_len, "listDatabases")) { *kind = DBS_REQ_READ; return; }
    if (op_is(req, req_len, "dropDatabase"))  { *kind = DBS_REQ_WRITE; return; }
    if (op_is(req, req_len, "transferLeadership")) { *kind = DBS_REQ_TRANSFER; return; }
    dbs_request_kind(req, req_len, kind);
}

/*
 * The database this request names, opening it if needed.
 *
 *   0  resolved into *out
 *   1  refused, and `out` holds the refusal
 *  <0  no response could be built
 *
 * A request that names a database is a request to USE it, so it is made
 * if it is not there -- the same rule an insert into an absent collection
 * follows, and the same rule client.db(name) follows in process. There
 * is nothing to be gained by making "create a database" a separate act
 * whose only effect is a directory.
 */
static int resolve(dbi *i, const uint8_t *req, size_t len, dbs **out, dbuf *res) {
    const uint8_t *name; uint32_t nlen; int found = 0;
    int e = req_str(req, len, "db", &name, &nlen, &found);
    if (e) { e = dbs_refusal(DC_ERR_REQ_MALFORMED, res); return e ? e : 1; }
    if (!found) {
        /* No default, deliberately: a write landing in a database
         * nobody named is worse than a refusal a client can act on. */
        e = dbs_refusal(DC_ERR_REQ_MISSING_FIELD, res);
        return e ? e : 1;
    }
    e = dbi_database(i, (const char *)name, nlen, 1, out);
    if (e == DC_ERR_FORMAT_NEWER) {
        /* The one refusal whose useful content is two numbers the code
         * cannot carry: which version the directory holds, and which this
         * build understands. The session is already gone (dbs_open freed
         * it on refusal), so the stamp is re-read through a short-lived
         * namespace -- a failure to re-read omits the number rather than
         * inventing one. */
        int64_t found_v = -1;
        bj_ns ns;
        memset(&ns, 0, sizeof ns);
        if (i->root.open_ns(i->root.ctx, (const char *)name, (uint32_t)nlen, 0, &ns) == BJ_OK) {
            found_v = dbs_peek_format(&ns);
            i->root.close_ns(i->root.ctx, &ns);
        }
        e = dbs_refusal_format_newer(res, found_v, DC_FORMAT_VERSION);
        return e ? e : 1;
    }
    if (e) { e = dbs_refusal(e, res); return e ? e : 1; }
    return 0;
}

int dbi_read_is_long(dbi *i, const uint8_t *req, size_t req_len, int64_t min_docs) {
    if (!i || !req) return 0;
    /* The instance's own three ops name no collection, so none of them can
     * be a long read; asking a session about them would mean resolving a
     * database that is not named. */
    if (op_is(req, req_len, "ping") || op_is(req, req_len, "listDatabases") ||
        op_is(req, req_len, "dropDatabase") || op_is(req, req_len, "transferLeadership"))
        return 0;

    const uint8_t *name; uint32_t nlen; int found = 0;
    if (req_str(req, req_len, "db", &name, &nlen, &found) || !found) return 0;
    dbs *s = NULL;
    /* create = 0: sizing a read must not bring a database into existence.
     * dbi_handle will make one if the request deserves it. */
    if (dbi_database(i, (const char *)name, nlen, 0, &s)) return 0;
    return dbs_read_is_long(s, req, req_len, min_docs);
}

int dbi_request_wrecks_files(const uint8_t *req, size_t req_len) {
    if (!req) return 1;
    /* The instance's own three: dropDatabase removes a whole directory,
     * which is every file under it. The other two touch none. */
    if (op_is(req, req_len, "dropDatabase")) return 1;
    if (op_is(req, req_len, "ping") || op_is(req, req_len, "listDatabases") ||
        op_is(req, req_len, "transferLeadership")) return 0;
    return dbs_request_wrecks_files(req, req_len);
}

/*
 * Is this entry an INSTANCE-LEVEL drop -- `{ d, i: "drop" }`? The one
 * entry whose apply destroys every record its own database held, which is
 * what makes it the one entry after which the host's instance-level
 * applied mark is load-bearing (server/applied.h). Anything unreadable
 * answers 0: a malformed entry fails at apply, loudly, and this question
 * is only about bookkeeping.
 */
int dbi_entry_is_db_drop(const uint8_t *payload, uint32_t len) {
    const uint8_t *cmd; size_t cmd_len;
    if (dbi_entry_cmd(payload, len, NULL, 0, &cmd, &cmd_len) != BJ_OK) return 0;
    if (cmd) return 0;                     /* a command is FOR a database */
    const uint8_t *act; uint32_t alen; int af = 0;
    if (req_str(payload, len, "i", &act, &alen, &af) || !af) return 0;
    return alen == 4 && memcmp(act, "drop", 4) == 0;
}

int dbi_entry_wrecks_files(const uint8_t *payload, uint32_t len) {
    if (!payload) return 1;
    /*
     * An entry with no command is the instance-level dropDatabase form
     * ({d, i:"drop"}), which removes the database's whole directory.
     * dbi_entry_cmd returns no command for it -- and for an entry that is
     * not an envelope at all, which is a log this build cannot read and is
     * treated as the worst case rather than the best.
     */
    const uint8_t *cmd = NULL; size_t cmd_len = 0;
    if (dbi_entry_cmd(payload, len, NULL, 0, &cmd, &cmd_len)) return 1;
    if (!cmd) return 1;

    int op = 0;
    const uint8_t *coll = NULL; uint32_t coll_len = 0;
    if (dc_wal_parse(cmd, (uint32_t)cmd_len, &op, &coll, &coll_len)) return 1;
    /*
     * The four document ops append; the DDL three make and unmake files.
     * Asked of db_wal.h rather than answered here, so the two cannot come
     * to different conclusions about what a command does -- and so an
     * opcode added later is DDL until db_wal.h says otherwise.
     */
    return !dc_wal_is_document(op);
}

int dbi_read_view(dbi *i, const uint8_t *req, size_t req_len, dc_collection **out) {
    if (!i || !req || !out) return BJ_ERR_STATE;
    *out = NULL;
    const uint8_t *db; uint32_t db_len; int found = 0;
    if (req_str(req, req_len, "db", &db, &db_len, &found) || !found)
        return DC_ERR_REQ_MISSING_FIELD;
    const uint8_t *coll; uint32_t coll_len; found = 0;
    if (req_str(req, req_len, "coll", &coll, &coll_len, &found) || !found)
        return DC_ERR_REQ_MISSING_FIELD;

    dbs *s = NULL;
    /* create = 0, for the same reason dbi_read_is_long uses 0: taking a
     * view is preparation for answering a read, and a read of a database
     * that is not there must refuse rather than make one. */
    int e = dbi_database(i, (const char *)db, db_len, 0, &s);
    if (e) return e;
    return dbs_read_view(s, (const char *)coll, coll_len, out);
}

int dbi_handle(dbi *i, uint64_t client, const uint8_t *req, size_t req_len,
               dbuf *out) {
    if (!i || !req || !out) return BJ_ERR_STATE;
    int r = instance_op(i, req, req_len, out);
    if (r) return r < 0 ? r : BJ_OK;

    dbs *s = NULL;
    r = resolve(i, req, req_len, &s, out);
    if (r) return r < 0 ? r : BJ_OK;
    return dbs_handle(s, client, req, req_len, out);
}

/* ---- the replicated fork ------------------------------------------------ */

/*
 * Every command of `in` (a binjson ARRAY), wrapped in the envelope that
 * says which database it is for. The command itself is spliced in as it
 * stands -- what it MEANS to a collection is db_wal.h's and is not this
 * layer's to restate.
 */
static int wrap_commands(const char *db, uint32_t db_len, const dbuf *in, dbuf *out) {
    cur c = { in->data, in->len, 0 };
    uint32_t count = 0;
    int e = array_begin(&c, &count);
    if (e) return e;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    e = bj_begin_array(b);
    for (uint32_t k = 0; k < count && !e; k++) {
        size_t start = c.pos;
        e = skip_value(&c);
        if (e) break;
        if (!e) e = bj_begin_object(b);
        if (!e) e = bj_put_key(b, (const uint8_t *)"d", 1);
        if (!e) e = bj_put_string(b, (const uint8_t *)db, db_len);
        if (!e) e = bj_put_key(b, (const uint8_t *)"c", 1);
        if (!e) e = bj_put_raw(b, c.d + start, (uint32_t)(c.pos - start));
        if (!e) e = bj_end_object(b);
    }
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        out->len = 0;
        e = d ? dbuf_put(out, d, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/* The slot a token belongs to, or NULL for one nobody issued -- and
 * NULL too when the occupancy that minted it has since been closed (a
 * committed dropDatabase): the generation is the proof. */
static dbi_slot *slot_of_token(dbi *i, uint64_t token) {
    uint32_t slot = TOKEN_SLOT(token);
    if (slot >= DBI_MAX_DATABASES) return NULL;
    if ((i->gen[slot] & 0xffu) != TOKEN_GEN(token)) return NULL;
    return i->db[slot].used ? &i->db[slot] : NULL;
}

/* The one-entry plan of an instance act: [ { d: <name>, i: <act> } ].
 * The `c`-less envelope is the marker -- dbi_entry_cmd already reads
 * such an entry as "no command", so a change-stream replay skips it
 * without knowing it exists. */
static int plan_instance(const uint8_t *name, uint32_t nlen, const char *act,
                         dbuf *cmds) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);
    if (!e) e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"d", 1);
    if (!e) e = bj_put_string(b, name, nlen);
    if (!e) e = bj_put_key(b, (const uint8_t *)"i", 1);
    if (!e) e = bj_put_string(b, (const uint8_t *)act, (uint32_t)strlen(act));
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t n = 0;
        const uint8_t *d = bj_builder_data(b, &n);
        cmds->len = 0;
        e = d ? dbuf_put(cmds, d, n) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

int dbi_propose(dbi *i, uint64_t client, const uint8_t *req, size_t req_len,
                uint64_t *token, dbuf *cmds, dbuf *out) {
    if (!i || !req || !token || !cmds || !out) return BJ_ERR_STATE;
    *token = 0;

    /*
     * dropDatabase, ON THE REPLICATED PATH, is a log entry like any
     * other write -- answered outright it removed a directory only the
     * leader stopped having, which was the divergence recorded for as
     * long as the instance existed. The entry carries the act rather
     * than its outcome; every member applies it against its own root,
     * and the leader's apply result is the reply. Validated here the
     * way any proposal is: a name the entry could never apply is
     * refused before anything is appended.
     */
    if (op_is(req, req_len, "dropDatabase")) {
        const uint8_t *name; uint32_t nlen; int found = 0;
        int e = req_str(req, req_len, "db", &name, &nlen, &found);
        if (e || !found) {
            e = dbs_refusal(e ? DC_ERR_REQ_MALFORMED : DC_ERR_REQ_MISSING_FIELD, out);
            return e < 0 ? e : BJ_OK;
        }
        e = dc_check_db_name((const char *)name, nlen);
        if (!e && nlen >= DBI_NAME_MAX) e = DC_ERR_INVALID_DB_NAME;
        if (e) { e = dbs_refusal(e, out); return e < 0 ? e : BJ_OK; }
        e = plan_instance(name, nlen, "drop", cmds);
        if (e) return e;
        *token = TOKEN_INSTANCE;
        return BJ_OK;
    }

    int r = instance_op(i, req, req_len, out);
    if (r) return r < 0 ? r : BJ_OK;   /* answered outright; nothing to log */

    dbs *s = NULL;
    r = resolve(i, req, req_len, &s, out);
    if (r) return r < 0 ? r : BJ_OK;

    dbi_slot *d = NULL;
    for (int k = 0; k < DBI_MAX_DATABASES; k++)
        if (i->db[k].used && i->db[k].s == s) { d = &i->db[k]; break; }
    if (!d) return BJ_ERR_STATE;

    uint64_t inner = 0;
    int e = dbs_propose(s, client, req, req_len, &inner, cmds, out);
    if (e || !inner) return e;
    e = wrap_commands(d->name, d->name_len, cmds, cmds);
    if (e) { dbs_abandon(s, inner); return e; }
    *token = TOKEN_MAKE((uint32_t)(d - i->db), i->gen[d - i->db], inner);
    return BJ_OK;
}

int dbi_step(dbi *i, uint64_t token, const uint64_t *indices,
             const dbs_result *results, uint32_t n,
             uint64_t *next, dbuf *cmds, dbuf *out) {
    if (!i || !next || !cmds || !out) return BJ_ERR_STATE;
    *next = 0;

    /* An instance act settles in one trip: its apply result -- built by
     * dbi_apply on every replica, read here on the one with the client
     * -- IS the reply, refusal included (a deterministic rc is an answer
     * every replica computed identically). */
    if (token == TOKEN_INSTANCE) {
        if (!results || n != 1) return BJ_ERR_STATE;
        if (results[0].rc) {
            int e = dbs_refusal(results[0].rc, out);
            return e < 0 ? e : BJ_OK;
        }
        return dbuf_put(out, results[0].data, results[0].len);
    }

    dbi_slot *d = slot_of_token(i, token);
    if (!d) {
        /* The occupancy that planned this request is gone -- a committed
         * dropDatabase closed it while the request was in flight. The
         * client is refused, not the server errored: the log applied
         * everything in order and the state is exactly what it says. */
        int e = dbs_refusal(DC_ERR_DB_DROPPED, out);
        return e < 0 ? e : BJ_OK;
    }

    uint64_t inner = 0;
    int e = dbs_step(d->s, TOKEN_INNER(token), indices, results, n, &inner, cmds, out);
    if (e || !inner) return e;
    e = wrap_commands(d->name, d->name_len, cmds, cmds);
    if (e) { dbs_abandon(d->s, inner); return e; }
    *next = TOKEN_MAKE((uint32_t)(d - i->db), i->gen[d - i->db], inner);
    return BJ_OK;
}

void dbi_abandon(dbi *i, uint64_t token) {
    if (!i || token == TOKEN_INSTANCE) return;
    dbi_slot *d = slot_of_token(i, token);
    if (d) dbs_abandon(d->s, TOKEN_INNER(token));
}

uint32_t dbi_inflight(const dbi *i) {
    if (!i) return 0;
    uint32_t n = 0;
    for (int k = 0; k < DBI_MAX_DATABASES; k++)
        if (i->db[k].used) n += dbs_inflight(i->db[k].s);
    return n;
}

/* ---- applying ----------------------------------------------------------- */

int dbi_apply(dbi *i, uint64_t index, const uint8_t *payload, uint32_t len,
              dbuf *result) {
    if (!i || !payload || !result) return BJ_ERR_STATE;

    const uint8_t *name; uint32_t nlen; int found = 0;
    int e = req_str(payload, len, "d", &name, &nlen, &found);
    if (e || !found) return DC_ERR_WAL_MISSING_FIELD;

    const uint8_t *cmd; size_t cmd_len;
    e = obj_get_field(payload, len, (const uint8_t *)"c", 1, &cmd, &cmd_len, &found);
    if (e) return DC_ERR_WAL_MISSING_FIELD;
    if (!found) {
        /* No command: an INSTANCE act, about the database rather than
         * for it. An act this build cannot name is refused rather than
         * skipped -- a replica that ignores what it does not understand
         * has diverged from one that does not. */
        const uint8_t *act; uint32_t alen; int af = 0;
        if (req_str(payload, len, "i", &act, &alen, &af) || !af)
            return DC_ERR_WAL_MISSING_FIELD;
        if (alen == 4 && memcmp(act, "drop", 4) == 0) {
            int dropped = 0;
            e = dbi_drop(i, (const char *)name, nlen, &dropped);
            if (e) return e;
            return respond_flag(result, "dropped", dropped);
        }
        return DC_ERR_WAL_MISSING_FIELD;
    }

    /* Created if this is the entry that makes it, the way a collection is
     * made by the insert that first names it: a replica applying a
     * committed prefix must arrive where the leader did, and refusing
     * here would leave it permanently short of one database. */
    dbs *s = NULL;
    e = dbi_database(i, (const char *)name, nlen, 1, &s);
    if (e) return e;
    return dbs_apply(s, index, cmd, (uint32_t)cmd_len, result);
}

int dbi_entry_cmd(const uint8_t *payload, uint32_t len,
                  const char *db, size_t db_len,
                  const uint8_t **cmd, size_t *cmd_len) {
    if (!payload || !cmd || !cmd_len) return BJ_ERR_STATE;
    *cmd = NULL;
    *cmd_len = 0;
    /* Not an envelope at all -- a payload from before databases had
     * names, or a probe. Nothing to serve, nothing to stop on: the
     * caller is replaying a log, and an entry that is not a command is
     * an entry with no events in it. */
    const uint8_t *name; uint32_t nlen; int found = 0;
    if (req_str(payload, len, "d", &name, &nlen, &found) || !found) return BJ_OK;
    /* db == NULL asks for ANY database's command: a caller classifying what
     * an entry does -- rather than replaying one database's history -- does
     * not know or care whose it is. Kept here rather than given a second
     * reader, because the envelope's shape has one owner. */
    if (db && (nlen != db_len || memcmp(name, db, db_len) != 0)) return BJ_OK;
    const uint8_t *c; size_t clen;
    if (obj_get_field(payload, len, (const uint8_t *)"c", 1, &c, &clen, &found) || !found)
        return BJ_OK;
    *cmd = c;
    *cmd_len = clen;
    return BJ_OK;
}

uint64_t dbi_applied_floor(dbi *i) {
    if (!i) return 0;
    dbuf names = {0};
    uint64_t floor = 0;
    if (dbi_list(i, &names) != BJ_OK) { dbuf_free(&names); return 0; }

    cur c = { names.data, names.len, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count) == BJ_OK) {
        for (uint32_t k = 0; k < count; k++) {
            const uint8_t *n; uint32_t nlen;
            if (take_string(&c, &n, &nlen)) break;
            /* Was it already open? One that was not is closed again, so
             * asking for the floor does not quietly fill the table. */
            int was_open = find(i, (const char *)n, nlen) != NULL;
            dbs *s = NULL;
            if (dbi_database(i, (const char *)n, nlen, 0, &s) != BJ_OK) continue;
            uint64_t at = dbs_applied_floor(s);
            if (at > floor) floor = at;
            if (!was_open) {
                dbi_slot *d = find(i, (const char *)n, nlen);
                if (d) slot_close(i, d);
            }
        }
    }
    dbuf_free(&names);
    return floor;
}

/*
 * SWEEP EVERY DATABASE, ONCE, AT STARTUP.
 *
 * dbi_database sweeps on the open that first reads a database, which is
 * correct but lazy: on a server that is not replicated nothing opens a
 * database at boot, so the disk an interrupted operation wasted is
 * reclaimed only when somebody happens to use it, and the count the host
 * wants to LOG is zero at the moment it logs. Asking here makes it a
 * startup fact instead.
 *
 * Same walk dbi_applied_floor makes, and the same courtesy: a database
 * that was not open is closed again, so a sweep does not quietly fill the
 * table. Nothing to sweep with (no list_files) is not an error -- it is
 * every host that has not been given one.
 */
int dbi_sweep_all(dbi *i) {
    if (!i) return BJ_ERR_STATE;
    if (!i->root.list_files) return BJ_OK;
    dbuf names = {0};
    if (dbi_list(i, &names) != BJ_OK) { dbuf_free(&names); return BJ_OK; }

    cur c = { names.data, names.len, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count) == BJ_OK) {
        for (uint32_t k = 0; k < count; k++) {
            const uint8_t *n; uint32_t nlen;
            if (take_string(&c, &n, &nlen)) break;
            int was_open = find(i, (const char *)n, nlen) != NULL;
            dbs *s = NULL;
            /* The sweep is dbi_database's, on the open below. */
            if (dbi_database(i, (const char *)n, nlen, 0, &s) != BJ_OK) continue;
            if (!was_open) {
                dbi_slot *d = find(i, (const char *)n, nlen);
                if (d) slot_close(i, d);
            }
        }
    }
    dbuf_free(&names);
    return BJ_OK;
}

/* ---- clients and streams ------------------------------------------------ */

void dbi_drop_client(dbi *i, uint64_t client) {
    if (!i) return;
    for (int k = 0; k < DBI_MAX_DATABASES; k++)
        if (i->db[k].used) dbs_drop_client(i->db[k].s, client);
}

int dbi_stream_take(dbi *i, uint64_t client, dbuf *out, int *have) {
    if (!i || !out || !have) return BJ_ERR_STATE;
    *have = 0;
    for (int k = 0; k < DBI_MAX_DATABASES; k++) {
        if (!i->db[k].used) continue;
        int e = dbs_stream_take(i->db[k].s, client, out, have);
        if (e) return e;
        if (*have) return BJ_OK;
    }
    return BJ_OK;
}

int dbi_stream_pending(const dbi *i) {
    if (!i) return 0;
    for (int k = 0; k < DBI_MAX_DATABASES; k++)
        if (i->db[k].used && dbs_stream_pending(i->db[k].s)) return 1;
    return 0;
}
