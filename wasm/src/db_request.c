/*
 * db_request.c — the request grammar: one binjson object in, one out.
 *
 * See db_session.h for the contract. Two rules shape everything here:
 *
 *   A refusal is a RESPONSE, not a return code. Every way a client can
 *   be wrong comes back as {ok:false, code, msg} with BJ_OK returned,
 *   because a client that asked a question is owed an answer. Only a
 *   failure to build any response at all (out of memory) is returned to
 *   the transport, which is the one failure the transport can do
 *   anything about.
 *
 *   Nothing is re-encoded on the way through. Filters, documents and
 *   updates arrive as binjson and are handed to the engine as the bytes
 *   they arrived as; results leave as the bytes the engine produced,
 *   spliced in with bj_put_raw. A layer that decoded and rebuilt them
 *   would be a second opinion about what the client said.
 */
#include "db_session.h"

#include "db_wal.h"
#include "db_query.h"
#include "db_agg.h"
#include "db_update.h"
#include "db_validate.h"
#include "db_bulk.h"
#include "bjcursor.h"
#include "binjson.h"

#include <stdlib.h>
#include <string.h>

/*
 * The op names, in the one place they appear. A host dispatches on the
 * enum; the strings exist here and nowhere else, which is the same reason
 * db_wal.c keeps its OP_NAME table to itself.
 */
typedef enum {
    OP_PING = 0,
    OP_FIND,
    OP_FIND_ONE,
    OP_COUNT,
    OP_DISTINCT,
    OP_INSERT,
    OP_UPDATE,
    OP_UPDATE_MANY,
    OP_REPLACE,
    OP_DELETE,
    OP_DELETE_MANY,
    OP_GET_MORE,
    OP_CLOSE_CURSOR,
    OP_COMPACT,
    OP_CREATE_COLLECTION,
    OP_DROP_COLLECTION,
    OP_CREATE_INDEX,
    OP_DROP_INDEX,
    OP_LIST_INDEXES,
    OP_LIST_COLLECTIONS,
    OP_INSERT_MANY,
    OP_BULK_WRITE,
    OP_AGGREGATE,
    OP_EXPLAIN,
    OP_FIND_ONE_AND_UPDATE,
    OP_FIND_ONE_AND_REPLACE,
    OP_FIND_ONE_AND_DELETE,
    OP_FIND_BY_INDEX,
    OP_PRUNE_EXPIRED,
    OP_WATCH,
    OP_CLOSE_STREAM
} dbs_op;

/* The length comes from the literal itself, so the two cannot disagree.
 * They did, briefly: renaming killCursor to closeCursor left a hand-
 * written 10 beside an eleven-character name, and an op whose length is
 * wrong is an op that simply never matches. */
#define OP(name, code) { name, (uint32_t)(sizeof(name) - 1), code }

static const struct { const char *name; uint32_t len; dbs_op op; } OP_NAMES[] = {
    OP("ping",        OP_PING),
    OP("find",        OP_FIND),
    OP("findOne",     OP_FIND_ONE),
    OP("count",       OP_COUNT),
    OP("distinct",    OP_DISTINCT),
    OP("insert",      OP_INSERT),
    OP("update",      OP_UPDATE),
    OP("updateMany",  OP_UPDATE_MANY),
    OP("replace",     OP_REPLACE),
    OP("delete",      OP_DELETE),
    OP("deleteMany",  OP_DELETE_MANY),
    OP("getMore",     OP_GET_MORE),
    OP("closeCursor", OP_CLOSE_CURSOR),
    OP("compact",     OP_COMPACT),
    OP("createCollection", OP_CREATE_COLLECTION),
    OP("dropCollection",   OP_DROP_COLLECTION),
    OP("createIndex",      OP_CREATE_INDEX),
    OP("dropIndex",        OP_DROP_INDEX),
    OP("listIndexes",      OP_LIST_INDEXES),
    OP("listCollections",  OP_LIST_COLLECTIONS),
    OP("insertMany",       OP_INSERT_MANY),
    OP("bulkWrite",        OP_BULK_WRITE),
    OP("aggregate",        OP_AGGREGATE),
    OP("explain",          OP_EXPLAIN),
    OP("findOneAndUpdate",  OP_FIND_ONE_AND_UPDATE),
    OP("findOneAndReplace", OP_FIND_ONE_AND_REPLACE),
    OP("findOneAndDelete",  OP_FIND_ONE_AND_DELETE),
    OP("findByIndex",       OP_FIND_BY_INDEX),
    OP("pruneExpired",      OP_PRUNE_EXPIRED),
    OP("watch",             OP_WATCH),
    OP("closeStream",       OP_CLOSE_STREAM),
};

#undef OP

/* ---- reading the request ----------------------------------------------- */

static int field_raw(const uint8_t *o, size_t olen, const char *key,
                     const uint8_t **p, size_t *plen, int *found) {
    return obj_get_field(o, olen, (const uint8_t *)key, (uint32_t)strlen(key),
                         p, plen, found);
}

static int field_str(const uint8_t *o, size_t olen, const char *key,
                     const uint8_t **s, uint32_t *slen, int *found) {
    const uint8_t *v; size_t vlen;
    int e = field_raw(o, olen, key, &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    return take_string(&c, s, slen);
}

static int field_int(const uint8_t *o, size_t olen, const char *key,
                     int64_t *out, int64_t dflt) {
    const uint8_t *v; size_t vlen; int found = 0;
    *out = dflt;
    int e = field_raw(o, olen, key, &v, &vlen, &found);
    if (e || !found) return e;
    cur c = { v, vlen, 0 };
    double d;
    e = read_number(&c, &d);
    if (e) return e;
    *out = (int64_t)d;
    return BJ_OK;
}

/* `dflt` is what absent means, and it is not always false: `upsert` is off
 * unless asked for, `ordered` is on unless turned off -- the same defaults
 * the driver methods have, so a client that omits a field gets what
 * omitting it means everywhere else. */
static int field_flag(const uint8_t *o, size_t olen, const char *key, int dflt, int *out) {
    const uint8_t *v; size_t vlen; int found = 0;
    *out = dflt;
    int e = field_raw(o, olen, key, &v, &vlen, &found);
    if (e || !found) return e;
    cur c = { v, vlen, 0 };
    return read_bool(&c, out);
}

/* The 12 id bytes a write falls back on, as an OID or a 12-byte binary --
 * whichever the client's codec produced. */
static int field_id(const uint8_t *o, size_t olen, uint8_t out[12], int *found) {
    const uint8_t *v; size_t vlen;
    int e = field_raw(o, olen, "id", &v, &vlen, found);
    if (e || !*found) return e;
    if (vlen == 13 && (v[0] == BJ_TYPE_OID)) { memcpy(out, v + 1, 12); return BJ_OK; }
    cur c = { v, vlen, 0 };
    const uint8_t *b; uint32_t blen;
    uint8_t t;
    if (take_type(&c, &t) || t != BJ_TYPE_BINARY) return DC_ERR_REQ_MALFORMED;
    if (take_u32(&c, &blen)) return BJ_ERR_EOF;
    if (blen != 12 || cur_need(&c, blen)) return DC_ERR_REQ_MALFORMED;
    b = c.d + c.pos;
    memcpy(out, b, 12);
    return BJ_OK;
}

/* The caller's clock reading, in milliseconds -- present only when the
 * request carries a write that might need one. */
static int field_ms(const uint8_t *o, size_t olen, int64_t *out, int *found) {
    const uint8_t *v; size_t vlen;
    *out = 0;
    int e = field_raw(o, olen, "now", &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    double d;
    if (read_number(&c, &d)) return DC_ERR_REQ_MALFORMED;
    *out = (int64_t)d;
    return BJ_OK;
}

/*
 * Rewrite {$currentDate: {...}} into the {$set: {...}} the engine
 * understands -- with the CALLER's clock.
 *
 * The rewrite is upd_resolve_current_date's: which fields, and the rule
 * that a field already targeted by another operator cannot also be dated.
 * Every host of this library calls it before proposing a write, so that
 * what is written down carries a concrete date rather than a rule that
 * would read a different clock on replay (db_wal.h). This server is a
 * host too. What it does not have is a clock -- deliberately, which is
 * the same reason an insert's _id comes from whoever asked -- so the
 * milliseconds arrive with the request, and an update that needs them and
 * was not given them is refused rather than given a time invented here.
 *
 * `scratch` owns the rewrite when there is one; with no $currentDate in
 * the update, *out is the update as it arrived, unmoved and uncopied.
 */
static int resolve_dates(const uint8_t *upd, size_t upd_len,
                         int64_t now_ms, int have_now, dbuf *scratch,
                         const uint8_t **out, size_t *out_len) {
    *out = upd; *out_len = upd_len;
    if (!upd) return BJ_OK;
    const uint8_t *cd; size_t cd_len; int has = 0;
    if (obj_get_field(upd, upd_len, (const uint8_t *)"$currentDate", 12,
                      &cd, &cd_len, &has))
        return DC_ERR_REQ_MALFORMED;
    if (!has) return BJ_OK;
    if (!have_now) return DC_ERR_REQ_MISSING_FIELD;
    scratch->len = 0;
    int e = upd_resolve_current_date(upd, upd_len, now_ms, scratch);
    if (e) return e;
    *out = scratch->data; *out_len = scratch->len;
    return BJ_OK;
}

/* find's options, as db_query.h wants them. Absent is "none" for every
 * one of them, which is what an empty request means. */
static int read_opts(const uint8_t *req, size_t req_len, qry_options *qo, int *have,
                     int64_t *batch_size) {
    const uint8_t *o; size_t olen; int found = 0;
    memset(qo, 0, sizeof(*qo));
    *have = 0;
    if (batch_size) *batch_size = 0;
    int e = field_raw(req, req_len, "opts", &o, &olen, &found);
    if (e || !found) return e;
    *have = 1;

    size_t vlen; int f = 0;
    const uint8_t *v;
    if ((e = field_raw(o, olen, "sort", &v, &vlen, &f))) return e;
    if (f) { qo->sort = v; qo->sort_len = (uint32_t)vlen; }
    f = 0;
    if ((e = field_raw(o, olen, "projection", &v, &vlen, &f))) return e;
    if (f) { qo->projection = v; qo->projection_len = (uint32_t)vlen; }
    if ((e = field_int(o, olen, "skip", &qo->skip, 0))) return e;
    if ((e = field_int(o, olen, "limit", &qo->limit, 0))) return e;
    if (batch_size && (e = field_int(o, olen, "batchSize", batch_size, 0))) return e;
    return BJ_OK;
}

/* ---- building the response --------------------------------------------- */

#define PUT_KEY(b, s) bj_put_key((b), (const uint8_t *)(s), (uint32_t)strlen(s))

static int finish(bj_builder *b, dbuf *out) {
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) return BJ_ERR_OOM;
    return dbuf_put(out, data, len);
}

/* {ok:false, code, msg}. The text is dc_strerror's, so a client reads the
 * same sentence a native caller would have. */
static int respond_error(dbuf *out, int code) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    const char *msg = dc_strerror(code);
    bj_begin_object(b);
    PUT_KEY(b, "ok");   bj_put_bool(b, 0);
    PUT_KEY(b, "code"); bj_put_int(b, code);
    PUT_KEY(b, "msg");  bj_put_string(b, (const uint8_t *)msg, (uint32_t)strlen(msg));
    bj_end_object(b);
    int e = finish(b, out);
    bj_builder_free(b);
    return e;
}

/* The same refusal, naming a POSITION in a list of operations: which one
 * of a bulkWrite's writes was malformed. The extra field is additive --
 * a client reading {ok:false, code, msg} reads this one unchanged -- and
 * it exists because "operation 7 has no filter" is the whole content of
 * the answer, and a client cannot work out which 7 was from the code. */
static int respond_error_at(dbuf *out, int code, int index) {
    if (index < 0) return respond_error(out, code);
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    const char *msg = dc_strerror(code);
    bj_begin_object(b);
    PUT_KEY(b, "ok");    bj_put_bool(b, 0);
    PUT_KEY(b, "code");  bj_put_int(b, code);
    PUT_KEY(b, "msg");   bj_put_string(b, (const uint8_t *)msg, (uint32_t)strlen(msg));
    PUT_KEY(b, "index"); bj_put_int(b, index);
    bj_end_object(b);
    int e = finish(b, out);
    bj_builder_free(b);
    return e;
}

int dbs_refusal(int code, dbuf *out) {
    if (!out) return BJ_ERR_STATE;
    return respond_error(out, code);
}

/* {_id: <oid>} -- the filter that reads one document back by id, which
 * the query layer answers with a single bpt_search rather than a scan. */
static int id_filter(const uint8_t id[12], dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    PUT_KEY(b, "_id"); bj_put_oid(b, id);
    bj_end_object(b);
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    int e = data ? dbuf_put(out, data, len) : BJ_ERR_OOM;
    bj_builder_free(b);
    return e;
}

/* ---- the ops ------------------------------------------------------------ */

/* Sum the per-command results a many-form produced into the single result
 * a client gets back. This belongs in C for the reason db_wal.h gives for
 * the result shape itself: under replication the result of applying a
 * command is part of its semantics, and two hosts that added up two
 * commands differently would answer one committed write two ways. */
static void accumulate(const uint8_t *res, size_t res_len,
                       int64_t *matched, int64_t *modified, int64_t *deleted) {
    static const struct { const char *key; int which; } FIELDS[] = {
        { "matchedCount", 0 }, { "modifiedCount", 1 }, { "deletedCount", 2 },
    };
    for (size_t i = 0; i < sizeof(FIELDS) / sizeof(FIELDS[0]); i++) {
        const uint8_t *v; size_t vlen; int found = 0;
        if (obj_get_field(res, res_len, (const uint8_t *)FIELDS[i].key,
                          (uint32_t)strlen(FIELDS[i].key), &v, &vlen, &found) || !found)
            continue;
        cur c = { v, vlen, 0 };
        double d = 0;
        if (read_number(&c, &d)) continue;
        if (FIELDS[i].which == 0) *matched  += (int64_t)d;
        else if (FIELDS[i].which == 1) *modified += (int64_t)d;
        else *deleted += (int64_t)d;
    }
}

/*
 * One change event, from the command that caused it and the result of
 * applying it.
 *
 * This is where a change stream comes from, and it costs almost nothing
 * because a LOGGED COMMAND already names the one document it touched:
 * the planner expanded updateMany into one command per matched document
 * before any of this ran. The derivation is the one every other host of
 * this library makes (wasm/nisaba-wasm.js's _applyCommand), including
 * the one read it cannot avoid -- an update names its CHANGES, not its
 * outcome, so the document has to be read back to say what it now is.
 * That read is a bpt_search by _id, and it happens only while somebody
 * is watching.
 *
 * An upsert reaches here as a plain insert, because that is what the
 * planner wrote down; a watcher sees `insert`, which is what happened.
 */
static int emit_change(dbs *s, dc_collection *c, const char *coll, uint32_t coll_len,
                       const uint8_t *cmd, uint32_t cmd_len,
                       const uint8_t *result, size_t result_len) {
    int op = 0;
    const uint8_t *cname; uint32_t cname_len;
    int e = dc_wal_parse(cmd, cmd_len, &op, &cname, &cname_len);
    if (e) return e;

    /* Nothing happened, nothing to say: a delete that removed no
     * document, an update whose target had already gone. */
    static const struct { const char *key; } NIL = { NULL };
    (void)NIL;
    int64_t n = 0;
    {
        static const char *COUNTS[] = { "deletedCount", "matchedCount" };
        for (size_t i = 0; i < sizeof(COUNTS) / sizeof(COUNTS[0]); i++) {
            const uint8_t *v; size_t vlen; int f = 0;
            if (obj_get_field(result, result_len, (const uint8_t *)COUNTS[i],
                              (uint32_t)strlen(COUNTS[i]), &v, &vlen, &f) || !f) continue;
            cur rc = { v, vlen, 0 };
            double d = 0;
            if (!read_number(&rc, &d)) n += (int64_t)d;
        }
        /* An insert reports neither count; it reports an id. */
        if (op != DC_WAL_INSERT && n == 0) return BJ_OK;
    }

    const uint8_t *doc = NULL; size_t doc_len = 0; int has_doc = 0;
    if ((e = field_raw(cmd, cmd_len, "doc", &doc, &doc_len, &has_doc))) return e;

    uint8_t id[12];
    int has_id = 0;
    if (op == DC_WAL_INSERT) {
        if (!has_doc) return BJ_ERR_STATE;
        e = dc_document_id(doc, doc_len, id);
        if (e) return e;
        has_id = 1;
    } else {
        const uint8_t *v; size_t vlen; int f = 0;
        if ((e = field_raw(cmd, cmd_len, "id", &v, &vlen, &f))) return e;
        if (!f || vlen != 13 || v[0] != BJ_TYPE_OID) return BJ_ERR_STATE;
        memcpy(id, v + 1, 12);
        has_id = 1;
    }
    (void)has_id;

    /* An update's post-image, read back by the id the command names. */
    dbuf after = {0};
    if (op == DC_WAL_UPDATE) {
        dbuf idf = {0};
        if ((e = id_filter(id, &idf))) { dbuf_free(&idf); return e; }
        uint8_t *d = NULL; size_t dlen = 0; int got = 0;
        e = dc_find_one(c, idf.data, (uint32_t)idf.len, NULL, 0, &got, &d, &dlen);
        dbuf_free(&idf);
        if (!e && got) e = dbuf_put(&after, d, dlen);
        free(d);
        if (e) { dbuf_free(&after); return e; }
    }

    static const char *const TYPE[] = { "insert", "update", "replace", "delete" };
    const char *type = (op >= 0 && op <= DC_WAL_DELETE) ? TYPE[op] : NULL;
    if (!type) { dbuf_free(&after); return BJ_OK; }   /* DDL: not a document change */

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&after); return BJ_ERR_OOM; }
    bj_begin_object(b);
    /* `ns` first, then the event: the shape an in-process watcher gets
     * (wasm/nisaba-wasm.js's _emitChange), so a consumer reads the same
     * object whichever side of a socket it is on. */
    PUT_KEY(b, "ns");
    bj_begin_object(b);
    PUT_KEY(b, "coll"); bj_put_string(b, (const uint8_t *)coll, coll_len);
    bj_end_object(b);
    PUT_KEY(b, "operationType");
    bj_put_string(b, (const uint8_t *)type, (uint32_t)strlen(type));
    PUT_KEY(b, "documentKey");
    bj_begin_object(b);
    PUT_KEY(b, "_id"); bj_put_oid(b, id);
    bj_end_object(b);
    if (op == DC_WAL_INSERT || op == DC_WAL_REPLACE) {
        PUT_KEY(b, "fullDocument"); bj_put_raw(b, doc, (uint32_t)doc_len);
    } else if (op == DC_WAL_UPDATE && after.len) {
        PUT_KEY(b, "fullDocument"); bj_put_raw(b, after.data, (uint32_t)after.len);
    }
    bj_end_object(b);
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (data) dbs_emit(s, coll, coll_len, data, len);
    e = data ? BJ_OK : BJ_ERR_STATE;
    bj_builder_free(b);
    dbuf_free(&after);
    return e;
}

/* What one write did, in the terms every result is built from. A list of
 * writes adds its members into one of these, which is what makes
 * bulkWrite's totals the sum of its parts rather than a second opinion
 * about them. */
typedef struct {
    int64_t matched, modified, deleted, inserted, upserted;
    /* DC_PLAN_NOTHING / _MATCHED / _UPSERT, and the single document id
     * the plan resolved -- which the find-one-and-* family needs to read
     * a post-image back, and which is the upserted id when the outcome
     * says so. One field, because they are one fact. */
    int     outcome;
    int     has_target_id;
    uint8_t target_id[12];
} write_result;

/* ---- the replicated fork (db_session.h) ---------------------------------
 *
 * Every write here is plan-then-apply. On a replica the two halves are
 * separated by a quorum, and these three lines are the whole of the
 * difference: take a plan (fresh, or the one this request already made),
 * apply one command (at index 0, or at the index it committed at), and
 * give the plan back (unless the session is holding it across the wait).
 *
 * Written as helpers rather than as a branch in each of the four loops
 * because there are four of them -- run_write, insertMany, bulkWrite and
 * DDL -- and a rule copied four times is a rule with four chances to
 * drift.
 */

/* The plan for this write. DC_PENDING means it was built and kept: the
 * commands belong to the log now, and nothing may be applied until they
 * come back. */
static int plan_open(dbs *s, dc_collection *c, const char *coll, uint32_t coll_len,
                     int wreq, const uint8_t *a, uint32_t a_len,
                     const uint8_t *b, uint32_t b_len,
                     int upsert, const uint8_t id[12], dc_wal_plan **out) {
    if (dbs_repl_replaying(s)) {
        *out = dbs_repl_resuming(s);
        /* Exhausted means this run planned more than the first did, which
         * cannot happen over the same bytes and the same catalog -- and
         * if it ever does, it must not be papered over by planning
         * afresh against state a quorum never saw. */
        return *out ? BJ_OK : BJ_ERR_STATE;
    }
    int e = dc_wal_plan_build(c, coll, coll_len, wreq, a, a_len, b, b_len,
                              upsert, id, out);
    if (e) return e;
    if (!dbs_repl_planning(s)) return BJ_OK;
    e = dbs_repl_hold(s, *out);
    if (e) { dc_wal_plan_free(*out); *out = NULL; return e; }
    /* Planned and kept. The caller skips its apply loop and carries on --
     * bulkWrite must reach its LAST operation to plan it, so this can
     * never be an early return out of the request. */
    return DC_PENDING;
}

/* Apply one command, staging the index it committed at. Zero on a
 * single-process server, where there is no log and dc_wal_apply is told
 * so. */
static int apply_cmd(dbs *s, dc_collection *c, const uint8_t *cmd, uint32_t clen,
                     dbuf *one) {
    return dc_wal_apply(c, dbs_repl_next_index(s), cmd, clen, one);
}

/* Give the plan back, unless the session is holding it for a quorum --
 * in which case it belongs to the session until the answer comes. */
static void plan_close(dbs *s, dc_wal_plan *p) {
    if (p && !dbs_repl_active(s)) dc_wal_plan_free(p);
}

/* Plan a write and apply every command it produced, reporting what
 * happened through *wr. Errors are the caller's to turn into a response:
 * a refusal for a single write, one entry in `errors` inside a list. */
static int run_write(dbs *s, dc_collection *c, const char *coll, uint32_t coll_len,
                     int wreq, const uint8_t *a, uint32_t a_len,
                     const uint8_t *b, uint32_t b_len,
                     int upsert, const uint8_t id[12], write_result *wr,
                     dbuf *preimage) {
    memset(wr, 0, sizeof *wr);

    dc_wal_plan *p = NULL;
    int e = plan_open(s, c, coll, coll_len, wreq, a, a_len, b, b_len,
                      upsert, id, &p);
    if (e == DC_PENDING) return BJ_OK;   /* planned; the applies wait for a quorum */
    if (e) return e;

    /* The document as it was, taken before anything is applied. The
     * planner already had it in hand -- it is how the target was
     * resolved -- so this costs a copy rather than a second query, which
     * is the whole reason dc_wal_plan_preimage exists. NULL asks for
     * nothing: every write but the find-one-and-* family ignores it. */
    if (preimage) {
        uint32_t plen = 0;
        const uint8_t *img = dc_wal_plan_preimage(p, &plen);
        if (img && plen) {
            e = dbuf_put(preimage, img, plen);
            if (e) { dc_wal_plan_free(p); return e; }
        }
    }

    int64_t inserted = 0;
    uint32_t n = dc_wal_plan_count(p);
    const int watching = dbs_watched(s, coll, coll_len);
    dbuf one = {0};

    for (uint32_t i = 0; i < n; i++) {
        uint32_t clen = 0;
        const uint8_t *cmd = dc_wal_plan_cmd(p, i, &clen);
        if (!cmd) { e = BJ_ERR_STATE; goto done; }
        one.len = 0;
        /* index 0: this collection is not log-driven here, so the applied
         * index is not staged. A replicated server passes the log index
         * it committed at, and dc_wal_apply stages it with the mutation. */
        e = apply_cmd(s, c, cmd, clen, &one);
        if (e) goto done;
        accumulate(one.data, one.len, &wr->matched, &wr->modified, &wr->deleted);
        const uint8_t *v; size_t vlen; int found = 0;
        if (!obj_get_field(one.data, one.len, (const uint8_t *)"insertedId", 10,
                           &v, &vlen, &found) && found) {
            inserted++;
        }
        /* Told after the write committed, and only if somebody asked to
         * be told: a stream is an observer, never a participant, so it
         * cannot fail the write it is watching. */
        if (watching) {
            int ee = emit_change(s, c, coll, coll_len, cmd, clen, one.data, one.len);
            if (ee) { e = ee; goto done; }
        }
    }
    wr->outcome = dc_wal_plan_outcome(p);
    {
        const uint8_t *tid = dc_wal_plan_target_id(p);
        if (tid) { memcpy(wr->target_id, tid, 12); wr->has_target_id = 1; }
    }
    /* An upsert is APPLIED as an insert, so the apply result says
     * "insertedId" -- but a driver counts it as an upsert and not as
     * both, and the plan is the only thing that still knows which it
     * was. */
    if (wr->outcome == DC_PLAN_UPSERT) wr->upserted = 1;
    else                               wr->inserted = inserted;

done:
    dbuf_free(&one);
    plan_close(s, p);
    return e;
}

/* One write's result, in the shape a single-document driver method
 * returns. */
static int render_write(const write_result *wr, dbuf *out) {
    bj_builder *rb = bj_builder_new();
    if (!rb) return BJ_ERR_OOM;
    bj_begin_object(rb);
    PUT_KEY(rb, "acknowledged"); bj_put_bool(rb, 1);
    PUT_KEY(rb, "matchedCount");  bj_put_int(rb, wr->matched);
    PUT_KEY(rb, "modifiedCount"); bj_put_int(rb, wr->modified);
    PUT_KEY(rb, "deletedCount");  bj_put_int(rb, wr->deleted);
    PUT_KEY(rb, "insertedCount"); bj_put_int(rb, wr->inserted);
    PUT_KEY(rb, "upsertedId");
    if (wr->outcome == DC_PLAN_UPSERT && wr->has_target_id) bj_put_oid(rb, wr->target_id);
    else bj_put_null(rb);
    bj_end_object(rb);
    size_t rlen = 0;
    const uint8_t *rdata = bj_builder_data(rb, &rlen);
    int e = rdata ? dbuf_put(out, rdata, rlen) : BJ_ERR_OOM;
    bj_builder_free(rb);
    return e;
}

static int do_write(dbs *s, dc_collection *c, const char *coll, uint32_t coll_len,
                    int wreq, const uint8_t *a, uint32_t a_len,
                    const uint8_t *b, uint32_t b_len,
                    int upsert, const uint8_t id[12], dbuf *out) {
    write_result wr;
    int e = run_write(s, c, coll, coll_len, wreq, a, a_len, b, b_len, upsert, id, &wr, NULL);
    if (e) return e;
    return render_write(&wr, out);
}

/* ---- lists of writes ----------------------------------------------------
 *
 * insertMany and bulkWrite. They are not the same operation -- one list
 * holds documents and the other holds writes of six different kinds --
 * but they fail the same way, so they answer in the same shape.
 *
 * A FAILED MEMBER IS A RESULT, NOT A REFUSAL. That is what makes
 * `ordered` mean anything: false attempts every member regardless of
 * earlier failures, true stops at the first. The response says how many
 * were ATTEMPTED, because with ordered:true "never tried" and "tried and
 * succeeded" are different answers and nothing else in the response tells
 * them apart. Which of the attempted ones succeeded is then everything
 * not named in `errors`.
 *
 * Inserted ids are not returned and upserted ids are: an insert's id was
 * chosen by whoever asked and is already known to them, while an upsert's
 * was resolved here (from the filter, if the filter named one).
 */

/* One {index, code, msg} in the errors array. The text is dc_strerror's,
 * the same sentence a refusal carries -- a client with no engine in it
 * has no error table and should not grow one. */
static int put_error(bj_builder *b, uint32_t index, int code) {
    const char *msg = dc_strerror(code);
    bj_begin_object(b);
    PUT_KEY(b, "index"); bj_put_int(b, (int64_t)index);
    PUT_KEY(b, "code");  bj_put_int(b, code);
    PUT_KEY(b, "msg");   bj_put_string(b, (const uint8_t *)msg, (uint32_t)strlen(msg));
    bj_end_object(b);
    return bj_builder_error(b);
}

/* Splice a finished builder's ARRAY in under `key`, or null when it holds
 * nothing -- the same "absent means none" the rest of the wire uses. */
static int put_list(bj_builder *rb, const char *key, bj_builder *list, int count) {
    bj_put_key(rb, (const uint8_t *)key, (uint32_t)strlen(key));
    if (!count) return bj_put_null(rb);
    size_t len = 0;
    const uint8_t *data = bj_builder_data(list, &len);
    if (!data) return BJ_ERR_STATE;
    return bj_put_raw(rb, data, (uint32_t)len);
}

static int respond_many(dbuf *out, const write_result *t, uint32_t attempted,
                        bj_builder *upserted, int nupserted,
                        bj_builder *errors, int nerrors) {
    bj_builder *rb = bj_builder_new();
    if (!rb) return BJ_ERR_OOM;
    bj_begin_object(rb);
    PUT_KEY(rb, "ok"); bj_put_bool(rb, 1);
    PUT_KEY(rb, "result");
    bj_begin_object(rb);
    PUT_KEY(rb, "acknowledged");  bj_put_bool(rb, 1);
    PUT_KEY(rb, "insertedCount"); bj_put_int(rb, t->inserted);
    PUT_KEY(rb, "matchedCount");  bj_put_int(rb, t->matched);
    PUT_KEY(rb, "modifiedCount"); bj_put_int(rb, t->modified);
    PUT_KEY(rb, "deletedCount");  bj_put_int(rb, t->deleted);
    PUT_KEY(rb, "upsertedCount"); bj_put_int(rb, t->upserted);
    bj_end_object(rb);
    PUT_KEY(rb, "attempted"); bj_put_int(rb, (int64_t)attempted);
    int e = put_list(rb, "upserted", upserted, nupserted);
    if (!e) e = put_list(rb, "errors", errors, nerrors);
    bj_end_object(rb);
    if (!e) e = finish(rb, out);
    bj_builder_free(rb);
    return e;
}

/*
 * insertMany: ONE plan, one command per document, applied in document
 * order -- so a command's position IS its document's, and a failure is
 * named by index without this loop and the planner having to agree about
 * anything else.
 *
 * Every document must already carry its own _id, and all of them are
 * checked before any of them runs. C will not invent an id (that needs a
 * clock, which db.h keeps out of this layer), and the check has to happen
 * up front for db_bulk.h's reason: an unordered run must be able to
 * attempt every document, which it cannot do if document seven is
 * unusable in a way that only surfaces once one through six have landed.
 */
static int do_insert_many(dbs *s, dc_collection *c, const char *coll, uint32_t coll_len,
                          const uint8_t *docs, size_t docs_len, int ordered,
                          dbuf *out) {
    static const uint8_t NO_ID[12] = {0};   /* INSERT_MANY needs no default */

    cur scan = { docs, docs_len, 0 };
    uint32_t count = 0;
    int e = array_begin(&scan, &count);
    if (e) return DC_ERR_REQ_MALFORMED;
    if (count == 0) return DC_ERR_BULK_EMPTY;

    for (uint32_t i = 0; i < count; i++) {
        size_t start = scan.pos;
        if (skip_value(&scan)) return DC_ERR_REQ_MALFORMED;
        const uint8_t *v; size_t vlen; int f = 0;
        if (obj_get_field(docs + start, scan.pos - start,
                          (const uint8_t *)"_id", 3, &v, &vlen, &f) || !f)
            return DC_ERR_REQ_MISSING_FIELD;
    }

    dc_wal_plan *p = NULL;
    e = plan_open(s, c, coll, coll_len, DC_WREQ_INSERT_MANY,
                  docs, (uint32_t)docs_len, NULL, 0, 0, NO_ID, &p);
    if (e == DC_PENDING) return BJ_OK;
    if (e) return e;

    bj_builder *errb = bj_builder_new();
    if (!errb) { dc_wal_plan_free(p); return BJ_ERR_OOM; }
    bj_begin_array(errb);

    write_result total;
    memset(&total, 0, sizeof total);
    const int watching = dbs_watched(s, coll, coll_len);
    uint32_t attempted = 0, n = dc_wal_plan_count(p);
    int nerr = 0;
    dbuf one = {0};

    for (uint32_t i = 0; i < n; i++) {
        uint32_t clen = 0;
        const uint8_t *cmd = dc_wal_plan_cmd(p, i, &clen);
        if (!cmd) { e = BJ_ERR_STATE; break; }
        one.len = 0;
        int rc = apply_cmd(s, c, cmd, clen, &one);
        if (!rc && watching) {
            int ee = emit_change(s, c, coll, coll_len, cmd, clen, one.data, one.len);
            if (ee) { e = ee; break; }
        }
        attempted++;
        if (rc) {
            if ((e = put_error(errb, i, rc))) break;
            nerr++;
            if (ordered) break;
            continue;
        }
        total.inserted++;
    }
    dbuf_free(&one);
    plan_close(s, p);

    if (!e) {
        bj_end_array(errb);
        e = bj_builder_error(errb);
    }
    if (!e) e = respond_many(out, &total, attempted, NULL, 0, errb, nerr);
    bj_builder_free(errb);
    return e;
}

/* One bulk operation, read out of its spec object: which write it is and
 * what it writes with. dc_bulk_parse has already checked that the fields
 * each kind requires are there; what is checked here is the WIRE's own
 * rules -- the 12 bytes a write needs when it turns out to need one, and
 * the milliseconds a $currentDate needs. Both are the caller's to supply,
 * and both are checked over the whole list before any of it runs. */
typedef struct {
    int wreq;
    const uint8_t *a; uint32_t a_len;
    const uint8_t *b; uint32_t b_len;
    int upsert;
    uint8_t id[12];
} bulk_write;

static int read_bulk_write(int type, const uint8_t *sp, size_t sp_len,
                           int have_now, bulk_write *bw) {
    memset(bw, 0, sizeof *bw);
    const uint8_t *v; size_t vlen; int f = 0;
    int have_id = 0;

    if (field_flag(sp, sp_len, "upsert", 0, &bw->upsert)) return DC_ERR_REQ_MALFORMED;
    if (field_id(sp, sp_len, bw->id, &have_id)) return DC_ERR_REQ_MALFORMED;

    switch (type) {
        case DC_BULK_INSERT_ONE: {
            if (field_raw(sp, sp_len, "document", &v, &vlen, &f) || !f)
                return DC_ERR_REQ_MALFORMED;
            const uint8_t *iv; size_t ilen; int has = 0;
            if (obj_get_field(v, vlen, (const uint8_t *)"_id", 3, &iv, &ilen, &has) || !has)
                return DC_ERR_REQ_MISSING_FIELD;
            bw->wreq = DC_WREQ_INSERT_ONE;
            bw->a = v; bw->a_len = (uint32_t)vlen;
            bw->upsert = 0;
            return BJ_OK;
        }
        case DC_BULK_UPDATE_ONE:
        case DC_BULK_UPDATE_MANY:
        case DC_BULK_REPLACE_ONE: {
            const char *second = (type == DC_BULK_REPLACE_ONE) ? "replacement" : "update";
            if (field_raw(sp, sp_len, "filter", &v, &vlen, &f) || !f)
                return DC_ERR_REQ_MALFORMED;
            bw->a = v; bw->a_len = (uint32_t)vlen;
            f = 0;
            if (field_raw(sp, sp_len, second, &v, &vlen, &f) || !f)
                return DC_ERR_REQ_MALFORMED;
            bw->b = v; bw->b_len = (uint32_t)vlen;
            bw->wreq = (type == DC_BULK_UPDATE_ONE)  ? DC_WREQ_UPDATE_ONE
                     : (type == DC_BULK_UPDATE_MANY) ? DC_WREQ_UPDATE_MANY
                                                     : DC_WREQ_REPLACE_ONE;
            /* An upsert that matches nothing inserts, and an insert needs
             * an id from whoever asked. */
            if (bw->upsert && !have_id) return DC_ERR_REQ_MISSING_FIELD;
            /* And an update that dates a field needs a clock reading,
             * for the same reason. The rewrite itself waits until this
             * operation actually runs; what cannot wait is finding out
             * that it could never have run. */
            if (type != DC_BULK_REPLACE_ONE && !have_now) {
                const uint8_t *cd; size_t cdlen; int has = 0;
                if (obj_get_field(bw->b, bw->b_len, (const uint8_t *)"$currentDate", 12,
                                  &cd, &cdlen, &has))
                    return DC_ERR_REQ_MALFORMED;
                if (has) return DC_ERR_REQ_MISSING_FIELD;
            }
            return BJ_OK;
        }
        default:
            if (field_raw(sp, sp_len, "filter", &v, &vlen, &f) || !f)
                return DC_ERR_REQ_MALFORMED;
            bw->a = v; bw->a_len = (uint32_t)vlen;
            bw->wreq = (type == DC_BULK_DELETE_ONE) ? DC_WREQ_DELETE_ONE
                                                    : DC_WREQ_DELETE_MANY;
            bw->upsert = 0;
            return BJ_OK;
    }
}

/* Walk to the next {name: spec} of the operations array and hand back the
 * SPEC. The name was matched by dc_bulk_parse, which is why nothing here
 * looks at it -- one owner for the grammar. */
static int next_spec(cur *ops, const uint8_t **sp, size_t *sp_len) {
    size_t start = ops->pos;
    int e = skip_value(ops);
    if (e) return e;
    const uint8_t *entry = ops->d + start;
    cur in = { entry, ops->pos - start, 0 };
    uint32_t nkeys;
    const uint8_t *kp; uint32_t klen;
    if ((e = object_begin(&in, &nkeys))) return e;
    if ((e = take_key(&in, &kp, &klen))) return e;
    size_t at = in.pos;
    if ((e = skip_value(&in))) return e;
    *sp = entry + at;
    *sp_len = in.pos - at;
    return BJ_OK;
}

/*
 * bulkWrite: a list of writes of six different kinds, run here rather
 * than looped by the client. In a host that shares a process with the
 * engine that loop is JavaScript's job (db_bulk.h says so, and it stays
 * true there); over a socket the same loop is N round trips, and a client
 * with no engine in it has no dc_bulk_parse to check the list with
 * either.
 *
 * `types` is that parse's output: one code per operation, in order.
 */
static int do_bulk_write(dbs *s, dc_collection *c, const char *coll, uint32_t coll_len,
                         const uint8_t *ops, size_t ops_len,
                         const uint8_t *types, size_t types_len,
                         int ordered, int64_t now_ms, int have_now, dbuf *out) {
    cur oc = { ops, ops_len, 0 }, tc = { types, types_len, 0 };
    uint32_t count = 0, tcount = 0;
    int e = array_begin(&oc, &count);
    if (!e) e = array_begin(&tc, &tcount);
    if (e) return e;
    if (count != tcount) return BJ_ERR_STATE;   /* one type per operation */

    /* Pass one: the wire's id rule over the whole list, before any of it
     * runs -- the same up-front discipline dc_bulk_parse applies to the
     * grammar, and for the same reason. */
    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *sp; size_t sp_len; double d; bulk_write bw;
        if ((e = next_spec(&oc, &sp, &sp_len))) return DC_ERR_REQ_MALFORMED;
        if ((e = read_number(&tc, &d))) return e;
        if ((e = read_bulk_write((int)d, sp, sp_len, have_now, &bw))) return e;
    }

    oc.pos = 0; tc.pos = 0;
    if ((e = array_begin(&oc, &count))) return e;
    if ((e = array_begin(&tc, &tcount))) return e;

    bj_builder *errb = bj_builder_new();
    bj_builder *upb  = bj_builder_new();
    if (!errb || !upb) {
        bj_builder_free(errb); bj_builder_free(upb);
        return BJ_ERR_OOM;
    }
    bj_begin_array(errb);
    bj_begin_array(upb);

    write_result total;
    memset(&total, 0, sizeof total);
    dbuf dates = {0};
    uint32_t attempted = 0;
    int nerr = 0, nups = 0;

    for (uint32_t i = 0; i < count; i++) {
        const uint8_t *sp; size_t sp_len; double d; bulk_write bw;
        if ((e = next_spec(&oc, &sp, &sp_len))) break;
        if ((e = read_number(&tc, &d))) break;
        if ((e = read_bulk_write((int)d, sp, sp_len, have_now, &bw))) break;

        /* $currentDate becomes a concrete date HERE, once per operation
         * but from one clock reading for the whole request: two members
         * of one bulkWrite dating the same field should not disagree
         * about when it was. */
        const uint8_t *ub; size_t ub_len;
        if ((e = resolve_dates(bw.b, bw.b_len, now_ms, have_now, &dates, &ub, &ub_len))) break;

        write_result wr;
        int rc = run_write(s, c, coll, coll_len, bw.wreq, bw.a, bw.a_len,
                           ub, (uint32_t)ub_len, bw.upsert, bw.id, &wr, NULL);
        attempted++;
        if (rc) {
            if ((e = put_error(errb, i, rc))) break;
            nerr++;
            if (ordered) break;
            continue;
        }
        total.inserted += wr.inserted;
        total.matched  += wr.matched;
        total.modified += wr.modified;
        total.deleted  += wr.deleted;
        total.upserted += wr.upserted;
        if (wr.outcome == DC_PLAN_UPSERT && wr.has_target_id) {
            bj_begin_object(upb);
            PUT_KEY(upb, "index"); bj_put_int(upb, (int64_t)i);
            PUT_KEY(upb, "id");    bj_put_oid(upb, wr.target_id);
            bj_end_object(upb);
            if ((e = bj_builder_error(upb))) break;
            nups++;
        }
    }

    if (!e) {
        bj_end_array(errb);
        bj_end_array(upb);
        e = bj_builder_error(errb);
        if (!e) e = bj_builder_error(upb);
    }
    if (!e) e = respond_many(out, &total, attempted, upb, nups, errb, nerr);
    dbuf_free(&dates);
    bj_builder_free(errb);
    bj_builder_free(upb);
    return e;
}

/*
 * One batch out of an open cursor, and the id to ask for the next one --
 * or null, which is how a client learns the scan is over without a
 * second round trip to find out. A drained cursor is closed HERE rather
 * than waiting for a closeCursor that a well-behaved client would have no
 * reason to send.
 *
 * { ok:true, docs:[...], cursor: <id> | null }
 */
static int respond_batch(dbs *s, uint64_t client, uint64_t id, dc_cursor *cur,
                         uint32_t batch, dbuf *out) {
    uint8_t *docs = NULL; size_t docs_len = 0; int done = 0;
    int e = dc_cursor_next_batch(cur, batch, &docs, &docs_len, &done);
    if (e) return e;

    if (done && id) dbs_cursor_drop(s, client, id);

    bj_builder *b = bj_builder_new();
    if (!b) { free(docs); return BJ_ERR_OOM; }
    bj_begin_object(b);
    PUT_KEY(b, "ok"); bj_put_bool(b, 1);
    PUT_KEY(b, "docs");
    if (docs_len) bj_put_raw(b, docs, (uint32_t)docs_len); else bj_put_null(b);
    PUT_KEY(b, "cursor");
    if (done) bj_put_null(b); else bj_put_int(b, (int64_t)id);
    bj_end_object(b);
    free(docs);
    e = finish(b, out);
    bj_builder_free(b);
    return e;
}

/*
 * One DDL request, taken the way a write is taken: planned into the
 * command a log would carry, then applied.
 *
 * The two steps look like ceremony while there is no log to put the
 * command in, and they are not. Planning is where the command's SHAPE is
 * decided (db_wal.c, once, for every host); applying is where its RESULT
 * is computed (dbs_apply, once, for every replica). Doing it directly
 * would decide both here, a second time, and the gap between this
 * server's DDL and a replicated one would be a rewrite rather than a
 * proposal.
 *
 * `index` 0: nothing is staged for DDL. An index build commits catalog
 * and index files but not the primary tree, so a staged applied-index
 * would not persist with it (src/db-wal.js says the same, at length).
 */
static int do_ddl(dbs *s, const char *coll, uint32_t coll_len, int wreq,
                  const uint8_t *a, size_t a_len,
                  const uint8_t *b, size_t b_len,
                  dbuf *result) {
    dc_wal_plan *p = NULL;
    int e = plan_open(s, NULL, coll, coll_len, wreq,
                      a, (uint32_t)a_len, b, (uint32_t)b_len, 0, NULL, &p);
    if (e == DC_PENDING) return BJ_OK;
    if (e) return e;
    uint32_t clen = 0;
    const uint8_t *cmd = dc_wal_plan_cmd(p, 0, &clen);
    /* index 0 for DDL either way: an index build commits catalog and
     * index files but not the primary tree, so a staged applied-index
     * would not persist with it. dbs_repl_next_index is still consumed,
     * because the caller proposed a command for it and the lists must
     * stay in step. */
    if (dbs_repl_replaying(s)) (void)dbs_repl_next_index(s);
    e = cmd ? dbs_apply(s, 0, cmd, clen, result) : BJ_ERR_STATE;
    plan_close(s, p);
    return e;
}

/* ---- dispatch ----------------------------------------------------------- */

int dbs_handle(dbs *s, uint64_t client, const uint8_t *req, size_t req_len,
               dbuf *out) {
    if (!s || !req || !out) return BJ_ERR_STATE;

    /* Everything from here to the dispatch answers with a response rather
     * than a return: a malformed request is the client's problem and it
     * is owed the sentence saying so. */
    const uint8_t *ops; uint32_t ops_len; int found = 0;
    int e = field_str(req, req_len, "op", &ops, &ops_len, &found);
    if (e) return respond_error(out, DC_ERR_REQ_MALFORMED);
    if (!found) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);

    int op = -1;
    for (size_t i = 0; i < sizeof(OP_NAMES) / sizeof(OP_NAMES[0]); i++) {
        if (ops_len == OP_NAMES[i].len && memcmp(ops, OP_NAMES[i].name, ops_len) == 0) {
            op = (int)OP_NAMES[i].op;
            break;
        }
    }
    if (op < 0) return respond_error(out, DC_ERR_REQ_UNKNOWN_OP);

    /*
     * ping, before anything is resolved: it is the one op that is not
     * about a collection, and its whole job is to be cheap. A client
     * holding a connection warm sends this so the server's idle timeout
     * (server/main.c) does not take its slot back, and it costs the
     * database nothing -- no catalog, no tree, no file.
     */
    if (op == OP_PING) {
        bj_builder *pb = bj_builder_new();
        if (!pb) return BJ_ERR_OOM;
        bj_begin_object(pb);
        PUT_KEY(pb, "ok");   bj_put_bool(pb, 1);
        PUT_KEY(pb, "pong"); bj_put_bool(pb, 1);
        bj_end_object(pb);
        int pe = finish(pb, out);
        bj_builder_free(pb);
        return pe;
    }

    /*
     * getMore and closeCursor name a CURSOR, not a collection: the cursor
     * already knows which collection it is scanning, and asking a client
     * to name it again would be asking it to keep a fact the server
     * already holds -- and to be believed about it.
     */
    if (op == OP_GET_MORE || op == OP_CLOSE_CURSOR) {
        int64_t id = 0;
        int have = 0;
        {
            const uint8_t *v; size_t vlen;
            if ((e = field_raw(req, req_len, "cursor", &v, &vlen, &have)))
                return respond_error(out, DC_ERR_REQ_MALFORMED);
            if (!have) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);
            cur c = { v, vlen, 0 };
            double d;
            if (read_number(&c, &d)) return respond_error(out, DC_ERR_REQ_MALFORMED);
            id = (int64_t)d;
        }
        if (id <= 0) return respond_error(out, DC_ERR_NO_CURSOR);

        if (op == OP_CLOSE_CURSOR) {
            e = dbs_cursor_drop(s, client, (uint64_t)id);
            if (e) return respond_error(out, e);
            bj_builder *kb = bj_builder_new();
            if (!kb) return BJ_ERR_OOM;
            bj_begin_object(kb);
            PUT_KEY(kb, "ok");     bj_put_bool(kb, 1);
            PUT_KEY(kb, "closed"); bj_put_bool(kb, 1);
            bj_end_object(kb);
            int ke = finish(kb, out);
            bj_builder_free(kb);
            return ke;
        }

        dc_cursor *cur_h = NULL; uint32_t batch = 0;
        e = dbs_cursor_get(s, client, (uint64_t)id, &cur_h, &batch);
        if (e) return respond_error(out, e);
        /* A getMore may resize the batch for this call; absent means
         * whatever the find asked for. */
        int64_t want = 0;
        {
            qry_options ignored; int had = 0;
            if ((e = read_opts(req, req_len, &ignored, &had, &want)))
                return respond_error(out, DC_ERR_REQ_MALFORMED);
        }
        if (want <= 0) want = (int64_t)batch;
        e = respond_batch(s, client, (uint64_t)id, cur_h, (uint32_t)want, out);
        if (e) return respond_error(out, e);
        return BJ_OK;
    }

    /*
     * closeStream names a STREAM, and like a cursor's id that is all it
     * needs to name: the stream already knows which collection it
     * watches.
     */
    if (op == OP_CLOSE_STREAM) {
        int64_t id = 0;
        const uint8_t *v; size_t vlen; int have = 0;
        if ((e = field_raw(req, req_len, "stream", &v, &vlen, &have)))
            return respond_error(out, DC_ERR_REQ_MALFORMED);
        if (!have) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);
        { cur c = { v, vlen, 0 }; double d;
          if (read_number(&c, &d)) return respond_error(out, DC_ERR_REQ_MALFORMED);
          id = (int64_t)d; }
        e = dbs_close_stream(s, client, (uint64_t)id);
        if (e) return respond_error(out, e);
        bj_builder *sb = bj_builder_new();
        if (!sb) return BJ_ERR_OOM;
        bj_begin_object(sb);
        PUT_KEY(sb, "ok");     bj_put_bool(sb, 1);
        PUT_KEY(sb, "closed"); bj_put_bool(sb, 1);
        bj_end_object(sb);
        int se = finish(sb, out);
        bj_builder_free(sb);
        return se;
    }

    /*
     * listCollections names no collection -- it is the question you ask
     * when you do not know what there is -- so it is answered here,
     * before `coll` is required, for the same reason ping is.
     */
    if (op == OP_LIST_COLLECTIONS) {
        dbuf names = {0};
        e = dbs_list_collections(s, &names);
        if (e) { dbuf_free(&names); return respond_error(out, e); }
        bj_builder *lb = bj_builder_new();
        if (!lb) { dbuf_free(&names); return BJ_ERR_OOM; }
        bj_begin_object(lb);
        PUT_KEY(lb, "ok"); bj_put_bool(lb, 1);
        PUT_KEY(lb, "collections");
        if (names.len) bj_put_raw(lb, names.data, (uint32_t)names.len); else bj_put_null(lb);
        bj_end_object(lb);
        dbuf_free(&names);
        int le = finish(lb, out);
        bj_builder_free(lb);
        return le;
    }

    const uint8_t *coll; uint32_t coll_len;
    e = field_str(req, req_len, "coll", &coll, &coll_len, &found);
    if (e) return respond_error(out, DC_ERR_REQ_MALFORMED);
    /*
     * compact with no collection named is the SWEEP: every collection,
     * with the three options that are the whole difference between a
     * sweep and a loop (db_session.h). It is answered here because it is
     * the one op whose collection is optional -- naming one asks for
     * that one and hears about it going wrong, naming none asks for all
     * of them and hears what each did.
     */
    if (!found && op == OP_COMPACT) {
        int64_t min_bytes = 0;
        if ((e = field_int(req, req_len, "minBytes", &min_bytes, 0)))
            return respond_error(out, DC_ERR_REQ_MALFORMED);
        double factor = 0;
        {
            const uint8_t *v; size_t vlen; int f = 0;
            if ((e = field_raw(req, req_len, "factor", &v, &vlen, &f)))
                return respond_error(out, DC_ERR_REQ_MALFORMED);
            if (f) {
                cur c = { v, vlen, 0 };
                if (read_number(&c, &factor)) return respond_error(out, DC_ERR_REQ_MALFORMED);
            }
        }
        int skip_busy = 0;
        if ((e = field_flag(req, req_len, "skipBusy", 0, &skip_busy)))
            return respond_error(out, DC_ERR_REQ_MALFORMED);

        dbuf all = {0};
        e = dbs_compact_all(s, min_bytes, factor, skip_busy, &all);
        if (e) { dbuf_free(&all); return respond_error(out, e); }
        bj_builder *ab = bj_builder_new();
        if (!ab) { dbuf_free(&all); return BJ_ERR_OOM; }
        bj_begin_object(ab);
        PUT_KEY(ab, "ok"); bj_put_bool(ab, 1);
        PUT_KEY(ab, "result");
        if (all.len) bj_put_raw(ab, all.data, (uint32_t)all.len); else bj_put_null(ab);
        bj_end_object(ab);
        dbuf_free(&all);
        int ae = finish(ab, out);
        bj_builder_free(ab);
        return ae;
    }
    if (!found) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);

    /*
     * Making and unmaking, before anything is resolved: createCollection
     * is asked precisely when there is nothing to open, and
     * dropCollection has to work whether or not the collection is
     * currently open. listIndexes only reads the catalog.
     *
     * DDL TAKES THE SAME ROUTE A WRITE DOES -- plan the command, then
     * apply it -- for the reason run_write does: every mutation this
     * server performs should be one a log could have carried. It was not
     * true of DDL, which called dbs_* directly and left nothing a
     * follower could be sent (docs/replicaton-roadmap.md step 4: "the
     * single-node 'unlogged DDL is safe' argument dies with the first
     * follower"). Nothing here writes a log yet -- when one exists, the
     * change is to propose the command instead of applying it, in this
     * one place, exactly as it will be for writes.
     *
     * createCollection is the exception, and stays direct: there is no
     * command for it, because an insert makes a collection implicitly
     * and THAT is logged. What does not replicate is a collection that
     * was named and never written to, which carries nothing.
     */
    if (op == OP_CREATE_COLLECTION || op == OP_DROP_COLLECTION ||
        op == OP_LIST_INDEXES || op == OP_DROP_INDEX) {
        bj_builder *sb = NULL;
        if (op == OP_LIST_INDEXES) {
            dbuf list = {0};
            e = dbs_list_indexes(s, (const char *)coll, coll_len, &list);
            if (e) { dbuf_free(&list); return respond_error(out, e); }
            sb = bj_builder_new();
            if (!sb) { dbuf_free(&list); return BJ_ERR_OOM; }
            bj_begin_object(sb);
            PUT_KEY(sb, "ok"); bj_put_bool(sb, 1);
            PUT_KEY(sb, "indexes");
            if (list.len) bj_put_raw(sb, list.data, (uint32_t)list.len); else bj_put_null(sb);
            bj_end_object(sb);
            dbuf_free(&list);
        } else if (op == OP_DROP_INDEX || op == OP_DROP_COLLECTION) {
            const uint8_t *ixn = NULL; uint32_t ixn_len = 0;
            if (op == OP_DROP_INDEX) {
                int have = 0;
                if ((e = field_str(req, req_len, "index", &ixn, &ixn_len, &have)))
                    return respond_error(out, DC_ERR_REQ_MALFORMED);
                if (!have) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);
            }
            dbuf res = {0};
            e = do_ddl(s, (const char *)coll, coll_len,
                       op == OP_DROP_INDEX ? DC_WREQ_DROP_INDEX
                                           : DC_WREQ_DROP_COLLECTION,
                       ixn, ixn_len, NULL, 0, &res);
            if (e) { dbuf_free(&res); return respond_error(out, e); }
            /* `dropped` is the applier's, spliced through: what a command
             * DID is part of the command's meaning, and under replication
             * every replica computes it. */
            const uint8_t *v; size_t vlen; int f = 0;
            e = obj_get_field(res.data, res.len, (const uint8_t *)"dropped", 7,
                              &v, &vlen, &f);
            if (e || !f) { dbuf_free(&res); return respond_error(out, BJ_ERR_STATE); }
            sb = bj_builder_new();
            if (!sb) { dbuf_free(&res); return BJ_ERR_OOM; }
            bj_begin_object(sb);
            PUT_KEY(sb, "ok");      bj_put_bool(sb, 1);
            PUT_KEY(sb, "dropped"); bj_put_raw(sb, v, (uint32_t)vlen);
            bj_end_object(sb);
            dbuf_free(&res);
        } else {
            int did = 0;
            e = dbs_create_collection(s, (const char *)coll, coll_len, &did);
            if (e) return respond_error(out, e);
            sb = bj_builder_new();
            if (!sb) return BJ_ERR_OOM;
            bj_begin_object(sb);
            PUT_KEY(sb, "ok");      bj_put_bool(sb, 1);
            PUT_KEY(sb, "created"); bj_put_bool(sb, did);
            bj_end_object(sb);
        }
        int se = finish(sb, out);
        bj_builder_free(sb);
        return se;
    }

    /*
     * bulkWrite parses BEFORE it resolves, which is why it is here rather
     * than in the switch below. The parse is what says whether this list
     * inserts anything, and that is what decides whether a name with no
     * collection behind it should get one.
     */
    if (op == OP_BULK_WRITE) {
        const uint8_t *writes; size_t writes_len; int have = 0;
        if ((e = field_raw(req, req_len, "writes", &writes, &writes_len, &have)))
            return respond_error(out, DC_ERR_REQ_MALFORMED);
        if (!have) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);
        int ordered = 1;
        if ((e = field_flag(req, req_len, "ordered", 1, &ordered)))
            return respond_error(out, DC_ERR_REQ_MALFORMED);
        int64_t bulk_now = 0;
        int bulk_have_now = 0;
        if ((e = field_ms(req, req_len, &bulk_now, &bulk_have_now)))
            return respond_error(out, DC_ERR_REQ_MALFORMED);

        dbuf types = {0};
        int bad = -1;
        e = dc_bulk_parse(writes, writes_len, &types, &bad);
        if (e) { dbuf_free(&types); return respond_error_at(out, e, bad); }

        /* Does this list insert? Only then does a missing collection get
         * made -- a bulkWrite of nothing but deletes against a name that
         * does not exist is a typo, exactly as a find of one is. */
        int inserts = 0;
        {
            cur tc = { types.data, types.len, 0 };
            uint32_t n = 0;
            if (!array_begin(&tc, &n)) {
                for (uint32_t i = 0; i < n; i++) {
                    double d;
                    if (read_number(&tc, &d)) break;
                    if ((int)d == DC_BULK_INSERT_ONE) { inserts = 1; break; }
                }
            }
        }

        dc_collection *bc = NULL;
        e = dbs_collection(s, (const char *)coll, coll_len, &bc);
        if (e == DC_ERR_NO_COLLECTION && inserts) {
            int made = 0;
            e = dbs_create_collection(s, (const char *)coll, coll_len, &made);
            if (!e) e = dbs_collection(s, (const char *)coll, coll_len, &bc);
        }
        if (!e) e = do_bulk_write(s, bc, (const char *)coll, coll_len,
                                  writes, writes_len, types.data, types.len,
                                  ordered, bulk_now, bulk_have_now, out);
        dbuf_free(&types);
        if (e) return respond_error(out, e);
        return BJ_OK;
    }

    /*
     * watch resolves nothing. A collection that does not exist yet is a
     * perfectly good thing to watch -- the first insert makes it, and
     * that insert is an event the watcher wants. Opening it here would
     * refuse exactly the case a change stream is most useful for.
     */
    if (op == OP_WATCH) {
        uint64_t id = 0;
        e = dbs_watch(s, client, (const char *)coll, coll_len, &id);
        if (e) return respond_error(out, e);
        bj_builder *wb = bj_builder_new();
        if (!wb) return BJ_ERR_OOM;
        bj_begin_object(wb);
        PUT_KEY(wb, "ok");     bj_put_bool(wb, 1);
        PUT_KEY(wb, "stream"); bj_put_int(wb, (int64_t)id);
        bj_end_object(wb);
        int we = finish(wb, out);
        bj_builder_free(wb);
        return we;
    }

    dc_collection *c = NULL;
    e = dbs_collection(s, (const char *)coll, coll_len, &c);
    /*
     * A first insert makes the collection, the way it does in every host
     * of this library and in the database this is shaped after. Only an
     * insert: a count or a find of a name that does not exist is far
     * more likely to be a typo than an intention, and answering "no such
     * collection" is more useful than answering nothing from a
     * collection this just created on their behalf.
     */
    if (e == DC_ERR_NO_COLLECTION && (op == OP_INSERT || op == OP_INSERT_MANY)) {
        int made = 0;
        int ce = dbs_create_collection(s, (const char *)coll, coll_len, &made);
        if (ce) return respond_error(out, ce);
        e = dbs_collection(s, (const char *)coll, coll_len, &c);
    }
    if (e) return respond_error(out, e);

    /* The three raw spans an op may need, read once. An empty filter is
     * the empty object, which is what "everything" means everywhere else
     * in this library. */
    /* `{}`: the tag, the u32 SIZE OF EVERYTHING AFTER THE FIRST FIVE
     * BYTES (so 4 -- the count alone), then the count. It said 9 until a
     * command carried one: nothing had ever MEASURED it, because
     * object_begin reads the count and ignores the size, so a filter
     * built from it worked while a value spliced into another object
     * made its container run four bytes past its own end. */
    static const uint8_t EMPTY_OBJ[9] = { BJ_TYPE_OBJECT, 4,0,0,0, 0,0,0,0 };
    const uint8_t *filter = EMPTY_OBJ; size_t filter_len = sizeof EMPTY_OBJ;
    const uint8_t *doc = NULL;  size_t doc_len = 0;
    const uint8_t *upd = NULL;  size_t upd_len = 0;
    int has = 0;
    if ((e = field_raw(req, req_len, "filter", &filter, &filter_len, &has)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);
    if (!has) { filter = EMPTY_OBJ; filter_len = sizeof EMPTY_OBJ; }
    if ((e = field_raw(req, req_len, "doc", &doc, &doc_len, &has)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);
    if (!has) doc = NULL;
    if ((e = field_raw(req, req_len, "update", &upd, &upd_len, &has)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);
    if (!has) upd = NULL;

    uint8_t id[12];
    int have_id = 0;
    memset(id, 0, sizeof id);
    if ((e = field_id(req, req_len, id, &have_id)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);
    int upsert = 0;
    if ((e = field_flag(req, req_len, "upsert", 0, &upsert)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);
    int64_t now_ms = 0;
    int have_now = 0;
    if ((e = field_ms(req, req_len, &now_ms, &have_now)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);

    dbuf body = {0};
    dbuf dates = {0};       /* a $currentDate rewrite, when there was one */
    const char *body_key = NULL;
    int64_t number = 0;
    int is_number = 0, is_bool_found = 0, found_doc = 0;

    switch (op) {
        case OP_FIND: {
            qry_options qo; int have_opts = 0; int64_t batch = 0;
            if ((e = read_opts(req, req_len, &qo, &have_opts, &batch))) break;

            /*
             * batchSize asks for a cursor. A SORTED find cannot have one:
             * an arbitrary sort needs every match before it can emit the
             * first ordered result, which is why dc_cursor_open has no
             * sort parameter and why the in-process cursor
             * (wasm/nisaba-wasm.js) refuses next() on a sorted find too.
             * Three layers, one rule, said once each -- rather than this
             * one quietly materialising the lot and calling it a cursor.
             */
            if (batch > 0) {
                if (have_opts && qo.sort) { e = DC_ERR_CURSOR_SORTED; break; }
                dc_cursor *cur_h = NULL;
                e = dc_cursor_open(c, filter, (uint32_t)filter_len,
                                   have_opts ? qo.projection : NULL,
                                   have_opts ? qo.projection_len : 0,
                                   have_opts ? qo.skip : 0,
                                   have_opts ? qo.limit : 0, &cur_h);
                if (e) break;
                uint64_t id = 0;
                e = dbs_cursor_add(s, client, cur_h, (uint32_t)batch, &id);
                if (e) { dc_cursor_close(cur_h); break; }
                e = respond_batch(s, client, id, cur_h, (uint32_t)batch, out);
                if (e) {
                    dbs_cursor_drop(s, client, id);
                    break;
                }
                dbuf_free(&body); dbuf_free(&dates);
                return BJ_OK;   /* respond_batch wrote the whole response */
            }

            uint8_t *docs = NULL; size_t docs_len = 0;
            e = dc_find(c, filter, (uint32_t)filter_len,
                        have_opts ? &qo : NULL, &docs, &docs_len);
            if (e) break;
            e = dbuf_put(&body, docs, docs_len);
            free(docs);
            body_key = "docs";
            break;
        }
        case OP_FIND_ONE: {
            uint8_t *d = NULL; size_t dlen = 0;
            e = dc_find_one(c, filter, (uint32_t)filter_len, NULL, 0,
                            &found_doc, &d, &dlen);
            if (e) break;
            if (found_doc) e = dbuf_put(&body, d, dlen);
            free(d);
            body_key = "doc";
            is_bool_found = 1;
            break;
        }
        case OP_COUNT: {
            int64_t n = 0;
            e = dc_count(c, filter, (uint32_t)filter_len, &n);
            if (e) break;
            number = n; is_number = 1; body_key = "n";
            break;
        }
        case OP_DISTINCT: {
            const uint8_t *field; uint32_t field_len; int f = 0;
            if ((e = field_str(req, req_len, "field", &field, &field_len, &f))) break;
            if (!f) { e = DC_ERR_REQ_MISSING_FIELD; break; }
            uint8_t *vals = NULL; size_t vals_len = 0;
            e = dc_distinct(c, (const char *)field, (int)field_len,
                            filter, (uint32_t)filter_len, &vals, &vals_len);
            if (e) break;
            e = dbuf_put(&body, vals, vals_len);
            free(vals);
            body_key = "values";
            break;
        }
        case OP_CREATE_INDEX: {
            const uint8_t *keys; size_t keys_len; int have = 0;
            if ((e = field_raw(req, req_len, "keys", &keys, &keys_len, &have))) break;
            if (!have) { e = DC_ERR_REQ_MISSING_FIELD; break; }
            const uint8_t *opt = NULL; size_t opt_len = 0; int has_opt = 0;
            if ((e = field_raw(req, req_len, "options", &opt, &opt_len, &has_opt))) break;
            /* Absent options are the empty object, not an absent field: a
             * command carries what it needs to be performed anywhere, and
             * a missing `options` would be a command a replica had to
             * guess at. */
            dbuf res = {0};
            e = do_ddl(s, (const char *)coll, coll_len, DC_WREQ_CREATE_INDEX,
                       keys, keys_len,
                       has_opt ? opt : EMPTY_OBJ,
                       has_opt ? opt_len : sizeof EMPTY_OBJ, &res);
            if (e) { dbuf_free(&res); break; }
            /* The chosen name, spliced in under `name` below -- the whole
             * answer, and the same thing the in-process createIndex
             * returns. It is the APPLIER's, because which name the
             * catalog chose is part of what the command did. */
            const uint8_t *v; size_t vlen; int f = 0;
            e = obj_get_field(res.data, res.len, (const uint8_t *)"name", 4,
                              &v, &vlen, &f);
            if (!e && !f) e = BJ_ERR_STATE;
            if (!e) e = dbuf_put(&body, v, vlen);
            dbuf_free(&res);
            body_key = "name";
            break;
        }
        case OP_COMPACT: {
            /*
             * The one op that is not a query or a write: it replaces the
             * collection's files. It is here rather than in the
             * transport because it is a REQUEST -- a client asks, and
             * gets the same {ok:false, code, msg} refusal shape as any
             * other request when a cursor is in the way (-49).
             */
            dbs_compact_stats st;
            e = dbs_compact(s, (const char *)coll, coll_len, &st);
            if (e) break;
            bj_builder *cb = bj_builder_new();
            if (!cb) { e = BJ_ERR_OOM; break; }
            bj_begin_object(cb);
            PUT_KEY(cb, "generation");  bj_put_int(cb, st.generation);
            PUT_KEY(cb, "bytesBefore"); bj_put_int(cb, (int64_t)st.bytes_before);
            PUT_KEY(cb, "bytesAfter");  bj_put_int(cb, (int64_t)st.bytes_after);
            PUT_KEY(cb, "bytesFreed");
            bj_put_int(cb, st.bytes_before > st.bytes_after
                           ? (int64_t)(st.bytes_before - st.bytes_after) : 0);
            bj_end_object(cb);
            size_t clen = 0;
            const uint8_t *cdata = bj_builder_data(cb, &clen);
            e = cdata ? dbuf_put(&body, cdata, clen) : BJ_ERR_OOM;
            bj_builder_free(cb);
            body_key = "result";
            break;
        }
        case OP_FIND_ONE_AND_UPDATE:
        case OP_FIND_ONE_AND_REPLACE:
        case OP_FIND_ONE_AND_DELETE: {
            /*
             * One document, read and written by one request, answering
             * with the document rather than a count -- which is the
             * whole point of the family: updateOne says how many changed,
             * not which, so getting it back otherwise means a second
             * query with a gap in the middle.
             *
             * The BEFORE image costs nothing: the planner already read
             * the document to resolve the target, and run_write now
             * keeps what it read. The AFTER image is one read back by
             * the id the plan resolved -- a bpt_search, not a scan.
             */
            int wreq;
            const uint8_t *arg = NULL; uint32_t arg_len = 0;
            if (op == OP_FIND_ONE_AND_UPDATE) {
                if (!upd) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                const uint8_t *ub; size_t ub_len;
                if ((e = resolve_dates(upd, upd_len, now_ms, have_now,
                                       &dates, &ub, &ub_len))) break;
                wreq = DC_WREQ_UPDATE_ONE;
                arg = ub; arg_len = (uint32_t)ub_len;
            } else if (op == OP_FIND_ONE_AND_REPLACE) {
                if (!doc) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                wreq = DC_WREQ_REPLACE_ONE;
                arg = doc; arg_len = (uint32_t)doc_len;
            } else {
                wreq = DC_WREQ_DELETE_ONE;
            }
            /* An upsert that matches nothing inserts, and an insert needs
             * the caller's 12 bytes. */
            if (upsert && !have_id) { e = DC_ERR_REQ_MISSING_FIELD; break; }

            /* Which image to answer with. A delete has only one: the
             * document is gone, so there is no `after` to return, and
             * asking for one is not an error -- it is the same question. */
            int return_new = 0;
            if ((e = field_flag(req, req_len, "returnNew", 0, &return_new))) break;
            if (op == OP_FIND_ONE_AND_DELETE) return_new = 0;

            write_result wr;
            dbuf pre = {0};
            e = run_write(s, c, (const char *)coll, coll_len, wreq,
                          filter, (uint32_t)filter_len, arg, arg_len,
                          upsert, id, &wr, return_new ? NULL : &pre);
            if (e) { dbuf_free(&pre); break; }

            is_bool_found = 1;
            body_key = "doc";
            if (wr.outcome == DC_PLAN_NOTHING) {
                found_doc = 0;           /* nothing matched, nothing written */
            } else if (return_new) {
                if (wr.has_target_id) {
                    dbuf idf = {0};
                    if ((e = id_filter(wr.target_id, &idf))) { dbuf_free(&idf); break; }
                    uint8_t *d = NULL; size_t dlen = 0; int got = 0;
                    e = dc_find_one(c, idf.data, (uint32_t)idf.len, NULL, 0, &got, &d, &dlen);
                    dbuf_free(&idf);
                    if (!e && got) { e = dbuf_put(&body, d, dlen); found_doc = 1; }
                    free(d);
                }
            } else if (pre.len) {
                e = dbuf_put(&body, pre.data, pre.len);
                found_doc = 1;
            } else {
                /* An upsert has no prior state to show, so `before` is
                 * null -- which is what MongoDB answers too. */
                found_doc = 0;
            }
            dbuf_free(&pre);
            break;
        }
        case OP_PRUNE_EXPIRED: {
            /*
             * The TTL sweep. Expiry is not a background thread here --
             * the engine runs no timers and reads no clock -- so it is a
             * sweep somebody asks for, with their own `now`.
             *
             * Which indexes expire what, and the cutoff arithmetic, are
             * dbs_ttl_filters' (db_ttl.h owns the policy, the session
             * owns the catalog that knows the indexes). What is left
             * here is what every host does differently: the deleting,
             * which goes through the same plan/apply path as any other
             * write, so a swept document is one a log could have carried.
             */
            if (!have_now) { e = DC_ERR_REQ_MISSING_FIELD; break; }
            dbuf filters = {0};
            if ((e = dbs_ttl_filters(s, (const char *)coll, coll_len, now_ms, &filters))) {
                dbuf_free(&filters);
                break;
            }
            cur fc = { filters.data, filters.len, 0 };
            uint32_t nf = 0;
            e = array_begin(&fc, &nf);
            int64_t deleted = 0;
            for (uint32_t i = 0; !e && i < nf; i++) {
                size_t start = fc.pos;
                if ((e = skip_value(&fc))) break;
                write_result wr;
                e = run_write(s, c, (const char *)coll, coll_len, DC_WREQ_DELETE_MANY,
                              filters.data + start, (uint32_t)(fc.pos - start),
                              NULL, 0, 0, id, &wr, NULL);
                if (!e) deleted += wr.deleted;
            }
            dbuf_free(&filters);
            if (e) break;
            number = deleted; is_number = 1; body_key = "deletedCount";
            break;
        }
        case OP_FIND_BY_INDEX: {
            /*
             * The lookup that names its index instead of describing what
             * it wants: an O(log n + k) range scan of that index, with
             * no planner in the way. Whether the index exists, is the
             * right kind, and was given one value per field are all the
             * collection's to answer -- three distinct codes, because a
             * client on the far end of a socket cannot guess which of
             * the three it got wrong.
             */
            const uint8_t *ixn; uint32_t ixn_len; int have = 0;
            if ((e = field_str(req, req_len, "index", &ixn, &ixn_len, &have))) break;
            if (!have) { e = DC_ERR_REQ_MISSING_FIELD; break; }
            const uint8_t *vals; size_t vals_len; int hasv = 0;
            if ((e = field_raw(req, req_len, "values", &vals, &vals_len, &hasv))) break;
            if (!hasv) { e = DC_ERR_REQ_MISSING_FIELD; break; }

            uint8_t *docs = NULL; size_t docs_len = 0;
            e = dc_collection_find_by_index(c, (const char *)ixn, (int)ixn_len,
                                            vals, (uint32_t)vals_len, &docs, &docs_len);
            if (e) break;
            e = dbuf_put(&body, docs, docs_len);
            free(docs);
            body_key = "docs";
            break;
        }
        case OP_EXPLAIN: {
            /*
             * Which source the dispatch WOULD use, without running
             * anything: dc_explain consults the very planners the
             * queries consult, so a report cannot drift from what
             * actually happens -- and the name of the plan is C's too,
             * so this server and an in-process host cannot describe one
             * plan two ways.
             */
            int kind = 0; uint8_t *name = NULL; size_t name_len = 0;
            e = dc_explain(c, filter, (uint32_t)filter_len, &kind, &name, &name_len);
            if (e) break;
            const char *source = dc_explain_source(kind);
            bj_builder *xb = bj_builder_new();
            if (!xb) { free(name); e = BJ_ERR_OOM; break; }
            bj_begin_object(xb);
            PUT_KEY(xb, "source");
            bj_put_string(xb, (const uint8_t *)source, (uint32_t)strlen(source));
            PUT_KEY(xb, "index");
            if (name) bj_put_string(xb, name, (uint32_t)name_len); else bj_put_null(xb);
            bj_end_object(xb);
            free(name);
            size_t xlen = 0;
            const uint8_t *xdata = bj_builder_data(xb, &xlen);
            e = xdata ? dbuf_put(&body, xdata, xlen) : BJ_ERR_OOM;
            bj_builder_free(xb);
            body_key = "plan";
            break;
        }
        case OP_AGGREGATE: {
            /*
             * The whole pipeline in one call, including the decision to
             * push a leading $match into the scan so the planner and any
             * index can serve it -- db_agg.h owns that, as it does for
             * every other host. Nothing here knows a stage name.
             *
             * One frame, no cursor. A pipeline is not a scan that can be
             * resumed: $sort and $group need every match before the first
             * result exists, which is the same reason a sorted find
             * cannot be batched. The server holds the result no longer
             * than it takes to write it out.
             */
            const uint8_t *stages; size_t stages_len; int have = 0;
            if ((e = field_raw(req, req_len, "stages", &stages, &stages_len, &have))) break;
            if (!have) { e = DC_ERR_REQ_MISSING_FIELD; break; }

            uint8_t *docs = NULL; size_t docs_len = 0;
            int bad = -1;
            e = dc_aggregate(c, stages, stages_len, &bad, &docs, &docs_len);
            if (e) {
                /* Which stage, named the way a malformed bulkWrite names
                 * its operation: a list has positions, and the client is
                 * holding the list. */
                free(docs);
                dbuf_free(&body); dbuf_free(&dates);
                return respond_error_at(out, e, bad);
            }
            e = dbuf_put(&body, docs, docs_len);
            free(docs);
            body_key = "docs";
            break;
        }
        case OP_INSERT_MANY: {
            const uint8_t *docs_v; size_t docs_vlen; int have = 0;
            if ((e = field_raw(req, req_len, "docs", &docs_v, &docs_vlen, &have))) break;
            if (!have) { e = DC_ERR_REQ_MISSING_FIELD; break; }
            int ordered = 1;
            if ((e = field_flag(req, req_len, "ordered", 1, &ordered))) break;
            e = do_insert_many(s, c, (const char *)coll, coll_len,
                               docs_v, docs_vlen, ordered, out);
            if (e) break;
            dbuf_free(&body); dbuf_free(&dates);
            return BJ_OK;   /* do_insert_many wrote the whole response */
        }
        case OP_INSERT:
        case OP_UPDATE:
        case OP_UPDATE_MANY:
        case OP_REPLACE:
        case OP_DELETE:
        case OP_DELETE_MANY: {
            int wreq;
            const uint8_t *a = filter; uint32_t a_len = (uint32_t)filter_len;
            const uint8_t *b = NULL;   uint32_t b_len = 0;
            switch (op) {
                case OP_INSERT:
                    if (!doc) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                    wreq = DC_WREQ_INSERT_ONE; a = doc; a_len = (uint32_t)doc_len;
                    break;
                case OP_UPDATE:
                case OP_UPDATE_MANY: {
                    if (!upd) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                    wreq = (op == OP_UPDATE) ? DC_WREQ_UPDATE_ONE : DC_WREQ_UPDATE_MANY;
                    const uint8_t *ub; size_t ub_len;
                    if ((e = resolve_dates(upd, upd_len, now_ms, have_now,
                                           &dates, &ub, &ub_len))) break;
                    b = ub; b_len = (uint32_t)ub_len;
                    break;
                }
                case OP_REPLACE:
                    if (!doc) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                    wreq = DC_WREQ_REPLACE_ONE; b = doc; b_len = (uint32_t)doc_len;
                    break;
                default:
                    wreq = (op == OP_DELETE) ? DC_WREQ_DELETE_ONE : DC_WREQ_DELETE_MANY;
                    break;
            }
            if (e) break;
            /*
             * An insert's document carries its own _id -- the same rule
             * insertMany applies to every member of its list, and for
             * the same reason: a document's identity is in the document,
             * and inventing one here would need a clock (db.h's top
             * comment). `id` is NOT an alternative place to put it. It
             * answers the other question, the one only a matching write
             * asks: an upsert cannot know whether it needs an id until
             * it has matched, so it is handed one in advance.
             *
             * Checked here so the refusal names the request's own
             * missing field. dc_wal_plan_build refuses it too
             * (DC_ERR_WAL_NO_ID), because it is reachable from hosts
             * that never come through this file -- but a client that
             * reaches this one is told in the vocabulary of the wire.
             */
            if (op == OP_INSERT) {
                const uint8_t *v; size_t vlen; int f = 0;
                if (!doc ||
                    obj_get_field(doc, doc_len, (const uint8_t *)"_id", 3,
                                  &v, &vlen, &f) || !f) {
                    e = DC_ERR_REQ_MISSING_FIELD;
                    break;
                }
            } else if (!have_id && upsert) {
                e = DC_ERR_REQ_MISSING_FIELD;
                break;
            }
            e = do_write(s, c, (const char *)coll, coll_len, wreq,
                         a, a_len, b, b_len, upsert, id, &body);
            body_key = "result";
            break;
        }
        default:
            e = DC_ERR_REQ_UNKNOWN_OP;
            break;
    }

    if (e) {
        dbuf_free(&body); dbuf_free(&dates);
        return respond_error(out, e);
    }

    bj_builder *rb = bj_builder_new();
    if (!rb) { dbuf_free(&body); dbuf_free(&dates); return BJ_ERR_OOM; }
    bj_begin_object(rb);
    PUT_KEY(rb, "ok"); bj_put_bool(rb, 1);
    if (is_number) {
        PUT_KEY(rb, body_key); bj_put_int(rb, number);
    } else if (is_bool_found) {
        PUT_KEY(rb, "found"); bj_put_bool(rb, found_doc);
        PUT_KEY(rb, body_key);
        if (found_doc && body.len) bj_put_raw(rb, body.data, (uint32_t)body.len);
        else bj_put_null(rb);
    } else {
        PUT_KEY(rb, body_key);
        if (body.len) bj_put_raw(rb, body.data, (uint32_t)body.len);
        else bj_put_null(rb);
    }
    bj_end_object(rb);
    e = finish(rb, out);
    bj_builder_free(rb);
    dbuf_free(&body); dbuf_free(&dates);
    return e;
}
