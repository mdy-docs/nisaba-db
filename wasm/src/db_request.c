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
#include "db_validate.h"
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
    OP_KILL_CURSOR,
    OP_COMPACT
} dbs_op;

static const struct { const char *name; uint32_t len; dbs_op op; } OP_NAMES[] = {
    { "ping",       4, OP_PING        },
    { "find",       4, OP_FIND        },
    { "findOne",    7, OP_FIND_ONE    },
    { "count",      5, OP_COUNT       },
    { "distinct",   8, OP_DISTINCT    },
    { "insert",     6, OP_INSERT      },
    { "update",     6, OP_UPDATE      },
    { "updateMany",10, OP_UPDATE_MANY },
    { "replace",    7, OP_REPLACE     },
    { "delete",     6, OP_DELETE      },
    { "deleteMany",10, OP_DELETE_MANY },
    { "getMore",    7, OP_GET_MORE    },
    { "killCursor",10, OP_KILL_CURSOR },
    { "compact",    7, OP_COMPACT     },
};

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

static int field_flag(const uint8_t *o, size_t olen, const char *key, int *out) {
    const uint8_t *v; size_t vlen; int found = 0;
    *out = 0;
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

int dbs_refusal(int code, dbuf *out) {
    if (!out) return BJ_ERR_STATE;
    return respond_error(out, code);
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

/* Plan a write, apply every command it produced, and answer with one
 * result. Errors are the caller's to turn into a response. */
static int do_write(dc_collection *c, const char *coll, uint32_t coll_len,
                    int wreq, const uint8_t *a, uint32_t a_len,
                    const uint8_t *b, uint32_t b_len,
                    int upsert, const uint8_t id[12], dbuf *out) {
    dc_wal_plan *p = NULL;
    int e = dc_wal_plan_build(c, coll, coll_len, wreq, a, a_len, b, b_len,
                              upsert, id, &p);
    if (e) return e;

    int64_t matched = 0, modified = 0, deleted = 0, inserted = 0;
    const uint8_t *inserted_id = NULL;
    uint32_t n = dc_wal_plan_count(p);
    dbuf one = {0};

    for (uint32_t i = 0; i < n; i++) {
        uint32_t clen = 0;
        const uint8_t *cmd = dc_wal_plan_cmd(p, i, &clen);
        if (!cmd) { e = BJ_ERR_STATE; goto done; }
        one.len = 0;
        /* index 0: this collection is not log-driven here, so the applied
         * index is not staged. A replicated server passes the log index
         * it committed at, and dc_wal_apply stages it with the mutation. */
        e = dc_wal_apply(c, 0, cmd, clen, &one);
        if (e) goto done;
        accumulate(one.data, one.len, &matched, &modified, &deleted);
        const uint8_t *v; size_t vlen; int found = 0;
        if (!obj_get_field(one.data, one.len, (const uint8_t *)"insertedId", 10,
                           &v, &vlen, &found) && found) {
            inserted++;
        }
    }
    if (dc_wal_plan_outcome(p) == DC_PLAN_UPSERT) inserted_id = dc_wal_plan_target_id(p);

    {
        bj_builder *rb = bj_builder_new();
        if (!rb) { e = BJ_ERR_OOM; goto done; }
        bj_begin_object(rb);
        PUT_KEY(rb, "acknowledged"); bj_put_bool(rb, 1);
        PUT_KEY(rb, "matchedCount");  bj_put_int(rb, matched);
        PUT_KEY(rb, "modifiedCount"); bj_put_int(rb, modified);
        PUT_KEY(rb, "deletedCount");  bj_put_int(rb, deleted);
        PUT_KEY(rb, "insertedCount"); bj_put_int(rb, inserted);
        PUT_KEY(rb, "upsertedId");
        if (inserted_id) bj_put_oid(rb, inserted_id); else bj_put_null(rb);
        bj_end_object(rb);
        size_t rlen = 0;
        const uint8_t *rdata = bj_builder_data(rb, &rlen);
        e = rdata ? dbuf_put(out, rdata, rlen) : BJ_ERR_OOM;
        bj_builder_free(rb);
    }

done:
    dbuf_free(&one);
    dc_wal_plan_free(p);
    return e;
}

/*
 * One batch out of an open cursor, and the id to ask for the next one --
 * or null, which is how a client learns the scan is over without a
 * second round trip to find out. A drained cursor is closed HERE rather
 * than waiting for a killCursor that a well-behaved client would have no
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
     * getMore and killCursor name a CURSOR, not a collection: the cursor
     * already knows which collection it is scanning, and asking a client
     * to name it again would be asking it to keep a fact the server
     * already holds -- and to be believed about it.
     */
    if (op == OP_GET_MORE || op == OP_KILL_CURSOR) {
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

        if (op == OP_KILL_CURSOR) {
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

    const uint8_t *coll; uint32_t coll_len;
    e = field_str(req, req_len, "coll", &coll, &coll_len, &found);
    if (e) return respond_error(out, DC_ERR_REQ_MALFORMED);
    if (!found) return respond_error(out, DC_ERR_REQ_MISSING_FIELD);

    dc_collection *c = NULL;
    e = dbs_collection(s, (const char *)coll, coll_len, &c);
    if (e) return respond_error(out, e);

    /* The three raw spans an op may need, read once. An empty filter is
     * the empty object, which is what "everything" means everywhere else
     * in this library. */
    static const uint8_t EMPTY_OBJ[9] = { BJ_TYPE_OBJECT, 9,0,0,0, 0,0,0,0 };
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
    if ((e = field_flag(req, req_len, "upsert", &upsert)))
        return respond_error(out, DC_ERR_REQ_MALFORMED);

    dbuf body = {0};
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
                dbuf_free(&body);
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
                case OP_UPDATE_MANY:
                    if (!upd) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                    wreq = (op == OP_UPDATE) ? DC_WREQ_UPDATE_ONE : DC_WREQ_UPDATE_MANY;
                    b = upd; b_len = (uint32_t)upd_len;
                    break;
                case OP_REPLACE:
                    if (!doc) { e = DC_ERR_REQ_MISSING_FIELD; break; }
                    wreq = DC_WREQ_REPLACE_ONE; b = doc; b_len = (uint32_t)doc_len;
                    break;
                default:
                    wreq = (op == OP_DELETE) ? DC_WREQ_DELETE_ONE : DC_WREQ_DELETE_MANY;
                    break;
            }
            if (e) break;
            /* An insert whose document carries no _id, or an upsert with
             * nothing to match, needs the 12 bytes the client was asked
             * to supply. Inventing one here would need a clock. */
            if (!have_id && (op == OP_INSERT || upsert)) {
                int needs = 1;
                if (op == OP_INSERT && doc) {
                    const uint8_t *v; size_t vlen; int f = 0;
                    if (!obj_get_field(doc, doc_len, (const uint8_t *)"_id", 3,
                                       &v, &vlen, &f) && f)
                        needs = 0;
                }
                if (needs) { e = DC_ERR_REQ_MISSING_FIELD; break; }
            }
            e = do_write(c, (const char *)coll, coll_len, wreq,
                         a, a_len, b, b_len, upsert, id, &body);
            body_key = "result";
            break;
        }
        default:
            e = DC_ERR_REQ_UNKNOWN_OP;
            break;
    }

    if (e) {
        dbuf_free(&body);
        return respond_error(out, e);
    }

    bj_builder *rb = bj_builder_new();
    if (!rb) { dbuf_free(&body); return BJ_ERR_OOM; }
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
    dbuf_free(&body);
    return e;
}
