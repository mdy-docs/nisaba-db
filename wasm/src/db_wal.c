/*
 * db_wal.c — the WAL command grammar and planner. See db_wal.h for what
 * moved here and why the opcode set got smaller in the process.
 */
#include "db_wal.h"
#include "db_query.h"
#include "db_update.h"
#include "bjcursor.h"
#include "dbuf.h"

#include <stdlib.h>
#include <string.h>

/* The wire spelling of each opcode, indexed by dc_wal_op. The only place
 * in the repository these strings exist -- the host dispatches on the
 * enum, and every command is built below. */
static const char *const OP_NAME[] = {
    "i", "u", "r", "d", "createIndex", "dropIndex", "dropCollection"
};
#define OP_COUNT ((int)(sizeof(OP_NAME) / sizeof(OP_NAME[0])))

/* ---------------------------------------------------------------- plan */

struct dc_wal_plan {
    dbuf cmds;                            /* every command, concatenated */
    struct { size_t off, len; } *at;      /* one span into cmds each     */
    uint32_t n, cap;
    int outcome;
    uint8_t target[12];
    int has_target;
    uint8_t *preimage;
    size_t preimage_len;
};

static int plan_push(dc_wal_plan *p, const uint8_t *cmd, size_t len) {
    if (p->n == p->cap) {
        uint32_t nc = p->cap ? p->cap * 2 : 8;
        void *na = realloc(p->at, (size_t)nc * sizeof(*p->at));
        if (!na) return BJ_ERR_OOM;
        p->at = na;
        p->cap = nc;
    }
    size_t off = p->cmds.len;
    int e = dbuf_put(&p->cmds, cmd, len);
    if (e) return e;
    p->at[p->n].off = off;
    p->at[p->n].len = len;
    p->n++;
    return BJ_OK;
}

/*
 * Build one command and append it to the plan. `id` (12 bytes or NULL),
 * `a` and `b` carry whatever the opcode's shape needs -- see the table in
 * db_wal.h. Field order is c, op, then the payload, matching what
 * src/db-wal.js used to encode so an existing log still decodes the same
 * way for the opcodes that survived.
 */
static int emit(dc_wal_plan *p, dc_wal_op op,
                const char *coll, uint32_t coll_len,
                const uint8_t *id,
                const uint8_t *a, size_t a_len,
                const uint8_t *b, size_t b_len) {
    bj_builder *bd = bj_builder_new();
    if (!bd) return BJ_ERR_OOM;

    const char *name = OP_NAME[op];
    int e = bj_begin_object(bd);
    if (!e) e = bj_put_key(bd, (const uint8_t *)"c", 1);
    if (!e) e = bj_put_string(bd, (const uint8_t *)coll, coll_len);
    if (!e) e = bj_put_key(bd, (const uint8_t *)"op", 2);
    if (!e) e = bj_put_string(bd, (const uint8_t *)name, (uint32_t)strlen(name));

    switch (op) {
        case DC_WAL_INSERT:
            if (!e) e = bj_put_key(bd, (const uint8_t *)"doc", 3);
            if (!e) e = bj_put_raw(bd, a, (uint32_t)a_len);
            break;
        case DC_WAL_UPDATE:
            if (!e) e = bj_put_key(bd, (const uint8_t *)"id", 2);
            if (!e) e = bj_put_oid(bd, id);
            if (!e) e = bj_put_key(bd, (const uint8_t *)"update", 6);
            if (!e) e = bj_put_raw(bd, b, (uint32_t)b_len);
            break;
        case DC_WAL_REPLACE:
            if (!e) e = bj_put_key(bd, (const uint8_t *)"id", 2);
            if (!e) e = bj_put_oid(bd, id);
            if (!e) e = bj_put_key(bd, (const uint8_t *)"doc", 3);
            if (!e) e = bj_put_raw(bd, b, (uint32_t)b_len);
            break;
        case DC_WAL_DELETE:
            if (!e) e = bj_put_key(bd, (const uint8_t *)"id", 2);
            if (!e) e = bj_put_oid(bd, id);
            break;
        case DC_WAL_CREATE_INDEX:
            if (!e) e = bj_put_key(bd, (const uint8_t *)"keys", 4);
            if (!e) e = bj_put_raw(bd, a, (uint32_t)a_len);
            if (!e) e = bj_put_key(bd, (const uint8_t *)"options", 7);
            if (!e) e = bj_put_raw(bd, b, (uint32_t)b_len);
            break;
        case DC_WAL_DROP_INDEX:
            if (!e) e = bj_put_key(bd, (const uint8_t *)"name", 4);
            if (!e) e = bj_put_string(bd, a, (uint32_t)a_len);
            break;
        case DC_WAL_DROP_COLLECTION:
            break;
    }

    if (!e) e = bj_end_object(bd);
    if (!e) {
        size_t n; const uint8_t *d = bj_builder_data(bd, &n);
        if (!d) e = bj_builder_error(bd) ? bj_builder_error(bd) : BJ_ERR_STATE;
        else e = plan_push(p, d, n);
    }
    bj_builder_free(bd);
    return e;
}

/* Remember the single document this plan is about: its id (reported as
 * the matched/upserted id) and its pre-image (returned by the
 * findOneAnd* forms). Takes ownership of nothing; copies. */
static int keep_preimage(dc_wal_plan *p, const uint8_t *doc, size_t len) {
    return dbuf_dup(doc, len, &p->preimage, &p->preimage_len);
}

/* --------------------------------------------------------------- query */

/*
 * Every matching document's id, via dc_find under an {_id: 1} projection.
 * The projection matters: without it this materializes every matched
 * document in full, and updateMany over a large collection is precisely
 * where that would hurt.
 *
 * Built rather than kept as a byte literal -- the encoding has a length
 * prefix a hand-written constant would have to keep correct, and twenty
 * bytes of builder next to a collection scan costs nothing measurable.
 */
static int id_projection(uint8_t **out, size_t *out_len) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"_id", 3);
    if (!e) e = bj_put_int(b, 1);
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *d = bj_builder_data(b, &n);
        if (!d) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_dup(d, n, out, out_len);
    }
    bj_builder_free(b);
    return e;
}

static int for_each_match_id(dc_collection *c,
                             const uint8_t *filter, uint32_t filter_len,
                             uint8_t **docs_out, size_t *docs_len,
                             uint32_t *count_out) {
    uint8_t *proj = NULL; size_t proj_len = 0;
    int e = id_projection(&proj, &proj_len);
    if (e) return e;

    qry_options opts = {0};
    opts.projection = proj;
    opts.projection_len = (uint32_t)proj_len;
    e = dc_find(c, filter, filter_len, &opts, docs_out, docs_len);
    free(proj);
    if (e) return e;

    cur cr = { *docs_out, *docs_len, 0 };
    e = array_begin(&cr, count_out);
    if (e) { free(*docs_out); *docs_out = NULL; }
    return e;
}

/* ------------------------------------------------------------ planning */

static int plan_update_like(dc_wal_plan *p, dc_collection *c,
                            const char *coll, uint32_t coll_len,
                            int req,
                            const uint8_t *filter, uint32_t filter_len,
                            const uint8_t *arg, uint32_t arg_len,
                            int upsert, const uint8_t default_id[12]) {
    int is_update = (req == DC_WREQ_UPDATE_ONE || req == DC_WREQ_UPDATE_MANY);
    int is_many   = (req == DC_WREQ_UPDATE_MANY || req == DC_WREQ_DELETE_MANY);

    /* Reject a malformed update before touching the collection: an
     * unordered batch is entitled to attempt every operation, which it
     * cannot do if the grammar error only surfaces halfway through. Same
     * reasoning as db_bulk.h's up-front validation. */
    if (is_update) {
        int e = upd_validate(arg, arg_len);
        if (e) return e;
    }

    if (is_many) {
        uint8_t *docs = NULL; size_t docs_len = 0; uint32_t count = 0;
        int e = for_each_match_id(c, filter, filter_len, &docs, &docs_len, &count);
        if (e) return e;

        cur cr = { docs, docs_len, 0 };
        (void)array_begin(&cr, &count);          /* re-validated above    */
        for (uint32_t i = 0; !e && i < count; i++) {
            size_t start = cr.pos;
            e = skip_value(&cr);
            if (e) break;
            uint8_t id[12];
            e = dc_document_id(cr.d + start, (uint32_t)(cr.pos - start), id);
            if (e) break;
            e = emit(p, is_update ? DC_WAL_UPDATE : DC_WAL_DELETE,
                     coll, coll_len, id, NULL, 0, arg, arg_len);
        }
        free(docs);
        if (e) return e;

        if (count > 0) { p->outcome = DC_PLAN_MATCHED; return BJ_OK; }
        /* No match. Only updateMany can upsert (deleteMany has no such
         * option), and it inserts exactly one document -- the same
         * single-document upsert updateOne performs. */
        if (!(is_update && upsert)) { p->outcome = DC_PLAN_NOTHING; return BJ_OK; }
    } else {
        int found = 0; uint8_t *doc = NULL; size_t doc_len = 0;
        int e = dc_find_one(c, filter, filter_len, NULL, 0, &found, &doc, &doc_len);
        if (e) { free(doc); return e; }

        if (found) {
            uint8_t id[12];
            e = dc_document_id(doc, (uint32_t)doc_len, id);
            if (!e) e = keep_preimage(p, doc, doc_len);
            if (!e) {
                memcpy(p->target, id, 12);
                p->has_target = 1;
                switch (req) {
                    case DC_WREQ_UPDATE_ONE:
                        e = emit(p, DC_WAL_UPDATE, coll, coll_len, id, NULL, 0, arg, arg_len);
                        break;
                    case DC_WREQ_REPLACE_ONE:
                        /* replaceOne may not move a document to a new _id.
                         * Caught here rather than at apply so the log never
                         * holds a command that is certain to fail -- the
                         * proposer, unlike the applier, still has a caller
                         * to hand the error to. */
                        {
                            uint8_t rid[12]; int has = 0;
                            e = dc_document_id_opt(arg, arg_len, rid, &has);
                            if (!e && has && memcmp(rid, id, 12) != 0) e = DC_ERR_ID_MISMATCH;
                        }
                        if (!e) e = emit(p, DC_WAL_REPLACE, coll, coll_len, id, NULL, 0, arg, arg_len);
                        break;
                    default: /* DC_WREQ_DELETE_ONE */
                        e = emit(p, DC_WAL_DELETE, coll, coll_len, id, NULL, 0, NULL, 0);
                        break;
                }
            }
            free(doc);
            if (e) return e;
            p->outcome = DC_PLAN_MATCHED;
            return BJ_OK;
        }
        free(doc);
        if (!upsert || req == DC_WREQ_DELETE_ONE) { p->outcome = DC_PLAN_NOTHING; return BJ_OK; }
    }

    /*
     * The upsert, resolved the whole way: build the document the write
     * would have inserted and log a plain INSERT. This is what removes
     * the old `uu`/`ru` opcodes, the filter they carried into the log,
     * and the second full query their apply performed.
     */
    uint8_t *ins = NULL; size_t ins_len = 0;
    int e = is_update
        ? dc_upsert_document(filter, filter_len, arg, arg_len, default_id, &ins, &ins_len)
        : dc_replace_document(arg, arg_len, filter, filter_len, default_id, &ins, &ins_len);
    if (e) return e;
    e = dc_document_id(ins, (uint32_t)ins_len, p->target);
    if (!e) {
        p->has_target = 1;
        e = emit(p, DC_WAL_INSERT, coll, coll_len, NULL, ins, ins_len, NULL, 0);
    }
    free(ins);
    if (e) return e;
    p->outcome = DC_PLAN_UPSERT;
    return BJ_OK;
}

int dc_wal_plan_build(dc_collection *c, const char *coll, uint32_t coll_len,
                int req,
                const uint8_t *a, uint32_t a_len,
                const uint8_t *b, uint32_t b_len,
                int upsert, const uint8_t default_id[12],
                dc_wal_plan **out) {
    *out = NULL;
    dc_wal_plan *p = (dc_wal_plan *)calloc(1, sizeof(*p));
    if (!p) return BJ_ERR_OOM;
    p->outcome = DC_PLAN_MATCHED;

    int e = BJ_OK;
    switch (req) {
        case DC_WREQ_INSERT_ONE:
            /* The host assigned the id before calling (db.h's top
             * comment); dc_document_id rejects a document that has none,
             * here rather than at apply time. */
            e = dc_document_id(a, a_len, p->target);
            if (!e) { p->has_target = 1;
                      e = emit(p, DC_WAL_INSERT, coll, coll_len, NULL, a, a_len, NULL, 0); }
            break;

        case DC_WREQ_INSERT_MANY: {
            cur cr = { a, a_len, 0 };
            uint32_t count = 0;
            e = array_begin(&cr, &count);
            if (!e && count == 0) e = DC_ERR_WAL_BAD_REQUEST;
            for (uint32_t i = 0; !e && i < count; i++) {
                size_t start = cr.pos;
                e = skip_value(&cr);
                if (!e) e = emit(p, DC_WAL_INSERT, coll, coll_len, NULL,
                                 cr.d + start, cr.pos - start, NULL, 0);
            }
            break;
        }

        case DC_WREQ_UPDATE_ONE:
        case DC_WREQ_UPDATE_MANY:
        case DC_WREQ_REPLACE_ONE:
        case DC_WREQ_DELETE_ONE:
        case DC_WREQ_DELETE_MANY:
            if (!c) { e = DC_ERR_WAL_BAD_REQUEST; break; }
            e = plan_update_like(p, c, coll, coll_len, req, a, a_len, b, b_len,
                                 upsert, default_id);
            break;

        case DC_WREQ_CREATE_INDEX:
            e = emit(p, DC_WAL_CREATE_INDEX, coll, coll_len, NULL, a, a_len, b, b_len);
            break;
        case DC_WREQ_DROP_INDEX:
            e = emit(p, DC_WAL_DROP_INDEX, coll, coll_len, NULL, a, a_len, NULL, 0);
            break;
        case DC_WREQ_DROP_COLLECTION:
            e = emit(p, DC_WAL_DROP_COLLECTION, coll, coll_len, NULL, NULL, 0, NULL, 0);
            break;

        default:
            e = DC_ERR_WAL_BAD_REQUEST;
            break;
    }

    if (e) { dc_wal_plan_free(p); return e; }
    *out = p;
    return BJ_OK;
}

int dc_wal_plan_outcome(const dc_wal_plan *p) { return p ? p->outcome : DC_PLAN_NOTHING; }
uint32_t dc_wal_plan_count(const dc_wal_plan *p) { return p ? p->n : 0; }

const uint8_t *dc_wal_plan_cmd(const dc_wal_plan *p, uint32_t i, uint32_t *len) {
    if (!p || i >= p->n) { *len = 0; return NULL; }
    *len = (uint32_t)p->at[i].len;
    return p->cmds.data + p->at[i].off;
}

const uint8_t *dc_wal_plan_preimage(const dc_wal_plan *p, uint32_t *len) {
    if (!p || !p->preimage) { *len = 0; return NULL; }
    *len = (uint32_t)p->preimage_len;
    return p->preimage;
}

const uint8_t *dc_wal_plan_target_id(const dc_wal_plan *p) {
    return (p && p->has_target) ? p->target : NULL;
}

void dc_wal_plan_free(dc_wal_plan *p) {
    if (!p) return;
    dbuf_free(&p->cmds);
    free(p->at);
    free(p->preimage);
    free(p);
}

/* ---------------------------------------------------------------- parse */

/* Which fields each opcode requires, indexed by dc_wal_op. `id` is
 * checked for being an OID specifically; the rest only for presence,
 * since their own consumers (upd_validate, dc_insert_one, createIndex)
 * validate their contents far better than a shape check here could. */
static const struct { const char *f[3]; int needs_id; } REQUIRED[] = {
    /* INSERT          */ { { "doc", NULL, NULL },        0 },
    /* UPDATE          */ { { "update", NULL, NULL },     1 },
    /* REPLACE         */ { { "doc", NULL, NULL },        1 },
    /* DELETE          */ { { NULL, NULL, NULL },         1 },
    /* CREATE_INDEX    */ { { "keys", "options", NULL },  0 },
    /* DROP_INDEX      */ { { "name", NULL, NULL },       0 },
    /* DROP_COLLECTION */ { { NULL, NULL, NULL },         0 }
};

int dc_wal_parse(const uint8_t *buf, uint32_t len,
                 int *op_out, const uint8_t **coll, uint32_t *coll_len) {
    *op_out = -1;
    *coll = NULL;
    *coll_len = 0;

    const uint8_t *vp; size_t vlen; int found;
    int e = obj_get_field(buf, len, (const uint8_t *)"op", 2, &vp, &vlen, &found);
    if (e) return e;
    if (!found) return DC_ERR_WAL_MISSING_FIELD;

    /* A STRING value is a 1-byte tag, a u32 length, then the bytes. */
    if (vlen < 5 || vp[0] != BJ_TYPE_STRING) return DC_ERR_WAL_UNKNOWN_OP;
    uint32_t oplen = rdu32(vp + 1);
    if ((size_t)oplen + 5 != vlen) return DC_ERR_WAL_UNKNOWN_OP;

    int op = -1;
    for (int i = 0; i < OP_COUNT; i++) {
        if ((uint32_t)strlen(OP_NAME[i]) == oplen &&
            memcmp(vp + 5, OP_NAME[i], oplen) == 0) { op = i; break; }
    }
    if (op < 0) return DC_ERR_WAL_UNKNOWN_OP;

    e = obj_get_field(buf, len, (const uint8_t *)"c", 1, &vp, &vlen, &found);
    if (e) return e;
    if (!found || vlen < 5 || vp[0] != BJ_TYPE_STRING) return DC_ERR_WAL_MISSING_FIELD;
    uint32_t clen = rdu32(vp + 1);
    if ((size_t)clen + 5 != vlen) return DC_ERR_WAL_MISSING_FIELD;

    if (REQUIRED[op].needs_id) {
        e = obj_get_field(buf, len, (const uint8_t *)"id", 2, &vp, &vlen, &found);
        if (e) return e;
        if (!found || vlen != 13 || vp[0] != BJ_TYPE_OID) return DC_ERR_WAL_MISSING_FIELD;
    }
    for (int i = 0; i < 3 && REQUIRED[op].f[i]; i++) {
        const char *f = REQUIRED[op].f[i];
        e = obj_get_field(buf, len, (const uint8_t *)f, (uint32_t)strlen(f), &vp, &vlen, &found);
        if (e) return e;
        if (!found) return DC_ERR_WAL_MISSING_FIELD;
    }

    /* Resolved last so a rejected command reports nothing at all. */
    e = obj_get_field(buf, len, (const uint8_t *)"c", 1, &vp, &vlen, &found);
    if (e) return e;
    *coll = vp + 5;
    *coll_len = clen;
    *op_out = op;
    return BJ_OK;
}
