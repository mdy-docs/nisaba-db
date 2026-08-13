/*
 * db_session.c — see db_session.h for what this is and why it exists.
 *
 * The whole file is one loop with careful unwinding: walk the plan
 * dc_catalog_open_plan produced, open what it names, attach what it
 * describes, and on any refusal close everything opened so far. Nothing
 * here decides what a collection is made of.
 */
#include "db_session.h"
#include "db_ttl.h"
#include "db_wal.h"   /* the command grammar: this file performs the DDL three */

#include "db_catalog.h"
#include "db_names.h"
#include "db_validate.h"
#include "bjcursor.h"
#include "bplustree.h"
#include "rtree.h"
#include "dbuf.h"

#include <stdlib.h>
#include <string.h>

/* An index's files, whatever kind it is: up to three trees (text) or one
 * tree (equality) or one rtree (geo). Held so close can release them --
 * dc_collection_free deliberately does not, because the trees are the
 * host's (db.h). */
#define DBS_MAX_INDEX_FILES 3

typedef struct {
    int      kind;                          /* dc_index_plan_kind */
    int      n_files;
    char    *name;                          /* owned copy */
    size_t   name_len;
    bj_io    io[DBS_MAX_INDEX_FILES];
    bpt     *tree[DBS_MAX_INDEX_FILES];     /* equality: [0]; text: [0..2] */
    rtree   *rt;                            /* geo */
} dbs_index;

typedef struct {
    int            used;
    char          *name;                    /* owned copy */
    size_t         name_len;
    dc_collection *coll;
    bpt           *primary;
    bj_io          primary_io;
    bj_io          journal_io;
    int            has_journal;
    dbs_index      idx[DBS_MAX_INDEXES];
    int            n_idx;
} dbs_entry;

/* An open cursor and who it belongs to. `id` is 0 in a free slot, and
 * ids are never reused: a client that kept a stale id gets
 * DC_ERR_NO_CURSOR rather than somebody else's scan. */
typedef struct {
    uint64_t   id;
    uint64_t   client;
    uint32_t   batch;                       /* the batch size it was opened with */
    dc_cursor *cur;
} dbs_cursor;

/* A change stream and who it belongs to. `queue` holds already-encoded
 * event objects back to back -- the transport splices them into frames
 * without decoding one, the same way every other result crosses this
 * layer. `idx` rides beside it, one log index per queued event (0 when
 * no log gave it one), so taking an event can record how far the
 * consumer has got without decoding what it took.
 *
 * `overflowed` is a flag and not a count, deliberately: once a stream
 * overflows nothing more is BUILT for it (dbs_watched stops naming it),
 * so any count would be "however many happened before the transport
 * next looked" -- a number that means something different depending on
 * timing. The consumer's remedy is in `last_index`: the overflow frame
 * names it, and with a log behind the stream the consumer resumes from
 * there and misses nothing. Without one it re-watches and re-reads. */
typedef struct {
    uint64_t id;
    uint64_t client;
    char    *coll;                          /* owned copy */
    size_t   coll_len;
    dbuf     queue;
    uint64_t idx[DBS_STREAM_EVENTS];
    uint32_t count;
    uint64_t last_index;                    /* highest index DELIVERED */
    int      overflowed;
} dbs_stream;

/*
 * One write planned and waiting for a quorum. `req` is kept because
 * dbs_complete re-runs the request over the held plan: the response is
 * then built by exactly the code that builds it on a single-process
 * server, rather than by a second assembler that could disagree with it.
 */
/*
 * One command that has already been applied, and what applying it said.
 *
 * A stepped request runs its whole dispatch again for every trip to the
 * log, so the same command is walked past several times -- and a
 * database is not a pure function: applying one twice inserts it twice.
 * The result is kept the first time and handed back on every later pass,
 * so the accumulators that build the response see exactly the sequence
 * they saw when it happened.
 */
typedef struct {
    dbuf result;
    int  rc;
    int  done;
} dbs_applied;

typedef struct {
    uint64_t      token;
    uint64_t      client;
    dbuf          req;
    /*
     * PLANS, plural, and made ONE AT A TIME.
     *
     * bulkWrite is a list of writes rather than a batch of them:
     * operation two runs against a database operation one has already
     * changed. That is what MongoDB does, and what both other
     * implementations here do -- do_bulk_write plans each operation
     * after applying the last, and runBulkWrite (src/nisaba-wasm.js)
     * dispatches each to its own logged method, awaited.
     *
     * A replica cannot cheat that by planning the list up front, because
     * operation two would resolve its filter against a document
     * operation one has not inserted yet. So each pass applies what is
     * already planned and plans exactly ONE more thing: a bulkWrite of
     * three operations takes three trips to the log, and every other
     * write takes one.
     */
    dc_wal_plan **plans;
    uint32_t      nplans, plan_cap, at_plan;
    dbs_applied  *applied;              /* one per command, across all plans */
    uint32_t      napplied, applied_cap, at_applied;
    int           more;                 /* this pass planned something new */
} dbs_pending;

static void pending_release(dbs_pending *p) {
    for (uint32_t i = 0; i < p->nplans; i++) dc_wal_plan_free(p->plans[i]);
    free(p->plans);
    for (uint32_t i = 0; i < p->napplied; i++) dbuf_free(&p->applied[i].result);
    free(p->applied);
    dbuf_free(&p->req);
    memset(p, 0, sizeof *p);
}

struct dbs {
    bj_ns      *ns;                         /* borrowed; the caller's */
    int         order;
    bj_io       catalog_io;
    bpt        *catalog;
    dbs_entry   open[DBS_MAX_COLLECTIONS];
    dbs_cursor  cursors[DBS_MAX_CURSORS];
    uint64_t    next_cursor_id;             /* 1, 2, 3, ... never reused */
    /* ...unless an instance is minting them (dbs_set_id_source). Two
     * databases in one process each start at 1, and a client holding a
     * cursor in both would then hold two with the same id -- routed
     * apart by the request's `db` today, and by nothing at all the first
     * time a caller names the wrong one. A shared counter removes the
     * question instead of answering it. */
    uint64_t   *id_source;
    dbs_stream  streams[DBS_MAX_STREAMS];
    uint64_t    next_stream_id;

    /* The log this database's writes go through, if it goes through one
     * (dbs_set_log) -- what makes a watch resumable. `db_name` is this
     * database's own name, kept so replay can ask the reader to filter
     * an instance-wide log down to its entries. */
    dbs_log     log;
    char       *db_name;                    /* owned copy; NULL = no log */
    size_t      db_name_len;

    /*
     * Writes planned but not yet replicated (db_session.h's fork). A
     * plan is held rather than a command list because the RESPONSE needs
     * more than the commands: whether an upsert upserted, which document
     * it resolved to, and the pre-image the find-one-and-* family
     * answers with. All three are the plan's, and re-deriving them from
     * the commands would be a second opinion about a fact that already
     * has an owner.
     */
    dbs_pending pending[DBS_MAX_INFLIGHT];
    uint64_t    next_token;
    /* The request being run right now, and where this pass has got to in
     * its caller's index list. Never nested: dbs_propose and dbs_step
     * each run one pass of one request to completion. */
    dbs_pending      *resuming;
    const uint64_t   *indices;
    const dbs_result *results;
    uint32_t          nindices, nresults, at_index;
};

/* ---- plan reading ------------------------------------------------------ */

/* A field of a binjson object as a string. Absent is not an error here --
 * *found says which, because several plan fields are legitimately
 * optional and the caller knows which of its own are not. */
static int plan_str(const uint8_t *obj, size_t obj_len, const char *key,
                    const uint8_t **s, uint32_t *slen, int *found) {
    const uint8_t *v; size_t vlen;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)key,
                          (uint32_t)strlen(key), &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    return take_string(&c, s, slen);
}

/* A field as an int (binjson numbers are doubles on the wire). */
static int plan_int(const uint8_t *obj, size_t obj_len, const char *key,
                    int *out, int *found) {
    const uint8_t *v; size_t vlen;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)key,
                          (uint32_t)strlen(key), &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    return read_int31(&c, out);
}

/* A field as a boolean, defaulting to 0 when absent -- which is what
 * every flag in a plan means when the catalog never stored it. */
static int plan_flag(const uint8_t *obj, size_t obj_len, const char *key, int *out) {
    const uint8_t *v; size_t vlen; int found = 0;
    *out = 0;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)key,
                          (uint32_t)strlen(key), &v, &vlen, &found);
    if (e || !found) return e;
    cur c = { v, vlen, 0 };
    return read_bool(&c, out);
}

/* A field's RAW encoded value, handed to the attach calls untouched:
 * `fields` and `partialFilterExpression` are binjson the index layer
 * parses itself, and re-encoding them here would be a second opinion
 * about what the catalog stored. */
static int plan_raw(const uint8_t *obj, size_t obj_len, const char *key,
                    const uint8_t **p, size_t *plen, int *found) {
    return obj_get_field(obj, obj_len, (const uint8_t *)key,
                         (uint32_t)strlen(key), p, plen, found);
}

/* The i-th element of a binjson array, as a raw span. */
static int arr_at(const uint8_t *arr, size_t arr_len, uint32_t want,
                  const uint8_t **p, size_t *plen) {
    cur c = { arr, arr_len, 0 };
    uint32_t n;
    int e = array_begin(&c, &n);
    if (e) return e;
    if (want >= n) return BJ_ERR_RANGE;
    for (uint32_t i = 0; i < n; i++) {
        size_t start = c.pos;
        e = skip_value(&c);
        if (e) return e;
        if (i == want) { *p = c.d + start; *plen = c.pos - start; return BJ_OK; }
    }
    return BJ_ERR_RANGE;
}

static int arr_len(const uint8_t *arr, size_t arr_len_bytes, uint32_t *out) {
    cur c = { arr, arr_len_bytes, 0 };
    return array_begin(&c, out);
}

/* ---- opening ----------------------------------------------------------- */

static void index_release(bj_ns *ns, dbs_index *ix) {
    for (int i = 0; i < ix->n_files; i++) {
        if (ix->tree[i]) { bpt_free(ix->tree[i]); ix->tree[i] = NULL; }
    }
    if (ix->rt) { rtree_free(ix->rt); ix->rt = NULL; }
    for (int i = 0; i < ix->n_files; i++) ns->close(ns->ctx, &ix->io[i]);
    ix->n_files = 0;
    free(ix->name);
    ix->name = NULL;
    ix->name_len = 0;
}

/* Everything one entry holds, released in the reverse of the order it was
 * acquired: the collection first (it only unregisters indexes), then the
 * index trees, then the primary, then the ios. */
static void entry_release(bj_ns *ns, dbs_entry *en) {
    if (en->coll) { dc_collection_free(en->coll); en->coll = NULL; }
    for (int i = 0; i < en->n_idx; i++) index_release(ns, &en->idx[i]);
    en->n_idx = 0;
    if (en->primary) { bpt_free(en->primary); en->primary = NULL; }
    if (en->has_journal) { ns->close(ns->ctx, &en->journal_io); en->has_journal = 0; }
    ns->close(ns->ctx, &en->primary_io);
    free(en->name);
    en->name = NULL;
    en->name_len = 0;
    en->used = 0;
}

/* Open one index from its plan-shaped definition and attach it. On any
 * refusal the index's own handles are closed and nothing is attached --
 * the caller unwinds the rest. */
static int open_index(dbs *s, dbs_entry *en, const char *coll, size_t coll_len,
                      const uint8_t *def, size_t def_len) {
    (void)coll; (void)coll_len;
    if (en->n_idx >= DBS_MAX_INDEXES) return DC_ERR_TOO_MANY_INDEXES;

    dbs_index *ix = &en->idx[en->n_idx];
    memset(ix, 0, sizeof(*ix));

    int found = 0;
    const uint8_t *name; uint32_t name_len;
    int e = plan_str(def, def_len, "name", &name, &name_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;

    int kind = DC_INDEX_EQUALITY;
    e = plan_int(def, def_len, "kind", &kind, &found);
    if (e) return e;
    ix->kind = kind;

    /* Kept so a later compaction can match a plan's build entry to the
     * index it names rather than trusting two arrays to be in the same
     * order. They are, today -- both come from the catalog entry's index
     * list -- but streaming the wrong tree into the wrong file corrupts
     * an index silently, which is too quiet a failure to leave resting
     * on an ordering nobody checks. */
    ix->name = (char *)malloc(name_len ? name_len : 1);
    if (!ix->name) return BJ_ERR_OOM;
    memcpy(ix->name, name, name_len);
    ix->name_len = name_len;

    const uint8_t *files; size_t files_len;
    e = plan_raw(def, def_len, "files", &files, &files_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;
    uint32_t n_files = 0;
    e = arr_len(files, files_len, &n_files);
    if (e) return e;
    if (n_files == 0 || n_files > DBS_MAX_INDEX_FILES) return DC_ERR_CATALOG_ENTRY;

    /* Open every file the plan named, IN ITS ORDER -- for a text index
     * that order is exactly the order the attach call takes its three
     * trees, which is the plan's job and not this loop's. */
    for (uint32_t i = 0; i < n_files; i++) {
        const uint8_t *fv; size_t fvlen;
        e = arr_at(files, files_len, i, &fv, &fvlen);
        if (e) goto fail;
        cur fc = { fv, fvlen, 0 };
        const uint8_t *fname; uint32_t fname_len;
        e = take_string(&fc, &fname, &fname_len);
        if (e) goto fail;
        e = s->ns->open(s->ns->ctx, (const char *)fname, fname_len, 0, &ix->io[i]);
        if (e) goto fail;
        ix->n_files = (int)i + 1;
    }

    if (kind == DC_INDEX_GEO) {
        ix->rt = rtree_open(&ix->io[0]);
        if (!ix->rt) { e = BJ_ERR_STATE; goto fail; }
        const uint8_t *field; uint32_t field_len;
        e = plan_str(def, def_len, "field", &field, &field_len, &found);
        if (e) goto fail;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        e = dc_collection_attach_geo_index(en->coll, (const char *)name, (int)name_len,
                                           ix->rt, (const char *)field, (int)field_len);
        if (e) goto fail;
    } else if (kind == DC_INDEX_TEXT) {
        if (n_files != DBS_MAX_INDEX_FILES) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        for (uint32_t i = 0; i < n_files; i++) {
            ix->tree[i] = bpt_open(&ix->io[i]);
            if (!ix->tree[i]) { e = BJ_ERR_STATE; goto fail; }
        }
        const uint8_t *field; uint32_t field_len;
        e = plan_str(def, def_len, "field", &field, &field_len, &found);
        if (e) goto fail;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        e = dc_collection_attach_text_index(en->coll, (const char *)name, (int)name_len,
                                            ix->tree[0], ix->tree[1], ix->tree[2],
                                            (const char *)field, (int)field_len);
        if (e) goto fail;
    } else {
        ix->tree[0] = bpt_open(&ix->io[0]);
        if (!ix->tree[0]) { e = BJ_ERR_STATE; goto fail; }
        const uint8_t *fields; size_t fields_len;
        e = plan_raw(def, def_len, "fields", &fields, &fields_len, &found);
        if (e) goto fail;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        int uniq = 0, sparse = 0;
        if ((e = plan_flag(def, def_len, "unique", &uniq))) goto fail;
        if ((e = plan_flag(def, def_len, "sparse", &sparse))) goto fail;
        const uint8_t *pfe = NULL; size_t pfe_len = 0; int has_pfe = 0;
        e = plan_raw(def, def_len, "partialFilterExpression", &pfe, &pfe_len, &has_pfe);
        if (e) goto fail;
        e = dc_collection_attach_index(en->coll, (const char *)name, (int)name_len,
                                       ix->tree[0], fields, (uint32_t)fields_len,
                                       uniq, sparse,
                                       has_pfe ? pfe : NULL,
                                       has_pfe ? (uint32_t)pfe_len : 0);
        if (e) goto fail;
    }

    en->n_idx++;
    return BJ_OK;

fail:
    index_release(s->ns, ix);
    return e;
}

static int open_collection(dbs *s, dbs_entry *en,
                           const char *name, size_t name_len,
                           const uint8_t *entry, size_t entry_len) {
    dbuf plan = {0};
    int e = dc_catalog_open_plan(entry, entry_len, name, name_len, &plan);
    if (e) return e;

    memset(en, 0, sizeof(*en));

    int found = 0;
    const uint8_t *pf; uint32_t pf_len;
    e = plan_str(plan.data, plan.len, "primary", &pf, &pf_len, &found);
    if (e) goto done;
    if (!found) { e = DC_ERR_CATALOG_ENTRY; goto done; }

    e = s->ns->open(s->ns->ctx, (const char *)pf, pf_len, 0, &en->primary_io);
    if (e) goto done;

    en->primary = bpt_open(&en->primary_io);
    if (!en->primary) { e = BJ_ERR_STATE; goto done; }

    en->coll = dc_collection_open(en->primary);
    if (!en->coll) { e = BJ_ERR_OOM; goto done; }

    /* Indexes, then the journal: dc_collection_recover must run only
     * after every index is attached, the same ordering nisaba-wasm.js
     * documents and tix_recover requires. */
    const uint8_t *ixs; size_t ixs_len; int has_ixs = 0;
    e = plan_raw(plan.data, plan.len, "indexes", &ixs, &ixs_len, &has_ixs);
    if (e) goto done;
    if (has_ixs) {
        uint32_t n = 0;
        e = arr_len(ixs, ixs_len, &n);
        if (e) goto done;
        for (uint32_t i = 0; i < n; i++) {
            const uint8_t *def; size_t def_len;
            e = arr_at(ixs, ixs_len, i, &def, &def_len);
            if (e) goto done;
            e = open_index(s, en, name, name_len, def, def_len);
            if (e) goto done;
        }
    }

    const uint8_t *jf; uint32_t jf_len;
    e = plan_str(plan.data, plan.len, "journal", &jf, &jf_len, &found);
    if (e) goto done;
    if (!found) { e = DC_ERR_CATALOG_ENTRY; goto done; }
    e = s->ns->open(s->ns->ctx, (const char *)jf, jf_len, BJ_NS_CREATE, &en->journal_io);
    if (e) goto done;
    en->has_journal = 1;

    e = dc_collection_recover(en->coll, &en->journal_io);
    if (e) goto done;

    en->name = (char *)malloc(name_len ? name_len : 1);
    if (!en->name) { e = BJ_ERR_OOM; goto done; }
    memcpy(en->name, name, name_len);
    en->name_len = name_len;
    en->used = 1;

done:
    dbuf_free(&plan);
    if (e) entry_release(s->ns, en);
    return e;
}

/* ---- public ------------------------------------------------------------ */

/* The format stamp, checked and (when creating) written -- the same
 * gate Db.open() applies in JavaScript, against the same key and the
 * same {v} shape, because a database is only one format however many
 * hosts read it. A database with no stamp predates the stamp and is
 * version 1 by definition. */
static int check_format(dbs *s, int create) {
    bpt_key key = { .is_string = 1, .num = 0,
                    .str = (const uint8_t *)DC_FORMAT_KEY,
                    .str_len = (uint32_t)strlen(DC_FORMAT_KEY) };
    int found = 0;
    const uint8_t *vp = NULL; size_t vlen = 0;
    int e = bpt_search(s->catalog, &key, &found, &vp, &vlen);
    if (e) return e;

    int64_t v = DC_FORMAT_VERSION;
    if (found) {
        const uint8_t *nv; size_t nvlen; int f = 0;
        if ((e = obj_get_field(vp, vlen, (const uint8_t *)"v", 1, &nv, &nvlen, &f))) return e;
        if (f) {
            cur c = { nv, nvlen, 0 };
            double d = 0;
            if ((e = read_number(&c, &d))) return e;
            v = (int64_t)d;
        }
        /* Newer than this build understands: refused, not opened and
         * misread. There is no way to know what a field this build has
         * never seen means. */
        if (v > DC_FORMAT_VERSION) return DC_ERR_FORMAT_NEWER;
    }
    if (!create || (found && v == DC_FORMAT_VERSION)) return BJ_OK;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"v", 1);
    bj_put_int(b, DC_FORMAT_VERSION);
    bj_end_object(b);
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    e = data ? bpt_add(s->catalog, &key, data, (uint32_t)len) : BJ_ERR_OOM;
    bj_builder_free(b);
    return e;
}

int dbs_open(bj_ns *ns, int order, int create, dbs **out) {
    if (!ns || !ns->open || !ns->close || !out) return BJ_ERR_STATE;
    *out = NULL;

    dbs *s = (dbs *)calloc(1, sizeof(dbs));
    if (!s) return BJ_ERR_OOM;
    s->ns = ns;
    s->order = order;

    /* No catalog is its own answer: a directory with no database in it is
     * something a caller can act on ("make one"), where BJ_ERR_STATE
     * reaches a user as "builder state error" and helps nobody. */
    int e = ns->open(ns->ctx, DC_CATALOG_FILE, (uint32_t)strlen(DC_CATALOG_FILE),
                     create ? BJ_NS_CREATE : 0, &s->catalog_io);
    if (e) { free(s); return DC_ERR_NO_DATABASE; }

    /* An empty file is a directory that has just become a database: the
     * catalog tree is written here, not found. Anything else is opened,
     * which is also the check that it IS a catalog. */
    s->catalog = s->catalog_io.size(s->catalog_io.ctx) == 0
                 ? bpt_create(&s->catalog_io, order)
                 : bpt_open(&s->catalog_io);
    if (!s->catalog) {
        ns->close(ns->ctx, &s->catalog_io);
        free(s);
        return BJ_ERR_STATE;
    }
    if ((e = check_format(s, create))) {
        bpt_free(s->catalog);
        ns->close(ns->ctx, &s->catalog_io);
        free(s);
        return e;   /* *out stays NULL: nothing was opened */
    }

    *out = s;
    return BJ_OK;
}

void dbs_set_id_source(dbs *s, uint64_t *shared) {
    if (s) s->id_source = shared;
}

int dbs_collection(dbs *s, const char *name, size_t name_len, dc_collection **out) {
    if (!s || !name || !out) return BJ_ERR_STATE;
    *out = NULL;

    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) {
        dbs_entry *en = &s->open[i];
        if (en->used && en->name_len == name_len &&
            memcmp(en->name, name, name_len) == 0) {
            *out = en->coll;
            return BJ_OK;
        }
    }

    int slot = -1;
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) {
        if (!s->open[i].used) { slot = i; break; }
    }
    if (slot < 0) return DC_ERR_TOO_MANY_COLLECTIONS;

    bpt_key key = { .is_string = 1, .num = 0,
                    .str = (const uint8_t *)name, .str_len = (uint32_t)name_len };
    int found = 0;
    const uint8_t *vp = NULL; size_t vlen = 0;
    int e = bpt_search(s->catalog, &key, &found, &vp, &vlen);
    if (e) return e;
    if (!found) return DC_ERR_NO_COLLECTION;

    /* COPY the entry before opening anything. bpt_search hands back the
     * tree's own output buffer, which dies on that tree's next operation
     * -- and opening a collection performs plenty. */
    uint8_t *entry = NULL; size_t entry_len = 0;
    e = dbuf_dup(vp, vlen, &entry, &entry_len);
    if (e) return e;

    e = open_collection(s, &s->open[slot], name, name_len, entry, entry_len);
    free(entry);
    if (e) return e;

    *out = s->open[slot].coll;
    return BJ_OK;
}

/* ---- creation and schema ------------------------------------------------
 *
 * See db_session.h. Every one of these writes the catalog, and every one
 * of them writes it LAST: the files are made first, so a failure leaves
 * garbage nothing references rather than an entry pointing at something
 * that is not there. The one exception is dropping, where the entry goes
 * first for exactly the same reason -- an unreferenced file is an
 * orphan, a referenced missing one is a broken database.
 */

static dbs_entry *find_entry(dbs *s, const char *name, size_t name_len);

/* The catalog entry for `name`, copied out of the tree's own buffer
 * (which dies on that tree's next operation, and these all perform
 * several). */
static int catalog_get(dbs *s, const char *name, size_t name_len,
                       uint8_t **entry, size_t *entry_len, int *found) {
    bpt_key key = { .is_string = 1, .num = 0,
                    .str = (const uint8_t *)name, .str_len = (uint32_t)name_len };
    const uint8_t *vp = NULL; size_t vlen = 0;
    *entry = NULL; *entry_len = 0;
    int e = bpt_search(s->catalog, &key, found, &vp, &vlen);
    if (e || !*found) return e;
    return dbuf_dup(vp, vlen, entry, entry_len);
}

static int catalog_put(dbs *s, const char *name, size_t name_len,
                       const uint8_t *entry, size_t entry_len) {
    bpt_key key = { .is_string = 1, .num = 0,
                    .str = (const uint8_t *)name, .str_len = (uint32_t)name_len };
    return bpt_add(s->catalog, &key, entry, (uint32_t)entry_len);
}

/*
 * THE CATALOG CARRIES AN APPLIED INDEX OF ITS OWN, and a dropped collection
 * is the whole reason it has to.
 *
 * Every other structure here records the last log index applied to it in its
 * own metadata, staged before the mutation so the mutation's commit persists
 * both atomically (db.h's dc_set_applied_index, bplustree.h's setter). That
 * is exactly right until the mutation IS the deletion of the structure
 * holding the record. `dbs_applied_floor` is a MAX over the collections that
 * still exist, so dropping the collection carrying the highest index makes
 * the floor go BACKWARDS -- by however far that collection was ahead.
 *
 * Harmless while the log still holds the entries between the regressed floor
 * and the truth, because replay is idempotent under each collection's own
 * guard. Fatal once the log has been COMPACTED past them: replay then resumes
 * at the log's base, which is ahead of what the live files contain, and the
 * first entry naming the dropped collection cannot apply. That is -37, which
 * dc_is_deterministic deliberately treats as possible divergence, so the
 * member halts -- and replay being deterministic, it halts again on every
 * later boot. Measured: a drop followed by a restart with nothing written
 * after it leaves a database that cannot be opened again, no crash required.
 * test/repro-halt-on-drop.js does it in one go.
 *
 * The catalog is the one structure a drop cannot delete AND does write, so
 * the record belongs here. All three DDL ops make the catalog's commit their
 * decisive durable act -- createIndex writes it last, having built and
 * attached; dropIndex and dropCollection write it FIRST and then remove
 * files, so what a crash leaves behind is an orphan nothing references. In
 * every case, a catalog commit means the entry was applied, which is exactly
 * what may be recorded in it.
 *
 * ONLY for those three. Staging an index the catalog's next commit would
 * carry without that commit containing the entry's mutation would let the
 * floor claim an entry whose effects are not durable -- a lost write, which
 * is worse than the halt this fixes.
 */
static int catalog_note_applied(dbs *s, uint64_t index) {
    if (!s || !s->catalog || !index) return BJ_OK;
    /* The setter refuses a decrease (BJ_ERR_STATE) and the value is sticky.
     * A replay that re-offers an index at or below what is already recorded
     * is the guard working, not an error to fail the apply with. */
    if (index <= bpt_applied_index(s->catalog)) return BJ_OK;
    return bpt_set_applied_index(s->catalog, index);
}

/* Create one file, empty, replacing anything already there: a leftover
 * from an interrupted attempt is garbage, and building on top of it
 * would be building on an unknown. */
static int make_file(dbs *s, const uint8_t *name, uint32_t name_len, bj_io *io) {
    return s->ns->open(s->ns->ctx, (const char *)name, name_len,
                       BJ_NS_CREATE | BJ_NS_TRUNC, io);
}

/* Delete every name in a binjson array of strings. Best effort: what
 * survives is an orphan, which the catalog already does not reference. */
static void remove_all(dbs *s, const uint8_t *files, size_t files_len) {
    uint32_t n = 0;
    if (arr_len(files, files_len, &n)) return;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *fv; size_t fvlen;
        if (arr_at(files, files_len, i, &fv, &fvlen)) return;
        cur c = { fv, fvlen, 0 };
        const uint8_t *fname; uint32_t fname_len;
        if (take_string(&c, &fname, &fname_len)) continue;
        s->ns->remove(s->ns->ctx, (const char *)fname, fname_len);
    }
}

int dbs_create_collection(dbs *s, const char *name, size_t name_len, int *created) {
    if (!s || !name || !created) return BJ_ERR_STATE;
    *created = 0;

    /* The same name rules every host applies, from the file that owns
     * them -- including the reserved format stamp. */
    int e = dc_check_collection_name(name, name_len);
    if (e) return e;

    uint8_t *entry = NULL; size_t entry_len = 0; int found = 0;
    if ((e = catalog_get(s, name, name_len, &entry, &entry_len, &found))) return e;
    if (found) { free(entry); return BJ_OK; }   /* already there is success */

    dbuf fresh = {0};
    if ((e = dc_catalog_new_entry(name, name_len, &fresh))) { dbuf_free(&fresh); return e; }

    /* The primary file, named by the entry rather than by this function:
     * the naming scheme has one owner (db_names.h, through the entry). */
    const uint8_t *fp; uint32_t flen; int f = 0;
    if ((e = plan_str(fresh.data, fresh.len, "file", &fp, &flen, &f))) { dbuf_free(&fresh); return e; }
    if (!f) { dbuf_free(&fresh); return DC_ERR_CATALOG_ENTRY; }

    bj_io io;
    if ((e = make_file(s, fp, flen, &io))) { dbuf_free(&fresh); return e; }
    bpt *tree = bpt_create(&io, s->order);
    if (!tree) {
        s->ns->close(s->ns->ctx, &io);
        s->ns->remove(s->ns->ctx, (const char *)fp, flen);
        dbuf_free(&fresh);
        return BJ_ERR_STATE;
    }
    /* Written and closed: the collection is opened the ordinary way, by
     * dbs_collection, from the catalog -- one open path, not two. */
    bpt_free(tree);
    s->ns->close(s->ns->ctx, &io);

    e = catalog_put(s, name, name_len, fresh.data, fresh.len);
    dbuf_free(&fresh);
    if (e) { s->ns->remove(s->ns->ctx, (const char *)fp, flen); return e; }

    *created = 1;
    return BJ_OK;
}

/*
 * CURSORS FIRST, BEFORE ANY entry_release. A batched cursor is positioned
 * inside the collection's live tree, and entry_release closes that tree and
 * its io: a cursor that outlives the release reads through freed memory on
 * its next getMore. Found by ASan as a heap-use-after-free in
 * bjfile_read_record, reachable by ANY client -- open a cursor, let anyone
 * dropIndex on that collection, ask for the next batch. On a non-sanitized
 * build the freed bytes usually fail to parse (-3, "Unexpected end of
 * data"), which is the lucky outcome of a UAF, not a behavior.
 *
 * ALL cursors, not this collection's: neither dbs_cursor nor dc_cursor
 * records which collection it walks, so there is nothing to select by. DDL
 * is rare and a killed cursor is a clean, actionable client error
 * (DC_ERR_NO_CURSOR on the next getMore -- ids are never reused). Tagging
 * cursors with their collection to close selectively is a refinement, not
 * the fix.
 *
 * dropCollection always did this (it is where this loop and its comment
 * lived); dropIndex called entry_release WITHOUT it, which was the bug.
 * Compact needs neither: it refuses to run while any cursor is open.
 */
static void close_all_cursors(dbs *s) {
    for (int i = 0; i < DBS_MAX_CURSORS; i++) {
        if (!s->cursors[i].id) continue;
        dc_cursor_close(s->cursors[i].cur);
        memset(&s->cursors[i], 0, sizeof s->cursors[i]);
    }
}

int dbs_drop_collection(dbs *s, const char *name, size_t name_len, int *dropped) {
    if (!s || !name || !dropped) return BJ_ERR_STATE;
    *dropped = 0;

    int e = dc_check_collection_name(name, name_len);
    if (e) return e;

    uint8_t *entry = NULL; size_t entry_len = 0; int found = 0;
    if ((e = catalog_get(s, name, name_len, &entry, &entry_len, &found))) return e;
    if (!found) return BJ_OK;   /* nothing to drop is not a failure */

    /* Cursors first: a scan positioned in files about to be unlinked
     * would read bytes that are gone (close_all_cursors says why, and
     * why all of them). */
    close_all_cursors(s);

    dbs_entry *en = find_entry(s, name, name_len);
    if (en) entry_release(s->ns, en);

    /* Which files an entry claims is the sweep's question too, answered
     * by the same C: a kind this missed would be an orphan on every
     * drop, and one the sweep missed would be deleted from under a live
     * collection. */
    dbuf files = {0};
    e = dc_collection_files(entry, entry_len, name, name_len, &files);
    free(entry);
    if (e) { dbuf_free(&files); return e; }

    bpt_key key = { .is_string = 1, .num = 0,
                    .str = (const uint8_t *)name, .str_len = (uint32_t)name_len };
    e = bpt_delete(s->catalog, &key);
    if (e) { dbuf_free(&files); return e; }

    remove_all(s, files.data, files.len);
    dbuf_free(&files);
    *dropped = 1;
    return BJ_OK;
}

/*
 * Create the files a planned index names, attach it (which BACKFILLS it
 * against every document already in the collection -- db.h) and record
 * the definition. Split out because the three kinds differ only in
 * which attach call takes which handles.
 */
static int build_index(dbs *s, dbs_entry *en, const uint8_t *def, size_t def_len) {
    int found = 0;
    const uint8_t *name; uint32_t name_len;
    int e = plan_str(def, def_len, "name", &name, &name_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;

    int kind = DC_INDEX_EQUALITY;
    if ((e = plan_int(def, def_len, "kind", &kind, &found))) return e;

    const uint8_t *files; size_t files_len;
    if ((e = plan_raw(def, def_len, "files", &files, &files_len, &found))) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;
    uint32_t n_files = 0;
    if ((e = arr_len(files, files_len, &n_files))) return e;
    if (n_files == 0 || n_files > DBS_MAX_INDEX_FILES) return DC_ERR_CATALOG_ENTRY;

    bj_io io[DBS_MAX_INDEX_FILES];
    bpt *tree[DBS_MAX_INDEX_FILES] = { NULL, NULL, NULL };
    rtree *rt = NULL;
    uint32_t made = 0;

    for (uint32_t i = 0; i < n_files; i++) {
        const uint8_t *fv; size_t fvlen;
        if ((e = arr_at(files, files_len, i, &fv, &fvlen))) goto fail;
        cur fc = { fv, fvlen, 0 };
        const uint8_t *fname; uint32_t fname_len;
        if ((e = take_string(&fc, &fname, &fname_len))) goto fail;
        if ((e = make_file(s, fname, fname_len, &io[i]))) goto fail;
        made = i + 1;
    }

    if (kind == DC_INDEX_GEO) {
        rt = rtree_create(&io[0], s->order);
        if (!rt) { e = BJ_ERR_STATE; goto fail; }
        const uint8_t *field; uint32_t field_len;
        if ((e = plan_str(def, def_len, "field", &field, &field_len, &found))) goto fail;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        e = dc_collection_add_geo_index(en->coll, (const char *)name, (int)name_len,
                                        rt, (const char *)field, (int)field_len);
        if (e) goto fail;
    } else if (kind == DC_INDEX_TEXT) {
        if (n_files != DBS_MAX_INDEX_FILES) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        for (uint32_t i = 0; i < n_files; i++) {
            tree[i] = bpt_create(&io[i], s->order);
            if (!tree[i]) { e = BJ_ERR_STATE; goto fail; }
        }
        const uint8_t *field; uint32_t field_len;
        if ((e = plan_str(def, def_len, "field", &field, &field_len, &found))) goto fail;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        e = dc_collection_add_text_index(en->coll, (const char *)name, (int)name_len,
                                         tree[0], tree[1], tree[2],
                                         (const char *)field, (int)field_len);
        if (e) goto fail;
    } else {
        tree[0] = bpt_create(&io[0], s->order);
        if (!tree[0]) { e = BJ_ERR_STATE; goto fail; }
        const uint8_t *fields; size_t fields_len;
        if ((e = plan_raw(def, def_len, "fields", &fields, &fields_len, &found))) goto fail;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
        int uniq = 0, sparse = 0;
        if ((e = plan_flag(def, def_len, "unique", &uniq))) goto fail;
        if ((e = plan_flag(def, def_len, "sparse", &sparse))) goto fail;
        const uint8_t *pfe = NULL; size_t pfe_len = 0; int has_pfe = 0;
        if ((e = plan_raw(def, def_len, "partialFilterExpression", &pfe, &pfe_len, &has_pfe))) goto fail;
        e = dc_collection_add_index(en->coll, (const char *)name, (int)name_len,
                                    tree[0], fields, (uint32_t)fields_len,
                                    uniq, sparse,
                                    has_pfe ? pfe : NULL, has_pfe ? (uint32_t)pfe_len : 0);
        if (e) goto fail;
    }

    /* Attached and backfilled. The handles belong to the session's entry
     * now, so the ordinary close path releases them. */
    {
        dbs_index *ix = &en->idx[en->n_idx];
        memset(ix, 0, sizeof(*ix));
        ix->kind = kind;
        ix->n_files = (int)n_files;
        ix->name = (char *)malloc(name_len ? name_len : 1);
        if (!ix->name) { e = BJ_ERR_OOM; goto fail; }
        memcpy(ix->name, name, name_len);
        ix->name_len = name_len;
        for (uint32_t i = 0; i < n_files; i++) { ix->io[i] = io[i]; ix->tree[i] = tree[i]; }
        ix->rt = rt;
        en->n_idx++;
    }
    return BJ_OK;

fail:
    /* Nothing was recorded, so the files this made are garbage THIS call
     * knows about -- deleted here rather than left for a sweep. */
    for (uint32_t i = 0; i < n_files; i++) if (tree[i]) bpt_free(tree[i]);
    if (rt) rtree_free(rt);
    for (uint32_t i = 0; i < made; i++) s->ns->close(s->ns->ctx, &io[i]);
    remove_all(s, files, files_len);
    return e;
}

int dbs_create_index(dbs *s, const char *coll, size_t coll_len,
                     const uint8_t *keys, size_t keys_len,
                     const uint8_t *options, size_t options_len,
                     dbuf *name_out) {
    if (!s || !coll || !keys || !name_out) return BJ_ERR_STATE;

    dc_collection *c = NULL;
    int e = dbs_collection(s, coll, coll_len, &c);
    if (e) return e;
    dbs_entry *en = find_entry(s, coll, coll_len);
    if (!en) return BJ_ERR_STATE;
    if (en->n_idx >= DBS_MAX_INDEXES) return DC_ERR_TOO_MANY_INDEXES;

    /* C decides what kind of index this is, what it is called and which
     * files it needs -- all of it pure, all of it before anything
     * exists. This function creates exactly what the plan named. */
    dbuf plan = {0};
    if ((e = dc_index_create_plan(keys, keys_len, options, options_len,
                                  coll, coll_len, &plan))) {
        dbuf_free(&plan);
        return e;
    }

    int found = 0;
    const uint8_t *name; uint32_t name_len;
    if ((e = plan_str(plan.data, plan.len, "name", &name, &name_len, &found))) {
        dbuf_free(&plan); return e;
    }
    if (!found) { dbuf_free(&plan); return DC_ERR_CATALOG_ENTRY; }

    for (int i = 0; i < en->n_idx; i++) {
        if (en->idx[i].name_len == name_len &&
            memcmp(en->idx[i].name, name, name_len) == 0) {
            dbuf_free(&plan);
            return DC_ERR_INDEX_EXISTS;
        }
    }

    if ((e = build_index(s, en, plan.data, plan.len))) { dbuf_free(&plan); return e; }

    /* Built and attached; now recorded. The plan IS the stored shape's
     * input, so one definition flows create -> catalog -> open rather
     * than being rebuilt by whoever writes it down. */
    uint8_t *entry = NULL; size_t entry_len = 0;
    if ((e = catalog_get(s, coll, coll_len, &entry, &entry_len, &found))) return e;
    if (!found) { free(entry); return DC_ERR_NO_COLLECTION; }

    dbuf updated = {0};
    e = dc_catalog_put_index(entry, entry_len, plan.data, plan.len, &updated);
    free(entry);
    if (!e) e = catalog_put(s, coll, coll_len, updated.data, updated.len);
    dbuf_free(&updated);
    if (!e) e = dbuf_put(name_out, name, name_len);
    dbuf_free(&plan);
    return e;
}

int dbs_drop_index(dbs *s, const char *coll, size_t coll_len,
                   const char *name, size_t name_len) {
    if (!s || !coll || !name) return BJ_ERR_STATE;

    uint8_t *entry = NULL; size_t entry_len = 0; int found = 0;
    int e = catalog_get(s, coll, coll_len, &entry, &entry_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_NO_COLLECTION;

    /*
     * Find the definition before it is gone: its files are named there and
     * nowhere else. WHICH files is db_catalog.h's to say -- this used to
     * read a plan-shaped `files` array straight out of a STORED definition,
     * which has `file` for an equality or geo index and an OBJECT for a
     * text one, so it matched nothing and deleted nothing. Every dropped
     * index leaked its whole file, no crash required, and at the time
     * nothing in C swept it up either. dc_index_files is the one answer the
     * sweep and dropCollection use too.
     */
    dbuf files = {0};
    {
        const uint8_t *indexes; size_t indexes_len; int has = 0;
        if ((e = plan_raw(entry, entry_len, "indexes", &indexes, &indexes_len, &has))) goto done;
        uint32_t n = 0;
        if (has && (e = arr_len(indexes, indexes_len, &n))) goto done;
        int seen = 0;
        for (uint32_t i = 0; has && i < n; i++) {
            const uint8_t *def; size_t def_len;
            if ((e = arr_at(indexes, indexes_len, i, &def, &def_len))) goto done;
            const uint8_t *dn; uint32_t dn_len; int f = 0;
            if ((e = plan_str(def, def_len, "name", &dn, &dn_len, &f))) goto done;
            if (!f || dn_len != name_len || memcmp(dn, name, name_len) != 0) continue;
            seen = 1;
            e = dc_index_files(def, def_len, &files);
            break;
        }
        if (!seen) { e = DC_ERR_NO_INDEX; goto done; }
        if (e) goto done;
    }

    /* The entry first: while it names the index, the files are live. */
    {
        dbuf updated = {0};
        e = dc_catalog_drop_index(entry, entry_len, name, name_len, &updated);
        if (!e) e = catalog_put(s, coll, coll_len, updated.data, updated.len);
        dbuf_free(&updated);
        if (e) goto done;
    }

    /* Then the cursors, the open handles, and the files, in that order.
     * The cursor close is load-bearing: entry_release frees the tree a
     * batched cursor is positioned in, and a cursor that outlives it is a
     * use-after-free on its next getMore (close_all_cursors -- this call
     * being absent HERE, while dropCollection had it, was the bug).
     * Reopened lazily: the next request that wants this collection gets
     * it from the catalog, which no longer mentions the index. */
    {
        dbs_entry *en = find_entry(s, coll, coll_len);
        if (en) {
            close_all_cursors(s);
            entry_release(s->ns, en);
        }
    }
    if (files.len) remove_all(s, files.data, files.len);

done:
    dbuf_free(&files);
    free(entry);
    return e;
}

/*
 * The catalog as dc_sweep_plan's input: a binjson ARRAY of { key, value }
 * rows. That is the shape the browser host produces with the tree's own
 * toArray(), and it is why the sweep takes BYTES rather than a tree --
 * one implementation of "which files does this database own", fed from
 * either side of the bridge.
 */
static int catalog_rows(dbs *s, dbuf *out) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);
    bpt_cursor *cur_h = bpt_cursor_open(s->catalog, NULL, NULL);
    if (!cur_h) { bj_builder_free(b); return BJ_ERR_STATE; }
    for (;;) {
        bpt_key key;
        const uint8_t *val; size_t vlen;
        int r = bpt_cursor_next(cur_h, &key, &val, &vlen);
        if (r < 0) { e = r; break; }
        if (r == 0) break;
        if (!key.is_string) continue;          /* a catalog key is a name */
        if (!e) e = bj_begin_object(b);
        if (!e) e = bj_put_key(b, (const uint8_t *)"key", 3);
        if (!e) e = bj_put_string(b, key.str, key.str_len);
        if (!e) e = bj_put_key(b, (const uint8_t *)"value", 5);
        if (!e) e = bj_put_raw(b, val, (uint32_t)vlen);
        if (!e) e = bj_end_object(b);
        if (e) break;
    }
    bpt_cursor_close(cur_h);
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        e = data ? dbuf_put(out, data, len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/*
 * DELETE WHAT THE CATALOG DOES NOT REFERENCE.
 *
 * Everything above this line assumes somebody sweeps: "an orphan the
 * sweep collects" is the sentence that lets createIndex build files
 * before the catalog names them, lets compact write a whole generation
 * before adopting it, and lets dropIndex and dropCollection commit the
 * catalog before removing anything. Every one of those leaves files
 * nothing references if it is interrupted, and until this function the
 * only host that swept was the browser one -- so a nisaba-server that
 * crashed inside a compaction kept a full second copy of the collection
 * FOR EVER, and every dropped index's file with it. Measured by
 * test/crash-matrix.js, which reports it as a LEAK: correct answers,
 * permanently more disk.
 *
 * The listing is an INPUT because bj_ns cannot produce one (bjns.h says
 * why: OPFS enumeration is asynchronous), so the host that owns the
 * directory reads it and passes it in -- NUL-separated, exactly what
 * server/root.c's root_list_files already returns.
 *
 * SAFE ONLY BEFORE THE DATABASE IS IN USE, which is where dbi_database
 * calls it: on a first open, nothing else holds this session, no cursor
 * is positioned in a file, and no compaction is half-way through writing
 * one. A file that is unreferenced then is garbage by definition -- and
 * "unreferenced" is decided by the same dc_collection_files the drops
 * use, so the two cannot disagree about what a collection is made of.
 */
int dbs_sweep_orphans(dbs *s, const char *names, size_t names_len,
                      uint32_t *deleted) {
    if (deleted) *deleted = 0;
    if (!s || !s->catalog || !s->ns) return BJ_ERR_STATE;
    if (!names || !names_len) return BJ_OK;
    dbuf rows = {0};
    int e = catalog_rows(s, &rows);
    if (!e) e = dc_sweep_execute(s->ns, rows.data, rows.len, names, names_len, deleted);
    dbuf_free(&rows);
    return e;
}

int dbs_list_collections(dbs *s, dbuf *out) {
    if (!s || !out) return BJ_ERR_STATE;

    /* The catalog's keys ARE the collection names -- there is no list
     * kept beside them to fall out of step. Everything except the format
     * stamp, which is a reserved name no collection can have
     * (db_validate.h refuses it) and which every host filters out the
     * same way. */
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);

    bpt_cursor *cur_h = bpt_cursor_open(s->catalog, NULL, NULL);
    if (!cur_h) { bj_builder_free(b); return BJ_ERR_STATE; }

    for (;;) {
        bpt_key key;
        const uint8_t *val; size_t vlen;
        int r = bpt_cursor_next(cur_h, &key, &val, &vlen);
        if (r < 0) { e = r; break; }
        if (r == 0) break;
        if (!key.is_string) continue;          /* a catalog key is a name */
        if (key.str_len == strlen(DC_FORMAT_KEY) &&
            memcmp(key.str, DC_FORMAT_KEY, key.str_len) == 0) continue;
        if ((e = bj_put_string(b, key.str, key.str_len))) break;
    }
    bpt_cursor_close(cur_h);

    if (!e) e = bj_end_array(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        e = data ? dbuf_put(out, data, len) : BJ_ERR_OOM;
    }
    bj_builder_free(b);
    return e;
}

int dbs_ttl_filters(dbs *s, const char *coll, size_t coll_len,
                    int64_t now_ms, dbuf *out) {
    if (!s || !coll || !out) return BJ_ERR_STATE;
    uint8_t *entry = NULL; size_t entry_len = 0; int found = 0;
    int e = catalog_get(s, coll, coll_len, &entry, &entry_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_NO_COLLECTION;

    bj_builder *b = bj_builder_new();
    if (!b) { free(entry); return BJ_ERR_OOM; }
    bj_begin_array(b);

    const uint8_t *idxs; size_t idxs_len; int has = 0;
    e = plan_raw(entry, entry_len, "indexes", &idxs, &idxs_len, &has);
    if (!e && has && idxs_len >= 1 && idxs[0] == BJ_TYPE_ARRAY) {
        cur c = { idxs, idxs_len, 0 };
        uint32_t n = 0;
        e = array_begin(&c, &n);
        for (uint32_t i = 0; !e && i < n; i++) {
            size_t start = c.pos;
            if ((e = skip_value(&c))) break;
            const uint8_t *def = idxs + start;
            size_t def_len = c.pos - start;

            /* expireAfterSeconds is the whole test, and nothing here
             * looks at the index's KIND: db_catalog.h refuses that
             * option on anything but a single-field equality index at
             * creation, so a def carrying it is one by construction.
             * Asking anyway would mean this file deciding what "text"
             * means for a second time -- and a stored entry spells kind
             * as a STRING while an open plan spells it as an int, which
             * is exactly the sort of second opinion that gets one of the
             * two wrong. A def without it is skipped in silence: a
             * collection with no TTL index is owed no sweep. */
            const uint8_t *v; size_t vlen; int hs = 0;
            if ((e = plan_raw(def, def_len, "expireAfterSeconds", &v, &vlen, &hs))) break;
            if (!hs) continue;
            double secs = 0;
            { cur nc = { v, vlen, 0 }; if ((e = read_number(&nc, &secs))) break; }

            /* A TTL index is single-field (db_catalog.h enforces it at
             * creation), so the field is fields[0]. */
            const uint8_t *fields; size_t fields_len; int hf = 0;
            if ((e = plan_raw(def, def_len, "fields", &fields, &fields_len, &hf))) break;
            if (!hf) { e = DC_ERR_CATALOG_ENTRY; break; }
            cur fc = { fields, fields_len, 0 };
            uint32_t fn = 0;
            if ((e = array_begin(&fc, &fn))) break;
            if (fn < 1) { e = DC_ERR_CATALOG_ENTRY; break; }
            const uint8_t *fp; uint32_t flen;
            if ((e = take_string(&fc, &fp, &flen))) break;

            int64_t cutoff = 0;
            if ((e = dc_ttl_cutoff_ms(now_ms, secs, &cutoff))) break;
            dbuf one = {0};
            e = dc_ttl_filter(&one, (const char *)fp, flen, cutoff);
            if (!e) e = bj_put_raw(b, one.data, (uint32_t)one.len);
            dbuf_free(&one);
        }
    }
    free(entry);
    if (!e) { bj_end_array(b); e = bj_builder_error(b); }
    if (!e) {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        e = data ? dbuf_put(out, data, len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

int dbs_list_indexes(dbs *s, const char *coll, size_t coll_len, dbuf *out) {
    if (!s || !coll || !out) return BJ_ERR_STATE;
    uint8_t *entry = NULL; size_t entry_len = 0; int found = 0;
    int e = catalog_get(s, coll, coll_len, &entry, &entry_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_NO_COLLECTION;
    e = dc_catalog_list_indexes(entry, entry_len, out);
    free(entry);
    return e;
}

/* ---- compaction ---------------------------------------------------------
 *
 * The whole of docs/compaction.md, from the side of a host that has no
 * asynchronous opens: plan, stream, flip, reopen, delete. The browser
 * needs a pre-open pass between the plan and the execute because OPFS
 * opens are promises; here ns->open really opens, so the same two calls
 * sit next to each other with nothing between them. That difference --
 * and only that difference -- is what the plan/execute split buys.
 *
 * Refused while a cursor is scanning: dc_compact_execute returns
 * DC_ERR_CURSORS_OPEN before writing a byte, because the scan is
 * positioned in files this is about to replace. Nothing here has to
 * remember that; the tree counts its readers (bpt_pinned).
 */

static dbs_entry *find_entry(dbs *s, const char *name, size_t name_len) {
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) {
        dbs_entry *en = &s->open[i];
        if (en->used && en->name_len == name_len && memcmp(en->name, name, name_len) == 0)
            return en;
    }
    return NULL;
}

/* Every byte this collection currently occupies: the primary, the
 * journal, and each index file. The same measure before and after, which
 * is the only way the difference means anything. */
static uint64_t entry_bytes(const dbs_entry *en) {
    uint64_t n = en->primary_io.size ? en->primary_io.size(en->primary_io.ctx) : 0;
    if (en->has_journal && en->journal_io.size) n += en->journal_io.size(en->journal_io.ctx);
    for (int i = 0; i < en->n_idx; i++) {
        for (int j = 0; j < en->idx[i].n_files; j++) {
            const bj_io *io = &en->idx[i].io[j];
            if (io->size) n += io->size(io->ctx);
        }
    }
    return n;
}

/* The live structures to stream, in the plan's build order: the primary,
 * then each index's files in the order the plan named them. Matched to
 * the session's open indexes BY NAME -- see dbs_index.name. */
static int gather_sources(dbs_entry *en, const uint8_t *plan, size_t plan_len,
                          void **sources, int *kinds, uint32_t cap, uint32_t *n_out) {
    uint32_t n = 0;
    if (cap == 0) return BJ_ERR_RANGE;
    sources[n] = en->primary; kinds[n] = DC_SRC_BPT; n++;

    const uint8_t *build; size_t build_len; int found = 0;
    int e = plan_raw(plan, plan_len, "build", &build, &build_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;
    uint32_t n_build = 0;
    if ((e = arr_len(build, build_len, &n_build))) return e;

    for (uint32_t i = 0; i < n_build; i++) {
        const uint8_t *def; size_t def_len;
        if ((e = arr_at(build, build_len, i, &def, &def_len))) return e;

        const uint8_t *name; uint32_t name_len;
        if ((e = plan_str(def, def_len, "name", &name, &name_len, &found))) return e;
        if (!found) return DC_ERR_CATALOG_ENTRY;

        dbs_index *ix = NULL;
        for (int j = 0; j < en->n_idx; j++) {
            if (en->idx[j].name_len == name_len &&
                memcmp(en->idx[j].name, name, name_len) == 0) { ix = &en->idx[j]; break; }
        }
        /* A plan naming an index this session does not hold open means
         * the catalog and the open collection disagree, which is a
         * corrupt database rather than a bad request. */
        if (!ix) return DC_ERR_CATALOG_ENTRY;

        const uint8_t *files; size_t files_len;
        if ((e = plan_raw(def, def_len, "files", &files, &files_len, &found))) return e;
        if (!found) return DC_ERR_CATALOG_ENTRY;
        uint32_t n_files = 0;
        if ((e = arr_len(files, files_len, &n_files))) return e;
        if ((int)n_files != ix->n_files) return DC_ERR_CATALOG_ENTRY;

        for (uint32_t f = 0; f < n_files; f++) {
            if (n >= cap) return BJ_ERR_RANGE;
            if (ix->rt) { sources[n] = ix->rt; kinds[n] = DC_SRC_RTREE; }
            else        { sources[n] = ix->tree[f]; kinds[n] = DC_SRC_BPT; }
            n++;
        }
    }
    *n_out = n;
    return BJ_OK;
}

int dbs_compact(dbs *s, const char *name, size_t name_len, dbs_compact_stats *out) {
    if (!s || !name || !out) return BJ_ERR_STATE;
    memset(out, 0, sizeof(*out));

    /* Open it first: the sources are the live structures, so there has to
     * be something live to stream from. */
    dc_collection *coll = NULL;
    int e = dbs_collection(s, name, name_len, &coll);
    if (e) return e;
    dbs_entry *en = find_entry(s, name, name_len);
    if (!en) return BJ_ERR_STATE;

    bpt_key key = { .is_string = 1, .num = 0,
                    .str = (const uint8_t *)name, .str_len = (uint32_t)name_len };
    int found = 0;
    const uint8_t *vp = NULL; size_t vlen = 0;
    if ((e = bpt_search(s->catalog, &key, &found, &vp, &vlen))) return e;
    if (!found) return DC_ERR_NO_COLLECTION;

    /* Copied for the same reason dbs_collection copies it: this is the
     * catalog's own output buffer, and the flip below writes to that very
     * tree. */
    uint8_t *entry = NULL; size_t entry_len = 0;
    if ((e = dbuf_dup(vp, vlen, &entry, &entry_len))) return e;

    dbuf plan = {0};
    e = dc_compact_plan(entry, entry_len, name, name_len, &plan);
    free(entry);
    if (e) { dbuf_free(&plan); return e; }

    void *sources[1 + DBS_MAX_INDEXES * DBS_MAX_INDEX_FILES];
    int kinds[1 + DBS_MAX_INDEXES * DBS_MAX_INDEX_FILES];
    uint32_t nsources = 0;
    e = gather_sources(en, plan.data, plan.len, sources, kinds,
                       (uint32_t)(sizeof sources / sizeof sources[0]), &nsources);
    if (e) { dbuf_free(&plan); return e; }

    int gen = 0;
    if ((e = plan_int(plan.data, plan.len, "gen", &gen, &found))) { dbuf_free(&plan); return e; }

    out->bytes_before = entry_bytes(en);

    uint64_t built = 0;
    e = dc_compact_execute(s->ns, s->catalog, name, name_len, plan.data, plan.len,
                           sources, kinds, nsources, &built);
    if (e) { dbuf_free(&plan); return e; }   /* nothing flipped; still on the old generation */

    /*
     * Flipped. From here the new generation is authoritative, so every
     * remaining step is best-effort in the sense that failing one does
     * not put the old files back -- reopening is what a failure here
     * costs, and the catalog already says which generation is real.
     *
     * The session's handles are the OLD files: let them go before the
     * names are unlinked, and reopen from the flipped catalog.
     */
    entry_release(s->ns, en);

    const uint8_t *old_files; size_t old_len; int have_old = 0;
    if (plan_raw(plan.data, plan.len, "oldFiles", &old_files, &old_len, &have_old) == BJ_OK && have_old) {
        uint32_t n_old = 0;
        if (arr_len(old_files, old_len, &n_old) == BJ_OK) {
            for (uint32_t i = 0; i < n_old; i++) {
                const uint8_t *fv; size_t fvlen;
                if (arr_at(old_files, old_len, i, &fv, &fvlen)) break;
                cur c = { fv, fvlen, 0 };
                const uint8_t *fp; uint32_t flen;
                if (take_string(&c, &fp, &flen)) continue;
                /* Best effort, like every other sweep: a name left behind
                 * is an orphan the next sweep collects, never a
                 * correctness problem (db_catalog.h). */
                s->ns->remove(s->ns->ctx, (const char *)fp, flen);
            }
        }
    }
    dbuf_free(&plan);

    /* Reopen, which is also the proof: if the new generation cannot be
     * opened, the caller hears about it now rather than on the next
     * request. */
    e = dbs_collection(s, name, name_len, &coll);
    if (e) return e;
    en = find_entry(s, name, name_len);
    out->bytes_after = en ? entry_bytes(en) : built;
    out->generation = gen;
    return BJ_OK;
}

/* The size this collection's files were right after its last
 * compaction, as the catalog recorded it at the flip. 0 when it has
 * never been compacted, which is what makes `factor` alone let a fresh
 * collection through. */
static int compacted_bytes(dbs *s, const char *name, size_t name_len, double *out) {
    *out = 0;
    uint8_t *entry = NULL; size_t entry_len = 0; int found = 0;
    int e = catalog_get(s, name, name_len, &entry, &entry_len, &found);
    if (e) return e;
    if (!found) { free(entry); return DC_ERR_NO_COLLECTION; }
    const uint8_t *v; size_t vlen; int has = 0;
    e = plan_raw(entry, entry_len, "compactedBytes", &v, &vlen, &has);
    if (!e && has) {
        cur c = { v, vlen, 0 };
        e = read_number(&c, out);
    }
    free(entry);
    return e;
}

int dbs_compact_all(dbs *s, int64_t min_bytes, double factor, int skip_busy,
                    dbuf *out) {
    if (!s || !out) return BJ_ERR_STATE;
    dbuf names = {0};
    int e = dbs_list_collections(s, &names);
    if (e) { dbuf_free(&names); return e; }

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&names); return BJ_ERR_OOM; }
    bj_begin_object(b);

    cur c = { names.data, names.len, 0 };
    uint32_t n = 0;
    e = array_begin(&c, &n);
    for (uint32_t i = 0; !e && i < n; i++) {
        const uint8_t *np; uint32_t nlen;
        if ((e = take_string(&c, &np, &nlen))) break;
        const char *name = (const char *)np;

        int skip = 0;
        if (min_bytes > 0 || factor > 0) {
            /* Measuring means opening: the sizes are the live files'.
             * The in-process sweep opens each one too. */
            dc_collection *coll = NULL;
            if ((e = dbs_collection(s, name, nlen, &coll))) break;
            dbs_entry *en = find_entry(s, name, nlen);
            uint64_t now_bytes = en ? entry_bytes(en) : 0;
            double was = 0;
            if ((e = compacted_bytes(s, name, nlen, &was))) break;
            double floor_bytes = (double)min_bytes;
            if (factor > 0 && factor * was > floor_bytes) floor_bytes = factor * was;
            if ((double)now_bytes < floor_bytes) skip = 1;
        }

        dbs_compact_stats st;
        int rc = skip ? 0 : dbs_compact(s, name, nlen, &st);
        /* Busy is a skip for a sweep and an error for a request that
         * named one collection: the difference is whether anybody is
         * waiting to hear about this particular one. */
        if (!skip && rc == DC_ERR_CURSORS_OPEN && skip_busy) { skip = 1; rc = 0; }
        if (rc) { e = rc; break; }

        bj_put_key(b, np, nlen);
        if (skip) {
            bj_put_null(b);
        } else {
            bj_begin_object(b);
            bj_put_key(b, (const uint8_t *)"generation", 10);
            bj_put_int(b, st.generation);
            bj_put_key(b, (const uint8_t *)"bytesBefore", 11);
            bj_put_int(b, (int64_t)st.bytes_before);
            bj_put_key(b, (const uint8_t *)"bytesAfter", 10);
            bj_put_int(b, (int64_t)st.bytes_after);
            bj_put_key(b, (const uint8_t *)"bytesFreed", 10);
            bj_put_int(b, st.bytes_before > st.bytes_after
                          ? (int64_t)(st.bytes_before - st.bytes_after) : 0);
            bj_end_object(b);
        }
    }
    dbuf_free(&names);
    if (!e) { bj_end_object(b); e = bj_builder_error(b); }
    if (!e) {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        e = data ? dbuf_put(out, data, len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

/* ---- cursors ------------------------------------------------------------
 *
 * A fixed table, refused explicitly when full, like every other table
 * here. What makes cursors different from open collections is that they
 * have an OWNER: a collection is the session's for as long as the
 * session lives, but a cursor belongs to whoever opened it and dies with
 * them.
 */

int dbs_cursor_add(dbs *s, uint64_t client, dc_cursor *cur, uint32_t batch,
                   uint64_t *id_out) {
    if (!s || !cur || !id_out) return BJ_ERR_STATE;
    for (int i = 0; i < DBS_MAX_CURSORS; i++) {
        if (s->cursors[i].id) continue;
        s->cursors[i].id     = s->id_source ? ++*s->id_source : ++s->next_cursor_id;
        s->cursors[i].client = client;
        s->cursors[i].batch  = batch;
        s->cursors[i].cur    = cur;
        *id_out = s->cursors[i].id;
        return BJ_OK;
    }
    return DC_ERR_TOO_MANY_CURSORS;
}

/* Everything a stream owns, given back. Its slot is free afterwards and
 * its id is never reused. */
static void stream_release(dbs_stream *st) {
    free(st->coll);
    dbuf_free(&st->queue);
    memset(st, 0, sizeof *st);
}

/* A cursor is findable only by the client that opened it. "No such
 * cursor" and "not yours" are deliberately the same answer: telling them
 * apart would tell a client about another client's cursors. */
static dbs_cursor *cursor_slot(dbs *s, uint64_t client, uint64_t id) {
    if (!s || !id) return NULL;
    for (int i = 0; i < DBS_MAX_CURSORS; i++) {
        if (s->cursors[i].id == id && s->cursors[i].client == client)
            return &s->cursors[i];
    }
    return NULL;
}

int dbs_cursor_get(dbs *s, uint64_t client, uint64_t id,
                   dc_cursor **out, uint32_t *batch_out) {
    dbs_cursor *slot = cursor_slot(s, client, id);
    if (!slot) return DC_ERR_NO_CURSOR;
    if (out) *out = slot->cur;
    if (batch_out) *batch_out = slot->batch;
    return BJ_OK;
}

int dbs_cursor_drop(dbs *s, uint64_t client, uint64_t id) {
    dbs_cursor *slot = cursor_slot(s, client, id);
    if (!slot) return DC_ERR_NO_CURSOR;
    dc_cursor_close(slot->cur);
    memset(slot, 0, sizeof *slot);
    return BJ_OK;
}

void dbs_drop_client(dbs *s, uint64_t client) {
    if (!s) return;
    /* A planned write whose client has gone is a plan nobody will ever
     * collect. The entries it proposed may still commit and be applied
     * by the pump like any other -- that is what committed means -- but
     * the response has nowhere to go. */
    for (int i = 0; i < DBS_MAX_INFLIGHT; i++)
        if (s->pending[i].token && s->pending[i].client == client)
            pending_release(&s->pending[i]);
    for (int i = 0; i < DBS_MAX_CURSORS; i++) {
        if (!s->cursors[i].id || s->cursors[i].client != client) continue;
        dc_cursor_close(s->cursors[i].cur);
        memset(&s->cursors[i], 0, sizeof s->cursors[i]);
    }
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        if (!s->streams[i].id || s->streams[i].client != client) continue;
        stream_release(&s->streams[i]);
    }
}

/* ---- change streams -----------------------------------------------------
 *
 * See db_session.h. A stream is a cursor's twin in every way that
 * matters -- a bounded table, an owner, a death with its client -- and
 * its opposite in one: a cursor is pulled, and this is pushed.
 */

int dbs_set_log(dbs *s, const dbs_log *log, const char *db_name, size_t db_len) {
    if (!s) return BJ_ERR_STATE;
    free(s->db_name);
    s->db_name = NULL;
    s->db_name_len = 0;
    memset(&s->log, 0, sizeof s->log);
    if (!log) return BJ_OK;
    s->db_name = (char *)malloc(db_len ? db_len : 1);
    if (!s->db_name) return BJ_ERR_OOM;
    memcpy(s->db_name, db_name, db_len);
    s->db_name_len = db_len;
    s->log = *log;
    return BJ_OK;
}

int dbs_has_log(const dbs *s)  { return s && s->log.entry != NULL; }
uint64_t dbs_log_base(dbs *s)  { return dbs_has_log(s) ? s->log.base(s->log.ctx)  : 0; }
uint64_t dbs_log_floor(dbs *s) { return dbs_has_log(s) ? s->log.floor(s->log.ctx) : 0; }

int dbs_log_entry(dbs *s, uint64_t index, dbuf *cmd) {
    if (!dbs_has_log(s) || !cmd) return BJ_ERR_STATE;
    return s->log.entry(s->log.ctx, s->db_name, s->db_name_len, index, cmd);
}

int dbs_watch(dbs *s, uint64_t client, const char *coll, size_t coll_len,
              uint64_t *id_out) {
    if (!s || !coll || !id_out) return BJ_ERR_STATE;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        dbs_stream *st = &s->streams[i];
        if (st->id) continue;
        st->coll = (char *)malloc(coll_len ? coll_len : 1);
        if (!st->coll) return BJ_ERR_OOM;
        memcpy(st->coll, coll, coll_len);
        st->coll_len = coll_len;
        st->id       = s->id_source ? ++*s->id_source : ++s->next_stream_id;
        st->client   = client;
        st->count      = 0;
        st->last_index = 0;
        st->overflowed = 0;
        *id_out = st->id;
        return BJ_OK;
    }
    return DC_ERR_TOO_MANY_STREAMS;
}

int dbs_close_stream(dbs *s, uint64_t client, uint64_t id) {
    if (!s || !id) return DC_ERR_NO_STREAM;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        dbs_stream *st = &s->streams[i];
        if (st->id != id || st->client != client) continue;
        stream_release(st);
        return BJ_OK;
    }
    return DC_ERR_NO_STREAM;
}

/*
 * The replay floor: the highest log index this database has actually
 * applied, across every collection it has.
 *
 * Apply is strictly ordered, so the MAX is the applied prefix -- the
 * same derivation DbStateMachine.appliedIndex makes in JavaScript, and
 * the same reason: dc_wal_apply stages the index atomically with the
 * mutation, so each collection's value is durable with the data it
 * describes.
 *
 * NOT the log's commit marker, which is advisory and rides the next sync
 * -- it can be behind what was applied, and resuming from it would apply
 * committed commands a second time. An insert would come back a
 * duplicate, which is survivable; an $inc would silently count twice,
 * which is not.
 */
uint64_t dbs_applied_floor(dbs *s) {
    if (!s) return 0;
    dbuf names = {0};
    /*
     * THE CATALOG COUNTS TOO, and it is the only term here that a
     * dropCollection cannot take away with it. Without it this max is over
     * surviving collections alone, so a drop makes the floor regress and a
     * compacted log leaves nothing to bridge the gap -- see
     * catalog_note_applied for what that costs.
     */
    uint64_t floor = s->catalog ? bpt_applied_index(s->catalog) : 0;
    if (dbs_list_collections(s, &names) != BJ_OK) { dbuf_free(&names); return floor; }
    cur c = { names.data, names.len, 0 };
    uint32_t count = 0;
    if (array_begin(&c, &count) == BJ_OK) {
        for (uint32_t i = 0; i < count; i++) {
            const uint8_t *name; uint32_t nlen;
            if (take_string(&c, &name, &nlen)) break;
            /* Opening it is what reads its staged value; a collection
             * nothing has asked for yet has no context to ask. The
             * handle is wanted, not merely the side effect: dbs_collection
             * refuses a NULL `out` outright, so asking without one
             * opened nothing and this floor was always zero -- which is
             * a replica replaying its whole log on every restart. */
            dc_collection *coll = NULL;
            if (dbs_collection(s, (const char *)name, nlen, &coll) != BJ_OK) continue;
            uint64_t at = dc_applied_index(coll);
            if (at > floor) floor = at;
        }
    }
    dbuf_free(&names);
    return floor;
}

uint64_t dbs_applied_index(dbs *s, const char *coll, size_t coll_len) {
    if (!s || !coll) return 0;
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) {
        const dbs_entry *en = &s->open[i];
        if (!en->used || en->name_len != coll_len || memcmp(en->name, coll, coll_len)) continue;
        return dc_applied_index(en->coll);
    }
    return 0;
}

int dbs_watched(const dbs *s, const char *coll, size_t coll_len) {
    if (!s || !coll) return 0;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        const dbs_stream *st = &s->streams[i];
        if (st->id && !st->overflowed && st->coll_len == coll_len &&
            memcmp(st->coll, coll, coll_len) == 0)
            return 1;
    }
    return 0;
}

/* The one enqueue, shared by broadcast and replay: full-or-would-be
 * overflows the stream, and the event is not queued and never will be --
 * from there the stream owes its consumer one sentence saying so (with
 * the last index it DID deliver), and nothing else. */
static int stream_enqueue(dbs_stream *st, uint64_t index,
                          const uint8_t *event, size_t len) {
    if (st->count >= DBS_STREAM_EVENTS ||
        st->queue.len + len > DBS_STREAM_BYTES ||
        dbuf_put(&st->queue, event, len) != BJ_OK) {
        st->overflowed = 1;
        return 0;
    }
    st->idx[st->count++] = index;
    return 1;
}

void dbs_emit(dbs *s, const char *coll, size_t coll_len, uint64_t index,
              const uint8_t *event, size_t len) {
    if (!s || !coll || !event || !len) return;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        dbs_stream *st = &s->streams[i];
        if (!st->id || st->overflowed) continue;
        if (st->coll_len != coll_len || memcmp(st->coll, coll, coll_len) != 0) continue;
        stream_enqueue(st, index, event, len);
    }
}

int dbs_stream_feed(dbs *s, uint64_t client, uint64_t id, uint64_t index,
                    const uint8_t *event, size_t len, int *fed) {
    if (!s || !event || !len || !fed) return BJ_ERR_STATE;
    *fed = 0;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        dbs_stream *st = &s->streams[i];
        if (st->id != id || st->client != client) continue;
        if (!st->overflowed) *fed = stream_enqueue(st, index, event, len);
        return BJ_OK;
    }
    return DC_ERR_NO_STREAM;
}

int dbs_stream_take(dbs *s, uint64_t client, dbuf *out, int *have) {
    if (!s || !out || !have) return BJ_ERR_STATE;
    *have = 0;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        dbs_stream *st = &s->streams[i];
        if (!st->id || st->client != client) continue;

        /* Queued events first, even on an overflowed stream: what was
         * collected before it filled is still true, and the consumer is
         * owed it before the news that the rest is gone. */
        if (st->count) {
            cur c = { st->queue.data, st->queue.len, 0 };
            size_t start = c.pos;
            int e = skip_value(&c);
            if (e) { stream_release(st); return e; }
            bj_builder *b = bj_builder_new();
            if (!b) return BJ_ERR_OOM;
            bj_begin_object(b);
            bj_put_key(b, (const uint8_t *)"stream", 6);
            bj_put_int(b, (int64_t)st->id);
            bj_put_key(b, (const uint8_t *)"event", 5);
            bj_put_raw(b, st->queue.data + start, (uint32_t)(c.pos - start));
            bj_end_object(b);
            size_t len = 0;
            const uint8_t *data = bj_builder_data(b, &len);
            e = data ? dbuf_put(out, data, len) : BJ_ERR_STATE;
            bj_builder_free(b);
            if (e) return e;
            /* Consumed: shift the rest down. A queue that is normally
             * empty or short does not earn a ring buffer. The index
             * array shifts with it, and the departing one is now the
             * highest this consumer has been GIVEN -- the fact the
             * overflow frame reports. */
            memmove(st->queue.data, st->queue.data + c.pos, st->queue.len - c.pos);
            st->queue.len -= c.pos;
            if (st->idx[0]) st->last_index = st->idx[0];
            memmove(st->idx, st->idx + 1, (st->count - 1) * sizeof st->idx[0]);
            st->count--;
            *have = 1;
            return BJ_OK;
        }

        if (st->overflowed) {
            uint64_t id = st->id;
            uint64_t last = st->last_index;
            stream_release(st);          /* said once, then gone */
            bj_builder *b = bj_builder_new();
            if (!b) return BJ_ERR_OOM;
            bj_begin_object(b);
            bj_put_key(b, (const uint8_t *)"stream", 6);
            bj_put_int(b, (int64_t)id);
            bj_put_key(b, (const uint8_t *)"overflow", 8);
            bj_put_bool(b, 1);
            if (last) {
                /* The resume point, when a log minted one: everything
                 * through `last` was delivered, so `from: last` misses
                 * nothing. Absent when there is no log, because a number
                 * nobody can act on is noise dressed as help. */
                bj_put_key(b, (const uint8_t *)"index", 5);
                bj_put_int(b, (int64_t)last);
            }
            bj_end_object(b);
            size_t len = 0;
            const uint8_t *data = bj_builder_data(b, &len);
            int e = data ? dbuf_put(out, data, len) : BJ_ERR_STATE;
            bj_builder_free(b);
            if (e) return e;
            *have = 1;
            return BJ_OK;
        }
    }
    return BJ_OK;
}

int dbs_stream_pending(const dbs *s) {
    if (!s) return 0;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        const dbs_stream *st = &s->streams[i];
        if (st->id && (st->count || st->overflowed)) return 1;
    }
    return 0;
}

int dbs_stream_count(const dbs *s) {
    int n = 0;
    if (!s) return 0;
    for (int i = 0; i < DBS_MAX_STREAMS; i++) if (s->streams[i].id) n++;
    return n;
}

int dbs_cursor_count(const dbs *s) {
    if (!s) return 0;
    int n = 0;
    for (int i = 0; i < DBS_MAX_CURSORS; i++) if (s->cursors[i].id) n++;
    return n;
}

int dbs_open_count(const dbs *s) {
    if (!s) return 0;
    int n = 0;
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) if (s->open[i].used) n++;
    return n;
}

int dbs_read_view(dbs *s, const char *name, size_t name_len, dc_collection **out) {
    if (!s || !name || !out) return BJ_ERR_STATE;
    /* Through dbs_collection rather than find_entry: a view of a
     * collection this session happens not to have touched yet is the same
     * view as one of a collection it has, and making the caller notice the
     * difference would only invite it to guess. */
    dc_collection *live = NULL;
    int e = dbs_collection(s, name, name_len, &live);
    if (e) return e;
    return dc_collection_snapshot(live, out);
}

void dbs_read_view_close(dc_collection *view) {
    /* dc_collection_free is what frees a view's handles (db.h): one free
     * function for both kinds, so there is never a question about which to
     * call. This wrapper exists so a caller holding a view need not know
     * that a view is a dc_collection at all. */
    dc_collection_free(view);
}

/* ---- applying a committed command --------------------------------------
 *
 * See db_session.h. Everything above this line is asked for by a client;
 * this is asked for by a LOG, and the difference matters in one place:
 * a client's request may be refused for being wrong, and a committed
 * command has already been agreed on. It is performed, and whatever
 * performing it produced -- including a failure every replica shares --
 * is reported rather than judged.
 */

/* { <key>: <bool> } -- the whole result of a drop. */
static int flag_result(dbuf *out, const char *key, int value) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
    if (!e) e = bj_put_bool(b, value);
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *d = bj_builder_data(b, &n);
        if (!d) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_put(out, d, n);
    }
    bj_builder_free(b);
    return e;
}

/* { name: "<the name the catalog chose>" } -- the whole result of an
 * index build, and the same thing the in-process createIndex returns. */
static int name_result(dbuf *out, const dbuf *name) {
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"name", 4);
    if (!e) e = bj_put_string(b, name->data, (uint32_t)name->len);
    if (!e) e = bj_end_object(b);
    if (!e) {
        size_t n; const uint8_t *d = bj_builder_data(b, &n);
        if (!d) e = bj_builder_error(b) ? bj_builder_error(b) : BJ_ERR_STATE;
        else e = dbuf_put(out, d, n);
    }
    bj_builder_free(b);
    return e;
}

int dbs_apply(dbs *s, uint64_t index, const uint8_t *cmd, uint32_t len,
              dbuf *result) {
    if (!s || !cmd || !result) return BJ_ERR_STATE;

    /* Parsed against the grammar first, so a command naming an op this
     * build cannot execute is REFUSED rather than skipped -- the
     * difference between a node that stops and one that has quietly
     * diverged from its peers. */
    int op = -1;
    const uint8_t *coll = NULL; uint32_t coll_len = 0;
    int e = dc_wal_parse(cmd, len, &op, &coll, &coll_len);
    if (e) return e;

    /*
     * The DDL three record their index on the CATALOG, before the mutation,
     * exactly as a document op records its own on the collection. See
     * catalog_note_applied: this is the only record that survives the drop
     * whose files carry every other one, and without it the replay floor
     * regresses and a compacted log cannot bridge the gap.
     */
    if (op == DC_WAL_DROP_COLLECTION || op == DC_WAL_CREATE_INDEX ||
        op == DC_WAL_DROP_INDEX) {
        if ((e = catalog_note_applied(s, index))) return e;
    }

    if (op == DC_WAL_DROP_COLLECTION) {
        int dropped = 0;
        e = dbs_drop_collection(s, (const char *)coll, coll_len, &dropped);
        if (e) return e;
        return flag_result(result, "dropped", dropped);
    }

    if (op == DC_WAL_CREATE_INDEX) {
        const uint8_t *keys = NULL, *options = NULL;
        uint32_t keys_len = 0, options_len = 0;
        e = dc_wal_index_spec(cmd, len, &keys, &keys_len, &options, &options_len);
        if (e) return e;
        dbuf name = {0};
        e = dbs_create_index(s, (const char *)coll, coll_len,
                             keys, keys_len, options, options_len, &name);
        if (!e) e = name_result(result, &name);
        dbuf_free(&name);
        return e;
    }

    if (op == DC_WAL_DROP_INDEX) {
        const uint8_t *name = NULL; uint32_t name_len = 0;
        e = dc_wal_index_name(cmd, len, &name, &name_len);
        if (e) return e;
        e = dbs_drop_index(s, (const char *)coll, coll_len,
                           (const char *)name, name_len);
        if (e) return e;
        return flag_result(result, "dropped", 1);
    }

    dc_collection *c = NULL;
    e = dbs_collection(s, (const char *)coll, coll_len, &c);
    /* A first insert makes the collection, the way it does on the wire
     * and in every host of this library -- which is what lets
     * createCollection have no command of its own. Only an insert: any
     * other command naming a collection that is not here is a replica
     * whose state does not match the log it is applying, and that is a
     * thing to stop on rather than to paper over. */
    if (e == DC_ERR_NO_COLLECTION && op == DC_WAL_INSERT) {
        int made = 0;
        e = dbs_create_collection(s, (const char *)coll, coll_len, &made);
        if (!e) e = dbs_collection(s, (const char *)coll, coll_len, &c);
    }
    if (e) return e;

    return dc_wal_apply(c, index, cmd, len, result);
}

void dbs_close(dbs *s) {
    if (!s) return;
    /* Plans held for a quorum that is not coming: they point at the
     * collections the loop below frees. */
    for (int i = 0; i < DBS_MAX_INFLIGHT; i++) {
        if (s->pending[i].token) pending_release(&s->pending[i]);
    }
    /* Cursors first: a scan is positioned against a tree the loop below
     * is about to free. */
    for (int i = 0; i < DBS_MAX_CURSORS; i++) {
        if (s->cursors[i].id) dc_cursor_close(s->cursors[i].cur);
    }
    /* And every stream: whatever it was still holding for a consumer
     * goes with the session that was holding it. */
    for (int i = 0; i < DBS_MAX_STREAMS; i++) {
        if (s->streams[i].id) stream_release(&s->streams[i]);
    }
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) {
        if (s->open[i].used) entry_release(s->ns, &s->open[i]);
    }
    if (s->catalog) bpt_free(s->catalog);
    s->ns->close(s->ns->ctx, &s->catalog_io);
    free(s->db_name);
    free(s);
}

/* ---- the replicated fork ------------------------------------------------
 *
 * See db_session.h. What db_request.c needs from this side is four
 * questions: am I planning, here is the plan I made, what plan am I
 * resuming, and which log index does the next command go in at.
 */

int dbs_repl_active(const dbs *s) { return s && s->resuming != NULL; }

/* Keep a plan. The slot is already chosen -- dbs_propose takes one
 * before dispatching, because a request that cannot be held is one that
 * must be refused before it plans anything. */
int dbs_repl_hold(dbs *s, dc_wal_plan *p) {
    if (!s || !s->resuming) return BJ_ERR_STATE;
    dbs_pending *e = s->resuming;
    e->more = 1;
    if (e->nplans == e->plan_cap) {
        uint32_t cap = e->plan_cap ? e->plan_cap * 2 : 4;
        dc_wal_plan **grown = (dc_wal_plan **)realloc(e->plans, cap * sizeof *grown);
        if (!grown) return BJ_ERR_OOM;
        e->plans = grown;
        e->plan_cap = cap;
    }
    e->plans[e->nplans++] = p;
    return BJ_OK;
}

/* The next plan this request already made. NULL means there is none yet
 * at this point in the dispatch, so plan one. */
dc_wal_plan *dbs_repl_resuming(dbs *s) {
    if (!s || !s->resuming) return NULL;
    dbs_pending *e = s->resuming;
    if (e->at_plan >= e->nplans) return NULL;
    return e->plans[e->at_plan++];
}

/*
 * What applying the next command produced.
 *
 * NOT an apply. The pump already performed it -- on every replica alike,
 * which is what makes it the one place a committed command is performed
 * -- and this hands the result back so the response can be assembled
 * from it. A cached one on a later pass and the caller's fresh one on
 * this pass are the same thing from here.
 *
 * BJ_ERR_RANGE when there is neither, which is a caller that stepped
 * with fewer results than the plan has commands.
 */
int dbs_repl_applied(dbs *s, dbuf *result, int *rc) {
    if (!s || !s->resuming) return BJ_ERR_STATE;
    dbs_pending *e = s->resuming;

    if (e->at_applied < e->napplied && e->applied[e->at_applied].done) {
        dbs_applied *a = &e->applied[e->at_applied++];
        *rc = a->rc;
        result->len = 0;
        return dbuf_put(result, a->result.data, a->result.len);
    }
    if (!s->results || e->at_applied - e->napplied >= s->nresults) return BJ_ERR_RANGE;

    const dbs_result *src = &s->results[e->at_applied - e->napplied];
    if (e->napplied == e->applied_cap) {
        uint32_t cap = e->applied_cap ? e->applied_cap * 2 : 8;
        dbs_applied *grown = (dbs_applied *)realloc(e->applied, cap * sizeof *grown);
        if (!grown) return BJ_ERR_OOM;
        memset(grown + e->applied_cap, 0, (cap - e->applied_cap) * sizeof *grown);
        e->applied = grown;
        e->applied_cap = cap;
    }
    dbs_applied *a = &e->applied[e->napplied++];
    a->result.len = 0;
    int err = src->len ? dbuf_put(&a->result, src->data, src->len) : BJ_OK;
    a->rc = src->rc;
    a->done = 1;
    e->at_applied = e->napplied;
    *rc = a->rc;
    result->len = 0;
    return err ? err : dbuf_put(result, a->result.data, a->result.len);
}

/*
 * The index the next command committed at.
 *
 * Zero once the caller's list is exhausted, which is not a hole to be
 * filled quietly: dc_wal_apply stages a zero as "not log-driven", and a
 * replica that staged one would come back believing it had applied
 * nothing. The mismatch is the caller's bug (it proposed fewer commands
 * than the plan produced) and dbs_complete refuses on it below.
 */
uint64_t dbs_repl_next_index(dbs *s) {
    if (!s || !s->indices || s->at_index >= s->nindices) return 0;
    return s->indices[s->at_index++];
}

static dbs_pending *pending_find(dbs *s, uint64_t token) {
    for (int i = 0; i < DBS_MAX_INFLIGHT; i++)
        if (s->pending[i].token == token) return &s->pending[i];
    return NULL;
}

uint32_t dbs_inflight(const dbs *s) {
    uint32_t n = 0;
    if (s) for (int i = 0; i < DBS_MAX_INFLIGHT; i++) if (s->pending[i].token) n++;
    return n;
}

void dbs_abandon(dbs *s, uint64_t token) {
    if (!s) return;
    dbs_pending *p = pending_find(s, token);
    if (p) pending_release(p);
}

/*
 * Run the request once more: apply everything already planned -- from
 * the cache, so nothing is applied twice -- plan at most one more thing,
 * and stop.
 *
 * The response it builds is only worth keeping on the LAST pass, when
 * nothing new was planned. Any earlier one describes applies that have
 * not happened yet, as zeroes, and a client must never see those.
 */
static int pass(dbs *s, dbs_pending *slot, dbuf *out) {
    const size_t out_was = out->len;
    slot->at_plan = slot->at_applied = 0;
    slot->more = 0;
    s->resuming = slot;
    int e = dbs_handle(s, slot->client, slot->req.data, slot->req.len, out);
    s->resuming = NULL;
    if (!e && slot->more) out->len = out_was;
    return e;
}

/* The commands of the plan this pass just made -- the only ones that
 * still need replicating. Everything before them is applied already, and
 * proposing it again would apply it twice. */
static int pass_commands(const dbs_pending *pd, dbuf *out) {
    const dc_wal_plan *p = pd->plans[pd->nplans - 1];
    out->len = 0;
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_array(b);
    uint32_t n = dc_wal_plan_count(p);
    for (uint32_t i = 0; !e && i < n; i++) {
        uint32_t clen = 0;
        const uint8_t *cmd = dc_wal_plan_cmd(p, i, &clen);
        if (!cmd) { e = BJ_ERR_STATE; break; }
        e = bj_put_raw(b, cmd, clen);
    }
    if (!e) e = bj_end_array(b);
    if (!e) e = bj_builder_error(b);
    if (!e) {
        size_t len = 0;
        const uint8_t *d = bj_builder_data(b, &len);
        e = d ? dbuf_put(out, d, len) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    return e;
}

int dbs_propose(dbs *s, uint64_t client, const uint8_t *req, size_t req_len,
                uint64_t *token, dbuf *cmds, dbuf *out) {
    if (!s || !req || !token || !cmds || !out) return BJ_ERR_STATE;
    *token = 0;

    dbs_pending *slot = pending_find(s, 0);
    if (!slot) return DC_ERR_TOO_MANY_CLIENTS;

    /* The request is kept BEFORE it is dispatched: dbs_complete replays
     * these exact bytes, and a caller is entitled to reuse its buffer
     * the moment this returns. */
    slot->req.len = 0;
    int e = dbuf_put(&slot->req, req, req_len);
    if (e) { dbuf_free(&slot->req); return e; }
    slot->token  = ++s->next_token;
    slot->client = client;

    /* No plan means nothing to replicate: a read, a ping, or a refusal,
     * and the answer is already in `out`. */
    e = pass(s, slot, out);
    if (e || !slot->more) { pending_release(slot); return e; }
    e = pass_commands(slot, cmds);
    if (e) { pending_release(slot); return e; }
    *token = slot->token;
    return BJ_OK;
}

int dbs_step(dbs *s, uint64_t token, const uint64_t *indices,
             const dbs_result *results, uint32_t n,
             uint64_t *next, dbuf *cmds, dbuf *out) {
    if (!s || !out || !token || !next || !cmds || !results) return BJ_ERR_STATE;
    *next = 0;
    dbs_pending *slot = pending_find(s, token);
    if (!slot || !slot->nplans) return BJ_ERR_STATE;
    /* One index per command of the plan that was handed out, or the
     * caller replicated something other than what it was given. Refusing
     * beats staging a zero and coming back believing nothing was
     * applied. */
    if (n != dc_wal_plan_count(slot->plans[slot->nplans - 1])) {
        pending_release(slot);
        return BJ_ERR_RANGE;
    }

    s->indices  = indices;
    s->results  = results;
    s->nindices = s->nresults = n;
    s->at_index = 0;
    int e = pass(s, slot, out);
    s->indices  = NULL;
    s->results  = NULL;
    s->nindices = s->nresults = s->at_index = 0;
    if (e) { pending_release(slot); return e; }

    if (slot->more) {                 /* another operation: another trip */
        e = pass_commands(slot, cmds);
        if (e) { pending_release(slot); return e; }
        *next = slot->token;
        return BJ_OK;
    }
    pending_release(slot);
    return BJ_OK;
}


