/*
 * db_session.c — see db_session.h for what this is and why it exists.
 *
 * The whole file is one loop with careful unwinding: walk the plan
 * dc_catalog_open_plan produced, open what it names, attach what it
 * describes, and on any refusal close everything opened so far. Nothing
 * here decides what a collection is made of.
 */
#include "db_session.h"

#include "db_catalog.h"
#include "db_names.h"
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

struct dbs {
    bj_ns     *ns;                          /* borrowed; the caller's */
    int        order;
    bj_io      catalog_io;
    bpt       *catalog;
    dbs_entry  open[DBS_MAX_COLLECTIONS];
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

int dbs_open(bj_ns *ns, int order, dbs **out) {
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
                     0, &s->catalog_io);
    if (e) { free(s); return DC_ERR_NO_DATABASE; }

    s->catalog = bpt_open(&s->catalog_io);
    if (!s->catalog) {
        ns->close(ns->ctx, &s->catalog_io);
        free(s);
        return BJ_ERR_STATE;
    }
    *out = s;
    return BJ_OK;
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

int dbs_open_count(const dbs *s) {
    if (!s) return 0;
    int n = 0;
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) if (s->open[i].used) n++;
    return n;
}

void dbs_close(dbs *s) {
    if (!s) return;
    for (int i = 0; i < DBS_MAX_COLLECTIONS; i++) {
        if (s->open[i].used) entry_release(s->ns, &s->open[i]);
    }
    if (s->catalog) bpt_free(s->catalog);
    s->ns->close(s->ns->ctx, &s->catalog_io);
    free(s);
}
