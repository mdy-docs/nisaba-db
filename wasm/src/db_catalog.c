/*
 * db_catalog.c — see db_catalog.h.
 */
#include "db_catalog.h"
#include "db_names.h"
#include "db_validate.h"
#include "bjcursor.h"
#include "rtree.h"

#include <string.h>

/* ---- small readers over an encoded entry ------------------------------- */

/* Element count of a binjson ARRAY, or -1. */
static long arr_len_of(const uint8_t *arr, size_t len) {
    cur c = { arr, len, 0 };
    uint32_t n;
    if (array_begin(&c, &n) != BJ_OK) return -1;
    return (long)n;
}

/* A STRING field's bytes, or found = 0. */
static int str_field(const uint8_t *obj, size_t obj_len, const char *key,
                     const uint8_t **sp, uint32_t *slen, int *found) {
    const uint8_t *v; size_t vlen;
    *found = 0;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)key,
                          (uint32_t)strlen(key), &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    if (take_string(&c, sp, slen) != BJ_OK) { *found = 0; return BJ_OK; }
    return BJ_OK;
}

/* A numeric field, or found = 0. */
static int num_field(const uint8_t *obj, size_t obj_len, const char *key,
                     double *out, int *found) {
    const uint8_t *v; size_t vlen;
    *found = 0;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)key,
                          (uint32_t)strlen(key), &v, &vlen, found);
    if (e || !*found) return e;
    cur c = { v, vlen, 0 };
    if (read_number(&c, out) != BJ_OK) { *found = 0; return BJ_OK; }
    return BJ_OK;
}

/* Truthiness of an optional boolean field (absent, false, or 0 => 0). */
static int flag_field(const uint8_t *obj, size_t obj_len, const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(obj, obj_len, (const uint8_t *)key,
                      (uint32_t)strlen(key), &v, &vlen, &found) != BJ_OK) return 0;
    if (!found || vlen < 1) return 0;
    if (v[0] == BJ_TYPE_TRUE) return 1;
    if (v[0] == BJ_TYPE_FALSE || v[0] == BJ_TYPE_NULL) return 0;
    double d;
    cur c = { v, vlen, 0 };
    if (read_number(&c, &d) == BJ_OK) return d != 0;
    return 1;   /* present and not falsy: treat as set */
}

/* Copy a whole encoded value through under `key`, if present. */
static void pass_through(bj_builder *b, const uint8_t *obj, size_t obj_len,
                         const char *key) {
    const uint8_t *v; size_t vlen; int found = 0;
    if (obj_get_field(obj, obj_len, (const uint8_t *)key,
                      (uint32_t)strlen(key), &v, &vlen, &found) != BJ_OK) return;
    if (!found || vlen < 1 || v[0] == BJ_TYPE_NULL) return;
    bj_put_key(b, (const uint8_t *)key, (uint32_t)strlen(key));
    bj_put_raw(b, v, (uint32_t)vlen);
}

/*
 * An index definition's kind. The stored form is a STRING, and its absence
 * means equality -- catalog entries written before milestone 6 had no kind
 * field at all, and those databases still open.
 */
static dc_index_plan_kind def_kind(const uint8_t *def, size_t def_len) {
    const uint8_t *sp; uint32_t slen; int found = 0;
    if (str_field(def, def_len, "kind", &sp, &slen, &found) != BJ_OK || !found)
        return DC_INDEX_EQUALITY;
    if (slen == 4 && memcmp(sp, "text", 4) == 0) return DC_INDEX_TEXT;
    if (slen == 3 && memcmp(sp, "geo", 3) == 0)  return DC_INDEX_GEO;
    return DC_INDEX_EQUALITY;
}

/* ---- dc_catalog_open_plan --------------------------------------------- */

/* The three text-index files, in the order attach_text_index takes them. */
static const char *const TEXT_ROLE_KEYS[3] = { "index", "docTerms", "docLengths" };

static int put_text_files(bj_builder *b, const uint8_t *def, size_t def_len) {
    const uint8_t *files; size_t files_len; int found = 0;
    int e = obj_get_field(def, def_len, (const uint8_t *)"files", 5,
                          &files, &files_len, &found);
    if (e) return e;
    if (!found || files_len < 1 || files[0] != BJ_TYPE_OBJECT)
        return DC_ERR_CATALOG_ENTRY;

    bj_begin_array(b);
    for (int r = 0; r < 3; r++) {
        const uint8_t *sp; uint32_t slen; int have = 0;
        e = str_field(files, files_len, TEXT_ROLE_KEYS[r], &sp, &slen, &have);
        if (e) return e;
        if (!have) return DC_ERR_CATALOG_ENTRY;
        bj_put_string(b, sp, slen);
    }
    bj_end_array(b);
    return BJ_OK;
}

static int put_single_file(bj_builder *b, const uint8_t *def, size_t def_len) {
    const uint8_t *sp; uint32_t slen; int found = 0;
    int e = str_field(def, def_len, "file", &sp, &slen, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;
    bj_begin_array(b);
    bj_put_string(b, sp, slen);
    bj_end_array(b);
    return BJ_OK;
}

int dc_catalog_open_plan(const uint8_t *entry, size_t entry_len,
                         const char *coll, size_t coll_len, dbuf *out) {
    if (entry_len < 1 || entry[0] != BJ_TYPE_OBJECT) return DC_ERR_CATALOG_ENTRY;

    /* The primary file is the one thing an entry cannot be without. */
    const uint8_t *primary; uint32_t primary_len; int found = 0;
    int e = str_field(entry, entry_len, "file", &primary, &primary_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;

    double gen_d = 0;
    int has_gen = 0;
    if ((e = num_field(entry, entry_len, "gen", &gen_d, &has_gen))) return e;
    uint32_t gen = (has_gen && gen_d > 0) ? (uint32_t)gen_d : 0;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);

    bj_put_key(b, (const uint8_t *)"primary", 7);
    bj_put_string(b, primary, primary_len);

    /* Journal: recorded per generation since compact() started giving each
     * its own. An entry written before that field existed falls back to
     * the generation-0 name, which is why this needs the collection name. */
    bj_put_key(b, (const uint8_t *)"journal", 7);
    {
        const uint8_t *jp; uint32_t jlen; int has_journal = 0;
        if ((e = str_field(entry, entry_len, "journal", &jp, &jlen, &has_journal))) goto fail;
        if (has_journal) {
            bj_put_string(b, jp, jlen);
        } else {
            dbuf name = {0};
            e = dc_journal_file_name(&name, coll, coll_len, 0);
            if (!e) bj_put_string(b, name.data, (uint32_t)name.len);
            dbuf_free(&name);
            if (e) goto fail;
        }
    }

    bj_put_key(b, (const uint8_t *)"gen", 3);
    bj_put_int(b, (int64_t)gen);

    bj_put_key(b, (const uint8_t *)"indexes", 7);
    bj_begin_array(b);
    {
        const uint8_t *idxs; size_t idxs_len; int has_idxs = 0;
        e = obj_get_field(entry, entry_len, (const uint8_t *)"indexes", 7,
                          &idxs, &idxs_len, &has_idxs);
        if (e) goto fail;
        if (has_idxs && idxs_len >= 1 && idxs[0] == BJ_TYPE_ARRAY) {
            cur c = { idxs, idxs_len, 0 };
            uint32_t n;
            if ((e = array_begin(&c, &n))) goto fail;
            for (uint32_t i = 0; i < n; i++) {
                size_t dstart = c.pos;
                if ((e = skip_value(&c))) goto fail;
                const uint8_t *def = idxs + dstart;
                size_t def_len = c.pos - dstart;
                if (def_len < 1 || def[0] != BJ_TYPE_OBJECT) { e = DC_ERR_CATALOG_ENTRY; goto fail; }

                const uint8_t *np; uint32_t nlen; int has_name = 0;
                if ((e = str_field(def, def_len, "name", &np, &nlen, &has_name))) goto fail;
                if (!has_name) { e = DC_ERR_CATALOG_ENTRY; goto fail; }

                dc_index_plan_kind kind = def_kind(def, def_len);

                bj_begin_object(b);
                bj_put_key(b, (const uint8_t *)"name", 4);
                bj_put_string(b, np, nlen);
                bj_put_key(b, (const uint8_t *)"kind", 4);
                bj_put_int(b, (int64_t)kind);

                bj_put_key(b, (const uint8_t *)"files", 5);
                e = (kind == DC_INDEX_TEXT) ? put_text_files(b, def, def_len)
                                            : put_single_file(b, def, def_len);
                if (e) goto fail;

                if (kind == DC_INDEX_EQUALITY) {
                    const uint8_t *f; size_t flen; int has_fields = 0;
                    if ((e = obj_get_field(def, def_len, (const uint8_t *)"fields", 6,
                                           &f, &flen, &has_fields))) goto fail;
                    if (!has_fields || flen < 1 || f[0] != BJ_TYPE_ARRAY) {
                        e = DC_ERR_CATALOG_ENTRY; goto fail;
                    }
                    bj_put_key(b, (const uint8_t *)"fields", 6);
                    bj_put_raw(b, f, (uint32_t)flen);
                    bj_put_key(b, (const uint8_t *)"unique", 6);
                    bj_put_bool(b, flag_field(def, def_len, "unique"));
                    bj_put_key(b, (const uint8_t *)"sparse", 6);
                    bj_put_bool(b, flag_field(def, def_len, "sparse"));
                    pass_through(b, def, def_len, "partialFilterExpression");
                    pass_through(b, def, def_len, "expireAfterSeconds");
                } else {
                    const uint8_t *fp; uint32_t fplen; int has_field = 0;
                    if ((e = str_field(def, def_len, "field", &fp, &fplen, &has_field))) goto fail;
                    if (!has_field) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
                    bj_put_key(b, (const uint8_t *)"field", 5);
                    bj_put_string(b, fp, fplen);
                }
                bj_end_object(b);
            }
        }
    }
    bj_end_array(b);
    bj_end_object(b);

    if ((e = bj_builder_error(b))) goto fail;
    {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { e = BJ_ERR_STATE; goto fail; }
        e = dbuf_put(out, data, len);
    }

fail:
    bj_builder_free(b);
    return e;
}

/* ---- dc_catalog_list_indexes ------------------------------------------ */

int dc_catalog_list_indexes(const uint8_t *entry, size_t entry_len, dbuf *out) {
    if (entry_len < 1 || entry[0] != BJ_TYPE_OBJECT) return DC_ERR_CATALOG_ENTRY;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_array(b);

    const uint8_t *idxs; size_t idxs_len; int has_idxs = 0;
    int e = obj_get_field(entry, entry_len, (const uint8_t *)"indexes", 7,
                          &idxs, &idxs_len, &has_idxs);
    if (e) goto fail;

    if (has_idxs && idxs_len >= 1 && idxs[0] == BJ_TYPE_ARRAY) {
        cur c = { idxs, idxs_len, 0 };
        uint32_t n;
        if ((e = array_begin(&c, &n))) goto fail;
        for (uint32_t i = 0; i < n; i++) {
            size_t dstart = c.pos;
            if ((e = skip_value(&c))) goto fail;
            const uint8_t *def = idxs + dstart;
            size_t def_len = c.pos - dstart;
            if (def_len < 1 || def[0] != BJ_TYPE_OBJECT) { e = DC_ERR_CATALOG_ENTRY; goto fail; }

            const uint8_t *np; uint32_t nlen; int has_name = 0;
            if ((e = str_field(def, def_len, "name", &np, &nlen, &has_name))) goto fail;
            if (!has_name) { e = DC_ERR_CATALOG_ENTRY; goto fail; }

            bj_begin_object(b);
            bj_put_key(b, (const uint8_t *)"name", 4);
            bj_put_string(b, np, nlen);

            /* `key` is the inverse of what createIndex was handed: stored
             * `fields` become {field: 1} pairs in order, and a text/geo
             * `field` becomes {field: 'text'} / {field: '2dsphere'}. */
            bj_put_key(b, (const uint8_t *)"key", 3);
            bj_begin_object(b);
            dc_index_plan_kind kind = def_kind(def, def_len);
            if (kind == DC_INDEX_EQUALITY) {
                const uint8_t *f; size_t flen; int has_fields = 0;
                if ((e = obj_get_field(def, def_len, (const uint8_t *)"fields", 6,
                                       &f, &flen, &has_fields))) goto fail;
                if (!has_fields || flen < 1 || f[0] != BJ_TYPE_ARRAY) {
                    e = DC_ERR_CATALOG_ENTRY; goto fail;
                }
                cur fc = { f, flen, 0 };
                uint32_t fn;
                if ((e = array_begin(&fc, &fn))) goto fail;
                for (uint32_t j = 0; j < fn; j++) {
                    const uint8_t *sp; uint32_t slen;
                    if (take_string(&fc, &sp, &slen) != BJ_OK) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
                    bj_put_key(b, sp, slen);
                    bj_put_int(b, 1);
                }
            } else {
                const uint8_t *fp; uint32_t fplen; int has_field = 0;
                if ((e = str_field(def, def_len, "field", &fp, &fplen, &has_field))) goto fail;
                if (!has_field) { e = DC_ERR_CATALOG_ENTRY; goto fail; }
                bj_put_key(b, fp, fplen);
                if (kind == DC_INDEX_TEXT) bj_put_string(b, (const uint8_t *)"text", 4);
                else                       bj_put_string(b, (const uint8_t *)"2dsphere", 8);
            }
            bj_end_object(b);

            /* Options are reported only when set, matching the driver (and
             * matching what the JS projection did). */
            if (kind == DC_INDEX_EQUALITY) {
                if (flag_field(def, def_len, "unique")) {
                    bj_put_key(b, (const uint8_t *)"unique", 6);
                    bj_put_bool(b, 1);
                }
                if (flag_field(def, def_len, "sparse")) {
                    bj_put_key(b, (const uint8_t *)"sparse", 6);
                    bj_put_bool(b, 1);
                }
                pass_through(b, def, def_len, "partialFilterExpression");
                pass_through(b, def, def_len, "expireAfterSeconds");
            }
            bj_end_object(b);
        }
    }
    bj_end_array(b);

    if ((e = bj_builder_error(b))) goto fail;
    {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { e = BJ_ERR_STATE; goto fail; }
        e = dbuf_put(out, data, len);
    }

fail:
    bj_builder_free(b);
    return e;
}

/* ---- writing ------------------------------------------------------------ */

/* Copy every field of `entry` except `indexes`, which the caller rebuilds. */
static int copy_entry_except_indexes(bj_builder *b, const uint8_t *entry, size_t entry_len) {
    cur c = { entry, entry_len, 0 };
    uint32_t n;
    int e = object_begin(&c, &n);
    if (e) return e;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&c, &kp, &klen))) return e;
        size_t vstart = c.pos;
        if ((e = skip_value(&c))) return e;
        if (klen == 7 && memcmp(kp, "indexes", 7) == 0) continue;
        bj_put_key(b, kp, klen);
        bj_put_raw(b, entry + vstart, (uint32_t)(c.pos - vstart));
    }
    return BJ_OK;
}

/*
 * Write one plan-shaped definition out in the STORED form. The two shapes
 * differ deliberately: a plan says `kind: <int>` and `files: [...]`
 * because that is what a host loop needs, while the catalog stores
 * `kind: "<name>"` and `file`/`files` because that is what is readable in
 * a dump and what older readers already understand.
 */
static int put_stored_def(bj_builder *b, const uint8_t *def, size_t def_len) {
    const uint8_t *np; uint32_t nlen; int found = 0;
    int e = str_field(def, def_len, "name", &np, &nlen, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;

    double kd = 0; int has_kind = 0;
    if ((e = num_field(def, def_len, "kind", &kd, &has_kind))) return e;
    dc_index_plan_kind kind = has_kind ? (dc_index_plan_kind)(int)kd : DC_INDEX_EQUALITY;

    const uint8_t *files; size_t files_len; int has_files = 0;
    if ((e = obj_get_field(def, def_len, (const uint8_t *)"files", 5,
                           &files, &files_len, &has_files))) return e;
    if (!has_files || files_len < 1 || files[0] != BJ_TYPE_ARRAY) return DC_ERR_CATALOG_ENTRY;
    cur fc = { files, files_len, 0 };
    uint32_t fn;
    if ((e = array_begin(&fc, &fn))) return e;

    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"name", 4);
    bj_put_string(b, np, nlen);
    bj_put_key(b, (const uint8_t *)"kind", 4);
    bj_put_string(b, (const uint8_t *)(kind == DC_INDEX_TEXT ? "text"
                                     : kind == DC_INDEX_GEO  ? "geo" : "equality"),
                  (uint32_t)(kind == DC_INDEX_TEXT ? 4 : kind == DC_INDEX_GEO ? 3 : 8));

    if (kind == DC_INDEX_TEXT) {
        if (fn != 3) return DC_ERR_CATALOG_ENTRY;
        bj_put_key(b, (const uint8_t *)"files", 5);
        bj_begin_object(b);
        for (int r = 0; r < 3; r++) {
            const uint8_t *sp; uint32_t slen;
            if (take_string(&fc, &sp, &slen) != BJ_OK) return DC_ERR_CATALOG_ENTRY;
            bj_put_key(b, (const uint8_t *)TEXT_ROLE_KEYS[r],
                       (uint32_t)strlen(TEXT_ROLE_KEYS[r]));
            bj_put_string(b, sp, slen);
        }
        bj_end_object(b);
    } else {
        if (fn != 1) return DC_ERR_CATALOG_ENTRY;
        const uint8_t *sp; uint32_t slen;
        if (take_string(&fc, &sp, &slen) != BJ_OK) return DC_ERR_CATALOG_ENTRY;
        bj_put_key(b, (const uint8_t *)"file", 4);
        bj_put_string(b, sp, slen);
    }

    if (kind == DC_INDEX_EQUALITY) {
        const uint8_t *f; size_t flen; int has_fields = 0;
        if ((e = obj_get_field(def, def_len, (const uint8_t *)"fields", 6,
                               &f, &flen, &has_fields))) return e;
        if (!has_fields || flen < 1 || f[0] != BJ_TYPE_ARRAY) return DC_ERR_CATALOG_ENTRY;
        bj_put_key(b, (const uint8_t *)"fields", 6);
        bj_put_raw(b, f, (uint32_t)flen);
        /* Optional flags are stored only when true, which is what keeps a
         * simple entry small and readable -- and what listIndexes'
         * report-only-when-set behavior mirrors. */
        if (flag_field(def, def_len, "unique")) {
            bj_put_key(b, (const uint8_t *)"unique", 6);
            bj_put_bool(b, 1);
        }
        if (flag_field(def, def_len, "sparse")) {
            bj_put_key(b, (const uint8_t *)"sparse", 6);
            bj_put_bool(b, 1);
        }
        pass_through(b, def, def_len, "partialFilterExpression");
        pass_through(b, def, def_len, "expireAfterSeconds");
    } else {
        const uint8_t *fp; uint32_t fplen; int has_field = 0;
        if ((e = str_field(def, def_len, "field", &fp, &fplen, &has_field))) return e;
        if (!has_field) return DC_ERR_CATALOG_ENTRY;
        bj_put_key(b, (const uint8_t *)"field", 5);
        bj_put_string(b, fp, fplen);
    }
    bj_end_object(b);
    return BJ_OK;
}

/* Shared body: rebuild `indexes`, dropping any definition named `skip`
 * and appending `add` (either may be absent). */
static int rewrite_indexes(const uint8_t *entry, size_t entry_len,
                           const char *skip, size_t skip_len,
                           const uint8_t *add, size_t add_len, dbuf *out) {
    if (entry_len < 1 || entry[0] != BJ_TYPE_OBJECT) return DC_ERR_CATALOG_ENTRY;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_object(b);
    int e = copy_entry_except_indexes(b, entry, entry_len);
    if (e) goto fail;

    bj_put_key(b, (const uint8_t *)"indexes", 7);
    bj_begin_array(b);
    {
        const uint8_t *idxs; size_t idxs_len; int has_idxs = 0;
        if ((e = obj_get_field(entry, entry_len, (const uint8_t *)"indexes", 7,
                               &idxs, &idxs_len, &has_idxs))) goto fail;
        if (has_idxs && idxs_len >= 1 && idxs[0] == BJ_TYPE_ARRAY) {
            cur c = { idxs, idxs_len, 0 };
            uint32_t n;
            if ((e = array_begin(&c, &n))) goto fail;
            for (uint32_t i = 0; i < n; i++) {
                size_t dstart = c.pos;
                if ((e = skip_value(&c))) goto fail;
                const uint8_t *def = idxs + dstart;
                size_t def_len = c.pos - dstart;
                const uint8_t *np; uint32_t nlen; int has_name = 0;
                if ((e = str_field(def, def_len, "name", &np, &nlen, &has_name))) goto fail;
                if (has_name && skip && nlen == skip_len &&
                    memcmp(np, skip, nlen) == 0) continue;      /* dropped or replaced */
                bj_put_raw(b, def, (uint32_t)def_len);
            }
        }
        if (add) {
            if ((e = put_stored_def(b, add, add_len))) goto fail;
        }
    }
    bj_end_array(b);
    bj_end_object(b);

    if ((e = bj_builder_error(b))) goto fail;
    {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { e = BJ_ERR_STATE; goto fail; }
        e = dbuf_put(out, data, len);
    }

fail:
    bj_builder_free(b);
    return e;
}

int dc_catalog_put_index(const uint8_t *entry, size_t entry_len,
                         const uint8_t *def, size_t def_len, dbuf *out) {
    const uint8_t *np; uint32_t nlen; int found = 0;
    int e = str_field(def, def_len, "name", &np, &nlen, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;
    return rewrite_indexes(entry, entry_len, (const char *)np, nlen, def, def_len, out);
}

int dc_catalog_drop_index(const uint8_t *entry, size_t entry_len,
                          const char *name, size_t name_len, dbuf *out) {
    return rewrite_indexes(entry, entry_len, name, name_len, NULL, 0, out);
}

/* ---- planning a new index ---------------------------------------------- */

/* Does `options` carry any of the equality-only options? */
static int has_equality_only_option(const uint8_t *opt, size_t opt_len) {
    static const char *const KEYS[] = {
        "unique", "sparse", "partialFilterExpression", "expireAfterSeconds"
    };
    for (size_t i = 0; i < sizeof(KEYS) / sizeof(KEYS[0]); i++) {
        const uint8_t *v; size_t vlen; int found = 0;
        if (obj_get_field(opt, opt_len, (const uint8_t *)KEYS[i],
                          (uint32_t)strlen(KEYS[i]), &v, &vlen, &found) != BJ_OK) continue;
        /* Explicitly false/null is not "supplied" -- a caller spreading a
         * default options object should not be refused a text index. */
        if (!found || vlen < 1) continue;
        if (v[0] == BJ_TYPE_FALSE || v[0] == BJ_TYPE_NULL) continue;
        return 1;
    }
    return 0;
}

/*
 * The single field of a one-field spec whose value is the STRING `want`,
 * e.g. {body: "text"}. Returns 1 and fills the field span on a match.
 */
static int special_spec(const uint8_t *keys, size_t keys_len, const char *want,
                        const uint8_t **field, uint32_t *field_len) {
    cur c = { keys, keys_len, 0 };
    uint32_t n;
    if (object_begin(&c, &n) != BJ_OK || n != 1) return 0;
    const uint8_t *kp; uint32_t klen;
    if (take_key(&c, &kp, &klen) != BJ_OK) return 0;
    const uint8_t *sp; uint32_t slen;
    if (take_string(&c, &sp, &slen) != BJ_OK) return 0;
    if (slen != strlen(want) || memcmp(sp, want, slen) != 0) return 0;
    *field = kp; *field_len = klen;
    return 1;
}

/* options.name, or NULL. */
static int explicit_name(const uint8_t *opt, size_t opt_len,
                         const uint8_t **np, uint32_t *nlen) {
    int found = 0;
    if (str_field(opt, opt_len, "name", np, nlen, &found) != BJ_OK) return 0;
    return found;
}

int dc_index_create_plan(const uint8_t *keys, size_t keys_len,
                         const uint8_t *options, size_t options_len,
                         const char *coll, size_t coll_len, dbuf *out) {
    if (keys_len < 1 || keys[0] != BJ_TYPE_OBJECT) return DC_ERR_EMPTY_KEY_SPEC;
    int has_opts = options && options_len >= 1 && options[0] == BJ_TYPE_OBJECT;

    const uint8_t *field; uint32_t field_len;
    int is_text = special_spec(keys, keys_len, "text", &field, &field_len);
    int is_geo  = !is_text && special_spec(keys, keys_len, "2dsphere", &field, &field_len);

    if ((is_text || is_geo) && has_opts && has_equality_only_option(options, options_len))
        return DC_ERR_INDEX_OPTION_UNSUPPORTED;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = BJ_OK;
    dbuf name = {0}, f0 = {0}, f1 = {0}, f2 = {0}, fields = {0};

    /* Name: options.name, else the driver's convention. */
    {
        const uint8_t *np; uint32_t nlen;
        if (has_opts && explicit_name(options, options_len, &np, &nlen)) {
            e = dbuf_put(&name, np, nlen);
        } else if (is_text || is_geo) {
            e = dbuf_put(&name, field, field_len);
            if (!e) e = dbuf_put(&name, (const uint8_t *)(is_text ? "_text" : "_2dsphere"),
                                 is_text ? 5 : 9);
        } else {
            /* "team_1", "team_1_age_1" -- each field, then "_1", joined
             * by "_". Validation happens below; this only needs the keys. */
            cur c = { keys, keys_len, 0 };
            uint32_t n;
            if ((e = object_begin(&c, &n))) goto done;
            if (n == 0) { e = DC_ERR_EMPTY_KEY_SPEC; goto done; }
            for (uint32_t i = 0; i < n && !e; i++) {
                const uint8_t *kp; uint32_t klen;
                if ((e = take_key(&c, &kp, &klen))) break;
                if ((e = skip_value(&c))) break;
                if (i) e = dbuf_put(&name, (const uint8_t *)"_", 1);
                if (!e) e = dbuf_put(&name, kp, klen);
                if (!e) e = dbuf_put(&name, (const uint8_t *)"_1", 2);
            }
        }
        if (e) goto done;
    }

    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"name", 4);
    bj_put_string(b, name.data, (uint32_t)name.len);
    bj_put_key(b, (const uint8_t *)"kind", 4);
    bj_put_int(b, is_text ? DC_INDEX_TEXT : is_geo ? DC_INDEX_GEO : DC_INDEX_EQUALITY);

    bj_put_key(b, (const uint8_t *)"files", 5);
    bj_begin_array(b);
    if (is_text) {
        /* Attach order, same as everywhere else this triple appears. */
        e = dc_text_index_file_name(&f0, coll, coll_len, (const char *)name.data, name.len,
                                    0, DC_TEXT_ROLE_TERMS);
        if (!e) e = dc_text_index_file_name(&f1, coll, coll_len, (const char *)name.data,
                                            name.len, 0, DC_TEXT_ROLE_DOCUMENTS);
        if (!e) e = dc_text_index_file_name(&f2, coll, coll_len, (const char *)name.data,
                                            name.len, 0, DC_TEXT_ROLE_LENGTHS);
        if (e) goto done;
        bj_put_string(b, f0.data, (uint32_t)f0.len);
        bj_put_string(b, f1.data, (uint32_t)f1.len);
        bj_put_string(b, f2.data, (uint32_t)f2.len);
    } else {
        e = dc_index_file_name(&f0, coll, coll_len, (const char *)name.data, name.len, 0);
        if (e) goto done;
        bj_put_string(b, f0.data, (uint32_t)f0.len);
    }
    bj_end_array(b);

    if (is_text || is_geo) {
        bj_put_key(b, (const uint8_t *)"field", 5);
        bj_put_string(b, field, field_len);
    } else {
        /* Validating here rather than earlier keeps the special-index path
         * from being refused by a rule that does not apply to it: {body:
         * "text"} is not an ascending spec and must not be judged as one. */
        if ((e = dc_check_index_key_spec(keys, keys_len, &fields))) goto done;
        bj_put_key(b, (const uint8_t *)"fields", 6);
        bj_put_raw(b, fields.data, (uint32_t)fields.len);

        int unique = has_opts && flag_field(options, options_len, "unique");
        int sparse = has_opts && flag_field(options, options_len, "sparse");
        bj_put_key(b, (const uint8_t *)"unique", 6);
        bj_put_bool(b, unique);
        bj_put_key(b, (const uint8_t *)"sparse", 6);
        bj_put_bool(b, sparse);
        if (has_opts) {
            pass_through(b, options, options_len, "partialFilterExpression");

            const uint8_t *ttl; size_t ttl_len; int has_ttl = 0;
            if ((e = obj_get_field(options, options_len,
                                   (const uint8_t *)"expireAfterSeconds", 18,
                                   &ttl, &ttl_len, &has_ttl))) goto done;
            if (has_ttl && ttl_len >= 1 && ttl[0] != BJ_TYPE_NULL) {
                /* A TTL index is an ordinary index over one Date field; a
                 * compound one has no single field to expire on. */
                if (arr_len_of(fields.data, fields.len) != 1) {
                    e = DC_ERR_TTL_NEEDS_SINGLE_FIELD;
                    goto done;
                }
                bj_put_key(b, (const uint8_t *)"expireAfterSeconds", 18);
                bj_put_raw(b, ttl, (uint32_t)ttl_len);
            }
        }
    }
    bj_end_object(b);

    if ((e = bj_builder_error(b))) goto done;
    {
        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { e = BJ_ERR_STATE; goto done; }
        e = dbuf_put(out, data, len);
    }

done:
    dbuf_free(&fields); dbuf_free(&f2); dbuf_free(&f1); dbuf_free(&f0); dbuf_free(&name);
    bj_builder_free(b);
    return e;
}

/* ---- the orphan sweep --------------------------------------------------- */

/* Is `name` among the NUL-separated `refs`? */
static int name_in(const dbuf *refs, const char *name, size_t name_len) {
    size_t at = 0;
    while (at < refs->len) {
        const char *r = (const char *)refs->data + at;
        size_t rlen = strlen(r);
        if (rlen == name_len && memcmp(r, name, rlen) == 0) return 1;
        at += rlen + 1;
    }
    return 0;
}

static int ref_add(dbuf *refs, const uint8_t *s, uint32_t len) {
    int e = dbuf_put(refs, s, len);
    if (e) return e;
    static const uint8_t nul = 0;
    return dbuf_put(refs, &nul, 1);
}

/* Every file one catalog entry lays claim to. */
static int refs_of_entry(dbuf *refs, const uint8_t *name, uint32_t name_len,
                         const uint8_t *entry, size_t entry_len) {
    if (entry_len < 1 || entry[0] != BJ_TYPE_OBJECT) return DC_ERR_CATALOG_ENTRY;

    const uint8_t *sp; uint32_t slen; int found = 0;
    int e = str_field(entry, entry_len, "file", &sp, &slen, &found);
    if (e) return e;
    if (found && (e = ref_add(refs, sp, slen))) return e;

    /* Same journal fallback the open plan applies -- an entry written
     * before that field existed still owns its generation-0 journal, and
     * sweeping it would destroy crash recovery for that collection. */
    if ((e = str_field(entry, entry_len, "journal", &sp, &slen, &found))) return e;
    if (found) {
        if ((e = ref_add(refs, sp, slen))) return e;
    } else {
        dbuf jn = {0};
        e = dc_journal_file_name(&jn, (const char *)name, name_len, 0);
        if (!e) e = ref_add(refs, jn.data, (uint32_t)jn.len);
        dbuf_free(&jn);
        if (e) return e;
    }

    const uint8_t *idxs; size_t idxs_len; int has_idxs = 0;
    if ((e = obj_get_field(entry, entry_len, (const uint8_t *)"indexes", 7,
                           &idxs, &idxs_len, &has_idxs))) return e;
    if (!has_idxs || idxs_len < 1 || idxs[0] != BJ_TYPE_ARRAY) return BJ_OK;

    cur c = { idxs, idxs_len, 0 };
    uint32_t n;
    if ((e = array_begin(&c, &n))) return e;
    for (uint32_t i = 0; i < n; i++) {
        size_t dstart = c.pos;
        if ((e = skip_value(&c))) return e;
        const uint8_t *def = idxs + dstart;
        size_t def_len = c.pos - dstart;
        if (def_kind(def, def_len) == DC_INDEX_TEXT) {
            const uint8_t *files; size_t files_len; int has_files = 0;
            if ((e = obj_get_field(def, def_len, (const uint8_t *)"files", 5,
                                   &files, &files_len, &has_files))) return e;
            if (!has_files || files_len < 1 || files[0] != BJ_TYPE_OBJECT) continue;
            /* Iterate whatever roles are present rather than the three we
             * expect: a future role must not be swept away by an older
             * reader that does not know its name. */
            cur fc = { files, files_len, 0 };
            uint32_t fn;
            if ((e = object_begin(&fc, &fn))) return e;
            for (uint32_t j = 0; j < fn; j++) {
                const uint8_t *fk; uint32_t fklen;
                if ((e = take_key(&fc, &fk, &fklen))) return e;
                const uint8_t *fv; uint32_t fvlen;
                cur vc = fc;
                if (take_string(&vc, &fv, &fvlen) == BJ_OK) {
                    if ((e = ref_add(refs, fv, fvlen))) return e;
                }
                if ((e = skip_value(&fc))) return e;
            }
        } else {
            if ((e = str_field(def, def_len, "file", &sp, &slen, &found))) return e;
            if (found && (e = ref_add(refs, sp, slen))) return e;
        }
    }
    return BJ_OK;
}

int dc_sweep_plan(const uint8_t *catalog, size_t catalog_len,
                  const char *names, size_t names_len, dbuf *out) {
    dbuf refs = {0};
    int e = ref_add(&refs, (const uint8_t *)DC_CATALOG_FILE, (uint32_t)strlen(DC_CATALOG_FILE));
    if (e) { dbuf_free(&refs); return e; }

    if (catalog_len >= 1 && catalog[0] == BJ_TYPE_ARRAY) {
        cur c = { catalog, catalog_len, 0 };
        uint32_t n;
        if ((e = array_begin(&c, &n))) { dbuf_free(&refs); return e; }
        for (uint32_t i = 0; i < n; i++) {
            size_t rstart = c.pos;
            if ((e = skip_value(&c))) break;
            const uint8_t *row = catalog + rstart;
            size_t row_len = c.pos - rstart;

            const uint8_t *kp; uint32_t klen; int found = 0;
            if ((e = str_field(row, row_len, "key", &kp, &klen, &found))) break;
            if (!found) continue;
            /* The format stamp is a catalog row that owns no files. */
            if (klen == strlen(DC_FORMAT_KEY) && memcmp(kp, DC_FORMAT_KEY, klen) == 0) continue;

            const uint8_t *val; size_t val_len; int has_val = 0;
            if ((e = obj_get_field(row, row_len, (const uint8_t *)"value", 5,
                                   &val, &val_len, &has_val))) break;
            if (!has_val) continue;
            if ((e = refs_of_entry(&refs, kp, klen, val, val_len))) break;
        }
    }
    if (e) { dbuf_free(&refs); return e; }

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&refs); return BJ_ERR_OOM; }
    bj_begin_array(b);
    {
        size_t at = 0;
        while (at < names_len) {
            const char *nm = names + at;
            /* memchr, not strnlen: strnlen is POSIX-2008, and wasi-libc
             * does not declare it under -std=c11. Same semantics -- an
             * unterminated final name runs to the end of the buffer. */
            const char *nul = (const char *)memchr(nm, '\0', names_len - at);
            size_t nlen = nul ? (size_t)(nul - nm) : names_len - at;
            if (nlen == 0) { at += 1; continue; }
            /* Unreferenced AND ours: either condition alone deletes the
             * wrong thing. */
            if (!name_in(&refs, nm, nlen) && dc_is_db_file(nm, nlen))
                bj_put_string(b, (const uint8_t *)nm, (uint32_t)nlen);
            at += nlen + 1;
        }
    }
    bj_end_array(b);
    dbuf_free(&refs);

    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, len);
    bj_builder_free(b);
    return e;
}

/* ---- per-entry file lists ----------------------------------------------- */

int dc_collection_files(const uint8_t *entry, size_t entry_len,
                        const char *coll, size_t coll_len, dbuf *out) {
    /* Reuses the sweep's own reference collector, so drop and sweep can
     * never disagree about what a collection owns. */
    dbuf refs = {0};
    int e = refs_of_entry(&refs, (const uint8_t *)coll, (uint32_t)coll_len,
                          entry, entry_len);
    if (e) { dbuf_free(&refs); return e; }

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&refs); return BJ_ERR_OOM; }
    bj_begin_array(b);
    {
        size_t at = 0;
        while (at < refs.len) {
            const char *nm = (const char *)refs.data + at;
            size_t nlen = strlen(nm);
            if (nlen) bj_put_string(b, (const uint8_t *)nm, (uint32_t)nlen);
            at += nlen + 1;
        }
    }
    bj_end_array(b);
    dbuf_free(&refs);

    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, len);
    bj_builder_free(b);
    return e;
}

int dc_catalog_new_entry(const char *coll, size_t coll_len, dbuf *out) {
    dbuf file = {0};
    int e = dc_collection_file_name(&file, coll, coll_len, 0);
    if (e) { dbuf_free(&file); return e; }

    bj_builder *b = bj_builder_new();
    if (!b) { dbuf_free(&file); return BJ_ERR_OOM; }
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"file", 4);
    bj_put_string(b, file.data, (uint32_t)file.len);
    bj_end_object(b);
    dbuf_free(&file);

    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }
    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, len);
    bj_builder_free(b);
    return e;
}

/* ---- planning a compaction ---------------------------------------------- */

/* Emit an index definition with its files renamed into `gen`, and record
 * both the new names (into `build`) and the old ones (into `old`). */
static int regen_index(bj_builder *entry_out, bj_builder *build_out, dbuf *old,
                       const uint8_t *def, size_t def_len,
                       const char *coll, size_t coll_len, uint32_t gen) {
    const uint8_t *np; uint32_t nlen; int found = 0;
    int e = str_field(def, def_len, "name", &np, &nlen, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;
    dc_index_plan_kind kind = def_kind(def, def_len);

    /* Copy the definition through, replacing only its file names -- so
     * options (unique, sparse, partialFilterExpression, TTL) survive a
     * compaction untouched, which a hand-built replacement can forget. */
    bj_begin_object(entry_out);
    bj_begin_object(build_out);
    bj_put_key(build_out, (const uint8_t *)"name", 4);
    bj_put_string(build_out, np, nlen);
    bj_put_key(build_out, (const uint8_t *)"kind", 4);
    bj_put_int(build_out, (int64_t)kind);
    bj_put_key(build_out, (const uint8_t *)"files", 5);
    bj_begin_array(build_out);

    cur c = { def, def_len, 0 };
    uint32_t n;
    if ((e = object_begin(&c, &n))) return e;
    for (uint32_t i = 0; i < n; i++) {
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&c, &kp, &klen))) return e;
        size_t vstart = c.pos;
        if ((e = skip_value(&c))) return e;
        const uint8_t *v = def + vstart;
        size_t vlen = c.pos - vstart;

        if (klen == 4 && memcmp(kp, "file", 4) == 0) {
            /* Old name goes on the delete list; new name replaces it. */
            const uint8_t *sp; uint32_t slen;
            cur vc = { v, vlen, 0 };
            if (take_string(&vc, &sp, &slen) != BJ_OK) return DC_ERR_CATALOG_ENTRY;
            if ((e = ref_add(old, sp, slen))) return e;
            dbuf nf = {0};
            e = dc_index_file_name(&nf, coll, coll_len, (const char *)np, nlen, gen);
            if (!e) {
                bj_put_key(entry_out, (const uint8_t *)"file", 4);
                bj_put_string(entry_out, nf.data, (uint32_t)nf.len);
                bj_put_string(build_out, nf.data, (uint32_t)nf.len);
            }
            dbuf_free(&nf);
            if (e) return e;
        } else if (klen == 5 && memcmp(kp, "files", 5) == 0) {
            if (vlen < 1 || v[0] != BJ_TYPE_OBJECT) return DC_ERR_CATALOG_ENTRY;
            bj_put_key(entry_out, (const uint8_t *)"files", 5);
            bj_begin_object(entry_out);
            for (int r = 0; r < 3; r++) {
                const uint8_t *sp; uint32_t slen; int have = 0;
                if ((e = str_field(v, vlen, TEXT_ROLE_KEYS[r], &sp, &slen, &have))) return e;
                if (!have) return DC_ERR_CATALOG_ENTRY;
                if ((e = ref_add(old, sp, slen))) return e;
                dbuf nf = {0};
                e = dc_text_index_file_name(&nf, coll, coll_len, (const char *)np, nlen,
                                            gen, (dc_text_role)r);
                if (!e) {
                    bj_put_key(entry_out, (const uint8_t *)TEXT_ROLE_KEYS[r],
                               (uint32_t)strlen(TEXT_ROLE_KEYS[r]));
                    bj_put_string(entry_out, nf.data, (uint32_t)nf.len);
                    bj_put_string(build_out, nf.data, (uint32_t)nf.len);
                }
                dbuf_free(&nf);
                if (e) return e;
            }
            bj_end_object(entry_out);
        } else {
            bj_put_key(entry_out, kp, klen);
            bj_put_raw(entry_out, v, (uint32_t)vlen);
        }
    }
    bj_end_array(build_out);
    bj_end_object(build_out);
    bj_end_object(entry_out);
    return BJ_OK;
}

int dc_compact_plan(const uint8_t *entry, size_t entry_len,
                    const char *coll, size_t coll_len, dbuf *out) {
    if (entry_len < 1 || entry[0] != BJ_TYPE_OBJECT) return DC_ERR_CATALOG_ENTRY;

    double gen_d = 0; int has_gen = 0;
    int e = num_field(entry, entry_len, "gen", &gen_d, &has_gen);
    if (e) return e;
    uint32_t gen = ((has_gen && gen_d > 0) ? (uint32_t)gen_d : 0) + 1;

    dbuf old = {0}, newfile = {0}, newjournal = {0};
    bj_builder *b = bj_builder_new();
    bj_builder *ent = bj_builder_new();
    bj_builder *bld = bj_builder_new();
    if (!b || !ent || !bld) { e = BJ_ERR_OOM; goto done; }

    if ((e = dc_collection_file_name(&newfile, coll, coll_len, gen))) goto done;
    if ((e = dc_journal_file_name(&newjournal, coll, coll_len, gen))) goto done;

    /* Old primary and journal, with the usual gen-0 journal fallback. */
    {
        const uint8_t *sp; uint32_t slen; int found = 0;
        if ((e = str_field(entry, entry_len, "file", &sp, &slen, &found))) goto done;
        if (!found) { e = DC_ERR_CATALOG_ENTRY; goto done; }
        if ((e = ref_add(&old, sp, slen))) goto done;

        if ((e = str_field(entry, entry_len, "journal", &sp, &slen, &found))) goto done;
        if (found) {
            if ((e = ref_add(&old, sp, slen))) goto done;
        } else {
            dbuf j0 = {0};
            e = dc_journal_file_name(&j0, coll, coll_len, 0);
            if (!e) e = ref_add(&old, j0.data, (uint32_t)j0.len);
            dbuf_free(&j0);
            if (e) goto done;
        }
    }

    /* The new entry: the old one with gen, file and journal replaced and
     * every index's files regenerated. Copying rather than rebuilding is
     * what carries compactedBytes-style fields and any future field
     * through a compaction untouched. */
    bj_begin_object(ent);
    bj_begin_array(bld);
    {
        cur c = { entry, entry_len, 0 };
        uint32_t n;
        if ((e = object_begin(&c, &n))) goto done;
        int wrote_indexes = 0;
        for (uint32_t i = 0; i < n; i++) {
            const uint8_t *kp; uint32_t klen;
            if ((e = take_key(&c, &kp, &klen))) goto done;
            size_t vstart = c.pos;
            if ((e = skip_value(&c))) goto done;
            const uint8_t *v = entry + vstart;
            size_t vlen = c.pos - vstart;

            if (klen == 4 && memcmp(kp, "file", 4) == 0) continue;      /* replaced below */
            if (klen == 7 && memcmp(kp, "journal", 7) == 0) continue;   /* replaced below */
            if (klen == 3 && memcmp(kp, "gen", 3) == 0) continue;       /* replaced below */
            if (klen == 7 && memcmp(kp, "indexes", 7) == 0) {
                wrote_indexes = 1;
                bj_put_key(ent, (const uint8_t *)"indexes", 7);
                bj_begin_array(ent);
                if (vlen >= 1 && v[0] == BJ_TYPE_ARRAY) {
                    cur ic = { v, vlen, 0 };
                    uint32_t in;
                    if ((e = array_begin(&ic, &in))) goto done;
                    for (uint32_t j = 0; j < in; j++) {
                        size_t dstart = ic.pos;
                        if ((e = skip_value(&ic))) goto done;
                        if ((e = regen_index(ent, bld, &old, v + dstart, ic.pos - dstart,
                                             coll, coll_len, gen))) goto done;
                    }
                }
                bj_end_array(ent);
                continue;
            }
            bj_put_key(ent, kp, klen);
            bj_put_raw(ent, v, (uint32_t)vlen);
        }
        if (!wrote_indexes) {
            bj_put_key(ent, (const uint8_t *)"indexes", 7);
            bj_begin_array(ent);
            bj_end_array(ent);
        }
    }
    bj_put_key(ent, (const uint8_t *)"file", 4);
    bj_put_string(ent, newfile.data, (uint32_t)newfile.len);
    bj_put_key(ent, (const uint8_t *)"journal", 7);
    bj_put_string(ent, newjournal.data, (uint32_t)newjournal.len);
    bj_put_key(ent, (const uint8_t *)"gen", 3);
    bj_put_int(ent, (int64_t)gen);
    bj_end_object(ent);
    bj_end_array(bld);

    if ((e = bj_builder_error(ent))) goto done;
    if ((e = bj_builder_error(bld))) goto done;

    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"gen", 3);
    bj_put_int(b, (int64_t)gen);
    bj_put_key(b, (const uint8_t *)"newEntry", 8);
    {
        size_t l = 0;
        const uint8_t *d = bj_builder_data(ent, &l);
        if (!d) { e = BJ_ERR_STATE; goto done; }
        bj_put_raw(b, d, (uint32_t)l);
    }
    bj_put_key(b, (const uint8_t *)"build", 5);
    {
        size_t l = 0;
        const uint8_t *d = bj_builder_data(bld, &l);
        if (!d) { e = BJ_ERR_STATE; goto done; }
        bj_put_raw(b, d, (uint32_t)l);
    }
    bj_put_key(b, (const uint8_t *)"oldFiles", 8);
    bj_begin_array(b);
    {
        size_t at = 0;
        while (at < old.len) {
            const char *nm = (const char *)old.data + at;
            size_t nlen = strlen(nm);
            if (nlen) bj_put_string(b, (const uint8_t *)nm, (uint32_t)nlen);
            at += nlen + 1;
        }
    }
    bj_end_array(b);
    bj_end_object(b);

    if ((e = bj_builder_error(b))) goto done;
    {
        size_t l = 0;
        const uint8_t *d = bj_builder_data(b, &l);
        if (!d) { e = BJ_ERR_STATE; goto done; }
        e = dbuf_put(out, d, l);
    }

done:
    dbuf_free(&newjournal); dbuf_free(&newfile); dbuf_free(&old);
    bj_builder_free(bld); bj_builder_free(ent); bj_builder_free(b);
    return e;
}

/* ---- executing the sweep ------------------------------------------------ */

int dc_sweep_execute(bj_ns *ns, const uint8_t *catalog, size_t catalog_len,
                     const char *names, size_t names_len, uint32_t *deleted) {
    *deleted = 0;
    if (!ns || !ns->remove) return BJ_ERR_STATE;

    dbuf victims = {0};
    int e = dc_sweep_plan(catalog, catalog_len, names, names_len, &victims);
    if (e) { dbuf_free(&victims); return e; }

    cur c = { victims.data, victims.len, 0 };
    uint32_t n;
    e = array_begin(&c, &n);
    for (uint32_t i = 0; !e && i < n; i++) {
        const uint8_t *sp; uint32_t slen;
        if (take_string(&c, &sp, &slen) != BJ_OK) { e = BJ_ERR_STATE; break; }
        /* Best-effort per file: one stubborn orphan must not strand the
         * rest, and it will be offered again at the next open. */
        if (ns->remove(ns->ctx, (const char *)sp, slen) == BJ_OK) (*deleted)++;
    }
    dbuf_free(&victims);
    return e;
}

/* ---- executing a compaction --------------------------------------------- */

/* Stream one live structure into a freshly opened destination. */
static int compact_into(bj_ns *ns, const uint8_t *name, uint32_t name_len,
                        void *src, int kind, uint64_t *bytes) {
    bj_io dst;
    int e = ns->open(ns->ctx, (const char *)name, name_len,
                     BJ_NS_CREATE | BJ_NS_TRUNC, &dst);
    if (e) return e;
    /*
     * TRUNC is a request, not a guarantee: bjns_bridge.c ignores flags
     * because the host opened the file before C ran, and re-opening is
     * exactly what an asynchronous open forbids. So do it here too,
     * where every adapter can.
     *
     * It matters when a compaction is retried after one crashed
     * mid-build. The catalog still records the old generation, so the
     * plan names gen+1 again -- the same name, over the dead attempt's
     * partial file. Without this the rebuilt tree keeps that tail, and
     * dst.size() below reports it: a compaction that reclaims nothing
     * and records a compactedBytes bigger than the file it just wrote.
     */
    if (dst.truncate && (e = dst.truncate(dst.ctx, 0))) {
        if (ns->close) ns->close(ns->ctx, &dst);
        return e;
    }
    e = (kind == DC_SRC_RTREE) ? rtree_compact((rtree *)src, &dst)
                               : bpt_compact((bpt *)src, &dst);
    if (!e && dst.size) *bytes += dst.size(dst.ctx);
    if (ns->close) ns->close(ns->ctx, &dst);
    return e;
}

int dc_compact_execute(bj_ns *ns, bpt *catalog,
                       const char *coll, size_t coll_len,
                       const uint8_t *plan, size_t plan_len,
                       void *const *sources, const int *source_kinds,
                       uint32_t nsources, uint64_t *bytes_built) {
    *bytes_built = 0;
    if (!ns || !ns->open || !catalog) return BJ_ERR_STATE;

    const uint8_t *entry; size_t entry_len; int found = 0;
    int e = obj_get_field(plan, plan_len, (const uint8_t *)"newEntry", 8,
                          &entry, &entry_len, &found);
    if (e) return e;
    if (!found) return DC_ERR_CATALOG_ENTRY;

    uint32_t used = 0;

    /* ---- Build. The old structures are only READ (bpt_compact and
     * rtree_compact never touch their source), so a failure anywhere in
     * here leaves the collection entirely live on its old generation. */
    {
        const uint8_t *sp; uint32_t slen;
        if ((e = str_field(entry, entry_len, "file", &sp, &slen, &found))) return e;
        if (!found) return DC_ERR_CATALOG_ENTRY;
        if (used >= nsources) return BJ_ERR_RANGE;
        if ((e = compact_into(ns, sp, slen, sources[used], source_kinds[used], bytes_built)))
            return e;
        used++;
    }

    const uint8_t *build; size_t build_len; int has_build = 0;
    if ((e = obj_get_field(plan, plan_len, (const uint8_t *)"build", 5,
                           &build, &build_len, &has_build))) return e;
    if (has_build && build_len >= 1 && build[0] == BJ_TYPE_ARRAY) {
        cur c = { build, build_len, 0 };
        uint32_t n;
        if ((e = array_begin(&c, &n))) return e;
        for (uint32_t i = 0; i < n; i++) {
            size_t dstart = c.pos;
            if ((e = skip_value(&c))) return e;
            const uint8_t *def = build + dstart;
            size_t def_len = c.pos - dstart;

            const uint8_t *files; size_t files_len; int has_files = 0;
            if ((e = obj_get_field(def, def_len, (const uint8_t *)"files", 5,
                                   &files, &files_len, &has_files))) return e;
            if (!has_files || files_len < 1 || files[0] != BJ_TYPE_ARRAY)
                return DC_ERR_CATALOG_ENTRY;
            cur fc = { files, files_len, 0 };
            uint32_t fn;
            if ((e = array_begin(&fc, &fn))) return e;
            for (uint32_t j = 0; j < fn; j++) {
                const uint8_t *sp; uint32_t slen;
                if (take_string(&fc, &sp, &slen) != BJ_OK) return DC_ERR_CATALOG_ENTRY;
                if (used >= nsources) return BJ_ERR_RANGE;
                if ((e = compact_into(ns, sp, slen, sources[used], source_kinds[used],
                                      bytes_built))) return e;
                used++;
            }
        }
    }

    /* ---- The new generation's own empty journal. The old one's recorded
     * lengths describe the old files only, so it cannot be carried over;
     * TRUNC also clears a leftover from a crashed earlier attempt. */
    {
        const uint8_t *sp; uint32_t slen;
        if ((e = str_field(entry, entry_len, "journal", &sp, &slen, &found))) return e;
        if (!found) return DC_ERR_CATALOG_ENTRY;
        bj_io jio;
        if ((e = ns->open(ns->ctx, (const char *)sp, slen,
                          BJ_NS_CREATE | BJ_NS_TRUNC, &jio))) return e;
        if (jio.truncate) e = jio.truncate(jio.ctx, 0);
        if (!e && jio.sync) e = jio.sync(jio.ctx);
        if (ns->close) ns->close(ns->ctx, &jio);
        if (e) return e;
    }

    /* ---- Flip. The entry gains compactedBytes -- Db.compact()'s growth
     * baseline -- and is written as a single catalog commit, then made
     * durable BEFORE the caller deletes the old files: a lost flip after
     * those deletes would leave the catalog pointing at files that no
     * longer exist. */
    {
        bj_builder *b = bj_builder_new();
        if (!b) return BJ_ERR_OOM;
        bj_begin_object(b);
        cur c = { entry, entry_len, 0 };
        uint32_t n;
        if ((e = object_begin(&c, &n))) { bj_builder_free(b); return e; }
        for (uint32_t i = 0; i < n; i++) {
            const uint8_t *kp; uint32_t klen;
            if ((e = take_key(&c, &kp, &klen))) { bj_builder_free(b); return e; }
            size_t vstart = c.pos;
            if ((e = skip_value(&c))) { bj_builder_free(b); return e; }
            if (klen == 14 && memcmp(kp, "compactedBytes", 14) == 0) continue;
            bj_put_key(b, kp, klen);
            bj_put_raw(b, entry + vstart, (uint32_t)(c.pos - vstart));
        }
        bj_put_key(b, (const uint8_t *)"compactedBytes", 14);
        bj_put_int(b, (int64_t)*bytes_built);
        bj_end_object(b);
        if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }

        size_t len = 0;
        const uint8_t *data = bj_builder_data(b, &len);
        if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }

        bpt_key key;
        key.is_string = 1;
        key.num = 0;
        key.str = (const uint8_t *)coll;
        key.str_len = (uint32_t)coll_len;
        e = bpt_add(catalog, &key, data, (uint32_t)len);
        bj_builder_free(b);
        if (e) return e;
        return bpt_sync(catalog);
    }
}
