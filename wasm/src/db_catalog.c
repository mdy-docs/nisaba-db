/*
 * db_catalog.c — see db_catalog.h.
 */
#include "db_catalog.h"
#include "db_names.h"
#include "bjcursor.h"

#include <string.h>

/* ---- small readers over an encoded entry ------------------------------- */

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
static const char *TEXT_ROLE_KEYS[3] = { "index", "docTerms", "docLengths" };

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
