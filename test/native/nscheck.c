/* nscheck.c — see nscheck.h. */
#include "nscheck.h"

#include "binjson.h"
#include "bjcursor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Fixed capacities. A compaction of a collection with a dozen indexes
 * declares under thirty names, and db_names.h caps a file name far below
 * this -- so overflowing either means a test built something the real
 * scheme cannot, which is worth failing on rather than growing for. */
#define NSCHECK_MAX_DECLS 64
#define NSCHECK_NAME_MAX  256

typedef struct {
    char     name[NSCHECK_NAME_MAX];
    uint32_t len;
    int      opened;
} decl;

struct nscheck {
    bj_ns    inner;
    decl     declared[NSCHECK_MAX_DECLS];
    uint32_t ndecl;
    uint32_t opens, removes, violations;
    char     first[NSCHECK_NAME_MAX + 64];
    int      have_first;
};

static void violation(nscheck *k, const char *what, const char *name, uint32_t len) {
    k->violations++;
    if (k->have_first) return;
    if (len >= NSCHECK_NAME_MAX) len = NSCHECK_NAME_MAX - 1;
    snprintf(k->first, sizeof(k->first), "%s \"%.*s\"", what, (int)len, name);
    k->have_first = 1;
}

/* ---- the vtable --------------------------------------------------------- */

static int32_t nc_open(void *ctx, const char *name, uint32_t name_len,
                       uint32_t flags, bj_io *out) {
    nscheck *k = (nscheck *)ctx;

    decl *d = NULL;
    for (uint32_t i = 0; i < k->ndecl; i++) {
        if (k->declared[i].len == name_len &&
            memcmp(k->declared[i].name, name, name_len) == 0) {
            d = &k->declared[i];
            break;
        }
    }
    if (!d) {
        violation(k, "open of undeclared name", name, name_len);
        return BJ_ERR_STATE;   /* exactly what bjns_bridge.c returns */
    }

    /*
     * The bridge cannot honor TRUNC or EXCL -- the host opened the file
     * already, and re-opening is precisely what an async open forbids.
     * Dropping them here means C that quietly relies on either fails its
     * own assertions natively instead of in a browser. CREATE survives:
     * the host is told `{create: true}` for every name it pre-opens.
     */
    flags &= ~(BJ_NS_TRUNC | BJ_NS_EXCL);

    int32_t rc = k->inner.open(k->inner.ctx, name, name_len, flags, out);
    if (rc == BJ_OK) {
        d->opened = 1;
        k->opens++;
    }
    return rc;
}

static int32_t nc_close(void *ctx, bj_io *io) {
    nscheck *k = (nscheck *)ctx;
    return k->inner.close ? k->inner.close(k->inner.ctx, io) : BJ_OK;
}

static int32_t nc_remove(void *ctx, const char *name, uint32_t name_len) {
    nscheck *k = (nscheck *)ctx;
    k->removes++;
    return k->inner.remove ? k->inner.remove(k->inner.ctx, name, name_len) : BJ_OK;
}

static int32_t nc_sync(void *ctx) {
    nscheck *k = (nscheck *)ctx;
    return k->inner.sync ? k->inner.sync(k->inner.ctx) : BJ_OK;
}

/* ---- lifecycle ---------------------------------------------------------- */

nscheck *nscheck_new(const bj_ns *inner, bj_ns *out) {
    if (!inner || !inner->open) return NULL;
    nscheck *k = (nscheck *)calloc(1, sizeof(nscheck));
    if (!k) return NULL;
    k->inner = *inner;
    bj_ns ns = {
        .ctx    = k,
        .open   = nc_open,
        .close  = nc_close,
        .remove = nc_remove,
        .sync   = nc_sync,
    };
    *out = ns;
    return k;
}

void nscheck_free(nscheck *k) { free(k); }

/* ---- declarations ------------------------------------------------------- */

void nscheck_begin(nscheck *k) { k->ndecl = 0; }
void nscheck_end(nscheck *k)   { k->ndecl = 0; }

int nscheck_declare(nscheck *k, const char *name, uint32_t len) {
    if (k->ndecl == NSCHECK_MAX_DECLS) return BJ_ERR_RANGE;
    if (len == 0 || len >= NSCHECK_NAME_MAX) return BJ_ERR_RANGE;
    decl *d = &k->declared[k->ndecl++];
    memcpy(d->name, name, len);
    d->len = len;
    d->opened = 0;
    return BJ_OK;
}

/* Declare the string a field holds, given the field's raw value span. */
static int declare_field(nscheck *k, const uint8_t *obj, size_t obj_len,
                         const char *field) {
    const uint8_t *v; size_t vlen; int found = 0;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)field,
                          (uint32_t)strlen(field), &v, &vlen, &found);
    if (e) return e;
    if (!found) return BJ_ERR_STATE;
    cur c = { v, vlen, 0 };
    const uint8_t *sp; uint32_t slen;
    if (take_string(&c, &sp, &slen) != BJ_OK) return BJ_ERR_STATE;
    return nscheck_declare(k, (const char *)sp, slen);
}

int nscheck_declare_compact_plan(nscheck *k, const uint8_t *plan, size_t plan_len) {
    const uint8_t *entry; size_t entry_len; int found = 0;
    int e = obj_get_field(plan, plan_len, (const uint8_t *)"newEntry", 8,
                          &entry, &entry_len, &found);
    if (e) return e;
    if (!found) return BJ_ERR_STATE;

    /* The primary first, then the indexes, then the journal -- the order
     * wasm/nisaba-wasm.js awaits them in and dc_compact_execute consumes
     * them in. */
    if ((e = declare_field(k, entry, entry_len, "file"))) return e;

    const uint8_t *build; size_t build_len; int has_build = 0;
    if ((e = obj_get_field(plan, plan_len, (const uint8_t *)"build", 5,
                           &build, &build_len, &has_build))) return e;
    if (has_build) {
        cur c = { build, build_len, 0 };
        uint32_t n;
        if ((e = array_begin(&c, &n))) return e;
        for (uint32_t i = 0; i < n; i++) {
            size_t start = c.pos;
            if ((e = skip_value(&c))) return e;
            const uint8_t *def = build + start;
            size_t def_len = c.pos - start;

            const uint8_t *files; size_t files_len; int has_files = 0;
            if ((e = obj_get_field(def, def_len, (const uint8_t *)"files", 5,
                                   &files, &files_len, &has_files))) return e;
            if (!has_files) return BJ_ERR_STATE;
            cur fc = { files, files_len, 0 };
            uint32_t fn;
            if ((e = array_begin(&fc, &fn))) return e;
            for (uint32_t j = 0; j < fn; j++) {
                const uint8_t *sp; uint32_t slen;
                if (take_string(&fc, &sp, &slen) != BJ_OK) return BJ_ERR_STATE;
                if ((e = nscheck_declare(k, (const char *)sp, slen))) return e;
            }
        }
    }

    return declare_field(k, entry, entry_len, "journal");
}

/* ---- reporting ---------------------------------------------------------- */

uint32_t nscheck_opens(const nscheck *k)      { return k->opens; }
uint32_t nscheck_removes(const nscheck *k)    { return k->removes; }
uint32_t nscheck_violations(const nscheck *k) { return k->violations; }

uint32_t nscheck_unopened(const nscheck *k) {
    uint32_t n = 0;
    for (uint32_t i = 0; i < k->ndecl; i++) if (!k->declared[i].opened) n++;
    return n;
}

const char *nscheck_first_violation(const nscheck *k) {
    return k->have_first ? k->first : NULL;
}
