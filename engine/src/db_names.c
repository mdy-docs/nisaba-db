/*
 * db_names.c — see db_names.h.
 */
#include "db_names.h"

#include <string.h>

/* Append a decimal uint32. No stdio anywhere in this codebase (and none
 * under a bare wasi-libc build worth pulling in for four digits). */
static int put_u32(dbuf *out, uint32_t v) {
    char tmp[10];
    int n = 0;
    do { tmp[n++] = (char)('0' + (v % 10)); v /= 10; } while (v);
    char rev[10];
    for (int i = 0; i < n; i++) rev[i] = tmp[n - 1 - i];
    return dbuf_put(out, (const uint8_t *)rev, (size_t)n);
}

static int put_lit(dbuf *out, const char *s) {
    return dbuf_put(out, (const uint8_t *)s, strlen(s));
}

/* `g<N>-` for gen > 0, nothing for gen 0. */
static int put_gen_prefix(dbuf *out, uint32_t gen) {
    if (!gen) return BJ_OK;
    int e = put_lit(out, "g");
    if (e) return e;
    e = put_u32(out, gen);
    if (e) return e;
    return put_lit(out, "-");
}

int dc_collection_file_name(dbuf *out, const char *coll, size_t coll_len, uint32_t gen) {
    int e = put_gen_prefix(out, gen);
    if (e) return e;
    e = put_lit(out, "coll-");
    if (e) return e;
    e = dbuf_put(out, (const uint8_t *)coll, coll_len);
    if (e) return e;
    return put_lit(out, ".bj");
}

/* `<gen>idx-<coll>-<idx>` -- the shared stem of every index file name. */
static int put_index_stem(dbuf *out, const char *coll, size_t coll_len,
                          const char *idx, size_t idx_len, uint32_t gen) {
    int e = put_gen_prefix(out, gen);
    if (e) return e;
    e = put_lit(out, "idx-");
    if (e) return e;
    e = dbuf_put(out, (const uint8_t *)coll, coll_len);
    if (e) return e;
    e = put_lit(out, "-");
    if (e) return e;
    return dbuf_put(out, (const uint8_t *)idx, idx_len);
}

int dc_index_file_name(dbuf *out, const char *coll, size_t coll_len,
                       const char *idx, size_t idx_len, uint32_t gen) {
    int e = put_index_stem(out, coll, coll_len, idx, idx_len, gen);
    if (e) return e;
    return put_lit(out, ".bj");
}

int dc_text_index_file_name(dbuf *out, const char *coll, size_t coll_len,
                            const char *idx, size_t idx_len, uint32_t gen,
                            dc_text_role role) {
    int e = put_index_stem(out, coll, coll_len, idx, idx_len, gen);
    if (e) return e;
    switch (role) {
        case DC_TEXT_ROLE_TERMS:     return put_lit(out, "-terms.bj");
        case DC_TEXT_ROLE_DOCUMENTS: return put_lit(out, "-documents.bj");
        case DC_TEXT_ROLE_LENGTHS:   return put_lit(out, "-lengths.bj");
    }
    return BJ_ERR_RANGE;
}

int dc_journal_file_name(dbuf *out, const char *coll, size_t coll_len, uint32_t gen) {
    int e = put_gen_prefix(out, gen);
    if (e) return e;
    e = put_lit(out, "coll-");
    if (e) return e;
    e = dbuf_put(out, (const uint8_t *)coll, coll_len);
    if (e) return e;
    return put_lit(out, "-journal.bj");
}

int dc_is_db_file(const char *name, size_t len) {
    size_t at = 0;

    /*
     * Optional `g<digits>-`. No backtracking needed even though the
     * equivalent regex would allow it: the alternative branch requires
     * the name to start with "coll-"/"idx-", and neither begins with 'g',
     * so a name that enters this branch can never match the other one.
     */
    if (at < len && name[at] == 'g') {
        size_t p = at + 1;
        size_t digits = 0;
        while (p < len && name[p] >= '0' && name[p] <= '9') { p++; digits++; }
        if (digits > 0 && p < len && name[p] == '-') at = p + 1;
    }

    size_t rest = len - at;
    if (rest >= 5 && memcmp(name + at, "coll-", 5) == 0) at += 5;
    else if (rest >= 4 && memcmp(name + at, "idx-", 4) == 0) at += 4;
    else return 0;

    /* `.*\.bj$` -- anything (possibly nothing) then the extension. */
    if (len - at < 3) return 0;
    return memcmp(name + len - 3, ".bj", 3) == 0 ? 1 : 0;
}
