/*
 * db_bulk.c — see db_bulk.h.
 */
#include "db_bulk.h"
#include "bjcursor.h"

#include <string.h>

/* One operation name, its type code, and the field it cannot do without.
 * `filter2` is the second required field for the update-shaped ops. */
typedef struct {
    const char *name;
    dc_bulk_op  code;
    const char *required;
    const char *required2;   /* NULL when there is only one */
} bulk_spec;

static const bulk_spec SPECS[] = {
    { "insertOne",  DC_BULK_INSERT_ONE,  "document", NULL          },
    { "updateOne",  DC_BULK_UPDATE_ONE,  "filter",   "update"      },
    { "updateMany", DC_BULK_UPDATE_MANY, "filter",   "update"      },
    { "replaceOne", DC_BULK_REPLACE_ONE, "filter",   "replacement" },
    { "deleteOne",  DC_BULK_DELETE_ONE,  "filter",   NULL          },
    { "deleteMany", DC_BULK_DELETE_MANY, "filter",   NULL          },
};
static const size_t SPEC_COUNT = sizeof(SPECS) / sizeof(SPECS[0]);

static int has_field(const uint8_t *obj, size_t obj_len, const char *name) {
    const uint8_t *val; size_t val_len; int found = 0;
    int e = obj_get_field(obj, obj_len, (const uint8_t *)name,
                          (uint32_t)strlen(name), &val, &val_len, &found);
    if (e) return 0;
    /* Present but explicitly null/absent-valued is not present enough to
     * run: it would reach the method as `undefined` either way. */
    if (found && val_len >= 1 && val[0] == BJ_TYPE_NULL) return 0;
    return found;
}

int dc_bulk_parse(const uint8_t *ops, size_t len, dbuf *out, int *bad_index) {
    *bad_index = -1;

    cur c = { ops, len, 0 };
    uint32_t count;
    int e = array_begin(&c, &count);
    if (e) return e;
    if (count == 0) return DC_ERR_BULK_EMPTY;

    /* Pass one: validate everything, emitting nothing. An unordered
     * bulkWrite must be able to attempt every operation, which it cannot
     * do if a malformed one only surfaces halfway through. */
    cur scan = c;
    for (uint32_t i = 0; i < count; i++) {
        size_t op_start = scan.pos;
        if ((e = skip_value(&scan))) { *bad_index = (int)i; return e; }
        const uint8_t *op = ops + op_start;
        size_t op_len = scan.pos - op_start;

        cur inner = { op, op_len, 0 };
        uint32_t nkeys;
        if (object_begin(&inner, &nkeys) != BJ_OK || nkeys != 1) {
            *bad_index = (int)i;
            return DC_ERR_BULK_UNKNOWN_OP;
        }
        const uint8_t *kp; uint32_t klen;
        if (take_key(&inner, &kp, &klen) != BJ_OK) {
            *bad_index = (int)i;
            return DC_ERR_BULK_UNKNOWN_OP;
        }
        const bulk_spec *spec = NULL;
        for (size_t s = 0; s < SPEC_COUNT; s++) {
            if (klen == strlen(SPECS[s].name) && memcmp(kp, SPECS[s].name, klen) == 0) {
                spec = &SPECS[s];
                break;
            }
        }
        if (!spec) { *bad_index = (int)i; return DC_ERR_BULK_UNKNOWN_OP; }

        /* The operation's own spec object. */
        size_t spec_start = inner.pos;
        if (skip_value(&inner) != BJ_OK) { *bad_index = (int)i; return DC_ERR_BULK_MISSING_FIELD; }
        const uint8_t *sp = op + spec_start;
        size_t sp_len = inner.pos - spec_start;
        if (sp_len < 1 || sp[0] != BJ_TYPE_OBJECT) {
            *bad_index = (int)i;
            return DC_ERR_BULK_MISSING_FIELD;
        }
        if (!has_field(sp, sp_len, spec->required) ||
            (spec->required2 && !has_field(sp, sp_len, spec->required2))) {
            *bad_index = (int)i;
            return DC_ERR_BULK_MISSING_FIELD;
        }
    }

    /* Pass two: emit the type codes. */
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    bj_begin_array(b);
    for (uint32_t i = 0; i < count; i++) {
        size_t op_start = c.pos;
        if ((e = skip_value(&c))) { bj_builder_free(b); return e; }
        cur inner = { ops + op_start, c.pos - op_start, 0 };
        uint32_t nkeys;
        if ((e = object_begin(&inner, &nkeys))) { bj_builder_free(b); return e; }
        const uint8_t *kp; uint32_t klen;
        if ((e = take_key(&inner, &kp, &klen))) { bj_builder_free(b); return e; }
        for (size_t s = 0; s < SPEC_COUNT; s++) {
            if (klen == strlen(SPECS[s].name) && memcmp(kp, SPECS[s].name, klen) == 0) {
                bj_put_int(b, (int64_t)SPECS[s].code);
                break;
            }
        }
    }
    bj_end_array(b);
    if ((e = bj_builder_error(b))) { bj_builder_free(b); return e; }

    size_t out_len = 0;
    const uint8_t *data = bj_builder_data(b, &out_len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, out_len);
    bj_builder_free(b);
    return e;
}
