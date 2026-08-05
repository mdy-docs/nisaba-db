/*
 * db_ttl.c — see db_ttl.h.
 */
#include "db_ttl.h"

#include <math.h>
#include <string.h>

int dc_ttl_cutoff_ms(int64_t now_ms, double expire_after_seconds, int64_t *out_ms) {
    if (!isfinite(expire_after_seconds)) return BJ_ERR_RANGE;

    double delta_ms = expire_after_seconds * 1000.0;
    /* Keep the subtraction in the range where an int64 conversion is
     * defined. A TTL far beyond the representable range means "nothing has
     * expired yet", which is the honest answer -- not undefined behavior
     * from an out-of-range cast. */
    double cutoff = (double)now_ms - delta_ms;
    if (cutoff > 9.2e18) return BJ_ERR_RANGE;
    if (cutoff < -9.2e18) return BJ_ERR_RANGE;

    *out_ms = (int64_t)cutoff;
    return BJ_OK;
}

int dc_ttl_filter(dbuf *out, const char *field, size_t field_len, int64_t cutoff_ms) {
    if (field_len == 0) return BJ_ERR_RANGE;
    if (field_len > 0xffffffffu) return BJ_ERR_RANGE;

    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;

    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)field, (uint32_t)field_len);
    bj_begin_object(b);
    bj_put_key(b, (const uint8_t *)"$lt", 3);
    bj_put_date(b, cutoff_ms);
    bj_end_object(b);
    bj_end_object(b);

    int e = bj_builder_error(b);
    if (e) { bj_builder_free(b); return e; }

    size_t len = 0;
    const uint8_t *data = bj_builder_data(b, &len);
    if (!data) { bj_builder_free(b); return BJ_ERR_STATE; }
    e = dbuf_put(out, data, len);
    bj_builder_free(b);
    return e;
}
