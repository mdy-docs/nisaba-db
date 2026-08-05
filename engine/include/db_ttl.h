/*
 * db_ttl.h — TTL index expiry policy.
 *
 * A TTL index is an ordinary equality index over a Date field plus one
 * number, expireAfterSeconds. Expiry is not a background thread here: it
 * is a sweep the host triggers (Collection.pruneExpired), which deletes
 * every document whose indexed Date is older than the cutoff.
 *
 * Two decisions live in that sentence, and both were duplicated verbatim
 * in JavaScript -- once in src/nisaba-wasm.js and once in src/db-wal.js,
 * whose own header comment notes "pruneExpired's TTL cutoffs become
 * concrete Dates inside ordinary logged deletes":
 *
 *   1. the cutoff arithmetic (now - expireAfterSeconds seconds), and
 *   2. the filter shape it produces ({field: {$lt: <Date cutoff>}}).
 *
 * Both are here now. The two hosts still differ in what they DO with the
 * filter -- the plain collection deletes directly, the WAL logs a delete
 * command first -- which is a real difference and stays theirs. This is
 * the "plan in C, log in JS, apply in C" split the later phases build on,
 * in its smallest form.
 *
 * The clock stays a host parameter for the same reason textlog's ts_ms
 * does: WASM has no portable clock, and a caller-supplied `now` is what
 * makes a sweep reproducible in a test.
 */
#ifndef DB_TTL_H
#define DB_TTL_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * The cutoff instant for a TTL index: documents whose indexed Date is
 * strictly older than this have expired. `now_ms` and the result are
 * milliseconds since the epoch.
 *
 * expire_after_seconds is a double because that is what a JS index
 * definition carries and MongoDB permits fractional values; it is
 * rejected (BJ_ERR_RANGE) if it is NaN or infinite, which would otherwise
 * produce a cutoff that silently matches everything or nothing.
 */
int dc_ttl_cutoff_ms(int64_t now_ms, double expire_after_seconds, int64_t *out_ms);

/*
 * Append the expiry filter {<field>: {$lt: <Date cutoff_ms>}} to `out` as
 * one binjson OBJECT -- exactly the shape dc_delete_many takes.
 *
 * $lt rather than $lte: a document stamped exactly at the cutoff has been
 * alive for precisely expireAfterSeconds and has not yet outlived it.
 */
int dc_ttl_filter(dbuf *out, const char *field, size_t field_len, int64_t cutoff_ms);

#ifdef __cplusplus
}
#endif

#endif /* DB_TTL_H */
