/*
 * db_agg.h — the aggregation pipeline.
 *
 * A deliberately small subset (docs/db-api.md): $match, $sort, $skip,
 * $limit, $project, $group, $count. No $lookup, no $unwind, no expression
 * operators -- see the non-goals in README.md.
 *
 * Why this is here rather than in JavaScript
 * ------------------------------------------
 * docs/db-api.md defends aggregate() as "executed in JS over materialized
 * find() results -- enough for the 'group and summarize' reach-for-it
 * moments without duplicating the C query engine". The JS implementation
 * then duplicated the C query engine anyway, in miniature: its own
 * matcher over a nine-operator subset, its own cross-type total order,
 * its own equality, and its own sort. Its own comment admitted as much,
 * saying the helpers "define their own explicit, documented semantics
 * rather than mirroring db_query.c's evaluator".
 *
 * So this file is mostly composition, not new logic. $match is
 * qry_matches. $sort/$skip/$limit/$project are one qry_collect call --
 * the same code path find() takes. Only $group and $count are new, and
 * $group's grouping key is the value's own binjson encoding, which is
 * already canonical.
 *
 * Three consequences, all improvements
 * ------------------------------------
 * 1. A $match after the first stage now has the FULL operator grammar
 *    ($regex, $elemMatch, $size, $all, $type, $mod, $not, $nor, ...),
 *    where the JS subset threw on anything outside its nine operators.
 * 2. Sorting is the engine's ordering, not a second total order that
 *    happened to disagree about how types rank.
 * 3. $group and $addToSet identity is exact encoded-byte equality rather
 *    than JSON.stringify, which was key-order sensitive in a way nobody
 *    intended: {a:1,b:2} and {b:2,a:1} grouped separately.
 *
 * The leading-$match pushdown also moves here: if the first stage is
 * exactly one $match, it becomes the filter of the underlying find, so
 * indexes and the planner serve it. That decision was JS's, and it is the
 * kind of decision that belongs next to the planner it feeds.
 */
#ifndef DB_AGG_H
#define DB_AGG_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "db.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Continuing the DC_ERR_* range (db_bulk.h ends at -22). */
#define DC_ERR_AGG_BAD_STAGE      (-23)
#define DC_ERR_AGG_UNKNOWN_STAGE  (-24)
#define DC_ERR_AGG_BAD_ACCUMULATOR (-25)
#define DC_ERR_AGG_PROJECT_MIXED  (-26)

/*
 * Run `stages` (a binjson ARRAY of single-key OBJECTs) over `c` and emit
 * the result as a binjson ARRAY of documents through *out / *out_len
 * (freshly malloc'd, caller frees).
 *
 * A leading $match is pushed into the underlying scan; everything after
 * it runs over materialized documents, exactly as before.
 *
 * On a stage error, *bad_stage is that stage's index so the caller can
 * name it -- the caller is holding the pipeline, so it can quote the
 * offending stage without C having to format a message around user data.
 * -1 when the failure was not stage-specific.
 */
int dc_aggregate(dc_collection *c, const uint8_t *stages, size_t stages_len,
                 int *bad_stage, uint8_t **out, size_t *out_len);

#ifdef __cplusplus
}
#endif

#endif /* DB_AGG_H */
