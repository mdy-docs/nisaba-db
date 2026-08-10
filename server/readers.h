/*
 * readers.h — a small pool of threads that perform LONG reads, so that one
 * client's scan stops delaying every other client on the process.
 *
 * WHY THIS EXISTS, in one measurement. A solo native server holding 50,000
 * documents, eight connections doing `_id` point lookups: 52,389 reads in
 * three seconds, median 0.37ms. Add ONE connection running a scanning
 * count and the same eight get 1,992 -- four percent -- at a median of
 * 11.57ms, which is one scan exactly. Every small read was waiting behind
 * a whole large one. That is what this fixes, and it is a latency-isolation
 * fix before it is a throughput one.
 *
 * WHAT A WORKER IS ALLOWED TO TOUCH, and it is very little:
 *
 *   - a READ VIEW (db.h's dc_collection_snapshot), which is a collection
 *     pinned at one instant, minted on the serving thread and handed over.
 *     Every view is private to one job, so two workers share no bpt, no
 *     bjfile read buffer and no dbuf -- which is what makes N workers no
 *     riskier than one. The hazard that would have made sharing handles
 *     impossible (bjfile_read_record reallocs the handle's buffer and
 *     returns a pointer INTO it) simply cannot arise between them.
 *   - dbs_read, which performs the request against that view and nothing
 *     else: no session, no catalog, no cursor table, no log.
 *
 * It opens no file, closes no file, and touches no shared engine state.
 * The one piece of process-global state a read can reach is regex-engine's
 * compiler, and engine/src/regex.c serializes that with its own lock.
 *
 * HOW AN ANSWER GETS BACK. A worker fills a dbuf, queues it, and writes one
 * byte to a self-pipe whose read end lives in the serving thread's pollset
 * -- poll() blocks with no timeout, so a completion could not otherwise
 * wake it. The serving thread reaps completions and delivers them through
 * the deferral path that already existed for a write waiting on the log.
 * No new response machinery, and `conn.out` stays owned by one thread.
 *
 * IDENTITY IS A SEQUENCE NUMBER, not a pointer. A job carries the client id
 * it is for and a never-reused `seq`; it holds no conn*, no pending*, and
 * no dbs*. The connection table is compacted when a client drops and the
 * parked request is released with it, so a pointer would dangle and an
 * index would be reused -- while a seq that matches nothing simply means
 * the client left, and the answer is dropped.
 *
 * NATIVE ONLY. wasm has no threads on either target; the whole file
 * compiles away there and the server answers every read inline as it
 * always did, which is also what --read-threads 0 does natively.
 */
#ifndef READERS_H
#define READERS_H

#include <stddef.h>
#include <stdint.h>

#include "db.h"
#include "dbuf.h"

#if defined(__EMSCRIPTEN__) || defined(__wasi__)
#define SERVER_HAS_READ_THREADS 0
#else
#define SERVER_HAS_READ_THREADS 1
#endif

typedef struct rdpool rdpool;

/*
 * Start `threads` workers, accepting at most `max_inflight` submitted-but-
 * unreaped jobs. Returns NULL on OOM, on a thread that would not start, or
 * when threads <= 0 -- a caller wanting none should not call this.
 *
 * The bound is not defensive padding: one deferred answer per connection
 * means no more reads can be in flight than the server has connections, so
 * a queue sized to that cannot overflow, and rdpool_submit refusing past it
 * reports a broken invariant rather than absorbing one.
 */
rdpool *rdpool_open(int threads, int max_inflight);

/*
 * Stop and join every worker, then free everything still held -- including
 * the read views of jobs that were queued and never run, and the answers of
 * jobs that finished and were never reaped.
 *
 * MUST be called before whatever owns the collections those views were
 * taken from. A view shares its live handles' ios: it closes no file, but
 * it points into one.
 */
void rdpool_close(rdpool *p);

/* The read end of the wake pipe, for the serving thread's pollset. -1 if
 * there is none. Drain it with rdpool_drain_wake before reaping. */
int rdpool_wake_fd(const rdpool *p);
/* Consume whatever wake bytes have arrived. Never blocks. Reaping is what
 * actually reports work; this only stops poll() returning immediately
 * forever. */
void rdpool_drain_wake(rdpool *p);

/*
 * Hand one read over. TAKES OWNERSHIP OF `view` on success -- the worker
 * frees it when the read finishes -- and copies `req`, which belongs to a
 * connection buffer the serving thread will reuse the moment this returns.
 *
 * BJ_OK, or BJ_ERR_RANGE past max_inflight / BJ_ERR_OOM. On any failure
 * `view` is NOT taken and the caller still owns it, so the read can simply
 * be performed inline instead: this is a scheduling choice, and failing to
 * make it must never fail a request.
 */
int rdpool_submit(rdpool *p, uint64_t client, uint64_t seq,
                  dc_collection *view, const uint8_t *req, size_t req_len);

/*
 * Take one finished read, oldest first. *have = 0 when there is none.
 *
 * `out` receives the answer bytes dbs_read built -- appended, so the caller
 * decides where they go. *rc is what dbs_read returned: non-zero with an
 * empty answer means it could not build one at all, and the caller renders
 * the refusal on the serving thread, where every other refusal is built.
 *
 * The view is already freed by the worker. There is nothing else to release.
 */
int rdpool_reap(rdpool *p, uint64_t *client, uint64_t *seq, int *rc,
                dbuf *out, int *have);

/* Submitted and not yet reaped, queued and running alike. What "no reader
 * is inside the engine" is measured by, which is what anything about to
 * truncate or replace a file under a view has to wait for. */
int rdpool_inflight(const rdpool *p);

/*
 * Block until at least one finished read is waiting to be reaped, or until
 * nothing is outstanding at all. Returns without reaping anything.
 *
 * It does not reap, and it does not drain, DELIBERATELY. A finished read has
 * a client waiting for its answer, and something that threw those answers
 * away to reach an idle state would leave every one of those clients
 * hanging on a reply that was built and discarded. So the caller pairs this
 * with its own reaping, which delivers them: see replica_wait_reads_idle.
 *
 * Serving thread only. Reads are submitted from there, so nothing else
 * could be sure the count is falling.
 */
void rdpool_wait_completion(rdpool *p);

#endif /* READERS_H */
