/*
 * writer.h — the thread that performs APPLIES, so that applying committed
 * entries stops delaying every client on the process.
 *
 * WHY THIS EXISTS, in one measurement. Eight connections of `_id` point
 * lookups against 50,000 documents hold ~18,700 reads/s idle at a median
 * of 0.34ms. Start ONE updateMany over the collection and they hold 246/s
 * -- one percent -- at a median of 12.3ms, which is one apply wave
 * exactly. The reader threads (readers.h) moved long READS off the
 * serving thread; the applies stayed, and a burst of committed entries is
 * a burst of documents written with nothing else served in between. That
 * is the head-of-line blocking this removes, and like the reader pool it
 * is a latency-isolation fix before it is a throughput one.
 *
 * WHY IT IS POSSIBLE NOW. Apply is ordered, so a worker holding one
 * 716ms createIndex entry would either block the loop anyway (anything
 * needing an entry boundary waits out the entry) or force every apply
 * off-loop behind it. The staged build (docs/db-server.md) capped the
 * largest entry at one chunk -- ~2.3ms at the default k -- so "the writer
 * parks at the next entry boundary" is now a bounded promise, which is
 * the load-bearing fact under the whole design.
 *
 * THE OWNERSHIP SPLIT, and it is strict:
 *
 *   - the LOOP owns the raft node, the entry log, and every socket. It
 *     fetches committed entries (elog_get stays loop-side -- the log
 *     stays single-threaded), COPIES each payload, and hands them over.
 *     rn_applied is called by the loop, at reap, never here.
 *   - the WRITER owns dbi_apply and nothing else. One thread, in log
 *     order, one entry at a time. It opens no socket, reads no log,
 *     touches no node.
 *
 * Everything else the loop does against live trees -- minting read
 * views, planning writes, cursors, streams, snapshots -- happens with
 * the writer PARKED at an entry boundary (wr_pause) or idle, which the
 * caller arranges; this file only promises that a parked writer is not
 * inside the engine.
 *
 * HOW AN ANSWER GETS BACK: exactly readers.h's shape. A finished apply
 * queues its (index, rc, result) and writes one byte to a self-pipe
 * whose read end lives in the serving thread's pollset. The loop reaps
 * in completion order -- which IS log order, single worker -- routes the
 * result to whoever proposed the entry, and calls rn_applied.
 *
 * NATIVE ONLY. wasm has no threads on either target; the whole file
 * compiles away there and the server applies inline as it always did,
 * which is also what --write-thread 0 does natively.
 */
#ifndef WRITER_H
#define WRITER_H

#include <stddef.h>
#include <stdint.h>

#include "db_instance.h"
#include "dbuf.h"

#if defined(__EMSCRIPTEN__) || defined(__wasi__)
#define SERVER_HAS_WRITE_THREAD 0
#else
#define SERVER_HAS_WRITE_THREAD 1
#endif

typedef struct wrpool wrpool;

/*
 * Start the writer over `inst`, which is BORROWED and must outlive this
 * -- except across an install, where wr_set_instance swaps it while the
 * writer is idle. NULL on OOM, on a thread that would not start, or on a
 * target with no threads.
 */
wrpool *wr_open(dbi *inst);

/*
 * Stop and join the worker, then free every job still held. Entries
 * submitted and never applied are simply not applied -- the applied
 * floor never covered them, so the next boot replays them, which is the
 * same crash story a kill mid-apply has always had.
 *
 * MUST be called before whatever owns the instance those applies run
 * against.
 */
void wr_close(wrpool *p);

/*
 * The instance applies run against, replaced. Only while nothing is
 * submitted and unapplied (an install drains first -- the caller's
 * discipline, asserted here by refusing otherwise): the old pointer may
 * be freed the moment this returns.
 */
int wr_set_instance(wrpool *p, dbi *inst);

/* The read end of the wake pipe, for the serving thread's pollset. -1 if
 * there is none. Drain it with wr_drain_wake before reaping. */
int wr_wake_fd(const wrpool *p);
void wr_drain_wake(wrpool *p);

/*
 * Hand one committed entry over. COPIES `payload` -- it belongs to the
 * log, whose pointer dies on the next log operation. Entries must be
 * submitted in log order; the worker applies them in exactly that order.
 *
 * BJ_OK, or BJ_ERR_OOM / BJ_ERR_STATE. On failure nothing was taken and
 * the caller may apply inline instead -- scheduling, not correctness.
 */
int wr_submit(wrpool *p, uint64_t index, const uint8_t *payload, uint32_t len);

/*
 * Take one finished apply, oldest first -- log order, since there is one
 * worker. *have = 0 when there is none. `result` receives what dbi_apply
 * appended (the answer the proposer is owed); *rc is what it returned,
 * and judging it (deterministic refusal vs halt) is the caller's, exactly
 * as it was when the apply ran inline.
 */
int wr_reap(wrpool *p, uint64_t *index, int *rc, dbuf *result, int *have);

/* Submitted and not yet reaped. */
int wr_inflight(const wrpool *p);

/* Submitted and not yet APPLIED -- the part a boundary pause waits out.
 * Distinct from wr_inflight: an applied-but-unreaped entry holds only
 * its result, and nothing in the engine. */
int wr_unapplied(const wrpool *p);

/*
 * Whether some apply since the last wr_events_set(0) queued change
 * events -- the worker checks its own work and raises this, so the loop
 * only pays a boundary pause for the stream drain when there is
 * something to drain. Re-arm it UNDER that pause, from what
 * dbi_stream_pending says after draining: a stream held back by the
 * high-water mark must keep the flag up, or its events sleep until the
 * next unrelated apply.
 */
int  wr_events(const wrpool *p);
void wr_events_set(wrpool *p, int on);

/*
 * THE BOUNDARY PAUSE. Return with the worker parked between entries: not
 * inside dbi_apply, and unable to start another until wr_resume. Queued
 * entries stay queued. Bounded by the largest single apply, which the
 * staged index build capped -- that promise is the reason this can be on
 * every cursor step and stream drain without becoming the old stall.
 *
 * Pauses NEST: a teardown inside a paused block is two holders of the
 * same park, and the worker runs again when the last wr_resume lets go.
 * Serving thread only, and never while holding anything the worker's
 * completion path needs.
 */
void wr_pause(wrpool *p);
void wr_resume(wrpool *p);

/*
 * Block until everything submitted has been APPLIED (not reaped: the
 * results wait for the caller, who delivers them -- see readers.h's
 * rdpool_wait_completion for why a drain must never discard answers).
 * The caller then reaps. Serving thread only. Must not be called while
 * paused: a parked worker applies nothing, and this would wait forever
 * on it.
 */
void wr_wait_applied(wrpool *p);

#endif /* WRITER_H */
