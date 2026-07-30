/*
 * raft_drive_wasm.c — Emscripten glue over raft_drive.h.
 *
 * Everything here is small and scalar, so it crosses as scalars: the
 * biggest call takes six numbers and answers with five. raft_core_wasm.c
 * uses binjson objects for its two big decisions because fifteen
 * positional numbers is a transposition bug waiting to happen; nothing
 * here is near that.
 *
 * The election round has to survive across calls -- it accumulates votes
 * as replies trickle in -- so it lives in a heap slot the host holds a
 * handle to, like every other stateful thing in this bridge.
 *
 * Indices and terms become doubles here, exact to 2^53. raft_drive.h's
 * uint64_t is what a native host gets; this is where the browser's
 * ceiling lives.
 */
#include "raft_drive.h"

#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten/emscripten.h>
#else
#define EMSCRIPTEN_KEEPALIVE
#endif

/* ---- elections ---------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE raft_round *rdw_round_new(double term, int quorum, int pre_vote) {
    raft_round *r = (raft_round *)calloc(1, sizeof(raft_round));
    if (!r) return NULL;
    raft_round_begin(r, (uint64_t)term, (uint32_t)(quorum < 0 ? 0 : quorum), pre_vote);
    return r;
}
EMSCRIPTEN_KEEPALIVE void rdw_round_free(raft_round *r) { free(r); }

EMSCRIPTEN_KEEPALIVE int rdw_round_granted(const raft_round *r) { return (int)r->granted; }
EMSCRIPTEN_KEEPALIVE int rdw_round_settled(const raft_round *r) { return r->settled; }

/*
 * The action, with the step-down term written into `out_term` -- a
 * one-element f64 slot the caller owns, rather than a second call, so
 * the answer and the term it depends on cannot come from two different
 * moments.
 */
EMSCRIPTEN_KEEPALIVE int rdw_round_on_reply(raft_round *r, double reply_term,
                                            int vote_granted, double current_term,
                                            int is_leader, int is_candidate,
                                            double *out_term) {
    uint64_t step = 0;
    int action = raft_round_on_reply(r, (uint64_t)reply_term, vote_granted,
                                     (uint64_t)current_term, is_leader, is_candidate,
                                     &step);
    if (out_term) *out_term = (double)step;
    return action;
}

/* ---- replication -------------------------------------------------------- */

EMSCRIPTEN_KEEPALIVE int rdw_repl_action(double next, double base_index, int has_snapshot) {
    return raft_repl_decide((uint64_t)next, (uint64_t)base_index, has_snapshot);
}

/* `slots` is [match, next], read and written in place. */
EMSCRIPTEN_KEEPALIVE void rdw_repl_installed(double boundary, double *slots) {
    uint64_t match = (uint64_t)slots[0], next = 0;
    raft_repl_installed((uint64_t)boundary, &match, &next);
    slots[0] = (double)match;
    slots[1] = (double)next;
}

/* ---- snapshot streaming ------------------------------------------------- */

/*
 * `sizes` is an f64 array of nfiles entries; `out` is a 7-slot f64 array
 * receiving, in order:
 *
 *   0 fileIndex  1 offset  2 len  3 isFirst  4 isDone
 *   5 nextFile   6 nextOffset
 *
 * Returns 1 when a chunk was produced, 0 when the stream is exhausted.
 */
EMSCRIPTEN_KEEPALIVE int rdw_chunk_next(const double *sizes, int nfiles,
                                        int chunk_bytes,
                                        int cursor_file, double cursor_offset,
                                        double *out) {
    if (nfiles < 0 || chunk_bytes < 0 || cursor_file < 0) return 0;

    uint64_t *s = NULL;
    if (nfiles > 0) {
        s = (uint64_t *)malloc((size_t)nfiles * sizeof(uint64_t));
        if (!s) return 0;
        for (int i = 0; i < nfiles; i++) s[i] = (uint64_t)sizes[i];
    }

    raft_chunk c;
    int ok = raft_chunk_next(s, (uint32_t)nfiles, (uint32_t)chunk_bytes,
                             (uint32_t)cursor_file, (uint64_t)cursor_offset, &c);
    free(s);
    if (!ok) return 0;

    out[0] = (double)c.file_index;
    out[1] = (double)c.offset;
    out[2] = (double)c.len;
    out[3] = c.is_first ? 1 : 0;
    out[4] = c.is_done ? 1 : 0;
    out[5] = (double)c.next_file;
    out[6] = (double)c.next_offset;
    return 1;
}
