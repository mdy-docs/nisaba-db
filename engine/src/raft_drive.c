/* raft_drive.c — see raft_drive.h. */
#include "raft_drive.h"

#include <string.h>

/* ---- elections ---------------------------------------------------------- */

void raft_round_begin(raft_round *r, uint64_t term, uint32_t quorum, int pre_vote) {
    memset(r, 0, sizeof(*r));
    r->term     = term;
    r->quorum   = quorum ? quorum : 1;
    r->granted  = 1;            /* our own vote */
    r->pre_vote = pre_vote ? 1 : 0;
    r->settled  = 0;
}

int raft_round_on_reply(raft_round *r, uint64_t reply_term, int vote_granted,
                        uint64_t current_term, int is_leader, int is_candidate,
                        uint64_t *step_down_term) {
    if (r->settled) return RAFT_ROUND_IGNORE;

    /* A higher term deposes us whatever the round was doing, and it does
     * so even on a pre-vote reply -- the peer's term is news regardless
     * of whether it was willing to vote. */
    if (reply_term > current_term) {
        r->settled = 1;
        if (step_down_term) *step_down_term = reply_term;
        return RAFT_ROUND_STEP_DOWN;
    }

    /*
     * Is the world this round was started in still standing?
     *
     * A pre-vote polls from the CURRENT term about the next one, so it
     * stays valid only while the node has not advanced: current_term must
     * still be term - 1, and we must not have become leader in the
     * meantime (a pre-vote that wins after we already lead would start a
     * pointless election and depose us).
     *
     * A real round put its term on disk before asking, so validity is the
     * simpler statement: still a candidate, still in that term.
     */
    if (r->pre_vote) {
        if (is_leader || current_term != r->term - 1) {
            r->settled = 1;
            return RAFT_ROUND_IGNORE;
        }
    } else if (!is_candidate || current_term != r->term) {
        r->settled = 1;
        return RAFT_ROUND_IGNORE;
    }

    if (!vote_granted) return RAFT_ROUND_PENDING;

    if (++r->granted >= r->quorum) {
        r->settled = 1;
        return RAFT_ROUND_WON;
    }
    return RAFT_ROUND_PENDING;
}

/* ---- replication -------------------------------------------------------- */

int raft_repl_decide(uint64_t next, uint64_t base_index, int has_snapshot) {
    if (next > base_index) return RAFT_REPL_APPEND;
    return has_snapshot ? RAFT_REPL_SNAPSHOT : RAFT_REPL_PARK;
}

void raft_repl_installed(uint64_t boundary, uint64_t *match, uint64_t *next) {
    if (boundary > *match) *match = boundary;
    *next = *match + 1;
}

/* ---- snapshot streaming ------------------------------------------------- */

int raft_chunk_next(const uint64_t *file_sizes, uint32_t nfiles,
                    uint32_t chunk_bytes,
                    uint32_t cursor_file, uint64_t cursor_offset,
                    raft_chunk *out) {
    memset(out, 0, sizeof(*out));
    if (chunk_bytes == 0) return 0;

    out->is_first = (cursor_file == 0 && cursor_offset == 0);

    /* No files: one empty chunk, carrying the manifest and ending the
     * stream in the same message. The cursor moves to 1 so a second call
     * reports exhaustion rather than sending it again. */
    if (nfiles == 0) {
        if (!out->is_first) return 0;
        out->is_done    = 1;
        out->next_file  = 1;
        return 1;
    }

    if (cursor_file >= nfiles) return 0;
    if (cursor_offset > file_sizes[cursor_file]) return 0;

    uint64_t remaining = file_sizes[cursor_file] - cursor_offset;
    uint32_t len = remaining > (uint64_t)chunk_bytes ? chunk_bytes : (uint32_t)remaining;

    out->file_index = cursor_file;
    out->offset     = cursor_offset;
    out->len        = len;

    /* Finishing this file moves the cursor to the next one, rather than
     * to its own end -- see the header. An empty file finishes on the
     * chunk that carries none of it. */
    if (cursor_offset + len >= file_sizes[cursor_file]) {
        out->next_file   = cursor_file + 1;
        out->next_offset = 0;
        out->is_done     = (out->next_file >= nfiles);
    } else {
        out->next_file   = cursor_file;
        out->next_offset = cursor_offset + len;
    }
    return 1;
}
