/*
 * raft_drive.h — the leader's and candidate's own bookkeeping, as pure
 * functions.
 *
 * raft_core.h took the rules a node applies to a message it RECEIVES.
 * raft_msg.h took the grammar those messages are written in. What was
 * left in JavaScript is the other half of the conversation: deciding
 * what to send next, and what a reply to something we sent means.
 *
 *   raft_round_*        a candidate tallying an election round
 *   raft_repl_decide    what a leader owes a peer right now
 *   raft_chunk_next     walking a snapshot's files as a chunk stream
 *
 * These are not consensus rules in the sense raft_core.h's are -- a bug
 * here loses liveness or wastes a round trip rather than committed data.
 * They are here because they are the parts a host cannot get right by
 * accident: each is a handful of guards whose failure mode is a cluster
 * that mostly works.
 *
 * The election round is the sharpest case. Its reply handler ran inside
 * a promise callback, so by the time a vote arrives the world it was
 * cast in may be several terms gone: the node may have stepped down,
 * won already, or started a newer round. Four separate guards existed to
 * catch that, and getting any of them wrong yields a node that declares
 * victory on votes cast for a different term -- which is a split brain,
 * arrived at through liveness code rather than a safety rule.
 *
 * Nothing here mutates anything the caller did not pass in, and nothing
 * here does I/O. The host still owns the socket, the promise and the
 * clock.
 */
#ifndef RAFT_DRIVE_H
#define RAFT_DRIVE_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- elections ---------------------------------------------------------- */

/*
 * One election round in flight. `granted` starts at 1: a candidate votes
 * for itself, and a pre-voter counts itself willing.
 *
 * `settled` is the round's own latch, not the node's. Once a round has
 * produced its answer -- won, or deposed, or invalidated -- every later
 * reply belonging to it is noise, including replies that would otherwise
 * look decisive. Without it a straggling grant can re-trigger a
 * transition the node already made.
 */
typedef struct {
    uint64_t term;      /* the term being sought (currentTerm + 1)          */
    uint32_t quorum;    /* votes needed, self included                      */
    uint32_t granted;   /* votes counted so far                             */
    int      pre_vote;  /* a straw poll: nothing persisted, nothing changed */
    int      settled;   /* this round has produced its answer               */
} raft_round;

/* What the caller must do about one vote reply. */
typedef enum {
    RAFT_ROUND_IGNORE    = 0, /* noise: settled, or from a world that ended */
    RAFT_ROUND_PENDING   = 1, /* counted; still short of a quorum           */
    RAFT_ROUND_WON       = 2, /* quorum reached -- promote or campaign      */
    RAFT_ROUND_STEP_DOWN = 3  /* the reply carried a higher term            */
} raft_round_action;

/* Begin a round seeking `term`. */
void raft_round_begin(raft_round *r, uint64_t term, uint32_t quorum, int pre_vote);

/*
 * Count one reply.
 *
 * `current_term` and the two role flags describe the node AS IT IS NOW,
 * not as it was when the request went out -- that gap is the whole
 * reason this function exists. A pre-vote round is valid only while the
 * node has not advanced past the term it was polling from
 * (current_term == term - 1); a real round only while the node is still
 * a candidate in the term it sought.
 *
 * On RAFT_ROUND_STEP_DOWN, *step_down_term receives the term to adopt.
 * The round is latched in every case except RAFT_ROUND_PENDING.
 */
int raft_round_on_reply(raft_round *r, uint64_t reply_term, int vote_granted,
                        uint64_t current_term, int is_leader, int is_candidate,
                        uint64_t *step_down_term);

/* ---- replication -------------------------------------------------------- */

/* What a leader owes one peer. */
typedef enum {
    RAFT_REPL_APPEND   = 0, /* AppendEntries from next_index               */
    RAFT_REPL_SNAPSHOT = 1, /* next_index is compacted away; stream one    */
    RAFT_REPL_PARK     = 2  /* compacted away and no snapshot to serve     */
} raft_repl_action;

/*
 * Which of the three, for a peer whose next index is `next`.
 *
 * The boundary is `next <= base_index`: base_index is the last index the
 * log no longer holds, so a peer needing it cannot be caught up by
 * AppendEntries no matter how far the leader rewinds. Getting this
 * comparison off by one is a leader that backs off forever against a
 * follower it can never satisfy -- the peer stays one entry behind and
 * every heartbeat re-derives the same impossible request.
 */
int raft_repl_decide(uint64_t next, uint64_t base_index, int has_snapshot);

/*
 * Where a peer stands after an install completes. `boundary` is the
 * snapshot's lastIncludedIndex. Match never regresses -- a peer that
 * already had more than the snapshot covers keeps it.
 */
void raft_repl_installed(uint64_t boundary, uint64_t *match, uint64_t *next);

/* ---- snapshot streaming ------------------------------------------------- */

/*
 * One chunk of a snapshot transfer.
 *
 * The stream is the concatenation of every file in manifest order, cut
 * at `chunk_bytes`. Cuts never span files: a chunk names exactly one
 * file, so the receiver can write it without knowing the layout.
 */
typedef struct {
    uint32_t file_index;  /* which file in the manifest                     */
    uint64_t offset;      /* byte offset within that file                   */
    uint32_t len;         /* bytes in this chunk (0 only for an empty file) */
    int      is_first;    /* carries the manifest                           */
    int      is_done;     /* last chunk of the last file                    */
    uint32_t next_file;   /* cursor to pass back for the following chunk    */
    uint64_t next_offset;
} raft_chunk;

/*
 * The chunk that follows the cursor (`cursor_file`, `cursor_offset`), or
 * 0 when the stream is exhausted; 1 on success.
 *
 * Start at {0, 0} and advance with `out->next_file` / `out->next_offset`.
 * The cursor is returned rather than derived because deriving it is
 * ambiguous: a zero-length file yields one empty chunk, after which
 * offset + len is still 0, indistinguishable from not having sent it.
 * That reads as a pedantic edge case and is not -- it is an infinite
 * loop re-sending the same empty chunk, on any snapshot whose last file
 * happens to be empty.
 *
 * A zero-length file gets its chunk rather than being skipped, so the
 * receiver creates it: an absent file and an empty one are different
 * things to the manifest check on the other side.
 *
 * A snapshot with no files at all yields a single empty chunk with
 * is_first and is_done both set. The manifest still has to travel, and
 * the boundary it declares is the whole point of the transfer.
 *
 * This replaces a doubly-nested loop with a labelled break, a `first`
 * flag mutated across both levels, and a `done` computed from two
 * indices at once -- correct in JavaScript, and three separate things to
 * get wrong again in the next host.
 */
int raft_chunk_next(const uint64_t *file_sizes, uint32_t nfiles,
                    uint32_t chunk_bytes,
                    uint32_t cursor_file, uint64_t cursor_offset,
                    raft_chunk *out);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_DRIVE_H */
