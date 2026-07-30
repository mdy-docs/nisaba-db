/*
 * raft_msg.h — the Raft wire grammar, and the two RPC handlers that run
 * entirely in C.
 *
 * Phase 7a moved the RULES (raft_core.h) but left src/raft.js holding the
 * handlers around them: decode the message, gather state, ask C, carry
 * out the answer, build a reply. That left two things wrong.
 *
 * The message grammar was still JavaScript's. Every transport decodes an
 * envelope and hands the node an object, so what a Raft message IS lived
 * in the reader of each field rather than anywhere nameable -- and a
 * native host would have needed its own second opinion.
 *
 * And the entries went across the bridge TWICE. They arrive binjson-
 * encoded on the wire, get decoded so JavaScript can pass them along,
 * then get re-encoded to reach the conflict rule. That is the whole
 * AppendEntries payload, on the replication hot path, for nothing.
 *
 * Both go away by giving C the raw bytes. rmsg_handle_request_vote and
 * rmsg_handle_append_entries take the message exactly as it came off the
 * wire and return the reply exactly as it should go back. They own the
 * log too, so the persistence Raft demands -- the vote before the reply,
 * the entries before the ack -- happens inside one synchronous call
 * where nothing can interleave. That is not a convenience: it is why
 * these two handlers can be complete in C while elections and
 * replication cannot yet, since those await a network.
 *
 * What the caller still owns is the node's VOLATILE state -- role,
 * leaderId, timers, commit index -- which it passes in and gets deltas
 * back for. Not because C could not hold it, but because the rest of the
 * node still does; when the node struct itself moves, these signatures
 * lose their state arguments and nothing else about them changes.
 */
#ifndef RAFT_MSG_H
#define RAFT_MSG_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "entrylog.h"
#include "raft_core.h"

#ifdef __cplusplus
extern "C" {
#endif

/* The message is not a Raft message this build understands: an unknown
 * `kind`, or a known one missing a field it cannot proceed without.
 * Refused rather than half-read -- a peer speaking a grammar we do not
 * know is a peer we cannot safely answer. */
#define RAFT_ERR_MESSAGE (-51)

/* Message kinds, as they appear in `kind`. The spellings live in
 * raft_msg.c's KIND_NAME table and nowhere else. */
typedef enum {
    RAFT_MSG_REQUEST_VOTE     = 0,
    RAFT_MSG_APPEND_ENTRIES   = 1,
    RAFT_MSG_INSTALL_SNAPSHOT = 2,
    RAFT_MSG_JOIN             = 3,
    RAFT_MSG_LEAVE            = 4
} raft_msg_kind;

/* Which kind is this? RAFT_ERR_MESSAGE if it is none of them. Lets a
 * host route a message it will not itself interpret. */
int rmsg_kind(const uint8_t *msg, uint32_t len, int *kind_out);

/*
 * The node state these handlers read and change. In on the way down,
 * updated on the way back -- the caller writes the changed fields onto
 * its node and fires whatever observability it wants from `changed`.
 */
typedef struct {
    uint64_t self_id;
    int      is_follower;
    int      is_leader;
    int      self_is_voter;
    uint64_t leader_id;
    uint64_t commit_index;
    int64_t  now;
    int64_t  last_leader_contact;
    int64_t  min_election_timeout;
} raft_msg_state;

typedef struct {
    /* Volatile state the caller must adopt. */
    int      became_follower;      /* role -> follower, leader_id below   */
    uint64_t new_leader_id;
    uint64_t new_commit_index;     /* 0 = unchanged                       */
    int64_t  new_last_leader_contact;
    int      touched_leader;       /* new_last_leader_contact is valid    */
    int      reset_election_timer;
    int      quiesce;              /* the leader is parking the group     */

    /* For the caller's event stream; no action required. */
    int      granted_vote;
    uint64_t truncated_from;       /* 0 = no conflict truncation          */
    uint64_t match_index;
    int      success;
} raft_msg_effect;

/*
 * Handle one RequestVote. Appends the encoded reply to `reply`.
 *
 * Persists the vote through `log` BEFORE returning, when the rules say
 * to -- so a caller that sends the reply the instant this returns is
 * correct by construction, which is the only ordering that survives a
 * power cut mid-election.
 */
int rmsg_handle_request_vote(elog *log, const raft_msg_state *st,
                             const uint8_t *msg, uint32_t len,
                             raft_msg_effect *eff, dbuf *reply);

/*
 * Handle one AppendEntries. Appends the encoded reply to `reply`.
 *
 * Runs the consistency check, the conflict rule, the append and the sync
 * against `log` -- the entries never leave the buffer they arrived in.
 * Durable before the reply, for the same reason.
 */
int rmsg_handle_append_entries(elog *log, const raft_msg_state *st,
                               const uint8_t *msg, uint32_t len,
                               raft_msg_effect *eff, dbuf *reply);

/* ---- building the requests a leader sends ------------------------------ */

/*
 * Encode a RequestVote. `pre_vote` marks the round that persists nothing
 * and bumps no term.
 */
int rmsg_build_request_vote(uint64_t term, uint64_t candidate_id,
                            uint64_t last_log_index, uint64_t last_log_term,
                            int pre_vote, dbuf *out);

/*
 * Encode an AppendEntries, taking the entries straight from `log` --
 * from `next_index`, up to `max_bytes` worth, or none at all for a
 * heartbeat. The batch never round-trips through the host.
 *
 * `*out_count` receives how many entries went in, which is what the
 * leader adds to prevLogIndex to know the match index a success implies.
 */
int rmsg_build_append_entries(elog *log, uint64_t term, uint64_t leader_id,
                              uint64_t next_index, uint64_t prev_log_term,
                              uint64_t leader_commit, size_t max_bytes,
                              int quiesce, uint32_t *out_count, dbuf *out);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_MSG_H */
