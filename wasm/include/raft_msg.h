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
    RAFT_MSG_LEAVE            = 4,
    /* Graceful leadership transfer (section 3.10). Carried here so the
     * grammar stays the whole grammar: a kind C cannot name is one the
     * host has to route by decoding the message itself, which is the
     * split this header exists to remove. C classifies it; the election
     * it triggers is still the host's, until phase 7c. */
    RAFT_MSG_TIMEOUT_NOW      = 5
} raft_msg_kind;

/* Which kind is this? RAFT_ERR_MESSAGE if it is none of them. Lets a
 * host route a message it will not itself interpret. */
int rmsg_kind(const uint8_t *msg, uint32_t len, int *kind_out);

/*
 * Who sent it: candidateId on a RequestVote, leaderId on the rest.
 *
 * Every message in this grammar names its sender, so nobody has to be
 * TOLD who a message came from -- and a fact nobody has to state is a
 * fact nobody can state wrongly. RAFT_ERR_MESSAGE for a kind with no
 * sender (join and leave come from outside the cluster) or one that
 * names id 0, which is "nobody" everywhere here.
 */
int rmsg_sender(const uint8_t *msg, uint32_t len, uint64_t *out);

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

/* ---- TimeoutNow, join and leave ----------------------------------------- */

/*
 * The membership grammar, so that the three kinds a node answers without
 * touching its log are written down in the same file as the two it
 * answers with one.
 *
 * rmsg_join_member returns the member RECORD a join carries, as a span
 * into `msg`; rmsg_leave_id the id a leave names. RAFT_ERR_MESSAGE if it
 * is missing, or names id 0 -- which is "nobody" everywhere here.
 */
int rmsg_join_member(const uint8_t *msg, uint32_t len,
                     const uint8_t **record, uint32_t *record_len, uint64_t *id);
int rmsg_leave_id(const uint8_t *msg, uint32_t len, uint64_t *id);

/* The `term` a message carries (0 if it carries none). */
int rmsg_term(const uint8_t *msg, uint32_t len, uint64_t *term);

/* One field of a member record, as the encoded span it occupies, so two
 * records can be compared on it without either being decoded. NULL and
 * length 0 when the record does not carry that field. */
int rmsg_record_field(const uint8_t *record, uint32_t len, const char *key,
                      const uint8_t **v, uint32_t *vlen);

/*
 * `record` again, with `voting` forced to `voting_flag` and whatever
 * `voting` it carried dropped. Appended to `b`, which is mid-array.
 *
 * Explicitly SET rather than merely omitted: raft_members_merge fills an
 * absent key from the known record, so a promotion that just dropped the
 * flag would re-inherit the voting:false it is trying to lift.
 */
int rmsg_record_with_voting(const uint8_t *record, uint32_t len, int voting_flag,
                            bj_builder *b);

/*
 * A join, and a leave -- the two messages a node ANSWERS and, until a
 * native member could grow a cluster, nothing in C ever sent. `record`
 * is the applicant's `{ id, host, port }`, spliced in as it stands.
 *
 * Neither names a sender, and that is the grammar rather than an
 * omission: both come from outside the cluster, where there is no id to
 * name yet (rmsg_sender refuses them for exactly this reason).
 */
int rmsg_build_join(const uint8_t *record, uint32_t record_len, dbuf *out);
int rmsg_build_leave(uint64_t id, dbuf *out);

/*
 * ...and that answer, read back. Every span points into `msg` and dies
 * with it.
 *
 * The four shapes are mutually exclusive and each means something
 * different to a seed loop: `ok` is done, `error` will never heal, a
 * redirect says where to ask instead, and `retry` says ask again. A
 * reader that collapsed any two of them would either give up on a
 * cluster mid-election or retry a malformed request forever.
 */
typedef struct {
    int            ok;
    const uint8_t *members;     uint32_t members_len;      /* ok        */
    const uint8_t *error;       uint32_t error_len;        /* refused   */
    int            retry;                                  /* busy      */
    uint64_t       leader_id;                              /* redirect  */
    const uint8_t *leader_host; uint32_t leader_host_len;
    int            leader_port;
} rmsg_membership;

int rmsg_read_membership_reply(const uint8_t *msg, uint32_t len, rmsg_membership *out);

/* `{ term, ok }` -- TimeoutNow's answer. */
int rmsg_build_ack(uint64_t term, int ok, dbuf *out);

/*
 * A join/leave answer, in one of its four shapes: adopted
 * (`{ok:true, members}`), refused (`{ok:false, error}`), busy
 * (`{ok:false, retry:true}`), or a redirect (`{ok:false, leaderId,
 * leaderAddress}`). Exactly one of `members` / `error` / `retry` /
 * `leader_id` applies, in that order of precedence.
 *
 * The redirect carries the leader's ADDRESS, lifted out of its member
 * record, because a joiner knows addresses and not ids: an id alone
 * would send it back to the seed it just asked.
 */
int rmsg_build_membership_reply(int ok, const uint8_t *members, uint32_t members_len,
                                const char *error, int retry,
                                uint64_t leader_id, const uint8_t *leader_record,
                                uint32_t leader_record_len, dbuf *out);

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

/* ---- InstallSnapshot ----------------------------------------------------
 *
 * The fifth kind, and the one whose HANDLER is not here: it writes files,
 * so it belongs to whoever owns a namespace (raft_node.h). What is here
 * is what every host needed anyway and each wrote for itself -- the
 * envelope, in one place, so a leader in C and a follower in JavaScript
 * cannot disagree about it.
 *
 * A transfer is the concatenation of a snapshot's files, cut into
 * chunks (raft_drive.h's raft_chunk_next decides where). Each chunk
 * names exactly one file, so a receiver writes it without knowing the
 * layout. The FIRST carries the manifest -- what the files are, and the
 * cluster shape at the boundary -- and the LAST is flagged `done`.
 */

/*
 * What an installSnapshot says, as spans into `msg`. Valid for as long
 * as `msg` is.
 *
 *   role == NULL      this chunk names no file: the empty stream, which
 *                     still has to carry a manifest and a boundary.
 *   manifest == NULL  not the first chunk of this transfer.
 *   data_len == 0     legal, and not the same as no chunk: a zero-length
 *                     FILE gets a chunk of its own, because "absent" and
 *                     "empty" are different things to the manifest check
 *                     on the other side.
 */
typedef struct {
    uint64_t term, leader_id;
    uint64_t last_included_index, last_included_term;
    const char    *role;     uint32_t role_len;
    uint64_t       offset;
    const uint8_t *data;     uint32_t data_len;
    int            done;
    const uint8_t *manifest; uint32_t manifest_len;
} raft_install;

int rmsg_install_read(const uint8_t *msg, uint32_t len, raft_install *out);

/*
 * Encode one chunk. `manifest` is a whole binjson OBJECT, spliced in as
 * it stands, and NULL for every chunk after the first -- what goes IN it
 * is not this file's business (raft_node.h builds it, because it is the
 * layer that owns a snapshot store).
 *
 * `role` NULL means the chunk names no file; `data` may be NULL with
 * data_len 0.
 */
int rmsg_build_install_snapshot(uint64_t term, uint64_t leader_id,
                                uint64_t last_included_index,
                                uint64_t last_included_term,
                                const char *role, uint32_t role_len,
                                uint64_t offset,
                                const uint8_t *data, uint32_t data_len,
                                int done,
                                const uint8_t *manifest, uint32_t manifest_len,
                                dbuf *out);

/*
 * `{ term, success }`, plus `restart: true` when the receiver has no
 * install to attach this chunk to and needs the manifest again -- a
 * leader that restarted, or a staging attempt this node abandoned.
 *
 * `restart` is only meaningful with success false, and is omitted
 * otherwise rather than sent as false: the sender's rule is "retry from
 * the top", and a field that says so only sometimes is one a reader has
 * to know the precedence of.
 */
int rmsg_build_install_reply(uint64_t term, int ok, int restart, dbuf *out);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_MSG_H */
