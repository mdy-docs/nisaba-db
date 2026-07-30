/*
 * raft_core.h — the Raft rules that decide safety, as pure functions.
 *
 * src/raft.js is a deterministic state machine already: it never reads a
 * clock or a random source (both injected), never owns a socket, and its
 * two most important RPC handlers are synchronous because every EntryLog
 * operation is. What it is NOT is reachable from a host without a
 * JavaScript runtime -- and it is the last thing standing between this
 * codebase and a Node-free server binary.
 *
 * This file is the first half of moving it: every rule whose violation is
 * a CONSENSUS bug, extracted as a function of its arguments and nothing
 * else. Not the orchestration -- who to call, when to retry, how to
 * settle a promise -- which is still the host's and moves next. Just the
 * decisions:
 *
 *   raft_decide_vote            §5.2, §5.4.1: who may become leader
 *   raft_decide_append          §5.3: the consistency check and its hint
 *   raft_append_entries_to_log  §5.3: the conflict rule
 *   raft_follower_commit        what a follower may consider committed
 *   raft_commit_candidate       §5.4.2: what a leader may commit, and
 *                               the current-term rule that makes it safe
 *   raft_backoff                how far a leader rewinds on a rejection
 *   raft_members_adopt/_merge   who is in the cluster and who may vote
 *
 * WHY THESE, AND WHY AS PURE FUNCTIONS
 *
 * Each is a rule with a paper citation and a failure mode measured in
 * lost writes. Each is also, in JavaScript, a handful of lines tangled
 * with the bookkeeping around it, which is exactly the shape that makes
 * a subtle divergence between two implementations invisible. Pulling
 * them out means the native core and the browser one cannot disagree
 * about when a vote is granted, and it means the rules can be tested
 * directly rather than inferred from a simulated cluster's behaviour.
 *
 * The decision functions do not mutate anything. They report what the
 * caller must do, in order -- persist this, step down to that, reply with
 * this term -- because the ORDER is itself a safety rule: a vote must
 * reach the disk before the reply leaves, or a machine that loses power
 * can vote twice in one term.
 *
 * Indices and terms are uint64_t here. The JS bridge converts through
 * double, which caps them at 2^53 -- the same ceiling entrylog.h's own
 * glue has always had, and the reason the interim rule in the porting
 * plan says no NEW export may use double for an index. The ceiling lives
 * in the glue, not in the logic.
 */
#ifndef RAFT_CORE_H
#define RAFT_CORE_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "entrylog.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A member record is malformed: a missing or non-positive id. Ids are
 * positive because 0 is EntryLog's "voted for nobody". */
#define RAFT_ERR_MEMBER (-50)

/* ---- membership -------------------------------------------------------- */

/*
 * Adopt a member set: normalize `members` (a binjson ARRAY of records
 * {id, host?, port?, voting?, ...} or bare INT ids) into the three lists
 * every other rule reads, appended to `out` as
 *
 *   { members: [records, sorted by id], voters: [ids], peers: [ids] }
 *
 * `voters` excludes any record with `voting: false` -- learners receive
 * everything a voter does but are excluded from quorum arithmetic, never
 * campaign, and refuse votes, so that adding capacity can never thin the
 * failure margin. `peers` is every member except `self_id`.
 *
 * Sorting is not cosmetic: two nodes that adopt the same set must derive
 * the same lists, and the quorum count is taken from a position in one.
 *
 * Bare ids become {id} records. `self_id` is NOT added if absent -- a
 * node applying its own removal must see itself gone.
 */
int raft_members_adopt(const uint8_t *members, uint32_t len, uint64_t self_id, dbuf *out);

/*
 * Merge a proposed member set with what is already known: a record in
 * `input` that names no host inherits the known record for that id.
 *
 * This is what stops `changeMembership([1, 2, 3, 4])` from silently
 * erasing the addresses the log already carries -- the log being the
 * single source of truth for the cluster's shape is the property that
 * removes the separate address book, and it holds only if an id-only
 * proposal cannot destroy it.
 *
 * Appends a binjson ARRAY of records to `out`. RAFT_ERR_MEMBER if any
 * input record has no positive integer id, or if the set is empty.
 */
int raft_members_merge(const uint8_t *input, uint32_t input_len,
                       const uint8_t *known, uint32_t known_len, dbuf *out);

/* Majority of VOTERS. Learners add capacity, not quorum weight. */
uint32_t raft_quorum(uint32_t voter_count);

/* ---- §5.2, §5.4.1: granting a vote ------------------------------------- */

typedef struct {
    /* From the RequestVote message. */
    uint64_t msg_term;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
    int      pre_vote;          /* a round that persists nothing */

    /* Ours. */
    uint64_t current_term;
    uint64_t voted_for;         /* 0 = nobody */
    uint64_t our_last_index;
    uint64_t our_last_term;
    int      self_is_voter;
    int      is_leader;
    uint64_t leader_id;         /* 0 = none known */

    /* Leader stickiness, for the pre-vote round only. All three in the
     * same units; the caller's clock is the caller's business. */
    int64_t  now;
    int64_t  last_leader_contact;
    int64_t  min_election_timeout;
} raft_vote_in;

typedef struct {
    int      grant;
    uint64_t reply_term;        /* the term AFTER any step-down below */

    /* Do these first, in this order, then reply. */
    int      step_down;         /* become a follower of nobody at... */
    uint64_t step_down_term;
    int      persist;           /* ...then commit this hard state, which
                                 * MUST reach the disk before the reply
                                 * leaves: a machine that loses power
                                 * after answering but before persisting
                                 * can vote twice in one term. */
    uint64_t persist_term;
    uint64_t persist_voted_for;
    int      reset_election_timer;
} raft_vote_out;

void raft_decide_vote(const raft_vote_in *in, raft_vote_out *out);

/* ---- §5.3: accepting entries ------------------------------------------- */

typedef struct {
    /* From the AppendEntries message. */
    uint64_t msg_term;
    uint64_t leader_id;
    uint64_t prev_log_index;
    uint64_t prev_log_term;
    uint32_t entry_count;

    /* Ours. */
    uint64_t current_term;
    int      is_follower;
    uint64_t our_base_index;
    uint64_t our_last_index;
    /* elog_term_at(prev_log_index), meaningful only when prev_log_index
     * is within [base, last]; the caller need not look it up otherwise. */
    uint64_t our_prev_term;
} raft_append_in;

typedef struct {
    /* Nothing at all happens: a stale leader gets our term and no side
     * effects. Distinct from a rejection, which still refreshes our
     * election timer -- the leader is current, just misaligned. */
    int      stale;

    int      step_down;
    uint64_t step_down_term;
    uint64_t step_down_leader;

    int      success;
    uint64_t reply_term;
    int      has_hint;
    uint64_t hint_index;        /* where the leader should resume from */
    uint64_t match_index;       /* on success: prev + entry_count */
} raft_append_out;

void raft_decide_append(const raft_append_in *in, raft_append_out *out);

/*
 * The conflict rule (§5.3), against a real log: for each entry in
 * `entries` (a binjson ARRAY of {index, term, type, payload}), skip it if
 * we already hold that index at that term; on the first disagreement,
 * truncate our suffix from there and append the rest.
 *
 * Raft guarantees no conflict at or below the commit index, and
 * EntryLog's own truncate guard enforces exactly that invariant -- so a
 * truncation this refuses is a violated invariant somewhere upstream, not
 * a condition to paper over.
 *
 * Syncs iff anything was appended: durable BEFORE the follower acks,
 * which is what makes the ack mean what the leader thinks it means.
 * `*truncated_from` receives the index truncation started at, or 0.
 */
int raft_append_entries_to_log(elog *log, const uint8_t *entries, uint32_t len,
                               uint64_t *truncated_from);

/*
 * What a follower may mark committed after appending: the leader's commit
 * index, but never past what we actually hold. Returns 1 and writes
 * *out if it advances, 0 if not.
 */
int raft_follower_commit(uint64_t leader_commit, uint64_t our_commit,
                         uint64_t our_last_index, uint64_t *out);

/* ---- §5.4.2: what a leader may commit ---------------------------------- */

/*
 * The highest index a quorum of voters holds, given the leader's own
 * last index and each VOTING peer's match index -- then the rule that
 * makes counting safe: only an entry of the CURRENT term commits by
 * counting replicas. Earlier terms commit implicitly once a current-term
 * entry above them does.
 *
 * Omitting that check is the classic Raft bug (figure 8 in the paper): a
 * new leader counts an old entry to a majority, commits it, and then
 * loses it to a node that never had it.
 *
 * `term_at_candidate` is elog_term_at of the candidate index -- the
 * caller looks it up because it holds the log. Pass 0 for a candidate at
 * or below the base index; the check rejects it either way.
 *
 * Returns 1 and writes *out if the commit index may advance, else 0.
 * `matches` may be empty (a single-node cluster commits on its own).
 */
uint32_t raft_commit_candidate(uint64_t leader_last, const uint64_t *matches, uint32_t n,
                               uint32_t quorum, uint64_t *out);

int raft_may_commit(uint64_t candidate, uint64_t commit_index, uint64_t base_index,
                    uint64_t term_at_candidate, uint64_t current_term);

/* ---- leader backoff ----------------------------------------------------- */

typedef struct {
    uint64_t next;
    uint64_t match;
    int      match_regressed;   /* the peer lost data; see below */
} raft_backoff_out;

/*
 * Where to resume after a follower rejects. Back up along its hint, never
 * forward of a plain decrement.
 *
 * The hint may fall at or below what we recorded as its match index. A
 * follower can only regress like that by LOSING ITS DISK -- a blank
 * replacement machine reusing the same id. Believe it, and drop
 * matchIndex with it: a replica that no longer holds the entries must
 * stop counting toward quorums (or the leader will happily commit on a
 * majority that includes a node with nothing), and nextIndex must be free
 * to fall back to the snapshot-install path.
 */
void raft_backoff(uint64_t hint, int have_hint, uint64_t next, uint64_t match,
                  raft_backoff_out *out);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_CORE_H */
