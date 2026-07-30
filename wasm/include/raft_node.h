/*
 * raft_node.h — the replication state machine, with an outbox instead of
 * a call stack.
 *
 * THE PROBLEM THIS SOLVES
 *
 * src/raft.js drives replication with `await transport.call(peer, msg)`.
 * That single line is why the Raft node cannot leave JavaScript: it
 * suspends C-equivalent logic on a promise, and there is no promise
 * under WASI or native. Asyncify and JSPI were both rejected for the
 * same reason -- they exist only in a browser, so adopting either would
 * give the server a different control flow from the browser, which is
 * the exact problem this whole effort exists to remove.
 *
 * The resolution is the one bjns.h already uses for files, applied to
 * the network: C DECIDES, the host DELIVERS, C CONSUMES the answer.
 * Nothing here blocks, and nothing here knows what a socket is.
 *
 *     rn_tick(n, now)              -- timers fire; messages queue
 *     rn_handle(n, bytes)          -- an incoming message; reply queues
 *     for i in 0 .. rn_out_count:  -- the host sends each one, however
 *         host sends rn_out_*(i)      it likes: a promise, a blocking
 *     rn_out_clear(n)                 write, a postMessage
 *     rn_on_reply(n, corr, bytes)  -- ...and feeds each answer back
 *     rn_on_fail(n, corr)          -- ...or says it never came
 *
 * Every entry point that can arm an election timer takes `random01`
 * rather than drawing one: a node that reads its own random source is a
 * node the simulator cannot replay, and test/raft-harness.js's
 * determinism is the single biggest asset this port has.
 *
 * A correlation id, not a closure, is what ties a reply to the request
 * that caused it. That is the whole difference: a closure needs a stack
 * to live on, an integer does not.
 *
 * WHAT IS AND IS NOT HERE
 *
 * Here: role, term transitions, the election timer and its round, the
 * heartbeat timer, and the per-peer replication cursors (next, match,
 * when it last answered, whether a request is outstanding). All of it
 * volatile -- the durable half is the elog this wraps, which already
 * owns currentTerm, votedFor and the entries.
 *
 * Not here, and deliberately: promises, waiters, the apply pump, the
 * snapshot transfer's file reads, and the membership-change orchestration.
 * Those need host resources -- a state machine to apply into, files to
 * read, a promise to settle -- so they stay with the host and call in.
 * raft_drive.h already gives them their decisions.
 *
 * The host calling back IN is the "what the host still owns" section at
 * the bottom: rn_propose for an entry it wants replicated, rn_campaign
 * when a transferring leader tells this node to stand, rn_observe_leader
 * for the one message class (InstallSnapshot) whose handler lives up
 * there but whose term still deposes us. Each is the exact head or tail
 * of a path C already owns -- none of them is a rule this file does not
 * otherwise enforce.
 *
 * This does not replace raft_core.h or raft_msg.h; it drives them. Every
 * safety rule is still theirs, and this file contains no `if` that
 * decides whether a vote may be granted.
 */
#ifndef RAFT_NODE_H
#define RAFT_NODE_H

#include <stdint.h>
#include <stddef.h>

#include "binjson.h"
#include "dbuf.h"
#include "entrylog.h"

#ifdef __cplusplus
extern "C" {
#endif

/* A peer that is not a member, or a correlation id nobody issued. */
#define RAFT_ERR_PEER (-52)

/* The member set is larger than this build can hold (rn_max_peers).
 * Refused whole -- see rn_set_members. */
#define RAFT_ERR_CAPACITY (-53)

typedef struct raft_node raft_node;

/* Roles, matching src/raft.js's ROLE. */
typedef enum {
    RAFT_FOLLOWER  = 0,
    RAFT_CANDIDATE = 1,
    RAFT_LEADER    = 2
} raft_role;

/*
 * Create a node with `self_id`, over an already-open log. The log is
 * BORROWED: it outlives nothing here and this never frees it.
 *
 * The node starts as a follower with no election deadline -- call
 * rn_start to arm the timers, so that constructing a node is not the
 * same act as joining an election.
 */
raft_node *rn_new(uint64_t self_id, elog *log);
void       rn_free(raft_node *n);

/*
 * Point the node at a different log. EntryLog cannot rebase in place, so
 * both compaction paths -- a follower adopting an install, a leader
 * snapshotting locally -- close the old log and open a fresh one based at
 * the boundary. The node holds a BORROWED pointer, so it has to be told,
 * and the host must have quiesced everything that could touch the old one
 * before it closed it.
 */
void rn_set_log(raft_node *n, elog *log);

/*
 * Adopt a member set: a binjson ARRAY of records ({ id, voting? }), the
 * same shape raft_core.h's members_adopt takes. This is the ONE funnel
 * -- bootstrap, a CONFIG entry applying, a snapshot install -- which is
 * what keeps the voter list and the peer cursors from drifting apart.
 *
 * A peer that disappears loses its cursors; one that arrives starts at
 * next = lastIndex + 1, which is what a fresh leader would give it.
 *
 * ALL OR NOTHING. Every input is validated before anything is adopted,
 * so a refusal leaves the previous set intact and the caller holding an
 * error it has to do something about. RAFT_ERR_CAPACITY if the set
 * exceeds rn_max_peers(); RAFT_ERR_MEMBER if it is malformed, or if its
 * voter list names someone who is not a member.
 *
 * A partial adoption would be worse than either: a host derives its own
 * member list from the same raft_members_adopt, and the two agreeing is
 * the property that removes the second source of truth. They cannot
 * agree about a set that only one of them trimmed.
 */
int rn_set_members(raft_node *n, const uint8_t *members, uint32_t len);

/* The largest peer count (members excluding self) this build can hold.
 * A host that proposes membership should refuse a larger set at the
 * caller, where a human is standing, rather than at apply, where every
 * replica can only halt. */
uint32_t rn_max_peers(void);

/*
 * Timing. `min_election` and `max_election` bound the randomised
 * election timeout; `heartbeat` is the leader's idle interval. The
 * random draw is passed IN to every call that can arm a timer, for the
 * replayability reason at the top of this file.
 */
void rn_set_timing(raft_node *n, int64_t min_election, int64_t max_election,
                   int64_t heartbeat);
void rn_set_limits(raft_node *n, uint32_t max_batch_bytes);

/* Arm the timers at `now`, with `random01` in [0,1) for the first
 * election deadline. */
void rn_start(raft_node *n, int64_t now, double random01);
void rn_stop(raft_node *n);

/* ---- the clock ---------------------------------------------------------- */

/*
 * Advance to `now`. `random01` seeds any election timeout this tick
 * arms. Queues whatever the timers make due: a pre-vote round when the
 * election deadline passes, heartbeats when the leader's interval does.
 *
 * Idempotent in the sense that matters: calling it twice with the same
 * `now` queues nothing the second time.
 */
int rn_tick(raft_node *n, int64_t now, double random01);

/* Park the timers (a quiesced group) and unpark them. */
void rn_quiesce(raft_node *n);
void rn_wake(raft_node *n, int64_t now, double random01);

/* ---- messages ----------------------------------------------------------- */

/*
 * An incoming message. The reply is queued as an outbox entry addressed
 * to `from` with the correlation id the sender used, so a host that
 * cannot distinguish a request from a reply on the wire does not have
 * to: it just sends what the outbox holds.
 *
 * Only the two hot kinds are answered here (RequestVote, AppendEntries);
 * anything else is RAFT_ERR_MESSAGE, because answering it needs host
 * resources -- see the note at the top.
 */
int rn_handle(raft_node *n, uint64_t from, uint32_t corr,
              const uint8_t *msg, uint32_t len, double random01);

/*
 * A reply to something this node sent. `corr` is the id that went out
 * with it; a stale or unknown id is RAFT_ERR_PEER, which is not an error
 * the host needs to act on -- it means the round it belonged to is over.
 */
int rn_on_reply(raft_node *n, uint32_t corr, const uint8_t *reply, uint32_t len,
                double random01);

/* The request with this correlation id will never be answered. */
int rn_on_fail(raft_node *n, uint32_t corr);

/* ---- the outbox --------------------------------------------------------- */

/*
 * Messages waiting to go out. Valid until the next call that mutates the
 * node -- drain immediately, which every host does anyway because there
 * is nothing else to do with them.
 *
 * A peer of 0 never appears: every entry is addressed. Replies carry the
 * correlation id they are answering; requests carry a fresh one.
 */
uint32_t       rn_out_count(const raft_node *n);
uint64_t       rn_out_peer(const raft_node *n, uint32_t i);
uint32_t       rn_out_corr(const raft_node *n, uint32_t i);
int            rn_out_is_reply(const raft_node *n, uint32_t i);
const uint8_t *rn_out_bytes(const raft_node *n, uint32_t i, uint32_t *len);
void           rn_out_clear(raft_node *n);

/* ---- what the host still owns ------------------------------------------- */

/*
 * Effects the host has to act on, drained like the outbox. C cannot
 * apply an entry, read a snapshot file or settle a promise, so it says
 * what happened and the host does it.
 */
typedef enum {
    RN_EFFECT_ROLE          = 0, /* role changed; arg = new role         */
    RN_EFFECT_COMMIT        = 1, /* commitIndex advanced; arg = index    */
    RN_EFFECT_NEEDS_SNAPSHOT= 2, /* peer is below our base; arg = peer   */
    RN_EFFECT_PROMOTE       = 3, /* learner is caught up; arg = peer     */
    RN_EFFECT_REACHABLE     = 4, /* peer reachability changed; arg = peer*/
    RN_EFFECT_TRUNCATED     = 5, /* our log was cut back; arg = from     */
    RN_EFFECT_ELECTION      = 6  /* standing for `arg`; flag = pre-vote  */
} rn_effect_kind;

uint32_t rn_effect_count(const raft_node *n);
int      rn_effect_kind_at(const raft_node *n, uint32_t i);
uint64_t rn_effect_arg(const raft_node *n, uint32_t i);
int      rn_effect_flag(const raft_node *n, uint32_t i);
void     rn_effects_clear(raft_node *n);

/*
 * Did this node ever have an effect to report and no room to hold it?
 *
 * It cannot happen to a host that drains after each call: the queue is
 * sized so that every peer's actionable effects fit at once, and those
 * kinds coalesce per peer rather than accumulating (see raft_node.c).
 * A host that batches many calls between drains can still overrun the
 * narrative kinds, and this is how it finds out.
 *
 * Sticky, and never cleared by rn_effects_clear. There is no way to
 * recover what was not said, so a host that reads 1 here should stop
 * rather than keep acting on a picture it knows is incomplete.
 */
int      rn_effects_lost(const raft_node *n);

/* ---- accessors ---------------------------------------------------------- */

int      rn_role(const raft_node *n);
uint64_t rn_leader_id(const raft_node *n);
uint64_t rn_commit_index(const raft_node *n);
uint64_t rn_match(const raft_node *n, uint64_t peer);
uint64_t rn_next(const raft_node *n, uint64_t peer);
/* The correlation id outstanding at `peer`, or 0 -- the leader's own
 * "is this one busy" bit, exposed for the host's status snapshot. */
uint32_t rn_inflight(const raft_node *n, uint64_t peer);
uint32_t rn_quorum(const raft_node *n);
int      rn_is_quiesced(const raft_node *n);
/* Has a quorum of voters answered within `within_ms` (check-quorum)? A
 * peer that has never answered this term counts for nothing, however
 * recently the term began. */
int      rn_has_quorum_contact(const raft_node *n, int64_t within_ms);

/*
 * Force a replication pass at `peer` -- what the host calls after
 * appending an entry, or after an install completes. Queues at most one
 * message; a peer with a request already outstanding is skipped, which
 * is what keeps a busy leader from flooding a slow follower.
 */
int rn_replicate(raft_node *n, uint64_t peer);

/* Tell the node an install finished at `boundary`, so the peer's
 * cursors move to it (raft_drive.h's raft_repl_installed) and whatever
 * that newly puts within reach of a quorum commits. */
int rn_installed(raft_node *n, uint64_t peer, uint64_t boundary);

/* ---- what the host still owns ------------------------------------------- */

/*
 * Append one entry at the current term, make it durable, and replicate
 * it -- the leader's half of a proposal. The host owns the promise that
 * settles when it applies; this owns everything up to that.
 *
 * The commit check runs here too, because a single-voter group reaches a
 * quorum without sending anything: no reply will ever arrive to trigger
 * it, and such a group would otherwise append forever and commit
 * nothing. BJ_ERR_STATE if this node is not the leader -- the host
 * refuses first with a routing error the caller can act on.
 */
int rn_propose(raft_node *n, int type, const uint8_t *payload, uint32_t len,
               uint64_t *out_index);

/*
 * Seed the commit index at startup from the log's persisted (advisory)
 * marker and the state machine's applied floor. Never lowers it. The
 * host knows both numbers and the node knows neither.
 */
void rn_seed_commit(raft_node *n, uint64_t index);

/*
 * Stand for election NOW: a real round, skipping pre-vote, because the
 * transferring leader that sent TimeoutNow (section 3.10) has certified
 * this node is caught up and leader stickiness would otherwise make
 * every peer refuse while that leader still lives. A leader, a stopped
 * node and a non-voter all decline.
 */
int rn_campaign(raft_node *n, double random01);

/*
 * A message the HOST answered (InstallSnapshot) carrying a leader's
 * term: adopt it the way an AppendEntries would -- step down if it is
 * newer or we are not already following, record the leader, refresh the
 * election timer. Returns 0 for a stale term, which is the caller's cue
 * to refuse the message.
 */
int rn_observe_leader(raft_node *n, uint64_t term, uint64_t leader_id, double random01);

/*
 * A reply the HOST awaited (a snapshot chunk) carrying a higher term:
 * step down. Unlike rn_observe_leader this records no leader and
 * refreshes no contact -- the peer that deposed us is not thereby our
 * leader. Returns 1 if it deposed us.
 */
int rn_step_down(raft_node *n, uint64_t term, double random01);

#ifdef __cplusplus
}
#endif

#endif /* RAFT_NODE_H */
