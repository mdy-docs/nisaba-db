/* server/join.c — see join.h. */
/* Before any header, the way server/main.c asks and for the same reason:
 * -std=c11 defines __STRICT_ANSI__, and a sysroot is entitled to hide
 * nanosleep behind that. wasi-libc's do; Darwin's does not. */
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "join.h"

#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#include "binjson.h"
#include "raft_msg.h"

/*
 * How many rounds of every seed, and how long between them --
 * src/raft-host.js's seedRequest defaults, copied because the two are
 * meant to behave the same. Twenty rounds at a quarter of a second is
 * five seconds of trying, which comfortably outlasts an election.
 */
#define JOIN_ATTEMPTS 20
#define JOIN_DELAY_MS 250

/* One call's own patience. Shorter than a round of every seed, so a seed
 * that accepts a connection and says nothing cannot eat the whole
 * budget. */
#define JOIN_CALL_MS 2000

static void say(char *why, size_t cap, const char *fmt, ...) {
    if (!why || !cap) return;
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(why, cap, fmt, ap);
    va_end(ap);
}

static void nap(int ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (long)(ms % 1000) * 1000000L;
    nanosleep(&ts, NULL);
}

/*
 * One message, offered to seed after seed until somebody answers it for
 * good. `preferred` is the leader's address as the last redirect gave
 * it: tried FIRST on the next round, because a redirect that is not
 * followed leaves a joiner asking the same follower forever.
 */
static int seed_request(const seed_addr *seeds, int nseeds,
                        const uint8_t *msg, uint32_t len,
                        dbuf *members, char *why, size_t why_cap) {
    if (!seeds || nseeds <= 0) {
        say(why, why_cap, "at least one seed address is required");
        return BJ_ERR_STATE;
    }
    seed_addr preferred;
    int have_preferred = 0;
    say(why, why_cap, "no seed answered");

    for (int attempt = 0; attempt < JOIN_ATTEMPTS; attempt++) {
        for (int t = -1; t < nseeds; t++) {
            const seed_addr *to;
            if (t < 0) {
                if (!have_preferred) continue;
                to = &preferred;
            } else {
                to = &seeds[t];
            }

            dbuf reply = {0};
            char err[192];
            int rc = peers_call(to->host, to->port, msg, len, JOIN_CALL_MS,
                                &reply, err, sizeof err);
            if (rc < 0) {
                /* That seed is down, or not listening yet. Ordinary: it
                 * is the case a cluster exists to survive. */
                say(why, why_cap, "%s:%d did not answer", to->host, to->port);
                dbuf_free(&reply);
                continue;
            }
            if (rc > 0) {
                /* The node heard the question and could not take it --
                 * out of room to park the answer, most likely. Not a
                 * verdict on what was asked, so it is worth asking
                 * again. */
                say(why, why_cap, "%s:%d refused it: %s", to->host, to->port,
                    err[0] ? err : "no reason given");
                dbuf_free(&reply);
                continue;
            }

            rmsg_membership m;
            int e = rmsg_read_membership_reply(reply.data, (uint32_t)reply.len, &m);
            if (e) {
                say(why, why_cap, "%s:%d answered something that is not a membership reply",
                    to->host, to->port);
                dbuf_free(&reply);
                continue;
            }
            if (m.ok) {
                e = dbuf_put(members, m.members, m.members_len);
                dbuf_free(&reply);
                return e;
            }
            if (m.error) {
                /* A validation refusal never heals by being repeated. */
                say(why, why_cap, "%.*s", (int)m.error_len, (const char *)m.error);
                dbuf_free(&reply);
                return BJ_ERR_STATE;
            }
            if (m.retry) {
                say(why, why_cap, "a membership change is already in flight");
            } else if (m.leader_host && m.leader_port > 0 &&
                       m.leader_host_len < sizeof preferred.host) {
                memcpy(preferred.host, m.leader_host, m.leader_host_len);
                preferred.host[m.leader_host_len] = '\0';
                preferred.port = m.leader_port;
                have_preferred = 1;
                say(why, why_cap, "redirected to node %llu at %s:%d",
                    (unsigned long long)m.leader_id, preferred.host, preferred.port);
            } else {
                /* A redirect with no address: this node knows of no
                 * leader, which is an election in progress. */
                have_preferred = 0;
                say(why, why_cap, "no leader yet (last hint: node %llu)",
                    (unsigned long long)m.leader_id);
            }
            dbuf_free(&reply);
        }
        nap(JOIN_DELAY_MS);
    }
    return BJ_ERR_STATE;
}

int join_cluster(const seed_addr *seeds, int nseeds, uint64_t self_id,
                 const char *self_host, int self_port,
                 dbuf *members, char *why, size_t why_cap) {
    if (!self_id || !self_host || self_port <= 0) return BJ_ERR_STATE;

    /* This node's record, as the cluster will hold it. The ADDRESS is
     * the point: a member set that named ids alone would be a set nobody
     * could replicate to, and the log is where a cluster's shape is
     * written down. */
    bj_builder *b = bj_builder_new();
    if (!b) return BJ_ERR_OOM;
    int e = bj_begin_object(b);
    if (!e) e = bj_put_key(b, (const uint8_t *)"id", 2);
    if (!e) e = bj_put_int(b, (int64_t)self_id);
    if (!e) e = bj_put_key(b, (const uint8_t *)"host", 4);
    if (!e) e = bj_put_string(b, (const uint8_t *)self_host, (uint32_t)strlen(self_host));
    if (!e) e = bj_put_key(b, (const uint8_t *)"port", 4);
    if (!e) e = bj_put_int(b, self_port);
    if (!e) e = bj_end_object(b);
    if (!e) e = bj_builder_error(b);

    dbuf msg = {0};
    if (!e) {
        size_t rlen = 0;
        const uint8_t *rec = bj_builder_data(b, &rlen);
        e = rec ? rmsg_build_join(rec, (uint32_t)rlen, &msg) : BJ_ERR_STATE;
    }
    bj_builder_free(b);
    if (e) { dbuf_free(&msg); return e; }

    e = seed_request(seeds, nseeds, msg.data, (uint32_t)msg.len, members, why, why_cap);
    dbuf_free(&msg);
    return e;
}

int leave_cluster(const seed_addr *seeds, int nseeds, uint64_t id,
                  dbuf *members, char *why, size_t why_cap) {
    dbuf msg = {0};
    int e = rmsg_build_leave(id, &msg);
    if (e) { dbuf_free(&msg); return e; }
    e = seed_request(seeds, nseeds, msg.data, (uint32_t)msg.len, members, why, why_cap);
    dbuf_free(&msg);
    return e;
}
