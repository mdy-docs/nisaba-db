/*
 * readers.c — see readers.h.
 *
 * One mutex, one condition variable, two singly-linked queues and a
 * self-pipe. Deliberately the smallest thing that can be reasoned about
 * whole: everything shared between the serving thread and a worker is in
 * `struct rdpool` below and is touched only while `m` is held, with two
 * stated exceptions -- the pipe (a write to which needs no lock) and a
 * job's own fields once it has been unlinked from a queue, which by then
 * exactly one thread can see.
 */
#include "readers.h"

#include "db_session.h"   /* dbs_read */

#include <stdlib.h>
#include <string.h>

#if SERVER_HAS_READ_THREADS

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

/*
 * One read, from handed over to reaped.
 *
 * `req` is a COPY. The bytes arrive in a connection's input buffer, which
 * the serving thread reuses for the next request the moment this one is
 * submitted -- so a worker reading the original would be racing the next
 * client frame, and would sometimes answer it instead.
 */
typedef struct rdjob {
    uint64_t       client;
    uint64_t       seq;
    dc_collection *view;      /* owned between submit and completion */
    dbuf           req;       /* owned copy */
    dbuf           answer;    /* what dbs_read built */
    int            rc;        /* what dbs_read returned */
    struct rdjob  *next;
} rdjob;

struct rdpool {
    pthread_t       *tid;
    int              nthreads;
    int              max_inflight;

    pthread_mutex_t  m;
    pthread_cond_t   cv;        /* workers wait here for work */
    pthread_cond_t   idle;      /* the serving thread waits here to drain */
    rdjob           *todo_head, *todo_tail;
    rdjob           *done_head, *done_tail;
    int              inflight;  /* submitted, not yet reaped */
    int              stopping;

    int              wake[2];   /* [0] read: the pollset's. [1] write. */
};

static void job_free(rdjob *j) {
    if (!j) return;
    /* A view whose job never ran, or ran and was never reaped. Freeing one
     * closes no file (db.h) -- it gives back buffers. */
    if (j->view) dbs_read_view_close(j->view);
    dbuf_free(&j->req);
    dbuf_free(&j->answer);
    free(j);
}

/* One byte, best effort. A full pipe already holds a wake nobody has
 * consumed, so losing this one loses nothing -- and a worker must never
 * block here, which is why the write end is non-blocking. */
static void wake_up(rdpool *p) {
    if (p->wake[1] < 0) return;
    const uint8_t one = 1;
    ssize_t w;
    do { w = write(p->wake[1], &one, 1); } while (w < 0 && errno == EINTR);
    (void)w;
}

static void *worker(void *ctx) {
    rdpool *p = (rdpool *)ctx;
    for (;;) {
        pthread_mutex_lock(&p->m);
        while (!p->todo_head && !p->stopping) pthread_cond_wait(&p->cv, &p->m);
        if (!p->todo_head) { pthread_mutex_unlock(&p->m); return NULL; }
        rdjob *j = p->todo_head;
        p->todo_head = j->next;
        if (!p->todo_head) p->todo_tail = NULL;
        j->next = NULL;
        pthread_mutex_unlock(&p->m);

        /*
         * Outside the lock, and this is the whole point: the scan happens
         * here while the serving thread answers everybody else. `j` is
         * unlinked, so no other thread can see it; the view is private to
         * this job; dbs_read touches nothing else.
         */
        j->rc = dbs_read(j->view, j->req.data, j->req.len, &j->answer);
        /* Given back as soon as the read is over rather than at reap, so a
         * view is never held for the length of a poll cycle it has no
         * reason to be held for. */
        dbs_read_view_close(j->view);
        j->view = NULL;

        pthread_mutex_lock(&p->m);
        j->next = NULL;
        if (p->done_tail) p->done_tail->next = j; else p->done_head = j;
        p->done_tail = j;
        /* Announced, not just queued. The serving thread learns about a
         * completion two ways and needs both: through the pipe when it is
         * in poll(), and through this when it is BLOCKED waiting for the
         * readers to finish so it can unmake a file. Signalling only on the
         * first was a drain that slept through every landing it was waiting
         * for. */
        pthread_cond_broadcast(&p->idle);
        pthread_mutex_unlock(&p->m);
        wake_up(p);
    }
}

rdpool *rdpool_open(int threads, int max_inflight) {
    if (threads <= 0 || max_inflight <= 0) return NULL;
    rdpool *p = (rdpool *)calloc(1, sizeof *p);
    if (!p) return NULL;
    p->max_inflight = max_inflight;
    p->wake[0] = p->wake[1] = -1;
    if (pthread_mutex_init(&p->m, NULL)) { free(p); return NULL; }
    if (pthread_cond_init(&p->cv, NULL)) { pthread_mutex_destroy(&p->m); free(p); return NULL; }
    if (pthread_cond_init(&p->idle, NULL)) {
        pthread_cond_destroy(&p->cv); pthread_mutex_destroy(&p->m); free(p); return NULL;
    }

    if (pipe(p->wake) != 0) { p->wake[0] = p->wake[1] = -1; rdpool_close(p); return NULL; }
    /* BOTH ends non-blocking. The read end because the serving thread
     * drains it speculatively; the write end because a worker holding a
     * finished answer must never block on a pipe nobody is reading. */
    for (int i = 0; i < 2; i++) {
        int fl = fcntl(p->wake[i], F_GETFL, 0);
        if (fl < 0 || fcntl(p->wake[i], F_SETFL, fl | O_NONBLOCK) < 0) {
            rdpool_close(p);
            return NULL;
        }
    }

    p->tid = (pthread_t *)calloc((size_t)threads, sizeof(pthread_t));
    if (!p->tid) { rdpool_close(p); return NULL; }
    for (int i = 0; i < threads; i++) {
        if (pthread_create(&p->tid[i], NULL, worker, p) != 0) {
            /* However many did start are joined by rdpool_close, which is
             * why nthreads counts them as they appear rather than up front. */
            rdpool_close(p);
            return NULL;
        }
        p->nthreads = i + 1;
    }
    return p;
}

void rdpool_close(rdpool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->m);
    p->stopping = 1;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->m);
    for (int i = 0; i < p->nthreads; i++) pthread_join(p->tid[i], NULL);
    free(p->tid);

    /* Joined, so nothing else can reach these lists. */
    for (rdjob *j = p->todo_head; j; ) { rdjob *n = j->next; job_free(j); j = n; }
    for (rdjob *j = p->done_head; j; ) { rdjob *n = j->next; job_free(j); j = n; }
    if (p->wake[0] >= 0) close(p->wake[0]);
    if (p->wake[1] >= 0) close(p->wake[1]);
    pthread_cond_destroy(&p->idle);
    pthread_cond_destroy(&p->cv);
    pthread_mutex_destroy(&p->m);
    free(p);
}

int rdpool_wake_fd(const rdpool *p) { return p ? p->wake[0] : -1; }

void rdpool_drain_wake(rdpool *p) {
    if (!p || p->wake[0] < 0) return;
    uint8_t buf[64];
    for (;;) {
        ssize_t got = read(p->wake[0], buf, sizeof buf);
        if (got > 0) continue;
        if (got < 0 && errno == EINTR) continue;
        return;   /* drained (EAGAIN), or closed */
    }
}

int rdpool_submit(rdpool *p, uint64_t client, uint64_t seq,
                  dc_collection *view, const uint8_t *req, size_t req_len) {
    if (!p || !view || !req) return BJ_ERR_STATE;

    rdjob *j = (rdjob *)calloc(1, sizeof *j);
    if (!j) return BJ_ERR_OOM;
    j->client = client;
    j->seq = seq;
    int e = dbuf_put(&j->req, req, req_len);
    if (e) { j->view = NULL; job_free(j); return e; }

    pthread_mutex_lock(&p->m);
    if (p->stopping || p->inflight >= p->max_inflight) {
        pthread_mutex_unlock(&p->m);
        j->view = NULL;   /* not taken: the caller still owns it */
        job_free(j);
        return BJ_ERR_RANGE;
    }
    /* Ownership transfers HERE, under the lock and after the last thing
     * that can fail -- so there is no window in which both sides believe
     * they own the view, and none in which neither does. */
    j->view = view;
    if (p->todo_tail) p->todo_tail->next = j; else p->todo_head = j;
    p->todo_tail = j;
    p->inflight++;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->m);
    return BJ_OK;
}

int rdpool_reap(rdpool *p, uint64_t *client, uint64_t *seq, int *rc,
                dbuf *out, int *have) {
    *have = 0;
    if (!p || !out) return BJ_ERR_STATE;

    pthread_mutex_lock(&p->m);
    rdjob *j = p->done_head;
    if (j) {
        p->done_head = j->next;
        if (!p->done_head) p->done_tail = NULL;
        p->inflight--;
        /* Whoever is draining wants to know the moment the last one lands. */
        if (p->inflight == 0) pthread_cond_broadcast(&p->idle);
    }
    pthread_mutex_unlock(&p->m);
    if (!j) return BJ_OK;

    *client = j->client;
    *seq = j->seq;
    *rc = j->rc;
    int e = j->answer.len ? dbuf_put(out, j->answer.data, j->answer.len) : BJ_OK;
    *have = 1;
    job_free(j);
    return e;
}

int rdpool_inflight(const rdpool *p) {
    if (!p) return 0;
    /* Cast away const to take the lock: reading an int without it is a race
     * TSan would report, and a torn count here decides whether something
     * destructive proceeds. */
    rdpool *q = (rdpool *)p;
    pthread_mutex_lock(&q->m);
    int n = q->inflight;
    pthread_mutex_unlock(&q->m);
    return n;
}

void rdpool_wait_completion(rdpool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->m);
    while (p->inflight > 0 && !p->done_head) pthread_cond_wait(&p->idle, &p->m);
    pthread_mutex_unlock(&p->m);
}

#else  /* no threads on this target: every entry point is a refusal or a nop */

rdpool *rdpool_open(int threads, int max_inflight) {
    (void)threads; (void)max_inflight;
    return NULL;
}
void rdpool_close(rdpool *p) { (void)p; }
int  rdpool_wake_fd(const rdpool *p) { (void)p; return -1; }
void rdpool_drain_wake(rdpool *p) { (void)p; }
int  rdpool_submit(rdpool *p, uint64_t client, uint64_t seq,
                   dc_collection *view, const uint8_t *req, size_t req_len) {
    (void)p; (void)client; (void)seq; (void)view; (void)req; (void)req_len;
    return BJ_ERR_STATE;
}
int rdpool_reap(rdpool *p, uint64_t *client, uint64_t *seq, int *rc,
                dbuf *out, int *have) {
    (void)p; (void)client; (void)seq; (void)rc; (void)out;
    *have = 0;
    return BJ_OK;
}
int  rdpool_inflight(const rdpool *p) { (void)p; return 0; }
void rdpool_wait_completion(rdpool *p) { (void)p; }

#endif
