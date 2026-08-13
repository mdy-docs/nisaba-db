/*
 * writer.c — see writer.h.
 *
 * readers.c's shape, deliberately: one mutex, one condition variable for
 * the worker, one for the serving thread, two singly-linked queues and a
 * self-pipe. Everything shared is in `struct wrpool` and touched only
 * while `m` is held, with the same two stated exceptions -- the pipe,
 * and a job's own fields once it is unlinked, which by then exactly one
 * thread can see.
 *
 * The one thing readers.c does not have is the BOUNDARY PAUSE, and it is
 * three fields: `pause` (the loop wants the worker parked), `applying`
 * (the worker is inside dbi_apply right now), and the rule that the
 * worker re-checks `pause` under the lock before every pop. wr_pause
 * sets the flag and waits out `applying`; from then until wr_resume the
 * worker cannot enter the engine, which is the whole promise.
 */
#include "writer.h"

#include "db_validate.h"   /* dc_is_deterministic: the halt is judged HERE */

#include <stdlib.h>
#include <string.h>

#if SERVER_HAS_WRITE_THREAD

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <unistd.h>

typedef struct wrjob {
    uint64_t      index;
    dbuf          payload;    /* owned copy; freed once applied */
    dbuf          result;     /* what dbi_apply built */
    int           rc;         /* what dbi_apply returned */
    struct wrjob *next;
} wrjob;

struct wrpool {
    dbi             *inst;      /* borrowed; swapped only while idle */
    pthread_t        tid;
    int              started;

    pthread_mutex_t  m;
    pthread_cond_t   cv;        /* the worker waits here for work / resume */
    pthread_cond_t   done;      /* the serving thread waits here to drain/pause */
    wrjob           *todo_head, *todo_tail;
    wrjob           *done_head, *done_tail;
    int              inflight;  /* submitted, not yet reaped */
    int              unapplied; /* submitted, not yet applied */
    int              applying;  /* the worker is inside dbi_apply */
    /*
     * A COUNT, not a flag: pauses nest. A client teardown inside the
     * event-drain block is two holders of the same park, and the worker
     * runs again when the LAST one lets go -- a flag there would resume
     * the writer under the outer holder's feet.
     */
    int              pause;
    /*
     * AN APPLY FAILED NON-DETERMINISTICALLY: this member has diverged,
     * and the strongest thing it does is stop. The judgement has to be
     * made HERE, not at reap -- apply is ordered, and by the time the
     * loop reaps entry N the worker would already be inside N+1, which
     * may depend on the very thing N failed to do. So the worker parks
     * itself the moment it happens; the loop reaps the failed entry,
     * sees the code, and halts the replica exactly as the inline pump
     * always did. Queued entries stay queued and die with the pool,
     * unapplied -- which is what "the floor never covered them" means.
     */
    int              halted;
    int              stopping;

    int              wake[2];   /* [0] read: the pollset's. [1] write. */
};

static void job_free(wrjob *j) {
    if (!j) return;
    dbuf_free(&j->payload);
    dbuf_free(&j->result);
    free(j);
}

/* One byte, best effort -- a full pipe already holds a wake nobody has
 * consumed (readers.c says the rest). */
static void wake_up(wrpool *p) {
    if (p->wake[1] < 0) return;
    const uint8_t one = 1;
    ssize_t w;
    do { w = write(p->wake[1], &one, 1); } while (w < 0 && errno == EINTR);
    (void)w;
}

static void *worker(void *ctx) {
    wrpool *p = (wrpool *)ctx;
    for (;;) {
        pthread_mutex_lock(&p->m);
        /* Parked while paused EVEN WITH WORK QUEUED: the pause is the
         * loop's promise to itself that nothing is inside the engine,
         * and a worker that kept applying through it would break every
         * caller that touches a live tree under it. Parked for good
         * once halted, for the reason on the field. */
        while ((!p->todo_head || p->pause || p->halted) && !p->stopping)
            pthread_cond_wait(&p->cv, &p->m);
        if (p->stopping) { pthread_mutex_unlock(&p->m); return NULL; }
        wrjob *j = p->todo_head;
        p->todo_head = j->next;
        if (!p->todo_head) p->todo_tail = NULL;
        j->next = NULL;
        p->applying = 1;
        dbi *inst = p->inst;
        pthread_mutex_unlock(&p->m);

        /*
         * Outside the lock: the apply happens here while the serving
         * thread answers everybody else. `j` is unlinked, so only this
         * thread sees it; `inst` was read under the lock and cannot be
         * swapped while `applying` holds (wr_set_instance refuses while
         * anything is unapplied, and unapplied covers this job).
         */
        j->rc = dbi_apply(inst, j->index, j->payload.data,
                          (uint32_t)j->payload.len, &j->result);
        /* The payload's work is done; the result may wait a while to be
         * reaped and should not hold the entry's bytes with it. */
        dbuf_free(&j->payload);

        pthread_mutex_lock(&p->m);
        p->applying = 0;
        p->unapplied--;
        if (j->rc && !dc_is_deterministic(j->rc)) p->halted = 1;
        j->next = NULL;
        if (p->done_tail) p->done_tail->next = j; else p->done_head = j;
        p->done_tail = j;
        /* Both ways, like readers.c's completion: through the pipe when
         * the loop is in poll(), and through the condvar when it is
         * BLOCKED in wr_pause or wr_wait_applied. */
        pthread_cond_broadcast(&p->done);
        pthread_mutex_unlock(&p->m);
        wake_up(p);
    }
}

wrpool *wr_open(dbi *inst) {
    if (!inst) return NULL;
    wrpool *p = (wrpool *)calloc(1, sizeof *p);
    if (!p) return NULL;
    p->inst = inst;
    p->wake[0] = p->wake[1] = -1;
    if (pthread_mutex_init(&p->m, NULL)) { free(p); return NULL; }
    if (pthread_cond_init(&p->cv, NULL)) { pthread_mutex_destroy(&p->m); free(p); return NULL; }
    if (pthread_cond_init(&p->done, NULL)) {
        pthread_cond_destroy(&p->cv); pthread_mutex_destroy(&p->m); free(p);
        return NULL;
    }

    if (pipe(p->wake) != 0) { p->wake[0] = p->wake[1] = -1; wr_close(p); return NULL; }
    for (int i = 0; i < 2; i++) {
        int fl = fcntl(p->wake[i], F_GETFL, 0);
        if (fl < 0 || fcntl(p->wake[i], F_SETFL, fl | O_NONBLOCK) < 0) {
            wr_close(p);
            return NULL;
        }
    }

    if (pthread_create(&p->tid, NULL, worker, p) != 0) {
        wr_close(p);
        return NULL;
    }
    p->started = 1;
    return p;
}

void wr_close(wrpool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->m);
    p->stopping = 1;
    pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->m);
    if (p->started) pthread_join(p->tid, NULL);

    /* Joined, so nothing else can reach these lists. An unapplied entry
     * is simply not applied: the floor never covered it and the next
     * boot replays it -- the crash story a kill mid-apply always had. */
    for (wrjob *j = p->todo_head; j; ) { wrjob *n = j->next; job_free(j); j = n; }
    for (wrjob *j = p->done_head; j; ) { wrjob *n = j->next; job_free(j); j = n; }
    if (p->wake[0] >= 0) close(p->wake[0]);
    if (p->wake[1] >= 0) close(p->wake[1]);
    pthread_cond_destroy(&p->done);
    pthread_cond_destroy(&p->cv);
    pthread_mutex_destroy(&p->m);
    free(p);
}

int wr_set_instance(wrpool *p, dbi *inst) {
    if (!p || !inst) return BJ_ERR_STATE;
    pthread_mutex_lock(&p->m);
    /* The caller's drain discipline, asserted: a swap under an unapplied
     * entry would apply it against an instance being freed. */
    if (p->unapplied > 0) { pthread_mutex_unlock(&p->m); return BJ_ERR_STATE; }
    p->inst = inst;
    pthread_mutex_unlock(&p->m);
    return BJ_OK;
}

int wr_wake_fd(const wrpool *p) { return p ? p->wake[0] : -1; }

void wr_drain_wake(wrpool *p) {
    if (!p || p->wake[0] < 0) return;
    uint8_t buf[64];
    for (;;) {
        ssize_t got = read(p->wake[0], buf, sizeof buf);
        if (got > 0) continue;
        if (got < 0 && errno == EINTR) continue;
        return;
    }
}

int wr_submit(wrpool *p, uint64_t index, const uint8_t *payload, uint32_t len) {
    if (!p || !payload) return BJ_ERR_STATE;
    wrjob *j = (wrjob *)calloc(1, sizeof *j);
    if (!j) return BJ_ERR_OOM;
    j->index = index;
    int e = dbuf_put(&j->payload, payload, len);
    if (e) { job_free(j); return e; }

    pthread_mutex_lock(&p->m);
    if (p->stopping) {
        pthread_mutex_unlock(&p->m);
        job_free(j);
        return BJ_ERR_STATE;
    }
    if (p->todo_tail) p->todo_tail->next = j; else p->todo_head = j;
    p->todo_tail = j;
    p->inflight++;
    p->unapplied++;
    pthread_cond_signal(&p->cv);
    pthread_mutex_unlock(&p->m);
    return BJ_OK;
}

int wr_reap(wrpool *p, uint64_t *index, int *rc, dbuf *result, int *have) {
    *have = 0;
    if (!p || !result) return BJ_ERR_STATE;

    pthread_mutex_lock(&p->m);
    wrjob *j = p->done_head;
    if (j) {
        p->done_head = j->next;
        if (!p->done_head) p->done_tail = NULL;
        p->inflight--;
    }
    pthread_mutex_unlock(&p->m);
    if (!j) return BJ_OK;

    *index = j->index;
    *rc = j->rc;
    int e = j->result.len ? dbuf_put(result, j->result.data, j->result.len) : BJ_OK;
    *have = 1;
    job_free(j);
    return e;
}

int wr_inflight(const wrpool *p) {
    if (!p) return 0;
    wrpool *q = (wrpool *)p;   /* readers.c says why: a torn int decides things */
    pthread_mutex_lock(&q->m);
    int n = q->inflight;
    pthread_mutex_unlock(&q->m);
    return n;
}

int wr_unapplied(const wrpool *p) {
    if (!p) return 0;
    wrpool *q = (wrpool *)p;
    pthread_mutex_lock(&q->m);
    int n = q->unapplied;
    pthread_mutex_unlock(&q->m);
    return n;
}

void wr_pause(wrpool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->m);
    p->pause++;
    /* The worker checks `pause` under the lock before every pop, so once
     * `applying` falls it cannot rise again until the count reaches
     * zero. */
    while (p->applying) pthread_cond_wait(&p->done, &p->m);
    pthread_mutex_unlock(&p->m);
}

void wr_resume(wrpool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->m);
    if (p->pause > 0 && --p->pause == 0) pthread_cond_broadcast(&p->cv);
    pthread_mutex_unlock(&p->m);
}

void wr_wait_applied(wrpool *p) {
    if (!p) return;
    pthread_mutex_lock(&p->m);
    /* A halted worker applies nothing more, ever: what it owes the
     * caller is the failed entry, waiting in the done queue. */
    while (p->unapplied > 0 && !p->pause && !p->halted)
        pthread_cond_wait(&p->done, &p->m);
    pthread_mutex_unlock(&p->m);
}

#else  /* no threads on this target: every entry point is a refusal or a nop */

wrpool *wr_open(dbi *inst) { (void)inst; return NULL; }
void wr_close(wrpool *p) { (void)p; }
int  wr_set_instance(wrpool *p, dbi *inst) { (void)p; (void)inst; return BJ_ERR_STATE; }
int  wr_wake_fd(const wrpool *p) { (void)p; return -1; }
void wr_drain_wake(wrpool *p) { (void)p; }
int  wr_submit(wrpool *p, uint64_t index, const uint8_t *payload, uint32_t len) {
    (void)p; (void)index; (void)payload; (void)len;
    return BJ_ERR_STATE;
}
int wr_reap(wrpool *p, uint64_t *index, int *rc, dbuf *result, int *have) {
    (void)p; (void)index; (void)rc; (void)result;
    *have = 0;
    return BJ_OK;
}
int  wr_inflight(const wrpool *p) { (void)p; return 0; }
int  wr_unapplied(const wrpool *p) { (void)p; return 0; }
void wr_pause(wrpool *p) { (void)p; }
void wr_resume(wrpool *p) { (void)p; }
void wr_wait_applied(wrpool *p) { (void)p; }

#endif
