/*
 * nscheck.h — a bj_ns that enforces the plan/execute invariant.
 *
 *     No C operation may open a file it did not name in its own
 *     preceding plan call.
 *
 * That sentence is the load-bearing assumption of the entire async
 * design (bjns.h). It is what lets `bj_ns.open` be synchronous in a
 * browser, where opening a file is not: the host pre-opens exactly the
 * names the plan returned, registers them, and the adapter serves
 * `open` as a table lookup. A name nobody declared is BJ_ERR_STATE.
 *
 * Under POSIX the invariant is invisible. `openat` opens anything, so C
 * that opens an undeclared file works perfectly -- natively, and only
 * natively. The break shows up in a browser, in the one operation
 * (compaction) that is hardest to reach from a test, and it shows up as
 * a bare BJ_ERR_STATE with nothing to say which name caused it.
 *
 * So this adapter makes the native build pretend to be the browser. It
 * wraps a real bj_ns and models bjns_bridge.c's two constraints:
 *
 *   1. `open` resolves ONLY names declared since the last nscheck_begin.
 *      An undeclared one returns BJ_ERR_STATE -- the same code the
 *      bridge returns -- and is recorded with its name, which the bridge
 *      cannot do.
 *
 *   2. BJ_NS_TRUNC and BJ_NS_EXCL are STRIPPED before delegating. The
 *      bridge ignores flags entirely (`(void)flags`): the host already
 *      opened the file, with `{create: true}` and nothing else, and
 *      re-opening is the one thing that adapter cannot do. So C may ask
 *      for either flag -- both are right for POSIX -- but must not
 *      depend on getting it. Stripping them here is what turns "must
 *      not depend on" into something a test can find out.
 *
 * Declarations are derived from a plan's actual output
 * (nscheck_declare_compact_plan), not hand-written by the test. A
 * hand-written list would only assert that the test and the executor
 * agree; deriving it asserts that the PLAN and the executor agree, which
 * is the property the browser actually depends on.
 *
 * Not a production adapter and not built into one: it lives in
 * test/native/ because its job is to fail a test, and because the
 * browser has the real constraint rather than a simulation of it.
 */
#ifndef NSCHECK_H
#define NSCHECK_H

#include <stddef.h>
#include <stdint.h>

#include "bjns.h"

typedef struct nscheck nscheck;

/*
 * Wrap `inner` and fill *out with the checking namespace. `inner` is
 * BORROWED -- it must outlive the nscheck, and the caller still frees it
 * (with bjns_posix_free or whatever opened it). Returns NULL on OOM.
 *
 * The caller keeps `inner` usable on purpose: harness setup opens files
 * as the host, and the host is not the thing under test. Pass *out only
 * to the C entry points whose opens are being checked.
 */
nscheck *nscheck_new(const bj_ns *inner, bj_ns *out);
void     nscheck_free(nscheck *k);

/*
 * Open a declaration window, forgetting the previous one. "The
 * IMMEDIATELY preceding plan call" is literal: the browser host builds
 * Module.bjnsScopes[scope] before the execute call and deletes it in a
 * finally afterwards, so a name declared for one operation can never
 * satisfy the next.
 */
void nscheck_begin(nscheck *k);

/* Declare one name. BJ_OK, or BJ_ERR_RANGE past the fixed capacities. */
int nscheck_declare(nscheck *k, const char *name, uint32_t len);

/*
 * Declare exactly what a host pre-opens for dc_compact_execute, read out
 * of dc_compact_plan's own output: newEntry.file, then every
 * build[].files[] in order, then newEntry.journal.
 *
 * That order is not decorative -- it is the order wasm/nisaba-wasm.js
 * awaits them in, and the order dc_compact_execute consumes `sources`
 * in. Keeping one function that knows it means a plan gaining a file
 * teaches the host, the executor and this checker at once.
 */
int nscheck_declare_compact_plan(nscheck *k, const uint8_t *plan, size_t plan_len);

/* Close the window. Every subsequent open is undeclared. */
void nscheck_end(nscheck *k);

/* How many opens were served -- so a test can prove it checked anything
 * at all. A checker that never sees an open passes vacuously. */
uint32_t nscheck_opens(const nscheck *k);
/* Closes served. Paired with nscheck_opens, this is how a test asserts
 * that something GAVE BACK what it opened -- an all-or-nothing failure
 * path that unwinds badly leaks handles, and LeakSanitizer only exists
 * on Linux, so without a counter the assertion cannot be made on a
 * developer's machine at all. */
uint32_t nscheck_closes(const nscheck *k);
/* Removes seen. Never checked against the declaration: the bridge queues
 * any name at all, and bjns.h permits deferring them. */
uint32_t nscheck_removes(const nscheck *k);
/* Declared but never opened: the plan named a file the executor did not
 * want. In a browser that is an awaited OPFS handle opened for nothing. */
uint32_t nscheck_unopened(const nscheck *k);

uint32_t    nscheck_violations(const nscheck *k);
/* The first violation, as "open of undeclared name \"g1-coll-x.bj\"",
 * or NULL. First rather than last: later ones are usually its wake. */
const char *nscheck_first_violation(const nscheck *k);

#endif /* NSCHECK_H */
