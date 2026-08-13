/*
 * server/applied.h — the instance's own applied index.
 *
 * WHAT IT IS FOR. Every structure records the last log index applied to
 * it in its own metadata, and the per-database catalog records the DDL
 * three -- but a dropDatabase removes the whole directory, catalog
 * included, so nothing INSIDE the instance survives to remember how far
 * the instance had applied. After such a drop plus a log compaction, the
 * floor (a max over what survives) sits below the log's base, and the
 * committed generation at the base is restored so replay can reach the
 * live state (replica.c's restore_if_unusable).
 *
 * Correct -- and, without this file, RECURRING: converging by
 * restore-then-replay reaches a state that still records nothing above
 * the base, so an idle instance restored on EVERY boot, rewriting its
 * whole dataset each time, until one ordinary write happened to land.
 * docs/db-server.md carried that as a known cost from the day the
 * restore shipped.
 *
 * THE RECORD COSTS NO WRITE ANY WRITE PATH PAYS. It is written at
 * exactly the two moments durable evidence for its value comes into
 * existence, and nowhere else:
 *
 *   - a LOCAL SNAPSHOT commits: every entry at or below the boundary is
 *     captured in the generation, durably, whatever later happens to the
 *     live files;
 *   - an INSTALL is adopted: the leader's generation at its boundary has
 *     just become this member's whole state.
 *
 * Recording a value only when a committed generation proves it is what
 * keeps the lost-write rule intact (a record ahead of durable truth
 * would let the floor claim entries whose effects could vanish -- the
 * same rule catalog_note_applied states). A crash between the manifest
 * commit and this write costs one extra restore on the next boot, which
 * is yesterday's behavior, converging.
 *
 * WHO READS IT: the boot floor (max'd with dbi_applied_floor) and
 * restore_if_unusable's own floor -- which is the line that ends the
 * recurrence: a marked instance's floor is at least the base, so a
 * restore that already happened is never mistaken for one still owed.
 *
 * Lives at the ROOT, beside __group__.bj, because the root is the one
 * thing a dropDatabase cannot delete. Same encoding rules as the group
 * id: a binjson number read back through a double, so 53 bits is the
 * ceiling -- comfortably past any log this store will hold.
 */
#ifndef NISABA_SERVER_APPLIED_H
#define NISABA_SERVER_APPLIED_H

#include <stdint.h>

#include "bjio.h"
#include "bjns.h"

#define APPLIED_FILE "__applied__.bj"

/* BJ_OK with *out == 0 when absent or empty -- every instance older than
 * this file, and every instance that has never snapshotted. A file that
 * exists and cannot be parsed IS an error. */
int applied_mark_load(bj_ns *ns, uint64_t *out);

/* Created, truncated and synced before returning. Callers pass only a
 * committed generation's boundary -- see the header comment for why that
 * is the whole contract. */
int applied_mark_store(bj_ns *ns, uint64_t applied);

#endif /* NISABA_SERVER_APPLIED_H */
