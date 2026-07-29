/*
 * tap.h — the whole assert framework for the native C harness.
 *
 * Deliberately not a dependency. The harness exists to run the C layer
 * under ASan/UBSan in CI in seconds with nothing installed but a
 * compiler; adding Check/Unity/cmocka would trade that away for
 * features this needs none of.
 *
 * Usage:
 *   TEST(inserts_and_finds) { ... CHECK(cond); ... }
 *   ...
 *   int main(void) { RUN(inserts_and_finds); return tap_summary(); }
 *
 * A failing CHECK reports file:line and marks the test failed, but keeps
 * going to the end of the test body -- so one run reports every broken
 * assertion, not just the first. CHECK_FATAL stops the body (use it when
 * continuing would dereference NULL).
 */
#ifndef TAP_H
#define TAP_H

#include <stdio.h>
#include <string.h>

static int tap_tests, tap_failed, tap_current_failed;

#define TEST(name) static void test_##name(void)

#define RUN(name)                                                        \
    do {                                                                 \
        tap_tests++;                                                     \
        tap_current_failed = 0;                                          \
        test_##name();                                                   \
        if (tap_current_failed) {                                        \
            tap_failed++;                                                \
            printf("not ok %d - %s\n", tap_tests, #name);                \
        } else {                                                         \
            printf("ok %d - %s\n", tap_tests, #name);                    \
        }                                                                \
    } while (0)

#define TAP_FAIL(fmt, ...)                                               \
    do {                                                                 \
        tap_current_failed = 1;                                          \
        printf("  # %s:%d: " fmt "\n", __FILE__, __LINE__, __VA_ARGS__); \
    } while (0)

#define CHECK(cond)                                                      \
    do {                                                                 \
        if (!(cond)) TAP_FAIL("CHECK(%s) failed", #cond);                \
    } while (0)

#define CHECK_FATAL(cond)                                                \
    do {                                                                 \
        if (!(cond)) { TAP_FAIL("CHECK(%s) failed", #cond); return; }    \
    } while (0)

/* Return codes: nearly every C entry point here returns BJ_OK or a
 * negative BJ_ERR_ / DC_ERR_ code, so this is the most common assertion. */
#define CHECK_OK(expr)                                                   \
    do {                                                                 \
        int rc_ = (expr);                                                \
        if (rc_ != 0) TAP_FAIL("%s returned %d, want 0", #expr, rc_);    \
    } while (0)

#define CHECK_RC(expr, want)                                             \
    do {                                                                 \
        int rc_ = (expr);                                                \
        if (rc_ != (want))                                               \
            TAP_FAIL("%s returned %d, want %d", #expr, rc_, (want));     \
    } while (0)

#define CHECK_I64(got, want)                                             \
    do {                                                                 \
        long long g_ = (long long)(got), w_ = (long long)(want);         \
        if (g_ != w_)                                                    \
            TAP_FAIL("%s == %lld, want %lld", #got, g_, w_);             \
    } while (0)

#define CHECK_STR(got, want)                                             \
    do {                                                                 \
        const char *g_ = (got), *w_ = (want);                            \
        if (!g_ || strcmp(g_, w_) != 0)                                  \
            TAP_FAIL("%s == \"%s\", want \"%s\"", #got, g_ ? g_ : "(null)", w_); \
    } while (0)

static int tap_summary(void) {
    printf("1..%d\n", tap_tests);
    if (tap_failed) {
        printf("# FAILED %d of %d\n", tap_failed, tap_tests);
        return 1;
    }
    printf("# passed %d\n", tap_tests);
    return 0;
}

#endif /* TAP_H */
