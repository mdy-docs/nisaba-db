/*
 * regex.h — db_query.c's `$regex` operator, backed by
 * third_party/regex-engine (an ECMAScript-flavored engine extracted from
 * the jsvm2 JS engine: named groups, lookahead/lookbehind, backreferences,
 * non-greedy quantifiers, Unicode property escapes -- see that submodule's
 * README for the full syntax it accepts). This header's own function
 * signature is unchanged from the small hand-rolled engine that used to
 * back it, so db_query.c needed no changes to pick this up.
 *
 * Text is decoded as UTF-8 (binjson's own string encoding) and converted
 * to UTF-16 before reaching the engine, which is natively UTF-16/codepoint
 * -aware -- unlike the old byte-oriented engine, a multi-byte UTF-8
 * character is now matched by `.`/a class as one character, not one byte
 * at a time. Malformed UTF-8 decodes leniently (U+FFFD for invalid
 * sequences) rather than erroring the whole query over one hostile
 * document.
 *
 * Only the `i` (case-insensitive) match option is threaded through from
 * db_query.c's `$options` today, matching the previous engine's scope --
 * the new engine supports `m`/`s`/`u`/etc. too, but exposing them through
 * `$options` is a separate, not-yet-done step (db_query.c's own
 * $options-parsing still hard-rejects anything but `i`).
 *
 * Compiling a pattern is expensive, so this module keeps a small
 * process-lifetime LRU cache of compiled patterns (see regex.c) rather
 * than recompiling per document the way the old engine's rx_match did --
 * fine for a lightweight parser, catastrophic for this one at
 * collection-scan scale.
 *
 * The cache once had a second justification: a Program was a fixed-size
 * ~1.9 MB struct of embedded opcode and class tables. Engine v0.3.0 moved
 * those to the heap and sized them to the pattern, so a Program is 19 KB
 * now. The cache stays -- lex, parse and compile still cost far more than
 * a lookup -- but it is a compute cache, not a memory one, and the ~16 MB
 * ceiling the old sizing implied is really about 156 KB.
 */
#ifndef REGEX_H
#define REGEX_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Search `subject` (subject_len UTF-8 bytes) for `pattern` (pattern_len
 * UTF-8 bytes) anywhere within it (not anchored, unless the pattern itself
 * starts with `^`). `ignorecase` applies the `i` flag. Writes 1/0 through
 * *out_matches on success (BJ_OK); returns a negative BJ_ERR_* on failure
 * (BJ_ERR_STATE if `pattern` doesn't parse, BJ_ERR_OOM on allocation
 * failure). The compiled pattern is cached internally (keyed on the exact
 * pattern bytes + flags) and reused across calls -- callers don't manage
 * any pattern lifetime themselves, matching the old API exactly.
 */
int rx_match(const char *pattern, int pattern_len, int ignorecase,
             const char *subject, int subject_len, int *out_matches);

#ifdef __cplusplus
}
#endif

#endif /* REGEX_H */
