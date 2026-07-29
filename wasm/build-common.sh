#!/usr/bin/env bash
# Shared source/export assembly for this package's builds. Sourced by
# build-wasm.sh (emscripten) and build-native.sh (native cc / wasi-sdk);
# not executable on its own.
#
# The point of this file is that the two targets read ONE list. A source
# added to wasm/sources.txt is compiled by both, or deliberately excluded
# by name below -- there is no way to add a file to the browser build and
# silently forget the server build.
#
# Every path it emits is relative to the repository root, and every
# function assumes the caller has already cd'd there.

# Strip comments and blank lines from a manifest, prefixing each entry.
#   read_manifest <file> [prefix]
read_manifest() {
  local file="$1" prefix="${2:-}"
  while IFS= read -r line; do
    case "$line" in ''|'#'*) continue ;; esac
    printf '%s%s\n' "$prefix" "$line"
  done < "$file"
}

require_submodules() {
  local dep
  for dep in binjson binjson-structures regex-engine; do
    if [ ! -d "third_party/$dep/include" ] && [ ! -d "third_party/$dep/src" ]; then
      echo "error: third_party/$dep submodule not checked out -- run: git submodule update --init" >&2
      exit 1
    fi
  done
}

# Sources that are the JS ABI, not logic, and so are excluded from every
# non-emscripten build. Listed explicitly rather than matched as *_wasm.c
# because that glob is wrong twice over:
#
#   - third_party/regex-engine/src/regex_wasm.c is a genuine portable
#     shim, not JS glue -- its own header says it exists "so the shim's
#     own translation unit and native callers (e.g. test/smoke.c) share
#     one declaration". The native build NEEDS it; wasm/src/regex.c
#     includes regex_wasm.h and calls straight into it.
#   - src/hostio.c matches no glob at all, but is the single file that
#     hard-codes the EM_JS bridge, and is exactly what a native host
#     replaces with its own bj_io (see its own comment, and
#     third_party/binjson-structures/test/fuzz.sh).
#
# binjson_wasm.c is excluded because it is a process-wide singleton
# encoder/decoder (g_enc/g_events) that exists only to give JS one shared
# scratch buffer. Native callers use the instance-based bj_builder_* API
# in binjson.h directly, which is also the only thread-safe option.
NATIVE_EXCLUDE=(
  third_party/binjson/src/binjson_wasm.c
  third_party/binjson-structures/src/hostio.c
  third_party/binjson-structures/src/bjfile_wasm.c
  third_party/binjson-structures/src/bplustree_wasm.c
  third_party/binjson-structures/src/keyenc_wasm.c
  third_party/binjson-structures/src/rtree_wasm.c
  third_party/binjson-structures/src/textlog_wasm.c
  third_party/binjson-structures/src/entrylog_wasm.c
  third_party/binjson-structures/src/textindex_wasm.c
  wasm/src/db_names_wasm.c
  wasm/src/db_validate_wasm.c
  wasm/src/db_wasm.c
)

# All C sources for a target, one per line.
#   all_sources wasm|native
all_sources() {
  local mode="$1" src ex excluded
  {
    echo third_party/binjson/src/binjson.c
    echo third_party/binjson/src/binjson_wasm.c
    read_manifest third_party/binjson-structures/wasm/sources.txt third_party/binjson-structures/
    echo third_party/regex-engine/src/regexp.c
    echo third_party/regex-engine/src/regex_wasm.c
    read_manifest wasm/sources.txt
  } | while IFS= read -r src; do
    if [ "$mode" = native ]; then
      excluded=0
      for ex in "${NATIVE_EXCLUDE[@]}"; do
        [ "$src" = "$ex" ] && { excluded=1; break; }
      done
      [ "$excluded" = 1 ] && continue
    fi
    printf '%s\n' "$src"
  done
}

# Include paths shared by every target.
INCLUDE_FLAGS=(
  -Iwasm/include
  -Ithird_party/binjson/include
  -Ithird_party/binjson-structures/include
  -Ithird_party/regex-engine/include
)

# The comma-joined -sEXPORTED_FUNCTIONS list (emscripten only).
wasm_exports() {
  {
    echo _malloc
    echo _free
    # binjson's codec surface. Kept inline rather than in a manifest
    # because third_party/binjson has no exports.txt of its own; if it
    # ever grows one, read it here instead.
    cat <<'EOF'
_bjw_enc_reset
_bjw_put_null
_bjw_put_bool
_bjw_put_int
_bjw_put_float
_bjw_put_date
_bjw_put_pointer
_bjw_put_string
_bjw_put_binary
_bjw_put_oid
_bjw_put_key
_bjw_begin_array
_bjw_end_array
_bjw_begin_object
_bjw_end_object
_bjw_enc_finish
_bjw_enc_ptr
_bjw_enc_size
_bjw_decode
_bjw_events_ptr
_bjw_events_len
_bjw_consumed
_bjw_value_size
EOF
    read_manifest third_party/binjson-structures/wasm/exports.txt
    read_manifest wasm/exports.txt
  } | paste -sd, -
}
