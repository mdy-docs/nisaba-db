#!/usr/bin/env bash
# Shared source/export assembly for this package's builds. Sourced by
# build-wasm.sh (emscripten) and build-native.sh (native cc / wasi-sdk);
# not executable on its own.
#
# The point of this file is that the two targets read ONE list. A source
# added to engine/sources.txt is compiled by both, or deliberately excluded
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

# ---------------------------------------------------------------------------
# The pinned toolchains: where they are, and where they come from.
#
# This is the ONE place each version is written down. build-native.sh
# --wasi finds them through here, build/get-wasi-sdk.sh and
# build/get-wasmtime.sh fetch them through here, and CI runs those same
# scripts -- so a bump is one line, and a runner and a developer's machine
# cannot end up on different toolchains, which is the whole reason
# anything is pinned (a codegen change should arrive as a commit, not as a
# Tuesday).
# ---------------------------------------------------------------------------

WASI_SDK_VERSION="33.0"
WASMTIME_VERSION="47.0.2"

# macos|linux. Fails, loudly, rather than guessing for a platform these
# projects publish no asset for.
host_os() {
  case "$(uname -s)" in
    Darwin) printf 'macos\n' ;;
    Linux)  printf 'linux\n' ;;
    *) echo "error: no pinned toolchain build for $(uname -s)" >&2; return 1 ;;
  esac
}

# arm64|x86_64, canonically -- each project spells arm64 its own way, so
# the callers below translate rather than this.
host_arch() {
  local arch
  arch="$(uname -m)"
  # uname -m says x86_64 both on real Intel silicon and in an x86_64
  # shell running under Rosetta on an arm64 Mac; only proc_translated
  # tells them apart, and it is absent (not 0) everywhere else.
  if [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = 1 ]; then
    printf 'arm64\n'
    return 0
  fi
  case "$arch" in
    arm64|aarch64) printf 'arm64\n' ;;
    x86_64|amd64)  printf 'x86_64\n' ;;
    *) echo "error: no pinned toolchain build for $arch" >&2; return 1 ;;
  esac
}

# Where the fetch scripts install by default: BESIDE the repository, not
# inside it. That is where this project's other pinned toolchain (emsdk)
# already lives, it is shared by every checkout rather than re-downloaded
# per worktree, and a `git clean -xdf` cannot cost anyone a 600 MB
# download. Override with $TOOLCHAIN_HOME, or per-toolchain below.
toolchain_home() {
  printf '%s\n' "${TOOLCHAIN_HOME:-$(dirname "$PWD")}"
}

# Download and unpack a pinned toolchain into <dest>.
#
# Unpacks into a staging directory and moves it into place at the end, so
# an interrupted download can never leave something that LOOKS like a
# toolchain -- the discovery in each script would find it and every later
# build would fail somewhere further in. Compression is left to tar to
# detect, which both bsdtar and GNU tar do, because the two projects
# publish .tar.gz and .tar.xz respectively.
#   fetch_unpack <url> <dest>
fetch_unpack() {
  local url="$1" dest="$2" tmp stage
  command -v curl >/dev/null 2>&1 || { echo "error: curl is needed to fetch $url" >&2; return 1; }
  tmp="$(mktemp "${TMPDIR:-/tmp}/toolchain-XXXXXX.tar")"
  stage="$dest.partial.$$"
  # shellcheck disable=SC2064 -- expand now: $tmp/$stage are this call's
  trap "rm -rf -- '$tmp' '$stage'" EXIT
  curl -fL --progress-bar -o "$tmp" "$url" >&2
  mkdir -p "$stage"
  tar xf "$tmp" -C "$stage" --strip-components=1
  rm -rf -- "$dest"
  mkdir -p "$(dirname "$dest")"
  mv "$stage" "$dest"
  rm -f -- "$tmp"
  trap - EXIT
}

# ---- wasi-sdk ----

# The release tag and the asset name spell the version differently -- tag
# wasi-sdk-33, file wasi-sdk-33.0-<arch>-<os>.tar.gz -- so both are
# derived here from the constant above rather than written out twice.
#   wasi_sdk_url <arch>-<os>
wasi_sdk_url() {
  printf 'https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-%s/wasi-sdk-%s-%s.tar.gz\n' \
    "${WASI_SDK_VERSION%%.*}" "$WASI_SDK_VERSION" "$1"
}

# The <arch>-<os> this host needs, in wasi-sdk's naming (arm64-macos).
wasi_sdk_platform() {
  printf '%s-%s\n' "$(host_arch)" "$(host_os)"
}

wasi_sdk_home() {
  printf '%s\n' "${WASI_SDK_HOME:-$(toolchain_home)}"
}

# Every place a wasi-sdk of the pinned version might be, most deliberate
# first: an explicit $WASI_SDK, then what get-wasi-sdk.sh installs, then
# the shared location CI uses.
wasi_sdk_candidates() {
  if [ -n "${WASI_SDK:-}" ]; then printf '%s\n' "$WASI_SDK"; fi
  printf '%s\n' "$(wasi_sdk_home)/wasi-sdk-$WASI_SDK_VERSION"
  printf '%s\n' /opt/wasi-sdk
}

# Print the first candidate that is actually a wasi-sdk -- a clang and a
# sysroot, which is all this build needs from it. Nonzero and silent if
# there is none; the caller reports that, because only it knows what the
# user was trying to do.
find_wasi_sdk() {
  local dir
  # Falling through to the next candidate is right -- a stale variable in
  # a shell profile should not stop a build that has a perfectly good
  # toolchain installed -- but doing it quietly is not: the developer who
  # set it is owed the reason their choice was not used.
  if [ -n "${WASI_SDK:-}" ] &&
     { [ ! -x "$WASI_SDK/bin/clang" ] || [ ! -d "$WASI_SDK/share/wasi-sysroot" ]; }; then
    echo "warning: \$WASI_SDK=$WASI_SDK is not a wasi-sdk (no bin/clang + share/wasi-sysroot); looking elsewhere" >&2
  fi
  while IFS= read -r dir; do
    if [ -x "$dir/bin/clang" ] && [ -d "$dir/share/wasi-sysroot" ]; then
      printf '%s\n' "$dir"
      return 0
    fi
  done < <(wasi_sdk_candidates)
  return 1
}

# The version a wasi-sdk says it is: the first line of its VERSION file,
# up to the build suffix the releases carry ("33.0+m"). Empty if it has
# no such file -- old enough, or not a wasi-sdk at all.
#   wasi_sdk_version_of <dir>
wasi_sdk_version_of() {
  local line
  [ -r "$1/VERSION" ] || return 0
  IFS= read -r line < "$1/VERSION" || return 0
  printf '%s\n' "${line%%+*}"
}

# Say so when a found toolchain is not the pinned one. A warning and not
# an error: an explicit $WASI_SDK is a deliberate override and this build
# still works across nearby versions -- but CI is on the pin, so a
# difference here is the likeliest reason for a result that reproduces on
# one machine and not the other.
#   warn_unpinned_wasi_sdk <dir>
warn_unpinned_wasi_sdk() {
  local found
  found="$(wasi_sdk_version_of "$1")"
  if [ -n "$found" ] && [ "$found" != "$WASI_SDK_VERSION" ]; then
    echo "warning: $1 is wasi-sdk $found, not the pinned $WASI_SDK_VERSION (CI runs the pin)" >&2
  fi
}

# What to say when there is none. Names the version, the places looked,
# the script that fetches it and the escape hatch -- not just the
# variable that was missing, which is what this used to print.
wasi_sdk_missing() {
  local dir
  echo "error: no wasi-sdk $WASI_SDK_VERSION found (--wasi needs one; the version is pinned in build/build-common.sh)" >&2
  echo "  looked in:" >&2
  while IFS= read -r dir; do echo "    $dir" >&2; done < <(wasi_sdk_candidates)
  echo "  fetch it:  ./build/get-wasi-sdk.sh" >&2
  echo "  or point at your own:  WASI_SDK=/path/to/wasi-sdk $0 --wasi" >&2
}

# ---- wasmtime ----
#
# The SECOND WASI host. Not redundant with Node's: preview1 is
# rights-based, and the two hosts disagree about which rights a preopen
# carries -- wasmtime refuses fd_sync on a directory where Node's uvwasi
# forwards it to the real descriptor. That difference hid a durability
# bug in the POSIX adapter for as long as Node was the only host this was
# ever run under. One engine is one opinion.

wasmtime_url() {
  printf 'https://github.com/bytecodealliance/wasmtime/releases/download/v%s/wasmtime-v%s-%s.tar.xz\n' \
    "$WASMTIME_VERSION" "$WASMTIME_VERSION" "$1"
}

# wasmtime spells arm64 "aarch64", where wasi-sdk spells it "arm64" --
# the reason host_arch canonicalizes and each caller translates.
wasmtime_platform() {
  local arch
  arch="$(host_arch)"
  [ "$arch" = arm64 ] && arch=aarch64
  printf '%s-%s\n' "$arch" "$(host_os)"
}

wasmtime_home() {
  printf '%s\n' "${WASMTIME_HOME:-$(toolchain_home)}"
}

# Every place a wasmtime might be, most deliberate first: an explicit
# $WASMTIME, then what get-wasmtime.sh installs, then the shared location
# CI uses, then whatever is on PATH (a package manager's, most likely --
# hence the version warning below).
wasmtime_candidates() {
  if [ -n "${WASMTIME:-}" ]; then printf '%s\n' "$WASMTIME"; fi
  printf '%s\n' "$(wasmtime_home)/wasmtime-$WASMTIME_VERSION/wasmtime"
  printf '%s\n' /opt/wasmtime/wasmtime
  command -v wasmtime 2>/dev/null || true
}

# Print the first candidate that is an executable. Nonzero and silent if
# there is none -- unlike the wasi-sdk, this one is OPTIONAL: --wasi runs
# under Node's host either way, and a missing wasmtime costs a second
# opinion, not the check.
find_wasmtime() {
  local exe
  while IFS= read -r exe; do
    if [ -x "$exe" ]; then printf '%s\n' "$exe"; return 0; fi
  done < <(wasmtime_candidates)
  return 1
}

# Say so when the wasmtime found is not the pinned one -- most likely a
# package manager's, which moves on its own schedule. A warning, not an
# error, for the same reason as the wasi-sdk's: a deliberate override is
# allowed, but a version difference is the likeliest reason for a result
# that reproduces on one machine and not the other.
#   warn_unpinned_wasmtime <exe>
warn_unpinned_wasmtime() {
  local found
  found="$("$1" --version 2>/dev/null | awk 'NR==1 {print $2}')" || return 0
  if [ -n "$found" ] && [ "$found" != "$WASMTIME_VERSION" ]; then
    echo "warning: $1 is wasmtime $found, not the pinned $WASMTIME_VERSION (CI runs the pin)" >&2
  fi
}

# Third-party sources that are the JS ABI, not logic, and so are excluded
# from every non-emscripten build. This repository's own JS-ABI sources
# are not listed: they live in engine/jsabi/, and the loop below excludes
# that whole directory -- the split is structural, not a list to maintain.
# The suffix glob *_wasm.c would be wrong in BOTH directions:
#
#   - third_party/regex-engine/src/regex_wasm.c is a genuine portable
#     shim, not JS glue -- its own header says it exists "so the shim's
#     own translation unit and native callers (e.g. test/smoke.c) share
#     one declaration". The native build NEEDS it; engine/src/regex.c
#     includes regex_wasm.h and calls straight into it. This repo's own
#     portable flat-ABI shims (db_wal_wasm.c, raft_core/drive/msg_wasm.c)
#     live in engine/src for the same reason: every target compiles them.
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
  third_party/binjson-structures/src/bjns_bridge.c
  third_party/binjson-structures/src/bjfile_wasm.c
  third_party/binjson-structures/src/bplustree_wasm.c
  third_party/binjson-structures/src/keyenc_wasm.c
  third_party/binjson-structures/src/rtree_wasm.c
  third_party/binjson-structures/src/textlog_wasm.c
  third_party/binjson-structures/src/entrylog_wasm.c
  third_party/binjson-structures/src/textindex_wasm.c
)

# Sources only a non-emscripten build links: the POSIX bj_io/bj_ns adapter
# that stands in for hostio.c's EM_JS bridge. Its mirror image is the
# hostio.c entry in NATIVE_EXCLUDE above -- exactly one of the two is
# compiled into any given binary.
NATIVE_EXTRA=(
  third_party/binjson-structures/src/bjio_posix.c
)

# All C sources for a target, one per line.
#   all_sources wasm|native
all_sources() {
  local mode="$1" src ex excluded
  {
    echo third_party/binjson/src/binjson.c
    echo third_party/binjson/src/binjson_wasm.c
    read_manifest third_party/binjson-structures/wasm/sources.txt third_party/binjson-structures/
    # Four files, not one: the engine split regexp.c into lexer, parser,
    # compiler and VM at v0.3.0. Listed in dependency-free order -- the
    # link does not care -- and kept inline rather than in a manifest for
    # the same reason binjson's two are: this submodule ships no
    # engine/sources.txt of its own.
    echo third_party/regex-engine/src/re_lexer.c
    echo third_party/regex-engine/src/re_parser.c
    echo third_party/regex-engine/src/re_compiler.c
    echo third_party/regex-engine/src/re_vm.c
    echo third_party/regex-engine/src/regex_wasm.c
    read_manifest engine/sources.txt
  } | while IFS= read -r src; do
    if [ "$mode" = native ]; then
      # This repo's emscripten-only JS-ABI layer is a DIRECTORY, so its
      # exclusion needs no list: a new adapter lands in engine/jsabi/
      # and is out of every native build by construction.
      case "$src" in engine/jsabi/*) continue ;; esac
      excluded=0
      for ex in "${NATIVE_EXCLUDE[@]}"; do
        [ "$src" = "$ex" ] && { excluded=1; break; }
      done
      [ "$excluded" = 1 ] && continue
    fi
    printf '%s\n' "$src"
  done
  if [ "$mode" = native ]; then
    for src in "${NATIVE_EXTRA[@]}"; do printf '%s\n' "$src"; done
  fi
}

# Include paths shared by every target.
INCLUDE_FLAGS=(
  -Iengine/include
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
    read_manifest engine/jsabi/exports.txt
  } | paste -sd, -
}
