#!/usr/bin/env bash
# Build (and by default run) the native C test harness in test/native/ --
# the same C sources the browser build links, compiled by a plain
# compiler, with no emscripten, no WASM and no JavaScript in the process.
#
#   ./wasm/build-native.sh              build + run under ASan/UBSan
#   ./wasm/build-native.sh --no-run     build only
#   ./wasm/build-native.sh --no-san     build + run without sanitizers
#   ./wasm/build-native.sh --fuzz [n]   run the structures' hostile-file
#                                       fuzz harness instead
#   ./wasm/build-native.sh --wasi       cross-compile to wasm32-wasip1
#                                       and run it
#
# --wasi is the end-to-end proof of the whole C-pushdown effort: the same
# manifest, the same sources, a different toolchain, and the harness has
# to pass unchanged. It runs the result through Node's WASI host
# (wasm/run-wasi.mjs) rather than a standalone runtime -- the artifact
# imports nothing but wasi_snapshot_preview1 either way, and every CI
# runner already has Node.
#
# It needs a wasi-sdk, at the version pinned in wasm/build-common.sh. To
# get one:
#
#   ./wasm/get-wasi-sdk.sh      # fetches the pin, beside this repository
#
# and then --wasi finds it with no environment set up, along with an
# explicit $WASI_SDK or a copy in /opt/wasi-sdk (which is where CI's own
# run of that same script puts it). Run it once; it is a no-op after
# that. Do not reach for a wasi-sdk from a package manager instead: an
# unpinned toolchain that silently changes codegen is the thing the pin
# exists to prevent.
#
# --fuzz builds third_party/binjson-structures/test/fuzz.c against THIS
# repo's checkouts. That submodule ships its own test/fuzz.sh, but it
# resolves binjson through a nested submodule of its own, which a
# consumer's checkout does not have -- so running it from here is the
# only way this repo's CI gets fuzz coverage of the structures it links.
#
# The source list comes from wasm/build-common.sh -- the same file
# build-wasm.sh reads -- minus the JS-ABI adapters and hostio.c, whose
# job this build does with test/native/memfs.c instead. That shared
# manifest is the point: a C source cannot enter the browser build
# without also entering this one.
set -euo pipefail

cd "$(dirname "$0")/.."
. wasm/build-common.sh

RUN=1
SAN=1
WASI=0
FUZZ=0
FUZZ_ITERS=20000
for arg in "$@"; do
  case "$arg" in
    --no-run) RUN=0 ;;
    --no-san) SAN=0 ;;
    --fuzz)   FUZZ=1 ;;
    --wasi)   WASI=1; SAN=0 ;;
    [0-9]*)   FUZZ_ITERS="$arg" ;;
    *) echo "usage: $0 [--no-run] [--no-san] [--fuzz [iters]] [--wasi]" >&2; exit 2 ;;
  esac
done

require_submodules

OUT="${TMPDIR:-/tmp}/nisaba-native-test"

SOURCES=()
while IFS= read -r src; do SOURCES+=("$src"); done < <(all_sources native)
if [ "$FUZZ" = 1 ]; then
  OUT="${TMPDIR:-/tmp}/nisaba-native-fuzz"
  SOURCES+=(third_party/binjson-structures/test/fuzz.c)
else
  SOURCES+=(test/native/memfs.c test/native/nscheck.c test/native/main.c)
fi

LIBS=()
FLAGS=(
  -std=c11 -g -O1
  -Wall -Wextra -Werror
  "${INCLUDE_FLAGS[@]}"
  -Itest/native
)

if [ "$WASI" = 1 ]; then
  # Discovery, not an env var the caller has to know about: $WASI_SDK if
  # it is set, then what ./wasm/get-wasi-sdk.sh installs, then the shared
  # location CI uses. All three, and the pinned version they must be,
  # are wasm/build-common.sh's.
  #
  # Into its own name first: assigning straight to WASI_SDK would clear
  # the caller's value on the failing path, and the message below is
  # supposed to show every place that was looked -- theirs included.
  SDK="$(find_wasi_sdk)" || { wasi_sdk_missing; exit 1; }
  warn_unpinned_wasi_sdk "$SDK"
  CC="$SDK/bin/clang"
  OUT="$OUT.wasm"
  FLAGS+=(
    # wasip1, not the bare "wasm32-wasi" spelling: clang has deprecated
    # that one and this build is -Werror, so it is a hard error rather
    # than a warning. Needs wasi-sdk 22 or newer, which is where the
    # versioned triples arrived.
    --target=wasm32-wasip1
    --sysroot="$SDK/share/wasi-sysroot"
    # Not optional. emcc sets -sSTACK_SIZE=1048576 because the tree
    # traversals recurse up to their depth caps (BJ_MAX_DEPTH) on a
    # corrupt file before erroring out; wasi-sdk's default stack is far
    # smaller, so hostile input smashes the stack instead of being
    # rejected. Keep this in lockstep with build-wasm.sh's value.
    -Wl,-z,stack-size=1048576
    # NOT -DBJIO_REQUIRE_SYNC, which this build used to set on the
    # reasoning that a WASI artifact backs its files with real
    # descriptors, so a writable bj_io with no sync callback is a
    # durability bug. True of a shipping artifact; false of THIS binary,
    # which is the test harness, and most of whose tests run on memfs --
    # where a NULL sync is exactly what bjio.h says it should be. The
    # flag belongs on the server binary when there is one, not here.
    #
    # Setting it here found something anyway, which is why the story is
    # worth keeping: bjio_check duly refused every memfs io, twelve of
    # the thirteen bjfile_init call sites ignored the refusal, and the
    # structures carried on with a zeroed vtable until the first write
    # trapped. See bjfile.h.
  )
else
  CC="${CC:-cc}"
  # geo.c uses sin/cos/atan2/sqrt; emscripten links libm implicitly,
  # a native toolchain does not. AFTER the sources on the command line:
  # GNU ld resolves left to right and drops a library nothing has asked
  # for yet, so -lm in FLAGS links on macOS and fails on Linux with
  # --no-san (with sanitizers it survives, because their runtime pulls
  # libm in behind the objects). build-server.sh hit exactly that.
  LIBS+=(-lm)
  if [ "$SAN" = 1 ]; then
    FLAGS+=(-fsanitize=address,undefined -fno-sanitize-recover=all)
  fi
fi

echo "cc: $CC  (${#SOURCES[@]} sources)"
"$CC" "${FLAGS[@]}" -o "$OUT" "${SOURCES[@]}" ${LIBS+"${LIBS[@]}"}
echo "built $OUT"

if [ "$RUN" = 1 ]; then
  if [ "$WASI" = 1 ]; then
    # Positional parameters rather than an array: macOS ships bash 3.2,
    # where "${ARR[@]}" on an empty array is an unbound-variable error
    # under `set -u` -- the same reason build-wasm.sh reads its manifests
    # with a while-read loop instead of mapfile. "$@" is safe when empty.
    if [ "$FUZZ" = 1 ]; then set -- "$FUZZ_ITERS" 1; else set --; fi
    RC=0

    # Node's WASI host: always, because every runner and every developer
    # machine already has one. See wasm/run-wasi.mjs for why this is Node
    # and not a runtime that would have to be pinned and installed.
    echo "run: node WASI host"
    node --experimental-wasi-unstable-preview1 wasm/run-wasi.mjs "$OUT" "$@" || RC=1

    # And wasmtime, when there is one. A second host is not a second copy
    # of the same check: preview1 is rights-based and the two disagree
    # about what a preopened directory carries, which is how a durability
    # bug in the POSIX adapter stayed hidden for as long as Node was the
    # only host this ever ran under. Optional, and it says when it skips.
    if WASMTIME_EXE="$(find_wasmtime)"; then
      warn_unpinned_wasmtime "$WASMTIME_EXE"
      echo "run: wasmtime ($WASMTIME_EXE)"
      # One preopened directory mapped to ".", created fresh and removed
      # afterwards -- exactly what run-wasi.mjs gives the Node host, so
      # the two runs differ in engine and in nothing else.
      WT_SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/nisaba-wasmtime-XXXXXX")"
      "$WASMTIME_EXE" run --dir "$WT_SCRATCH::." "$OUT" "$@" || RC=1
      rm -rf -- "$WT_SCRATCH"
    else
      echo "skip: wasmtime (./wasm/get-wasmtime.sh installs the pinned one)"
    fi
    exit "$RC"
  fi
  if [ "$FUZZ" = 1 ]; then exec "$OUT" "$FUZZ_ITERS" 1; fi
  exec "$OUT"
fi
exit 0
