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
#                                       and run it (requires $WASI_SDK,
#                                       wasi-sdk 22 or newer)
#
# --wasi is the end-to-end proof of the whole C-pushdown effort: the same
# manifest, the same sources, a different toolchain, and the harness has
# to pass unchanged. It runs the result through Node's WASI host
# (wasm/run-wasi.mjs) rather than a standalone runtime -- the artifact
# imports nothing but wasi_snapshot_preview1 either way, and every CI
# runner already has Node.
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

FLAGS=(
  -std=c11 -g -O1
  -Wall -Wextra -Werror
  "${INCLUDE_FLAGS[@]}"
  -Itest/native
)

if [ "$WASI" = 1 ]; then
  : "${WASI_SDK:?set WASI_SDK to a wasi-sdk checkout, e.g. /opt/wasi-sdk}"
  CC="$WASI_SDK/bin/clang"
  OUT="$OUT.wasm"
  FLAGS+=(
    # wasip1, not the bare "wasm32-wasi" spelling: clang has deprecated
    # that one and this build is -Werror, so it is a hard error rather
    # than a warning. Needs wasi-sdk 22 or newer, which is where the
    # versioned triples arrived.
    --target=wasm32-wasip1
    --sysroot="$WASI_SDK/share/wasi-sysroot"
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
  # a native toolchain does not.
  FLAGS+=(-lm)
  if [ "$SAN" = 1 ]; then
    FLAGS+=(-fsanitize=address,undefined -fno-sanitize-recover=all)
  fi
fi

echo "cc: $CC  (${#SOURCES[@]} sources)"
"$CC" "${FLAGS[@]}" -o "$OUT" "${SOURCES[@]}"
echo "built $OUT"

if [ "$RUN" = 1 ]; then
  if [ "$WASI" = 1 ]; then
    # Two calls rather than an array spread: macOS ships bash 3.2, where
    # "${ARR[@]}" on an empty array is an unbound-variable error under
    # `set -u` -- the same reason build-wasm.sh reads its manifests with a
    # while-read loop instead of mapfile.
    if [ "$FUZZ" = 1 ]; then
      exec node --experimental-wasi-unstable-preview1 wasm/run-wasi.mjs "$OUT" "$FUZZ_ITERS" 1
    fi
    exec node --experimental-wasi-unstable-preview1 wasm/run-wasi.mjs "$OUT"
  fi
  if [ "$FUZZ" = 1 ]; then exec "$OUT" "$FUZZ_ITERS" 1; fi
  exec "$OUT"
fi
exit 0
