#!/usr/bin/env bash
# Build the database SERVER -- server/main.c over this package's C
# sources, with no JavaScript in the process.
#
#   ./build/build-server.sh              wasm32-wasip2, sockets + --stdio
#   ./build/build-server.sh --native     a native binary, same sources
#   ./build/build-server.sh --wasip1     wasm32-wasip1, --stdio only
#   ./build/build-server.sh --run [args] build, then run it
#
# THREE TARGETS, ONE MAIN. The point of building all three from one file
# is the claim this whole effort rests on: the engine is C, the host is a
# detail. If server/main.c ever needs to know which of these it is beyond
# "are there sockets", something has leaked across that line.
#
# wasip2 is the deployment target (docs/db-server.md).
# It is the only wasm target with sockets: preview1 has no socket()
# at all -- not a missing right, no such function -- so the wasip1 build
# serves --stdio and says so if asked for a port.
#
# Sources come from build/build-common.sh, the same manifest the browser
# and native builds read, minus the JS-ABI adapters. The server main is
# added here rather than in the manifest for the reason test/native's is:
# a file defining main() must not enter a library build.
set -euo pipefail

cd "$(dirname "$0")/.."
. build/build-common.sh

TARGET=wasip2
RUN=0
RUN_ARGS=()
ARCH=""
SAN=""
while [ $# -gt 0 ]; do
  case "$1" in
    --native) TARGET=native; shift ;;
    --wasip1) TARGET=wasip1; shift ;;
    --wasip2) TARGET=wasip2; shift ;;
    # Sanitized SERVER builds. build-native.sh has always sanitized the
    # engine, but it links only server/root.c -- so main.c, replica.c,
    # peers.c, join.c and instns.c, which is every line of the poll loop
    # and the Raft host, have never been compiled under a sanitizer by
    # any build in this repository. The suite that exercises them hardest
    # (test/db.server.test.js) drives this binary, so pointing it at a
    # sanitized one is the cheapest real coverage available.
    #
    # Separate outputs, so a sanitized binary can never be mistaken for
    # a shipping one: they are 2-3x slower and ASan's allocator makes the
    # memory figures meaningless.
    --san)    SAN=asan;  shift ;;
    --tsan)   SAN=tsan;  shift ;;
    # macOS cross-arch: clang there compiles for either CPU with -arch,
    # which is how release CI builds both slices of a universal binary
    # on one arm64 runner (lipo joins them). Suffixes the output so the
    # two builds cannot overwrite each other. Meaningless off macOS.
    --arch)   ARCH="${2:?--arch needs an architecture (arm64|x86_64)}"; shift 2 ;;
    --run)    RUN=1; shift; RUN_ARGS=("$@"); break ;;
    *) echo "usage: $0 [--native|--wasip1|--wasip2] [--arch arm64|x86_64] [--run [args...]]" >&2; exit 2 ;;
  esac
done

require_submodules
mkdir -p build/lib

SOURCES=()
while IFS= read -r src; do SOURCES+=("$src"); done < <(all_sources native)
SOURCES+=(server/main.c server/replica.c server/peers.c server/root.c server/join.c server/instns.c server/readers.c server/writer.c server/group.c server/applied.c)

FLAGS=(-std=c11 -O2 -Wall -Wextra -Werror "${INCLUDE_FLAGS[@]}")
# Libraries go AFTER the sources: GNU ld resolves left to right and drops
# a library nothing has needed yet, so -lm among the flags links on macOS
# and leaves cos/log/floor undefined on Linux.
LIBS=()

case "$TARGET" in
  native)
    CC="${CC:-cc}"
    OUT=build/lib/nisaba-server
    FLAGS+=(-DNISABA_SOCKETS=1)
    LIBS+=(-lm)   # geo.c/textindex.c/db_session.c: cos, log, floor
    if [ -n "$ARCH" ]; then
      FLAGS+=(-arch "$ARCH")
      OUT="$OUT-$ARCH"
    fi
    case "$SAN" in
      # -O1 with frame pointers rather than -O2: the optimizer inlines
      # away the frames a report needs to name, and a report nobody can
      # read is a report nobody acts on.
      asan) FLAGS+=(-fsanitize=address,undefined -fno-sanitize-recover=all
                    -fno-omit-frame-pointer -g -O1)
            OUT="$OUT-asan" ;;
      tsan) FLAGS+=(-fsanitize=thread -fno-omit-frame-pointer -g -O1)
            OUT="$OUT-tsan" ;;
    esac
    ;;
  wasip1|wasip2)
    # No sanitizer runtime for these targets, and no threads to race.
    [ -n "$SAN" ] && { echo "error: --san/--tsan are native-only" >&2; exit 2; }
    SDK="$(find_wasi_sdk)" || { wasi_sdk_missing; exit 1; }
    warn_unpinned_wasi_sdk "$SDK"
    CC="$SDK/bin/clang"
    OUT="build/lib/nisaba-server-$TARGET.wasm"
    FLAGS+=(
      "--target=wasm32-$TARGET"
      --sysroot="$SDK/share/wasi-sysroot"
      # Keep in lockstep with build-wasm.sh and build-native.sh: the tree
      # traversals recurse to their depth caps on a corrupt file before
      # rejecting it, and wasi-sdk's default stack is far smaller.
      -Wl,-z,stack-size=1048576
    )
    # Only preview2 has sockets. Preview1 is not missing a right here --
    # it has no socket() to call.
    [ "$TARGET" = wasip2 ] && FLAGS+=(-DNISABA_SOCKETS=1)
    ;;
esac

echo "cc: $CC  (${#SOURCES[@]} sources) -> $OUT"
"$CC" "${FLAGS[@]}" -o "$OUT" "${SOURCES[@]}" ${LIBS+"${LIBS[@]}"}
echo "built $OUT ($(wc -c < "$OUT") bytes)"

if [ "$RUN" = 1 ]; then
  case "$TARGET" in
    native) exec "./$OUT" ${RUN_ARGS+"${RUN_ARGS[@]}"} ;;
    *)
      WT="$(find_wasmtime)" || {
        echo "error: no wasmtime to run it with -- ./build/get-wasmtime.sh" >&2
        exit 1
      }
      # -S inherit-network for the listener; the database directory is
      # the preopen, and the server opens "." and nothing else.
      exec "$WT" run -S inherit-network --dir "./data::." "$OUT" ${RUN_ARGS+"${RUN_ARGS[@]}"}
      ;;
  esac
fi
