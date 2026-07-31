#!/usr/bin/env bash
# Fetch the pinned wasmtime for this host and print where it is, so that
# `./wasm/build-native.sh --wasi` can run the harness under a SECOND WASI
# host rather than only Node's.
#
#   ./wasm/get-wasmtime.sh              fetch if missing, print the path
#   ./wasm/get-wasmtime.sh --dir DIR    install into DIR instead
#   ./wasm/get-wasmtime.sh --force      re-fetch over an existing copy
#   ./wasm/get-wasmtime.sh --print-url  print the asset URL and stop
#
# Optional, unlike its wasi-sdk sibling: without a wasmtime, --wasi still
# runs under Node's host and says it skipped the second one. With one, it
# runs both, which is how a rights-based disagreement between hosts gets
# noticed -- see the wasmtime section of wasm/build-common.sh, where the
# version and the URL come from.
#
# Everything else here mirrors wasm/get-wasi-sdk.sh: no-op when a usable
# copy is already there, path on stdout and messages on stderr, so
#
#   WASMTIME=$(./wasm/get-wasmtime.sh) ./wasm/build-native.sh --wasi
#
# works, though it does not have to -- build-native.sh looks in the same
# places this installs to.
set -euo pipefail

cd "$(dirname "$0")/.."
. wasm/build-common.sh

DEST=""
FORCE=0
PRINT_URL=0
while [ $# -gt 0 ]; do
  case "$1" in
    --dir)       DEST="${2:?--dir needs a path}"; shift 2 ;;
    --force)     FORCE=1; shift ;;
    --print-url) PRINT_URL=1; shift ;;
    *) echo "usage: $0 [--dir DIR] [--force] [--print-url]" >&2; exit 2 ;;
  esac
done

PLATFORM="$(wasmtime_platform)"
URL="$(wasmtime_url "$PLATFORM")"

if [ "$PRINT_URL" = 1 ]; then
  echo "$URL"
  exit 0
fi

# An explicit --dir is checked on its own: it is a request for THAT copy,
# so finding a different one on PATH would not answer it.
if [ "$FORCE" = 0 ]; then
  if [ -n "$DEST" ]; then
    if [ -x "$DEST/wasmtime" ]; then
      echo "wasmtime already at $DEST/wasmtime" >&2
      echo "$DEST/wasmtime"
      exit 0
    fi
  elif FOUND="$(find_wasmtime)"; then
    warn_unpinned_wasmtime "$FOUND"
    echo "wasmtime already at $FOUND" >&2
    echo "$FOUND"
    exit 0
  fi
fi

[ -n "$DEST" ] || DEST="$(wasmtime_home)/wasmtime-$WASMTIME_VERSION"

echo "fetching wasmtime $WASMTIME_VERSION ($PLATFORM)" >&2
echo "  from $URL" >&2
echo "  into $DEST" >&2

fetch_unpack "$URL" "$DEST"

if [ ! -x "$DEST/wasmtime" ]; then
  echo "error: $URL unpacked without a wasmtime executable" >&2
  exit 1
fi

echo "installed wasmtime $WASMTIME_VERSION at $DEST/wasmtime" >&2
echo "$DEST/wasmtime"
