#!/usr/bin/env bash
# Fetch the pinned wasi-sdk for this host and print where it is, so that
# `./wasm/build-native.sh --wasi` -- the only check whose memory model
# resembles the one this library ships on -- runs on a developer machine
# and not just on a CI runner.
#
#   ./wasm/get-wasi-sdk.sh              fetch if missing, print the path
#   ./wasm/get-wasi-sdk.sh --dir DIR    install into DIR instead
#   ./wasm/get-wasi-sdk.sh --force      re-fetch over an existing copy
#   ./wasm/get-wasi-sdk.sh --print-url  print the asset URL and stop
#
# A no-op when a usable toolchain is already there, so it is safe to run
# before every build. The PATH is the only thing on stdout -- every
# message goes to stderr -- so a caller can say:
#
#   WASI_SDK=$(./wasm/get-wasi-sdk.sh) ./wasm/build-native.sh --wasi
#
# though it does not have to: build-native.sh looks in the same places
# this installs to (find_wasi_sdk in wasm/build-common.sh), which is why
# CI runs this script rather than its own copy of the URL. The version,
# the tag and the asset name all come from wasm/build-common.sh; nothing
# about the pin is written down here.
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

PLATFORM="$(wasi_sdk_platform)"
URL="$(wasi_sdk_url "$PLATFORM")"

if [ "$PRINT_URL" = 1 ]; then
  echo "$URL"
  exit 0
fi

# Already have one? Say which, and do nothing. An explicit --dir is
# checked on its own: it is a request for THAT copy, so finding a
# different one elsewhere would not answer it.
if [ "$FORCE" = 0 ]; then
  if [ -n "$DEST" ]; then
    if [ -x "$DEST/bin/clang" ] && [ -d "$DEST/share/wasi-sysroot" ]; then
      echo "wasi-sdk $WASI_SDK_VERSION already at $DEST" >&2
      echo "$DEST"
      exit 0
    fi
  elif FOUND="$(find_wasi_sdk)"; then
    echo "wasi-sdk $WASI_SDK_VERSION already at $FOUND" >&2
    echo "$FOUND"
    exit 0
  fi
fi

[ -n "$DEST" ] || DEST="$(wasi_sdk_home)/wasi-sdk-$WASI_SDK_VERSION"

command -v curl >/dev/null 2>&1 || {
  echo "error: curl is needed to fetch $URL" >&2
  exit 1
}

echo "fetching wasi-sdk $WASI_SDK_VERSION ($PLATFORM)" >&2
echo "  from $URL" >&2
echo "  into $DEST" >&2

# Unpack into a sibling directory and move it into place at the end, so
# an interrupted download can never leave something that LOOKS like a
# toolchain -- the discovery above would find it and every later build
# would fail somewhere further in.
TMP_TAR="$(mktemp "${TMPDIR:-/tmp}/wasi-sdk-XXXXXX.tar.gz")"
STAGE="$DEST.partial.$$"
cleanup() { rm -rf -- "$TMP_TAR" "$STAGE"; }
trap cleanup EXIT

curl -fL --progress-bar -o "$TMP_TAR" "$URL" >&2
mkdir -p "$STAGE"
tar xzf "$TMP_TAR" -C "$STAGE" --strip-components=1

if [ ! -x "$STAGE/bin/clang" ] || [ ! -d "$STAGE/share/wasi-sysroot" ]; then
  echo "error: $URL unpacked without a bin/clang and a share/wasi-sysroot" >&2
  exit 1
fi

rm -rf -- "$DEST"
mkdir -p "$(dirname "$DEST")"
mv "$STAGE" "$DEST"

echo "installed wasi-sdk $WASI_SDK_VERSION at $DEST" >&2
echo "$DEST"
