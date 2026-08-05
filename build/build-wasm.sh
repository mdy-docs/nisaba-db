#!/usr/bin/env bash
# Build the standalone nisaba WASM module: build/lib/nisaba.wasm +
# build/lib/nisaba.wasm.mjs (the ES module loader), loaded by
# wasm/nisaba-wasm.js. Mirrors the parent project's c/build-wasm.sh (same
# flags, same combined-binary shape) but links only this package's own
# sources plus its nested binjson/binjson-structures/regex-engine
# submodules -- nothing here depends on the parent repo. Requires `emcc`
# on PATH (emsdk; the committed build/lib artifacts were built with 5.0.7,
# which CI pins -- keep .github/workflows/ci.yml in lockstep when
# upgrading) and the submodules checked out
# (`git submodule update --init`).
#
# The source and export lists live in engine/sources.txt and
# engine/jsabi/exports.txt, read via build/build-common.sh, which is the same
# file build/build-native.sh reads -- so the browser and server targets
# cannot drift about what this package contains.
set -euo pipefail

cd "$(dirname "$0")/.."
. build/build-common.sh

mkdir -p build/lib
require_submodules

# See the parent project's c/build-wasm.sh for why the stack size/overflow
# check flags matter (the tree traversals recurse up to their depth caps on
# a corrupt file before erroring out). ALLOW_TABLE_GROWTH=0 is deliberate
# and load-bearing, not a default: it means no JS function pointer can ever
# be added to the table, which is why the host seams (bj_io, and bj_ns when
# it lands) are plain vtables of C function pointers with no JS callbacks.
COMMON_FLAGS=(
  -O3
  -flto
  "${INCLUDE_FLAGS[@]}"
  -sMODULARIZE=1
  -sEXPORT_ES6=1
  -sALLOW_MEMORY_GROWTH=1
  -sSTACK_SIZE=1048576
  -sSTACK_OVERFLOW_CHECK=1
  -sENVIRONMENT=web,worker,node
  -sEXPORTED_RUNTIME_METHODS=HEAPU8
  -sALLOW_TABLE_GROWTH=0
  -sFILESYSTEM=0
  --no-entry
)

# Portable read-into-array: macOS ships bash 3.2, which has no `mapfile`.
SOURCES=()
while IFS= read -r src; do SOURCES+=("$src"); done < <(all_sources wasm)

emcc "${SOURCES[@]}" \
  "${COMMON_FLAGS[@]}" \
  -sEXPORT_NAME=createNisabaModule \
  -sEXPORTED_FUNCTIONS="$(wasm_exports)" \
  -o build/lib/nisaba.mjs

mv build/lib/nisaba.mjs build/lib/nisaba.wasm.mjs
echo "built build/lib/nisaba.wasm.mjs ($(wc -c < build/lib/nisaba.wasm) bytes wasm)"
