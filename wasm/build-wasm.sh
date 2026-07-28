#!/usr/bin/env bash
# Build the standalone nisaba WASM module: wasm/lib/nisaba.wasm +
# wasm/lib/nisaba.wasm.mjs (the ES module loader), loaded by
# wasm/nisaba-wasm.js. Mirrors the parent project's c/build-wasm.sh (same
# flags, same combined-binary shape) but links only this package's own
# sources plus its nested binjson/binjson-structures/regex-engine
# submodules -- nothing here depends on the parent repo. Requires `emcc`
# on PATH (emsdk; the committed wasm/lib artifacts were built with 5.0.7,
# which CI pins -- keep .github/workflows/ci.yml in lockstep when
# upgrading) and the submodules checked out
# (`git submodule update --init`).
set -euo pipefail

cd "$(dirname "$0")/.."
mkdir -p wasm/lib

for dep in binjson binjson-structures regex-engine; do
  if [ ! -d "third_party/$dep/include" ] && [ ! -d "third_party/$dep/src" ]; then
    echo "error: third_party/$dep submodule not checked out -- run: git submodule update --init" >&2
    exit 1
  fi
done

# Same flags as the parent project's combined build (c/build-wasm.sh) --
# see its own comment for why the stack size/overflow-check flags matter
# (the tree traversals recurse up to their depth caps on a corrupt file
# before erroring out).
COMMON_FLAGS=(
  -O3
  -flto
  -Iwasm/include
  -Ithird_party/binjson/include
  -Ithird_party/binjson-structures/include
  -Ithird_party/regex-engine/include
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

# Same export list as the parent's own combined build (c/build-wasm.sh) --
# this package's own binary needs the same full surface: binjson, the
# binjson-structures structures (bplustree, rtree, textlog, entrylog,
# diff, stemmer, textindex -- their symbol list comes from the
# submodule's own manifest, wasm/exports.txt, so it can't drift), and db
# (document collections, on top of bplustree).
STRUCT_EXPORTS=$(grep -v '^#' third_party/binjson-structures/wasm/exports.txt | grep -v '^$' | paste -sd, -)

EXPORTS='_malloc,_free,'\
'_bjw_enc_reset,_bjw_put_null,_bjw_put_bool,_bjw_put_int,_bjw_put_float,'\
'_bjw_put_date,_bjw_put_pointer,_bjw_put_string,_bjw_put_binary,_bjw_put_oid,'\
'_bjw_put_key,_bjw_begin_array,_bjw_end_array,_bjw_begin_object,_bjw_end_object,'\
'_bjw_enc_finish,_bjw_enc_ptr,_bjw_enc_size,'\
'_bjw_decode,_bjw_events_ptr,_bjw_events_len,_bjw_consumed,_bjw_value_size,'\
"$STRUCT_EXPORTS,"\
'_dcw_collection_open,_dcw_collection_free,_dcw_collection_recover,'\
'_dcw_applied_index,_dcw_set_applied_index,'\
'_dcw_collection_attach_index,_dcw_collection_add_index,_dcw_collection_remove_index,'\
'_dcw_collection_attach_text_index,_dcw_collection_add_text_index,'\
'_dcw_collection_attach_geo_index,_dcw_collection_add_geo_index,'\
'_dcw_find_by_index,'\
'_dcw_insert_one,_dcw_insert_many,_dcw_find_one,_dcw_find,_dcw_delete_one,_dcw_delete_many,'\
'_dcw_cursor_open,_dcw_cursor_next_batch,_dcw_cursor_close,'\
'_dcw_replace_one,_dcw_count,_dcw_distinct,_dcw_explain,'\
'_dcw_update_one,_dcw_update_many,'\
'_dcw_find_one_and_update,_dcw_find_one_and_replace,_dcw_find_one_and_delete,'\
'_dcw_out_new,_dcw_out_free,_dcw_out_ptr,_dcw_out_len'

# binjson-structures' sources come from its manifest (wasm/sources.txt),
# prefixed with the submodule path -- same reasoning as STRUCT_EXPORTS.
SOURCES=(third_party/binjson/src/binjson.c third_party/binjson/src/binjson_wasm.c)
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  SOURCES+=("third_party/binjson-structures/$line")
done < third_party/binjson-structures/wasm/sources.txt
SOURCES+=(
  third_party/regex-engine/src/regexp.c third_party/regex-engine/src/regex_wasm.c
  wasm/src/db_keyenc.c wasm/src/regex.c wasm/src/db_query.c wasm/src/db_update.c wasm/src/db.c wasm/src/db_wasm.c
)

emcc "${SOURCES[@]}" \
  "${COMMON_FLAGS[@]}" \
  -sEXPORT_NAME=createNisabaModule \
  -sEXPORTED_FUNCTIONS="$EXPORTS" \
  -o wasm/lib/nisaba.mjs

mv wasm/lib/nisaba.mjs wasm/lib/nisaba.wasm.mjs
echo "built wasm/lib/nisaba.wasm.mjs ($(wc -c < wasm/lib/nisaba.wasm) bytes wasm)"
