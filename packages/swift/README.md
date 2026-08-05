# NisabaDB for Swift

The Swift embedding of nisaba-db: the same C engine the wasm build
links, compiled natively by SwiftPM, with a thin Swift layer for binjson
values, ObjectIds, and the request loop.

## Assembling

`Sources/CNisaba` is assembled, not authored. From the repository root:

```sh
./packages/swift.build.sh
```

This copies the native C source closure — `build/build-common.sh`'s
`all_sources native`, the very list `build/build-native.sh` compiles and
CI runs under ASan/UBSan, plus `server/root.c` (the POSIX directory
seam) — into the package. Re-run it after pulling changes to the C.
Nothing under `Sources/CNisaba` is a source of truth; see the script's
comment.

Then:

```sh
cd packages/swift
swift build
swift test
```

## Using

```swift
import NisabaDB

let nisaba = try Nisaba(rootPath: "/path/to/data")   // created if absent

// Requests are binjson objects in the wire shape db_request.c owns --
// the same protocol the JS server client (src/db-server-client.js)
// speaks. Every request names its database.
let id = ObjectId()
try nisaba.call(["op": "insert", "db": "app", "coll": "users",
                 "doc": ["_id": .objectId(id), "name": "ada", "age": 36],
                 "id": .objectId(id)])

let res = try nisaba.call(["op": "find", "db": "app", "coll": "users",
                           "filter": ["age": ["$gte": 21]]])
let docs = res["docs"]?.arrayValue ?? []
```

- `handle(_:)` returns every response as-is, refusals included
  (`{ok: false, code, msg}`); `call(_:)` throws them as
  `NisabaError.server`.
- Ids and the clock are the caller's: mint an `ObjectId()` for every
  insert, send `"id"` with any upsert, and `"now"` (epoch ms as an
  integer) with any update — the engine deliberately reads no clock.
- `Nisaba` is NOT thread-safe; confine it to one thread or actor.

One root directory holds many databases, each a subdirectory
(`db_instance.h`). A database created here is byte-compatible with the
same directory served by the C server or opened by the Node host.

## Layout

- `CNisaba` — the assembled C engine (binjson, the B+ tree structures,
  the document engine, the request handler, the POSIX io/namespace/root
  adapters). Swift sees it through the explicit umbrella `CNisaba.h`.
- `NisabaDB` — `BJValue` (the document model), `BinJSON` (encode/decode
  through the C codec itself), `ObjectId`, `Nisaba` (the instance).
