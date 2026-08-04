# Nisaba REPL (macOS)

A minimal SwiftUI shell over the embedded nisaba-db engine
([packages/swift](../swift)), speaking mongosh:

```
use app
db.users.insertOne({name: "ada", age: 36})
db.users.find({age: {$gte: 21}}).sort({name: 1}).limit(5)
db.users.updateOne({name: 'ada'}, {$set: {age: 37}})
show collections
```

## Running

```sh
./packages/swift.build.sh     # once, from the repo root: assemble the C
cd packages/macos
swift run                     # or: swift run NisabaREPL --root ~/somewhere
```

Data lives in `~/Library/Application Support/NisabaREPL/data` unless
`--root` says otherwise. Type `help` in the shell for everything it
understands; `⌘K` clears the transcript, ↑/↓ walk history.

## Shape

- `NisabaREPLKit` — everything with no window in it, so the tests
  exercise exactly what the app runs:
  - `JSONish.swift` — the relaxed argument grammar (unquoted keys,
    single quotes, `ObjectId(...)`, `ISODate(...)`, `new Date(...)`).
  - `ShellCommand.swift` — one line into one command
    (`use`/`show`/`db.<coll>.<method>(...)` with find's
    `.sort/.skip/.limit` chain).
  - `ReplSession.swift` — commands into wire requests, following
    src/db-server-client.js so the two front ends cannot disagree about
    what, say, updateOne sends; an actor, because the engine is
    single-threaded C.
  - `Renderer.swift` — responses back into mongosh-style text.
- `NisabaREPL` — the SwiftUI app: transcript, prompt, history.

Reads against a database or collection that does not exist yet answer
as "nothing there" (mongosh's experience); every other refusal is shown
as the engine said it.
