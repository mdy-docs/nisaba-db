nisaba-server for Windows
=========================

This bundle is the nisaba database server as a wasm32-wasip2
WebAssembly component (nisaba-server-wasip2.wasm) plus the wasmtime
runtime that executes it, at the exact version the repository pins and
tests against. nisaba-server.cmd ties them together:

    cd C:\path\to\your\data
    C:\path\to\this\bundle\nisaba-server.cmd --port 8097

The directory you run it FROM is the database root -- the server owns
that directory and everything beneath it, and opens nothing else.

Why not a native .exe? The server is deliberately one C source over
POSIX (poll, BSD sockets, openat); wasip2 is its documented deployment
target, and wasmtime is how that target runs on every OS, Windows
included. Same artifact, same wire, same on-disk format as the native
Linux and macOS builds -- a database directory moves freely between
them.

The wire it serves and every flag it takes: docs/db-server.md in the
repository (https://github.com/mdy-docs/nisaba-db).
