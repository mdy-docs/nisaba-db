@echo off
rem nisaba-server.cmd -- the Windows face of the wasip2 artifact: the
rem server as a WebAssembly component, run by the wasmtime beside this
rem script. The current directory becomes the database root (the server
rem opens "." and nothing else), exactly as the native binary treats
rem its working directory on the other platforms.
rem
rem   nisaba-server --port 8097
rem   nisaba-server --port 9001 --raft 1 --raft-port 9101 ...
rem
rem docs/db-server.md documents every flag; README.txt in this bundle
rem says why this is wasmtime and not a .exe.
"%~dp0wasmtime.exe" run -S inherit-network --dir .::. "%~dp0nisaba-server-wasip2.wasm" %*
