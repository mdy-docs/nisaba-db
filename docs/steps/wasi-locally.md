# Chore: make the WASI target buildable and runnable locally

A work brief. Small, and worth doing before any of the design steps,
because it restores a check that currently only exists on a CI runner.

## Why it matters

`./wasm/build-native.sh --wasi` cross-compiles the C harness to
wasm32-wasip1 and runs it under Node's WASI host. The CI job's own
comment explains why it is not redundant with the native job:

> wasm32 has 32-bit pointers and a 1 MB stack where the native job has
> 64-bit pointers and 8 MB, so this is the only job whose memory model
> resembles the one the library actually SHIPS on. It found a regex VM
> stack frame larger than the entire browser stack on its first run — a
> corruption the native and node suites had both been passing over for as
> long as it existed.

It is also the end-to-end proof of the whole C-pushdown effort: the same
manifest, the same sources, a different toolchain, and the harness has to
pass unchanged.

Today it cannot run on a developer machine without manual setup, so every
change to the C layer is written and reviewed against two of the three
memory models and the third is discovered minutes later in CI — or, on a
branch that is not pushed often, much later than that.

## Current state

`wasm/build-native.sh` line ~75:

```sh
: "${WASI_SDK:?set WASI_SDK to a wasi-sdk checkout, e.g. /opt/wasi-sdk}"
CC="$WASI_SDK/bin/clang"
```

So it works if you have wasi-sdk and export `WASI_SDK`, and otherwise
prints one line and stops. CI does the setup by hand
(`.github/workflows/ci.yml`, the `wasi` job): downloads
`wasi-sdk-33.0-x86_64-linux.tar.gz` to `/opt/wasi-sdk` and passes
`WASI_SDK` as an env var. The pin is deliberate — "a toolchain bump that
changes codegen should arrive as a commit, not as a Tuesday."

The macOS assets exist under the same release tag and naming scheme
(verified by request):

```
https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-33/wasi-sdk-33.0-arm64-macos.tar.gz
https://github.com/WebAssembly/wasi-sdk/releases/download/wasi-sdk-33/wasi-sdk-33.0-x86_64-macos.tar.gz
```

Note the version appears twice in different forms: the tag is
`wasi-sdk-33`, the file is `wasi-sdk-33.0-`. Whatever automates this
should derive both from one constant.

## Goal

`./wasm/build-native.sh --wasi` works on a developer machine with no
manual environment setup, on macOS (both architectures) and Linux, at the
same pinned version CI uses.

**Done when** a developer who has just cloned the repository can run the
WASI suite, and the version they run is the one CI runs, with one place
to change it.

## Shape

Three pieces, in order of how much they buy:

1. **Discovery.** Before demanding `$WASI_SDK`, look in the conventional
   places: `$WASI_SDK`, then `/opt/wasi-sdk`, then something under the
   developer's own toolchain directory. This repository's emsdk lives
   outside the tree already, so follow whatever convention that
   established rather than inventing one.
2. **A fetch script.** `wasm/get-wasi-sdk.sh` (or a flag on the existing
   script) that downloads and unpacks the pinned version for the host
   platform into a gitignored location, and is a no-op if it is already
   there. Detect the platform from `uname -s` / `uname -m`; note the host
   this was written on reports `x86_64` on real Intel silicon, and an
   Apple Silicon machine running an x86_64 shell under Rosetta reports
   the same — so if you care about that case, check
   `sysctl -n sysctl.proc_translated` rather than trusting `uname -m`.
3. **One pinned version.** The tag currently appears only in
   `.github/workflows/ci.yml`. After this, CI and the local script should
   read it from the same place, so a bump is one edit and cannot leave
   them disagreeing — which would defeat the point of running it locally
   at all.

A better failure message is the minimum acceptable outcome if the rest is
deferred: name the version, the URL and the script that fetches it, not
just the variable that was missing.

## Verification

```
./wasm/build-native.sh --wasi                 # the harness, on wasm32-wasip1
./wasm/build-native.sh --wasi --fuzz 20000    # the structures' hostile-file fuzz
```

Both are what the CI job runs. They must pass on a clean checkout after
whatever setup step this brief produces, and the setup step must be
documented where someone will find it — `wasm/build-native.sh`'s header
comment is the natural home, since that is where the `--wasi` flag is
already explained.

Do not weaken the pin to make this easier. A floating toolchain that
silently changes codegen is worse than one that has to be installed.

## Out of scope

Running the WASI target in CI differently, or adding new WASI-specific
tests. The harness that runs there is the same `test/native/main.c` the
native job runs; this brief is about being able to run it, not about
changing it.
