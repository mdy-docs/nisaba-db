/*
 * run-wasi.mjs — run a wasm32-wasip1 binary under Node's WASI host.
 *
 *   node --experimental-wasi-unstable-preview1 build/run-wasi.mjs <file.wasm>
 *
 * Used by build/build-native.sh --wasi to execute the native C harness on
 * the WASI target, which is how this repo finds out whether "the same C
 * sources, built twice" is actually true rather than merely plausible.
 *
 * Node rather than wasmtime deliberately. The WASI build's whole claim is
 * that it needs no Node -- but the CI job that PROVES it may use whatever
 * is already installed, and every runner here already has Node. A
 * standalone runtime is one more thing to pin, install and keep current
 * for no additional signal: the binary under test imports nothing but
 * wasi_snapshot_preview1 either way.
 *
 * The harness writes real files (it is exercising bjio_posix over a real
 * filesystem, which is the point), so it gets one preopened directory and
 * nothing else. That is a sharper test than the native build gets: under
 * WASI a path outside the preopen cannot be reached at all, so anything
 * that reaches for an absolute path fails loudly here.
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import process from 'node:process';

// node:wasi is permanently experimental as far as this script is
// concerned -- that is the API, and the warning it prints on every single
// run is four lines of noise in every CI log and every local build. Drop
// that ONE warning and keep the rest: replacing Node's default printer
// rather than passing --disable-warning=ExperimentalWarning, which needs
// Node >= 21.3 and would turn an old-but-working Node into a hard error
// on an unknown flag. A deprecation or an unhandled rejection still
// prints, because those would be about this repo's own code.
process.removeAllListeners('warning');
process.on('warning', (w) => {
  if (w.name !== 'ExperimentalWarning') console.error(w.stack || String(w));
});

// Imported dynamically, and that is load-bearing: the warning fires when
// node:wasi is EVALUATED, and a static import is hoisted above every
// statement in this file -- including the filter above, which would then
// be installed a moment too late to catch it.
const { WASI } = await import('node:wasi');

const wasmPath = process.argv[2];
if (!wasmPath) {
  console.error('usage: run-wasi.mjs <file.wasm> [args...]');
  process.exit(2);
}

// One preopened directory, mapped to the guest as "." -- see SCRATCH_BASE
// in test/native/main.c. Removed afterwards however the run ends, so a
// failing run does not leave a half-built database behind to confuse the
// next one.
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'nisaba-wasi-'));

let code = 1;
try {
  const wasi = new WASI({
    version: 'preview1',
    args: [path.basename(wasmPath), ...process.argv.slice(3)],
    env: {},
    preopens: { '.': scratch },
    returnOnExit: true,
  });
  const module = await WebAssembly.compile(fs.readFileSync(wasmPath));
  const instance = await WebAssembly.instantiate(module, wasi.getImportObject());
  code = wasi.start(instance);
} finally {
  fs.rmSync(scratch, { recursive: true, force: true });
}
process.exit(code);
