import { defineConfig } from 'vite';

// Bundles the index.html multi-tab demo (see src/db-worker.js) so its
// cross-folder imports (src/nisaba-wasm.js, build/lib/nisaba.wasm(.mjs),
// third_party/binjson/js/binjson.js) resolve cleanly, and so the worker's
// WASM-backed code is code-split into its own chunk rather than bundled
// with index.html's main-thread entry (which never calls ready() and
// shouldn't pay for it). Standard Vite layout: index.html + src/ at the
// project root, no root/publicDir override needed.
export default defineConfig({
  server: {
    port: 8086
  },
  preview: {
    port: 8086
  }
});
