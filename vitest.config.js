import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globals: true,
    environment: 'node',
    testTimeout: 120000,
    // --expose-gc gives tests global.gc(), needed to exercise the
    // FinalizationRegistry safety net for abandoned find() cursors
    // (test/db.compact.test.js); the test skips itself if unavailable.
    poolOptions: {
      threads: { execArgv: ['--expose-gc'] },
      forks: { execArgv: ['--expose-gc'] }
    },
    // *.browser.test.js needs a real browser (Worker/BroadcastChannel/
    // navigator.locks/OPFS) -- run those via `npm run test:browser`
    // (vitest.browser.config.js) instead. third_party submodules carry
    // their own test suites (and their own devDependencies) -- run those
    // from the submodule repos, not here.
    exclude: ['**/node_modules/**', 'test/*.browser.test.js', 'third_party/**']
  }
});
