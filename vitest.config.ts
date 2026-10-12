import { defineConfig } from 'vitest/config';

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? 'postgres://app:app@localhost:5432/opportunities_test';

// Set it here, in the main process, so globalSetup (which runs the migrations)
// targets the test database too. `test.env` alone only reaches the test files.
process.env.DATABASE_URL = TEST_DATABASE_URL;

export default defineConfig({
  test: {
    globalSetup: './test/global-setup.ts',
    testTimeout: 60_000,
    // Worker tests claim jobs from a shared queue table; running files in
    // parallel would let one file's claim pick up another file's job.
    fileParallelism: false,
    env: {
      DATABASE_URL: TEST_DATABASE_URL,
    },
  },
});
