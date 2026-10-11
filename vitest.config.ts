import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    globalSetup: './test/global-setup.ts',
    testTimeout: 60_000,
    // Worker tests claim jobs from a shared queue table; running files in
    // parallel would let one file's claim pick up another file's job.
    fileParallelism: false,
    env: {
      DATABASE_URL:
        process.env.TEST_DATABASE_URL ?? 'postgres://app:app@localhost:5432/opportunities_test',
    },
  },
});
