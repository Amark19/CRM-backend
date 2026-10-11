import { Client } from 'pg';
import { DATABASE_URL, createPool } from '../db/pool';
import { Worker } from './worker';

const env = (name: string, fallback: number) => Number(process.env[name] ?? fallback);

const IDLE_POLL_MS = 2000;

async function main(): Promise<void> {
  // Separate, small pool: the worker can never use up the API's connections.
  const pool = createPool(env('WORKER_POOL_SIZE', 2));
  const worker = new Worker(pool, {
    chunkSize: env('WORKER_CHUNK_SIZE', 1000),
    rowsPerSecond: env('WORKER_ROWS_PER_SECOND', 5000),
    leaseSeconds: env('WORKER_LEASE_SECONDS', 30),
    sliceMs: env('WORKER_SLICE_MS', 2000),
    maxAttempts: env('WORKER_MAX_ATTEMPTS', 5),
    backoffSeconds: env('WORKER_BACKOFF_SECONDS', 5),
  });

  // LISTEN needs a dedicated connection that stays open; it can't be a pooled one.
  const listener = new Client({ connectionString: DATABASE_URL });
  await listener.connect();
  await listener.query('LISTEN bulk_jobs');
  let wake: (() => void) | null = null;
  listener.on('notification', () => wake?.());

  const shutdown = () => {
    console.log('worker stopping after current chunk');
    worker.stop();
    wake?.();
  };
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  console.log('worker started');
  while (!worker.isStopping) {
    const found = await worker.runOnce();
    if (!found && !worker.isStopping) {
      // Idle: sleep until a job is submitted (NOTIFY), or poll anyway, because
      // a job whose worker died sends no notification when its lease expires.
      await new Promise<void>((resolve) => {
        wake = resolve;
        setTimeout(resolve, IDLE_POLL_MS);
      });
      wake = null;
    }
  }

  await listener.end();
  await pool.end();
  console.log('worker stopped');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
