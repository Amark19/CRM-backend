import { spawn, type ChildProcess } from 'child_process';
import { afterAll, expect, it } from 'vitest';
import { submitBulkMove } from '../src/bulk/submit';
import { verifyJob } from '../src/bulk/verify';
import { createPool } from '../src/db/pool';
import { createWorkspace, drainQueue, seedOpportunities } from './helpers';

const pool = createPool();
afterAll(() => pool.end());

function startWorker(): ChildProcess {
  return spawn('npx', ['tsx', 'src/worker/main.ts'], {
    env: {
      ...process.env,
      WORKER_CHUNK_SIZE: '200',
      WORKER_ROWS_PER_SECOND: '4000',
      WORKER_LEASE_SECONDS: '2',
    },
    stdio: 'ignore',
  });
}

async function progress(jobId: number) {
  const { rows } = await pool.query(
    'SELECT status, moved + skipped AS processed, total FROM bulk_jobs WHERE id = $1',
    [jobId],
  );
  return rows[0] as { status: string; processed: number; total: number };
}

async function waitFor(check: () => Promise<boolean>, timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('timed out');
}

it('finishes correctly after the worker process is SIGKILLed mid-job', async () => {
  await drainQueue(pool);
  const ws = await createWorkspace(pool);
  await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 10_000);
  const { job } = await submitBulkMove(pool, ws.workspaceId, 'kill', {
    filter: { stageId: ws.stageIds[0] },
    targetStageId: ws.stageIds[1],
  });

  const first = startWorker();
  await waitFor(async () => (await progress(job.id)).processed >= 4_000, 30_000);
  first.kill('SIGKILL');

  const atKill = await progress(job.id);
  expect(atKill.status).toBe('running');
  expect(atKill.processed).toBeLessThan(10_000);
  expect(atKill.processed % 200).toBe(0); // only whole chunks were ever committed

  const second = startWorker();
  try {
    await waitFor(async () => (await progress(job.id)).status === 'completed', 30_000);
  } finally {
    second.kill('SIGTERM');
  }
  expect(await verifyJob(pool, job.id)).toEqual([]);
});
