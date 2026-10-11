import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { submitBulkMove } from '../src/bulk/submit';
import { createPool } from '../src/db/pool';
import { processChunk } from '../src/worker/chunk';
import { claimJob, recordFailure, releaseJob } from '../src/worker/queue';
import { Worker, type WorkerConfig } from '../src/worker/worker';
import { createWorkspace, drainQueue, seedOpportunities } from './helpers';

const pool = createPool();
afterAll(() => pool.end());
beforeEach(() => drainQueue(pool));

const fastConfig: WorkerConfig = {
  chunkSize: 20,
  rowsPerSecond: 100_000,
  leaseSeconds: 30,
  sliceMs: 10_000,
  maxAttempts: 3,
  backoffSeconds: 0,
};

async function jobFor(key: string) {
  const ws = await createWorkspace(pool);
  await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 50);
  const { job } = await submitBulkMove(pool, ws.workspaceId, key, {
    filter: { stageId: ws.stageIds[0] },
    targetStageId: ws.stageIds[1],
  });
  return { ws, job };
}

describe('queue', () => {
  // Fails without the "oldest unfinished job per workspace" condition.
  it('never runs two jobs of the same workspace at once', async () => {
    const ws = await createWorkspace(pool);
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 10);
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[1], 10);
    await submitBulkMove(pool, ws.workspaceId, 'one', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[2] });
    await submitBulkMove(pool, ws.workspaceId, 'two', { filter: { stageId: ws.stageIds[1] }, targetStageId: ws.stageIds[2] });

    const first = await claimJob(pool, 30);
    const second = await claimJob(pool, 30);

    expect(first).not.toBeNull();
    expect(second).toBeNull();
  });

  it('serves workspaces in turn', async () => {
    const a = await jobFor('a');
    const b = await jobFor('b');

    const first = (await claimJob(pool, 30))!;
    await processChunk(pool, first, 10, 30);
    await releaseJob(pool, first);
    const second = (await claimJob(pool, 30))!;

    expect(first.id).toBe(a.job.id);
    expect(second.id).toBe(b.job.id); // b was never served, so it goes before a's next slice
  });

  it('marks a job failed after repeated chunk failures and moves on', async () => {
    const { job } = await jobFor('f');
    let claimed = (await claimJob(pool, 30))!;
    for (let i = 0; i < 3; i++) {
      await recordFailure(pool, claimed, 'boom', 3, 0);
      claimed = (await claimJob(pool, 30)) ?? claimed;
    }
    const { rows } = await pool.query('SELECT status, last_error FROM bulk_jobs WHERE id = $1', [job.id]);
    expect(rows[0]).toEqual({ status: 'failed', last_error: 'boom' });
  });

  it('runs a job to completion through Worker.runOnce', async () => {
    const { job } = await jobFor('w');
    const worker = new Worker(pool, fastConfig);

    expect(await worker.runOnce()).toBe(true);

    const { rows } = await pool.query('SELECT status, moved FROM bulk_jobs WHERE id = $1', [job.id]);
    expect(rows[0]).toEqual({ status: 'completed', moved: 50 });
  });

  it('paces a job to its row rate', async () => {
    await jobFor('p');
    const worker = new Worker(pool, { ...fastConfig, chunkSize: 10, rowsPerSecond: 100 });

    const started = Date.now();
    await worker.runOnce();

    // 50 rows at 100 rows/s: at least ~400ms (the last chunk needs no wait).
    expect(Date.now() - started).toBeGreaterThanOrEqual(350);
  });
});
