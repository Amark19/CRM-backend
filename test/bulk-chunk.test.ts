import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { submitBulkMove } from '../src/bulk/submit';
import { verifyJob } from '../src/bulk/verify';
import { createPool } from '../src/db/pool';
import { moveOpportunity } from '../src/domain/opportunities';
import { LeaseLostError, processChunk } from '../src/worker/chunk';
import { claimJob, type ClaimedJob } from '../src/worker/queue';
import { createWorkspace, drainQueue, seedOpportunities, type TestWorkspace } from './helpers';

const pool = createPool();
afterAll(() => pool.end());

let ws: TestWorkspace;
let A: number, B: number, C: number;
beforeEach(async () => {
  await drainQueue(pool);
  ws = await createWorkspace(pool);
  [A, B, C] = ws.stageIds;
});

async function submitAtoB(key = 'k') {
  return (await submitBulkMove(pool, ws.workspaceId, key, { filter: { stageId: A }, targetStageId: B })).job;
}

async function runToCompletion(job: ClaimedJob, chunkSize: number) {
  for (;;) {
    const r = await processChunk(pool, job, chunkSize, 30);
    if (r.done) return;
  }
}

async function stageOf(id: number): Promise<number> {
  const { rows } = await pool.query<{ stage_id: number }>('SELECT stage_id FROM opportunities WHERE id = $1', [id]);
  return rows[0].stage_id;
}

async function jobState(jobId: number) {
  const { rows } = await pool.query('SELECT status, moved, skipped FROM bulk_jobs WHERE id = $1', [jobId]);
  return rows[0];
}

async function expireLease(jobId: number) {
  await pool.query(`UPDATE bulk_jobs SET lease_expires_at = now() - interval '1 second' WHERE id = $1`, [jobId]);
}

describe('processChunk', () => {
  it('moves every snapshot deal once and records one transition each', async () => {
    const ids = await seedOpportunities(pool, ws.workspaceId, A, 250);
    const view = await submitAtoB();
    const job = (await claimJob(pool, 30))!;

    await runToCompletion(job, 100);

    for (const id of ids) expect(await stageOf(id)).toBe(B);
    expect(await jobState(view.id)).toEqual({ status: 'completed', moved: 250, skipped: 0 });
    expect(await verifyJob(pool, view.id)).toEqual([]);
  });

  // Fails without `o.stage_id = c.snapshot_stage_id`: the job would drag the
  // manually moved deals back from C to B.
  it('never overwrites a manual move, before or after the cursor', async () => {
    const ids = await seedOpportunities(pool, ws.workspaceId, A, 300);
    const view = await submitAtoB();
    const job = (await claimJob(pool, 30))!;

    await processChunk(pool, job, 100, 30); // ids[0..99] moved to B
    const behindCursor = ids.slice(0, 10); // already in B
    const aheadOfCursor = ids.slice(200, 210); // still in A
    for (const id of [...behindCursor, ...aheadOfCursor]) {
      await moveOpportunity(pool, ws.workspaceId, id, C);
    }
    await runToCompletion(job, 100);

    for (const id of [...behindCursor, ...aheadOfCursor]) expect(await stageOf(id)).toBe(C);
    expect(await jobState(view.id)).toMatchObject({ moved: 290, skipped: 10 });
    expect(await verifyJob(pool, view.id)).toEqual([]);
  });

  it('skips a deal that no longer matches the filter', async () => {
    const ids = await seedOpportunities(pool, ws.workspaceId, A, 5);
    const view = (
      await submitBulkMove(pool, ws.workspaceId, 'k', { filter: { stageId: A, status: 'open' }, targetStageId: B })
    ).job;
    await pool.query(`UPDATE opportunities SET status = 'lost' WHERE id = $1`, [ids[2]]);
    const job = (await claimJob(pool, 30))!;

    await runToCompletion(job, 100);

    expect(await stageOf(ids[2])).toBe(A);
    expect(await jobState(view.id)).toMatchObject({ moved: 4, skipped: 1 });
    expect(await verifyJob(pool, view.id)).toEqual([]);
  });

  // Review focus 1
  it('advances past a chunk where every deal was moved away', async () => {
    const ids = await seedOpportunities(pool, ws.workspaceId, A, 150);
    const view = await submitAtoB();
    await pool.query('UPDATE opportunities SET stage_id = $1 WHERE id = ANY($2::bigint[])', [C, ids.slice(0, 100)]);
    const job = (await claimJob(pool, 30))!;

    const first = await processChunk(pool, job, 100, 30);
    expect(first).toEqual({ processed: 100, moved: 0, done: false });
    await runToCompletion(job, 100);

    expect(await jobState(view.id)).toEqual({ status: 'completed', moved: 50, skipped: 100 });
  });

  // Review focus 2
  it('completes on the last chunk when the total is a multiple of the chunk size', async () => {
    await seedOpportunities(pool, ws.workspaceId, A, 200);
    await submitAtoB();
    const job = (await claimJob(pool, 30))!;

    expect((await processChunk(pool, job, 100, 30)).done).toBe(false);
    expect((await processChunk(pool, job, 100, 30)).done).toBe(true);
  });

  // Fails without the lease_epoch check: the zombie would write alongside the new owner.
  it('rejects a chunk from a worker whose lease was taken over', async () => {
    await seedOpportunities(pool, ws.workspaceId, A, 300);
    const view = await submitAtoB();
    const zombie = (await claimJob(pool, 30))!;
    await processChunk(pool, zombie, 100, 30);

    // The zombie freezes past its lease; another worker takes the job over.
    await expireLease(view.id);
    const successor = (await claimJob(pool, 30))!;
    expect(successor.leaseEpoch).toBe(zombie.leaseEpoch + 1);

    await expect(processChunk(pool, zombie, 100, 30)).rejects.toBeInstanceOf(LeaseLostError);
    await runToCompletion(successor, 100);

    expect(await verifyJob(pool, view.id)).toEqual([]);
    expect(await jobState(view.id)).toMatchObject({ moved: 300 });
  });

  it('resumes from the committed cursor after the lease expires', async () => {
    const ids = await seedOpportunities(pool, ws.workspaceId, A, 500);
    const view = await submitAtoB();
    const first = (await claimJob(pool, 30))!;
    await processChunk(pool, first, 100, 30);
    await processChunk(pool, first, 100, 30); // "killed" here: 200 of 500 done, lease never released

    expect(await claimJob(pool, 30)).toBeNull(); // still leased
    await expireLease(view.id);
    const resumed = (await claimJob(pool, 30))!;
    await runToCompletion(resumed, 100);

    for (const id of ids) expect(await stageOf(id)).toBe(B);
    expect(await verifyJob(pool, view.id)).toEqual([]);
  });
});
