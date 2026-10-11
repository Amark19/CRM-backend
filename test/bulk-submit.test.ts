import request from 'supertest';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../src/api/app';
import { MAX_BULK_MOVE } from '../src/bulk/filter';
import { createPool } from '../src/db/pool';
import { createWorkspace, seedOpportunities, type TestWorkspace } from './helpers';

const pool = createPool();
const app = createApp(pool);
afterAll(() => pool.end());

let ws: TestWorkspace;
beforeEach(async () => {
  ws = await createWorkspace(pool);
});

function submit(workspaceId: number, key: string, body: object) {
  return request(app)
    .post('/bulk-moves')
    .set('X-Workspace-Id', String(workspaceId))
    .set('Idempotency-Key', key)
    .send(body);
}

async function count(sql: string, params: unknown[]): Promise<number> {
  const { rows } = await pool.query<{ n: number }>(sql, params);
  return rows[0].n;
}

describe('POST /bulk-moves', () => {
  it('snapshots the matching deals and returns 202 with a queued job', async () => {
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 30);
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[1], 5);

    const res = await submit(ws.workspaceId, 'k1', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[2] });

    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ status: 'queued', health: 'queued', total: 30, moved: 0, skipped: 0 });
    expect(await count('SELECT count(*)::int AS n FROM bulk_job_items WHERE job_id = $1', [res.body.id])).toBe(30);
  });

  it('replays the same job for the same key and body, without a second snapshot', async () => {
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 10);
    const body = { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] };

    const first = await submit(ws.workspaceId, 'same', body);
    const second = await submit(ws.workspaceId, 'same', body);

    expect(second.status).toBe(200);
    expect(second.headers['idempotent-replayed']).toBe('true');
    expect(second.body.id).toBe(first.body.id);
    expect(await count('SELECT count(*)::int AS n FROM bulk_jobs WHERE workspace_id = $1', [ws.workspaceId])).toBe(1);
  });

  // Fails without the unique constraint + ON CONFLICT: both requests would create a job.
  it('creates exactly one job when the same key arrives concurrently', async () => {
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 10);
    const body = { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] };

    const responses = await Promise.all(Array.from({ length: 10 }, () => submit(ws.workspaceId, 'race', body)));

    expect(new Set(responses.map((r) => r.body.id)).size).toBe(1);
    expect(responses.filter((r) => r.status === 202)).toHaveLength(1);
    expect(
      await count(
        'SELECT count(*)::int AS n FROM bulk_job_items i JOIN bulk_jobs j ON j.id = i.job_id WHERE j.workspace_id = $1',
        [ws.workspaceId],
      ),
    ).toBe(10);
  });

  it('rejects a reused key with a different body', async () => {
    await submit(ws.workspaceId, 'k', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] });
    const res = await submit(ws.workspaceId, 'k', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[2] });
    expect(res.status).toBe(422);
  });

  it('treats the same key in another workspace as a different request', async () => {
    const other = await createWorkspace(pool);
    const a = await submit(ws.workspaceId, 'shared', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] });
    const b = await submit(other.workspaceId, 'shared', { filter: { stageId: other.stageIds[0] }, targetStageId: other.stageIds[1] });
    expect(a.status).toBe(202);
    expect(b.status).toBe(202);
    expect(a.body.id).not.toBe(b.body.id);
  });

  it('requires the Idempotency-Key header', async () => {
    const res = await request(app)
      .post('/bulk-moves')
      .set('X-Workspace-Id', String(ws.workspaceId))
      .send({ filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] });
    expect(res.status).toBe(400);
  });

  // Review focus 3
  it(`rejects a filter matching more than ${MAX_BULK_MOVE} deals and leaves nothing behind`, async () => {
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], MAX_BULK_MOVE + 1);

    const tooMany = await submit(ws.workspaceId, 'big', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] });
    expect(tooMany.status).toBe(422);
    expect(await count('SELECT count(*)::int AS n FROM bulk_jobs WHERE workspace_id = $1', [ws.workspaceId])).toBe(0);

    // The key was never stored, so it can be reused with a narrower filter.
    const narrower = await submit(ws.workspaceId, 'big', {
      filter: { stageId: ws.stageIds[0], valueMaxCents: 0 },
      targetStageId: ws.stageIds[1],
    });
    expect(narrower.status).toBe(202);
  });

  // Review focus 4
  it('rejects a target equal to the source stage, or in another workspace', async () => {
    const other = await createWorkspace(pool);
    const same = await submit(ws.workspaceId, 'a', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[0] });
    const foreign = await submit(ws.workspaceId, 'b', { filter: { stageId: ws.stageIds[0] }, targetStageId: other.stageIds[0] });
    expect(same.status).toBe(422);
    expect(foreign.status).toBe(422);
  });

  it('completes immediately when nothing matches', async () => {
    const res = await submit(ws.workspaceId, 'empty', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] });
    expect(res.status).toBe(202);
    expect(res.body).toMatchObject({ status: 'completed', total: 0 });
  });
});

describe('GET /bulk-moves/:id', () => {
  // Review focus 5
  it('is not visible from another workspace', async () => {
    const other = await createWorkspace(pool);
    const job = (await submit(ws.workspaceId, 'k', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] })).body;

    const res = await request(app).get(`/bulk-moves/${job.id}`).set('X-Workspace-Id', String(other.workspaceId));

    expect(res.status).toBe(404);
  });
});

describe('job health', () => {
  async function jobWith(fields: string) {
    await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 10);
    const job = (await submit(ws.workspaceId, 'h', { filter: { stageId: ws.stageIds[0] }, targetStageId: ws.stageIds[1] })).body;
    await pool.query(`UPDATE bulk_jobs SET ${fields} WHERE id = $1`, [job.id]);
    return (await request(app).get(`/bulk-moves/${job.id}`).set('X-Workspace-Id', String(ws.workspaceId))).body;
  }

  it('reports progressing with a rate while chunks keep committing', async () => {
    const view = await jobWith(
      `status = 'running', moved = 4, started_at = now() - interval '2 seconds', last_progress_at = now()`,
    );
    expect(view.health).toBe('progressing');
    expect(view.rowsPerSecond).toBeCloseTo(2, 0);
  });

  it('reports stalled when nothing has committed for 60s', async () => {
    const view = await jobWith(
      `status = 'running', started_at = now() - interval '5 minutes', last_progress_at = now() - interval '61 seconds'`,
    );
    expect(view.health).toBe('stalled');
  });

  it('reports failed with the last error', async () => {
    const view = await jobWith(`status = 'failed', last_error = 'boom', finished_at = now()`);
    expect(view).toMatchObject({ health: 'failed', lastError: 'boom' });
  });
});
