import type { Pool } from 'pg';

export interface TestWorkspace {
  workspaceId: number;
  stageIds: number[];
}

// Each test gets its own workspace, so tests never see each other's rows
// and nothing needs truncating between them.
export async function createWorkspace(pool: Pool, stageCount = 3): Promise<TestWorkspace> {
  const ws = await pool.query<{ id: number }>(
    `INSERT INTO workspaces (name) VALUES ('test') RETURNING id`,
  );
  const workspaceId = ws.rows[0].id;
  const stages = await pool.query<{ id: number }>(
    `INSERT INTO stages (workspace_id, name, position)
     SELECT $1, 'Stage ' || p, p FROM generate_series(1, $2) AS p
     RETURNING id`,
    [workspaceId, stageCount],
  );
  // Identity ids follow insertion order, so sorting gives stages in position order.
  return { workspaceId, stageIds: stages.rows.map((r) => r.id).sort((a, b) => a - b) };
}

// Inserts `count` deals into one stage in a single statement; returns their ids in order.
export async function seedOpportunities(
  pool: Pool,
  workspaceId: number,
  stageId: number,
  count: number,
  overrides: { ownerId?: string; status?: string; valueCents?: number } = {},
): Promise<number[]> {
  const { rows } = await pool.query<{ id: number }>(
    `INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, status, owner_id)
     SELECT $1, $2, 'Deal ' || n, $4, $5::opportunity_status, $6
       FROM generate_series(1, $3) AS n
     RETURNING id`,
    [
      workspaceId,
      stageId,
      count,
      overrides.valueCents ?? 10_000,
      overrides.status ?? 'open',
      overrides.ownerId ?? 'owner-1',
    ],
  );
  return rows.map((r) => r.id).sort((a, b) => a - b);
}

// Worker tests share one queue table. Retire every unfinished job so a test
// only ever claims the jobs it created itself.
export async function drainQueue(pool: Pool): Promise<void> {
  await pool.query(
    `UPDATE bulk_jobs SET status = 'failed', last_error = 'test cleanup', finished_at = now()
      WHERE status IN ('queued', 'running')`,
  );
}
