import type { Pool } from 'pg';
import { createPool } from '../db/pool';

const STAGES = [
  'New Lead', 'Contacted', 'Qualified', 'Discovery', 'Demo Scheduled', 'Proposal Sent',
  'Negotiation', 'Contract Sent', 'Verbal Commit', 'Closed Won', 'Closed Lost', 'On Hold',
];
// Uneven on purpose: early stages are crowded, late ones thin. Sums to 100.
const STAGE_WEIGHTS = [22, 18, 14, 10, 8, 7, 6, 4, 3, 4, 3, 1];

async function createWorkspace(pool: Pool, name: string, deals: number): Promise<number> {
  const ws = await pool.query<{ id: number }>('INSERT INTO workspaces (name) VALUES ($1) RETURNING id', [name]);
  const workspaceId = ws.rows[0].id;
  const stages = await pool.query<{ id: number }>(
    `INSERT INTO stages (workspace_id, name, position)
     SELECT $1, name, position FROM unnest($2::text[]) WITH ORDINALITY AS s(name, position)
     RETURNING id`,
    [workspaceId, STAGES],
  );
  const stageIds = stages.rows.map((r) => r.id).sort((a, b) => a - b);

  for (let i = 0; i < stageIds.length; i++) {
    const count = Math.round((deals * STAGE_WEIGHTS[i]) / 100);
    // One INSERT ... SELECT per stage; Postgres generates the rows itself.
    // created_at is spread over ~18 months; status is mostly open.
    await pool.query(
      `WITH g AS (
         SELECT n, random() AS r, now() - random() * interval '540 days' AS t
           FROM generate_series(1, $3) AS n
       )
       INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, status, owner_id, created_at, updated_at)
       SELECT $1, $2, 'Deal ' || n,
              (random() * 5000000)::bigint,
              (CASE WHEN r < 0.80 THEN 'open' WHEN r < 0.88 THEN 'won'
                    WHEN r < 0.96 THEN 'lost' ELSE 'abandoned' END)::opportunity_status,
              'owner-' || (1 + floor(random() * 50))::int,
              t, t
         FROM g`,
      [workspaceId, stageIds[i], count],
    );
  }
  console.log(`workspace ${workspaceId} "${name}": ${deals} deals`);
  return workspaceId;
}

async function main(): Promise<void> {
  const large = process.argv.includes('--large');
  const pool = createPool(1);
  const started = Date.now();
  // Dev tool: start from empty tables every time.
  await pool.query(
    'TRUNCATE workspaces, stages, opportunities, stage_transitions, bulk_jobs, bulk_job_items RESTART IDENTITY CASCADE',
  );

  if (large) await createWorkspace(pool, 'large', 500_000);
  for (let i = 1; i <= 5; i++) await createWorkspace(pool, `small-${i}`, 3_000);

  await pool.query('ANALYZE');
  await pool.end();
  console.log(`seeded in ${((Date.now() - started) / 1000).toFixed(1)}s`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
