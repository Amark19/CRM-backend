import { afterAll, describe, expect, it } from 'vitest';
import { filterPredicate, type BulkMoveFilter } from '../src/bulk/filter';
import { createPool } from '../src/db/pool';
import { createWorkspace, seedOpportunities } from './helpers';

const pool = createPool();
afterAll(() => pool.end());

async function matching(workspaceId: number, filter: BulkMoveFilter): Promise<number[]> {
  const { rows } = await pool.query<{ id: number }>(
    `SELECT o.id FROM opportunities o
      WHERE o.workspace_id = $1 AND ${filterPredicate('o', '$2::jsonb')}
      ORDER BY o.id`,
    [workspaceId, filter],
  );
  return rows.map((r) => r.id);
}

describe('filterPredicate', () => {
  it('applies each criterion and ignores absent ones', async () => {
    const ws = await createWorkspace(pool);
    const [a] = await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 1, { ownerId: 'priya', valueCents: 500 });
    const [b] = await seedOpportunities(pool, ws.workspaceId, ws.stageIds[0], 1, { ownerId: 'rahul', valueCents: 5_000 });
    const [c] = await seedOpportunities(pool, ws.workspaceId, ws.stageIds[1], 1, { ownerId: 'priya', status: 'lost' });

    expect(await matching(ws.workspaceId, { stageId: ws.stageIds[0] })).toEqual([a, b]);
    expect(await matching(ws.workspaceId, { ownerId: 'priya' })).toEqual([a, c]);
    expect(await matching(ws.workspaceId, { status: 'lost' })).toEqual([c]);
    expect(await matching(ws.workspaceId, { valueMinCents: 1_000, valueMaxCents: 9_000 })).toEqual([b]);
    expect(
      await matching(ws.workspaceId, { createdFrom: '2000-01-01T00:00:00Z', createdTo: '2001-01-01T00:00:00Z' }),
    ).toEqual([]);
  });
});
