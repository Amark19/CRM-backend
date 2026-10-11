import type { Pool } from 'pg';
import { HttpError } from '../api/errors';
import { withTransaction } from '../db/transaction';

export type OpportunityStatus = 'open' | 'won' | 'lost' | 'abandoned';

export interface Opportunity {
  id: number;
  workspaceId: number;
  stageId: number;
  name: string;
  valueCents: number;
  status: OpportunityStatus;
  ownerId: string;
  version: number;
  createdAt: Date;
  updatedAt: Date;
}

export interface NewOpportunity {
  stageId: number;
  name: string;
  valueCents: number;
  status: OpportunityStatus;
  ownerId: string;
}

// snake_case columns -> camelCase fields, so rows come back shaped like Opportunity.
const COLUMNS = `
  id, workspace_id AS "workspaceId", stage_id AS "stageId", name,
  value_cents AS "valueCents", status, owner_id AS "ownerId", version,
  created_at AS "createdAt", updated_at AS "updatedAt"`;

export async function createOpportunity(
  pool: Pool,
  workspaceId: number,
  input: NewOpportunity,
): Promise<Opportunity> {
  const { rows } = await pool.query<Opportunity>(
    `INSERT INTO opportunities (workspace_id, stage_id, name, value_cents, status, owner_id)
     VALUES ($1, $2, $3, $4, $5, $6)
     RETURNING ${COLUMNS}`,
    [workspaceId, input.stageId, input.name, input.valueCents, input.status, input.ownerId],
  );
  return rows[0];
}

export async function moveOpportunity(
  pool: Pool,
  workspaceId: number,
  opportunityId: number,
  toStageId: number,
  expectedVersion?: number,
): Promise<Opportunity> {
  return withTransaction(pool, async (client) => {
    // Lock the row so from_stage_id and version can't change between this read
    // and our UPDATE. A concurrent move of the same deal waits here.
    const current = await client.query<{ stageId: number; version: number }>(
      `SELECT stage_id AS "stageId", version
         FROM opportunities
        WHERE id = $1 AND workspace_id = $2
        FOR UPDATE`,
      [opportunityId, workspaceId],
    );
    if (current.rowCount === 0) throw new HttpError(404, 'opportunity not found');
    const { stageId: fromStageId, version } = current.rows[0];

    if (expectedVersion !== undefined && expectedVersion !== version) {
      throw new HttpError(409, 'opportunity was modified', { currentVersion: version });
    }

    if (fromStageId === toStageId) {
      // Already there: no stage change, so no transition. Makes retries harmless.
      const { rows } = await client.query<Opportunity>(
        `SELECT ${COLUMNS} FROM opportunities WHERE id = $1`,
        [opportunityId],
      );
      return rows[0];
    }

    const { rows } = await client.query<Opportunity>(
      `UPDATE opportunities
          SET stage_id = $3, version = version + 1, updated_at = now()
        WHERE id = $1 AND workspace_id = $2
        RETURNING ${COLUMNS}`,
      [opportunityId, workspaceId, toStageId],
    );
    await client.query(
      `INSERT INTO stage_transitions (workspace_id, opportunity_id, from_stage_id, to_stage_id, source)
       VALUES ($1, $2, $3, $4, 'manual')`,
      [workspaceId, opportunityId, fromStageId, toStageId],
    );
    return rows[0];
  });
}

export interface Page<T> {
  items: T[];
  nextCursor: number | null;
}

// Keyset pagination: "rows after id X" walks the (workspace_id, stage_id, id)
// index and stops after `limit` rows. OFFSET would scan and discard every
// skipped row, which gets slower the deeper you page into 500k rows.
export async function listOpportunitiesInStage(
  pool: Pool,
  workspaceId: number,
  stageId: number,
  limit: number,
  cursor: number,
): Promise<Page<Opportunity>> {
  // Fetch one extra row to know whether another page exists.
  const { rows } = await pool.query<Opportunity>(
    `SELECT ${COLUMNS}
       FROM opportunities
      WHERE workspace_id = $1 AND stage_id = $2 AND id > $3
      ORDER BY id
      LIMIT $4`,
    [workspaceId, stageId, cursor, limit + 1],
  );
  const hasMore = rows.length > limit;
  const items = hasMore ? rows.slice(0, limit) : rows;
  return { items, nextCursor: hasMore ? items[items.length - 1].id : null };
}
