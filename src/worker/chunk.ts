import type { Pool } from 'pg';
import { filterPredicate } from '../bulk/filter';
import { withTransaction } from '../db/transaction';
import type { ClaimedJob } from './queue';

export class LeaseLostError extends Error {
  constructor() {
    super('lease lost: another worker owns this job');
  }
}

export interface ChunkResult {
  processed: number;
  moved: number;
  done: boolean;
}

// One chunk, one statement. Everything that must agree commits together:
// the deals that moved, their transitions, the cursor and the counters.
//   $1 job id   $2 cursor   $3 chunk size   $4 target stage
//   $5 workspace   $6 filter (jsonb)   $7 lease seconds
const CHUNK_SQL = `
WITH chunk AS (
  SELECT opportunity_id, snapshot_stage_id
    FROM bulk_job_items
   WHERE job_id = $1 AND opportunity_id > $2
   ORDER BY opportunity_id
   LIMIT $3
), moved AS (
  -- The collision guard: still in the stage it was snapshotted in (no manual
  -- move since submit) AND still matches the filter. Under READ COMMITTED, a
  -- row locked by a concurrent manual move is re-checked after that move commits.
  UPDATE opportunities o
     SET stage_id = $4, version = o.version + 1, updated_at = now()
    FROM chunk c
   WHERE o.id = c.opportunity_id
     AND o.workspace_id = $5
     AND o.stage_id = c.snapshot_stage_id
     AND ${filterPredicate('o', '$6::jsonb')}
  RETURNING o.id, c.snapshot_stage_id AS from_stage_id
), transitions AS (
  INSERT INTO stage_transitions (workspace_id, opportunity_id, from_stage_id, to_stage_id, source, bulk_job_id)
  SELECT $5, id, from_stage_id, $4, 'bulk', $1 FROM moved
), counts AS (
  SELECT (SELECT count(*) FROM chunk)::int AS processed,
         (SELECT count(*) FROM moved)::int AS moved,
         (SELECT max(opportunity_id) FROM chunk) AS last_id
)
UPDATE bulk_jobs j
   SET cursor = COALESCE(c.last_id, j.cursor),
       moved = j.moved + c.moved,
       skipped = j.skipped + c.processed - c.moved,
       attempts = 0,
       last_progress_at = now(),
       lease_expires_at = now() + make_interval(secs => $7),
       status = CASE WHEN j.moved + j.skipped + c.processed >= j.total
                     THEN 'completed' ELSE j.status END::bulk_job_status,
       finished_at = CASE WHEN j.moved + j.skipped + c.processed >= j.total THEN now() END
  FROM counts c
 WHERE j.id = $1
RETURNING c.processed, c.moved, j.status = 'completed' AS done`;

export async function processChunk(
  pool: Pool,
  job: ClaimedJob,
  chunkSize: number,
  leaseSeconds: number,
): Promise<ChunkResult> {
  return withTransaction(pool, async (client) => {
    // Fencing check first, before touching any deal: lock our job row and prove
    // the lease is still ours. A worker that froze past its lease and was taken
    // over finds a newer epoch here and writes nothing.
    const fence = await client.query<{ cursor: number }>(
      `SELECT cursor FROM bulk_jobs
        WHERE id = $1 AND lease_epoch = $2 AND status = 'running'
        FOR UPDATE`,
      [job.id, job.leaseEpoch],
    );
    if (fence.rowCount === 0) throw new LeaseLostError();

    const { rows } = await client.query<ChunkResult>(CHUNK_SQL, [
      job.id,
      fence.rows[0].cursor,
      chunkSize,
      job.targetStageId,
      job.workspaceId,
      job.filter,
      leaseSeconds,
    ]);
    return rows[0];
  });
}
