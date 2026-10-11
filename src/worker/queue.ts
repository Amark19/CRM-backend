import type { Pool } from 'pg';
import type { BulkMoveFilter } from '../bulk/filter';

export interface ClaimedJob {
  id: number;
  workspaceId: number;
  targetStageId: number;
  filter: BulkMoveFilter;
  leaseEpoch: number;
}

// Claim one runnable job:
// - unfinished, and either never leased or its lease has expired (owner died or released it);
// - only the OLDEST unfinished job in each workspace is eligible, so a workspace
//   never has two jobs running at once;
// - least recently served first, so workspaces take turns;
// - SKIP LOCKED so concurrent workers never wait on or pick the same row.
// Bumping lease_epoch hands this worker a fencing token for processChunk.
export async function claimJob(pool: Pool, leaseSeconds: number): Promise<ClaimedJob | null> {
  const { rows } = await pool.query<ClaimedJob>(
    `UPDATE bulk_jobs j
        SET status = 'running',
            lease_epoch = j.lease_epoch + 1,
            lease_expires_at = now() + make_interval(secs => $1),
            started_at = COALESCE(j.started_at, now()),
            last_progress_at = COALESCE(j.last_progress_at, now()),
            last_served_at = now()
      WHERE j.id = (
        SELECT c.id FROM bulk_jobs c
         WHERE c.status IN ('queued', 'running')
           AND (c.lease_expires_at IS NULL OR c.lease_expires_at < now())
           AND c.id = (SELECT min(x.id) FROM bulk_jobs x
                        WHERE x.workspace_id = c.workspace_id
                          AND x.status IN ('queued', 'running'))
         ORDER BY c.last_served_at NULLS FIRST, c.id
         LIMIT 1
         FOR UPDATE SKIP LOCKED)
      RETURNING j.id, j.workspace_id AS "workspaceId", j.target_stage_id AS "targetStageId",
                j.filter, j.lease_epoch AS "leaseEpoch"`,
    [leaseSeconds],
  );
  return rows[0] ?? null;
}

// Give the job back early (end of a time slice, or shutdown) so it can be
// claimed again immediately instead of waiting for the lease to run out.
export async function releaseJob(pool: Pool, job: ClaimedJob): Promise<void> {
  await pool.query(
    `UPDATE bulk_jobs SET lease_expires_at = NULL
      WHERE id = $1 AND lease_epoch = $2 AND status = 'running'`,
    [job.id, job.leaseEpoch],
  );
}

// A chunk threw something other than LeaseLostError. Count the attempt, keep
// the lease until the backoff has passed (so nobody retries immediately), and
// give up after maxAttempts. Committed chunks stay committed either way.
export async function recordFailure(
  pool: Pool,
  job: ClaimedJob,
  error: string,
  maxAttempts: number,
  backoffSeconds: number,
): Promise<void> {
  await pool.query(
    `UPDATE bulk_jobs
        SET attempts = attempts + 1,
            last_error = $3,
            status = CASE WHEN attempts + 1 >= $4 THEN 'failed' ELSE status END::bulk_job_status,
            finished_at = CASE WHEN attempts + 1 >= $4 THEN now() END,
            lease_expires_at = now() + make_interval(secs => $5)
      WHERE id = $1 AND lease_epoch = $2`,
    [job.id, job.leaseEpoch, error, maxAttempts, backoffSeconds],
  );
}
