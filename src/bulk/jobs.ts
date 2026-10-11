import type { Pool, PoolClient } from 'pg';
import type { BulkMoveFilter } from './filter';

export type BulkJobStatus = 'queued' | 'running' | 'completed' | 'failed';
export type BulkJobHealth = 'queued' | 'progressing' | 'stalled' | 'completed' | 'failed';

export interface BulkJobView {
  id: number;
  workspaceId: number;
  status: BulkJobStatus;
  health: BulkJobHealth;
  targetStageId: number;
  filter: BulkMoveFilter;
  total: number;
  moved: number;
  skipped: number;
  processed: number;
  percent: number;
  rowsPerSecond: number | null;
  createdAt: Date;
  startedAt: Date | null;
  lastProgressAt: Date | null;
  finishedAt: Date | null;
  lastError: string | null;
}

interface JobRow extends Omit<BulkJobView, 'health' | 'processed' | 'percent' | 'rowsPerSecond'> {
  secondsSinceProgress: number | null;
  activeSeconds: number | null;
}

// No committed chunk for this long means the job is stuck, not slow: a healthy
// job commits a chunk every few hundred ms, and a job waiting for its next
// turn between slices waits seconds, not a minute.
export const STALL_AFTER_SECONDS = 60;

// Times are computed by Postgres (now()), so every API instance agrees on them.
const JOB_COLUMNS = `
  id, workspace_id AS "workspaceId", status, target_stage_id AS "targetStageId", filter,
  total, moved, skipped, created_at AS "createdAt", started_at AS "startedAt",
  last_progress_at AS "lastProgressAt", finished_at AS "finishedAt", last_error AS "lastError",
  EXTRACT(EPOCH FROM now() - COALESCE(last_progress_at, started_at))::float8 AS "secondsSinceProgress",
  EXTRACT(EPOCH FROM COALESCE(last_progress_at, now()) - started_at)::float8 AS "activeSeconds"`;

export function toView(row: JobRow): BulkJobView {
  const { secondsSinceProgress, activeSeconds, ...job } = row;
  const processed = job.moved + job.skipped;

  let health: BulkJobHealth;
  if (job.status === 'completed' || job.status === 'failed') health = job.status;
  else if (job.startedAt === null) health = 'queued';
  else if ((secondsSinceProgress ?? 0) > STALL_AFTER_SECONDS) health = 'stalled';
  else health = 'progressing';

  return {
    ...job,
    health,
    processed,
    percent: job.total === 0 ? 100 : Math.floor((processed / job.total) * 100),
    // Average over the job's active time; "slow" is a progressing job with a low rate.
    rowsPerSecond:
      activeSeconds && activeSeconds > 0 ? Math.round((processed / activeSeconds) * 10) / 10 : null,
  };
}

export async function getJobView(
  db: Pool | PoolClient,
  workspaceId: number,
  jobId: number,
): Promise<BulkJobView | null> {
  const { rows } = await db.query<JobRow>(
    `SELECT ${JOB_COLUMNS} FROM bulk_jobs WHERE id = $1 AND workspace_id = $2`,
    [jobId, workspaceId],
  );
  return rows[0] ? toView(rows[0]) : null;
}
