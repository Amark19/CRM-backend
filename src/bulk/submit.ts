import {createHash} from 'crypto';
import type {Pool} from 'pg';
import {HttpError} from '../api/errors';
import {withTransaction} from '../db/transaction';
import {MAX_BULK_MOVE, filterPredicate, type BulkMoveFilter} from './filter';
import {getJobView, type BulkJobView} from './jobs';

export interface BulkMoveRequest {
    filter: BulkMoveFilter;
    targetStageId: number;
}

// Key order must not change the hash: {a,b} and {b,a} are the same request.
function canonicalJson(value: unknown): string {
    if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
    if (value && typeof value === 'object') {
        const record = value as Record<string, unknown>;
        const entries = Object.keys(record)
            .sort()
            .map((k) => `${JSON.stringify(k)}:${canonicalJson(record[k])}`);
        return `{${entries.join(',')}}`;
    }
    return JSON.stringify(value);
}

export async function submitBulkMove(
    pool: Pool,
    workspaceId: number,
    idempotencyKey: string,
    request: BulkMoveRequest,
): Promise<{ job: BulkJobView; replayed: boolean }> {
    if (request.filter.stageId === request.targetStageId) {
        throw new HttpError(422, 'target stage is the same as the filter stage');
    }
    const requestHash = createHash('sha256').update(canonicalJson(request)).digest('hex');

    return withTransaction(pool, async (client) => {
        // If another request with this key is mid-flight, this INSERT waits for it
        // to commit and then does nothing, so concurrent duplicates collapse to one job.
        const inserted = await client.query<{ id: number }>(
            `INSERT INTO bulk_jobs (workspace_id, idempotency_key, request_hash, filter, target_stage_id)
             VALUES ($1, $2, $3, $4, $5) ON CONFLICT (workspace_id, idempotency_key) DO NOTHING
       RETURNING id`,
            [workspaceId, idempotencyKey, requestHash, request.filter, request.targetStageId],
        );

        if (inserted.rowCount === 0) {
            const existing = await client.query<{ id: number; requestHash: string }>(
                `SELECT id, request_hash AS "requestHash"
                 FROM bulk_jobs
                 WHERE workspace_id = $1
                   AND idempotency_key = $2`,
                [workspaceId, idempotencyKey],
            );
            if (existing.rows[0].requestHash !== requestHash) {
                throw new HttpError(422, 'Idempotency-Key was already used with a different request');
            }
            return {job: (await getJobView(client, workspaceId, existing.rows[0].id))!, replayed: true};
        }

        const jobId = inserted.rows[0].id;

        // The snapshot: freeze the matching ids and each deal's current stage.
        // LIMIT one over the cap so "too many" is detectable without a separate count.
        const snapshot = await client.query<{ total: number }>(
            `WITH items AS (
             INSERT
             INTO bulk_job_items (job_id, opportunity_id, snapshot_stage_id)
            SELECT $1, o.id, o.stage_id
            FROM opportunities o
            WHERE o.workspace_id = $2
              AND ${filterPredicate('o', '$3::jsonb')}
            ORDER BY o.id
                LIMIT ${MAX_BULK_MOVE + 1}
                RETURNING 1)
            SELECT count(*) ::int AS total
            FROM items`,
            [jobId, workspaceId, request.filter],
        );
        const total = snapshot.rows[0].total;
        if (total > MAX_BULK_MOVE) {
            // Throwing rolls back the job row and the items; the key stays unused.
            throw new HttpError(422, `filter matches more than ${MAX_BULK_MOVE} opportunities`);
        }

        await client.query(
            `UPDATE bulk_jobs
             SET total  = $2,
                 status = CASE WHEN $2 = 0 THEN 'completed' ELSE 'queued' END::bulk_job_status,
              finished_at = CASE WHEN $2 = 0 THEN now()
            END
        WHERE id =
            $1`,
            [jobId, total],
        );
        // Delivered to LISTENing workers only if this transaction commits.
        await client.query('NOTIFY bulk_jobs');

        return {job: (await getJobView(client, workspaceId, jobId))!, replayed: false};
    });
}
