// Concurrency stress test: a real worker process runs a bulk move while many
// manual moves hit the same deals at the same time, some before the job reaches
// them, some after, some while their chunk is in flight.
//
// The filter deliberately does not name a stage, so the only thing protecting a
// manual move is the stage guard (`o.stage_id = c.snapshot_stage_id`). Remove it
// and manually moved deals get dragged back to the target stage: this test fails.
import { spawn, type ChildProcess } from 'child_process';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { submitBulkMove } from '../src/bulk/submit';
import { verifyJob } from '../src/bulk/verify';
import { createPool } from '../src/db/pool';
import { moveOpportunity } from '../src/domain/opportunities';
import { createWorkspace, drainQueue, seedOpportunities } from './helpers';

const pool = createPool(20);
afterAll(() => pool.end());
beforeEach(() => drainQueue(pool));

const DEALS = 10_000;
const MANUAL_MOVES = 200;
const PARALLEL = 5; // manual moves fired at once
const ROUNDS = 3; // races are probabilistic; repeat to catch the rare interleavings

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function startWorker(): ChildProcess {
  return spawn('npx', ['tsx', 'src/worker/main.ts'], {
    env: {
      ...process.env,
      // ~3.5s per job, so manual moves land throughout the run.
      WORKER_CHUNK_SIZE: '200',
      WORKER_ROWS_PER_SECOND: '3000',
    },
    stdio: 'ignore',
  });
}

async function jobRow(jobId: number) {
  const { rows } = await pool.query(
    'SELECT status, total, moved, skipped, moved + skipped AS processed FROM bulk_jobs WHERE id = $1',
    [jobId],
  );
  return rows[0] as { status: string; total: number; moved: number; skipped: number; processed: number };
}

function shuffle<T>(xs: T[]): T[] {
  const a = [...xs];
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

describe('bulk move under concurrent manual moves', () => {
  for (let round = 1; round <= ROUNDS; round++) {
    it(`never overwrites a manual move made while the job runs (round ${round})`, async () => {
      const ws = await createWorkspace(pool);
      const [A, B, C] = ws.stageIds;
      const ids = await seedOpportunities(pool, ws.workspaceId, A, DEALS, { ownerId: 'race' });
      const { job } = await submitBulkMove(pool, ws.workspaceId, `race-${round}`, {
        filter: { ownerId: 'race' },
        targetStageId: B,
      });
      expect(job.total).toBe(DEALS);

      const worker = startWorker();
      const manual = shuffle(ids).slice(0, MANUAL_MOVES);
      try {
        // Start once the job is under way, so some targets are already behind the cursor.
        while ((await jobRow(job.id)).processed === 0) await sleep(10);

        for (let i = 0; i < manual.length; i += PARALLEL) {
          await Promise.all(
            manual.slice(i, i + PARALLEL).map((id) => moveOpportunity(pool, ws.workspaceId, id, C)),
          );
          await sleep(50);
        }

        while ((await jobRow(job.id)).status !== 'completed') await sleep(50);
      } finally {
        worker.kill('SIGTERM');
      }

      // 1. Every manual move won: those deals are in C, never dragged back to B.
      const { rows: manualRows } = await pool.query<{ id: number; stage_id: number }>(
        'SELECT id, stage_id FROM opportunities WHERE id = ANY($1::bigint[])',
        [manual],
      );
      expect(manualRows.filter((r) => r.stage_id !== C)).toEqual([]);

      // 2. For each of them, the latest history row is the manual move to C.
      const { rows: lastTransitions } = await pool.query<{ source: string; to_stage_id: number }>(
        `SELECT DISTINCT ON (opportunity_id) source, to_stage_id
           FROM stage_transitions
          WHERE opportunity_id = ANY($1::bigint[])
          ORDER BY opportunity_id, id DESC`,
        [manual],
      );
      expect(lastTransitions).toHaveLength(MANUAL_MOVES);
      expect(lastTransitions.every((t) => t.source === 'manual' && t.to_stage_id === C)).toBe(true);

      // 3. Every deal nobody touched by hand was moved to B.
      const { rows: others } = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM opportunities
          WHERE workspace_id = $1 AND NOT (id = ANY($2::bigint[])) AND stage_id <> $3`,
        [ws.workspaceId, manual, B],
      );
      expect(others[0].n).toBe(0);

      // 4. Job invariants: no deal moved twice, counters match the data.
      expect(await verifyJob(pool, job.id)).toEqual([]);

      // 5. The skipped deals are exactly the manual moves that beat the job to the deal.
      const { rows: beaten } = await pool.query<{ n: number }>(
        `SELECT count(*)::int AS n FROM unnest($1::bigint[]) AS m(id)
          WHERE NOT EXISTS (SELECT 1 FROM stage_transitions t
                             WHERE t.bulk_job_id = $2 AND t.opportunity_id = m.id)`,
        [manual, job.id],
      );
      const final = await jobRow(job.id);
      expect(final.skipped).toBe(beaten[0].n);
      expect(final.moved + final.skipped).toBe(DEALS);

      // 6. The race really happened in both directions; otherwise this round proved nothing.
      expect(beaten[0].n).toBeGreaterThan(0); // manual move first, job skipped the deal
      expect(MANUAL_MOVES - beaten[0].n).toBeGreaterThan(0); // job first, manual move on top
    });
  }
});
