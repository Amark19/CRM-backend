import type { Pool } from 'pg';

// Invariants of a finished (or partially finished) job. Returns what's broken;
// an empty list means the job is correct. Used by tests and the kill/resume benchmark.
//
// A deal's outcome is derived, not stored: every item up to the cursor was
// handled in a committed chunk; it moved if it has a bulk transition from this
// job, otherwise it was skipped.
export async function verifyJob(pool: Pool, jobId: number): Promise<string[]> {
  const { rows } = await pool.query(
    `SELECT j.total, j.moved, j.skipped, j.status,
            (SELECT count(*) FROM bulk_job_items i WHERE i.job_id = j.id)::int AS items,
            (SELECT count(*) FROM bulk_job_items i WHERE i.job_id = j.id AND i.opportunity_id <= j.cursor)::int AS handled,
            (SELECT count(*) FROM stage_transitions t WHERE t.bulk_job_id = j.id)::int AS transitions,
            (SELECT count(DISTINCT opportunity_id) FROM stage_transitions t WHERE t.bulk_job_id = j.id)::int AS distinct_moved,
            (SELECT count(*) FROM stage_transitions t
              WHERE t.bulk_job_id = j.id
                AND NOT EXISTS (SELECT 1 FROM bulk_job_items i
                                 WHERE i.job_id = j.id AND i.opportunity_id = t.opportunity_id
                                   AND i.opportunity_id <= j.cursor))::int AS moved_outside_handled
       FROM bulk_jobs j WHERE j.id = $1`,
    [jobId],
  );
  const r = rows[0];
  const problems: string[] = [];
  if (r.items !== r.total) problems.push(`items ${r.items} != total ${r.total}`);
  if (r.transitions !== r.distinct_moved) {
    problems.push(`a deal was moved more than once (${r.transitions} transitions, ${r.distinct_moved} deals)`);
  }
  if (r.transitions !== r.moved) problems.push(`transitions ${r.transitions} != moved counter ${r.moved}`);
  if (r.moved_outside_handled !== 0) {
    problems.push(`${r.moved_outside_handled} deals moved that are not handled snapshot items`);
  }
  if (r.handled !== r.moved + r.skipped) {
    problems.push(`handled items ${r.handled} != moved ${r.moved} + skipped ${r.skipped}`);
  }
  if (r.status === 'completed' && r.handled !== r.total) {
    problems.push(`completed with ${r.handled} of ${r.total} handled`);
  }
  return problems;
}
