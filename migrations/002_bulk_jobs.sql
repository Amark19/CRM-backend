CREATE TYPE bulk_job_status AS ENUM ('queued', 'running', 'completed', 'failed');

CREATE TABLE bulk_jobs (
  id               bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workspace_id     bigint NOT NULL REFERENCES workspaces (id),
  idempotency_key  text NOT NULL,
  -- SHA-256 of the normalized request; a reused key with a different body is rejected.
  request_hash     text NOT NULL,
  filter           jsonb NOT NULL,
  target_stage_id  bigint NOT NULL,
  status           bulk_job_status NOT NULL DEFAULT 'queued',
  -- Committed progress. Updated in the same transaction as the deals it counts.
  total            int NOT NULL DEFAULT 0,
  moved            int NOT NULL DEFAULT 0,
  skipped          int NOT NULL DEFAULT 0,
  -- Highest opportunity_id processed so far; items are walked in id order.
  cursor           bigint NOT NULL DEFAULT 0,
-- Fencing token: +1 on every claim. A worker that paused past its lease (GC,
-- network) and was taken over still holds the old epoch, so its next chunk
-- fails the epoch check and writes nothing.
  lease_epoch      bigint NOT NULL DEFAULT 0,
    -- Until when the current claim on this job is valid. Extended by every chunk
    -- (heartbeat). NULL or past: any worker may claim it.
  lease_expires_at timestamptz,
  attempts         int NOT NULL DEFAULT 0,
  last_error       text,
  -- when was last chunked performed commits
  last_progress_at timestamptz,
    -- When a worker last claimed this job. The claim query picks the oldest,
    -- so workspaces take turns.
  last_served_at   timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now(),
  started_at       timestamptz,
  finished_at      timestamptz,
  UNIQUE (workspace_id, idempotency_key),
  FOREIGN KEY (workspace_id, target_stage_id) REFERENCES stages (workspace_id, id)
);

-- Only unfinished jobs; the claim query and the "oldest per workspace" check
-- scan this, so it stays tiny however many finished jobs pile up.
CREATE INDEX bulk_jobs_active_idx ON bulk_jobs (workspace_id, id)
  WHERE status IN ('queued', 'running');

-- The snapshot. Written once at submit, never updated. A deal's outcome is
-- derived: items up to the job's cursor were handled; those with a bulk
-- transition from this job moved, the rest were skipped.
CREATE TABLE bulk_job_items (
  job_id            bigint NOT NULL REFERENCES bulk_jobs (id),
  opportunity_id    bigint NOT NULL,
  snapshot_stage_id bigint NOT NULL,
  -- Also the chunking index: "job X, ids after the cursor, in order".
  PRIMARY KEY (job_id, opportunity_id)
);

ALTER TABLE stage_transitions
  ADD CONSTRAINT stage_transitions_bulk_job_fk FOREIGN KEY (bulk_job_id) REFERENCES bulk_jobs (id);

CREATE INDEX stage_transitions_bulk_job_idx ON stage_transitions (bulk_job_id, opportunity_id)
  WHERE bulk_job_id IS NOT NULL;
