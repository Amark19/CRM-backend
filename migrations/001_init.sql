-- Part 1 schema: workspaces, a single ordered pipeline of stages per workspace,
-- opportunities, and the transition log.

CREATE TYPE opportunity_status AS ENUM ('open', 'won', 'lost', 'abandoned');
CREATE TYPE transition_source AS ENUM ('manual', 'bulk');

CREATE TABLE workspaces (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  name       text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- One pipeline per workspace, so stages hang directly off the workspace.
CREATE TABLE stages (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workspace_id bigint NOT NULL REFERENCES workspaces (id),
  name         text NOT NULL,
  position     int NOT NULL,
  UNIQUE (workspace_id, position),
  -- Target for the composite FK below.
  UNIQUE (workspace_id, id)
);

CREATE TABLE opportunities (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workspace_id bigint NOT NULL REFERENCES workspaces (id),
  stage_id     bigint NOT NULL,
  name         text NOT NULL,
  value_cents  bigint NOT NULL CHECK (value_cents >= 0),
  status       opportunity_status NOT NULL DEFAULT 'open',
  owner_id     text NOT NULL,
  -- Bumped on every write; the optimistic-concurrency token.
  version      int NOT NULL DEFAULT 1,
  created_at   timestamptz NOT NULL DEFAULT now(),
  updated_at   timestamptz NOT NULL DEFAULT now(),
  -- Tenant safety enforced by the database: an opportunity can only point
  -- at a stage that belongs to its own workspace.
  FOREIGN KEY (workspace_id, stage_id) REFERENCES stages (workspace_id, id)
);

-- Serves "list opportunities in a stage" with keyset pagination on id,
-- and later the bulk job's chunked scans.
CREATE INDEX opportunities_ws_stage_id_idx ON opportunities (workspace_id, stage_id, id);

CREATE TABLE stage_transitions (
  id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  workspace_id   bigint NOT NULL REFERENCES workspaces (id),
  opportunity_id bigint NOT NULL REFERENCES opportunities (id),
  from_stage_id  bigint NOT NULL REFERENCES stages (id),
  to_stage_id    bigint NOT NULL REFERENCES stages (id),
  source         transition_source NOT NULL,
  -- Set for moves made by a bulk job; FK added when bulk_jobs exists (Part 2).
  bulk_job_id    bigint,
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX stage_transitions_opportunity_idx ON stage_transitions (opportunity_id, id);
