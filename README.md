# Opportunities — bulk stage move

A small opportunities (deals) service whose one hard feature is moving up to
50,000 opportunities to another stage in a single request: accepted
immediately, processed in the background, idempotent, resumable, observable,
safe under concurrent manual edits, and isolated from other work.

Design rationale is in [DESIGN.md](DESIGN.md); measurements in [BENCHMARKS.md](BENCHMARKS.md).

## Run everything

```bash
docker compose up --build
```

This starts Postgres, runs migrations, seeds a small dataset (5 workspaces ×
3,000 deals), runs the test suite once (`tests` service; results in its
logs), and leaves the API on `localhost:3000` and two workers running.

## Local development

```bash
npm install
docker compose up -d postgres
npm run migrate
npm run seed               # 5 small workspaces; add -- --large for the 500k workspace
npm start                  # API on :3000
npm run worker             # one worker process; start more in other terminals
npm test                   # uses the opportunities_test database
npm run typecheck
```

Tests run against a separate `opportunities_test` database (created on first
Postgres start by `docker/initdb`), so running workers never pick up test jobs.

To inspect data: `docker compose exec postgres psql -U app -d opportunities`.

## Tests

41 tests against a real Postgres, focused on the hard parts:

| File | Covers |
|---|---|
| `bulk-submit.test.ts` | idempotency: replay, 10 truly concurrent identical requests produce one job, same key with a different body returns 422, keys are per workspace; the 50,000 cap; tenant scoping; progress health |
| `bulk-chunk.test.ts` | manual moves are never overwritten; filter re-check; resume from the cursor after lease expiry; a "zombie" worker is rejected by the fencing token |
| `bulk-queue.test.ts` | one running job per workspace, workspaces take turns, failure after retries, pacing |
| `bulk-kill.test.ts` | a real worker **process** is SIGKILLed mid-job; another one finishes it and the invariants hold |
| `bulk-race.test.ts` | stress: a real worker moves 10,000 deals while 200 manual moves hit the same deals concurrently, ×3 rounds; every manual move wins, and the race is checked to have happened both ways |
| `opportunities.test.ts` | Part 1, including 20 parallel moves of one deal keeping an unbroken history |

Each safety mechanism was removed in turn to confirm a test fails without it: the stage guard,
the `lease_epoch` fence, `FOR UPDATE` on manual moves, and the one-job-per-workspace rule.

Postgres logs a few `violates foreign key constraint` errors during the run. They are expected:
they come from the tests that deliberately use another workspace's stage, which the composite
foreign key rejects (the API returns 422).

## API

Every request needs `X-Workspace-Id`. There is no auth.

```bash
# Create an opportunity
curl -X POST localhost:3000/opportunities -H 'X-Workspace-Id: 1' -H 'Content-Type: application/json' \
  -d '{"stageId": 1, "name": "Acme renewal", "valueCents": 1250000, "ownerId": "owner-7"}'

# Move one opportunity (expectedVersion optional: 409 if someone changed it first)
curl -X POST localhost:3000/opportunities/42/move -H 'X-Workspace-Id: 1' -H 'Content-Type: application/json' \
  -d '{"toStageId": 3, "expectedVersion": 2}'

# List a stage, keyset-paginated
curl 'localhost:3000/stages/1/opportunities?limit=50&cursor=0' -H 'X-Workspace-Id: 1'

# Bulk move: 202 with a job, or 200 + Idempotent-Replayed: true for a repeated key
curl -X POST localhost:3000/bulk-moves -H 'X-Workspace-Id: 1' -H 'Idempotency-Key: 7f3c…' \
  -H 'Content-Type: application/json' \
  -d '{"filter": {"stageId": 1, "ownerId": "owner-7", "status": "open"}, "targetStageId": 6}'

# Progress
curl localhost:3000/bulk-moves/12 -H 'X-Workspace-Id: 1'
```

Bulk filter fields (all optional, at least one): `stageId`, `ownerId`,
`status`, `valueMinCents`, `valueMaxCents`, `createdFrom` (inclusive),
`createdTo` (exclusive).

## Implemented

- Part 1: create, single move with transition and optimistic `expectedVersion`, keyset listing per stage.
- Part 2: bulk move with snapshot, `Idempotency-Key`, background workers, chunked guarded moves,
  lease + fencing token, resume after crash, progress with health (`queued` / `progressing` /
  `stalled` / `completed` / `failed`) and rate, one-job-per-workspace, round-robin slices, pacing.
- Seed script (small and 500k), benchmark script, tests for idempotency, resume (including a real
  SIGKILL of a worker process), concurrent-edit collisions and fencing.

## Not implemented

- Cancelling a running bulk move.
- An endpoint listing which deals were skipped (derivable; see DESIGN.md).
- Idempotency key expiry.
- Anything the brief puts out of scope (UI, auth, summaries, events, deployment).

## Assumptions

- A filter matching 0 deals creates a job that is immediately `completed` (total 0).
- A filter matching more than 50,000 deals is rejected with 422, not truncated.
- `stalled` = no committed chunk for 60s. "Slow" is a progressing job with a low `rowsPerSecond`.
- A chunk failing 5 times in a row marks the job `failed`; committed progress stays.
- `owner_id` is an opaque string; there is no users table.
- Money is stored as integer cents.
- The seed script truncates all tables before seeding.

## Stack choices

- **Express + TypeScript, not NestJS.** The graded part (worker, transactions, rate control) is
  framework-agnostic; Express keeps request paths and transaction boundaries explicit.
- **Raw SQL via `pg`, no ORM.** The core is a few guarded statements whose locking behaviour
  matters; they're written and reviewed as SQL.
- **PostgreSQL, no Redis/Kafka.** Postgres is both the store and the job queue
  (`FOR UPDATE SKIP LOCKED`, `LISTEN/NOTIFY`), so job state, cursor and data commit together.
  Full reasoning in DESIGN.md §0.
