# Benchmarks

> **Status: not yet measured.** The method and script below are final; the result tables are
> filled in from real runs only. No numbers here are estimates.

## Method

`npm run bench` (`src/scripts/bench.ts`), against the `--large` seed:

- **Dataset:** 1 workspace with 500,000 deals across 12 stages (uneven, about 18 months of
  `created_at`), plus 5 workspaces with 3,000 deals each.
- **Bulk move:** in the large workspace, the biggest stage, filtered by a `created_at` cut-off
  chosen so the filter matches about 50,000 deals. Moved to the last stage.
  - *Wall-clock* = `finished_at − created_at` (submit to done).
  - *Throughput* = total ÷ (`finished_at − started_at`).
- **Interactive load:** an open loop at 40 requests/s per workspace, half single-deal moves and
  half list-page reads, run against both the large workspace and a small one.
  - Requests fire on a fixed schedule regardless of response time, so slow responses show up as
    latency rather than as a lower request rate.
  - The same load runs for 20s with no bulk job, as a baseline.
- **Kill and resume:**
  1. Submit another ~50k move.
  2. SIGKILL every worker once 30,000 deals are processed.
  3. Restart the workers and time them to completion.
  4. Run `verifyJob` on the result.
- **Workers:** 2 processes, chunk 1,000, 5,000 rows/s per job, 30s lease, 2s slices (defaults).

## Reproduce

```bash
docker compose up -d postgres
npm run migrate
npm run seed -- --large
npm start &            # API
npm run bench          # spawns its own workers
```

## Hardware

_To be filled from the script's output._

## Results

_To be filled from 3 runs._

| | run 1 | run 2 | run 3 |
|---|---|---|---|
| 50k bulk move wall-clock (s) | | | |
| sustained throughput (rows/s) | | | |
| kill → resume to completion (s) | | | |
| verifyJob | | | |

Interactive latency (ms):

| scenario | p50 | p95 | p99 |
|---|---|---|---|
| same workspace, baseline | | | |
| same workspace, during bulk | | | |
| other workspace, baseline | | | |
| other workspace, during bulk | | | |
