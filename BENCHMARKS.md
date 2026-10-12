# Benchmarks

Measured with `npm run bench` (`src/scripts/bench.ts`). The numbers below are from a single run
on the machine described. Rough, honestly measured, and reproducible with the commands at the end.

## Hardware and setup

- **Machine:** 12th Gen Intel Core i7-12650H (10 cores / 16 threads), 16 GB RAM, Windows 11
  (build 26200).
- **Postgres 16:** in Docker Desktop (WSL2 backend), default settings, no tuning.
- **API, 2 worker processes, and the load generator:** native Node on the same machine. Everything
  shares one laptop, so the load generator competes with the system it measures.
- **Worker settings:** the defaults. Chunk 1,000 deals, ≤ 5,000 rows/s per job, 30s lease, 2s
  turns.

## Dataset

`npm run seed -- --large`, seeded in **24.0s**:
- 1 workspace with **500,000** deals across 12 stages (uneven: the biggest stage holds ~22%),
  `created_at` spread over ~18 months;
- 5 workspaces with 3,000 deals each.

## Results

### 50,000-deal bulk move

| Metric | Value |
|---|---|
| Deals moved | 50,000 |
| Wall-clock (submit → done) | **10.4 s** |
| Active processing time | 10.0 s |
| Sustained throughput | **4,980 rows/s** |

Throughput sits at the 5,000 rows/s pacing cap, so the job is limited by its own throttle, not by
the hardware. A slower database would show up as throughput **below** the cap.

### Interactive latency during the bulk move (ms)

Open-loop load at 40 requests/s per workspace (half single-deal moves, half list-page reads).
The baseline is 20s of the same load with no bulk job running.

| Scenario | Requests | p50 | p95 | p99 |
|---|---|---|---|---|
| Same workspace, baseline | 650 | 10.1 | 24.0 | 33.9 |
| Same workspace, during bulk | 327 | 8.0 | 21.1 | 32.7 |
| Other workspace, baseline | 649 | 10.4 | 23.1 | 36.2 |
| Other workspace, during bulk | 327 | 7.8 | 22.5 | 34.0 |

**Reading:** no measurable degradation, in the same workspace or in another one. The "during"
figures are slightly lower than baseline; that is noise, not an improvement. The baseline runs
first, while caches and the JIT warm up, and the samples are small.

### Kill and resume

| Metric | Value |
|---|---|
| Workers SIGKILLed at | 30,000 of 50,000 processed |
| Restart → completed | **34.6 s** |
| `verifyJob` | **OK**, all invariants hold |

34.6s is the 30s lease wait plus ~4s to move the remaining 20,000 deals at the paced rate.
Recovery time is dominated by the lease length, which is a deliberate trade-off (DESIGN.md §5).
That progress stopped until the lease expired also confirms the kill really stopped the workers.

`verifyJob` checked, at 50,000 deals:
- no deal has more than one bulk transition;
- transitions equal the `moved` counter;
- every handled item is counted exactly once;
- every snapshot item was handled.

## Caveats

- **One run.** Repeat runs and an unpaced run (`WORKER_ROWS_PER_SECOND=1000000`, to show what
  pacing protects against) are listed below as next measurements.
- **The latency samples are small** (~330 requests during the bulk run), so p99 rests on a handful
  of requests.
- **This laptop has far more headroom than a small production database.** The relative comparison
  (during vs baseline) is the meaningful result; absolute latencies are not.
- **No tuning:** Postgres runs with default settings.

## Not yet measured

- 2 more repeat runs, for variance.
- An unpaced run, comparing p99 with and without pacing.
- A run with Postgres limited to production-like resources (e.g. 1 CPU / 1 GB).

## Reproduce

```bash
docker compose up -d postgres
npm install
npm run migrate
npm run seed -- --large
npm start            # API on :3000, in its own terminal
npm run bench        # spawns its own 2 workers; don't run other workers at the same time
```

Re-seed before each repeat run (`npm run seed -- --large`), since each run moves ~100k deals.
