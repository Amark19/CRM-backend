# Design — bulk stage move

## Shape of the system

- **API** (`src/api`, `src/bulk`): validates, dedupes, snapshots, returns `202` with a job id.
- **Workers** (`src/worker`): separate processes that claim jobs from a Postgres table and
  process them in chunks.
- **Postgres** is the only stateful component: data, job queue, cursor, progress and
  idempotency keys all live there, so they commit together and survive any process restart.

Tables: `bulk_jobs` (one row per job: request, status, counters, cursor, lease) and
`bulk_job_items` (one row per snapshotted deal: `job_id, opportunity_id, snapshot_stage_id`).

## 0. Technology choices

**No Redis or Kafka.** Every safety property rests on one transaction per chunk: the moved deals,
their history rows, the job's cursor and its counters commit together. So job state must live in
Postgres whatever else we add.
- A broker would carry only "job X is ready", as a second system that can disagree with the
  database (a job committed but never enqueued, or the reverse).
- Kafka's strengths (partitioned high-volume streams, replay) don't fit a few bulk jobs a minute.
- Redis would hold idempotency keys whose durability we'd then have to defend.

Postgres already has what a queue needs: `FOR UPDATE SKIP LOCKED` and `LISTEN/NOTIFY`.

**Delivery semantics.** This is not a broker's push model.
- `NOTIFY` is a broadcast wake-up: every idle worker hears it and tries to claim.
- The **claim** (`SKIP LOCKED` + a lease, like an SQS visibility timeout) is the delivery, so
  exactly one worker owns a job at a time.
- A dead owner's lease expires and the job is re-claimed. Delivery is therefore
  **at-least-once**, and processing is idempotent (atomic chunks, cursor, fencing token, a guard
  that can't move a deal twice), so the **effect is exactly-once per deal**.
- `NOTIFY` is fire-and-forget, so it only cuts latency. A 2s idle poll guarantees pickup.

**Raw SQL, no ORM.** The core is a few statements whose locking matters: a guarded
`UPDATE … FROM … RETURNING` inside a data-modifying CTE, and `SKIP LOCKED`. An ORM would split
them into several round trips held inside one transaction, which means longer row locks, and it
would hide exactly what each statement locks.

## 1. Chunking and the cursor

At submit, the matching ids are frozen into `bulk_job_items` with one `INSERT … SELECT`. That
statement uses the `opportunities (workspace_id, stage_id, id)` index whenever the filter names a
stage.

A worker processes the job in chunks of 1,000:

```sql
SELECT opportunity_id, snapshot_stage_id FROM bulk_job_items
 WHERE job_id = $1 AND opportunity_id > $cursor ORDER BY opportunity_id LIMIT 1000
```

This is a range read on the primary key `(job_id, opportunity_id)`. It starts exactly at the
cursor, so chunk 50 costs the same as chunk 1.

Each chunk is **one transaction** containing:
- the guarded `UPDATE` of the deals;
- the `stage_transitions` inserts for the deals that actually moved;
- the job's `cursor`, `moved`/`skipped` counters, `last_progress_at` and lease extension.

These all come from a single data-modifying CTE statement (`src/worker/chunk.ts`). The cursor
therefore can never be ahead of or behind the data. A crash mid-chunk rolls the whole chunk
back, and the cursor still points at the end of the last committed chunk.

**Alternatives considered:**
- *All ids in one array column:* about 6× smaller, but every chunk would unpack the whole array
  (≈8 MB per chunk at 500k).
- *One row per chunk holding an id array:* small, but the chunk size is fixed at submit.

The item table costs ≈100 bytes per deal (≈5 MB for 50k), and its per-chunk cost does not grow
with the job size.

## 2. Idempotency

| Replay | Protection | Where the state lives |
|---|---|---|
| Client retries `POST /bulk-moves` | `UNIQUE (workspace_id, idempotency_key)` + `INSERT … ON CONFLICT DO NOTHING`. A repeat returns the same job (`200`, `Idempotent-Replayed: true`). | `bulk_jobs` row in Postgres, so it survives restarts |
| Two identical requests at once | The second `INSERT` waits on the first's uncommitted unique-index entry, then does nothing | same |
| Same key, different body | A SHA-256 of the normalized request is compared; mismatch returns `422` | `bulk_jobs.request_hash` |
| A chunk re-run after a crash | The cursor commits with the chunk; a rolled-back chunk left no trace | `bulk_jobs.cursor` |
| A stale worker writing after losing its lease | Fencing token (below) | `bulk_jobs.lease_epoch` |
| Anything reaching an already-moved deal | The guard `stage_id = snapshot_stage_id` is false once the deal is in the target stage. Submit rejects target = source. | the deal row |

A submit that fails (for example more than 50,000 matches) rolls back entirely, so its key is
never stored and can be reused.

**Where a retry can still slip through:**
- A client that sends a **new key** per retry creates a second job. Its guard skips every deal
  the first job already moved, so no deal moves twice, but the user sees a second job made up of
  skips.
- Keys never expire, so a key is unusable forever once taken.

## 3. Concurrency on a single opportunity

**Manual move.** `SELECT … FOR UPDATE` on the row, an optional `expectedVersion` check (409),
then `UPDATE` with `version + 1`, plus a transition, all in one transaction.

**Bulk move.** A guarded update:

```sql
UPDATE opportunities o SET stage_id = $target, version = o.version + 1
  FROM chunk c
 WHERE o.id = c.opportunity_id
   AND o.stage_id = c.snapshot_stage_id      -- nobody moved it by hand since submit
   AND <still matches the filter>
```

**Policy: a manual move made after submission always wins.**

| Timing | Result |
|---|---|
| Manual move *before* the job reaches the deal | Its stage is no longer the snapshot stage, so the job skips it |
| Manual move *after* the job moved it | The job never revisits a deal behind its cursor; the manual move stands |
| Both at the same moment | Whoever locks the row first goes first. If the manual move wins the lock, Postgres (READ COMMITTED) re-evaluates the job's `WHERE` against the new row version after the manual move commits, and the job skips the deal. If the job wins, the manual move waits one chunk and then applies on top. |

Edits other than stage (for example a rename) do not block the move. An edit that takes the deal
out of the filter (for example marking it lost when the filter is `status: open`) does.

**Accepted edge case.** A deal moved out of its stage and back again before the job reaches it is
moved by the job, because its current state matches what was requested.

**Deadlocks.** A manual move locks one row, so it cannot deadlock with a chunk. Two bulk jobs in
one workspace never run at once (section 5).

**How this is tested.**
- Manual moves made between chunks, before and after the cursor, with and without a stage in the
  filter, must never be overwritten. Removing the stage guard makes this test fail.
- 20 truly parallel manual moves of one deal must leave an unbroken transition chain. Removing
  `FOR UPDATE` makes this test fail.
- **Not directly tested:** a manual move and a chunk hitting the same row at the same instant.
  That case relies on Postgres's documented READ COMMITTED re-check of the `WHERE` clause.

## 4. Snapshot vs live set

We act on the **snapshot**: the deals matching the filter at submit time. Each deal is then
re-checked at move time (section 3).

- The total is fixed, so progress (`moved + skipped` of `total`) is exact and survives restarts.
- The job moves only what the user approved. Deals that **enter** the filter after submission
  (created, or edited into it) are **not** moved.
- Deals that **leave** the filter, or were moved by hand, are skipped.
- A deal's outcome is derived, not stored: items up to the cursor were handled. Those with a bulk
  transition from this job moved; the rest were skipped.

**Consequence.** A user expecting "everything matching right now, at the end" will find newly
matching deals left behind and has to run another move. We chose that over moving deals nobody
selected.

## 5. Isolation

The mechanisms, all implemented:

1. **One running job per workspace.** The claim query only considers the oldest unfinished job of
   each workspace.
2. **Round-robin time slices.** A worker holds a job for at most about 2 seconds, then releases it.
   Claims prefer the least recently served job, so workspaces take turns.
3. **Pacing.** Each job is held to ≤ 5,000 rows/s by sleeping between chunks. The gap is time
   Postgres spends on interactive queries.
4. **Separate connection pool.** Each worker process has 2 connections and the API has 10. A bulk
   job can never use up the API's connections.
5. **A global ceiling.** At most `worker processes × 5,000 rows/s` in total, whatever the number
   of submitted jobs.
6. **Short lock holds.** One statement per chunk, so a manual move waits at most one chunk.

Claiming uses `FOR UPDATE SKIP LOCKED` with a 30s lease. Each claim increments `lease_epoch`, and
every chunk first checks `lease_epoch = mine` (`FOR UPDATE` on the job row). A worker that froze
past its lease and was replaced writes nothing.

**The hole it leaves.**
- Every workspace shares Postgres's CPU, disk bandwidth, WAL and autovacuum. A large job in one
  workspace still consumes them.
- Pacing is a fixed rate, not adaptive to actual database load, so a busy database gets the same
  bulk write rate as an idle one.
- Turns are ordered per **job**, not per workspace. When a workspace's job finishes, its next job
  starts with `last_served_at = NULL` and goes ahead of the others for one slice. The fix is to
  order by the workspace's last-served time.
- One job per workspace trades throughput for isolation. Spare workers stay idle rather than help
  a workspace that has several jobs queued.

## 6. Resumability

**"Correct" means**, at completion:
- every snapshot deal either moved exactly once (one bulk transition) or was skipped;
- `moved + skipped = total`;
- no deal manually moved after submission was overwritten.

`verifyJob()` checks these invariants in the tests and after the kill/resume benchmark.

**Recovery after a crash.** If a worker is killed mid-job:
1. Postgres rolls back its in-flight chunk.
2. The lease expires within 30s.
3. Any worker claims the job with a new epoch and continues from the cursor.

A graceful shutdown (SIGTERM) finishes the current chunk and releases the job immediately.

**What a half-applied job looks like to the user.** For example, `status: running`,
`processed: 30000 / 50000`. Those 30,000 deals are really in the target stage and the rest really
aren't. If no worker resumes it, `health` becomes `stalled` after 60s without a committed chunk.
Once resumed, the counters continue from 30,000; they never reset or double-count.

## 7. What breaks at 10× (500k-deal move, 20M opportunities)

1. **The first thing to fail: dead tuples and bloat.** Each update leaves a dead row version, and
   `stage_id` is indexed, so these are not HOT updates and every index gets a new entry too.
   500k moved deals means 500k dead tuples in `opportunities` plus index bloat. With 20M rows,
   autovacuum's default 20% threshold is 4M dead tuples, so it falls behind.
   **Fix:** per-table autovacuum settings for `opportunities` (lower scale factor, higher cost
   limit), and bloat monitoring.
2. **The snapshot inside the request.** Inserting 500k items in the submit transaction takes
   seconds and holds a connection.
   **Fix:** make the snapshot the job's first phase in the worker, written in batches, with
   progress reported as "snapshotting".
3. **Snapshot query cost.** A filter without a stage scans the workspace's slice of a 20M-row
   table.
   **Fix:** indexes for the common filter shapes (owner, created_at), or partition
   `opportunities` by workspace.
4. **Fixed pacing.** A fixed rate is either too slow for quiet periods or too aggressive for busy
   ones.
   **Fix:** adaptive pacing driven by interactive p99 or database load.
5. **Many workers.** Each worker holds 3 connections, so 100 workers need 300, which is above
   Postgres's default `max_connections` of 100. Every idle worker also wakes on each `NOTIFY`
   (N claim queries per job, one of which wins) and polls every 2s.
   **Fix:** PgBouncer for the pooled connections (LISTEN needs a direct connection), a longer idle
   poll, and beyond that a broker for one-consumer delivery, with Postgres remaining the source of
   truth for job state.

## 8. Another week, ranked

1. **Adaptive pacing** from measured interactive latency. It closes the main isolation hole.
2. **Snapshot inside the worker**, as the job's first phase. It removes the biggest
   request-path cost at scale.
3. **Cancel endpoint** (`status: cancelled`; the worker stops at the next chunk).
4. **Skipped-deals endpoint with reasons** ("moved manually" vs "no longer matches").
5. **Idempotency key expiry** (e.g. 24h), so keys can be reused.
6. **Metrics:** chunk latency, rows/s per workspace, lease takeovers, stalled jobs.
