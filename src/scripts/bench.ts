// Measures, on the --large seed:
//   1. a ~50k bulk move: wall-clock and sustained throughput;
//   2. interactive p50/p95/p99 during it, same workspace and another one, vs a baseline;
//   3. SIGKILL of all workers at ~30k, restart, time to completion, verifyJob.
// Needs the API running (npm start). Spawns its own workers so it controls kill
// timing; stop any other workers first.
import { spawn, type ChildProcess } from 'child_process';
import os from 'os';
import { verifyJob } from '../bulk/verify';
import { createPool } from '../db/pool';

const API = process.env.API_URL ?? 'http://localhost:3000';
const WORKERS = Number(process.env.BENCH_WORKERS ?? 2);
const RATE_PER_SECOND = 40; // interactive requests per workspace, open loop
const BASELINE_MS = 20_000;
const pool = createPool(4);

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function startWorkers(): ChildProcess[] {
  return Array.from({ length: WORKERS }, () =>
    spawn(process.execPath, ['--import','tsx', 'src/worker/main.ts'], { stdio: 'ignore', env: process.env }),
  );
}

function percentile(sorted: number[], p: number): number {
  return sorted[Math.min(sorted.length - 1, Math.floor((p / 100) * sorted.length))];
}

interface Target {
  workspaceId: number;
  stageIds: number[];
  dealIds: number[];
}

async function target(workspaceId: number): Promise<Target> {
  const stages = await pool.query<{ id: number }>(
    'SELECT id FROM stages WHERE workspace_id = $1 ORDER BY position',
    [workspaceId],
  );
  const deals = await pool.query<{ id: number }>(
    'SELECT id FROM opportunities WHERE workspace_id = $1 ORDER BY random() LIMIT 2000',
    [workspaceId],
  );
  return { workspaceId, stageIds: stages.rows.map((r) => r.id), dealIds: deals.rows.map((r) => r.id) };
}

// Open-loop load: fire at a fixed rate regardless of response times, so slow
// responses show up as latency instead of quietly lowering the request rate.
async function load(t: Target, isDone: () => boolean): Promise<number[]> {
  const latencies: number[] = [];
  const inflight: Promise<void>[] = [];
  const pick = <T>(xs: T[]) => xs[Math.floor(Math.random() * xs.length)];
  while (!isDone()) {
    const started = performance.now();
    const req =
      Math.random() < 0.5
        ? fetch(`${API}/opportunities/${pick(t.dealIds)}/move`, {
            method: 'POST',
            headers: { 'content-type': 'application/json', 'x-workspace-id': String(t.workspaceId) },
            body: JSON.stringify({ toStageId: pick(t.stageIds) }),
          })
        : fetch(`${API}/stages/${pick(t.stageIds)}/opportunities?limit=50`, {
            headers: { 'x-workspace-id': String(t.workspaceId) },
          });
    inflight.push(
      req.then(async (res) => {
        await res.arrayBuffer();
        latencies.push(performance.now() - started);
      }),
    );
    await sleep(1000 / RATE_PER_SECOND);
  }
  await Promise.all(inflight);
  return latencies.sort((a, b) => a - b);
}

function row(label: string, xs: number[]): string {
  return `| ${label} | ${xs.length} | ${percentile(xs, 50).toFixed(1)} | ${percentile(xs, 95).toFixed(1)} | ${percentile(xs, 99).toFixed(1)} |`;
}

// Biggest stage, with a created_at cut-off so the filter matches ~50,000 deals.
async function submit50k(workspaceId: number, key: string): Promise<number> {
  const { rows: [s] } = await pool.query(
    `SELECT stage_id FROM opportunities WHERE workspace_id = $1 GROUP BY 1 ORDER BY count(*) DESC LIMIT 1`,
    [workspaceId],
  );
  const { rows: [c] } = await pool.query(
    `SELECT created_at FROM opportunities WHERE workspace_id = $1 AND stage_id = $2
      ORDER BY created_at OFFSET 49999 LIMIT 1`,
    [workspaceId, s.stage_id],
  );
  const { rows: [to] } = await pool.query(
    `SELECT id FROM stages WHERE workspace_id = $1 AND id <> $2 ORDER BY position DESC LIMIT 1`,
    [workspaceId, s.stage_id],
  );
  const res = await fetch(`${API}/bulk-moves`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'x-workspace-id': String(workspaceId), 'idempotency-key': key },
    body: JSON.stringify({
      filter: { stageId: s.stage_id, createdTo: new Date(c.created_at.getTime() + 1).toISOString() },
      targetStageId: to.id,
    }),
  });
  const job = await res.json();
  console.log(`submitted job ${job.id}: ${job.total} deals (HTTP ${res.status})`);
  return job.id;
}

async function jobRow(id: number) {
  const { rows } = await pool.query(
    `SELECT status, total, moved + skipped AS processed,
            EXTRACT(EPOCH FROM finished_at - created_at)::float8 AS wall,
            EXTRACT(EPOCH FROM finished_at - started_at)::float8 AS active
       FROM bulk_jobs WHERE id = $1`,
    [id],
  );
  return rows[0];
}

async function main(): Promise<void> {
  const { rows: ws } = await pool.query<{ id: number }>(
    `SELECT workspace_id AS id FROM opportunities GROUP BY 1 ORDER BY count(*) DESC`,
  );
  const large = await target(ws[0].id);
  const small = await target(ws[ws.length - 1].id);

  const cpus = os.cpus();
  console.log(
    `## Hardware\n${cpus[0].model} × ${cpus.length}, ${(os.totalmem() / 2 ** 30).toFixed(0)} GB RAM, ` +
      `${os.platform()} ${os.release()}, workers=${WORKERS}\n`,
  );

  // 1. Baseline: interactive latency with no bulk job running.
  let baselineOver = false;
  setTimeout(() => (baselineOver = true), BASELINE_MS);
  const [baseLarge, baseSmall] = await Promise.all([load(large, () => baselineOver), load(small, () => baselineOver)]);

  // 2. During a 50k bulk move.
  let workers = startWorkers();
  const jobId = await submit50k(large.workspaceId, `bench-${Date.now()}`);
  let done = false;
  const poll = (async () => {
    while (!done) {
      done = (await jobRow(jobId)).status === 'completed';
      await sleep(200);
    }
  })();
  const [runLarge, runSmall] = await Promise.all([load(large, () => done), load(small, () => done), poll]);
  const run = await jobRow(jobId);
  workers.forEach((w) => w.kill('SIGTERM'));

  console.log('## Bulk move');
  console.log(
    `total ${run.total}, wall-clock ${run.wall.toFixed(1)}s (submit → done), active ${run.active.toFixed(1)}s, ` +
      `throughput ${(run.total / run.active).toFixed(0)} rows/s\n`,
  );
  console.log('## Interactive latency (ms)\n| scenario | requests | p50 | p95 | p99 |\n|---|---|---|---|---|');
  console.log(row('same workspace, baseline', baseLarge));
  console.log(row('same workspace, during bulk', runLarge));
  console.log(row('other workspace, baseline', baseSmall));
  console.log(row('other workspace, during bulk', runSmall));

  // 3. Kill at ~30k and resume.
  await sleep(1000);
  workers = startWorkers();
  const killJob = await submit50k(large.workspaceId, `bench-kill-${Date.now()}`);
  while ((await jobRow(killJob)).processed < 30_000) await sleep(50);
  workers.forEach((w) => w.kill('SIGKILL'));
  const atKill = (await jobRow(killJob)).processed;
  const restartedAt = Date.now();
  workers = startWorkers();
  while ((await jobRow(killJob)).status !== 'completed') await sleep(100);
  const resumeSeconds = (Date.now() - restartedAt) / 1000;
  workers.forEach((w) => w.kill('SIGTERM'));
  const problems = await verifyJob(pool, killJob);

  console.log(
    `\n## Kill and resume\nkilled at ${atKill} processed; restart → completed in ${resumeSeconds.toFixed(1)}s ` +
      `(includes waiting out the lease)`,
  );
  console.log(`verifyJob: ${problems.length === 0 ? 'OK, all invariants hold' : problems.join('; ')}`);
  await pool.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
