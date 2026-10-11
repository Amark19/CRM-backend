import type { Pool } from 'pg';
import { LeaseLostError, processChunk, type ChunkResult } from './chunk';
import { claimJob, recordFailure, releaseJob, type ClaimedJob } from './queue';

export interface WorkerConfig {
  chunkSize: number;
  rowsPerSecond: number; // per job; one job per workspace, so effectively per workspace
  leaseSeconds: number;
  sliceMs: number; // how long one job keeps this worker before others get a turn
  maxAttempts: number;
  backoffSeconds: number;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

export class Worker {
  private stopping = false;

  constructor(
    private pool: Pool,
    private config: WorkerConfig,
  ) {}

  // Finish the current chunk, release the job and stop taking new ones.
  stop(): void {
    this.stopping = true;
  }

  get isStopping(): boolean {
    return this.stopping;
  }

  // Claim one job and run one slice of it. Returns whether a job was found.
  async runOnce(): Promise<boolean> {
    if (this.stopping) return false;
    const job = await claimJob(this.pool, this.config.leaseSeconds);
    if (!job) return false;
    await this.runSlice(job);
    return true;
  }

  private async runSlice(job: ClaimedJob): Promise<void> {
    const { chunkSize, rowsPerSecond, leaseSeconds, sliceMs } = this.config;
    const sliceStart = Date.now();

    while (!this.stopping && Date.now() - sliceStart < sliceMs) {
      const chunkStart = Date.now();
      let result: ChunkResult;
      try {
        result = await processChunk(this.pool, job, chunkSize, leaseSeconds);
      } catch (err) {
        if (err instanceof LeaseLostError) return; // someone else owns it now; walk away
        await recordFailure(this.pool, job, String(err), this.config.maxAttempts, this.config.backoffSeconds);
        return;
      }
      if (result.done) return;

      // Pacing: hold this job to rowsPerSecond. At 5,000 rows/s a 1,000-row chunk
      // takes at least 200ms; the gap is time Postgres spends on everyone else.
      const minMs = (result.processed / rowsPerSecond) * 1000;
      await sleep(Math.max(0, minMs - (Date.now() - chunkStart)));
    }
    await releaseJob(this.pool, job);
  }
}
