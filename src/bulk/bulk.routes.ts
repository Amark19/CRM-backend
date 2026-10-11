import { Router } from 'express';
import Joi from 'joi';
import type { Pool } from 'pg';
import { HttpError } from '../api/errors';
import { id, validate } from '../api/validate';
import { filterSchema } from './filter';
import { getJobView } from './jobs';
import { submitBulkMove, type BulkMoveRequest } from './submit';

const submitSchema = Joi.object<BulkMoveRequest>({
  filter: filterSchema.required(),
  targetStageId: id.required(),
});
const keySchema = Joi.string().trim().min(1).max(200).required().label('Idempotency-Key');

export function bulkRouter(pool: Pool): Router {
  const router = Router();

  router.post('/bulk-moves', async (req, res) => {
    const key = validate(keySchema, req.header('Idempotency-Key'));
    const body = validate(submitSchema, req.body);
    const { job, replayed } = await submitBulkMove(pool, req.workspaceId, key, body);
    if (replayed) res.set('Idempotent-Replayed', 'true');
    res.status(replayed ? 200 : 202).json(job);
  });

  router.get('/bulk-moves/:id', async (req, res) => {
    const jobId = validate(id.required().label('id'), req.params.id);
    const job = await getJobView(pool, req.workspaceId, jobId);
    if (!job) throw new HttpError(404, 'bulk move not found');
    res.json(job);
  });

  return router;
}
