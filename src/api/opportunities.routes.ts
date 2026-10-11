import { Router } from 'express';
import Joi from 'joi';
import type { Pool } from 'pg';
import {
  createOpportunity,
  listOpportunitiesInStage,
  moveOpportunity,
  type NewOpportunity,
} from '../domain/opportunities';
import { id, validate } from './validate';

const createSchema = Joi.object<NewOpportunity>({
  stageId: id.required(),
  name: Joi.string().trim().min(1).max(200).required(),
  valueCents: Joi.number().integer().min(0).required(),
  status: Joi.string().valid('open', 'won', 'lost', 'abandoned').default('open'),
  ownerId: Joi.string().trim().min(1).max(100).required(),
});

interface MoveBody {
  toStageId: number;
  expectedVersion?: number;
}
const moveSchema = Joi.object<MoveBody>({
  toStageId: id.required(),
  expectedVersion: Joi.number().integer().min(1),
});

interface ListQuery {
  limit: number;
  cursor: number;
}
const listSchema = Joi.object<ListQuery>({
  limit: Joi.number().integer().min(1).max(200).default(50),
  cursor: Joi.number().integer().min(0).default(0),
});

export function opportunitiesRouter(pool: Pool): Router {
  const router = Router();

  router.post('/opportunities', async (req, res) => {
    const body = validate(createSchema, req.body);
    const opportunity = await createOpportunity(pool, req.workspaceId, body);
    res.status(201).json(opportunity);
  });

  router.post('/opportunities/:id/move', async (req, res) => {
    const opportunityId = validate(id.required().label('id'), req.params.id);
    const body = validate(moveSchema, req.body);
    const opportunity = await moveOpportunity(
      pool,
      req.workspaceId,
      opportunityId,
      body.toStageId,
      body.expectedVersion,
    );
    res.json(opportunity);
  });

  router.get('/stages/:stageId/opportunities', async (req, res) => {
    const stageId = validate(id.required().label('stageId'), req.params.stageId);
    const query = validate(listSchema, req.query);
    const page = await listOpportunitiesInStage(pool, req.workspaceId, stageId, query.limit, query.cursor);
    res.json(page);
  });

  return router;
}
