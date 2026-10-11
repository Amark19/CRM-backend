import express, { type Express } from 'express';
import type { Pool } from 'pg';
import { bulkRouter } from '../bulk/bulk.routes';
import { errorHandler } from './errors';
import { opportunitiesRouter } from './opportunities.routes';
import { requireWorkspace } from './workspace';

// Built separately from server.ts so tests can mount the app on a test pool.
export function createApp(pool: Pool): Express {
  const app = express();
  app.use(express.json());

  app.get('/health', (_req, res) => {
    res.json({ ok: true });
  });

  app.use(requireWorkspace);
  app.use(opportunitiesRouter(pool));
  app.use(bulkRouter(pool));

  // Express 5 forwards rejected promises from async handlers here.
  app.use(errorHandler);
  return app;
}
