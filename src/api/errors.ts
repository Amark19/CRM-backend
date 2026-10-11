import type { ErrorRequestHandler } from 'express';

export class HttpError extends Error {
  constructor(
    public status: number,
    message: string,
    public details?: unknown,
  ) {
    super(message);
  }
}

// Postgres error code for a foreign key violation.
const FOREIGN_KEY_VIOLATION = '23503';

export const errorHandler: ErrorRequestHandler = (err, _req, res, _next) => {
  if (err instanceof HttpError) {
    res.status(err.status).json({ error: err.message, details: err.details });
    return;
  }
  // The composite FK (workspace_id, stage_id) -> stages rejects a stage that
  // doesn't exist or belongs to another workspace; both look the same to the caller.
  if (err?.code === FOREIGN_KEY_VIOLATION) {
    res.status(422).json({ error: 'stage or workspace not found' });
    return;
  }
  console.error(err);
  res.status(500).json({ error: 'internal error' });
};
