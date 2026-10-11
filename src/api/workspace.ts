import type { RequestHandler } from 'express';
import { id, validate } from './validate';

// Teach TypeScript that every Express request carries a workspaceId.
// Safe because requireWorkspace runs before every route and sets it.
declare global {
  namespace Express {
    interface Request {
      workspaceId: number;
    }
  }
}

// Tenant scoping: every request must say which workspace it acts on.
// Every query downstream filters on this id; nothing reads across workspaces.
export const requireWorkspace: RequestHandler = (req, _res, next) => {
  req.workspaceId = validate(id.required().label('X-Workspace-Id'), req.header('X-Workspace-Id'));
  next();
};
