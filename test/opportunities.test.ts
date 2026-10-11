import request from 'supertest';
import { afterAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../src/api/app';
import { createPool } from '../src/db/pool';
import { createWorkspace, type TestWorkspace } from './helpers';

const pool = createPool();
const app = createApp(pool);
afterAll(() => pool.end());

let ws: TestWorkspace;
beforeEach(async () => {
  ws = await createWorkspace(pool);
});

function create(workspaceId: number, stageId: number, name = 'Deal') {
  return request(app)
    .post('/opportunities')
    .set('X-Workspace-Id', String(workspaceId))
    .send({ stageId, name, valueCents: 10_000, ownerId: 'owner-1' });
}

function move(workspaceId: number, id: number, body: object) {
  return request(app)
    .post(`/opportunities/${id}/move`)
    .set('X-Workspace-Id', String(workspaceId))
    .send(body);
}

async function transitionsOf(opportunityId: number) {
  const { rows } = await pool.query(
    `SELECT from_stage_id AS "from", to_stage_id AS "to", source
       FROM stage_transitions WHERE opportunity_id = $1 ORDER BY id`,
    [opportunityId],
  );
  return rows;
}

describe('create', () => {
  it('creates an open opportunity at version 1', async () => {
    const res = await create(ws.workspaceId, ws.stageIds[0]);
    expect(res.status).toBe(201);
    expect(res.body).toMatchObject({ stageId: ws.stageIds[0], status: 'open', version: 1 });
  });

  it('requires the workspace header', async () => {
    const res = await request(app).post('/opportunities').send({});
    expect(res.status).toBe(400);
  });

  it("rejects another workspace's stage", async () => {
    const other = await createWorkspace(pool);
    const res = await create(ws.workspaceId, other.stageIds[0]);
    expect(res.status).toBe(422);
  });
});

describe('move', () => {
  it('moves the deal, bumps the version and records one transition', async () => {
    const opp = (await create(ws.workspaceId, ws.stageIds[0])).body;

    const res = await move(ws.workspaceId, opp.id, { toStageId: ws.stageIds[1] });

    expect(res.status).toBe(200);
    expect(res.body).toMatchObject({ stageId: ws.stageIds[1], version: 2 });
    expect(await transitionsOf(opp.id)).toEqual([
      { from: ws.stageIds[0], to: ws.stageIds[1], source: 'manual' },
    ]);
  });

  it('returns 409 on a stale expectedVersion and changes nothing', async () => {
    const opp = (await create(ws.workspaceId, ws.stageIds[0])).body;
    await move(ws.workspaceId, opp.id, { toStageId: ws.stageIds[1] });

    const res = await move(ws.workspaceId, opp.id, { toStageId: ws.stageIds[2], expectedVersion: 1 });

    expect(res.status).toBe(409);
    expect(res.body.details).toEqual({ currentVersion: 2 });
    expect(await transitionsOf(opp.id)).toHaveLength(1);
  });

  it('is a no-op when the deal is already in the target stage', async () => {
    const opp = (await create(ws.workspaceId, ws.stageIds[0])).body;

    const res = await move(ws.workspaceId, opp.id, { toStageId: ws.stageIds[0] });

    expect(res.body.version).toBe(1);
    expect(await transitionsOf(opp.id)).toHaveLength(0);
  });

  it("cannot move a deal into another workspace's stage", async () => {
    const other = await createWorkspace(pool);
    const opp = (await create(ws.workspaceId, ws.stageIds[0])).body;

    const res = await move(ws.workspaceId, opp.id, { toStageId: other.stageIds[0] });

    expect(res.status).toBe(422);
    expect(await transitionsOf(opp.id)).toHaveLength(0);
  });

  it("cannot see a deal from another workspace", async () => {
    const other = await createWorkspace(pool);
    const opp = (await create(ws.workspaceId, ws.stageIds[0])).body;

    const res = await move(other.workspaceId, opp.id, { toStageId: other.stageIds[1] });

    expect(res.status).toBe(404);
  });

  // Fails without SELECT ... FOR UPDATE: concurrent moves would read the same
  // from_stage_id and the transition history would no longer form a chain.
  it('keeps the transition history consistent under concurrent moves', async () => {
    const opp = (await create(ws.workspaceId, ws.stageIds[0])).body;
    const targets = Array.from({ length: 20 }, (_, i) => ws.stageIds[(i % 2) + 1]);

    await Promise.all(targets.map((toStageId) => move(ws.workspaceId, opp.id, { toStageId })));

    const history = await transitionsOf(opp.id);
    expect(history[0].from).toBe(ws.stageIds[0]);
    for (let i = 1; i < history.length; i++) {
      expect(history[i].from).toBe(history[i - 1].to);
    }
    const final = await pool.query('SELECT stage_id, version FROM opportunities WHERE id = $1', [opp.id]);
    expect(final.rows[0].stage_id).toBe(history[history.length - 1].to);
    expect(final.rows[0].version).toBe(history.length + 1);
  });
});

describe('list by stage', () => {
  it('pages through a stage with a cursor, scoped to the workspace', async () => {
    const other = await createWorkspace(pool);
    await create(other.workspaceId, other.stageIds[0]);
    for (let i = 0; i < 5; i++) await create(ws.workspaceId, ws.stageIds[0], `Deal ${i}`);
    await create(ws.workspaceId, ws.stageIds[1]);

    const seen: number[] = [];
    let cursor = 0;
    let pages = 0;
    do {
      const res = await request(app)
        .get(`/stages/${ws.stageIds[0]}/opportunities`)
        .query({ limit: 2, cursor })
        .set('X-Workspace-Id', String(ws.workspaceId));
      seen.push(...res.body.items.map((o: { id: number }) => o.id));
      cursor = res.body.nextCursor;
      pages++;
    } while (cursor !== null);

    expect(pages).toBe(3);
    expect(seen).toHaveLength(5);
    expect(new Set(seen).size).toBe(5);
  });
});
