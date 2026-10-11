import Joi from 'joi';
import type { OpportunityStatus } from '../domain/opportunities';

export const MAX_BULK_MOVE = 50_000;

export interface BulkMoveFilter {
  stageId?: number;
  ownerId?: string;
  status?: OpportunityStatus;
  valueMinCents?: number;
  valueMaxCents?: number;
  createdFrom?: string; // ISO timestamp, inclusive
  createdTo?: string; // ISO timestamp, exclusive
}

export const filterSchema = Joi.object<BulkMoveFilter>({
  stageId: Joi.number().integer().positive(),
  ownerId: Joi.string().trim().min(1).max(100),
  status: Joi.string().valid('open', 'won', 'lost', 'abandoned'),
  valueMinCents: Joi.number().integer().min(0),
  valueMaxCents: Joi.number().integer().min(0),
  createdFrom: Joi.string().isoDate(),
  createdTo: Joi.string().isoDate(),
}).min(1);

// The single definition of "this opportunity matches the bulk filter", used
// when taking the snapshot and again when each chunk re-checks a deal.
// The filter travels as one jsonb parameter; an absent key matches everything.
// `row` and `filterParam` are code constants (e.g. 'o', '$3::jsonb'), never user input.
export function filterPredicate(row: string, filterParam: string): string {
  const f = filterParam;
  return `(
        (${f}->>'stageId' IS NULL OR ${row}.stage_id = (${f}->>'stageId')::bigint)
    AND (${f}->>'ownerId' IS NULL OR ${row}.owner_id = ${f}->>'ownerId')
    AND (${f}->>'status' IS NULL OR ${row}.status = (${f}->>'status')::opportunity_status)
    AND (${f}->>'valueMinCents' IS NULL OR ${row}.value_cents >= (${f}->>'valueMinCents')::bigint)
    AND (${f}->>'valueMaxCents' IS NULL OR ${row}.value_cents <= (${f}->>'valueMaxCents')::bigint)
    AND (${f}->>'createdFrom' IS NULL OR ${row}.created_at >= (${f}->>'createdFrom')::timestamptz)
    AND (${f}->>'createdTo' IS NULL OR ${row}.created_at < (${f}->>'createdTo')::timestamptz)
  )`;
}
