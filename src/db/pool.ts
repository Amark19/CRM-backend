import { Pool, types } from 'pg';

export const DATABASE_URL =
  process.env.DATABASE_URL ?? 'postgres://app:app@localhost:5432/opportunities';

// pg returns bigint (int8) columns as strings because JS numbers lose precision
// above 2^53. Our ids and cent amounts stay far below that, so parse them as numbers.
types.setTypeParser(types.builtins.INT8, (value) => Number(value));

export function createPool(max = 10): Pool {
  return new Pool({ connectionString: DATABASE_URL, max });
}
