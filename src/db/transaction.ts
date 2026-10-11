import type { Pool, PoolClient } from 'pg';

// A transaction lives on one connection, so every statement inside `work`
// must go through the `client` passed in, never through `pool`.
// COMMIT if `work` resolves, ROLLBACK if it throws, and always hand the
// connection back to the pool.
export async function withTransaction<T>(
  pool: Pool,
  work: (client: PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await work(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}
