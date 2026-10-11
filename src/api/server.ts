import { createPool } from '../db/pool';
import { createApp } from './app';

const PORT = Number(process.env.PORT ?? 3000);

const pool = createPool();
const server = createApp(pool).listen(PORT, () => {
  console.log(`api listening on :${PORT}`);
});

function shutdown(): void {
  server.close(() => {
    pool.end().then(() => process.exit(0));
  });
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
