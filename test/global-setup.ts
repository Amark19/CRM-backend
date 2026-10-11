import { migrate } from '../src/db/migrate';

export default async function setup(): Promise<void> {
  await migrate();
}
