import { Client } from 'pg';
import { makePool } from '../src/db';
import { migrateDb, seedShardMap } from '../src/migrate';

// Use 127.0.0.1 rather than localhost: on some Docker Desktop/WSL2 setups the IPv6 ::1 listener
// accepts the socket and then resets it, while the IPv4 mapping works.
export const TEST_URLS = {
  control: process.env.TEST_CONTROL_URL ?? 'postgres://jobsched:jobsched@127.0.0.1:55432/jobsched_control_test',
  shardA: process.env.TEST_SHARD_A_URL ?? 'postgres://jobsched:jobsched@127.0.0.1:55433/jobsched_shard_test',
  shardB: process.env.TEST_SHARD_B_URL ?? 'postgres://jobsched:jobsched@127.0.0.1:55434/jobsched_shard_test',
};

async function recreate(url: string): Promise<void> {
  const parsed = new URL(url);
  const dbName = decodeURIComponent(parsed.pathname.replace(/^\//, ''));
  if (!dbName || !dbName.endsWith('_test')) throw new Error(`Refusing to recreate non-test database: ${dbName}`);
  const adminUrl = new URL(url);
  adminUrl.pathname = '/postgres';
  const admin = new Client({ connectionString: adminUrl.toString() });
  await admin.connect();
  try {
    await admin.query('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1', [dbName]);
    await admin.query(`DROP DATABASE IF EXISTS "${dbName}"`);
    await admin.query(`CREATE DATABASE "${dbName}"`);
  } finally {
    await admin.end();
  }
}

export default async function setup(): Promise<void> {
  for (const url of Object.values(TEST_URLS)) await recreate(url);
  const control = makePool('control-test', TEST_URLS.control, 4);
  await migrateDb(control, 'control');
  await seedShardMap(control, ['shard-a', 'shard-b']);
  const a = makePool('shard-a-test', TEST_URLS.shardA, 4);
  const b = makePool('shard-b-test', TEST_URLS.shardB, 4);
  await migrateDb(a, 'shard');
  await migrateDb(b, 'shard');
  await Promise.all([control, a, b].map((d) => d.pool.end()));
}

