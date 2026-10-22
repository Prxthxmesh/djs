import fs from 'node:fs';
import path from 'node:path';
import { loadConfig, shardIds, log } from './config';
import { makePool, waitForDb, Db, withTransaction } from './db';

function migrationDir(kind: 'control' | 'shard'): string {
  const candidates = [
    path.join(__dirname, 'migrations', kind),
    path.join(process.cwd(), 'src', 'migrations', kind),
    path.join(__dirname, '..', 'src', 'migrations', kind),
  ];
  const found = candidates.find((p) => fs.existsSync(p));
  if (!found) throw new Error(`Migration directory for ${kind} not found`);
  return found;
}

export async function migrateDb(db: Db, kind: 'control' | 'shard'): Promise<void> {
  await waitForDb(db);
  await withTransaction(db, async (client) => {
    await client.query("SELECT pg_advisory_xact_lock(hashtext('jobsched_migrations'))");
    await client.query(
      'CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())',
    );
    const dir = migrationDir(kind);
    const files = fs.readdirSync(dir).filter((f) => f.endsWith('.sql')).sort();
    for (const file of files) {
      const existing = await client.query('SELECT 1 FROM schema_migrations WHERE version = $1', [file]);
      if (existing.rowCount) continue;
      const sql = fs.readFileSync(path.join(dir, file), 'utf8');
      await client.query(sql);
      await client.query('INSERT INTO schema_migrations(version) VALUES ($1)', [file]);
      log('info', 'migration applied', { db: db.name, file });
    }
  });
}

export async function seedShardMap(control: Db, ids: string[]): Promise<void> {
  if (ids.length === 0) throw new Error('At least one shard is required');
  await withTransaction(control, async (client) => {
    for (let p = 0; p < 64; p++) {
      const shard = ids[p % ids.length]!;
      await client.query(
        `INSERT INTO shard_map(partition_id, shard_id, updated_at) VALUES ($1, $2, now())
         ON CONFLICT (partition_id) DO NOTHING`,
        [p, shard],
      );
    }
  });
}

async function main(): Promise<void> {
  const cfg = loadConfig();
  const ids = shardIds(cfg);
  const control = makePool('control', cfg.CONTROL_DATABASE_URL, 2);
  await migrateDb(control, 'control');
  await seedShardMap(control, ids);
  const pools = ids.map((id) => makePool(id, cfg.shardUrls[id]!, 2));
  for (const pool of pools) {
    await migrateDb(pool, 'shard');
  }
  await Promise.all([control, ...pools].map((d) => d.pool.end()));
  log('info', 'migrations complete', { shards: ids });
}

if (require.main === module) {
  main().catch((err) => {
    log('error', 'migration failed', { error: String(err), stack: (err as Error).stack });
    process.exit(1);
  });
}
