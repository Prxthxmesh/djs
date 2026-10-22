import { loadConfig, log, shardIds } from '../config';
import { Db, makePool } from '../db';

type Args = {
  days: number;
  runKeysDays: number;
  batch: number;
  dryRun: boolean;
};

function parseArgs(): Args {
  const argv = process.argv.slice(2);
  const get = (name: string): string | undefined => {
    const i = argv.indexOf(`--${name}`);
    return i >= 0 ? argv[i + 1] : undefined;
  };
  const days = Number(get('days') ?? process.env.RETENTION_DAYS ?? 30);
  const runKeysDays = Number(get('run-keys-days') ?? process.env.RETENTION_RUN_KEYS_DAYS ?? Math.max(days * 3, 90));
  const batch = Number(get('batch') ?? 5000);
  if (!Number.isFinite(days) || days < 0) throw new Error('--days must be a non-negative number');
  if (!Number.isFinite(runKeysDays) || runKeysDays < days) {
    throw new Error('--run-keys-days must be >= --days (run_keys is the idempotency ledger)');
  }
  if (!Number.isFinite(batch) || batch < 1) throw new Error('--batch must be positive');
  return { days, runKeysDays, batch, dryRun: argv.includes('--dry-run') };
}

async function deleteRows(
  db: Db,
  label: string,
  countSql: string,
  deleteSql: string,
  cutoff: Date,
  batch: number,
  dryRun: boolean,
): Promise<number> {
  if (dryRun) {
    const r = await db.pool.query<{ n: number }>(countSql, [cutoff]);
    return r.rows[0]?.n ?? 0;
  }
  let total = 0;
  for (;;) {
    const r = await db.pool.query(deleteSql, [cutoff, batch]);
    const deleted = r.rowCount ?? 0;
    total += deleted;
    if (deleted < batch) break;
  }
  log('info', 'retention delete', { db: db.name, table: label, deleted: total });
  return total;
}

async function main(): Promise<void> {
  const args = parseArgs();
  const cfg = loadConfig();
  const now = new Date();
  const historyCutoff = new Date(now.getTime() - args.days * 86400000);
  const runKeysCutoff = new Date(now.getTime() - args.runKeysDays * 86400000);

  const control = makePool('control', cfg.CONTROL_DATABASE_URL, 2);
  const shards = new Map<string, Db>();
  for (const id of shardIds(cfg)) shards.set(id, makePool(id, cfg.shardUrls[id]!, 2));

  const result: Record<string, unknown> = {
    timestamp: now.toISOString(),
    dryRun: args.dryRun,
    historyCutoff: historyCutoff.toISOString(),
    runKeysCutoff: runKeysCutoff.toISOString(),
    shards: {},
  };

  try {
    const shardResults: Record<string, unknown> = {};
    for (const [id, db] of shards) {
      const history = await deleteRows(
        db,
        'run_history',
        'SELECT count(*)::int AS n FROM run_history WHERE finished_at < $1',
        `DELETE FROM run_history WHERE id IN (
           SELECT id FROM run_history WHERE finished_at < $1 ORDER BY finished_at LIMIT $2
         ) RETURNING id`,
        historyCutoff,
        args.batch,
        args.dryRun,
      );
      const runKeys = await deleteRows(
        db,
        'run_keys',
        `SELECT count(*)::int AS n FROM run_keys k
         WHERE k.scheduled_time < $1
           AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.idempotency_key = k.idempotency_key)`,
        `DELETE FROM run_keys WHERE idempotency_key IN (
           SELECT k.idempotency_key FROM run_keys k
           WHERE k.scheduled_time < $1
             AND NOT EXISTS (SELECT 1 FROM runs r WHERE r.idempotency_key = k.idempotency_key)
           ORDER BY k.scheduled_time LIMIT $2
         ) RETURNING idempotency_key`,
        runKeysCutoff,
        args.batch,
        args.dryRun,
      );
      shardResults[id] = { runHistoryDeleted: history, runKeysDeleted: runKeys };
    }
    result.shards = shardResults;

    const jobs = await deleteRows(
      control,
      'jobs',
      `SELECT count(*)::int AS n FROM jobs
       WHERE status IN ('completed','cancelled') AND updated_at < $1`,
      `DELETE FROM jobs WHERE id IN (
         SELECT id FROM jobs
         WHERE status IN ('completed','cancelled') AND updated_at < $1
         ORDER BY updated_at LIMIT $2
       ) RETURNING id`,
      historyCutoff,
      args.batch,
      args.dryRun,
    );
    result.terminalJobsDeleted = jobs;

    console.log(JSON.stringify(result, null, 2));
  } finally {
    await Promise.all([control, ...shards.values()].map((d) => d.pool.end()));
  }
}

main().catch((err) => {
  log('error', 'retention failed', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
