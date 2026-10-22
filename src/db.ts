import { Pool, PoolClient, QueryResultRow } from 'pg';
import { Config, log } from './config';

export type Db = {
  name: string;
  pool: Pool;
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export function makePool(name: string, connectionString: string, max: number, latencyMs = 0): Db {
  const pool = new Pool({
    connectionString,
    max,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
    application_name: `jobsched-${name}`,
  });
  pool.on('error', (err) => {
    log('warn', 'idle database client error; pool will reconnect', { db: name, error: String(err) });
  });
  if (latencyMs > 0) {
    const original = pool.query.bind(pool) as typeof pool.query;
    (pool as unknown as { query: typeof pool.query }).query = ((...args: unknown[]) => {
      const apply = async () => {
        await sleep(latencyMs);
        return (original as (...a: unknown[]) => unknown)(...args);
      };
      return apply();
    }) as typeof pool.query;
  }
  return { name, pool };
}

export async function controlDb(cfg: Config): Promise<Db> {
  return makePool('control', cfg.CONTROL_DATABASE_URL, cfg.API_DB_POOL_MAX, cfg.DB_LATENCY_MS);
}

export function shardDbs(cfg: Config, max: number): Map<string, Db> {
  const map = new Map<string, Db>();
  for (const [id, dsn] of Object.entries(cfg.shardUrls)) {
    map.set(id, makePool(id, dsn, max, cfg.DB_LATENCY_MS));
  }
  return map;
}

export async function closeDbs(dbs: Iterable<Db>): Promise<void> {
  await Promise.allSettled([...dbs].map((d) => d.pool.end()));
}

export async function query<T extends QueryResultRow = QueryResultRow>(
  db: Db,
  text: string,
  values: unknown[] = [],
): Promise<T[]> {
  const res = await db.pool.query<T>(text, values);
  return res.rows;
}

export async function queryOne<T extends QueryResultRow = QueryResultRow>(
  db: Db,
  text: string,
  values: unknown[] = [],
): Promise<T | undefined> {
  return (await query<T>(db, text, values))[0];
}

export async function withTransaction<T>(db: Db, fn: (client: PoolClient) => Promise<T>): Promise<T> {
  const client = await db.pool.connect();
  try {
    await client.query('BEGIN');
    const out = await fn(client);
    await client.query('COMMIT');
    return out;
  } catch (err) {
    try {
      await client.query('ROLLBACK');
    } catch {
      /* connection may be gone */
    }
    throw err;
  } finally {
    client.release();
  }
}

export async function waitForDb(db: Db, timeoutMs = 60000): Promise<void> {
  const start = Date.now();
  let lastErr: unknown;
  while (Date.now() - start < timeoutMs) {
    try {
      await db.pool.query('SELECT 1');
      return;
    } catch (err) {
      lastErr = err;
      await sleep(500);
    }
  }
  throw new Error(`Database ${db.name} unavailable: ${String(lastErr)}`);
}
