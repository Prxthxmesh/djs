import { z } from 'zod';

const int = (def: number, min = 0, max = 10_000_000) =>
  z.coerce.number().int().min(min).max(max).default(def);

const envSchema = z.object({
  NODE_ENV: z.string().default('development'),
  CONTROL_DATABASE_URL: z.string().min(1),
  SHARD_DSN_MAP: z.string().min(2),
  SHARD_IDS: z.string().optional(),
  SERVICE_NAME: z.string().default('jobsched'),
  API_PORT: int(3000, 1, 65535),
  OPS_PORT: int(9101, 1, 65535),
  API_DB_POOL_MAX: int(20, 1, 200),
  WORKER_DB_POOL_MAX: int(18, 1, 200),
  SCHEDULER_DB_POOL_MAX: int(8, 1, 200),
  WORKER_ID: z.string().default(`worker-${process.pid}`),
  SCHEDULER_ID: z.string().default(`scheduler-${process.pid}`),
  WORKER_CONCURRENCY: int(64, 1, 1000),
  CLAIM_BATCH_SIZE: int(100, 1, 500),
  COMPLETION_BATCH_SIZE: int(100, 1, 500),
  WORKER_LEASE_TTL_MS: int(5000, 1000, 120000),
  WORKER_HEARTBEAT_MS: int(1500, 200, 60000),
  WORKER_POLL_MIN_MS: int(50, 1, 5000),
  WORKER_POLL_MAX_MS: int(1000, 10, 30000),
  REAPER_INTERVAL_MS: int(1000, 100, 30000),
  SCHEDULER_LEASE_TTL_MS: int(10000, 2000, 120000),
  SCHEDULER_RENEW_MS: int(2000, 200, 60000),
  SCHEDULER_POLL_MS: int(25, 1, 5000),
  SCHEDULER_ACQUIRE_BATCH: int(64, 1, 64),
  TRIGGER_BATCH_SIZE: int(500, 1, 1000),
  MISFIRE_GRACE_MS: int(30000, 0, 3600000),
  QUEUE_BACKPRESSURE_THRESHOLD: int(20000, 1, 10_000_000),
  BACKPRESSURE_CACHE_MS: int(250, 0, 60000),
  // Optional bearer token / x-api-key for the API routes. Empty disables authentication.
  API_AUTH_TOKEN: z.string().optional(),
  // Maximum active/paused/triggered jobs per tenant. 0 disables the quota.
  TENANT_MAX_ACTIVE_JOBS: int(0, 0, 10_000_000),
  SHUTDOWN_GRACE_MS: int(25000, 1000, 120000),
  DB_LATENCY_MS: int(0, 0, 10000),
  LOG_LEVEL: z.enum(['debug', 'info', 'warn', 'error']).default('info'),
});

export type Config = z.infer<typeof envSchema> & {
  shardUrls: Record<string, string>;
};

let cached: Config | undefined;

export function loadConfig(overrides: Record<string, string | undefined> = {}): Config {
  if (cached && Object.keys(overrides).length === 0) return cached;
  const parsed = envSchema.safeParse({ ...process.env, ...overrides });
  if (!parsed.success) {
    const detail = parsed.error.issues.map((i) => `${i.path.join('.')}: ${i.message}`).join('; ');
    throw new Error(`Invalid configuration: ${detail}`);
  }
  const e = parsed.data;
  let shardUrls: Record<string, string>;
  try {
    shardUrls = JSON.parse(e.SHARD_DSN_MAP) as Record<string, string>;
  } catch {
    throw new Error('SHARD_DSN_MAP must be a JSON object mapping shard id to connection string');
  }
  if (!shardUrls || Object.keys(shardUrls).length === 0) throw new Error('SHARD_DSN_MAP is empty');
  const cfg: Config = { ...e, shardUrls };
  if (Object.keys(overrides).length === 0) cached = cfg;
  return cfg;
}

export function shardIds(cfg: Config): string[] {
  const ids = cfg.SHARD_IDS ? cfg.SHARD_IDS.split(',').map((s) => s.trim()).filter(Boolean) : Object.keys(cfg.shardUrls).sort();
  for (const id of ids) {
    if (!cfg.shardUrls[id]) throw new Error(`No DSN for shard ${id} in SHARD_DSN_MAP`);
  }
  return ids;
}

export function log(level: 'debug' | 'info' | 'warn' | 'error', msg: string, extra?: Record<string, unknown>): void {
  const line = JSON.stringify({ ts: new Date().toISOString(), level, msg, ...extra });
  if (level === 'error') console.error(line);
  else if (level === 'warn') console.warn(line);
  else console.log(line);
}
