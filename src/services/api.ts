import crypto from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import Fastify, { FastifyInstance } from 'fastify';
import staticPlugin from '@fastify/static';
import { z } from 'zod';
import { Config } from '../config';
import { closeDbs, makePool, shardDbs, waitForDb } from '../db';
import {
  activeSchedulerCount,
  cancelJobRecord,
  getJob,
  getShardMap,
  insertJobs,
  listJobs,
  setJobStatus,
  TenantQuotaError,
} from '../db/control';
import {
  activeDepth,
  cancelJobRunsEverywhere,
  countActiveRunsEverywhere,
  listDeadLetters,
  listRecentHistory,
  listRunsForJobEverywhere,
  queueDepth,
} from '../db/shard';
import { Run } from '../domain/types';
import { createMetrics, refreshPoolMetrics, Metrics } from '../metrics';

const jobInput = z.object({
  id: z.string().min(1).max(200).optional(),
  tenantId: z.string().min(1).max(200),
  name: z.string().min(1).max(200),
  scheduleType: z.enum(['once', 'cron']),
  cronExpression: z.string().max(200).optional(),
  timezone: z.string().max(100).optional(),
  scheduledAt: z.coerce.date().optional(),
  misfirePolicy: z.enum(['skip', 'run_once']).optional(),
  payload: z.record(z.unknown()).optional(),
  maxAttempts: z.number().int().min(1).max(100).optional(),
});

const batchInput = z.object({ jobs: z.array(jobInput).min(1).max(500) });
const listQuery = z.object({ limit: z.coerce.number().int().min(1).max(500).default(100), tenantId: z.string().optional() });

type ShardRun = Run & { shard_id: string };

export type Api = {
  app: FastifyInstance;
  metrics: Metrics;
  close: () => Promise<void>;
};

export async function buildApi(cfg: Config): Promise<Api> {
  const control = makePool('control', cfg.CONTROL_DATABASE_URL, cfg.API_DB_POOL_MAX, cfg.DB_LATENCY_MS);
  await waitForDb(control);
  const shards = shardDbs(cfg, cfg.API_DB_POOL_MAX);
  for (const db of shards.values()) await waitForDb(db);
  const metrics = createMetrics('api');
  let shardMap = await getShardMap(control);
  let queueCache = { at: 0, value: 0 };

  const app = Fastify({ logger: false, bodyLimit: 8 * 1024 * 1024 });
  const authToken = cfg.API_AUTH_TOKEN?.trim() || null;
  if (authToken) {
    app.addHook('onRequest', async (req, reply) => {
      const route = (req.raw.url ?? req.url).split('?')[0] ?? '';
      const protectedRoute = ['/jobs', '/runs', '/dead-letters', '/shards'].some(
        (prefix) => route === prefix || route.startsWith(`${prefix}/`),
      );
      if (!protectedRoute) return;
      const header = req.headers.authorization;
      const bearer = header?.startsWith('Bearer ') ? header.slice('Bearer '.length).trim() : undefined;
      const rawKey = req.headers['x-api-key'];
      const apiKey = Array.isArray(rawKey) ? rawKey[0] : rawKey;
      const supplied = bearer ?? apiKey ?? '';
      const expected = Buffer.from(authToken);
      const actual = Buffer.from(supplied);
      if (expected.length !== actual.length || !crypto.timingSafeEqual(expected, actual)) {
        return reply.code(401).send({ error: 'unauthorized' });
      }
    });
  }
  app.addHook('onResponse', async (req, reply) => {
    if (reply.statusCode >= 500) console.error(JSON.stringify({ level: 'error', msg: 'http 5xx', url: req.url, code: reply.statusCode }));
  });

  async function currentQueueDepth(): Promise<number> {
    if (Date.now() - queueCache.at < cfg.BACKPRESSURE_CACHE_MS) return queueCache.value;
    const values = await Promise.all([...shards.values()].map(queueDepth));
    const total = values.reduce((a, b) => a + b, 0);
    queueCache = { at: Date.now(), value: total };
    return total;
  }

  function shardForPartition(partitionId: number): string | undefined {
    return shardMap.get(partitionId);
  }

  async function refreshShardMap(): Promise<void> {
    shardMap = await getShardMap(control);
  }

  app.get('/health', async (_req, reply) => {
    try {
      await Promise.all([control.pool.query('SELECT 1'), ...[...shards.values()].map((s) => s.pool.query('SELECT 1'))]);
      return { status: 'ok', shards: [...shards.keys()] };
    } catch (err) {
      return reply.code(503).send({ status: 'degraded', detail: String(err) });
    }
  });

  app.get('/metrics', async (_req, reply) => {
    const depths = await Promise.all([...shards.entries()].map(async ([id, db]) => [id, await queueDepth(db)] as const));
    for (const [id, depth] of depths) metrics.queueDepth.labels(id).set(depth);
    refreshPoolMetrics(metrics, [control, ...shards.values()]);
    reply.header('content-type', metrics.registry.contentType);
    return metrics.registry.metrics();
  });

  app.get('/jobs', async (req) => {
    const q = listQuery.parse(req.query);
    const jobs = await listJobs(control, q.limit, q.tenantId);
    await refreshShardMap();
    return {
      jobs: jobs.map((j) => ({
        ...j,
        shardId: shardForPartition(j.partition_id) ?? null,
      })),
    };
  });

  app.get('/jobs/:id', async (req, reply) => {
    const { id } = req.params as { id: string };
    const job = await getJob(control, id);
    if (!job) return reply.code(404).send({ error: 'job not found' });
    await refreshShardMap();
    return { ...job, shardId: shardForPartition(job.partition_id) ?? null };
  });

  app.post('/jobs', async (req, reply) => {
    const parsed = jobInput.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: 'validation', detail: parsed.error.issues });
    const depth = await currentQueueDepth();
    if (depth > cfg.QUEUE_BACKPRESSURE_THRESHOLD) {
      metrics.backpressure.inc();
      reply.header('Retry-After', '1');
      return reply.code(429).send({ error: 'backpressure', queueDepth: depth, limit: cfg.QUEUE_BACKPRESSURE_THRESHOLD });
    }
    try {
      const jobs = await insertJobs(control, [toCreateInput(parsed.data)], {
        maxActivePerTenant: cfg.TENANT_MAX_ACTIVE_JOBS,
      });
      return reply.code(201).send({ job: jobs[0] });
    } catch (err) {
      if (err instanceof TenantQuotaError) {
        reply.header('Retry-After', '5');
        return reply.code(429).send({ error: 'tenant quota exceeded', tenantId: err.tenantId, limit: err.limit });
      }
      if (isUniqueViolation(err)) return reply.code(409).send({ error: 'duplicate job id' });
      throw err;
    }
  });

  app.post('/jobs/batch', async (req, reply) => {
    const parsed = batchInput.safeParse(req.body);
    if (!parsed.success) return reply.code(400).send({ error: 'validation', detail: parsed.error.issues });
    const depth = await currentQueueDepth();
    if (depth > cfg.QUEUE_BACKPRESSURE_THRESHOLD) {
      metrics.backpressure.inc();
      reply.header('Retry-After', '1');
      return reply.code(429).send({ error: 'backpressure', queueDepth: depth, limit: cfg.QUEUE_BACKPRESSURE_THRESHOLD });
    }
    try {
      const jobs = await insertJobs(control, parsed.data.jobs.map(toCreateInput), {
        maxActivePerTenant: cfg.TENANT_MAX_ACTIVE_JOBS,
      });
      return reply.code(201).send({ jobs, count: jobs.length });
    } catch (err) {
      if (err instanceof TenantQuotaError) {
        reply.header('Retry-After', '5');
        return reply.code(429).send({ error: 'tenant quota exceeded', tenantId: err.tenantId, limit: err.limit });
      }
      if (isUniqueViolation(err)) return reply.code(409).send({ error: 'duplicate job id' });
      throw err;
    }
  });

  app.post('/jobs/:id/cancel', async (req, reply) => {
    const { id } = req.params as { id: string };
    const job = await getJob(control, id);
    if (!job) return reply.code(404).send({ error: 'job not found' });
    if (job.status === 'completed') return reply.send({ jobId: id, message: 'already completed' });
    if (job.status === 'cancelled') return reply.send({ jobId: id, message: 'already cancelled' });
    await refreshShardMap();
    if (job.status === 'triggered' && (await countActiveRunsEverywhere(shards.values(), id)) === 0) {
      await setJobStatus(control, id, 'completed');
      return reply.send({ jobId: id, message: 'already completed' });
    }
    const cancelled = await cancelJobRecord(control, id);
    if (!cancelled) return reply.send({ jobId: id, message: 'already completed' });
    // Runs can live on the pre-move shard while the map already points at the new shard.
    const result = await cancelJobRunsEverywhere(shards.values(), id);
    return { jobId: id, status: 'cancelled', ...result };
  });

  app.get('/jobs/:id/runs', async (req, reply) => {
    const { id } = req.params as { id: string };
    const q = listQuery.parse(req.query);
    const job = await getJob(control, id);
    if (!job) return reply.code(404).send({ error: 'job not found' });
    await refreshShardMap();
    // Include runs left on an old shard after a partition move.
    const runs = await listRunsForJobEverywhere(shards.values(), id, q.limit);
    return { job, shardId: shardForPartition(job.partition_id) ?? null, runs };
  });

  app.get('/runs', async (req) => {
    const q = listQuery.parse(req.query);
    const lists = await Promise.all([...shards.entries()].map(async ([id, db]) => (await listRecentHistory(db, q.limit)).map((r) => ({ ...r, shard_id: id }))));
    const runs = mergeRuns(lists.flat(), q.limit);
    return { runs };
  });

  app.get('/dead-letters', async (req) => {
    const q = listQuery.parse(req.query);
    const lists = await Promise.all([...shards.entries()].map(async ([id, db]) => (await listDeadLetters(db, q.limit)).map((r) => ({ ...r, shard_id: id }))));
    const runs = mergeRuns(lists.flat(), q.limit);
    return { deadLetters: runs, count: runs.length };
  });

  app.get('/shards', async () => {
    await refreshShardMap();
    const shardsOut = await Promise.all(
      [...shards.entries()].map(async ([id, db]) => ({ shardId: id, ...(await activeDepth(db)) })),
    );
    const schedulers = await activeSchedulerCount(control, 15000);
    const partitions: Record<string, number> = {};
    for (const shard of shardMap.values()) partitions[shard] = (partitions[shard] ?? 0) + 1;
    return { shards: shardsOut, partitions, activeSchedulers: schedulers };
  });

  const webDist = [path.join(process.cwd(), 'web', 'dist'), path.join(__dirname, '..', '..', 'web', 'dist')].find((p) =>
    fs.existsSync(path.join(p, 'index.html')),
  );
  if (webDist) {
    await app.register(staticPlugin, { root: webDist, prefix: '/' });
    app.setNotFoundHandler((req, reply) => {
      if (req.method === 'GET' && !req.url.startsWith('/jobs') && !req.url.startsWith('/dead') && !req.url.startsWith('/metrics') && !req.url.startsWith('/health')) {
        return reply.sendFile('index.html');
      }
      return reply.code(404).send({ error: 'not found' });
    });
  }

  await app.listen({ port: cfg.API_PORT, host: '0.0.0.0' });

  return {
    app,
    metrics,
    close: async () => {
      await app.close();
      await closeDbs([control, ...shards.values()]);
    },
  };
}

function mergeRuns(runs: ShardRun[], limit: number): ShardRun[] {
  return runs
    .sort((a, b) => new Date(b.finished_at ?? b.scheduled_time).getTime() - new Date(a.finished_at ?? a.scheduled_time).getTime())
    .slice(0, limit);
}

function toCreateInput(j: z.infer<typeof jobInput>) {
  return {
    id: j.id,
    tenantId: j.tenantId,
    name: j.name,
    scheduleType: j.scheduleType,
    cronExpression: j.cronExpression,
    timezone: j.timezone,
    scheduledAt: j.scheduledAt,
    misfirePolicy: j.misfirePolicy,
    payload: j.payload,
    maxAttempts: j.maxAttempts,
  };
}

function isUniqueViolation(err: unknown): boolean {
  return typeof err === 'object' && err !== null && 'code' in err && (err as { code?: string }).code === '23505';
}
