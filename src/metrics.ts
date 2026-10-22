import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from 'prom-client';
import { Db } from './db';

export function createMetrics(service: string): {
  registry: Registry;
  triggerLatency: Histogram;
  claimLatency: Histogram;
  retries: Counter;
  deadLetters: Counter;
  leaseExpirations: Counter;
  runsCompleted: Counter;
  triggers: Counter;
  queueDepth: Gauge;
  ownedPartitions: Gauge;
  backpressure: Counter;
  dbPool: Gauge;
} {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry, prefix: 'jobsched_' });
  registry.setDefaultLabels({ service });
  return {
    registry,
    triggerLatency: new Histogram({
      name: 'jobsched_trigger_latency_seconds',
      help: 'Scheduled time to durable trigger insertion',
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 0.75, 1, 2, 5],
      registers: [registry],
    }),
    claimLatency: new Histogram({
      name: 'jobsched_claim_wait_seconds',
      help: 'Time a queued run waited before being claimed',
      labelNames: ['shard'],
      buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5],
      registers: [registry],
    }),
    retries: new Counter({
      name: 'jobsched_retries_total',
      help: 'Runs rescheduled for retry',
      labelNames: ['shard'],
      registers: [registry],
    }),
    deadLetters: new Counter({
      name: 'jobsched_dead_letters_total',
      help: 'Runs moved to dead-letter state',
      labelNames: ['shard'],
      registers: [registry],
    }),
    leaseExpirations: new Counter({
      name: 'jobsched_lease_expirations_total',
      help: 'Running leases recovered after expiry',
      labelNames: ['shard'],
      registers: [registry],
    }),
    runsCompleted: new Counter({
      name: 'jobsched_runs_completed_total',
      help: 'Runs reaching a terminal state',
      labelNames: ['shard', 'status'],
      registers: [registry],
    }),
    triggers: new Counter({
      name: 'jobsched_triggers_inserted_total',
      help: 'Durable runs inserted by schedulers',
      labelNames: ['shard'],
      registers: [registry],
    }),
    queueDepth: new Gauge({
      name: 'jobsched_queue_depth',
      help: 'Queued runs per shard',
      labelNames: ['shard'],
      registers: [registry],
    }),
    ownedPartitions: new Gauge({
      name: 'jobsched_owned_partitions',
      help: 'Logical partitions currently leased by this scheduler',
      registers: [registry],
    }),
    backpressure: new Counter({
      name: 'jobsched_backpressure_rejections_total',
      help: 'Requests rejected with 429 due to queue depth',
      registers: [registry],
    }),
    dbPool: new Gauge({
      name: 'jobsched_db_pool_connections',
      help: 'PostgreSQL pool connections',
      labelNames: ['db', 'state'],
      registers: [registry],
    }),
  };
}

export type Metrics = ReturnType<typeof createMetrics>;

export function refreshPoolMetrics(metrics: Metrics, dbs: Iterable<Db>): void {
  for (const db of dbs) {
    const p = db.pool;
    metrics.dbPool.labels(db.name, 'total').set(p.totalCount);
    metrics.dbPool.labels(db.name, 'idle').set(p.idleCount);
    metrics.dbPool.labels(db.name, 'waiting').set(p.waitingCount);
    metrics.dbPool.labels(db.name, 'active').set(p.totalCount - p.idleCount);
  }
}
