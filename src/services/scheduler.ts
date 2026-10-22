import { randomUUID } from 'node:crypto';
import { Config, log } from '../config';
import { Db, closeDbs, makePool, shardDbs, waitForDb } from '../db';
import {
  DueJob,
  acquirePartitions,
  activeSchedulerCount,
  advanceJobs,
  fetchDueJobs,
  getShardMap,
  heartbeatScheduler,
  releasePartitions,
  releaseSomePartitions,
  renewPartitions,
  retireScheduler,
} from '../db/control';
import { registerFence, triggerBatch } from '../db/shard';
import { decideCron } from '../domain/cron';
import { PARTITION_COUNT } from '../domain/partition';
import { Advancement } from '../db/control';
import { Trigger } from '../domain/types';
import { createMetrics, refreshPoolMetrics, Metrics } from '../metrics';
import { sleep } from '../ops';

type Owned = { shardId: string; token: bigint; expiresAt: Date };

export class SchedulerService {
  readonly cfg: Config;
  readonly metrics: Metrics;
  readonly id: string;
  private control!: Db;
  private shards: Map<string, Db> = new Map();
  private owned = new Map<number, Owned>();
  private shardMap = new Map<number, string>();
  private stopping = false;
  private loops: Promise<void>[] = [];

  constructor(cfg: Config) {
    this.cfg = cfg;
    this.id = cfg.SCHEDULER_ID;
    this.metrics = createMetrics('scheduler');
  }

  async start(): Promise<void> {
    this.control = makePool('control', this.cfg.CONTROL_DATABASE_URL, this.cfg.SCHEDULER_DB_POOL_MAX);
    await waitForDb(this.control);
    this.shards = shardDbs(this.cfg, this.cfg.SCHEDULER_DB_POOL_MAX);
    for (const db of this.shards.values()) await waitForDb(db);
    await this.loadShardMap();
    await this.rebalance();
    this.loops.push(this.leadershipLoop(), this.triggerLoop(), this.metricsLoop());
  }

  async stop(): Promise<void> {
    this.stopping = true;
    await Promise.race([Promise.allSettled(this.loops), sleep(this.cfg.SHUTDOWN_GRACE_MS)]);
    try {
      await releasePartitions(this.control, this.id);
      await retireScheduler(this.control, this.id);
    } catch (err) {
      log('warn', 'scheduler release failed', { error: String(err) });
    }
    await closeDbs([this.control, ...this.shards.values()]);
  }

  health(): Promise<{ ok: boolean; detail?: string }> {
    return this.control.pool
      .query('SELECT 1')
      .then(() => ({ ok: true }))
      .catch((err) => ({ ok: false, detail: String(err) }));
  }

  private async loadShardMap(): Promise<void> {
    this.shardMap = await getShardMap(this.control);
    for (const shard of new Set(this.shardMap.values())) {
      if (!this.shards.has(shard)) log('error', 'shard map references unconfigured shard', { shard });
    }
  }

  private shardFor(partitionId: number): string | undefined {
    const shard = this.shardMap.get(partitionId);
    if (!shard || !this.shards.has(shard)) return undefined;
    return shard;
  }

  private async registerOwnedFences(): Promise<void> {
    await Promise.all(
      [...this.owned.entries()].map(async ([partitionId, o]) => {
        const shardId = this.shardFor(partitionId);
        const shard = shardId ? this.shards.get(shardId) : undefined;
        if (!shardId || !shard) return;
        if (o.shardId !== shardId) o.shardId = shardId;
        try {
          await registerFence(shard, partitionId, o.token, this.id, this.cfg.SCHEDULER_LEASE_TTL_MS + 5000);
        } catch (err) {
          log('warn', 'fence registration failed', { partitionId, shard: shardId, error: String(err) });
        }
      }),
    );
  }

  private async rebalance(): Promise<void> {
    await heartbeatScheduler(this.control, this.id);
    await this.loadShardMap();
    const count = Math.max(1, await activeSchedulerCount(this.control, this.cfg.SCHEDULER_LEASE_TTL_MS));
    const fair = Math.ceil(PARTITION_COUNT / count);

    const renewed = await renewPartitions(this.control, this.id, this.cfg.SCHEDULER_LEASE_TTL_MS);
    const renewedIds = new Set(renewed.map((l) => l.partition_id));
    for (const partitionId of [...this.owned.keys()]) {
      if (!renewedIds.has(partitionId)) this.owned.delete(partitionId);
    }
    for (const lease of renewed) {
      const o = this.owned.get(lease.partition_id);
      if (o) {
        o.expiresAt = lease.lease_expires_at;
      }
    }

    if (this.owned.size > fair) {
      const excess = [...this.owned.keys()].sort((a, b) => b - a).slice(0, this.owned.size - fair);
      await releaseSomePartitions(this.control, this.id, excess);
      for (const p of excess) this.owned.delete(p);
      log('info', 'released partitions for rebalance', { count: excess.length, fair });
    }

    if (this.owned.size < fair) {
      const acquired = await acquirePartitions(this.control, this.id, Math.min(fair - this.owned.size, this.cfg.SCHEDULER_ACQUIRE_BATCH), this.cfg.SCHEDULER_LEASE_TTL_MS);
      for (const lease of acquired) {
        const shardId = this.shardFor(lease.partition_id);
        if (!shardId) {
          log('error', 'acquired partition without configured shard', { partitionId: lease.partition_id });
          continue;
        }
        this.owned.set(lease.partition_id, {
          shardId,
          token: BigInt(lease.fencing_token),
          expiresAt: lease.lease_expires_at,
        });
      }
      if (acquired.length > 0) {
        log('info', 'acquired partitions', { count: acquired.length, owned: this.owned.size, fair, schedulers: count });
      }
    }

    await this.registerOwnedFences();
    this.metrics.ownedPartitions.set(this.owned.size);
    refreshPoolMetrics(this.metrics, this.shards.values());
  }

  private async leadershipLoop(): Promise<void> {
    while (!this.stopping) {
      await sleep(this.cfg.SCHEDULER_RENEW_MS);
      if (this.stopping) break;
      try {
        await this.rebalance();
      } catch (err) {
        log('error', 'scheduler rebalance failed', { error: String(err) });
      }
    }
  }

  private async triggerLoop(): Promise<void> {
    while (!this.stopping) {
      let processed = 0;
      try {
        processed = await this.triggerTick();
      } catch (err) {
        log('error', 'trigger tick failed', { error: String(err) });
      }
      if (processed < this.cfg.TRIGGER_BATCH_SIZE) await sleep(this.cfg.SCHEDULER_POLL_MS);
    }
  }

  /** Exposed for focused tests: one scheduler decision cycle. */
  async triggerTick(): Promise<number> {
    if (this.owned.size === 0) return 0;
    const partitions = [...this.owned.keys()];
    const jobs = await fetchDueJobs(this.control, this.id, partitions, this.cfg.TRIGGER_BATCH_SIZE, this.cfg.SCHEDULER_POLL_MS);
    if (jobs.length === 0) return 0;

    const now = new Date();
    const groups = new Map<string, { triggers: Trigger[]; advancements: Advancement[] }>();
    const groupFor = (shardId: string) => {
      let g = groups.get(shardId);
      if (!g) {
        g = { triggers: [], advancements: [] };
        groups.set(shardId, g);
      }
      return g;
    };

    for (const job of jobs) {
      try {
        const shardId = this.shardFor(job.partition_id);
        if (!shardId) throw new Error(`No shard for partition ${job.partition_id}`);
        const g = groupFor(shardId);
        const expected = job.next_run_at!;
        const fence = BigInt(job.fencing_token);
        if (job.schedule_type === 'once') {
          const triggerAt = expected;
          g.triggers.push(this.makeTrigger(job, triggerAt, new Date(Math.max(now.getTime(), triggerAt.getTime())), fence));
          g.advancements.push({ id: job.id, expectedNext: expected, newNext: null, type: 'once', partitionId: job.partition_id, fenceToken: fence });
        } else {
          const decision = decideCron(
            job.cron_expression!,
            job.timezone,
            expected,
            now,
            job.misfire_policy,
            this.cfg.MISFIRE_GRACE_MS,
          );
          if (decision.triggerAt) {
            const runAt = new Date(Math.max(now.getTime(), decision.triggerAt.getTime()));
            g.triggers.push(this.makeTrigger(job, decision.triggerAt, runAt, fence));
          }
          g.advancements.push({
            id: job.id,
            expectedNext: expected,
            newNext: decision.nextRunAt,
            type: 'cron',
            partitionId: job.partition_id,
            fenceToken: fence,
          });
        }
      } catch (err) {
        log('warn', 'job decision failed', { jobId: job.id, error: String(err) });
      }
    }

    for (const [shardId, g] of groups) {
      const shard = this.shards.get(shardId);
      if (!shard) continue;
      try {
        let inserted = 0;
        if (g.triggers.length > 0) {
          const res = await triggerBatch(shard, g.triggers);
          if (res.valid !== g.triggers.length) {
            log('warn', 'trigger fence rejected; not advancing jobs', {
              shard: shardId,
              valid: res.valid,
              expected: g.triggers.length,
            });
            continue;
          }
          inserted = res.inserted;
          this.metrics.triggers.labels(shardId).inc(inserted);
          for (const t of g.triggers) {
            this.metrics.triggerLatency.observe(Math.max(0, (now.getTime() - t.scheduledTime.getTime()) / 1000));
          }
        }
        const advanced = await advanceJobs(this.control, this.id, g.advancements);
        if (advanced < g.advancements.length) {
          log('debug', 'some jobs not advanced (fencing or concurrent update)', {
            shard: shardId,
            advanced,
            expected: g.advancements.length,
          });
        }
        if (inserted < g.triggers.length) {
          log('debug', 'duplicate trigger delivery suppressed', { shard: shardId, duplicates: g.triggers.length - inserted });
        }
      } catch (err) {
        log('error', 'trigger batch failed', { shard: shardId, error: String(err) });
      }
    }
    return jobs.length;
  }

  private makeTrigger(job: DueJob, scheduledTime: Date, runAt: Date, fence: bigint): Trigger {
    return {
      runId: randomUUID(),
      jobId: job.id,
      tenantId: job.tenant_id,
      partitionId: job.partition_id,
      scheduledTime,
      runAt,
      payload: job.payload,
      maxAttempts: job.max_attempts,
      triggerFence: fence,
      idempotencyKey: `${job.id}:${scheduledTime.toISOString()}`,
    };
  }

  private async metricsLoop(): Promise<void> {
    while (!this.stopping) {
      refreshPoolMetrics(this.metrics, [this.control, ...this.shards.values()]);
      this.metrics.ownedPartitions.set(this.owned.size);
      await sleep(2000);
    }
  }
}

