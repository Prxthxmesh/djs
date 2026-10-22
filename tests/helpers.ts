import { randomUUID } from 'node:crypto';
import { makePool, Db } from '../src/db';
import { registerFence, triggerBatch } from '../src/db/shard';
import { Trigger } from '../src/domain/types';
import { TEST_URLS } from './global-setup';

export type TestContext = {
  control: Db;
  shards: Map<string, Db>;
  close: () => Promise<void>;
};

export function context(): TestContext {
  const control = makePool('control-test', TEST_URLS.control, 8);
  const shards = new Map<string, Db>([
    ['shard-a', makePool('shard-a-test', TEST_URLS.shardA, 8)],
    ['shard-b', makePool('shard-b-test', TEST_URLS.shardB, 8)],
  ]);
  return {
    control,
    shards,
    close: async () => {
      await Promise.allSettled([control, ...shards.values()].map((d) => d.pool.end()));
    },
  };
}

export async function resetAll(ctx: TestContext): Promise<void> {
  await ctx.control.pool.query('TRUNCATE jobs, partition_leases, scheduler_registry');
  await ctx.control.pool.query('INSERT INTO partition_leases(partition_id) SELECT generate_series(0,63)');
  await ctx.control.pool.query(
    `INSERT INTO shard_map(partition_id, shard_id)
     SELECT g, CASE WHEN g % 2 = 0 THEN 'shard-a' ELSE 'shard-b' END FROM generate_series(0,63) g
     ON CONFLICT (partition_id) DO UPDATE SET shard_id=EXCLUDED.shard_id`,
  );
  for (const db of ctx.shards.values()) {
    await db.pool.query('TRUNCATE runs, run_history, run_keys, partition_fence');
  }
}

export function shardForPartition(partitionId: number): string {
  return partitionId % 2 === 0 ? 'shard-a' : 'shard-b';
}

export async function addRun(
  ctx: TestContext,
  opts: {
    shardId?: string;
    jobId?: string;
    tenantId?: string;
    partitionId?: number;
    scheduledTime?: Date;
    maxAttempts?: number;
    fence?: bigint;
    payload?: Record<string, unknown>;
    runAt?: Date;
  } = {},
): Promise<{ inserted: number; runId: string }> {
  const jobId = opts.jobId ?? `job-${randomUUID()}`;
  const partitionId = opts.partitionId ?? 0;
  const shardId = opts.shardId ?? shardForPartition(partitionId);
  const shard = ctx.shards.get(shardId)!;
  const fence = opts.fence ?? 1n;
  await registerFence(shard, partitionId, fence, 'test-owner', 600000);
  const scheduled = opts.scheduledTime ?? new Date();
  const trigger: Trigger = {
    runId: randomUUID(),
    jobId,
    tenantId: opts.tenantId ?? 'tenant-test',
    partitionId,
    scheduledTime: scheduled,
    runAt: opts.runAt ?? new Date(),
    payload: opts.payload ?? {},
    maxAttempts: opts.maxAttempts ?? 5,
    triggerFence: fence,
    idempotencyKey: `${jobId}:${scheduled.toISOString()}`,
  };
  const res = await triggerBatch(shard, [trigger]);
  return { inserted: res.inserted, runId: trigger.runId };
}
