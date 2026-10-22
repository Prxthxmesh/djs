import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import {
  acquirePartitions,
  activeSchedulerCount,
  advanceJobs,
  buildJobRow,
  fetchDueJobs,
  getShardMap,
  heartbeatScheduler,
  insertJobs,
  releasePartitions,
  releaseSomePartitions,
  renewPartitions,
  retireScheduler,
} from '../src/db/control';
import { registerFence, triggerBatch } from '../src/db/shard';
import { queryOne } from '../src/db';
import { randomUUID } from 'node:crypto';
import { context, resetAll, TestContext } from './helpers';

let ctx: TestContext;
beforeAll(() => {
  ctx = context();
});
afterAll(async () => {
  await ctx.close();
});
beforeEach(async () => {
  await resetAll(ctx);
});

describe('partition ownership leases', () => {
  it('assigns partitions once, renews owned leases, and transfers with monotonically increasing fences', async () => {
    const a = await acquirePartitions(ctx.control, 'sched-a', 64, 10000);
    expect(a).toHaveLength(64);
    expect(new Set(a.map((l) => l.fencing_token)).size).toBe(1);
    const tokenA = BigInt(a[0]!.fencing_token);
    expect(tokenA).toBe(1n);

    const b = await acquirePartitions(ctx.control, 'sched-b', 64, 10000);
    expect(b).toHaveLength(0);

    const renewedA = await renewPartitions(ctx.control, 'sched-a', 10000);
    expect(renewedA).toHaveLength(64);

    await releaseSomePartitions(ctx.control, 'sched-a', Array.from({ length: 32 }, (_, i) => i));
    const renewedAfterRelease = await renewPartitions(ctx.control, 'sched-a', 10000);
    expect(renewedAfterRelease).toHaveLength(32);
    expect(renewedAfterRelease.every((l) => l.partition_id >= 32)).toBe(true);

    const b2 = await acquirePartitions(ctx.control, 'sched-b', 64, 10000);
    expect(b2).toHaveLength(32);
    const bToken = BigInt(b2[0]!.fencing_token);
    expect(bToken).toBeGreaterThan(tokenA);

    await releasePartitions(ctx.control, 'sched-a');
    expect(await acquirePartitions(ctx.control, 'sched-b', 64, 10000)).toHaveLength(32);
  });

  it('counts active schedulers and removes retired ones from the fair-share calculation', async () => {
    await heartbeatScheduler(ctx.control, 'one');
    await heartbeatScheduler(ctx.control, 'two');
    expect(await activeSchedulerCount(ctx.control, 60000)).toBe(2);
    await retireScheduler(ctx.control, 'two');
    expect(await activeSchedulerCount(ctx.control, 60000)).toBe(1);
  });
});

describe('fenced job advancement', () => {
  it('rejects the old owner after partition transfer and accepts the new fence token', async () => {
    await acquirePartitions(ctx.control, 'sched-a', 64, 10000);
    const job = buildJobRow({
      tenantId: 'tenant',
      name: 'fenced',
      scheduleType: 'once',
      scheduledAt: new Date(Date.now() - 5000),
    });
    const [inserted] = await insertJobs(ctx.control, [
      {
        id: job.id,
        tenantId: job.tenant_id,
        name: job.name,
        scheduleType: 'once',
        scheduledAt: job.next_run_at!,
      },
    ]);
    const owned = await fetchDueJobs(ctx.control, 'sched-a', [inserted!.partition_id], 10, 0);
    expect(owned).toHaveLength(1);
    const oldToken = BigInt(owned[0]!.fencing_token);
    expect(oldToken).toBe(1n);

    await releasePartitions(ctx.control, 'sched-a');
    await acquirePartitions(ctx.control, 'sched-b', 64, 10000);
    const advancedByOld = await advanceJobs(ctx.control, 'sched-a', [
      {
        id: job.id,
        expectedNext: job.next_run_at!,
        newNext: null,
        type: 'once',
        partitionId: job.partition_id,
        fenceToken: oldToken,
      },
    ]);
    expect(advancedByOld).toBe(0);

    const reclaimed = await fetchDueJobs(ctx.control, 'sched-b', [inserted!.partition_id], 10, 0);
    expect(reclaimed).toHaveLength(1);
    const newToken = BigInt(reclaimed[0]!.fencing_token);
    expect(newToken).toBeGreaterThan(oldToken);
    const advancedByNew = await advanceJobs(ctx.control, 'sched-b', [
      {
        id: job.id,
        expectedNext: job.next_run_at!,
        newNext: null,
        type: 'once',
        partitionId: job.partition_id,
        fenceToken: newToken,
      },
    ]);
    expect(advancedByNew).toBe(1);
    const row = await queryOne<{ status: string }>(ctx.control, 'SELECT status FROM jobs WHERE id=$1', [job.id]);
    expect(row?.status).toBe('triggered');
  });
});

describe('multi-shard routing and isolation', () => {
  it('maps 64 logical partitions to physical shards and writes runs to the routed shard only', async () => {
    const map = await getShardMap(ctx.control);
    expect(map.size).toBe(64);
    expect(new Set(map.values())).toEqual(new Set(['shard-a', 'shard-b']));
    for (const [partition, shard] of map) {
      expect(shard).toBe(partition % 2 === 0 ? 'shard-a' : 'shard-b');
    }

    for (const partition of [0, 1]) {
      const shardId = map.get(partition)!;
      await registerFence(ctx.shards.get(shardId)!, partition, 1n, 'owner', 600000);
      const scheduled = new Date();
      const jobId = `route-${partition}-${randomUUID()}`;
      const res = await triggerBatch(ctx.shards.get(shardId)!, [
        {
          runId: randomUUID(),
          jobId,
          tenantId: 't',
          partitionId: partition,
          scheduledTime: scheduled,
          runAt: new Date(),
          payload: {},
          maxAttempts: 3,
          triggerFence: 1n,
          idempotencyKey: `${jobId}:${scheduled.toISOString()}`,
        },
      ]);
      expect(res.inserted).toBe(1);
    }
    const a = await queryOne<{ n: number }>(ctx.shards.get('shard-a')!, 'SELECT count(*)::int AS n FROM runs');
    const b = await queryOne<{ n: number }>(ctx.shards.get('shard-b')!, 'SELECT count(*)::int AS n FROM runs');
    expect(a?.n).toBe(1);
    expect(b?.n).toBe(1);
    const aPartitions = await queryOne<{ p: number }>(ctx.shards.get('shard-a')!, 'SELECT min(partition_id)::int AS p FROM runs');
    expect(aPartitions?.p).toBe(0);
  });
});

