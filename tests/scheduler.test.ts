import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config';
import { SchedulerService } from '../src/services/scheduler';
import { insertJobs } from '../src/db/control';
import { queryOne } from '../src/db';
import { TEST_URLS } from './global-setup';
import { context, resetAll, TestContext } from './helpers';

let ctx: TestContext;
let running: SchedulerService[] = [];

beforeAll(() => {
  ctx = context();
});
afterAll(async () => {
  await ctx.close();
});
beforeEach(async () => {
  await resetAll(ctx);
});
afterEach(async () => {
  await Promise.all(running.map((s) => s.stop()));
  running = [];
});

function cfgFor(id: string) {
  return loadConfig({
    CONTROL_DATABASE_URL: TEST_URLS.control,
    SHARD_DSN_MAP: JSON.stringify({ 'shard-a': TEST_URLS.shardA, 'shard-b': TEST_URLS.shardB }),
    SHARD_IDS: 'shard-a,shard-b',
    SCHEDULER_ID: id,
    SCHEDULER_LEASE_TTL_MS: '3000',
    SCHEDULER_RENEW_MS: '400',
    SCHEDULER_POLL_MS: '20',
    SCHEDULER_ACQUIRE_BATCH: '64',
    MISFIRE_GRACE_MS: '1000',
    DB_LATENCY_MS: '0',
    SHUTDOWN_GRACE_MS: '2000',
  });
}

async function waitFor(check: () => Promise<boolean>, timeoutMs: number, label: string): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`timeout waiting for ${label}`);
}

async function durableCount(): Promise<number> {
  let total = 0;
  for (const db of ctx.shards.values()) {
    const row = await queryOne<{ n: number }>(db, 'SELECT count(*)::int AS n FROM runs');
    total += row?.n ?? 0;
  }
  return total;
}

describe('distributed scheduler coordination', () => {
  it('runs a due cron job once with two concurrent schedulers and again after restart', async () => {
    const job = (
      await insertJobs(ctx.control, [
        {
          tenantId: 'tenant',
          name: 'cron-once',
          scheduleType: 'cron',
          cronExpression: '*/5 * * * *',
          timezone: 'UTC',
          misfirePolicy: 'run_once',
        },
      ])
    )[0]!;
    await ctx.control.pool.query("UPDATE jobs SET next_run_at=now() - interval '2 minutes' WHERE id=$1", [job.id]);

    const s1 = new SchedulerService(cfgFor('s1'));
    const s2 = new SchedulerService(cfgFor('s2'));
    running.push(s1, s2);
    await s1.start();
    await s2.start();

    await waitFor(async () => (await durableCount()) === 1, 10000, 'first cron trigger');
    await new Promise((r) => setTimeout(r, 1500));
    expect(await durableCount()).toBe(1);

    const jobShard = ctx.shards.get(job.partition_id % 2 === 0 ? 'shard-a' : 'shard-b')!;
    const firstRun = await queryOne<{ scheduled_time: Date; status: string; attempt: number }>(
      jobShard,
      'SELECT scheduled_time, status, attempt FROM runs LIMIT 1',
    );
    expect(firstRun?.status).toBe('queued');

    await Promise.all(running.map((s) => s.stop()));
    running = [];

    await ctx.control.pool.query("UPDATE jobs SET next_run_at=now() - interval '1 minute', status='active' WHERE id=$1", [job.id]);
    const s3 = new SchedulerService(cfgFor('s3'));
    running.push(s3);
    await s3.start();
    await waitFor(async () => (await durableCount()) === 2, 10000, 'cron trigger after scheduler restart');
    await new Promise((r) => setTimeout(r, 1200));
    expect(await durableCount()).toBe(2);

    const times = new Set<string>();
    for (const db of ctx.shards.values()) {
      const rows = await db.pool.query<{ scheduled_time: Date }>('SELECT scheduled_time FROM runs');
      for (const r of rows.rows) times.add(r.scheduled_time.toISOString());
    }
    expect(times.size).toBe(2);
  });

  it('re-registers shard fences after a shard map change', async () => {
    const created = await insertJobs(
      ctx.control,
      Array.from({ length: 9 }, (_, i) => ({
        tenantId: 'tenant',
        name: `remap-${i}`,
        scheduleType: 'once' as const,
        scheduledAt: new Date(Date.now() + 3600000),
      })),
    );
    const first = created[0]!;
    const second = created.slice(1).find((j) => j.partition_id % 2 === 1)!;
    expect(second.id).not.toBe(first.id);
    expect(second.partition_id % 2).toBe(1);
    await ctx.control.pool.query("UPDATE jobs SET next_run_at=now() WHERE id=$1", [first.id]);

    const s1 = new SchedulerService(cfgFor('s1'));
    running.push(s1);
    await s1.start();
    await waitFor(async () => (await durableCount()) === 1, 10000, 'first trigger before remap');

    await ctx.control.pool.query("UPDATE shard_map SET shard_id='shard-a'");
    await waitFor(
      async () => {
        const row = await queryOne<{ n: number }>(
          ctx.shards.get('shard-a')!,
          'SELECT count(*)::int AS n FROM partition_fence WHERE partition_id=$1',
          [second.partition_id],
        );
        return (row?.n ?? 0) === 1;
      },
      10000,
      'fence on new shard',
    );
    await ctx.control.pool.query("UPDATE jobs SET next_run_at=now() WHERE id=$1", [second.id]);

    await waitFor(
      async () => {
        const row = await queryOne<{ n: number }>(
          ctx.shards.get('shard-a')!,
          'SELECT count(*)::int AS n FROM runs WHERE job_id=$1',
          [second.id],
        );
        return (row?.n ?? 0) === 1;
      },
      15000,
      'trigger after shard map change',
    );

    // A map change must route the occurrence to exactly one shard. The old shard may still hold
    // an unexpired fence, so check both shards and the shard-local idempotency ledger.
    await new Promise((r) => setTimeout(r, 1500));
    const countOn = async (shardId: string) => {
      const row = await queryOne<{ n: number }>(
        ctx.shards.get(shardId)!,
        `SELECT (SELECT count(*) FROM runs WHERE job_id=$1)::int
              + (SELECT count(*) FROM run_history WHERE job_id=$1)::int AS n`,
        [second.id],
      );
      return row?.n ?? 0;
    };
    expect(await countOn('shard-a')).toBe(1);
    expect(await countOn('shard-b')).toBe(0);
    const keys = await queryOne<{ n: number }>(
      ctx.shards.get('shard-a')!,
      'SELECT count(*)::int AS n FROM run_keys WHERE job_id=$1',
      [second.id],
    );
    expect(keys?.n).toBe(1);
  });
});

