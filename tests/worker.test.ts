import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config';
import { queryOne } from '../src/db';
import { availableSlots, WorkerService } from '../src/services/worker';
import { TEST_URLS } from './global-setup';
import { addRun, context, resetAll, TestContext } from './helpers';

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

function workerCfg() {
  return loadConfig({
    CONTROL_DATABASE_URL: TEST_URLS.control,
    SHARD_DSN_MAP: JSON.stringify({ 'shard-a': TEST_URLS.shardA, 'shard-b': TEST_URLS.shardB }),
    SHARD_IDS: 'shard-a,shard-b',
    WORKER_ID: 'worker-concurrency-test',
    WORKER_CONCURRENCY: '4',
    CLAIM_BATCH_SIZE: '10',
    COMPLETION_BATCH_SIZE: '10',
    WORKER_LEASE_TTL_MS: '5000',
    WORKER_HEARTBEAT_MS: '500',
    WORKER_POLL_MIN_MS: '5',
    WORKER_POLL_MAX_MS: '20',
    REAPER_INTERVAL_MS: '500',
    DB_LATENCY_MS: '0',
    SHUTDOWN_GRACE_MS: '5000',
  });
}

describe('worker concurrency accounting', () => {
  it('counts active handlers and uncommitted completions against the budget', () => {
    expect(availableSlots(4, 0, 0)).toBe(4);
    expect(availableSlots(4, 4, 0)).toBe(0);
    expect(availableSlots(4, 0, 4)).toBe(0);
    expect(availableSlots(4, 1, 2)).toBe(1);
    expect(availableSlots(4, 5, 3)).toBe(0);
  });

  it('never exceeds the durable running budget while completions are slow', async () => {
    const shard = ctx.shards.get('shard-a')!;
    await shard.pool.query(`
      CREATE OR REPLACE FUNCTION test_slow_run_delete() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN PERFORM pg_sleep(0.05); RETURN OLD; END $$;
    `);
    await shard.pool.query('CREATE TRIGGER test_slow_run_delete BEFORE DELETE ON runs FOR EACH ROW EXECUTE FUNCTION test_slow_run_delete()');

    const total = 20;
    for (let i = 0; i < total; i++) await addRun(ctx, { partitionId: 0 });

    const worker = new WorkerService(workerCfg(), async () => {});
    let maxRunning = 0;
    try {
      await worker.start();
      const deadline = Date.now() + 4000;
      while (Date.now() < deadline) {
        const row = await queryOne<{ n: number }>(
          shard,
          "SELECT count(*)::int AS n FROM runs WHERE status='running'",
        );
        maxRunning = Math.max(maxRunning, row?.n ?? 0);
        const done = await queryOne<{ n: number }>(shard, "SELECT count(*)::int AS n FROM run_history WHERE status='succeeded'");
        if ((done?.n ?? 0) >= total) break;
        await new Promise((r) => setTimeout(r, 20));
      }
    } finally {
      await shard.pool.query('DROP TRIGGER IF EXISTS test_slow_run_delete ON runs');
      await shard.pool.query('DROP FUNCTION IF EXISTS test_slow_run_delete()');
    }

    const history = await queryOne<{ n: number }>(
      shard,
      "SELECT count(*)::int AS n FROM run_history WHERE status='succeeded'",
    );
    const active = await queryOne<{ n: number }>(shard, "SELECT count(*)::int AS n FROM runs WHERE status IN ('queued','running')");
    await worker.stop();

    expect(maxRunning).toBeLessThanOrEqual(4);
    expect(history?.n).toBe(total);
    expect(active?.n).toBe(0);
  });
});
