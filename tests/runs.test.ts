import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import {
  cancelJobRuns,
  cancelJobRunsEverywhere,
  claim,
  completeBatch,
  getRun,
  heartbeat,
  listDeadLetters,
  reapExpired,
  registerFence,
  triggerBatch,
} from '../src/db/shard';
import { queryOne } from '../src/db';
import { Trigger } from '../src/domain/types';
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

const shard = () => ctx.shards.get('shard-a')!;

describe('durable triggers and partition fencing', () => {
  it('inserts a run once under concurrent duplicate trigger attempts', async () => {
    const scheduled = new Date();
    const jobId = `dup-${randomUUID()}`;
    await registerFence(shard(), 0, 3n, 'owner', 600000);
    const trigger: Trigger = {
      runId: randomUUID(),
      jobId,
      tenantId: 't',
      partitionId: 0,
      scheduledTime: scheduled,
      runAt: new Date(),
      payload: {},
      maxAttempts: 3,
      triggerFence: 3n,
      idempotencyKey: `${jobId}:${scheduled.toISOString()}`,
    };
    const results = await Promise.all(Array.from({ length: 12 }, () => triggerBatch(shard(), [trigger])));
    expect(results.reduce((n, r) => n + r.inserted, 0)).toBe(1);
    expect(results.every((r) => r.valid === 1)).toBe(true);
    const runs = await queryOne<{ n: number }>(shard(), 'SELECT count(*)::int AS n FROM runs');
    const keys = await queryOne<{ n: number }>(shard(), 'SELECT count(*)::int AS n FROM run_keys');
    expect(runs?.n).toBe(1);
    expect(keys?.n).toBe(1);
  });

  it('rejects trigger writes carrying a stale fencing token', async () => {
    await registerFence(shard(), 0, 5n, 'new-owner', 600000);
    const scheduled = new Date();
    const trigger: Trigger = {
      runId: randomUUID(),
      jobId: 'stale',
      tenantId: 't',
      partitionId: 0,
      scheduledTime: scheduled,
      runAt: new Date(),
      payload: {},
      maxAttempts: 3,
      triggerFence: 4n,
      idempotencyKey: `stale:${scheduled.toISOString()}`,
    };
    const res = await triggerBatch(shard(), [trigger]);
    expect(res.valid).toBe(0);
    expect(res.inserted).toBe(0);
  });
});

describe('worker claims, leases, and fenced completion', () => {
  it('recovers an expired lease exactly once and ignores the stale worker result', async () => {
    const { runId } = await addRun(ctx, { partitionId: 0 });
    const [first] = await claim(shard(), 'worker-1', 10, 5000);
    expect(first?.id).toBe(runId);
    expect(first?.attempt).toBe(1);
    await shard().pool.query("UPDATE runs SET lease_expires_at=now() - interval '1 second' WHERE id=$1", [runId]);
    const reaped = await reapExpired(shard(), 10);
    expect(reaped.requeued).toBe(1);
    await shard().pool.query('UPDATE runs SET run_at=now() WHERE id=$1', [runId]);
    const [second] = await claim(shard(), 'worker-2', 10, 5000);
    expect(second?.attempt).toBe(2);
    expect(second?.lease_token).not.toBe(first?.lease_token);

    const stale = await completeBatch(shard(), [
      {
        runId,
        workerId: 'worker-1',
        shardId: 'shard-a',
        leaseToken: first!.lease_token,
        outcome: 'succeeded',
        error: null,
        retryAt: new Date(),
        durationMs: 1,
      },
    ]);
    expect(stale).toEqual([]);
    expect((await getRun(shard(), runId)).run?.status).toBe('running');

    const done = await completeBatch(shard(), [
      {
        runId,
        workerId: 'worker-2',
        shardId: 'shard-a',
        leaseToken: second!.lease_token,
        outcome: 'succeeded',
        error: null,
        retryAt: new Date(),
        durationMs: 1,
      },
    ]);
    expect(done[0]?.status).toBe('succeeded');
    const durable = await getRun(shard(), runId);
    expect(durable.isHistory).toBe(true);
    expect(durable.history?.status).toBe('succeeded');
    expect(durable.history?.attempt).toBe(2);
    expect(await queryOne<{ n: number }>(shard(), 'SELECT count(*)::int AS n FROM run_history')).toMatchObject({ n: 1 });
  });

  it('extends leases on heartbeat and reports cancellation', async () => {
    const { runId } = await addRun(ctx, { partitionId: 0 });
    const [claimed] = await claim(shard(), 'worker-1', 10, 5000);
    const before = await queryOne<{ lease_expires_at: Date }>(shard(), 'SELECT lease_expires_at FROM runs WHERE id=$1', [runId]);
    await new Promise((r) => setTimeout(r, 20));
    const hb = await heartbeat(shard(), 'worker-1', [{ runId, leaseToken: claimed!.lease_token }], 5000);
    expect(hb[0]?.cancel_requested).toBe(false);
    const after = await queryOne<{ lease_expires_at: Date }>(shard(), 'SELECT lease_expires_at FROM runs WHERE id=$1', [runId]);
    expect(after!.lease_expires_at.getTime()).toBeGreaterThanOrEqual(before!.lease_expires_at.getTime());

    await shard().pool.query('UPDATE runs SET cancel_requested=true WHERE id=$1', [runId]);
    const hb2 = await heartbeat(shard(), 'worker-1', [{ runId, leaseToken: claimed!.lease_token }], 5000);
    expect(hb2[0]?.cancel_requested).toBe(true);
  });
});

describe('retries and dead letters', () => {
  it('retries with backoff and dead-letters after exhaustion, preserving attempts and error', async () => {
    const { runId } = await addRun(ctx, { partitionId: 0, maxAttempts: 2 });
    const [first] = await claim(shard(), 'w1', 10, 5000);
    const r1 = await completeBatch(shard(), [
      {
        runId,
        workerId: 'w1',
        shardId: 'shard-a',
        leaseToken: first!.lease_token,
        outcome: 'failed',
        error: 'boom-1',
        retryAt: new Date(),
        durationMs: 1,
      },
    ]);
    expect(r1[0]?.status).toBe('queued');
    const queued = await queryOne<{ attempt: number; last_error: string }>(shard(), 'SELECT attempt, last_error FROM runs WHERE id=$1', [runId]);
    expect(queued).toMatchObject({ attempt: 1, last_error: 'boom-1' });

    const [second] = await claim(shard(), 'w2', 10, 5000);
    expect(second?.attempt).toBe(2);
    const r2 = await completeBatch(shard(), [
      {
        runId,
        workerId: 'w2',
        shardId: 'shard-a',
        leaseToken: second!.lease_token,
        outcome: 'failed',
        error: 'boom-2',
        retryAt: new Date(),
        durationMs: 1,
      },
    ]);
    expect(r2[0]?.status).toBe('dead');
    const dead = await listDeadLetters(shard(), 10);
    expect(dead).toHaveLength(1);
    expect(dead[0]).toMatchObject({ id: runId, status: 'dead', attempt: 2, max_attempts: 2, last_error: 'boom-2' });
    expect(await queryOne<{ n: number }>(shard(), 'SELECT count(*)::int AS n FROM runs')).toMatchObject({ n: 0 });
  });
});

describe('worker crash recovery', () => {
  it('dead-letters a run whose worker never returns after the lease expires', async () => {
    const { runId } = await addRun(ctx, { partitionId: 0, maxAttempts: 1 });
    const [claimed] = await claim(shard(), 'crashed-worker', 10, 5000);
    expect(claimed?.id).toBe(runId);
    await shard().pool.query("UPDATE runs SET lease_expires_at=now() - interval '1 second' WHERE id=$1", [runId]);

    const reaped = await reapExpired(shard(), 10);
    expect(reaped).toMatchObject({ expired: 1, requeued: 0, dead: 1, cancelled: 0 });

    expect(await queryOne<{ n: number }>(shard(), 'SELECT count(*)::int AS n FROM runs')).toMatchObject({ n: 0 });
    const durable = await getRun(shard(), runId);
    expect(durable.isHistory).toBe(true);
    expect(durable.history).toMatchObject({
      status: 'dead',
      attempt: 1,
      max_attempts: 1,
      worker_id: 'crashed-worker',
      last_error: 'lease expired after max attempts',
    });
    expect(durable.history?.finished_at).toBeInstanceOf(Date);
  });
});

describe('cancellation races', () => {
  it('cancels queued runs atomically and archives them', async () => {
    const { runId } = await addRun(ctx, { jobId: 'cancel-queued', partitionId: 0 });
    const result = await cancelJobRuns(shard(), 'cancel-queued');
    expect(result).toEqual({ cancelledQueued: 1, flaggedRunning: 0 });
    expect((await getRun(shard(), runId)).history?.status).toBe('cancelled');
    expect(await claim(shard(), 'w1', 10, 5000)).toEqual([]);
  });

  it('lets a running cancel win over a successful handler result', async () => {
    const { runId } = await addRun(ctx, { jobId: 'cancel-running', partitionId: 0 });
    const [claimed] = await claim(shard(), 'w1', 10, 5000);
    const cancel = await cancelJobRuns(shard(), 'cancel-running');
    expect(cancel.flaggedRunning).toBe(1);
    const done = await completeBatch(shard(), [
      {
        runId,
        workerId: 'w1',
        shardId: 'shard-a',
        leaseToken: claimed!.lease_token,
        outcome: 'succeeded',
        error: null,
        retryAt: new Date(),
        durationMs: 1,
      },
    ]);
    expect(done[0]?.status).toBe('cancelled');
    const durable = await getRun(shard(), runId);
    expect(durable.history?.status).toBe('cancelled');
    expect(durable.history?.cancel_requested).toBe(true);
  });

  it('reaches runs left on the old shard after a partition move', async () => {
    const jobId = `moved-${randomUUID()}`;
    const oldRun = await addRun(ctx, {
      shardId: 'shard-b',
      partitionId: 1,
      jobId,
      scheduledTime: new Date(Date.now() - 60000),
    });
    const newRun = await addRun(ctx, {
      shardId: 'shard-a',
      partitionId: 0,
      jobId,
      scheduledTime: new Date(),
    });

    const result = await cancelJobRunsEverywhere(ctx.shards.values(), jobId);
    expect(result.cancelledQueued).toBe(2);
    expect(result.flaggedRunning).toBe(0);
    expect(Object.keys(result.shards).sort()).toEqual(['shard-a-test', 'shard-b-test']);

    for (const [shardId, runId] of [
      ['shard-a', newRun.runId],
      ['shard-b', oldRun.runId],
    ] as const) {
      const durable = await getRun(ctx.shards.get(shardId)!, runId);
      expect(durable.history?.status).toBe('cancelled');
      expect(durable.history?.cancel_requested).toBe(true);
    }
  });

  it('never requeues a cancelled run during lease recovery', async () => {
    const { runId } = await addRun(ctx, { jobId: 'cancel-expiry', partitionId: 0 });
    const [claimed] = await claim(shard(), 'w1', 10, 5000);
    expect(claimed).toBeDefined();
    await cancelJobRuns(shard(), 'cancel-expiry');
    await shard().pool.query("UPDATE runs SET lease_expires_at=now() - interval '1 second' WHERE id=$1", [runId]);
    const reaped = await reapExpired(shard(), 10);
    expect(reaped).toMatchObject({ expired: 1, requeued: 0, cancelled: 1 });
    expect(await claim(shard(), 'w2', 10, 5000)).toEqual([]);
    expect((await getRun(shard(), runId)).history?.status).toBe('cancelled');
  });
});
