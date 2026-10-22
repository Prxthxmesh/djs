import { Config } from '../config';
import { Db, closeDbs, shardDbs, waitForDb } from '../db';
import { ClaimedRun, claim, completeBatch, heartbeat, queueDepth, reapExpired } from '../db/shard';
import { Run, RunResult } from '../domain/types';
import { retryDelayMs } from '../domain/retry';
import { createMetrics, refreshPoolMetrics, Metrics } from '../metrics';
import { backoffWithJitter, sleep } from '../ops';

export type JobHandler = (run: Run, signal: AbortSignal) => Promise<void>;

/**
 * A run keeps its concurrency slot until its completion transaction commits. Counting only
 * `active` (handler running) let a worker claim replacements while results sat in the pending
 * completion queue, so the durable `running` row count could exceed the configured budget by the
 * size of the backlog. Counting both bounds durable running rows to the budget.
 */
export function availableSlots(concurrency: number, active: number, pending: number): number {
  return Math.max(0, concurrency - active - pending);
}

export class ShutdownError extends Error {
  constructor() {
    super('worker shutting down');
  }
}

const defaultHandler: JobHandler = async (run, signal) => {
  const payload = run.payload as { sleepMs?: number; fail?: boolean; failTimes?: number };
  if (payload.sleepMs && payload.sleepMs > 0) {
    await new Promise<void>((resolve, reject) => {
      if (signal.aborted) return reject(new Error('aborted'));
      const t = setTimeout(resolve, Math.min(payload.sleepMs!, 120000));
      signal.addEventListener(
        'abort',
        () => {
          clearTimeout(t);
          reject(new Error('aborted'));
        },
        { once: true },
      );
    });
  }
  if (signal.aborted) throw new Error('aborted');
  if (payload.fail === true || (payload.failTimes !== undefined && run.attempt <= payload.failTimes)) {
    throw new Error(`handler failure (attempt ${run.attempt})`);
  }
};

type Active = {
  run: ClaimedRun;
  shardId: string;
  abort: AbortController;
  reason: 'cancel' | 'shutdown' | 'lease_lost' | null;
};

export class WorkerService {
  readonly cfg: Config;
  readonly metrics: Metrics;
  readonly workerId: string;
  private dbs: Map<string, Db> = new Map();
  private active = new Map<string, Active>();
  private pending: RunResult[] = [];
  // Results moved out of `pending` and currently being written by flushPending. They still
  // occupy concurrency slots until their completion transaction commits.
  private completing = 0;
  private stopping = false;
  private loops: Promise<void>[] = [];
  private emptyPolls = 0;
  private handler: JobHandler;

  constructor(cfg: Config, handler: JobHandler = defaultHandler) {
    this.cfg = cfg;
    this.workerId = cfg.WORKER_ID;
    this.metrics = createMetrics('worker');
    this.handler = handler;
  }

  async start(): Promise<void> {
    this.dbs = shardDbs(this.cfg, this.cfg.WORKER_DB_POOL_MAX);
    for (const db of this.dbs.values()) await waitForDb(db);
    this.loops.push(this.claimLoop(), this.completionLoop(), this.heartbeatLoop(), this.reaperLoop(), this.metricsLoop());
  }

  async stop(): Promise<void> {
    this.stopping = true;
    const deadline = Date.now() + this.cfg.SHUTDOWN_GRACE_MS;
    while (this.active.size > 0 && Date.now() < deadline) await sleep(25);
    if (this.active.size > 0) {
      for (const a of this.active.values()) {
        a.reason = 'shutdown';
        a.abort.abort('shutdown');
      }
      await sleep(100);
      await this.releaseLeases();
    }
    await Promise.race([
      Promise.allSettled(this.loops),
      sleep(Math.max(1000, this.cfg.SHUTDOWN_GRACE_MS)),
    ]);
    await this.flushPending(true);
    await closeDbs(this.dbs.values());
  }

  health(): Promise<{ ok: boolean; detail?: string }> {
    return Promise.all([...this.dbs.values()].map((d) => d.pool.query('SELECT 1')))
      .then(() => ({ ok: true }))
      .catch((err) => ({ ok: false, detail: String(err) }));
  }

  private async claimLoop(): Promise<void> {
    while (!this.stopping) {
      const occupied = () => this.active.size + this.pending.length + this.completing;
      const free = availableSlots(this.cfg.WORKER_CONCURRENCY, occupied(), 0);
      if (free <= 0) {
        await sleep(10);
        continue;
      }
      let claimedThisRound = 0;
      for (const [shardId, db] of this.dbs) {
        if (this.stopping) break;
        const slots = Math.min(
          this.cfg.CLAIM_BATCH_SIZE,
          availableSlots(this.cfg.WORKER_CONCURRENCY, occupied(), 0),
        );
        if (slots <= 0) break;
        try {
          const runs = await claim(db, this.workerId, slots, this.cfg.WORKER_LEASE_TTL_MS);
          claimedThisRound += runs.length;
          for (const run of runs) this.launch(shardId, run);
          if (runs.length > 0) {
            const n = Number(runs[0]!.claim_wait_ms);
            if (Number.isFinite(n)) this.metrics.claimLatency.labels(shardId).observe(Math.max(0, n / 1000));
          }
        } catch (err) {
          console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'warn', msg: 'claim failed', shard: shardId, error: String(err) }));
          await sleep(100);
        }
      }
      if (claimedThisRound > 0) {
        this.emptyPolls = 0;
        continue;
      }
      this.emptyPolls++;
      await sleep(backoffWithJitter(this.emptyPolls, this.cfg.WORKER_POLL_MIN_MS, this.cfg.WORKER_POLL_MAX_MS));
    }
  }

  private launch(shardId: string, run: ClaimedRun): void {
    const abort = new AbortController();
    const active: Active = { run, shardId, abort, reason: null };
    this.active.set(run.id, active);
    void this.execute(shardId, run, abort).then(
      (result) => {
        if (active.reason === 'shutdown' || active.reason === 'lease_lost') {
          this.active.delete(run.id);
          return;
        }
        this.pending.push(result);
        this.active.delete(run.id);
      },
      (err) => {
        console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', msg: 'execution crashed', runId: run.id, error: String(err) }));
        this.pending.push({
          runId: run.id,
          workerId: this.workerId,
          shardId,
          leaseToken: run.lease_token,
          outcome: 'failed',
          error: String(err),
          retryAt: new Date(Date.now() + retryDelayMs(run.attempt)),
          durationMs: 0,
        });
        this.active.delete(run.id);
      },
    );
  }

  private async execute(shardId: string, run: ClaimedRun, abort: AbortController): Promise<RunResult> {
    const started = Date.now();
    let outcome: RunResult['outcome'] = 'succeeded';
    let error: string | null = null;
    try {
      await this.handler(run, abort.signal);
      if (abort.signal.aborted) {
        outcome = 'cancelled';
        error = 'cancelled while running';
      }
    } catch (err) {
      if (abort.signal.aborted) {
        outcome = 'cancelled';
        error = 'cancelled while running';
      } else {
        outcome = 'failed';
        error = err instanceof Error ? err.message : String(err);
      }
    }
    return {
      runId: run.id,
      workerId: this.workerId,
      shardId,
      leaseToken: run.lease_token,
      outcome,
      error,
      retryAt: new Date(Date.now() + retryDelayMs(run.attempt)),
      durationMs: Date.now() - started,
    };
  }

  private async completionLoop(): Promise<void> {
    while (!this.stopping || this.pending.length > 0 || this.active.size > 0) {
      if (this.pending.length === 0) {
        await sleep(5);
        continue;
      }
      await this.flushPending(false);
    }
  }

  private async flushPending(final: boolean): Promise<void> {
    if (this.pending.length === 0) return;
    const batch = this.pending.splice(0, Math.min(this.pending.length, this.cfg.COMPLETION_BATCH_SIZE * this.dbs.size));
    this.completing += batch.length;
    const byShard = new Map<string, RunResult[]>();
    for (const r of batch) {
      const arr = byShard.get(r.shardId) ?? [];
      arr.push(r);
      byShard.set(r.shardId, arr);
    }
    try {
      for (const [shardId, results] of byShard) {
        const db = this.dbs.get(shardId);
        if (!db) continue;
        try {
          const updated = await completeBatch(db, results);
          const updatedMap = new Map(updated.map((u) => [u.id, u.status]));
          for (const r of results) {
            const status = updatedMap.get(r.runId);
            if (!status) continue;
            if (status === 'queued') this.metrics.retries.labels(shardId).inc();
            if (status === 'dead') this.metrics.deadLetters.labels(shardId).inc();
            if (status === 'succeeded' || status === 'dead' || status === 'cancelled') {
              this.metrics.runsCompleted.labels(shardId, status).inc();
            }
          }
        } catch (err) {
          if (final) {
            console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', msg: 'final completion failed; lease will expire', shard: shardId, error: String(err) }));
          } else {
            this.pending.unshift(...results);
            console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'warn', msg: 'completion failed; retrying', shard: shardId, error: String(err) }));
            await sleep(100);
          }
        }
      }
    } finally {
      this.completing -= batch.length;
    }
  }

  private async heartbeatLoop(): Promise<void> {
    while (!this.stopping) {
      await sleep(this.cfg.WORKER_HEARTBEAT_MS);
      if (this.active.size === 0) continue;
      const byShard = new Map<string, Active[]>();
      for (const a of this.active.values()) {
        const arr = byShard.get(a.shardId) ?? [];
        arr.push(a);
        byShard.set(a.shardId, arr);
      }
      for (const [shardId, active] of byShard) {
        const db = this.dbs.get(shardId);
        if (!db) continue;
        try {
          const leases = active.map((a) => ({ runId: a.run.id, leaseToken: a.run.lease_token }));
          const renewed = await heartbeat(db, this.workerId, leases, this.cfg.WORKER_LEASE_TTL_MS);
          const renewedIds = new Set(renewed.map((r) => r.id));
          const cancelIds = new Set(renewed.filter((r) => r.cancel_requested).map((r) => r.id));
          for (const a of active) {
            if (!renewedIds.has(a.run.id)) {
              a.reason = 'lease_lost';
              a.abort.abort('lease_lost');
            } else if (cancelIds.has(a.run.id) && a.reason !== 'cancel') {
              a.reason = 'cancel';
              a.abort.abort('cancel');
            }
          }
        } catch (err) {
          console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'warn', msg: 'heartbeat failed', shard: shardId, error: String(err) }));
        }
      }
    }
  }

  private async reaperLoop(): Promise<void> {
    while (!this.stopping) {
      await sleep(this.cfg.REAPER_INTERVAL_MS);
      for (const [shardId, db] of this.dbs) {
        try {
          const r = await reapExpired(db, 200);
          if (r.expired > 0) {
            this.metrics.leaseExpirations.labels(shardId).inc(r.expired);
            if (r.dead > 0) this.metrics.deadLetters.labels(shardId).inc(r.dead);
            if (r.cancelled > 0) this.metrics.runsCompleted.labels(shardId, 'cancelled').inc(r.cancelled);
          }
        } catch (err) {
          console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'warn', msg: 'reaper failed', shard: shardId, error: String(err) }));
        }
      }
    }
  }

  private async metricsLoop(): Promise<void> {
    while (!this.stopping) {
      for (const [shardId, db] of this.dbs) {
        try {
          this.metrics.queueDepth.labels(shardId).set(await queueDepth(db));
        } catch {
          /* transient */
        }
      }
      refreshPoolMetrics(this.metrics, this.dbs.values());
      await sleep(2000);
    }
  }

  private async releaseLeases(): Promise<void> {
    for (const db of this.dbs.values()) {
      try {
        await db.pool.query("UPDATE runs SET lease_expires_at=now() WHERE worker_id=$1 AND status='running'", [this.workerId]);
      } catch {
        /* best effort */
      }
    }
  }
}
