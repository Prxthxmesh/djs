import { Db, query, queryOne, withTransaction } from '../db';
import { RETRY_SQL } from '../domain/retry';
import { Run, RunResult, Trigger } from '../domain/types';

const HISTORY_COLUMNS = `id, job_id, tenant_id, partition_id, scheduled_time, idempotency_key, status,
  attempt, max_attempts, payload, run_at, created_at, triggered_at, started_at, finished_at,
  worker_id, cancel_requested, last_error, trigger_fence, lease_token`;

export type ClaimedRun = Run & { claim_wait_ms: number; shard_id?: string };

export type TriggerResult = { valid: number; inserted: number };

export async function registerFence(
  shard: Db,
  partitionId: number,
  fencingToken: bigint,
  ownerId: string,
  ttlMs: number,
): Promise<boolean> {
  const res = await shard.pool.query(
    `INSERT INTO partition_fence(partition_id, fencing_token, owner_id, fence_expires_at, updated_at)
     VALUES ($1,$2,$3, now() + ($4::int * interval '1 millisecond'), now())
     ON CONFLICT (partition_id) DO UPDATE
       SET fencing_token=EXCLUDED.fencing_token, owner_id=EXCLUDED.owner_id,
           fence_expires_at=EXCLUDED.fence_expires_at, updated_at=now()
       WHERE partition_fence.fencing_token <= EXCLUDED.fencing_token
     RETURNING partition_id`,
    [partitionId, fencingToken.toString(), ownerId, ttlMs],
  );
  return (res.rowCount ?? 0) > 0;
}

/**
 * Durable trigger insert. The shard-side fence token is checked in the same statement that
 * writes run_keys/runs, so a stale scheduler cannot mutate a shard after ownership moved.
 * ON CONFLICT on (job_id, scheduled_time) makes retries and duplicate scheduler deliveries safe.
 */
export async function triggerBatch(shard: Db, triggers: Trigger[]): Promise<TriggerResult> {
  if (triggers.length === 0) return { valid: 0, inserted: 0 };
  const payload = JSON.stringify(
    triggers.map((t) => ({
      run_id: t.runId,
      job_id: t.jobId,
      tenant_id: t.tenantId,
      partition_id: t.partitionId,
      scheduled_time: t.scheduledTime.toISOString(),
      run_at: t.runAt.toISOString(),
      payload: t.payload,
      max_attempts: t.maxAttempts,
      trigger_fence: t.triggerFence.toString(),
      idempotency_key: t.idempotencyKey,
    })),
  );
  const row = await queryOne<{ valid: number; inserted: number }>(
    shard,
    `WITH input AS (
       SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
         run_id text, job_id text, tenant_id text, partition_id smallint,
         scheduled_time timestamptz, run_at timestamptz, payload jsonb,
         max_attempts int, trigger_fence text, idempotency_key text)
     ), valid AS (
       SELECT i.* FROM input i
       JOIN partition_fence f ON f.partition_id=i.partition_id
         AND f.fencing_token=i.trigger_fence::bigint AND f.fence_expires_at > now()
     ), keys AS (
       INSERT INTO run_keys(job_id, scheduled_time, idempotency_key, run_id)
       SELECT job_id, scheduled_time, idempotency_key, run_id FROM valid
       ON CONFLICT DO NOTHING
       RETURNING job_id, scheduled_time
     ), ins AS (
       INSERT INTO runs(id, job_id, tenant_id, partition_id, scheduled_time, idempotency_key,
                        payload, run_at, triggered_at, max_attempts, trigger_fence)
       SELECT i.run_id, i.job_id, i.tenant_id, i.partition_id, i.scheduled_time, i.idempotency_key,
              i.payload, i.run_at, now(), i.max_attempts, i.trigger_fence::bigint
       FROM valid i JOIN keys k USING (job_id, scheduled_time)
       ON CONFLICT DO NOTHING
       RETURNING id
     )
     SELECT (SELECT count(*) FROM valid)::int AS valid, (SELECT count(*) FROM ins)::int AS inserted`,
    [payload],
  );
  return row ?? { valid: 0, inserted: 0 };
}

export async function claim(
  shard: Db,
  workerId: string,
  limit: number,
  leaseTtlMs: number,
): Promise<ClaimedRun[]> {
  return query<ClaimedRun>(
    shard,
    `WITH picked AS (
       SELECT id FROM runs
       WHERE status='queued' AND run_at <= now()
       ORDER BY run_at
       LIMIT $2
       FOR UPDATE SKIP LOCKED
     )
     UPDATE runs r
     SET status='running', worker_id=$1, attempt=r.attempt + 1,
         lease_token=r.lease_token + 1,
         lease_expires_at=now() + ($3::int * interval '1 millisecond'),
         started_at=COALESCE(r.started_at, now())
     FROM picked p
     WHERE r.id=p.id
     RETURNING r.*, EXTRACT(EPOCH FROM (now() - r.run_at)) * 1000 AS claim_wait_ms`,
    [workerId, limit, leaseTtlMs],
  );
}

export async function heartbeat(
  shard: Db,
  workerId: string,
  leases: Array<{ runId: string; leaseToken: string }>,
  leaseTtlMs: number,
): Promise<Array<{ id: string; cancel_requested: boolean }>> {
  if (leases.length === 0) return [];
  const payload = JSON.stringify(leases.map((l) => ({ run_id: l.runId, lease_token: l.leaseToken })));
  return query<{ id: string; cancel_requested: boolean }>(
    shard,
    `WITH input AS (
       SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(run_id text, lease_token text)
     ), updated AS (
       UPDATE runs r
       SET lease_expires_at=now() + ($3::int * interval '1 millisecond')
       FROM input i
       WHERE r.id=i.run_id AND r.status='running' AND r.worker_id=$2
         AND r.lease_token=i.lease_token::bigint
       RETURNING r.id, r.cancel_requested
     )
     SELECT * FROM updated`,
    [payload, workerId, leaseTtlMs],
  );
}

/**
 * Batch completion, all fenced by (worker_id, lease_token). Terminal rows move atomically to
 * run_history; retries return to queued with full-jitter backoff. A cancellation requested while
 * the handler ran wins over the handler result.
 */
export async function completeBatch(shard: Db, results: RunResult[]): Promise<Array<{ id: string; status: string }>> {
  if (results.length === 0) return [];
  const payload = JSON.stringify(
    results.map((r) => ({
      run_id: r.runId,
      worker_id: r.workerId,
      lease_token: r.leaseToken,
      outcome: r.outcome,
      error: r.error,
      retry_at: r.retryAt.toISOString(),
    })),
  );
  return withTransaction(shard, async (client) => {
    const res = await client.query<{ id: string; next_status: string }>(
      `WITH input AS (
         SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
           run_id text, worker_id text, lease_token text, outcome text, error text, retry_at timestamptz)
       ), locked AS (
         SELECT r.*, i.outcome, i.error, i.retry_at
         FROM runs r JOIN input i ON r.id=i.run_id
         WHERE r.status='running' AND r.lease_token=i.lease_token::bigint
           AND r.worker_id=i.worker_id
         FOR UPDATE OF r
       ), decided AS (
         SELECT l.*,
           CASE
             WHEN l.outcome='cancelled' OR l.cancel_requested THEN 'cancelled'
             WHEN l.outcome='succeeded' THEN 'succeeded'
             WHEN l.attempt >= l.max_attempts THEN 'dead'
             ELSE 'queued'
           END AS next_status
         FROM locked l
       ), updated AS (
         UPDATE runs r SET
           status = d.next_status,
           worker_id = CASE WHEN d.next_status='queued' THEN NULL ELSE r.worker_id END,
           lease_expires_at = CASE WHEN d.next_status='queued' THEN NULL ELSE r.lease_expires_at END,
           run_at = CASE WHEN d.next_status='queued' THEN d.retry_at ELSE r.run_at END,
           last_error = CASE WHEN d.next_status IN ('queued','dead') THEN d.error ELSE r.last_error END,
           finished_at = CASE WHEN d.next_status IN ('succeeded','dead','cancelled') THEN now() ELSE NULL END
         FROM decided d
         WHERE r.id=d.id
         RETURNING r.id, d.next_status
       )
       SELECT id, next_status FROM updated`,
      [payload],
    );
    const terminalIds = res.rows.filter((r) => r.next_status !== 'queued').map((r) => r.id);
    if (terminalIds.length > 0) {
      await client.query(
        `INSERT INTO run_history(${HISTORY_COLUMNS})
         SELECT ${HISTORY_COLUMNS} FROM runs WHERE id = ANY($1::text[]) AND status IN ('succeeded','dead','cancelled')
         ON CONFLICT DO NOTHING`,
        [terminalIds],
      );
      await client.query('DELETE FROM runs WHERE id = ANY($1::text[])', [terminalIds]);
    }
    return res.rows.map((r) => ({ id: r.id, status: r.next_status }));
  });
}

export type ReapResult = { expired: number; requeued: number; dead: number; cancelled: number };

export async function reapExpired(shard: Db, limit: number): Promise<ReapResult> {
  return withTransaction(shard, async (client) => {
    const res = await client.query<{ id: string; status: string }>(
      `WITH expired AS (
         SELECT id FROM runs
         WHERE status='running' AND lease_expires_at < now()
         ORDER BY lease_expires_at
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       ), updated AS (
         UPDATE runs r SET
           status = CASE WHEN r.cancel_requested THEN 'cancelled'
                         WHEN r.attempt >= r.max_attempts THEN 'dead'
                         ELSE 'queued' END,
           -- Keep the last worker on terminal rows for dead-letter diagnostics; clear it only
           -- when the run returns to the queue for another worker.
           worker_id = CASE WHEN r.cancel_requested OR r.attempt >= r.max_attempts THEN r.worker_id ELSE NULL END,
           lease_expires_at = NULL,
           run_at = CASE WHEN NOT r.cancel_requested AND r.attempt < r.max_attempts
                         THEN ${RETRY_SQL} ELSE r.run_at END,
           last_error = CASE WHEN r.cancel_requested THEN r.last_error
                             WHEN r.attempt >= r.max_attempts THEN 'lease expired after max attempts'
                             ELSE 'lease expired; retrying' END,
           finished_at = CASE WHEN r.cancel_requested OR r.attempt >= r.max_attempts THEN now() ELSE NULL END
         FROM expired e WHERE r.id=e.id
         RETURNING r.id, r.status
       )
       SELECT id, status FROM updated`,
      [limit],
    );
    const terminalIds = res.rows.filter((r) => r.status === 'cancelled' || r.status === 'dead').map((r) => r.id);
    if (terminalIds.length > 0) {
      await client.query(
        `INSERT INTO run_history(${HISTORY_COLUMNS})
         SELECT ${HISTORY_COLUMNS} FROM runs WHERE id = ANY($1::text[]) AND status IN ('cancelled','dead')
         ON CONFLICT DO NOTHING`,
        [terminalIds],
      );
      await client.query('DELETE FROM runs WHERE id = ANY($1::text[])', [terminalIds]);
    }
    return {
      expired: res.rowCount ?? 0,
      requeued: res.rows.filter((r) => r.status === 'queued').length,
      dead: res.rows.filter((r) => r.status === 'dead').length,
      cancelled: res.rows.filter((r) => r.status === 'cancelled').length,
    };
  });
}

/** Cancel queued runs atomically and flag running runs for cooperative cancellation. */
export async function cancelJobRuns(shard: Db, jobId: string): Promise<{ cancelledQueued: number; flaggedRunning: number }> {
  return withTransaction(shard, async (client) => {
    const queued = await client.query<{ id: string }>(
      `UPDATE runs SET status='cancelled', cancel_requested=true, finished_at=now()
       WHERE job_id=$1 AND status='queued' RETURNING id`,
      [jobId],
    );
    const ids = queued.rows.map((r) => r.id);
    if (ids.length > 0) {
      await client.query(
        `INSERT INTO run_history(${HISTORY_COLUMNS})
         SELECT ${HISTORY_COLUMNS} FROM runs WHERE id = ANY($1::text[])
         ON CONFLICT DO NOTHING`,
        [ids],
      );
      await client.query('DELETE FROM runs WHERE id = ANY($1::text[])', [ids]);
    }
    const running = await client.query(
      `UPDATE runs SET cancel_requested=true
       WHERE job_id=$1 AND status='running' RETURNING id`,
      [jobId],
    );
    return { cancelledQueued: ids.length, flaggedRunning: running.rowCount ?? 0 };
  });
}

/**
 * Cancellation must not depend on the partition's current shard map entry: a run may still be
 * active on the shard that owned the partition before a map change. Fan out to every configured
 * shard so old and new placements are both reached. The per-shard operation is idempotent.
 */
export async function cancelJobRunsEverywhere(
  shards: Iterable<Db>,
  jobId: string,
): Promise<{
  cancelledQueued: number;
  flaggedRunning: number;
  shards: Record<string, { cancelledQueued: number; flaggedRunning: number }>;
}> {
  const results = await Promise.all(
    [...shards].map(async (db) => ({ id: db.name, result: await cancelJobRuns(db, jobId) })),
  );
  const perShard: Record<string, { cancelledQueued: number; flaggedRunning: number }> = {};
  let cancelledQueued = 0;
  let flaggedRunning = 0;
  for (const { id, result } of results) {
    if (result.cancelledQueued > 0 || result.flaggedRunning > 0) perShard[id] = result;
    cancelledQueued += result.cancelledQueued;
    flaggedRunning += result.flaggedRunning;
  }
  return { cancelledQueued, flaggedRunning, shards: perShard };
}

export async function countActiveRunsEverywhere(shards: Iterable<Db>, jobId: string): Promise<number> {
  const counts = await Promise.all([...shards].map((db) => countActiveRuns(db, jobId)));
  return counts.reduce((a, b) => a + b, 0);
}

export async function listRunsForJobEverywhere(
  shards: Iterable<Db>,
  jobId: string,
  limit: number,
): Promise<Array<Run & { shard_id: string }>> {
  const lists = await Promise.all(
    [...shards].map(async (db) => (await listRunsForJob(db, jobId, limit)).map((r) => ({ ...r, shard_id: db.name }))),
  );
  return lists
    .flat()
    .sort((a, b) => b.scheduled_time.getTime() - a.scheduled_time.getTime())
    .slice(0, limit);
}

export async function getRun(shard: Db, runId: string): Promise<{ run?: Run; history?: Run; isHistory: boolean }> {
  const run = await queryOne<Run>(shard, 'SELECT * FROM runs WHERE id=$1', [runId]);
  if (run) return { run, isHistory: false };
  const history = await queryOne<Run>(shard, 'SELECT * FROM run_history WHERE id=$1', [runId]);
  return { history, isHistory: true };
}

export async function listRunsForJob(shard: Db, jobId: string, limit: number): Promise<Run[]> {
  const rows = await query<Run>(
    shard,
    `SELECT * FROM run_history WHERE job_id=$1 ORDER BY scheduled_time DESC LIMIT $2`,
    [jobId, limit],
  );
  const active = await query<Run>(
    shard,
    `SELECT * FROM runs WHERE job_id=$1 ORDER BY scheduled_time DESC LIMIT $2`,
    [jobId, limit],
  );
  return [...active, ...rows].sort((a, b) => b.scheduled_time.getTime() - a.scheduled_time.getTime()).slice(0, limit);
}

export async function listDeadLetters(shard: Db, limit: number): Promise<Run[]> {
  return query<Run>(
    shard,
    "SELECT * FROM run_history WHERE status='dead' ORDER BY finished_at DESC LIMIT $1",
    [limit],
  );
}

export async function listRecentHistory(shard: Db, limit: number): Promise<Run[]> {
  return query<Run>(shard, 'SELECT * FROM run_history ORDER BY finished_at DESC LIMIT $1', [limit]);
}

export async function countActiveRuns(shard: Db, jobId: string): Promise<number> {
  const row = await queryOne<{ n: number }>(
    shard,
    "SELECT count(*)::int AS n FROM runs WHERE job_id=$1 AND status IN ('queued','running')",
    [jobId],
  );
  return row?.n ?? 0;
}

export async function queueDepth(shard: Db): Promise<number> {
  const row = await queryOne<{ n: number }>(shard, "SELECT count(*)::int AS n FROM runs WHERE status='queued'");
  return row?.n ?? 0;
}

export async function activeDepth(shard: Db): Promise<{ queued: number; running: number }> {
  const row = await queryOne<{ queued: number; running: number }>(
    shard,
    `SELECT count(*) FILTER (WHERE status='queued')::int AS queued,
            count(*) FILTER (WHERE status='running')::int AS running
     FROM runs`,
  );
  return row ?? { queued: 0, running: 0 };
}

export async function latestTriggerLatency(shard: Db): Promise<number | null> {
  const row = await queryOne<{ ms: number | null }>(
    shard,
    `SELECT EXTRACT(EPOCH FROM (triggered_at - scheduled_time)) * 1000 AS ms
     FROM run_history ORDER BY finished_at DESC LIMIT 1`,
  );
  return row?.ms != null ? Number(row.ms) : null;
}

/** Used by tests and load verification: all durable records for a run prefix. */
export async function historyByJobPrefix(shard: Db, prefix: string): Promise<Array<{ job_id: string; status: string }>> {
  return query<{ job_id: string; status: string }>(
    shard,
    'SELECT job_id, status FROM run_history WHERE job_id LIKE $1',
    [`${prefix}%`],
  );
}
