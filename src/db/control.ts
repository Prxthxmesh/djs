import { Db, query, queryOne, withTransaction } from '../db';
import { Job } from '../domain/types';
import { partitionFor } from '../domain/partition';
import { nextCronOccurrence, validateCron } from '../domain/cron';
import { randomUUID } from 'node:crypto';

export type CreateJobInput = {
  id?: string;
  tenantId: string;
  name: string;
  scheduleType: 'once' | 'cron';
  cronExpression?: string | null;
  timezone?: string;
  scheduledAt?: Date | null;
  misfirePolicy?: 'skip' | 'run_once';
  payload?: Record<string, unknown>;
  maxAttempts?: number;
};

export function buildJobRow(input: CreateJobInput): Job {
  const id = input.id ?? randomUUID();
  const tenantId = input.tenantId;
  const timezone = input.timezone ?? 'UTC';
  const scheduleType = input.scheduleType;
  let schedule: Date;
  if (scheduleType === 'once') {
    schedule = input.scheduledAt ?? new Date();
  } else {
    if (!input.cronExpression) throw new Error('cronExpression is required for cron jobs');
    validateCron(input.cronExpression, timezone);
    schedule = nextCronOccurrence(input.cronExpression, timezone, new Date());
  }
  return {
    id,
    tenant_id: tenantId,
    name: input.name,
    partition_id: partitionFor(tenantId, id),
    schedule_type: scheduleType,
    cron_expression: input.cronExpression ?? null,
    timezone,
    next_run_at: schedule,
    misfire_policy: input.misfirePolicy ?? 'skip',
    payload: input.payload ?? {},
    max_attempts: input.maxAttempts ?? 5,
    status: 'active',
    created_at: new Date(),
    updated_at: new Date(),
    last_triggered_at: null,
  };
}

export class TenantQuotaError extends Error {
  constructor(
    readonly tenantId: string,
    readonly limit: number,
  ) {
    super(`tenant ${tenantId} would exceed the active-job quota of ${limit}`);
    this.name = 'TenantQuotaError';
  }
}

export async function countTenantActiveJobs(control: Db, tenantId: string): Promise<number> {
  const row = await queryOne<{ n: number }>(
    control,
    "SELECT count(*)::int AS n FROM jobs WHERE tenant_id=$1 AND status IN ('active','paused','triggered')",
    [tenantId],
  );
  return row?.n ?? 0;
}

export async function insertJobs(
  control: Db,
  inputs: CreateJobInput[],
  opts: { maxActivePerTenant?: number } = {},
): Promise<Job[]> {
  const jobs = inputs.map(buildJobRow);
  if (jobs.length === 0) return [];
  const maxActive = opts.maxActivePerTenant ?? 0;
  return withTransaction(control, async (client) => {
    if (maxActive > 0) {
      const perTenant = new Map<string, number>();
      for (const j of jobs) perTenant.set(j.tenant_id, (perTenant.get(j.tenant_id) ?? 0) + 1);
      for (const [tenantId, added] of perTenant) {
        // Serialize quota checks for a tenant so concurrent batches cannot both slip under the cap.
        await client.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`tenant-quota:${tenantId}`]);
        const row = await client.query<{ n: number }>(
          `SELECT count(*)::int AS n FROM jobs
           WHERE tenant_id=$1 AND status IN ('active','paused','triggered')`,
          [tenantId],
        );
        if ((row.rows[0]?.n ?? 0) + added > maxActive) throw new TenantQuotaError(tenantId, maxActive);
      }
    }
    const payload = JSON.stringify(
      jobs.map((j) => ({
        id: j.id,
        tenant_id: j.tenant_id,
        name: j.name,
        partition_id: j.partition_id,
        schedule_type: j.schedule_type,
        cron_expression: j.cron_expression,
        timezone: j.timezone,
        next_run_at: j.next_run_at?.toISOString() ?? null,
        misfire_policy: j.misfire_policy,
        payload: j.payload,
        max_attempts: j.max_attempts,
      })),
    );
    const rows = await client.query<Job>(
      `INSERT INTO jobs (id, tenant_id, name, partition_id, schedule_type, cron_expression, timezone,
                         next_run_at, misfire_policy, payload, max_attempts)
       SELECT id, tenant_id, name, partition_id, schedule_type, cron_expression, timezone,
              next_run_at::timestamptz, misfire_policy, payload, max_attempts
       FROM jsonb_to_recordset($1::jsonb) AS x(
         id text, tenant_id text, name text, partition_id smallint, schedule_type text,
         cron_expression text, timezone text, next_run_at text, misfire_policy text,
         payload jsonb, max_attempts int)
       RETURNING *`,
      [payload],
    );
    return rows.rows;
  });
}

export async function listJobs(control: Db, limit = 100, tenantId?: string): Promise<Job[]> {
  if (tenantId) {
    return query<Job>(
      control,
      'SELECT * FROM jobs WHERE tenant_id=$1 ORDER BY created_at DESC LIMIT $2',
      [tenantId, limit],
    );
  }
  return query<Job>(control, 'SELECT * FROM jobs ORDER BY created_at DESC LIMIT $1', [limit]);
}

export async function getJob(control: Db, id: string): Promise<Job | undefined> {
  return queryOne<Job>(control, 'SELECT * FROM jobs WHERE id=$1', [id]);
}

export async function cancelJobRecord(control: Db, id: string): Promise<Job | undefined> {
  return queryOne<Job>(
    control,
    `UPDATE jobs SET status='cancelled', updated_at=now()
     WHERE id=$1 AND status IN ('active','paused','triggered') RETURNING *`,
    [id],
  );
}

export async function setJobStatus(control: Db, id: string, status: Job['status']): Promise<Job | undefined> {
  return queryOne<Job>(
    control,
    'UPDATE jobs SET status=$2, updated_at=now() WHERE id=$1 RETURNING *',
    [id, status],
  );
}

export type DueJob = Job & { fencing_token: string };

export async function fetchDueJobs(
  control: Db,
  ownerId: string,
  partitions: number[],
  limit: number,
  leadMs: number,
): Promise<DueJob[]> {
  if (partitions.length === 0) return [];
  return query<DueJob>(
    control,
    `SELECT j.*, l.fencing_token FROM jobs j
     JOIN partition_leases l ON l.partition_id = j.partition_id
     WHERE j.status='active' AND j.next_run_at IS NOT NULL
       AND j.next_run_at <= now() + ($4::int * interval '1 millisecond')
       AND j.partition_id = ANY($2::smallint[])
       AND l.owner_id=$1 AND l.lease_expires_at > now()
     ORDER BY j.next_run_at
     LIMIT $3`,
    [ownerId, partitions, limit, leadMs],
  );
}

export type Advancement = {
  id: string;
  expectedNext: Date;
  newNext: Date | null;
  type: 'once' | 'cron';
  partitionId: number;
  fenceToken: bigint;
};

/** Fenced batch advancement: a stale scheduler cannot advance a job it no longer owns. */
export async function advanceJobs(
  control: Db,
  ownerId: string,
  updates: Advancement[],
): Promise<number> {
  if (updates.length === 0) return 0;
  const payload = JSON.stringify(
    updates.map((u) => ({
      id: u.id,
      expected_next: u.expectedNext.toISOString(),
      new_next: u.newNext?.toISOString() ?? null,
      job_type: u.type,
      partition_id: u.partitionId,
      fence_token: u.fenceToken.toString(),
    })),
  );
  return withTransaction(control, async (client) => {
    const res = await client.query(
      `WITH input AS (
         SELECT * FROM jsonb_to_recordset($1::jsonb) AS x(
           id text, expected_next timestamptz, new_next timestamptz, job_type text,
           partition_id smallint, fence_token text)
       )
       UPDATE jobs j SET
         status = CASE WHEN x.job_type='once' THEN 'triggered' ELSE j.status END,
         next_run_at = CASE WHEN x.job_type='once' THEN NULL ELSE x.new_next END,
         last_triggered_at = now(),
         updated_at = now()
       FROM input x
       WHERE j.id=x.id AND j.status='active'
         AND date_trunc('milliseconds', j.next_run_at) = date_trunc('milliseconds', x.expected_next)
         AND EXISTS (
           SELECT 1 FROM partition_leases l
           WHERE l.partition_id=j.partition_id AND l.owner_id=$2
             AND l.fencing_token=x.fence_token::bigint AND l.lease_expires_at > now()
         )
       RETURNING j.id`,
      [payload, ownerId],
    );
    return res.rowCount ?? 0;
  });
}

export type Lease = { partition_id: number; fencing_token: string; lease_expires_at: Date };

export async function acquirePartitions(
  control: Db,
  ownerId: string,
  limit: number,
  ttlMs: number,
): Promise<Lease[]> {
  return query<Lease>(
    control,
    `WITH candidates AS (
       SELECT partition_id FROM partition_leases
       WHERE lease_expires_at < now()
       ORDER BY lease_expires_at
       LIMIT $2
       FOR UPDATE SKIP LOCKED
     )
     UPDATE partition_leases l
     SET owner_id=$1, fencing_token=l.fencing_token + 1,
         lease_expires_at=now() + ($3::int * interval '1 millisecond'), updated_at=now()
     FROM candidates c WHERE l.partition_id=c.partition_id
     RETURNING l.partition_id, l.fencing_token, l.lease_expires_at`,
    [ownerId, limit, ttlMs],
  );
}

export async function renewPartitions(control: Db, ownerId: string, ttlMs: number): Promise<Lease[]> {
  return query<Lease>(
    control,
    `UPDATE partition_leases
     SET lease_expires_at=now() + ($2::int * interval '1 millisecond'), updated_at=now()
     WHERE owner_id=$1 AND lease_expires_at > now()
     RETURNING partition_id, fencing_token, lease_expires_at`,
    [ownerId, ttlMs],
  );
}

export async function releasePartitions(control: Db, ownerId: string): Promise<void> {
  await query(
    control,
    "UPDATE partition_leases SET lease_expires_at=now(), updated_at=now() WHERE owner_id=$1",
    [ownerId],
  );
}

export async function releaseSomePartitions(
  control: Db,
  ownerId: string,
  partitionIds: number[],
): Promise<void> {
  if (partitionIds.length === 0) return;
  await query(
    control,
    `UPDATE partition_leases SET lease_expires_at=now(), updated_at=now()
     WHERE owner_id=$1 AND partition_id = ANY($2::smallint[])`,
    [ownerId, partitionIds],
  );
}

export async function heartbeatScheduler(control: Db, schedulerId: string): Promise<void> {
  await query(
    control,
    `INSERT INTO scheduler_registry(id, started_at, last_seen) VALUES ($1, now(), now())
     ON CONFLICT (id) DO UPDATE SET last_seen=now()`,
    [schedulerId],
  );
}

export async function activeSchedulerCount(control: Db, staleMs: number): Promise<number> {
  const row = await queryOne<{ n: number }>(
    control,
    'SELECT count(*)::int AS n FROM scheduler_registry WHERE last_seen > now() - ($1::int * interval \'1 millisecond\')',
    [staleMs],
  );
  return row?.n ?? 0;
}

export async function retireScheduler(control: Db, schedulerId: string): Promise<void> {
  await query(control, "UPDATE scheduler_registry SET last_seen='epoch' WHERE id=$1", [schedulerId]);
}

export async function getShardMap(control: Db): Promise<Map<number, string>> {
  const rows = await query<{ partition_id: number; shard_id: string }>(
    control,
    'SELECT partition_id, shard_id FROM shard_map ORDER BY partition_id',
  );
  return new Map(rows.map((r) => [r.partition_id, r.shard_id]));
}


