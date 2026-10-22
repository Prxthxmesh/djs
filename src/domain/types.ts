export type JobStatus = 'active' | 'paused' | 'cancelled' | 'completed' | 'triggered';
export type ScheduleType = 'once' | 'cron';
export type RunStatus = 'queued' | 'running' | 'succeeded' | 'failed' | 'dead' | 'cancelled';
export type TerminalStatus = 'succeeded' | 'failed' | 'dead' | 'cancelled';

export type Job = {
  id: string;
  tenant_id: string;
  name: string;
  partition_id: number;
  schedule_type: ScheduleType;
  cron_expression: string | null;
  timezone: string;
  next_run_at: Date | null;
  misfire_policy: 'skip' | 'run_once';
  payload: Record<string, unknown>;
  max_attempts: number;
  status: JobStatus;
  created_at: Date;
  updated_at: Date;
  last_triggered_at: Date | null;
};

export type Run = {
  id: string;
  job_id: string;
  tenant_id: string;
  partition_id: number;
  scheduled_time: Date;
  idempotency_key: string;
  status: RunStatus;
  attempt: number;
  max_attempts: number;
  payload: Record<string, unknown>;
  run_at: Date;
  created_at: Date;
  triggered_at: Date;
  started_at: Date | null;
  finished_at: Date | null;
  lease_token: string;
  lease_expires_at: Date | null;
  worker_id: string | null;
  cancel_requested: boolean;
  last_error: string | null;
  trigger_fence: string;
};

export type Trigger = {
  runId: string;
  jobId: string;
  tenantId: string;
  partitionId: number;
  scheduledTime: Date;
  runAt: Date;
  payload: Record<string, unknown>;
  maxAttempts: number;
  triggerFence: bigint;
  idempotencyKey: string;
};

export type RunResult = {
  runId: string;
  workerId: string;
  shardId: string;
  leaseToken: string;
  outcome: 'succeeded' | 'failed' | 'cancelled';
  error: string | null;
  retryAt: Date;
  durationMs: number;
};
