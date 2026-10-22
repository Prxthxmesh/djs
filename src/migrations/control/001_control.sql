CREATE TABLE IF NOT EXISTS shard_map (
  partition_id SMALLINT PRIMARY KEY CHECK (partition_id BETWEEN 0 AND 63),
  shard_id TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS partition_leases (
  partition_id SMALLINT PRIMARY KEY CHECK (partition_id BETWEEN 0 AND 63),
  owner_id TEXT,
  fencing_token BIGINT NOT NULL DEFAULT 0,
  lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO partition_leases (partition_id)
SELECT generate_series(0, 63)
ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS scheduler_registry (
  id TEXT PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  name TEXT NOT NULL,
  partition_id SMALLINT NOT NULL CHECK (partition_id BETWEEN 0 AND 63),
  schedule_type TEXT NOT NULL CHECK (schedule_type IN ('once', 'cron')),
  cron_expression TEXT,
  timezone TEXT NOT NULL DEFAULT 'UTC',
  next_run_at TIMESTAMPTZ,
  misfire_policy TEXT NOT NULL DEFAULT 'skip' CHECK (misfire_policy IN ('skip', 'run_once')),
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  max_attempts INT NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 100),
  status TEXT NOT NULL DEFAULT 'active' CHECK (status IN ('active', 'paused', 'cancelled', 'completed', 'triggered')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_triggered_at TIMESTAMPTZ,
  CHECK ((schedule_type = 'cron' AND cron_expression IS NOT NULL) OR schedule_type = 'once')
);

CREATE INDEX IF NOT EXISTS jobs_due_idx ON jobs (partition_id, next_run_at) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS jobs_list_idx ON jobs (created_at DESC);
CREATE INDEX IF NOT EXISTS jobs_tenant_idx ON jobs (tenant_id, created_at DESC);

