CREATE TABLE IF NOT EXISTS partition_fence (
  partition_id SMALLINT PRIMARY KEY CHECK (partition_id BETWEEN 0 AND 63),
  fencing_token BIGINT NOT NULL,
  owner_id TEXT,
  fence_expires_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS run_keys (
  job_id TEXT NOT NULL,
  scheduled_time TIMESTAMPTZ NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  run_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (job_id, scheduled_time)
);

CREATE TABLE IF NOT EXISTS runs (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  partition_id SMALLINT NOT NULL CHECK (partition_id BETWEEN 0 AND 63),
  scheduled_time TIMESTAMPTZ NOT NULL,
  idempotency_key TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'dead', 'cancelled')),
  attempt INT NOT NULL DEFAULT 0,
  max_attempts INT NOT NULL DEFAULT 5,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  run_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  triggered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  lease_token BIGINT NOT NULL DEFAULT 0,
  lease_expires_at TIMESTAMPTZ,
  worker_id TEXT,
  cancel_requested BOOLEAN NOT NULL DEFAULT false,
  last_error TEXT,
  trigger_fence BIGINT NOT NULL,
  UNIQUE (job_id, scheduled_time)
);

CREATE INDEX IF NOT EXISTS runs_queued_idx ON runs (run_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS runs_running_lease_idx ON runs (lease_expires_at) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS runs_job_idx ON runs (job_id, scheduled_time DESC);
CREATE INDEX IF NOT EXISTS runs_worker_idx ON runs (worker_id) WHERE status = 'running';

CREATE TABLE IF NOT EXISTS run_history (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  partition_id SMALLINT NOT NULL,
  scheduled_time TIMESTAMPTZ NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'dead', 'cancelled')),
  attempt INT NOT NULL,
  max_attempts INT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  run_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  triggered_at TIMESTAMPTZ NOT NULL,
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ NOT NULL,
  worker_id TEXT,
  cancel_requested BOOLEAN NOT NULL DEFAULT false,
  last_error TEXT,
  trigger_fence BIGINT NOT NULL,
  lease_token BIGINT NOT NULL,
  UNIQUE (job_id, scheduled_time)
);

CREATE INDEX IF NOT EXISTS run_history_status_idx ON run_history (status, finished_at DESC);
CREATE INDEX IF NOT EXISTS run_history_job_idx ON run_history (job_id, scheduled_time DESC);
CREATE INDEX IF NOT EXISTS run_history_finished_idx ON run_history (finished_at DESC);
