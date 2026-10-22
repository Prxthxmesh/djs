CREATE INDEX IF NOT EXISTS jobs_due_time_idx
  ON jobs(next_run_at)
  WHERE status='active' AND next_run_at IS NOT NULL;
