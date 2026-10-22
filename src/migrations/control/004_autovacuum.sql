-- The jobs table is high-churn: every one-off job is inserted as active, updated when it is
-- triggered, and retained as terminal state. Default autovacuum (20% dead tuples + 60s naptime)
-- let dead tuples accumulate and produced multi-second trigger stalls during load.
ALTER TABLE jobs SET (
  autovacuum_enabled = true,
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 200,
  autovacuum_vacuum_insert_scale_factor = 0.02,
  autovacuum_vacuum_insert_threshold = 1000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 100,
  autovacuum_vacuum_cost_limit = 4000,
  autovacuum_vacuum_cost_delay = 5
);
