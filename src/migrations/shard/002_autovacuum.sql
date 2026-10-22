-- Shard tables are dominated by insert/update/delete churn: runs move to run_history on
-- completion, run_keys keep the idempotency ledger, and run_history is append-heavy. Tune
-- autovacuum per table so maintenance runs in short, frequent passes instead of one long pass
-- that blocks claim/trigger latency.
ALTER TABLE runs SET (
  autovacuum_enabled = true,
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 500,
  autovacuum_vacuum_insert_scale_factor = 0.02,
  autovacuum_vacuum_insert_threshold = 1000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 100,
  autovacuum_vacuum_cost_limit = 4000,
  autovacuum_vacuum_cost_delay = 2
);

ALTER TABLE run_keys SET (
  autovacuum_enabled = true,
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_threshold = 2000,
  autovacuum_vacuum_insert_scale_factor = 0.05,
  autovacuum_vacuum_insert_threshold = 5000,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_analyze_threshold = 1000,
  autovacuum_vacuum_cost_limit = 4000,
  autovacuum_vacuum_cost_delay = 2
);

ALTER TABLE run_history SET (
  autovacuum_enabled = true,
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_vacuum_threshold = 2000,
  autovacuum_vacuum_insert_scale_factor = 0.05,
  autovacuum_vacuum_insert_threshold = 5000,
  autovacuum_analyze_scale_factor = 0.02,
  autovacuum_analyze_threshold = 1000,
  autovacuum_vacuum_cost_limit = 4000,
  autovacuum_vacuum_cost_delay = 2
);
