-- Postgres table-level maintenance tuning for TokenIndex.
-- Apply on the primary database.

ALTER TABLE token_outpoints SET (
  autovacuum_vacuum_scale_factor = 0.01,
  autovacuum_vacuum_threshold = 20000,
  autovacuum_analyze_scale_factor = 0.005,
  autovacuum_analyze_threshold = 10000,
  autovacuum_vacuum_cost_limit = 4000
);

ALTER TABLE token_holders SET (
  autovacuum_vacuum_scale_factor = 0.02,
  autovacuum_vacuum_threshold = 5000,
  autovacuum_analyze_scale_factor = 0.01,
  autovacuum_analyze_threshold = 2500
);

ALTER TABLE token_stats SET (
  autovacuum_vacuum_scale_factor = 0.05,
  autovacuum_analyze_scale_factor = 0.02
);

ANALYZE token_outpoints;
ANALYZE token_holders;
ANALYZE token_stats;
