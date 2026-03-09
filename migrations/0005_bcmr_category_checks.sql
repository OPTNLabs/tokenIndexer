BEGIN;

CREATE TABLE IF NOT EXISTS bcmr_category_checks (
  category BYTEA PRIMARY KEY,
  last_checked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_result TEXT NOT NULL,
  last_error TEXT,
  candidate_txid BYTEA,
  candidate_vout INTEGER
);

CREATE INDEX IF NOT EXISTS idx_bcmr_category_checks_result_checked
  ON bcmr_category_checks (last_result, last_checked_at);

COMMIT;
