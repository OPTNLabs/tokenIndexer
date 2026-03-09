BEGIN;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_category_created_height
  ON token_outpoints (category, created_height DESC);

CREATE INDEX IF NOT EXISTS idx_token_outpoints_category_spent_height
  ON token_outpoints (category, spent_height DESC)
  WHERE spent_height IS NOT NULL;

COMMIT;
