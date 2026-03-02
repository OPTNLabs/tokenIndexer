BEGIN;

ALTER TABLE token_outpoints
  ADD COLUMN IF NOT EXISTS locking_address TEXT;

ALTER TABLE token_holders
  ADD COLUMN IF NOT EXISTS locking_address TEXT;

CREATE INDEX IF NOT EXISTS idx_token_holders_category_locking_address
  ON token_holders (category, locking_address)
  WHERE locking_address IS NOT NULL;

COMMIT;
