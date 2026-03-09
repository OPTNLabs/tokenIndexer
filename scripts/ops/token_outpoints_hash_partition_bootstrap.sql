-- Low-risk partition bootstrap for token_outpoints using HASH(txid).
-- This keeps the existing PK shape (txid, vout) compatible.
-- Run manually in a migration window and validate on staging first.

BEGIN;

CREATE TABLE IF NOT EXISTS token_outpoints_v2 (
  txid BYTEA NOT NULL,
  vout INTEGER NOT NULL CHECK (vout >= 0),
  category BYTEA NOT NULL,
  locking_bytecode BYTEA NOT NULL,
  locking_address TEXT,
  ft_amount NUMERIC(78,0),
  nft_capability SMALLINT,
  nft_commitment BYTEA,
  satoshis BIGINT NOT NULL,
  created_height INTEGER NOT NULL,
  spent_height INTEGER,
  PRIMARY KEY (txid, vout)
) PARTITION BY HASH (txid);

DO $$
BEGIN
  FOR i IN 0..15 LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS token_outpoints_v2_p%1$s PARTITION OF token_outpoints_v2 FOR VALUES WITH (MODULUS 16, REMAINDER %1$s)',
      i
    );
  END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_v2_category_unspent
  ON token_outpoints_v2 (category)
  WHERE spent_height IS NULL;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_v2_cat_lock_unspent
  ON token_outpoints_v2 (category, locking_bytecode)
  WHERE spent_height IS NULL;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_v2_cat_commit_unspent
  ON token_outpoints_v2 (category, nft_commitment)
  WHERE spent_height IS NULL AND nft_commitment IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_v2_category_created_height
  ON token_outpoints_v2 (category, created_height DESC);

CREATE INDEX IF NOT EXISTS idx_token_outpoints_v2_category_spent_height
  ON token_outpoints_v2 (category, spent_height DESC)
  WHERE spent_height IS NOT NULL;

COMMIT;
