BEGIN;

CREATE TABLE IF NOT EXISTS token_outpoints (
  txid BYTEA NOT NULL,
  vout INTEGER NOT NULL CHECK (vout >= 0),
  category BYTEA NOT NULL,
  locking_bytecode BYTEA NOT NULL,
  ft_amount NUMERIC(78,0),
  nft_capability SMALLINT,
  nft_commitment BYTEA,
  satoshis BIGINT NOT NULL,
  created_height INTEGER NOT NULL,
  spent_height INTEGER,
  PRIMARY KEY (txid, vout)
);

CREATE INDEX IF NOT EXISTS idx_token_outpoints_category_unspent
  ON token_outpoints (category)
  WHERE spent_height IS NULL;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_cat_lock_unspent
  ON token_outpoints (category, locking_bytecode)
  WHERE spent_height IS NULL;

CREATE INDEX IF NOT EXISTS idx_token_outpoints_cat_commit_unspent
  ON token_outpoints (category, nft_commitment)
  WHERE spent_height IS NULL AND nft_commitment IS NOT NULL;

CREATE TABLE IF NOT EXISTS token_holders (
  category BYTEA NOT NULL,
  locking_bytecode BYTEA NOT NULL,
  ft_balance NUMERIC(78,0) NOT NULL DEFAULT 0,
  utxo_count INTEGER NOT NULL DEFAULT 0,
  updated_height INTEGER NOT NULL,
  PRIMARY KEY (category, locking_bytecode)
);

CREATE INDEX IF NOT EXISTS idx_token_holders_rank
  ON token_holders (category, ft_balance DESC, locking_bytecode ASC);

CREATE INDEX IF NOT EXISTS idx_token_holders_by_locking
  ON token_holders (locking_bytecode, ft_balance DESC, category ASC);

CREATE TABLE IF NOT EXISTS token_stats (
  category BYTEA PRIMARY KEY,
  total_ft_supply NUMERIC(78,0) NOT NULL DEFAULT 0,
  holder_count INTEGER NOT NULL DEFAULT 0,
  utxo_count INTEGER NOT NULL DEFAULT 0,
  updated_height INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS chain_state (
  id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id = TRUE),
  height INTEGER NOT NULL,
  blockhash BYTEA NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS applied_blocks (
  height INTEGER PRIMARY KEY,
  hash BYTEA NOT NULL,
  prev_hash BYTEA,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_applied_blocks_hash ON applied_blocks(hash);

COMMIT;
