BEGIN;

CREATE TABLE IF NOT EXISTS bcmr_candidates (
  id BIGSERIAL PRIMARY KEY,
  txid BYTEA NOT NULL,
  vout INTEGER NOT NULL CHECK (vout >= 0),
  block_height INTEGER NOT NULL,
  op_return TEXT NOT NULL,
  claimed_hash_hex TEXT,
  encoded_url_hex TEXT,
  decoded_url TEXT,
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error TEXT,
  discovered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved_at TIMESTAMPTZ,
  category BYTEA,
  authbase BYTEA,
  registry_id BIGINT,
  UNIQUE (txid, vout)
);

CREATE INDEX IF NOT EXISTS idx_bcmr_candidates_status_next
  ON bcmr_candidates (status, next_attempt_at, id);

CREATE INDEX IF NOT EXISTS idx_bcmr_candidates_category
  ON bcmr_candidates (category)
  WHERE category IS NOT NULL;

CREATE TABLE IF NOT EXISTS bcmr_registries (
  id BIGSERIAL PRIMARY KEY,
  txid BYTEA NOT NULL,
  vout INTEGER NOT NULL CHECK (vout >= 0),
  category BYTEA,
  authbase BYTEA,
  source_url TEXT NOT NULL,
  content_hash_hex TEXT,
  claimed_hash_hex TEXT,
  request_status INTEGER,
  validity_checks JSONB,
  contents JSONB,
  latest_revision TIMESTAMPTZ,
  fetched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (txid, vout)
);

CREATE INDEX IF NOT EXISTS idx_bcmr_registries_category_fetched
  ON bcmr_registries (category, fetched_at DESC)
  WHERE category IS NOT NULL;

CREATE TABLE IF NOT EXISTS bcmr_category_metadata (
  category BYTEA PRIMARY KEY,
  registry_id BIGINT NOT NULL REFERENCES bcmr_registries(id) ON DELETE CASCADE,
  symbol TEXT,
  name TEXT,
  description TEXT,
  decimals INTEGER,
  icon_uri TEXT,
  token_uri TEXT,
  latest_revision TIMESTAMPTZ,
  identity_snapshot JSONB,
  nft_types JSONB,
  updated_height INTEGER NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bcmr_category_metadata_updated
  ON bcmr_category_metadata (updated_height DESC);

COMMIT;
