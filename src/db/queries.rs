//! Canonical query templates for low-latency endpoints.

pub const TOKEN_SUMMARY: &str = r#"
SELECT
  encode(s.category, 'hex') AS category,
  s.total_ft_supply::text AS total_supply,
  s.holder_count,
  s.utxo_count,
  s.updated_height,
  s.updated_at,
  encode(r.txid, 'hex') AS registry_txid_hex,
  m.symbol AS symbol,
  m.name AS name,
  m.description AS description,
  m.decimals AS decimals,
  m.icon_uri AS icon_uri,
  m.token_uri AS token_uri,
  m.latest_revision AS latest_revision,
  m.identity_snapshot AS identity_snapshot,
  m.nft_types AS nft_types,
  r.source_url AS source_url,
  r.content_hash_hex AS content_hash_hex,
  r.claimed_hash_hex AS claimed_hash_hex,
  r.request_status AS request_status,
  r.validity_checks AS validity_checks
FROM token_stats s
LEFT JOIN bcmr_category_metadata m
  ON m.category = s.category
LEFT JOIN bcmr_registries r
  ON r.id = m.registry_id
WHERE s.category = decode($1, 'hex')
  AND (s.total_ft_supply > 0 OR s.holder_count > 0 OR s.utxo_count > 0)
"#;

pub const TOP_HOLDERS: &str = r#"
SELECT
  encode(locking_bytecode, 'hex') AS locking_bytecode,
  locking_address,
  ft_balance::text AS ft_balance,
  utxo_count,
  updated_height
FROM token_holders
WHERE category = decode($1, 'hex')
  AND ft_balance > 0
ORDER BY ft_balance DESC, locking_bytecode ASC
LIMIT $2
"#;

pub const PAGED_HOLDERS: &str = r#"
SELECT
  encode(locking_bytecode, 'hex') AS locking_bytecode,
  locking_address,
  ft_balance::text AS ft_balance,
  utxo_count,
  updated_height
FROM token_holders
WHERE category = decode($1, 'hex')
  AND (
    $2::numeric IS NULL
    OR ft_balance < $2::numeric
    OR (ft_balance = $2::numeric AND locking_bytecode > decode($3, 'hex'))
  )
  AND ft_balance > 0
ORDER BY ft_balance DESC, locking_bytecode ASC
LIMIT $4
"#;

pub const ELIGIBILITY: &str = r#"
SELECT
  locking_address,
  COALESCE(SUM(ft_balance), 0)::text AS ft_balance,
  COALESCE(SUM(utxo_count), 0)::int AS utxo_count,
  MAX(updated_height) AS updated_height
FROM token_holders
WHERE category = decode($1, 'hex')
  AND locking_address = $2
GROUP BY locking_address
"#;

pub const HOLDER_TOKENS: &str = r#"
SELECT
  encode(h.category, 'hex') AS category,
  h.locking_address,
  COALESCE(SUM(h.ft_balance), 0)::text AS ft_balance,
  COALESCE(SUM(h.utxo_count), 0)::int AS utxo_count,
  MAX(h.updated_height) AS updated_height,
  m.symbol,
  m.name,
  m.description AS description,
  m.decimals AS decimals,
  m.icon_uri AS icon_uri,
  m.token_uri AS token_uri,
  m.latest_revision AS latest_revision,
  m.identity_snapshot AS identity_snapshot,
  m.nft_types AS nft_types,
  r.source_url AS source_url,
  r.content_hash_hex AS content_hash_hex,
  r.claimed_hash_hex AS claimed_hash_hex,
  r.request_status AS request_status,
  r.validity_checks AS validity_checks
FROM token_holders h
LEFT JOIN bcmr_category_metadata m
  ON m.category = h.category
LEFT JOIN bcmr_registries r
  ON r.id = m.registry_id
WHERE h.locking_address = $1
  AND (h.ft_balance > 0 OR h.utxo_count > 0)
GROUP BY
  h.category,
  h.locking_address,
  m.symbol AS symbol,
  m.name AS name,
  m.description,
  m.decimals,
  m.icon_uri,
  m.token_uri,
  m.latest_revision,
  m.identity_snapshot,
  m.nft_types,
  r.source_url,
  r.content_hash_hex,
  r.claimed_hash_hex,
  r.request_status,
  r.validity_checks
ORDER BY SUM(h.ft_balance) DESC, h.category ASC
LIMIT $2
"#;

pub const TOP_N_BALANCE_SUM: &str = r#"
SELECT COALESCE(SUM(ft_balance), 0)::text AS top_n_sum
FROM (
  SELECT ft_balance
  FROM token_holders
  WHERE category = decode($1, 'hex')
    AND ft_balance > 0
  ORDER BY ft_balance DESC, locking_bytecode ASC
  LIMIT $2
) ranked
"#;

pub const RECENT_ACTIVITY_BLOCKS: &str = r#"
SELECT COUNT(*)::bigint
FROM token_outpoints
WHERE category = decode($1, 'hex')
  AND created_height > (
    SELECT GREATEST(height - $2, 0)
    FROM chain_state
    WHERE id = TRUE
  )
"#;

pub const RECENT_SPENT_BLOCKS: &str = r#"
SELECT COUNT(*)::bigint
FROM token_outpoints
WHERE category = decode($1, 'hex')
  AND spent_height IS NOT NULL
  AND spent_height > (
    SELECT GREATEST(height - $2, 0)
    FROM chain_state
    WHERE id = TRUE
  )
"#;

pub const RECENT_ACTIVE_HOLDERS_BLOCKS: &str = r#"
SELECT COUNT(*)::bigint
FROM (
  SELECT DISTINCT locking_bytecode
  FROM token_outpoints
  WHERE category = decode($1, 'hex')
    AND created_height > (
      SELECT GREATEST(height - $2, 0)
      FROM chain_state
      WHERE id = TRUE
    )
  UNION
  SELECT DISTINCT locking_bytecode
  FROM token_outpoints
  WHERE category = decode($1, 'hex')
    AND spent_height IS NOT NULL
    AND spent_height > (
      SELECT GREATEST(height - $2, 0)
      FROM chain_state
      WHERE id = TRUE
    )
) active
"#;

pub const RECENT_CREATED_FT_VOLUME_BLOCKS: &str = r#"
SELECT COALESCE(SUM(ft_amount), 0)::text
FROM token_outpoints
WHERE category = decode($1, 'hex')
  AND created_height > (
    SELECT GREATEST(height - $2, 0)
    FROM chain_state
    WHERE id = TRUE
  )
"#;

pub const RECENT_SPENT_FT_VOLUME_BLOCKS: &str = r#"
SELECT COALESCE(SUM(ft_amount), 0)::text
FROM token_outpoints
WHERE category = decode($1, 'hex')
  AND spent_height IS NOT NULL
  AND spent_height > (
    SELECT GREATEST(height - $2, 0)
    FROM chain_state
    WHERE id = TRUE
  )
"#;

pub const BCMR_CATEGORY_METADATA: &str = r#"
SELECT
  encode(m.category, 'hex') AS category,
  m.symbol,
  m.name,
  m.description,
  m.decimals,
  m.icon_uri,
  m.token_uri,
  m.latest_revision,
  m.identity_snapshot,
  m.nft_types,
  m.updated_height,
  m.updated_at,
  r.source_url,
  r.content_hash_hex,
  r.claimed_hash_hex,
  r.request_status,
  r.validity_checks
FROM bcmr_category_metadata m
JOIN bcmr_registries r
  ON r.id = m.registry_id
WHERE m.category = decode($1, 'hex')
"#;

pub const KNOWN_TOKENS: &str = r#"
SELECT
  encode(s.category, 'hex') AS category,
  s.total_ft_supply::text AS total_supply,
  s.holder_count,
  s.utxo_count,
  s.updated_height,
  s.updated_at,
  m.description,
  m.decimals,
  m.icon_uri,
  m.token_uri,
  m.latest_revision,
  m.identity_snapshot,
  m.nft_types,
  r.source_url,
  r.content_hash_hex,
  r.claimed_hash_hex,
  r.request_status,
  r.validity_checks,
  m.symbol,
  m.name
FROM token_stats s
LEFT JOIN bcmr_category_metadata m
  ON m.category = s.category
LEFT JOIN bcmr_registries r
  ON r.id = m.registry_id
WHERE s.total_ft_supply > 0
   OR s.holder_count > 0
   OR s.utxo_count > 0
ORDER BY s.holder_count DESC, s.updated_height DESC, s.category ASC
LIMIT $1
"#;
