//! Canonical query templates for low-latency endpoints.

pub const TOKEN_SUMMARY: &str = r#"
SELECT
  encode(category, 'hex') AS category,
  total_ft_supply::text AS total_supply,
  holder_count,
  utxo_count,
  updated_height,
  updated_at
FROM token_stats
WHERE category = decode($1, 'hex')
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
  ft_balance::text AS ft_balance,
  utxo_count,
  updated_height
FROM token_holders
WHERE category = decode($1, 'hex')
  AND locking_bytecode = decode($2, 'hex')
"#;

pub const HOLDER_TOKENS: &str = r#"
SELECT
  encode(category, 'hex') AS category,
  locking_address,
  ft_balance::text AS ft_balance,
  utxo_count,
  updated_height
FROM token_holders
WHERE locking_bytecode = decode($1, 'hex')
  AND (ft_balance > 0 OR utxo_count > 0)
ORDER BY ft_balance DESC, category ASC
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
