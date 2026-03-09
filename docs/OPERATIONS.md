# TokenIndex Operations

## SLO Targets

- Hot endpoints (cache hit): p95 < 10ms
- Cold endpoints (DB hit): p95 < 150ms
- Error rate (5xx): < 1%
- Eligibility endpoint: sustain 1k+ RPS with cache hit ratio >= 80%

## Capacity Profiles

- Small: 2 vCPU / 4 GB RAM, Postgres 2 vCPU / 4 GB RAM
- Medium: 4 vCPU / 8 GB RAM for API (2 replicas), Postgres 4 vCPU / 16 GB RAM + PgBouncer
- Large: 8 vCPU / 16 GB RAM per API replica (4+ replicas), Redis shared cache, dedicated DB tuning

## Required Runtime Checks

- `GET /health` must return `status=ok`
- `GET /metrics` must expose counters
- `chain_state.height` should track BCHN tip with bounded lag
- `bcmr_candidates` should not accumulate in `processing`/`retry` unexpectedly
- `bcmr_category_metadata` should populate for known BCMR token categories

## Cache and Rate-Limit Knobs

- `TOKENINDEX_CACHE_TTL_SECS`: fresh window
- `TOKENINDEX_STALE_WHILE_ERROR_SECS`: stale-serve grace under DB pressure
- `TOKENINDEX_REDIS_URL`: enable shared cache across replicas
- `TOKENINDEX_DATABASE_READ_URL`: optional API read-replica connection URL
- `TOKENINDEX_RATE_LIMIT_DEFAULT_RPS`: per-IP default route budget
- `TOKENINDEX_RATE_LIMIT_HOLDERS_RPS`: per-IP holders route budget
- `TOKENINDEX_RATE_LIMIT_ELIGIBILITY_RPS`: per-IP eligibility route budget

## BCMR Runtime Knobs

- `TOKENINDEX_BCMR_ENABLED`: enable/disable resolver worker
- `TOKENINDEX_BCMR_POLL_MS`: resolver polling interval
- `TOKENINDEX_BCMR_BATCH_SIZE`: candidates picked per resolver cycle
- `TOKENINDEX_BCMR_MAX_ATTEMPTS`: retries before `failed`
- `TOKENINDEX_BCMR_RETRY_BACKOFF_SECS`: linear retry backoff base
- `TOKENINDEX_BCMR_MAX_AUTHCHAIN_DEPTH`: authchain traversal safety bound
- `TOKENINDEX_BCMR_HTTP_TIMEOUT_MS`: outbound BCMR fetch timeout
- `TOKENINDEX_BCMR_MAX_RESPONSE_BYTES`: hard payload limit for fetched BCMR JSON
- `TOKENINDEX_BCMR_IPFS_GATEWAYS`: comma-separated gateway fallback list for `ipfs://` URLs
- `TOKENINDEX_BCMR_BACKFILL_ENABLED`: run historical BCMR candidate scan on startup
- `TOKENINDEX_BCMR_BACKFILL_FROM_HEIGHT`: backfill start height
- `TOKENINDEX_BCMR_BACKFILL_TO_HEIGHT`: backfill end height (`0` = chain tip)
- `TOKENINDEX_BCMR_BACKFILL_BATCH_BLOCKS`: backfill block window size

## BCMR Security Posture

- outbound BCMR fetches are restricted to `https`
- redirects are disabled for BCMR fetches
- hostnames are DNS-resolved and blocked if any resolved IP is private/local
- fetched BCMR response bodies are bounded by `TOKENINDEX_BCMR_MAX_RESPONSE_BYTES`

## BCMR Validation Queries

```bash
curl -sS http://127.0.0.1:8080/health
curl -sS "http://127.0.0.1:8080/v1/bcmr/<category-hex>"
```

```sql
SELECT status, COUNT(*) FROM bcmr_candidates GROUP BY status ORDER BY status;
SELECT COUNT(*) AS registries FROM bcmr_registries;
SELECT COUNT(*) AS category_metadata FROM bcmr_category_metadata;
```

```sql
SELECT
  encode(category,'hex') AS category,
  symbol,
  name,
  updated_height
FROM bcmr_category_metadata
ORDER BY updated_height DESC;
```

## Read Replica Rollout

1. Provision PostgreSQL streaming replica from primary.
2. Point API reads to replica by setting:
```bash
TOKENINDEX_DATABASE_READ_URL=postgres://tokenindex:tokenindex@replica:5432/tokenindex
```
3. Keep ingest on primary (`TOKENINDEX_DATABASE_URL` remains primary).
4. Validate lag before enabling production traffic:
```sql
SELECT now() - pg_last_xact_replay_timestamp() AS replica_lag;
```
5. Roll back instantly by unsetting `TOKENINDEX_DATABASE_READ_URL` and restarting API pods.

## Postgres Autovacuum and Analyze Tuning

Run on primary database:

```sql
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
```

Low-traffic analyze job:

```sql
ANALYZE token_outpoints;
ANALYZE token_holders;
ANALYZE token_stats;
```

Or apply the checked-in script:

```bash
psql "$TOKENINDEX_DATABASE_URL" -f scripts/ops/postgres_tuning.sql
```

## Partition Migration Strategy

For the current schema, `token_outpoints` has primary key `(txid, vout)`.
Range partitioning by `created_height` requires changing that key shape. Use one of these paths:

### Path A (Low Risk): Hash Partition by `txid`

1. Create partitioned copy with equivalent columns and key.
2. Create 16 to 64 hash partitions.
3. Backfill with batched `INSERT INTO ... SELECT`.
4. Brief write pause, final delta copy, rename swap.

Example bootstrap SQL:

```sql
CREATE TABLE token_outpoints_v2 (
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
```

Or run the bootstrap script:

```bash
psql "$TOKENINDEX_DATABASE_URL" -f scripts/ops/token_outpoints_hash_partition_bootstrap.sql
```

### Path B (Advanced): Range Partition by Height

Requires schema/key redesign and ingest/app updates. Plan this as a dedicated migration project.
Do not attempt direct in-place conversion during normal release windows.

## Load Testing

### k6 eligibility hot path

```bash
CATEGORY=<hex> LOCKING=<hex> BASE_URL=http://127.0.0.1:8080 k6 run scripts/load/k6-eligibility.js
```

### vegeta holders endpoints

1. Replace `REPLACE_CATEGORY` in `scripts/load/vegeta-holders-targets.txt`
2. Run:

```bash
RATE=500 DURATION=60s scripts/load/run-vegeta.sh
```

## Reorg Validation

```bash
TOKENINDEX_RPC_URL=http://<node>:48334 \
TOKENINDEX_RPC_USER=<user> TOKENINDEX_RPC_PASS=<pass> \
scripts/integration/reorg-smoke.sh
```

Expected TokenIndex behavior:
- detect parent mismatch
- rollback within configured window
- reapply canonical branch

## Upgrade Strategy

1. Back up Postgres
2. Deploy new image
3. Let automatic SQL migrations run on startup
4. Confirm `/health`, `/metrics`, `/v1/token/*`, and `/v1/bcmr/*` correctness
5. Monitor reorg/log errors for first 15 minutes

## Performance Verification Checklist

1. Confirm cache hit ratio remains high after rollout (`/metrics`).
2. Confirm p95 latency regression-free for:
`/v1/token/:category/summary`, `/holders/top`, `/holders`, `/holder/:lockingBytecode`, `/insights`.
3. Confirm ingest stays near tip while load test runs.
4. Confirm replica lag remains within target if read replica is enabled.
