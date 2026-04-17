# TokenIndex Technical Blueprint

## 1) Architecture Overview

`TokenIndex` is a CashTokens-only indexer + REST API service.

### Components

1. **Ingest Worker** (single writer)
- Reads BCH blocks incrementally from BCHN JSON-RPC (`getblockhash`, `getblock ... 3`)
- Parses only token-bearing inputs/outputs (CashTokens fields via `tokenData`)
- Mutates token tables in one DB transaction per block
- Maintains `chain_state` and `applied_blocks` for restart + rollback

2. **REST API** (multi-reader)
- Axum JSON endpoints for token summary, top holders, paged holders, eligibility, holder tokens, and BCMR category metadata
- Small payloads, string balances, keyset pagination
- Cache-first reads with ETag + `Cache-Control`

3. **BCMR Resolver Worker** (single writer task)
- Collects BCMR OP_RETURN candidates from ingest/backfill
- Resolves registry URLs (`https` and `ipfs://` via configured gateways)
- Traverses token authchain to resolve authbase category
- Stores registry snapshots + projected category metadata for fast API reads

4. **Storage**
- Postgres is source of truth
- In-memory cache per instance (Moka)
- Optional Redis for shared cache across replicas

5. **Observability**
- `/health`
- Structured logs (`tracing`)
- Optional metrics endpoint (Prometheus) in production extension

### Dataflow

1. Ingest reads next block from BCHN
2. In transaction:
- apply spends from `token_outpoints` (set `spent_height`, decrement aggregates)
- apply new token-bearing outputs (insert outpoints, increment aggregates)
- upsert `token_stats` and `token_holders`
- update `chain_state` + `applied_blocks`
3. API serves from cache keyed by route+params+height version
4. Cache invalidates naturally as `updated_height` changes

## 2) DB Schema and Migrations

Migration files:
- `migrations/0001_init.sql` (core token schema)
- `migrations/0004_bcmr.sql` (BCMR candidate/registry/metadata tables)

### Tables

1. `token_outpoints`
- PK: `(txid, vout)`
- Fields: `category`, `locking_bytecode`, `ft_amount`, `nft_capability`, `nft_commitment`, `satoshis`, `created_height`, `spent_height`
- Only token-bearing outputs are stored

2. `token_holders`
- PK: `(category, locking_bytecode)`
- Aggregates: `ft_balance`, `utxo_count`, `updated_height`
- Query path for O(1) eligibility

3. `token_stats`
- PK: `category`
- Aggregates: `total_ft_supply`, `holder_count`, `utxo_count`, `updated_height`, `updated_at`

4. `chain_state`
- Singleton row (`id = TRUE`)
- Fields: `height`, `blockhash`, `updated_at`

5. `applied_blocks` (optional but recommended)
- `height`, `hash`, `prev_hash`, `applied_at`
- rollback anchor

6. `bcmr_candidates`
- queue of discovered BCMR OP_RETURN references
- status machine: `pending -> processing -> resolved|retry|failed`

7. `bcmr_registries`
- fetched BCMR registry payload and validation checks per candidate

8. `bcmr_category_metadata`
- projected latest metadata by token category for low-latency API serving

### Required Indexes

- `token_outpoints(category) WHERE spent_height IS NULL`
- `token_outpoints(category, locking_bytecode) WHERE spent_height IS NULL`
- `token_outpoints(category, nft_commitment) WHERE spent_height IS NULL`
- `token_holders(category, ft_balance DESC, locking_bytecode)`
- `token_holders(locking_bytecode, ft_balance DESC, category)`

### Pooling / PgBouncer

- Use SQLx pool in app (`TOKENINDEX_DB_MAX_CONNECTIONS`)
- For high-replica read scale, front Postgres with PgBouncer in transaction pooling mode
- Keep one ingest writer instance; scale API instances horizontally

## 3) Ingestion Design

### Bootstrap

1. Read `chain_state`; if missing, start at `TOKENINDEX_BOOTSTRAP_HEIGHT - 1`
2. Loop `height+1..tip`
3. For each height:
- fetch hash via `getblockhash`
- fetch block via `getblock(hash, 3)`
- `apply_block(height, block)` transaction

### Follow Tip

Two modes:
- Polling mode (default): 400-800ms poll interval with jitter
- ZMQ mode (optional): subscribe to raw block hash notifications, then confirm via RPC

### Reorg Detection

Before applying block `H`:
- load `chain_state.blockhash` for `H-1`
- compare with incoming `previousblockhash`
- mismatch => reorg event

### Rollback Strategy (windowed)

1. Determine common ancestor by scanning `applied_blocks` backward up to `TOKENINDEX_REORG_WINDOW` (10-20)
2. For each reverted height descending:
- reverse spends and creates written at that height
- restore prior aggregate values using `token_outpoints` history
3. Set `chain_state` to ancestor
4. Reapply canonical branch forward

No full rescan in normal operation.

### Missing Prevout/Token Fields Fallback

Order:
1. `token_outpoints` lookup (preferred)
2. bounded fallback `getrawtransaction(txid, true)` when needed
3. cache fallback responses in-memory for short TTL

## 4) REST API Spec (Portable + Cacheable)

Base path: `/v1`

### 1. `GET /v1/token/:category/summary`

Response:
```json
{
  "category": "<hex>",
  "total_supply": "12345678901234567890",
  "holder_count": 1200,
  "utxo_count": 3410,
  "updated_height": 850123,
  "updated_at": "2026-03-01T14:04:00Z"
}
```

### 2. `GET /v1/token/:category/holders/top?n=50`

Response:
```json
{
  "holders": [
    {
      "address": "<address>",
      "ft_balance": "9000000000",
      "utxo_count": 7,
      "updated_height": 850123
    }
  ]
}
```

### 3. `GET /v1/token/:category/holders?limit=100&cursor=...`

- Keyset order: `ft_balance DESC, locking_bytecode ASC`
- Cursor payload: base64url JSON `{ "balance": "...", "locking_bytecode": "..." }`

Response:
```json
{
  "holders": [
    {
      "address": "<address>",
      "ft_balance": "1000",
      "utxo_count": 2,
      "updated_height": 850123
    }
  ],
  "next_cursor": "eyJiYWxhbmNlIjoiMTAwMCIsImxvY2tpbmdfYnl0ZWNvZGUiOiIuLi4ifQ"
}
```

### 4. `GET /v1/token/:category/holder/:address`

Response:
```json
{
  "eligible": true,
  "ft_balance": "500",
  "utxo_count": 1,
  "updated_height": 850123
}
```

### 5. `GET /v1/address/:address/tokens`

Response:
```json
{
  "tokens": [
    {
      "category": "<hex>",
      "ft_balance": "500",
      "utxo_count": 1,
      "updated_height": 850123
    }
  ]
}
```

### 6. `GET /v1/bcmr/:category`

Response:
```json
{
  "category": "<hex>",
  "symbol": "TOKEN",
  "name": "Token Name",
  "description": "Metadata description",
  "decimals": 8,
  "uris": {
    "icon": "ipfs://...",
    "token": null
  },
  "latest_revision": "2026-03-01T14:04:00Z",
  "identity_snapshot": {},
  "nft_types": {},
  "registry": {
    "source_url": "https://.../.well-known/bitcoin-cash-metadata-registry.json",
    "content_hash_hex": "<sha256 hex>",
    "claimed_hash_hex": "<sha256 hex>",
    "request_status": 200,
    "validity_checks": {
      "bcmr_file_accessible": true,
      "bcmr_format_valid": true,
      "bcmr_hash_match": true,
      "identities_match": true
    }
  },
  "updated_height": 850123,
  "updated_at": "2026-03-01T14:04:00Z"
}
```

### Status Codes + Errors

- `200` success
- `400` invalid category/cursor/limit
- `404` unknown token for summary
- `404` no resolved BCMR metadata for `/v1/bcmr/:category`
- `429` rate limited
- `500` internal

Error shape:
```json
{
  "error": {
    "code": "token_not_found",
    "message": "Category not indexed"
  }
}
```

### Caching Semantics

- ETag formula: `hash(route + canonical_params + updated_height + dataset_version)`
- `Cache-Control`:
  - eligibility: `public, max-age=5, stale-while-revalidate=30`
  - holders/summary: `public, max-age=10-30, stale-while-revalidate=30`
- support `If-None-Match` -> `304`

## 5) Performance + Scaling Plan

### Baseline Strategy

1. **Avoid DB per request**
- hot endpoint responses cached in-process
- optional Redis shared cache for >1 API replicas

2. **Bound payloads**
- default `limit=100`
- top holders default `n=50`, max 500

3. **DB efficiency**
- only indexed point/range queries
- no OFFSET
- no per-request aggregations

4. **Load shedding**
- on DB timeout/overload, serve stale cache up to `TOKENINDEX_STALE_WHILE_ERROR_SECS`
- fail fast otherwise (`503`)

5. **Rate limiting**
- per IP + per route token bucket
- stricter budgets for expensive routes (`holders` list)
- edge proxy option: Traefik/Nginx rate-limit middleware

6. **BCMR fetch safeguards**
- outbound BCMR fetches only over `https`
- redirects disabled for BCMR fetch client
- DNS-resolved hosts blocked if they map to private/local IP ranges
- payload bounded by `TOKENINDEX_BCMR_MAX_RESPONSE_BYTES`

### Capacity Shape

- 2k clients: single API + Postgres + in-proc cache
- 20k clients: 2-4 API replicas + Redis shared cache + PgBouncer
- 200k clients: 8+ API replicas, aggressive cache TTL, staggered client polling, dedicated read replica strategy

### Client Polling Guidance

- Poll every 5-15s depending on screen importance
- Add 10-25% random jitter
- Always send `If-None-Match`

## 6) Rust Implementation Scaffolding

### Dependencies

See `Cargo.toml`: `tokio`, `axum`, `serde`, `sqlx`, `reqwest`, `tracing`, `moka`, optional `redis`.

### Module Layout

- `src/main.rs`: runtime bootstrap, run API + ingest tasks
- `src/config.rs`: env-driven config
- `src/ingest/`: BCHN RPC + block sync worker
- `src/bcmr/`: BCMR OP_RETURN parser + resolver worker
- `src/api/`: routes + caching/headers
- `src/db/`: pool + query templates
- `src/model.rs`: wire DTOs
- `migrations/`: SQL schema

### Ingestion SQL Patterns

On output create:
```sql
INSERT INTO token_outpoints(..., spent_height) VALUES (..., NULL);

INSERT INTO token_holders(category, locking_bytecode, ft_balance, utxo_count, updated_height)
VALUES ($1,$2,$3,1,$4)
ON CONFLICT (category, locking_bytecode)
DO UPDATE SET
  ft_balance = token_holders.ft_balance + EXCLUDED.ft_balance,
  utxo_count = token_holders.utxo_count + 1,
  updated_height = EXCLUDED.updated_height;
```

On spend:
```sql
UPDATE token_outpoints
SET spent_height = $spend_height
WHERE txid = $1 AND vout = $2 AND spent_height IS NULL
RETURNING category, locking_bytecode, ft_amount;

UPDATE token_holders
SET
  ft_balance = ft_balance - $amount,
  utxo_count = utxo_count - 1,
  updated_height = $height
WHERE category = $category AND locking_bytecode = $locking_bytecode;
```

`token_stats` updates should be done incrementally in same tx.

### Rollback Pseudocode

```text
for h in current_height down to fork_height+1:
  rows = token_outpoints where created_height = h
  delete those rows and decrement holders/stats

  rows = token_outpoints where spent_height = h
  set spent_height = NULL and increment holders/stats from original ft_amount

  delete applied_blocks[h]
set chain_state = fork_height/fork_hash
```

### Key SQL Queries

Implemented query templates in `src/db/queries.rs` for:
- summary
- top holders
- paged holders (keyset)
- eligibility O(1)
- holder -> tokens

### Validation cURL

```bash
curl -sS http://127.0.0.1:8080/health
curl -sS http://127.0.0.1:8080/v1/token/<category>/summary
curl -sS http://127.0.0.1:8080/v1/bcmr/<category>
curl -sS "http://127.0.0.1:8080/v1/token/<category>/holders/top?n=50"
curl -sS "http://127.0.0.1:8080/v1/token/<category>/holders?limit=100"
curl -sS http://127.0.0.1:8080/v1/token/<category>/holder/<address>
curl -sS http://127.0.0.1:8080/v1/address/<address>/tokens
```

## 7) Verification + Load Testing Checklist

### Correctness

- [ ] Every spend of token-bearing outpoint decrements exactly one holder record
- [ ] `sum(token_holders.ft_balance by category) == token_stats.total_ft_supply`
- [ ] `sum(unspent token_outpoints count by category) == token_stats.utxo_count`
- [ ] `holder_count` matches holders with `ft_balance > 0`
- [ ] Balances are always serialized as strings

### Reorg Tests

- [ ] simulate 1-2 block reorg and verify rollback/reapply idempotence
- [ ] simulate deep reorg within configured window
- [ ] ensure no full rescan is triggered in normal reorg handling

### Load Testing

Use k6 or vegeta with cache-hit assumptions:
- Mix: 60% eligibility, 20% summary, 15% top holders, 5% holder->tokens
- target 1k+ RPS
- with warm cache: single-digit ms p50/p95 for eligibility/summary
- cold p95 target: 50-150ms

## 8) Self-Hosting Documentation

### Prerequisites

- BCHN full node with JSON-RPC enabled (`rpcuser/rpcpassword`)
- Optional BCHN ZMQ for fast tip detection
- Docker + docker-compose, or bare-metal Rust/Postgres deployment

### Run with Docker Compose

1. Copy env file:
```bash
cp .env.example .env
```
2. Set BCHN credentials in `.env`
3. Start stack:
```bash
docker compose up -d --build
```
4. Validate:
```bash
curl -sS http://127.0.0.1:8080/health
```

### Bare-Metal (systemd optional)

1. Create Postgres DB and user
2. Set env vars from `.env.example`
3. Run:
```bash
cargo run --release
```

### Kubernetes (optional pattern)

- Deploy Postgres managed service or StatefulSet
- One ingest-writer deployment/leader
- API deployment with HPA and optional Redis shared cache
- Config via ConfigMap + Secret

### Resource Sizing

- Small (2k clients): 2 vCPU / 4 GB RAM API, 2 vCPU / 8 GB Postgres
- Medium (20k): 4-8 vCPU API pool, Redis 2 GB, Postgres 8 vCPU / 32 GB
- Large (200k): 8+ API replicas, Redis cluster, PgBouncer, Postgres tuned for high read concurrency

### Upgrade Strategy

1. Apply DB migration first
2. Deploy new binary
3. Keep backward-compatible API response fields
4. For index logic changes, use additive columns/tables and backfill by block range when possible

## Non-Goals

- No frontend
- No gRPC
- No Chaingraph dependency
- No full-chain generic indexing
- No OFFSET pagination
