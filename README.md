# TokenIndex

CashTokens-specific BCH indexer + REST API in Rust.

## Scope

- CashTokens-only indexing
- Fast associations: `category <-> addresses <-> balances/utxo_count`
- REST JSON API for mobile/downstream clients
- Self-hostable with Postgres and optional Redis

## Quick Start

1. Copy environment template:

```bash
cp .env.example .env
```

2. Set BCHN RPC values in `.env`
3. Start with Docker Compose:

```bash
docker compose up -d --build
```

4. Test:

```bash
curl -sS http://127.0.0.1:8080/health
curl -sS http://127.0.0.1:8080/metrics
```

## Public API

- Public deployment: `https://tokenindex.optnlabs.com`
- Health check: `https://tokenindex.optnlabs.com/health`
- Native API base: `https://tokenindex.optnlabs.com/v1`
- Legacy compatibility base: `https://tokenindex.optnlabs.com/api`
- Swagger UI: `https://tokenindex.optnlabs.com/docs`
- OpenAPI JSON: `https://tokenindex.optnlabs.com/openapi.json`
- Canonical route index: [docs/BLUEPRINT.md](docs/BLUEPRINT.md)
- Integration patterns: [docs/INTEGRATION_GUIDE.md](docs/INTEGRATION_GUIDE.md)
- Operations and rollout checks: [docs/OPERATIONS.md](docs/OPERATIONS.md)

Existing BCMR indexer consumers can switch the base URL from the Python `bcmr-indexer` service to TokenIndex without changing their request paths. Native integrations should prefer the `/v1/...` routes.

- Schema migrations:
  - [migrations/0001_init.sql](migrations/0001_init.sql)
  - [migrations/0003_activity_indexes.sql](migrations/0003_activity_indexes.sql)
  - [migrations/0004_bcmr.sql](migrations/0004_bcmr.sql)

All core holder/token endpoints return unified values in a single response:

- `confirmed_*`
- `unconfirmed_*`
- `effective_*` (`confirmed + unconfirmed`)

The native token summary includes BCMR metadata and authchain provenance when available, so common token lookups can usually stop at one call.

Usage notes:

- `category` path parameters must be 32-byte hex.
- `address` path parameters use the indexer's accepted holder-address format and are validated as non-empty strings.
- `limit` query params are clamped server-side:
  - `tokens/known`: default `200`, max `1000`
  - `holders/top`: default `50`, max `500`
  - `holders`, `nfts`, and `address/tokens`: default `100`, max `500`
  - `mempool`: default `20`, max `200`
- `holders` and `nfts` cursors are base64url-encoded JSON payloads.
- Polling clients should send `If-None-Match`; the API supports `304 Not Modified`.

## Production Notes

- Production deployment is Docker Compose based and can be rebuilt from a clean Git checkout with `docker compose up -d --build`.
- Set `TOKENINDEX_CHIPNET_EXPECTED_CHAIN` (e.g. `chip`) to prevent indexing the wrong network.
- For faster initial catch-up, disable optional workers until near tip:
  - `TOKENINDEX_BCMR_ENABLED=false`
  - `TOKENINDEX_BCMR_BACKFILL_ENABLED=false`
  - `TOKENINDEX_MEMPOOL_ENABLED=false`
  - `TOKENINDEX_RECONCILE_ENABLED=false`
- For reboot resilience, keep the startup wait gate enabled so the container waits for BCHN RPCs before the Rust process starts:
  - `TOKENINDEX_STARTUP_WAIT_FOR_UPSTREAMS=true`
  - `TOKENINDEX_STARTUP_WAIT_TIMEOUT_SECS=600`
- Simultaneous chipnet + mainnet in one process:
  - Primary chipnet stack uses `TOKENINDEX_CHIPNET_*` vars (legacy `TOKENINDEX_*` names still work).
  - Secondary mainnet stack is enabled when `TOKENINDEX_MAINNET_RPC_URL` is set.
  - Unified API uses one listener (`TOKENINDEX_API_HOST`/`TOKENINDEX_API_PORT`) and one route set (`/v1/...`).
  - Route handlers auto-select the appropriate chain dataset by category/lookup with fallback to mainnet schema when not found in chipnet schema.
  - Both stacks can share one Postgres database by using separate schemas:
    - `TOKENINDEX_CHIPNET_DB_SCHEMA` (e.g. `chipnet`)
    - `TOKENINDEX_MAINNET_DB_SCHEMA` (e.g. `mainnet`)
  - Optional overrides for secondary stack:
    - `TOKENINDEX_MAINNET_API_HOST`/`TOKENINDEX_MAINNET_API_PORT` are legacy compatibility knobs and are ignored by the unified listener path model.
    - `TOKENINDEX_MAINNET_DATABASE_URL` (if omitted, reuses primary DB URL)
    - `TOKENINDEX_MAINNET_DATABASE_READ_URL`
    - `TOKENINDEX_MAINNET_RPC_USER` / `TOKENINDEX_MAINNET_RPC_PASS` (fallback to primary creds)
    - `TOKENINDEX_MAINNET_BOOTSTRAP_HEIGHT` (defaults to primary `TOKENINDEX_BOOTSTRAP_HEIGHT` if unset)
  - Do not run both stacks against the same schema.
- Send `If-None-Match` on polling clients; API now supports `304 Not Modified`.
- Service uses in-memory response cache with stale-on-error fallback bounded by:
  - `TOKENINDEX_CACHE_TTL_SECS`
  - `TOKENINDEX_STALE_WHILE_ERROR_SECS`
- Set `TOKENINDEX_REDIS_URL` to enable shared cache in multi-replica deployments.
- Set `TOKENINDEX_DATABASE_READ_URL` to route API reads to a PostgreSQL replica while ingest stays on primary.
- Set `TOKENINDEX_APPLY_POSTGRES_TUNING=true` if you want the container to apply [scripts/ops/postgres_tuning.sql](scripts/ops/postgres_tuning.sql) at startup.
- Ingest throughput knobs:
  - `TOKENINDEX_RPC_BATCH_SIZE`
  - `TOKENINDEX_RPC_PREFETCH_BATCHES`
  - `TOKENINDEX_DB_INGEST_SYNCHRONOUS_COMMIT` (`off` is faster but reduces durability)
- Tune DB query upper bound with `TOKENINDEX_DB_STATEMENT_TIMEOUT_MS`.
- Per-IP route budgets:
  - `TOKENINDEX_RATE_LIMIT_DEFAULT_RPS`
  - `TOKENINDEX_RATE_LIMIT_HOLDERS_RPS`
  - `TOKENINDEX_RATE_LIMIT_ELIGIBILITY_RPS`
- Proxy/IP trust and cursor limits:
  - `TOKENINDEX_TRUST_X_FORWARDED_FOR` (set `true` only behind trusted proxy)
  - `TOKENINDEX_TRUSTED_PROXY_CIDRS` (CIDRs allowed to supply `X-Forwarded-For`)
  - `TOKENINDEX_IP_ALLOWLIST` (optional client CIDR/IP allowlist)
  - `TOKENINDEX_API_BEARER_TOKEN` (optional bearer auth for `/v1/*` and `/metrics`)
  - `TOKENINDEX_MAX_CURSOR_CHARS`
- RPC batching/tuning:
  - `TOKENINDEX_RPC_BATCH_SIZE`
  - `TOKENINDEX_RECONCILE_ENABLED`
  - `TOKENINDEX_RECONCILE_INTERVAL_SECS`
  - `TOKENINDEX_RPC_TIMEOUT_MS`
  - `TOKENINDEX_RPC_RETRIES`
  - `TOKENINDEX_RPC_RETRY_BACKOFF_MS`
- Mempool overlay:
  - `TOKENINDEX_MEMPOOL_ENABLED`
  - `TOKENINDEX_MEMPOOL_POLL_MS`
  - `TOKENINDEX_MEMPOOL_MAX_TXS`
- BCMR resolver + backfill:
  - `TOKENINDEX_BCMR_ENABLED`
  - `TOKENINDEX_BCMR_POLL_MS`
  - `TOKENINDEX_BCMR_BATCH_SIZE`
  - `TOKENINDEX_BCMR_MAX_ATTEMPTS`
  - `TOKENINDEX_BCMR_RETRY_BACKOFF_SECS`
  - `TOKENINDEX_BCMR_MAX_AUTHCHAIN_DEPTH`
  - `TOKENINDEX_BCMR_HTTP_TIMEOUT_MS`
  - `TOKENINDEX_BCMR_MAX_RESPONSE_BYTES`
  - `TOKENINDEX_BCMR_IPFS_GATEWAYS`
  - `TOKENINDEX_BCMR_BACKFILL_ENABLED`
  - `TOKENINDEX_BCMR_BACKFILL_FROM_HEIGHT`
  - `TOKENINDEX_BCMR_BACKFILL_TO_HEIGHT`
  - `TOKENINDEX_BCMR_BACKFILL_BATCH_BLOCKS`

## BCMR Notes

- BCMR metadata is resolved from on-chain BCMR OP_RETURN references and served via `GET /v1/bcmr/:category`.
- The BCMR worker also probes all known token categories from `token_stats` by checking each category/authbase transaction for BCMR OP_RETURN outputs.
- Probe outcomes are persisted in `bcmr_category_checks` as `candidate_found`, `no_candidate`, or `error`, so categories without BCMR are still marked as explicitly checked.
- Remote BCMR fetch hardening:
  - only `https` fetches are allowed
  - redirects are disabled
  - resolved DNS/IP targets are blocked if private/local
  - response bodies are capped by `TOKENINDEX_BCMR_MAX_RESPONSE_BYTES`
- IPFS sources are supported via `ipfs://...` with gateway fallback configured by `TOKENINDEX_BCMR_IPFS_GATEWAYS`.

## tokenExplorer Integration

- tokenExplorer should call one base URL with unified routes only:
  - `http://<host>:8080/v1/...`
  - no `/v1/main/...` route is required or exposed.
- Works with whichever chain data is configured at runtime:
  - chipnet only
  - mainnet only
  - chipnet + mainnet (single process)
- For dual-chain indexing, keep separate schemas even when sharing one DB:
  - `TOKENINDEX_CHIPNET_DB_SCHEMA` (primary, e.g. `chipnet`)
  - `TOKENINDEX_MAINNET_DB_SCHEMA` (secondary, e.g. `mainnet`)
- Recommended tokenExplorer queries:
  - `GET /v1/token/:category/summary`
  - `GET /v1/token/:category/holders/top?n=50`
  - `GET /v1/address/:address/tokens`
  - `GET /v1/bcmr/:category`
- BCMR response includes validation and resolved metadata fields used by tokenExplorer:
  - `registry.validity_checks.*`
  - `name`, `symbol`, `decimals`, `description`, `uris`

## Integration Scripts

- API smoke test: `scripts/integration/api-smoke.sh`
- Reorg simulation: `scripts/integration/reorg-smoke.sh`
- k6 load profile: `scripts/load/k6-eligibility.js`
- vegeta load profile: `scripts/load/vegeta-holders-targets.txt` + `scripts/load/run-vegeta.sh`
