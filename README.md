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

Existing BCMR indexer consumers can switch the base URL from the Python `bcmr-indexer` service to TokenIndex without changing their request paths. Native integrations should prefer the `/v1/...` routes.

## Docs

- Full blueprint and API contract: [docs/BLUEPRINT.md](docs/BLUEPRINT.md)
- Operations, SLOs, and load testing: [docs/OPERATIONS.md](docs/OPERATIONS.md)
- Schema migrations:
  - [migrations/0001_init.sql](migrations/0001_init.sql)
  - [migrations/0003_activity_indexes.sql](migrations/0003_activity_indexes.sql)
  - [migrations/0004_bcmr.sql](migrations/0004_bcmr.sql)

## Endpoints

- `GET /health`
- `GET /health/details`
- `GET /metrics`
- `GET /v1/token/:category`
- `GET /v1/token/:category/summary`
- `GET /v1/bcmr/:category`
- `GET /v1/token/:category/bcmr`
- `GET /v1/token/:category/authchain/head`
- `GET /v1/token/:category/holders/top?n=50`
- `GET /v1/token/:category/holders?limit=100&cursor=...`
- `GET /v1/token/:category/holder/:address`
- `GET /v1/address/:address/tokens`
- `GET /v1/token/:category/mempool?n=20`
- `GET /v1/token/:category/insights`

Legacy BCMR-compatible routes:

- `GET /api/status/latest-block`
- `GET /api/tokens/:category`
- `GET /api/registries/:category/latest`
- `GET /api/registry/:category/identity-snapshot`
- `GET /api/cashtokens/:category`

All core holder/token endpoints now return unified values in a single response:

- `confirmed_*`
- `unconfirmed_*`
- `effective_*` (`confirmed + unconfirmed`)

The native token summary also includes BCMR metadata and authchain provenance when available, so common token lookups can usually stop at one call.

Native response map:

- `GET /v1/token/:category` and `GET /v1/token/:category/summary` return the unified token summary
- `GET /v1/token/:category/bcmr` returns BCMR metadata only
- `GET /v1/token/:category/authchain/head` returns provenance only
- `GET /api/...` remains the compatibility surface for BCMR-style consumers

## Production Notes

- Production deployment is Docker Compose based and can be rebuilt from a clean Git checkout with `docker compose up -d --build`.
- Set `TOKENINDEX_CHIPNET_EXPECTED_CHAIN` (e.g. `chip`) to prevent indexing the wrong network.
- For faster initial catch-up, disable optional workers until near tip:
  - `TOKENINDEX_BCMR_ENABLED=false`
  - `TOKENINDEX_BCMR_BACKFILL_ENABLED=false`
  - `TOKENINDEX_MEMPOOL_ENABLED=false`
  - `TOKENINDEX_RECONCILE_ENABLED=false`
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
