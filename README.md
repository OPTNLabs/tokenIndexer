# TokenIndex

CashTokens-specific BCH indexer + REST API in Rust.

## Scope

- CashTokens-only indexing
- Fast associations: `category <-> locking_bytecode <-> balances/utxo_count`
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

## Docs

- Full blueprint and API contract: [docs/BLUEPRINT.md](docs/BLUEPRINT.md)
- Operations, SLOs, and load testing: [docs/OPERATIONS.md](docs/OPERATIONS.md)
- Schema migration: [migrations/0001_init.sql](migrations/0001_init.sql)

## Endpoints

- `GET /v1/token/:category/summary`
- `GET /v1/token/:category/holders/top?n=50`
- `GET /v1/token/:category/holders?limit=100&cursor=...`
- `GET /v1/token/:category/holder/:lockingBytecode`
- `GET /v1/holder/:lockingBytecode/tokens`
- `GET /v1/token/:category/mempool?n=20`
- `GET /v1/token/:category/insights`
- `GET /metrics`

All core holder/token endpoints now return unified values in a single response:
- `confirmed_*`
- `unconfirmed_*`
- `effective_*` (`confirmed + unconfirmed`)

## Production Notes

- Set `TOKENINDEX_EXPECTED_CHAIN` (e.g. `chip`) to prevent indexing the wrong network.
- Send `If-None-Match` on polling clients; API now supports `304 Not Modified`.
- Service uses in-memory response cache with stale-on-error fallback bounded by:
  - `TOKENINDEX_CACHE_TTL_SECS`
  - `TOKENINDEX_STALE_WHILE_ERROR_SECS`
- Set `TOKENINDEX_REDIS_URL` to enable shared cache in multi-replica deployments.
- Per-IP route budgets:
  - `TOKENINDEX_RATE_LIMIT_DEFAULT_RPS`
  - `TOKENINDEX_RATE_LIMIT_HOLDERS_RPS`
  - `TOKENINDEX_RATE_LIMIT_ELIGIBILITY_RPS`
- Mempool overlay:
  - `TOKENINDEX_MEMPOOL_ENABLED`
  - `TOKENINDEX_MEMPOOL_POLL_MS`
  - `TOKENINDEX_MEMPOOL_MAX_TXS`

## Integration Scripts

- API smoke test: `scripts/integration/api-smoke.sh`
- Reorg simulation: `scripts/integration/reorg-smoke.sh`
- k6 load profile: `scripts/load/k6-eligibility.js`
- vegeta load profile: `scripts/load/vegeta-holders-targets.txt` + `scripts/load/run-vegeta.sh`
