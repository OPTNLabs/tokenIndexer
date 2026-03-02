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

## Cache and Rate-Limit Knobs

- `TOKENINDEX_CACHE_TTL_SECS`: fresh window
- `TOKENINDEX_STALE_WHILE_ERROR_SECS`: stale-serve grace under DB pressure
- `TOKENINDEX_REDIS_URL`: enable shared cache across replicas
- `TOKENINDEX_RATE_LIMIT_DEFAULT_RPS`: per-IP default route budget
- `TOKENINDEX_RATE_LIMIT_HOLDERS_RPS`: per-IP holders route budget
- `TOKENINDEX_RATE_LIMIT_ELIGIBILITY_RPS`: per-IP eligibility route budget

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
4. Confirm `/health`, `/metrics`, and API correctness
5. Monitor reorg/log errors for first 15 minutes
