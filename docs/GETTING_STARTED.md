# TokenIndex Getting Started

This is the shortest reliable path to run TokenIndex locally and confirm it is ready for integration.

## 1. Prerequisites

Required:

- Docker + Docker Compose
- BCHN node with JSON-RPC enabled
- BCHN RPC username/password

Important:

- TokenIndex does not run BCHN for you.
- `docker-compose.yml` here starts TokenIndex + Postgres (and optional Redis).

## 2. Configure `.env`

```bash
cp .env.example .env
```

Set these values:

```bash
TOKENINDEX_CHIPNET_RPC_URL=http://<bchn-host>:48334
TOKENINDEX_CHIPNET_RPC_USER=<rpc-user>
TOKENINDEX_CHIPNET_RPC_PASS=<rpc-pass>
TOKENINDEX_CHIPNET_EXPECTED_CHAIN=chip
```

Defaults you usually keep:

```bash
TOKENINDEX_API_PORT=8080
TOKENINDEX_DATABASE_URL=postgres://tokenindex:tokenindex@postgres:5432/tokenindex
TOKENINDEX_CHIPNET_DB_SCHEMA=chipnet
```

## 3. Start Services

```bash
docker compose up -d --build
docker compose ps
```

You should see:

- `tokenindex`
- `tokenindex-postgres`

## 4. Health Checks

```bash
curl -sS http://127.0.0.1:8080/health
curl -sS http://127.0.0.1:8080/health/details
```

What to verify in `/health/details`:

- `chains.primary.rpc_ok = true`
- `chains.primary.indexed_height` present and increasing
- `chains.primary.lag_blocks` eventually decreases or stays bounded

## 5. Data Checks

```bash
curl -sS "http://127.0.0.1:8080/v1/tokens/known?limit=5"
```

If you already know a category:

```bash
CATEGORY=<token_category_hex>
curl -sS "http://127.0.0.1:8080/v1/token/$CATEGORY/summary"
curl -sS "http://127.0.0.1:8080/v1/token/$CATEGORY/holders/top?n=10"
```

Optional smoke script:

```bash
CATEGORY=<token_category_hex> \
LOCKING=<locking_bytecode_hex> \
BASE_URL=http://127.0.0.1:8080 \
scripts/integration/api-smoke.sh
```

## 6. Quick Troubleshooting

### `/health` is `degraded`

```bash
docker compose logs --tail=200 tokenindex
```

Then verify `.env` RPC URL/user/pass and Postgres container health.

### `rpc_ok=false`

- BCHN host/port unreachable from container
- RPC credentials invalid
- node JSON-RPC disabled

### Empty `tokens/known`

- initial sync still in progress
- wrong chain config (`chip` vs `main`)
- no relevant token activity yet at indexed heights

### `401 unauthorized`

If `TOKENINDEX_API_BEARER_TOKEN` is set, send:

```http
Authorization: Bearer <token>
```

for `/v1/*` and `/metrics`.

## 7. Next Step

Use [docs/INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md) to connect your app.
