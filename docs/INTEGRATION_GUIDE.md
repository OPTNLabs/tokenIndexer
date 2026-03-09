# TokenIndex Integration Guide

Use this when wiring TokenIndex into a backend, frontend, or mobile API layer.

## 1. Base URL and Auth

- Base URL: `http://<tokenindex-host>:8080`
- Token routes: `/v1/...`
- If enabled, send bearer token on `/v1/*` and `/metrics`:

```http
Authorization: Bearer <token>
```

## 2. Recommended Call Sequence

1. `GET /health`
2. `GET /v1/tokens/known?limit=50`
3. `GET /v1/token/:category/summary`
4. `GET /v1/token/:category/holders/top?n=50`
5. `GET /v1/token/:category/holder/:lockingBytecode`
6. `GET /v1/holder/:lockingBytecode/tokens?limit=100`
7. Optional metadata: `GET /v1/bcmr/:category`

## 3. Behaviors to Model Correctly

- Token balances are strings (large integers).
- Summary includes `confirmed`, `unconfirmed`, and `effective` fields.
- Holder responses include confirmed/unconfirmed/effective balance + UTXO fields.
- Paged holders endpoint uses cursor (`limit` + `cursor`).

## 4. Minimal cURL Smoke

```bash
BASE_URL="http://127.0.0.1:8080"
CATEGORY="<category_hex>"
LOCKING="<locking_bytecode_hex>"

curl -sS "$BASE_URL/health"
curl -sS "$BASE_URL/v1/tokens/known?limit=10"
curl -sS "$BASE_URL/v1/token/$CATEGORY/summary"
curl -sS "$BASE_URL/v1/token/$CATEGORY/holders/top?n=5"
curl -sS "$BASE_URL/v1/token/$CATEGORY/holder/$LOCKING"
curl -sS "$BASE_URL/v1/holder/$LOCKING/tokens?limit=25"
```

## 5. JavaScript/TypeScript Client Snippet

```ts
const BASE_URL = process.env.TOKENINDEX_URL ?? "http://127.0.0.1:8080";
const API_TOKEN = process.env.TOKENINDEX_TOKEN;

async function tokenIndexFetch(path: string) {
  const headers: Record<string, string> = {};
  if (API_TOKEN) headers.Authorization = `Bearer ${API_TOKEN}`;

  const res = await fetch(`${BASE_URL}${path}`, { headers });
  if (!res.ok) throw new Error(`TokenIndex ${res.status}: ${await res.text()}`);
  return res.json();
}

export async function loadToken(categoryHex: string) {
  const [summary, holders] = await Promise.all([
    tokenIndexFetch(`/v1/token/${categoryHex}/summary`),
    tokenIndexFetch(`/v1/token/${categoryHex}/holders/top?n=20`),
  ]);

  return { summary, holders: holders.holders };
}
```

## 6. Caching and Polling

- Poll summary every `5-15s`
- Poll holders every `15-30s`
- Poll BCMR every `5-30m`

Use `ETag`/`If-None-Match` and handle `304 Not Modified`.

## 7. Error Handling

- `400`: bad category/locking/cursor
- `401`: missing/invalid bearer token
- `403`: IP blocked by allowlist
- `404`: not found
- `429`: rate-limited
- `500`: server/db failure

Retry only transient failures (`429`, `500`, network timeout).

## 8. Ship Checklist

- Health endpoints are green
- App can load `tokens/known`
- Summary + holders + holder tokens render correctly
- Retry/backoff logic in place
- Auth behavior tested (if enabled)
