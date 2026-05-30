# TokenIndex Integration Guide

Use [README.md](../README.md) as the canonical entry point for links and public URLs. This guide focuses on integration patterns and client behavior.

Use this when wiring TokenIndex into a backend, frontend, or mobile API layer.

## 1. Base URL and Auth

- Base URL: `http://<tokenindex-host>:8080`
- Token routes: `/v1/...`
- If enabled, send bearer token on `/v1/*` and `/metrics`:

```http
Authorization: Bearer <token>
```

## 2. Recommended Call Sequence

1. Start with `GET /health`.
2. Resolve the summary with `GET /v1/token/:category` or `GET /v1/token/:category/summary`.
3. Add holders, address balances, and provenance only if the UI needs them.
4. Load `bcmr`, `mempool`, and `insights` lazily because they are auxiliary views.

For the full route inventory, response matrix, and field semantics, use [docs/BLUEPRINT.md](./BLUEPRINT.md). For public URLs and release navigation, use [README.md](../README.md).

## 3. Behaviors to Model Correctly

- Token balances are strings.
- Summary includes `confirmed`, `unconfirmed`, and `effective` fields.
- The summary response can include BCMR metadata and `authchain_head` when available.
- Holder responses include balance and UTXO fields.
- Paged holders and NFT endpoints use cursor pagination.
- `tokens/known` returns the highest-holder-count tokens, capped server-side.
- `mempool` and `insights` are read-only overlays derived from the latest mempool snapshot.
- Cursor payloads are base64url-encoded JSON.
- Exact route shapes, limit defaults, and example payloads live in [docs/BLUEPRINT.md](./BLUEPRINT.md).

## 4. Minimal cURL Smoke

```bash
BASE_URL="http://127.0.0.1:8080"
CATEGORY="<category_hex>"
ADDRESS="<cashaddr_or_other_indexed_address>"

curl -sS "$BASE_URL/health"
curl -sS "$BASE_URL/v1/tokens/known?limit=10"
curl -sS "$BASE_URL/v1/token/$CATEGORY"
curl -sS "$BASE_URL/v1/token/$CATEGORY/summary"
curl -sS "$BASE_URL/v1/token/$CATEGORY/holders/top?n=5"
curl -sS "$BASE_URL/v1/token/$CATEGORY/holders?limit=5"
curl -sS "$BASE_URL/v1/token/$CATEGORY/nfts?limit=5"
curl -sS "$BASE_URL/v1/token/$CATEGORY/holder/$ADDRESS"
curl -sS "$BASE_URL/v1/address/$ADDRESS/tokens?limit=25"
curl -sS "$BASE_URL/v1/token/$CATEGORY/bcmr"
curl -sS "$BASE_URL/v1/bcmr/$CATEGORY"
curl -sS "$BASE_URL/v1/token/$CATEGORY/authchain/head"
curl -sS "$BASE_URL/v1/token/$CATEGORY/mempool?n=20"
curl -sS "$BASE_URL/v1/token/$CATEGORY/insights"
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
- Poll mempool and insights only when the UI exposes those views; otherwise keep them on-demand

Use `ETag`/`If-None-Match` and handle `304 Not Modified`.

## 7. Error Handling

- `400`: bad category/address/cursor
- `401`: missing/invalid bearer token
- `403`: IP blocked by allowlist
- `404`: not found (`/v1/token/:category` and `/v1/bcmr/:category`)
- `429`: rate-limited
- `500`: server/db failure

Retry only transient failures (`429`, `500`, network timeout).

## 8. Ship Checklist

- Health endpoints are green
- `tokens/known` loads successfully
- NFT pagination works for supported categories
- Summary + holders + address token list render correctly
- `GET /v1/token/:category` returns BCMR + authchain provenance when available
- `GET /v1/token/:category/insights` and `GET /v1/token/:category/mempool` work for supported categories
- Retry/backoff logic in place
- Auth behavior tested (if enabled)
