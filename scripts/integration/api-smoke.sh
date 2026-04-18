#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
CATEGORY="${CATEGORY:?set CATEGORY=<token_category_hex>}"
ADDRESS="${ADDRESS:?set ADDRESS=<cashaddr_or_other_indexed_address>}"

echo "health"
curl -fsS "$BASE_URL/health" | jq .

echo "summary"
curl -fsS "$BASE_URL/v1/token/$CATEGORY" | jq .
curl -fsS "$BASE_URL/v1/token/$CATEGORY/summary" | jq .

echo "bcmr"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/bcmr" | jq .

echo "authchain head"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/authchain/head" | jq .

echo "top holders"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/holders/top?n=5" | jq .

echo "eligibility"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/holder/$ADDRESS" | jq .

echo "holder tokens"
curl -fsS "$BASE_URL/v1/address/$ADDRESS/tokens" | jq .
