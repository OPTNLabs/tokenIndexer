#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
CATEGORY="${CATEGORY:?set CATEGORY=<token_category_hex>}"
LOCKING="${LOCKING:?set LOCKING=<locking_bytecode_hex>}"

echo "health"
curl -fsS "$BASE_URL/health" | jq .

echo "summary"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/summary" | jq .

echo "top holders"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/holders/top?n=5" | jq .

echo "eligibility"
curl -fsS "$BASE_URL/v1/token/$CATEGORY/holder/$LOCKING" | jq .

echo "holder tokens"
curl -fsS "$BASE_URL/v1/holder/$LOCKING/tokens" | jq .
