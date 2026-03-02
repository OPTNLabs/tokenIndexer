#!/usr/bin/env bash
set -euo pipefail

# Reorg smoke test for BCHN-compatible nodes exposing invalidate/reconsider RPC.
RPC_URL="${TOKENINDEX_RPC_URL:-http://127.0.0.1:48334}"
RPC_USER="${TOKENINDEX_RPC_USER:-}"
RPC_PASS="${TOKENINDEX_RPC_PASS:-}"

if [[ -z "$RPC_USER" || -z "$RPC_PASS" ]]; then
  echo "TOKENINDEX_RPC_USER and TOKENINDEX_RPC_PASS are required" >&2
  exit 1
fi

rpc() {
  local method="$1"
  local params_json="${2:-[]}"
  curl -sS --user "$RPC_USER:$RPC_PASS" \
    --data-binary "{\"jsonrpc\":\"1.0\",\"id\":\"reorg\",\"method\":\"${method}\",\"params\":${params_json}}" \
    -H 'content-type: text/plain;' \
    "$RPC_URL/"
}

echo "Reading current tip..."
TIP_HASH=$(rpc getbestblockhash | sed -E 's/.*"result":"([0-9a-f]+)".*/\1/')
TIP_HEIGHT=$(rpc getblockcount | sed -E 's/.*"result":([0-9]+).*/\1/')
echo "Tip height=$TIP_HEIGHT hash=$TIP_HASH"

echo "Invalidating tip to force one-block reorg..."
rpc invalidateblock "[\"$TIP_HASH\"]" >/dev/null
sleep 2

NEW_TIP_HASH=$(rpc getbestblockhash | sed -E 's/.*"result":"([0-9a-f]+)".*/\1/')
NEW_TIP_HEIGHT=$(rpc getblockcount | sed -E 's/.*"result":([0-9]+).*/\1/')
echo "After invalidate height=$NEW_TIP_HEIGHT hash=$NEW_TIP_HASH"

echo "Reconsidering original tip..."
rpc reconsiderblock "[\"$TIP_HASH\"]" >/dev/null
sleep 2

FINAL_TIP_HASH=$(rpc getbestblockhash | sed -E 's/.*"result":"([0-9a-f]+)".*/\1/')
FINAL_TIP_HEIGHT=$(rpc getblockcount | sed -E 's/.*"result":([0-9]+).*/\1/')
echo "After reconsider height=$FINAL_TIP_HEIGHT hash=$FINAL_TIP_HASH"

echo "Reorg smoke sequence completed. Validate TokenIndex logs for rollback + reapply."
