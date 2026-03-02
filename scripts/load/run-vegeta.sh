#!/usr/bin/env bash
set -euo pipefail

TARGETS_FILE="${1:-scripts/load/vegeta-holders-targets.txt}"
RATE="${RATE:-500}"
DURATION="${DURATION:-60s}"

if ! command -v vegeta >/dev/null 2>&1; then
  echo "vegeta is required" >&2
  exit 1
fi

vegeta attack -targets="$TARGETS_FILE" -rate="$RATE" -duration="$DURATION" | tee /tmp/tokenindex.vegeta.bin | vegeta report
echo "Saved binary results: /tmp/tokenindex.vegeta.bin"
