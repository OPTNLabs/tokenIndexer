#!/bin/sh
set -eu

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ"
}

log() {
    printf '[entrypoint] %s %s\n' "$(timestamp)" "$*"
}

redact_url() {
    value="${1:-}"
    if [ -z "$value" ]; then
        printf '<unset>'
        return
    fi

    printf '%s' "$value" | sed -E 's#(://)[^:/@]+(:[^/@]*)?@#\1****:****@#'
}

log "starting tokenindex container"
log "cwd=$(pwd) uid=$(id -u) gid=$(id -g)"
log "api_bind=${TOKENINDEX_API_HOST:-<unset>}:${TOKENINDEX_API_PORT:-<unset>} log_level=${TOKENINDEX_LOG_LEVEL:-<unset>}"
log "database_url=$(redact_url "${TOKENINDEX_DATABASE_URL:-}")"
log "database_read_url=$(redact_url "${TOKENINDEX_DATABASE_READ_URL:-}") schema=${TOKENINDEX_DB_SCHEMA:-<unset>}"
log "rpc_url=$(redact_url "${TOKENINDEX_RPC_URL:-}") expected_chain=${TOKENINDEX_EXPECTED_CHAIN:-<unset>}"
log "rpc_batch_size=${TOKENINDEX_RPC_BATCH_SIZE:-<unset>} rpc_prefetch_batches=${TOKENINDEX_RPC_PREFETCH_BATCHES:-<unset>}"
log "db_ingest_synchronous_commit=${TOKENINDEX_DB_INGEST_SYNCHRONOUS_COMMIT:-<unset>}"
log "feature_flags mempool=${TOKENINDEX_MEMPOOL_ENABLED:-<unset>} bcmr=${TOKENINDEX_BCMR_ENABLED:-<unset>} reconcile=${TOKENINDEX_RECONCILE_ENABLED:-<unset>} redis=$( [ -n "${TOKENINDEX_REDIS_URL:-}" ] && printf true || printf false )"
log "postgres_tuning_apply=${TOKENINDEX_APPLY_POSTGRES_TUNING:-false}"
log "migrations_dir=$(ls -1 /app/migrations 2>/dev/null | wc -l | tr -d ' ') files"

if [ "${TOKENINDEX_APPLY_POSTGRES_TUNING:-false}" = "true" ]; then
    if [ -z "${TOKENINDEX_DATABASE_URL:-}" ]; then
        log "TOKENINDEX_APPLY_POSTGRES_TUNING=true but TOKENINDEX_DATABASE_URL is unset"
        exit 1
    fi

    log "applying postgres tuning script"
    PGPASSWORD="${PGPASSWORD:-}" psql "${TOKENINDEX_DATABASE_URL}" \
        -v ON_ERROR_STOP=1 \
        -f /app/scripts/ops/postgres_tuning.sql
    log "postgres tuning script applied"
fi

exec /usr/local/bin/tokenindex "$@"
