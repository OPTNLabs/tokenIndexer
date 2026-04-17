use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Request, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, warn};

use crate::api::cache::{CachedResponse, RedisCacheRecord};
use crate::api::{AppState, IpCidr};
use crate::db::{queries, Database};
use crate::model::{MempoolCategoryView, MempoolHolderDelta};

#[derive(Debug, Deserialize)]
pub struct TopQuery {
    #[serde(default = "default_top_n")]
    n: i64,
}

#[derive(Debug, Deserialize)]
pub struct PagedQuery {
    #[serde(default = "default_page_limit")]
    limit: i64,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct HolderTokensQuery {
    #[serde(default = "default_page_limit")]
    limit: i64,
}

#[derive(Debug, Deserialize)]
pub struct MempoolTopQuery {
    #[serde(default = "default_mempool_top_n")]
    n: i64,
}

#[derive(Debug, Deserialize)]
pub struct KnownTokensQuery {
    #[serde(default = "default_known_tokens_limit")]
    limit: i64,
}

#[derive(Debug, Serialize)]
struct ChainHealthDetails {
    chain: String,
    rpc_url: String,
    bcmr_enabled: bool,
    bcmr_backfill_enabled: bool,
    db_ok: bool,
    chain_state_present: bool,
    indexed_height: Option<i64>,
    node_height: Option<i64>,
    lag_blocks: Option<i64>,
    rpc_ok: bool,
    rpc_error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct HolderCursor {
    balance: String,
    locking_bytecode: String,
}

#[derive(Debug, sqlx::FromRow)]
struct BcmrCategoryRow {
    category: String,
    symbol: Option<String>,
    name: Option<String>,
    description: Option<String>,
    decimals: Option<i32>,
    icon_uri: Option<String>,
    token_uri: Option<String>,
    latest_revision: Option<chrono::DateTime<chrono::Utc>>,
    identity_snapshot: Option<serde_json::Value>,
    nft_types: Option<serde_json::Value>,
    updated_height: i32,
    updated_at: chrono::DateTime<chrono::Utc>,
    source_url: String,
    content_hash_hex: Option<String>,
    claimed_hash_hex: Option<String>,
    request_status: Option<i32>,
    validity_checks: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
struct TokenBcmrMetadata {
    symbol: Option<String>,
    name: Option<String>,
    description: Option<String>,
    decimals: Option<i32>,
    icon_uri: Option<String>,
    token_uri: Option<String>,
    latest_revision: Option<chrono::DateTime<chrono::Utc>>,
    identity_snapshot: Option<serde_json::Value>,
    nft_types: Option<serde_json::Value>,
    source_url: Option<String>,
    content_hash_hex: Option<String>,
    claimed_hash_hex: Option<String>,
    request_status: Option<i32>,
    validity_checks: Option<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow, Clone)]
struct TokenSummaryRow {
    category: String,
    total_supply: String,
    holder_count: i32,
    utxo_count: i32,
    updated_height: i32,
    updated_at: chrono::DateTime<chrono::Utc>,
    symbol: Option<String>,
    name: Option<String>,
    description: Option<String>,
    decimals: Option<i32>,
    icon_uri: Option<String>,
    token_uri: Option<String>,
    latest_revision: Option<chrono::DateTime<chrono::Utc>>,
    identity_snapshot: Option<serde_json::Value>,
    nft_types: Option<serde_json::Value>,
    source_url: Option<String>,
    content_hash_hex: Option<String>,
    claimed_hash_hex: Option<String>,
    request_status: Option<i32>,
    validity_checks: Option<serde_json::Value>,
}

#[derive(Debug, sqlx::FromRow, Clone)]
struct KnownTokenRow {
    category: String,
    total_supply: String,
    holder_count: i32,
    utxo_count: i32,
    updated_height: i32,
    updated_at: chrono::DateTime<chrono::Utc>,
    description: Option<String>,
    decimals: Option<i32>,
    icon_uri: Option<String>,
    token_uri: Option<String>,
    latest_revision: Option<chrono::DateTime<chrono::Utc>>,
    identity_snapshot: Option<serde_json::Value>,
    nft_types: Option<serde_json::Value>,
    source_url: Option<String>,
    content_hash_hex: Option<String>,
    claimed_hash_hex: Option<String>,
    request_status: Option<i32>,
    validity_checks: Option<serde_json::Value>,
    symbol: Option<String>,
    name: Option<String>,
}

#[derive(Debug, sqlx::FromRow, Clone)]
struct HolderTokenRow {
    category: String,
    locking_address: Option<String>,
    ft_balance: String,
    utxo_count: i32,
    updated_height: i32,
    symbol: Option<String>,
    name: Option<String>,
    description: Option<String>,
    decimals: Option<i32>,
    icon_uri: Option<String>,
    token_uri: Option<String>,
    latest_revision: Option<chrono::DateTime<chrono::Utc>>,
    identity_snapshot: Option<serde_json::Value>,
    nft_types: Option<serde_json::Value>,
    source_url: Option<String>,
    content_hash_hex: Option<String>,
    claimed_hash_hex: Option<String>,
    request_status: Option<i32>,
    validity_checks: Option<serde_json::Value>,
}

impl From<&BcmrCategoryRow> for TokenBcmrMetadata {
    fn from(value: &BcmrCategoryRow) -> Self {
        Self {
            symbol: value.symbol.clone(),
            name: value.name.clone(),
            description: value.description.clone(),
            decimals: value.decimals,
            icon_uri: value.icon_uri.clone(),
            token_uri: value.token_uri.clone(),
            latest_revision: value.latest_revision,
            identity_snapshot: value.identity_snapshot.clone(),
            nft_types: value.nft_types.clone(),
            source_url: Some(value.source_url.clone()),
            content_hash_hex: value.content_hash_hex.clone(),
            claimed_hash_hex: value.claimed_hash_hex.clone(),
            request_status: value.request_status,
            validity_checks: value.validity_checks.clone(),
        }
    }
}

impl From<&TokenSummaryRow> for TokenBcmrMetadata {
    fn from(value: &TokenSummaryRow) -> Self {
        Self {
            symbol: value.symbol.clone(),
            name: value.name.clone(),
            description: value.description.clone(),
            decimals: value.decimals,
            icon_uri: value.icon_uri.clone(),
            token_uri: value.token_uri.clone(),
            latest_revision: value.latest_revision,
            identity_snapshot: value.identity_snapshot.clone(),
            nft_types: value.nft_types.clone(),
            source_url: value.source_url.clone(),
            content_hash_hex: value.content_hash_hex.clone(),
            claimed_hash_hex: value.claimed_hash_hex.clone(),
            request_status: value.request_status,
            validity_checks: value.validity_checks.clone(),
        }
    }
}

impl From<&KnownTokenRow> for TokenBcmrMetadata {
    fn from(value: &KnownTokenRow) -> Self {
        Self {
            symbol: value.symbol.clone(),
            name: value.name.clone(),
            description: value.description.clone(),
            decimals: value.decimals,
            icon_uri: value.icon_uri.clone(),
            token_uri: value.token_uri.clone(),
            latest_revision: value.latest_revision,
            identity_snapshot: value.identity_snapshot.clone(),
            nft_types: value.nft_types.clone(),
            source_url: value.source_url.clone(),
            content_hash_hex: value.content_hash_hex.clone(),
            claimed_hash_hex: value.claimed_hash_hex.clone(),
            request_status: value.request_status,
            validity_checks: value.validity_checks.clone(),
        }
    }
}

impl From<&HolderTokenRow> for TokenBcmrMetadata {
    fn from(value: &HolderTokenRow) -> Self {
        Self {
            symbol: value.symbol.clone(),
            name: value.name.clone(),
            description: value.description.clone(),
            decimals: value.decimals,
            icon_uri: value.icon_uri.clone(),
            token_uri: value.token_uri.clone(),
            latest_revision: value.latest_revision,
            identity_snapshot: value.identity_snapshot.clone(),
            nft_types: value.nft_types.clone(),
            source_url: value.source_url.clone(),
            content_hash_hex: value.content_hash_hex.clone(),
            claimed_hash_hex: value.claimed_hash_hex.clone(),
            request_status: value.request_status,
            validity_checks: value.validity_checks.clone(),
        }
    }
}

enum CacheRead {
    Fresh(Arc<CachedResponse>),
    Stale(Arc<CachedResponse>),
    Miss,
}

fn default_top_n() -> i64 {
    50
}

fn default_page_limit() -> i64 {
    100
}

fn default_mempool_top_n() -> i64 {
    20
}

fn default_known_tokens_limit() -> i64 {
    200
}

pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path.starts_with("/health") || path == "/metrics" {
        return next.run(request).await;
    }

    let route_key = classify_rate_limit_route(path);
    let rps = match route_key {
        "holders" => state.config.rate_limit_holders_rps,
        "eligibility" => state.config.rate_limit_eligibility_rps,
        _ => state.config.rate_limit_default_rps,
    };
    let client_ip = extract_client_ip_addr(
        &request,
        state.config.trust_x_forwarded_for,
        &state.trusted_proxy_cidrs,
    )
    .map(|ip| ip.to_string())
    .unwrap_or_else(|| "unknown".to_string());

    if !state.rate_limiter.check(&client_ip, route_key, rps) {
        state.metrics.rate_limited.fetch_add(1, Ordering::Relaxed);
        let mut headers = HeaderMap::new();
        headers.insert(header::RETRY_AFTER, HeaderValue::from_static("1"));
        return (
            StatusCode::TOO_MANY_REQUESTS,
            headers,
            Json(json!({
                "error": {
                    "code": "rate_limited",
                    "message": "Too many requests for this route and client"
                }
            })),
        )
            .into_response();
    }

    next.run(request).await
}

pub async fn security_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path.starts_with("/health") {
        return next.run(request).await;
    }

    let client_ip = extract_client_ip_addr(
        &request,
        state.config.trust_x_forwarded_for,
        &state.trusted_proxy_cidrs,
    );
    if !state.ip_allowlist.is_empty() {
        let allowed = client_ip
            .map(|ip| state.ip_allowlist.iter().any(|cidr| cidr.contains(ip)))
            .unwrap_or(false);
        if !allowed {
            return error_json(
                StatusCode::FORBIDDEN,
                "ip_not_allowed",
                "Request IP is not allowed",
            );
        }
    }

    if let Some(expected) = state.api_bearer_token.as_ref() {
        if path.starts_with("/v1/") || path == "/metrics" {
            let token_ok = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .and_then(parse_bearer_token)
                .map(|token| constant_time_eq(token.as_bytes(), expected.as_bytes()))
                .unwrap_or(false);
            if !token_ok {
                let mut headers = HeaderMap::new();
                headers.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
                return (
                    StatusCode::UNAUTHORIZED,
                    headers,
                    Json(json!({
                        "error": {
                            "code": "unauthorized",
                            "message": "Missing or invalid bearer token"
                        }
                    })),
                )
                    .into_response();
            }
        }
    }

    next.run(request).await
}

pub async fn timing_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let started = Instant::now();
    let response = next.run(request).await;
    state
        .metrics
        .observe_request_duration_ms(started.elapsed().as_millis() as u64);
    response
}

pub async fn metrics(State(state): State<Arc<AppState>>) -> Response {
    let body = state.metrics.render_prometheus();
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    (StatusCode::OK, headers, body).into_response()
}

pub async fn health(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let db_ok = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(state.db.pool())
        .await
        .is_ok();

    let chain_state_present =
        sqlx::query_scalar::<_, i64>("SELECT height::bigint FROM chain_state WHERE id = TRUE")
            .fetch_optional(state.db.pool())
            .await
            .ok()
            .flatten()
            .is_some();

    let status = if db_ok && chain_state_present {
        "ok"
    } else {
        "degraded"
    };

    (
        StatusCode::OK,
        Json(json!({
            "status": status,
            "db_ok": db_ok,
            "chain_state_present": chain_state_present,
        })),
    )
}

pub async fn health_details(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let primary = inspect_chain_health(&state.db, &state.config).await;
    let secondary = if let (Some(db), Some(cfg)) = (&state.fallback_db, &state.fallback_config) {
        Some(inspect_chain_health(db, cfg).await)
    } else {
        None
    };

    let all_ok = primary.db_ok
        && primary.rpc_ok
        && (!primary.chain_state_present || primary.lag_blocks.unwrap_or(0) >= 0)
        && secondary
            .as_ref()
            .map(|s| {
                s.db_ok && s.rpc_ok && (!s.chain_state_present || s.lag_blocks.unwrap_or(0) >= 0)
            })
            .unwrap_or(true);

    let status = if all_ok { "ok" } else { "degraded" };

    (
        StatusCode::OK,
        Json(json!({
            "status": status,
            "chains": {
                "primary": primary,
                "secondary": secondary
            }
        })),
    )
}

async fn inspect_chain_health(db: &Database, cfg: &crate::config::Config) -> ChainHealthDetails {
    let db_ok = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(db.pool())
        .await
        .is_ok();

    let indexed_height: Option<i64> =
        sqlx::query_scalar("SELECT height::bigint FROM chain_state WHERE id = TRUE")
            .fetch_optional(db.pool())
            .await
            .ok()
            .flatten();

    let (node_height, rpc_error) =
        match rpc_block_count(&cfg.rpc_url, &cfg.rpc_user, &cfg.rpc_pass).await {
            Ok(height) => (Some(height), None),
            Err(err) => (None, Some(err)),
        };

    let lag_blocks = match (indexed_height, node_height) {
        (Some(indexed), Some(node)) => Some((node - indexed).max(0)),
        _ => None,
    };

    ChainHealthDetails {
        chain: cfg.expected_chain.clone(),
        rpc_url: cfg.rpc_url.clone(),
        bcmr_enabled: cfg.bcmr_enabled,
        bcmr_backfill_enabled: cfg.bcmr_backfill_enabled,
        db_ok,
        chain_state_present: indexed_height.is_some(),
        indexed_height,
        node_height,
        lag_blocks,
        rpc_ok: rpc_error.is_none(),
        rpc_error,
    }
}

async fn rpc_block_count(url: &str, user: &str, pass: &str) -> Result<i64, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .map_err(|err| format!("rpc client build failed: {err}"))?;

    let body = json!({
        "jsonrpc": "1.0",
        "id": "health",
        "method": "getblockcount",
        "params": []
    });

    let resp = client
        .post(url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .await
        .map_err(|err| format!("rpc request failed: {err}"))?;

    let status = resp.status();
    let payload: serde_json::Value = resp
        .json()
        .await
        .map_err(|err| format!("rpc response decode failed: {err}"))?;

    if !status.is_success() {
        return Err(format!("rpc http error {status}: {payload}"));
    }

    if !payload["error"].is_null() {
        return Err(format!("rpc returned error: {}", payload["error"]));
    }

    payload["result"]
        .as_i64()
        .ok_or_else(|| "rpc result missing block height".to_string())
}

pub async fn token_summary(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(category): Path<String>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_db = pick_db(&state, use_fallback);
    let active_snapshot = pick_mempool_snapshot(&state, use_fallback);
    let (mempool_version, mempool_view) = {
        let snapshot = active_snapshot.read().await;
        (
            snapshot.updated_at.timestamp(),
            snapshot.categories.get(&category).cloned(),
        )
    };
    let cache_key = format!(
        "v1:{}:summary:{category}:m{mempool_version}",
        state.config.expected_chain
    );
    let mut _fill_guard: Option<OwnedMutexGuard<()>> = None;

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let row = sqlx::query_as::<_, TokenSummaryRow>(queries::TOKEN_SUMMARY)
                .bind(category.clone())
                .fetch_optional(active_db.pool())
                .await;

            return match row {
                Ok(Some(row)) => {
                    let bcmr = TokenBcmrMetadata::from(&row);
                    let body = build_unified_summary_body(
                        &row.category,
                        &row.total_supply,
                        row.holder_count,
                        row.utxo_count,
                        row.updated_height,
                        row.updated_at,
                        active_chain_name(&state, use_fallback),
                        mempool_view.as_ref(),
                        &bcmr,
                    );
                    cache_and_respond(&state, &cache_key, body, row.updated_height, 10, &headers)
                }
                Ok(None) => error_json(
                    StatusCode::NOT_FOUND,
                    "token_not_found",
                    "Category not indexed",
                ),
                Err(_) => {
                    state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
                    state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                    cached_to_response(&stale, &headers, true)
                }
            };
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            _fill_guard = Some(cache_fill_lock(&state, &cache_key).await);
            if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
                state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return cached_to_response(&cached, &headers, false);
            }
        }
    }

    let row = sqlx::query_as::<_, TokenSummaryRow>(queries::TOKEN_SUMMARY)
        .bind(category.clone())
        .fetch_optional(active_db.pool())
        .await;

    match row {
        Ok(Some(row)) => {
            let bcmr = TokenBcmrMetadata::from(&row);
            let body = build_unified_summary_body(
                &row.category,
                &row.total_supply,
                row.holder_count,
                row.utxo_count,
                row.updated_height,
                row.updated_at,
                active_chain_name(&state, use_fallback),
                mempool_view.as_ref(),
                &bcmr,
            );
            cache_and_respond(&state, &cache_key, body, row.updated_height, 10, &headers)
        }
        Ok(None) => error_json(
            StatusCode::NOT_FOUND,
            "token_not_found",
            "Category not indexed",
        ),
        Err(err) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error!(
                category = %category,
                use_fallback,
                error = ?err,
                "token summary query failed"
            );
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading token summary",
            )
        }
    }
}

pub async fn known_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<KnownTokensQuery>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let limit = query.limit.clamp(1, 1000);
    let cache_key = format!("v1:{}:known_tokens:{limit}", state.config.expected_chain);

    if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
        state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
        return cached_to_response(&cached, &headers, false);
    }

    state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);

    let primary_rows = sqlx::query_as::<_, KnownTokenRow>(queries::KNOWN_TOKENS)
        .bind(limit)
        .fetch_all(state.db.pool())
        .await;

    let mut merged: HashMap<String, (KnownTokenRow, String)> = HashMap::new();
    match primary_rows {
        Ok(rows) => {
            for row in rows {
                merged.insert(
                    row.category.clone(),
                    (row, state.config.expected_chain.clone()),
                );
            }
        }
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            return error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading known tokens",
            );
        }
    }

    if let Some(fallback_db) = state.fallback_db.as_ref() {
        if let Ok(rows) = sqlx::query_as::<_, KnownTokenRow>(queries::KNOWN_TOKENS)
            .bind(limit)
            .fetch_all(fallback_db.pool())
            .await
        {
            let fallback_chain = fallback_chain_name(&state).to_string();
            for row in rows {
                match merged.get(&row.category) {
                    Some((existing, _)) if existing.updated_height >= row.updated_height => {}
                    _ => {
                        merged.insert(row.category.clone(), (row, fallback_chain.clone()));
                    }
                }
            }
        }
    }

    let mut tokens: Vec<_> = merged
        .into_values()
        .map(|(row, chain)| {
            let bcmr = TokenBcmrMetadata::from(&row);
            json!({
                "category": row.category,
                "chain": chain,
                "name": row.name,
                "symbol": row.symbol,
                "bcmr": bcmr_metadata_json(&bcmr),
                "total_supply": row.total_supply,
                "holder_count": row.holder_count,
                "utxo_count": row.utxo_count,
                "updated_height": row.updated_height,
                "updated_at": row.updated_at,
            })
        })
        .collect();

    tokens.sort_by(|a, b| {
        let ah = a["holder_count"].as_i64().unwrap_or(0);
        let bh = b["holder_count"].as_i64().unwrap_or(0);
        bh.cmp(&ah).then_with(|| {
            let au = a["updated_height"].as_i64().unwrap_or(0);
            let bu = b["updated_height"].as_i64().unwrap_or(0);
            bu.cmp(&au)
        })
    });
    tokens.truncate(limit as usize);

    let body = json!({ "tokens": tokens });
    cache_and_respond(&state, &cache_key, body, 0, 20, &headers)
}

pub async fn bcmr_category(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(category): Path<String>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_db = pick_db(&state, use_fallback);

    let cache_key = format!("v1:{}:bcmr:{}", state.config.expected_chain, category);

    if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
        state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
        return cached_to_response(&cached, &headers, false);
    }

    state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
    let row = sqlx::query_as::<_, BcmrCategoryRow>(queries::BCMR_CATEGORY_METADATA)
        .bind(&category)
        .fetch_optional(active_db.pool())
        .await;

    match row {
        Ok(Some(row)) => {
            let bcmr = TokenBcmrMetadata::from(&row);
            let body = json!({
                "category": row.category,
                "symbol": row.symbol,
                "name": row.name,
                "description": row.description,
                "decimals": row.decimals,
                "uris": {
                    "icon": row.icon_uri,
                    "token": row.token_uri,
                },
                "latest_revision": row.latest_revision,
                "identity_snapshot": row.identity_snapshot,
                "nft_types": row.nft_types,
                "registry": {
                    "source_url": row.source_url,
                    "content_hash_hex": row.content_hash_hex,
                    "claimed_hash_hex": row.claimed_hash_hex,
                    "request_status": row.request_status,
                    "validity_checks": row.validity_checks
                },
                "bcmr": bcmr_metadata_json(&bcmr),
                "updated_height": row.updated_height,
                "updated_at": row.updated_at,
            });
            cache_and_respond(&state, &cache_key, body, row.updated_height, 10, &headers)
        }
        Ok(None) => error_json(
            StatusCode::NOT_FOUND,
            "bcmr_not_found",
            "No BCMR metadata resolved for this category",
        ),
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading BCMR metadata",
            )
        }
    }
}

pub async fn top_holders(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(category): Path<String>,
    Query(query): Query<TopQuery>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_db = pick_db(&state, use_fallback);
    let active_snapshot = pick_mempool_snapshot(&state, use_fallback);
    let n = query.n.clamp(1, 500);
    let mempool_version = {
        let snapshot = active_snapshot.read().await;
        snapshot.updated_at.timestamp()
    };
    let cache_key = format!(
        "v1:{}:top:{category}:{n}:m{mempool_version}",
        state.config.expected_chain
    );
    let mut _fill_guard: Option<OwnedMutexGuard<()>> = None;

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(
                queries::TOP_HOLDERS,
            )
            .bind(&category)
            .bind(n)
            .fetch_all(active_db.pool())
            .await;

            return match rows {
                Ok(rows) => {
                    let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
                    let holders =
                        overlay_holders_for_category(active_snapshot.clone(), &category, rows)
                            .await;

                    cache_and_respond(
                        &state,
                        &cache_key,
                        json!({"holders": holders}),
                        updated_height,
                        10,
                        &headers,
                    )
                }
                Err(_) => {
                    state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
                    state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                    cached_to_response(&stale, &headers, true)
                }
            };
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            _fill_guard = Some(cache_fill_lock(&state, &cache_key).await);
            if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
                state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return cached_to_response(&cached, &headers, false);
            }
        }
    }

    let rows =
        sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::TOP_HOLDERS)
            .bind(&category)
            .bind(n)
            .fetch_all(active_db.pool())
            .await;

    match rows {
        Ok(rows) => {
            let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
            let holders =
                overlay_holders_for_category(active_snapshot.clone(), &category, rows).await;

            cache_and_respond(
                &state,
                &cache_key,
                json!({"holders": holders}),
                updated_height,
                10,
                &headers,
            )
        }
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading top holders",
            )
        }
    }
}

pub async fn paged_holders(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(category): Path<String>,
    Query(query): Query<PagedQuery>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_db = pick_db(&state, use_fallback);
    let active_snapshot = pick_mempool_snapshot(&state, use_fallback);
    let limit = query.limit.clamp(1, 500);
    let cursor_key = query.cursor.clone().unwrap_or_default();
    let mempool_version = {
        let snapshot = active_snapshot.read().await;
        snapshot.updated_at.timestamp()
    };
    let cache_key = format!(
        "v1:{}:holders:{category}:{limit}:{cursor_key}:m{mempool_version}",
        state.config.expected_chain
    );
    let mut _fill_guard: Option<OwnedMutexGuard<()>> = None;

    let decoded_cursor = match query.cursor {
        Some(value) => {
            if value.len() > state.config.max_cursor_chars {
                return error_json(
                    StatusCode::BAD_REQUEST,
                    "invalid_cursor",
                    "Cursor is too large",
                );
            }
            let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
                .decode(value)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<HolderCursor>(&bytes).ok());
            match decoded {
                Some(cursor) => Some(cursor),
                None => {
                    return error_json(
                        StatusCode::BAD_REQUEST,
                        "invalid_cursor",
                        "Cursor is invalid",
                    );
                }
            }
        }
        None => None,
    };

    let (cursor_balance, cursor_locking_bytecode) = match decoded_cursor {
        Some(cursor) => (Some(cursor.balance), Some(cursor.locking_bytecode)),
        None => (None, None),
    };

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(
                queries::PAGED_HOLDERS,
            )
            .bind(&category)
            .bind(&cursor_balance)
            .bind(&cursor_locking_bytecode)
            .bind(limit)
            .fetch_all(active_db.pool())
            .await;

            return match rows {
                Ok(rows) => {
                    let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
                    let next_cursor = rows.last().map(|last| {
                        let payload = HolderCursor {
                            balance: last.2.clone(),
                            locking_bytecode: last.0.clone(),
                        };
                        base64::engine::general_purpose::URL_SAFE_NO_PAD
                            .encode(serde_json::to_vec(&payload).unwrap_or_default())
                    });

                    let holders =
                        overlay_holders_for_category(active_snapshot.clone(), &category, rows)
                            .await;

                    cache_and_respond(
                        &state,
                        &cache_key,
                        json!({"holders": holders, "next_cursor": next_cursor}),
                        updated_height,
                        10,
                        &headers,
                    )
                }
                Err(_) => {
                    state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
                    state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                    cached_to_response(&stale, &headers, true)
                }
            };
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            _fill_guard = Some(cache_fill_lock(&state, &cache_key).await);
            if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
                state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return cached_to_response(&cached, &headers, false);
            }
        }
    }

    let rows =
        sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::PAGED_HOLDERS)
            .bind(&category)
            .bind(&cursor_balance)
            .bind(&cursor_locking_bytecode)
            .bind(limit)
            .fetch_all(active_db.pool())
            .await;

    match rows {
        Ok(rows) => {
            let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
            let next_cursor = rows.last().map(|last| {
                let payload = HolderCursor {
                    balance: last.2.clone(),
                    locking_bytecode: last.0.clone(),
                };
                base64::engine::general_purpose::URL_SAFE_NO_PAD
                    .encode(serde_json::to_vec(&payload).unwrap_or_default())
            });

            let holders =
                overlay_holders_for_category(active_snapshot.clone(), &category, rows).await;

            cache_and_respond(
                &state,
                &cache_key,
                json!({"holders": holders, "next_cursor": next_cursor}),
                updated_height,
                10,
                &headers,
            )
        }
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading paged holders",
            )
        }
    }
}

pub async fn holder_eligibility(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path((category, address)): Path<(String, String)>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    if !is_valid_holder_address(&address) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_address",
            "Address must be a non-empty string",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_db = pick_db(&state, use_fallback);
    let active_snapshot = pick_mempool_snapshot(&state, use_fallback);
    let (mempool_version, unconfirmed_ft_delta, unconfirmed_utxo_delta, unconfirmed_address) = {
        let snapshot = active_snapshot.read().await;
        let delta = snapshot
            .categories
            .get(&category)
            .map(|view| mempool_delta_for_address(view, &address))
            .filter(|delta| {
                parse_bigint_str(&delta.ft_delta) != num_bigint::BigInt::from(0u8)
                    || delta.utxo_delta != 0
                    || delta.locking_address.is_some()
            });
        (
            snapshot.updated_at.timestamp(),
            delta
                .as_ref()
                .map(|v| v.ft_delta.clone())
                .unwrap_or_else(|| "0".to_string()),
            delta.as_ref().map(|v| v.utxo_delta).unwrap_or(0),
            delta.and_then(|v| v.locking_address),
        )
    };
    let cache_key = format!(
        "v1:{}:eligibility:{category}:{address}:m{mempool_version}",
        state.config.expected_chain
    );
    let mut _fill_guard: Option<OwnedMutexGuard<()>> = None;

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let row = sqlx::query_as::<_, (Option<String>, String, i32, i32)>(queries::ELIGIBILITY)
                .bind(&category)
                .bind(&address)
                .fetch_optional(active_db.pool())
                .await;

            return match row {
                Ok(Some((locking_address, ft_balance, utxo_count, updated_height))) => {
                    let effective_ft =
                        parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                    let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                    cache_and_respond(
                        &state,
                        &cache_key,
                        json!({
                            "eligible": effective_ft > num_bigint::BigInt::from(0u8),
                            "confirmed_eligible": ft_balance != "0",
                            "effective_eligible": effective_ft > num_bigint::BigInt::from(0u8),
                            "address": locking_address.clone().or(unconfirmed_address.clone()),
                            "locking_address": locking_address.or(unconfirmed_address.clone()),
                            "ft_balance": effective_ft.to_string(),
                            "utxo_count": effective_utxo,
                            "confirmed_ft_balance": ft_balance,
                            "unconfirmed_ft_delta": unconfirmed_ft_delta,
                            "effective_ft_balance": effective_ft.to_string(),
                            "confirmed_utxo_count": utxo_count,
                            "unconfirmed_utxo_delta": unconfirmed_utxo_delta,
                            "effective_utxo_count": effective_utxo,
                            "updated_height": updated_height,
                        }),
                        updated_height,
                        5,
                        &headers,
                    )
                }
                Ok(None) => cache_and_respond(
                    &state,
                    &cache_key,
                    json!({
                        "eligible": parse_bigint_str(&unconfirmed_ft_delta) > num_bigint::BigInt::from(0u8),
                        "confirmed_eligible": false,
                        "effective_eligible": parse_bigint_str(&unconfirmed_ft_delta) > num_bigint::BigInt::from(0u8),
                        "address": unconfirmed_address.clone(),
                        "locking_address": unconfirmed_address.clone(),
                        "ft_balance": unconfirmed_ft_delta.clone(),
                        "utxo_count": unconfirmed_utxo_delta.max(0),
                        "confirmed_ft_balance": "0",
                        "unconfirmed_ft_delta": unconfirmed_ft_delta,
                        "effective_ft_balance": unconfirmed_ft_delta.clone(),
                        "confirmed_utxo_count": 0,
                        "unconfirmed_utxo_delta": unconfirmed_utxo_delta,
                        "effective_utxo_count": unconfirmed_utxo_delta.max(0),
                        "updated_height": 0,
                    }),
                    0,
                    5,
                    &headers,
                ),
                Err(_) => {
                    state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
                    state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                    cached_to_response(&stale, &headers, true)
                }
            };
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            _fill_guard = Some(cache_fill_lock(&state, &cache_key).await);
            if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
                state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return cached_to_response(&cached, &headers, false);
            }
        }
    }

    let row = sqlx::query_as::<_, (Option<String>, String, i32, i32)>(queries::ELIGIBILITY)
        .bind(&category)
        .bind(&address)
        .fetch_optional(active_db.pool())
        .await;

    match row {
        Ok(Some((locking_address, ft_balance, utxo_count, updated_height))) => {
            let effective_ft =
                parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
            let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
            cache_and_respond(
                &state,
                &cache_key,
                json!({
                    "eligible": effective_ft > num_bigint::BigInt::from(0u8),
                    "confirmed_eligible": ft_balance != "0",
                    "effective_eligible": effective_ft > num_bigint::BigInt::from(0u8),
                    "address": locking_address.clone().or(unconfirmed_address.clone()),
                    "locking_address": locking_address.or(unconfirmed_address),
                    "ft_balance": effective_ft.to_string(),
                    "utxo_count": effective_utxo,
                    "confirmed_ft_balance": ft_balance,
                    "unconfirmed_ft_delta": unconfirmed_ft_delta,
                    "effective_ft_balance": effective_ft.to_string(),
                    "confirmed_utxo_count": utxo_count,
                    "unconfirmed_utxo_delta": unconfirmed_utxo_delta,
                    "effective_utxo_count": effective_utxo,
                    "updated_height": updated_height,
                }),
                updated_height,
                5,
                &headers,
            )
        }
        Ok(None) => cache_and_respond(
            &state,
            &cache_key,
            json!({
                "eligible": parse_bigint_str(&unconfirmed_ft_delta) > num_bigint::BigInt::from(0u8),
                "confirmed_eligible": false,
                "effective_eligible": parse_bigint_str(&unconfirmed_ft_delta) > num_bigint::BigInt::from(0u8),
                "address": unconfirmed_address.clone(),
                "locking_address": unconfirmed_address,
                "ft_balance": unconfirmed_ft_delta.clone(),
                "utxo_count": unconfirmed_utxo_delta.max(0),
                "confirmed_ft_balance": "0",
                "unconfirmed_ft_delta": unconfirmed_ft_delta.clone(),
                "effective_ft_balance": unconfirmed_ft_delta,
                "confirmed_utxo_count": 0,
                "unconfirmed_utxo_delta": unconfirmed_utxo_delta,
                "effective_utxo_count": unconfirmed_utxo_delta.max(0),
                "updated_height": 0,
            }),
            0,
            5,
            &headers,
        ),
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading holder eligibility",
            )
        }
    }
}

pub async fn holder_tokens(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(address): Path<String>,
    Query(query): Query<HolderTokensQuery>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_holder_address(&address) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_address",
            "Address must be a non-empty string",
        );
    }
    let limit = query.limit.clamp(1, 500);
    let mempool_version = {
        let snapshot = state.mempool_snapshot.read().await;
        snapshot.updated_at.timestamp()
    };
    let cache_key = format!(
        "v1:{}:holder-tokens:{address}:{limit}:m{mempool_version}",
        state.config.expected_chain
    );
    let mut _fill_guard: Option<OwnedMutexGuard<()>> = None;

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let primary_rows = sqlx::query_as::<_, HolderTokenRow>(queries::HOLDER_TOKENS)
                .bind(&address)
                .bind(limit)
                .fetch_all(state.db.pool())
                .await;

            return match primary_rows {
                Ok(mut rows) => {
                    if let Some(fallback_db) = state.fallback_db.as_ref() {
                        if let Ok(mut fallback_rows) =
                            sqlx::query_as::<_, HolderTokenRow>(queries::HOLDER_TOKENS)
                                .bind(&address)
                                .bind(limit)
                                .fetch_all(fallback_db.pool())
                                .await
                        {
                            rows.append(&mut fallback_rows);
                            rows.sort_by(|a, b| {
                                parse_bigint_str(&b.ft_balance)
                                    .cmp(&parse_bigint_str(&a.ft_balance))
                                    .then_with(|| a.category.cmp(&b.category))
                            });
                            rows.truncate(limit as usize);
                        }
                    }
                    let updated_height = rows.first().map(|r| r.updated_height).unwrap_or(0);
                    let tokens = overlay_holder_tokens(&state, &address, rows).await;

                    cache_and_respond(
                        &state,
                        &cache_key,
                        json!({"tokens": tokens}),
                        updated_height,
                        10,
                        &headers,
                    )
                }
                Err(_) => {
                    state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
                    state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                    cached_to_response(&stale, &headers, true)
                }
            };
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            _fill_guard = Some(cache_fill_lock(&state, &cache_key).await);
            if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
                state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
                return cached_to_response(&cached, &headers, false);
            }
        }
    }

    let primary_rows = sqlx::query_as::<_, HolderTokenRow>(queries::HOLDER_TOKENS)
        .bind(&address)
        .bind(limit)
        .fetch_all(state.db.pool())
        .await;

    match primary_rows {
        Ok(mut rows) => {
            if let Some(fallback_db) = state.fallback_db.as_ref() {
                if let Ok(mut fallback_rows) =
                    sqlx::query_as::<_, HolderTokenRow>(queries::HOLDER_TOKENS)
                        .bind(&address)
                        .bind(limit)
                        .fetch_all(fallback_db.pool())
                        .await
                {
                    rows.append(&mut fallback_rows);
                    rows.sort_by(|a, b| {
                        parse_bigint_str(&b.ft_balance)
                            .cmp(&parse_bigint_str(&a.ft_balance))
                            .then_with(|| a.category.cmp(&b.category))
                    });
                    rows.truncate(limit as usize);
                }
            }
            let updated_height = rows.first().map(|r| r.updated_height).unwrap_or(0);
            let tokens = overlay_holder_tokens(&state, &address, rows).await;

            cache_and_respond(
                &state,
                &cache_key,
                json!({"tokens": tokens}),
                updated_height,
                10,
                &headers,
            )
        }
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading holder token list",
            )
        }
    }
}

pub async fn token_mempool(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
    Query(query): Query<MempoolTopQuery>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_snapshot = pick_mempool_snapshot(&state, use_fallback);
    let n = query.n.clamp(1, 200) as usize;
    let snapshot = active_snapshot.read().await;
    let mempool_updated_at = snapshot.updated_at;
    let category_view = snapshot.categories.get(&category).cloned();
    drop(snapshot);

    match category_view {
        Some(view) => {
            let mut holders: Vec<MempoolHolderDelta> = view.holders.into_values().collect();
            holders
                .sort_by(|a, b| parse_bigint_str(&b.ft_delta).cmp(&parse_bigint_str(&a.ft_delta)));
            holders.truncate(n);
            let body = json!({
                "category": category,
                "mempool_updated_at": mempool_updated_at,
                "tx_count": view.tx_count,
                "ft_credits": view.ft_credits,
                "ft_debits": view.ft_debits,
                "net_ft_delta": view.net_ft_delta,
                "utxo_delta": view.utxo_delta,
                "nft_count": view.nft_count,
                "holders_top_delta": holders,
            });
            with_common_headers(StatusCode::OK, body, 0, 5)
        }
        None => with_common_headers(
            StatusCode::OK,
            json!({
                "category": category,
                "mempool_updated_at": mempool_updated_at,
                "tx_count": 0,
                "ft_credits": "0",
                "ft_debits": "0",
                "net_ft_delta": "0",
                "utxo_delta": 0,
                "nft_count": 0,
                "holders_top_delta": [],
            }),
            0,
            5,
        ),
    }
}

pub async fn token_insights(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(category): Path<String>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    if !is_valid_hex_bytes(&category, 32, 32) {
        return error_json(
            StatusCode::BAD_REQUEST,
            "invalid_category",
            "Category must be 32-byte hex",
        );
    }
    let use_fallback = use_fallback_for_category(&state, &category).await;
    let active_db = pick_db(&state, use_fallback);
    let active_snapshot = pick_mempool_snapshot(&state, use_fallback);
    let mempool_version = {
        let snapshot = active_snapshot.read().await;
        snapshot.updated_at.timestamp()
    };
    let cache_key = format!(
        "v1:{}:insights:{category}:m{mempool_version}",
        state.config.expected_chain
    );
    let mut _fill_guard: Option<OwnedMutexGuard<()>> = None;
    let mut stale_cached: Option<Arc<CachedResponse>> = None;

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            stale_cached = Some(stale);
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    _fill_guard = Some(cache_fill_lock(&state, &cache_key).await);
    if let CacheRead::Fresh(cached) = read_cache(&state, &cache_key).await {
        state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
        return cached_to_response(&cached, &headers, false);
    }

    let summary_row = sqlx::query_as::<_, TokenSummaryRow>(queries::TOKEN_SUMMARY)
        .bind(category.clone())
        .fetch_optional(active_db.pool())
        .await;

    let summary = match summary_row {
        Ok(Some(row)) => row,
        Ok(None) => {
            return error_json(
                StatusCode::NOT_FOUND,
                "token_not_found",
                "Category not indexed",
            );
        }
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            if let Some(stale) = stale_cached {
                state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                return cached_to_response(&stale, &headers, true);
            }
            return error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading token insights",
            );
        }
    };
    let bcmr = TokenBcmrMetadata::from(&summary);

    let top_1_sum: String = sqlx::query_scalar(queries::TOP_N_BALANCE_SUM)
        .bind(category.clone())
        .bind(1_i64)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or_else(|_| "0".to_string());

    let top_10_sum: String = sqlx::query_scalar(queries::TOP_N_BALANCE_SUM)
        .bind(category.clone())
        .bind(10_i64)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or_else(|_| "0".to_string());

    let top_100_sum: String = sqlx::query_scalar(queries::TOP_N_BALANCE_SUM)
        .bind(category.clone())
        .bind(100_i64)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or_else(|_| "0".to_string());

    let recent_24h_outputs: i64 = sqlx::query_scalar(queries::RECENT_ACTIVITY_BLOCKS)
        .bind(category.clone())
        .bind(144_i32)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or(0);

    let recent_24h_spent: i64 = sqlx::query_scalar(queries::RECENT_SPENT_BLOCKS)
        .bind(category.clone())
        .bind(144_i32)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or(0);

    let recent_24h_active_holders: i64 = sqlx::query_scalar(queries::RECENT_ACTIVE_HOLDERS_BLOCKS)
        .bind(category.clone())
        .bind(144_i32)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or(0);

    let recent_created_ft_volume: String =
        sqlx::query_scalar(queries::RECENT_CREATED_FT_VOLUME_BLOCKS)
            .bind(category.clone())
            .bind(144_i32)
            .fetch_one(active_db.pool())
            .await
            .unwrap_or_else(|_| "0".to_string());

    let recent_spent_ft_volume: String = sqlx::query_scalar(queries::RECENT_SPENT_FT_VOLUME_BLOCKS)
        .bind(category.clone())
        .bind(144_i32)
        .fetch_one(active_db.pool())
        .await
        .unwrap_or_else(|_| "0".to_string());

    let total = parse_bigint_str(&summary.total_supply);
    let top1 = parse_bigint_str(&top_1_sum);
    let top10 = parse_bigint_str(&top_10_sum);
    let top100 = parse_bigint_str(&top_100_sum);
    let top_10_share_bps = if total == num_bigint::BigInt::from(0u8) {
        0_i64
    } else {
        ((top10 * num_bigint::BigInt::from(10_000_i64)) / total.clone())
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
    };
    let top_1_share_bps = if total == num_bigint::BigInt::from(0u8) {
        0_i64
    } else {
        ((top1 * num_bigint::BigInt::from(10_000_i64)) / total.clone())
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
    };
    let top_100_share_bps = if total == num_bigint::BigInt::from(0u8) {
        0_i64
    } else {
        ((top100 * num_bigint::BigInt::from(10_000_i64)) / total)
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
    };

    let mempool = active_snapshot.read().await;
    let mempool_overlay = mempool
        .categories
        .get(&category)
        .map(|v| {
            json!({
                "tx_count": v.tx_count,
                "net_ft_delta": v.net_ft_delta,
                "utxo_delta": v.utxo_delta,
                "nft_count": v.nft_count,
            })
        })
        .unwrap_or_else(|| {
            json!({
                "tx_count": 0,
                "net_ft_delta": "0",
                "utxo_delta": 0,
                "nft_count": 0,
            })
        });

    let body = json!({
        "category": summary.category,
        "summary": {
            "total_supply": summary.total_supply,
            "holder_count": summary.holder_count,
            "utxo_count": summary.utxo_count,
            "updated_height": summary.updated_height,
            "updated_at": summary.updated_at,
        },
        "bcmr": bcmr_metadata_json(&bcmr),
        "distribution": {
            "top_1_balance": top_1_sum,
            "top_1_share_bps": top_1_share_bps,
            "top_10_balance": top_10_sum,
            "top_10_share_bps": top_10_share_bps,
            "top_100_balance": top_100_sum,
            "top_100_share_bps": top_100_share_bps,
        },
        "activity": {
            "recent_outputs_24h_blocks": recent_24h_outputs,
            "recent_spent_24h_blocks": recent_24h_spent,
            "recent_active_holders_24h_blocks": recent_24h_active_holders,
            "recent_created_ft_volume_24h_blocks": recent_created_ft_volume,
            "recent_spent_ft_volume_24h_blocks": recent_spent_ft_volume,
        },
        "mempool_overlay": mempool_overlay,
    });

    cache_and_respond(
        &state,
        &cache_key,
        body,
        summary.updated_height,
        10,
        &headers,
    )
}

async fn overlay_holders_for_category(
    snapshot_source: Arc<tokio::sync::RwLock<crate::model::MempoolSnapshot>>,
    category: &str,
    rows: Vec<(String, Option<String>, String, i32, i32)>,
) -> Vec<serde_json::Value> {
    let snapshot = snapshot_source.read().await;
    let holders = snapshot.categories.get(category).map(|view| &view.holders);

    rows.into_iter()
        .map(
            |(locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)| {
                let holder_delta = holders.and_then(|h| h.get(&locking_bytecode));
                let unconfirmed_ft_delta = holder_delta
                    .map(|v| v.ft_delta.clone())
                    .unwrap_or_else(|| "0".to_string());
                let unconfirmed_utxo_delta = holder_delta.map(|v| v.utxo_delta).unwrap_or(0);
                let effective_ft =
                    parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                let effective_address = locking_address
                    .or_else(|| holder_delta.and_then(|v| v.locking_address.clone()));
                json!({
                    "address": effective_address.clone(),
                    "locking_address": effective_address,
                    "ft_balance": effective_ft.to_string(),
                    "utxo_count": effective_utxo,
                    "confirmed_ft_balance": ft_balance,
                    "unconfirmed_ft_delta": unconfirmed_ft_delta,
                    "effective_ft_balance": effective_ft.to_string(),
                    "confirmed_utxo_count": utxo_count,
                    "unconfirmed_utxo_delta": unconfirmed_utxo_delta,
                    "effective_utxo_count": effective_utxo,
                    "updated_height": updated_height,
                })
            },
        )
        .collect()
}

async fn overlay_holder_tokens(
    state: &Arc<AppState>,
    address: &str,
    rows: Vec<HolderTokenRow>,
) -> Vec<serde_json::Value> {
    let snapshot = state.mempool_snapshot.read().await;
    let mut seen_categories = std::collections::HashSet::new();
    let mut tokens = Vec::with_capacity(rows.len().saturating_add(8));

    for row in rows {
        let bcmr = TokenBcmrMetadata::from(&row);
        let category = row.category.clone();
        seen_categories.insert(category.clone());
        let delta = snapshot
            .categories
            .get(&category)
            .map(|view| mempool_delta_for_address(view, address));
        let u_ft = delta
            .as_ref()
            .map(|v| v.ft_delta.clone())
            .unwrap_or_else(|| "0".to_string());
        let u_utxo = delta.as_ref().map(|v| v.utxo_delta).unwrap_or(0);
        let u_addr = delta.and_then(|v| v.locking_address);
        let effective_ft = parse_bigint_str(&row.ft_balance) + parse_bigint_str(&u_ft);
        let effective_utxo = (row.utxo_count + u_utxo).max(0);
        tokens.push(json!({
            "category": category,
            "name": row.name,
            "symbol": row.symbol,
            "bcmr": bcmr_metadata_json(&bcmr),
            "address": row.locking_address.clone().or(u_addr.clone()),
            "locking_address": row.locking_address.or(u_addr),
            "ft_balance": effective_ft.to_string(),
            "utxo_count": effective_utxo,
            "confirmed_ft_balance": row.ft_balance,
            "unconfirmed_ft_delta": u_ft,
            "effective_ft_balance": effective_ft.to_string(),
            "confirmed_utxo_count": row.utxo_count,
            "unconfirmed_utxo_delta": u_utxo,
            "effective_utxo_count": effective_utxo,
            "updated_height": row.updated_height,
        }));
    }

    for (category, view) in &snapshot.categories {
        if seen_categories.contains(category) {
            continue;
        }
        let delta = mempool_delta_for_address(view, address);
        if parse_bigint_str(&delta.ft_delta) == num_bigint::BigInt::from(0u8)
            && delta.utxo_delta == 0
            && delta.locking_address.is_none()
        {
            continue;
        }
        let effective_ft = parse_bigint_str(&delta.ft_delta);
        let effective_utxo = delta.utxo_delta.max(0);
        tokens.push(json!({
            "category": category,
            "name": serde_json::Value::Null,
            "symbol": serde_json::Value::Null,
            "bcmr": bcmr_metadata_json(&TokenBcmrMetadata::default()),
            "address": delta.locking_address.clone(),
            "locking_address": delta.locking_address.clone(),
            "ft_balance": effective_ft.to_string(),
            "utxo_count": effective_utxo,
            "confirmed_ft_balance": "0",
            "unconfirmed_ft_delta": delta.ft_delta.clone(),
            "effective_ft_balance": effective_ft.to_string(),
            "confirmed_utxo_count": 0,
            "unconfirmed_utxo_delta": delta.utxo_delta,
            "effective_utxo_count": effective_utxo,
            "updated_height": 0,
        }));
    }

    tokens
}

#[derive(Debug, Clone, Default)]
struct AddressMempoolDelta {
    locking_address: Option<String>,
    ft_delta: String,
    utxo_delta: i32,
}

fn mempool_delta_for_address(view: &MempoolCategoryView, address: &str) -> AddressMempoolDelta {
    let mut ft_total = num_bigint::BigInt::from(0u8);
    let mut utxo_total = 0_i32;
    let mut found_address: Option<String> = None;

    for delta in view.holders.values() {
        if delta.locking_address.as_deref() != Some(address) {
            continue;
        }
        ft_total += parse_bigint_str(&delta.ft_delta);
        utxo_total += delta.utxo_delta;
        if found_address.is_none() {
            found_address = delta.locking_address.clone();
        }
    }

    AddressMempoolDelta {
        locking_address: found_address,
        ft_delta: ft_total.to_string(),
        utxo_delta: utxo_total,
    }
}

pub fn cors_layer(config: &crate::config::Config) -> anyhow::Result<CorsLayer> {
    if config.cors_allow_origins.iter().any(|v| v == "*") {
        return Ok(CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any));
    }

    let mut origins = Vec::with_capacity(config.cors_allow_origins.len());
    for origin in &config.cors_allow_origins {
        origins.push(origin.parse::<HeaderValue>()?);
    }

    Ok(CorsLayer::new()
        .allow_origin(origins)
        .allow_methods(Any)
        .allow_headers(Any))
}

async fn read_cache(state: &Arc<AppState>, cache_key: &str) -> CacheRead {
    if let Some(cached) = state.response_cache.get(cache_key).await {
        return cache_read_from_age(state, cached);
    }

    let Some(redis_conn) = state.redis_cache.clone() else {
        return CacheRead::Miss;
    };

    let mut redis_conn = redis_conn;
    match redis_conn.get::<_, Option<String>>(cache_key).await {
        Ok(Some(payload)) => {
            state.metrics.redis_hits.fetch_add(1, Ordering::Relaxed);
            let Ok(record) = serde_json::from_str::<RedisCacheRecord>(&payload) else {
                return CacheRead::Miss;
            };
            let Some(cached) = CachedResponse::from_redis_record(record) else {
                return CacheRead::Miss;
            };
            let cached = Arc::new(cached);
            state
                .response_cache
                .insert(cache_key.to_string(), cached.clone())
                .await;
            cache_read_from_age(state, cached)
        }
        Ok(None) => {
            state.metrics.redis_misses.fetch_add(1, Ordering::Relaxed);
            CacheRead::Miss
        }
        Err(err) => {
            state.metrics.redis_misses.fetch_add(1, Ordering::Relaxed);
            warn!(error = ?err, "redis cache read failed");
            CacheRead::Miss
        }
    }
}

fn cache_read_from_age(state: &Arc<AppState>, cached: Arc<CachedResponse>) -> CacheRead {
    let age_secs = cached.age_secs();
    if age_secs <= state.config.cache_ttl_secs {
        CacheRead::Fresh(cached)
    } else {
        CacheRead::Stale(cached)
    }
}

async fn cache_fill_lock(state: &Arc<AppState>, cache_key: &str) -> OwnedMutexGuard<()> {
    let lock = state
        .inflight_cache_fills
        .entry(cache_key.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone();
    lock.lock_owned().await
}

fn cache_and_respond(
    state: &Arc<AppState>,
    cache_key: &str,
    body: serde_json::Value,
    updated_height: i32,
    max_age_secs: i32,
    request_headers: &HeaderMap,
) -> Response {
    let cached = Arc::new(CachedResponse::new(
        StatusCode::OK,
        body,
        cache_key,
        updated_height,
        &state.config.dataset_version,
        max_age_secs,
    ));
    let cache = state.response_cache.clone();
    let key = cache_key.to_string();
    let cached_for_insert = cached.clone();
    tokio::spawn(async move {
        cache.insert(key, cached_for_insert).await;
    });

    if let Some(redis_conn) = state.redis_cache.clone() {
        let ttl_secs = state
            .config
            .cache_ttl_secs
            .saturating_add(state.config.stale_while_error_secs);
        let redis_key = cache_key.to_string();
        let payload = serde_json::to_string(&cached.to_redis_record());
        if let Ok(payload) = payload {
            tokio::spawn(async move {
                let mut redis_conn = redis_conn;
                if let Err(err) = redis_conn
                    .set_ex::<_, _, ()>(redis_key, payload, ttl_secs)
                    .await
                {
                    warn!(error = ?err, "redis cache write failed");
                }
            });
        }
    }

    cached_to_response(&cached, request_headers, false)
}

fn cached_to_response(
    cached: &CachedResponse,
    request_headers: &HeaderMap,
    stale: bool,
) -> Response {
    if_none_match_matches(request_headers, &cached.etag)
        .then(|| {
            let headers = cached.headers(stale);
            (StatusCode::NOT_MODIFIED, headers).into_response()
        })
        .unwrap_or_else(|| {
            let mut headers = cached.headers(stale);
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            (cached.status, headers, Json(cached.body.clone())).into_response()
        })
}

fn classify_rate_limit_route(path: &str) -> &'static str {
    if path.contains("/holders") {
        "holders"
    } else if path.contains("/holder/") && path.contains("/token/") {
        "eligibility"
    } else {
        "default"
    }
}

async fn use_fallback_for_category(state: &Arc<AppState>, category: &str) -> bool {
    let Some(fallback_db) = state.fallback_db.as_ref() else {
        return false;
    };

    let exists_primary = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1
            FROM token_stats
            WHERE category = decode($1, 'hex')
              AND (total_ft_supply > 0 OR holder_count > 0 OR utxo_count > 0)
        )",
    )
    .bind(category)
    .fetch_one(state.db.pool())
    .await
    .unwrap_or(false);
    if exists_primary {
        return false;
    }

    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(
            SELECT 1
            FROM token_stats
            WHERE category = decode($1, 'hex')
              AND (total_ft_supply > 0 OR holder_count > 0 OR utxo_count > 0)
        )",
    )
    .bind(category)
    .fetch_one(fallback_db.pool())
    .await
    .unwrap_or(false)
}

fn pick_db<'a>(state: &'a Arc<AppState>, use_fallback: bool) -> &'a Database {
    if use_fallback {
        state.fallback_db.as_ref().unwrap_or(&state.db)
    } else {
        &state.db
    }
}

fn pick_mempool_snapshot(
    state: &Arc<AppState>,
    use_fallback: bool,
) -> Arc<tokio::sync::RwLock<crate::model::MempoolSnapshot>> {
    if use_fallback {
        state
            .fallback_mempool_snapshot
            .clone()
            .unwrap_or_else(|| state.mempool_snapshot.clone())
    } else {
        state.mempool_snapshot.clone()
    }
}

fn with_common_headers(
    status: StatusCode,
    body: serde_json::Value,
    updated_height: i32,
    max_age_secs: i32,
) -> Response {
    let etag = format!("\"h{updated_height}\"");
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_str(&format!(
            "public, max-age={max_age_secs}, stale-while-revalidate=30"
        ))
        .unwrap_or(HeaderValue::from_static("public, max-age=5")),
    );
    headers.insert(
        header::ETAG,
        HeaderValue::from_str(&etag).unwrap_or(HeaderValue::from_static("\"h0\"")),
    );
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    (status, headers, Json(body)).into_response()
}

fn parse_bigint_str(value: &str) -> num_bigint::BigInt {
    value
        .parse::<num_bigint::BigInt>()
        .unwrap_or_else(|_| num_bigint::BigInt::from(0u8))
}

fn build_unified_summary_body(
    category: &str,
    total_supply: &str,
    holder_count: i32,
    utxo_count: i32,
    updated_height: i32,
    updated_at: chrono::DateTime<chrono::Utc>,
    chain: &str,
    mempool_view: Option<&MempoolCategoryView>,
    bcmr: &TokenBcmrMetadata,
) -> serde_json::Value {
    let (u_credits, u_debits, u_net, u_utxo, u_txs, u_nfts) = mempool_view
        .map(|v| {
            (
                v.ft_credits.clone(),
                v.ft_debits.clone(),
                v.net_ft_delta.clone(),
                v.utxo_delta as i32,
                v.tx_count,
                v.nft_count,
            )
        })
        .unwrap_or_else(|| ("0".to_string(), "0".to_string(), "0".to_string(), 0, 0, 0));

    let effective_supply = (parse_bigint_str(total_supply) + parse_bigint_str(&u_net)).to_string();
    let effective_utxos = (utxo_count + u_utxo).max(0);

    json!({
        "category": category,
        "chain": chain,
        "name": bcmr.name,
        "symbol": bcmr.symbol,
        "bcmr": bcmr_metadata_json(bcmr),
        "total_supply": effective_supply,
        "holder_count": holder_count,
        "utxo_count": effective_utxos,
        "updated_height": updated_height,
        "updated_at": updated_at,
        "confirmed": {
            "total_supply": total_supply,
            "holder_count": holder_count,
            "utxo_count": utxo_count,
        },
        "unconfirmed": {
            "ft_credits": u_credits,
            "ft_debits": u_debits,
            "net_ft_delta": u_net,
            "utxo_delta": u_utxo,
            "tx_count": u_txs,
            "nft_count": u_nfts,
        },
        "effective": {
            "total_supply": effective_supply,
            "holder_count": holder_count,
            "utxo_count": effective_utxos,
        }
    })
}

fn bcmr_metadata_json(bcmr: &TokenBcmrMetadata) -> serde_json::Value {
    let registry = if bcmr.source_url.is_none()
        && bcmr.content_hash_hex.is_none()
        && bcmr.claimed_hash_hex.is_none()
        && bcmr.request_status.is_none()
        && bcmr.validity_checks.is_none()
    {
        serde_json::Value::Null
    } else {
        json!({
            "source_url": bcmr.source_url,
            "content_hash_hex": bcmr.content_hash_hex,
            "claimed_hash_hex": bcmr.claimed_hash_hex,
            "request_status": bcmr.request_status,
            "validity_checks": bcmr.validity_checks,
        })
    };

    json!({
        "symbol": bcmr.symbol,
        "name": bcmr.name,
        "description": bcmr.description,
        "decimals": bcmr.decimals,
        "uris": {
            "icon": bcmr.icon_uri,
            "token": bcmr.token_uri,
        },
        "latest_revision": bcmr.latest_revision,
        "identity_snapshot": bcmr.identity_snapshot,
        "nft_types": bcmr.nft_types,
        "registry": registry,
    })
}

fn active_chain_name<'a>(state: &'a Arc<AppState>, use_fallback: bool) -> &'a str {
    if use_fallback {
        fallback_chain_name(state)
    } else {
        state.config.expected_chain.as_str()
    }
}

fn fallback_chain_name(state: &Arc<AppState>) -> &'static str {
    match state.config.expected_chain.as_str() {
        "main" => "chip",
        _ => "main",
    }
}

fn extract_client_ip_addr(
    request: &Request<axum::body::Body>,
    trust_x_forwarded_for: bool,
    trusted_proxy_cidrs: &[IpCidr],
) -> Option<IpAddr> {
    if trust_x_forwarded_for {
        let remote_ip = extract_remote_ip(request);
        let trusted_proxy = remote_ip
            .map(|ip| trusted_proxy_cidrs.iter().any(|cidr| cidr.contains(ip)))
            .unwrap_or(false);
        if trusted_proxy {
            if let Some(xff) = request
                .headers()
                .get("x-forwarded-for")
                .and_then(|v| v.to_str().ok())
            {
                if let Some(first) = xff.split(',').next() {
                    let ip = first.trim();
                    if !ip.is_empty() {
                        if let Ok(parsed) = ip.parse::<IpAddr>() {
                            return Some(parsed);
                        }
                    }
                }
            }
        }
    }

    extract_remote_ip(request)
}

fn extract_remote_ip(request: &Request<axum::body::Body>) -> Option<IpAddr> {
    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip())
}

fn parse_bearer_token(header_value: &str) -> Option<&str> {
    let (scheme, token) = header_value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    if token.is_empty() {
        None
    } else {
        Some(token)
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&left, &right) in a.iter().zip(b.iter()) {
        diff |= left ^ right;
    }
    diff == 0
}

fn if_none_match_matches(request_headers: &HeaderMap, etag: &str) -> bool {
    request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == etag)
        .unwrap_or(false)
}

fn is_valid_hex_bytes(value: &str, min_bytes: usize, max_bytes: usize) -> bool {
    if value.is_empty() || value.len() % 2 != 0 {
        return false;
    }
    if !value.bytes().all(|b| b.is_ascii_hexdigit()) {
        return false;
    }
    let byte_len = value.len() / 2;
    byte_len >= min_bytes && byte_len <= max_bytes
}

fn is_valid_holder_address(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && !value
            .bytes()
            .any(|b| b.is_ascii_control() || b.is_ascii_whitespace())
}

fn error_json(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    let body = json!({
        "error": {
            "code": code,
            "message": message,
        }
    });
    (status, Json(body)).into_response()
}
