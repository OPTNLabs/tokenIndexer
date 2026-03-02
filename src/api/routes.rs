use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::net::SocketAddr;

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{Request, header, HeaderMap, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use base64::Engine;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tower_http::cors::{Any, CorsLayer};
use tracing::warn;

use crate::api::cache::{CachedResponse, RedisCacheRecord};
use crate::api::AppState;
use crate::db::queries;
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

#[derive(Debug, Serialize, Deserialize)]
struct HolderCursor {
    balance: String,
    locking_bytecode: String,
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

pub async fn rate_limit_middleware(
    State(state): State<Arc<AppState>>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let path = request.uri().path();
    if path == "/health" || path == "/metrics" {
        return next.run(request).await;
    }

    let route_key = classify_rate_limit_route(path);
    let rps = match route_key {
        "holders" => state.config.rate_limit_holders_rps,
        "eligibility" => state.config.rate_limit_eligibility_rps,
        _ => state.config.rate_limit_default_rps,
    };
    let client_ip = extract_client_ip(&request);

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

    let chain_state_present = sqlx::query_scalar::<_, i64>("SELECT height::bigint FROM chain_state WHERE id = TRUE")
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

pub async fn token_summary(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(category): Path<String>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let (mempool_version, mempool_view) = {
        let snapshot = state.mempool_snapshot.read().await;
        (
            snapshot.updated_at.timestamp(),
            snapshot.categories.get(&category).cloned(),
        )
    };
    let cache_key = format!("v1:summary:{category}:m{mempool_version}");

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let row = sqlx::query_as::<_, (String, String, i32, i32, i32, chrono::DateTime<chrono::Utc>)>(
                queries::TOKEN_SUMMARY,
            )
            .bind(category.clone())
            .fetch_optional(state.db.pool())
            .await;

            return match row {
                Ok(Some((category, total_supply, holder_count, utxo_count, updated_height, updated_at))) => {
                    let body = build_unified_summary_body(
                        &category,
                        &total_supply,
                        holder_count,
                        utxo_count,
                        updated_height,
                        updated_at,
                        mempool_view.as_ref(),
                    );
                    cache_and_respond(&state, &cache_key, body, updated_height, 10, &headers)
                }
                Ok(None) => error_json(StatusCode::NOT_FOUND, "token_not_found", "Category not indexed"),
                Err(_) => {
                    state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
                    state.metrics.stale_served.fetch_add(1, Ordering::Relaxed);
                    cached_to_response(&stale, &headers, true)
                }
            };
        }
        CacheRead::Miss => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        }
    }

    let row = sqlx::query_as::<_, (String, String, i32, i32, i32, chrono::DateTime<chrono::Utc>)>(
        queries::TOKEN_SUMMARY,
    )
    .bind(category.clone())
    .fetch_optional(state.db.pool())
    .await;

    match row {
        Ok(Some((category, total_supply, holder_count, utxo_count, updated_height, updated_at))) => {
            let body = build_unified_summary_body(
                &category,
                &total_supply,
                holder_count,
                utxo_count,
                updated_height,
                updated_at,
                mempool_view.as_ref(),
            );
            cache_and_respond(&state, &cache_key, body, updated_height, 10, &headers)
        }
        Ok(None) => error_json(StatusCode::NOT_FOUND, "token_not_found", "Category not indexed"),
        Err(_) => {
            state.metrics.db_errors.fetch_add(1, Ordering::Relaxed);
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "db_error",
                "Failed reading token summary",
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
    let n = query.n.clamp(1, 500);
    let (mempool_version, holder_deltas) = {
        let snapshot = state.mempool_snapshot.read().await;
        let holders = snapshot
            .categories
            .get(&category)
            .map(|v| v.holders.clone())
            .unwrap_or_default();
        (snapshot.updated_at.timestamp(), holders)
    };
    let cache_key = format!("v1:top:{category}:{n}:m{mempool_version}");

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::TOP_HOLDERS)
                .bind(&category)
                .bind(n)
                .fetch_all(state.db.pool())
                .await;

            return match rows {
                Ok(rows) => {
                    let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
                    let holders: Vec<_> = rows
                        .into_iter()
                        .map(|(locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)| {
                            let holder_delta = holder_deltas.get(&locking_bytecode);
                            let unconfirmed_ft_delta = holder_delta
                                .map(|v| v.ft_delta.clone())
                                .unwrap_or_else(|| "0".to_string());
                            let unconfirmed_utxo_delta = holder_delta.map(|v| v.utxo_delta).unwrap_or(0);
                            let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                            let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                            let effective_address = locking_address
                                .or_else(|| holder_delta.and_then(|v| v.locking_address.clone()));
                            json!({
                                "locking_bytecode": locking_bytecode,
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
                        })
                        .collect();

                    cache_and_respond(&state, &cache_key, json!({"holders": holders}), updated_height, 10, &headers)
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
        }
    }

    let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::TOP_HOLDERS)
        .bind(&category)
        .bind(n)
        .fetch_all(state.db.pool())
        .await;

    match rows {
        Ok(rows) => {
            let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
            let holders: Vec<_> = rows
                .into_iter()
                .map(|(locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)| {
                    let holder_delta = holder_deltas.get(&locking_bytecode);
                    let unconfirmed_ft_delta = holder_delta
                        .map(|v| v.ft_delta.clone())
                        .unwrap_or_else(|| "0".to_string());
                    let unconfirmed_utxo_delta = holder_delta.map(|v| v.utxo_delta).unwrap_or(0);
                    let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                    let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                    let effective_address = locking_address
                        .or_else(|| holder_delta.and_then(|v| v.locking_address.clone()));
                    json!({
                        "locking_bytecode": locking_bytecode,
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
                })
                .collect();

            cache_and_respond(&state, &cache_key, json!({"holders": holders}), updated_height, 10, &headers)
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
    let limit = query.limit.clamp(1, 500);
    let cursor_key = query.cursor.clone().unwrap_or_default();
    let (mempool_version, holder_deltas) = {
        let snapshot = state.mempool_snapshot.read().await;
        let holders = snapshot
            .categories
            .get(&category)
            .map(|v| v.holders.clone())
            .unwrap_or_default();
        (snapshot.updated_at.timestamp(), holders)
    };
    let cache_key = format!("v1:holders:{category}:{limit}:{cursor_key}:m{mempool_version}");

    let decoded_cursor = query.cursor.and_then(|value| {
        base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(value)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<HolderCursor>(&bytes).ok())
    });

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
            let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::PAGED_HOLDERS)
                .bind(&category)
                .bind(&cursor_balance)
                .bind(&cursor_locking_bytecode)
                .bind(limit)
                .fetch_all(state.db.pool())
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

                    let holders: Vec<_> = rows
                        .into_iter()
                        .map(|(locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)| {
                            let holder_delta = holder_deltas.get(&locking_bytecode);
                            let unconfirmed_ft_delta = holder_delta
                                .map(|v| v.ft_delta.clone())
                                .unwrap_or_else(|| "0".to_string());
                            let unconfirmed_utxo_delta = holder_delta.map(|v| v.utxo_delta).unwrap_or(0);
                            let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                            let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                            let effective_address = locking_address
                                .or_else(|| holder_delta.and_then(|v| v.locking_address.clone()));
                            json!({
                                "locking_bytecode": locking_bytecode,
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
                        })
                        .collect();

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
        }
    }

    let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::PAGED_HOLDERS)
        .bind(&category)
        .bind(&cursor_balance)
        .bind(&cursor_locking_bytecode)
        .bind(limit)
        .fetch_all(state.db.pool())
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

            let holders: Vec<_> = rows
                .into_iter()
                .map(|(locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)| {
                    let holder_delta = holder_deltas.get(&locking_bytecode);
                    let unconfirmed_ft_delta = holder_delta
                        .map(|v| v.ft_delta.clone())
                        .unwrap_or_else(|| "0".to_string());
                    let unconfirmed_utxo_delta = holder_delta.map(|v| v.utxo_delta).unwrap_or(0);
                    let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                    let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                    let effective_address = locking_address
                        .or_else(|| holder_delta.and_then(|v| v.locking_address.clone()));
                    json!({
                        "locking_bytecode": locking_bytecode,
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
                })
                .collect();

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
    Path((category, locking_bytecode)): Path<(String, String)>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let (mempool_version, unconfirmed_ft_delta, unconfirmed_utxo_delta, unconfirmed_address) = {
        let snapshot = state.mempool_snapshot.read().await;
        let delta = snapshot
            .categories
            .get(&category)
            .and_then(|v| v.holders.get(&locking_bytecode));
        (
            snapshot.updated_at.timestamp(),
            delta.map(|v| v.ft_delta.clone()).unwrap_or_else(|| "0".to_string()),
            delta.map(|v| v.utxo_delta).unwrap_or(0),
            delta.and_then(|v| v.locking_address.clone()),
        )
    };
    let cache_key = format!(
        "v1:eligibility:{category}:{locking_bytecode}:m{mempool_version}"
    );

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let row = sqlx::query_as::<_, (Option<String>, String, i32, i32)>(queries::ELIGIBILITY)
                .bind(&category)
                .bind(&locking_bytecode)
                .fetch_optional(state.db.pool())
                .await;

            return match row {
                Ok(Some((locking_address, ft_balance, utxo_count, updated_height))) => {
                    let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
                    let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
                    cache_and_respond(
                        &state,
                        &cache_key,
                        json!({
                            "eligible": effective_ft > num_bigint::BigInt::from(0u8),
                            "confirmed_eligible": ft_balance != "0",
                            "effective_eligible": effective_ft > num_bigint::BigInt::from(0u8),
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
        }
    }

    let row = sqlx::query_as::<_, (Option<String>, String, i32, i32)>(queries::ELIGIBILITY)
        .bind(&category)
        .bind(&locking_bytecode)
        .fetch_optional(state.db.pool())
        .await;

    match row {
        Ok(Some((locking_address, ft_balance, utxo_count, updated_height))) => {
            let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&unconfirmed_ft_delta);
            let effective_utxo = (utxo_count + unconfirmed_utxo_delta).max(0);
            cache_and_respond(
                &state,
                &cache_key,
                json!({
                    "eligible": effective_ft > num_bigint::BigInt::from(0u8),
                    "confirmed_eligible": ft_balance != "0",
                    "effective_eligible": effective_ft > num_bigint::BigInt::from(0u8),
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
    Path(locking_bytecode): Path<String>,
    Query(query): Query<HolderTokensQuery>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);
    let limit = query.limit.clamp(1, 500);
    let (mempool_version, mempool_by_category) = {
        let snapshot = state.mempool_snapshot.read().await;
        let mut map = std::collections::HashMap::new();
        for (category, view) in &snapshot.categories {
            if let Some(delta) = view.holders.get(&locking_bytecode) {
                map.insert(
                    category.clone(),
                    (
                        delta.ft_delta.clone(),
                        delta.utxo_delta,
                        delta.locking_address.clone(),
                    ),
                );
            }
        }
        (snapshot.updated_at.timestamp(), map)
    };
    let cache_key = format!("v1:holder-tokens:{locking_bytecode}:{limit}:m{mempool_version}");

    match read_cache(&state, &cache_key).await {
        CacheRead::Fresh(cached) => {
            state.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return cached_to_response(&cached, &headers, false);
        }
        CacheRead::Stale(stale) => {
            state.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
            let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::HOLDER_TOKENS)
                .bind(&locking_bytecode)
                .bind(limit)
                .fetch_all(state.db.pool())
                .await;

            return match rows {
                Ok(rows) => {
                    let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
                    let mut seen_categories = std::collections::HashSet::new();
                    let mut tokens: Vec<_> = rows
                        .into_iter()
                        .map(|(category, locking_address, ft_balance, utxo_count, updated_height)| {
                            seen_categories.insert(category.clone());
                            let (u_ft, u_utxo, u_addr) = mempool_by_category
                                .get(&category)
                                .cloned()
                                .unwrap_or_else(|| ("0".to_string(), 0, None));
                            let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&u_ft);
                            let effective_utxo = (utxo_count + u_utxo).max(0);
                            json!({
                                "category": category,
                                "locking_address": locking_address.or(u_addr),
                                "ft_balance": effective_ft.to_string(),
                                "utxo_count": effective_utxo,
                                "confirmed_ft_balance": ft_balance,
                                "unconfirmed_ft_delta": u_ft,
                                "effective_ft_balance": effective_ft.to_string(),
                                "confirmed_utxo_count": utxo_count,
                                "unconfirmed_utxo_delta": u_utxo,
                                "effective_utxo_count": effective_utxo,
                                "updated_height": updated_height,
                            })
                        })
                        .collect();
                    for (category, (u_ft, u_utxo, u_addr)) in &mempool_by_category {
                        if seen_categories.contains(category) {
                            continue;
                        }
                        let effective_ft = parse_bigint_str(u_ft);
                        let effective_utxo = (*u_utxo).max(0);
                        tokens.push(json!({
                            "category": category,
                            "locking_address": u_addr,
                            "ft_balance": effective_ft.to_string(),
                            "utxo_count": effective_utxo,
                            "confirmed_ft_balance": "0",
                            "unconfirmed_ft_delta": u_ft,
                            "effective_ft_balance": effective_ft.to_string(),
                            "confirmed_utxo_count": 0,
                            "unconfirmed_utxo_delta": u_utxo,
                            "effective_utxo_count": effective_utxo,
                            "updated_height": 0,
                        }));
                    }

                    cache_and_respond(&state, &cache_key, json!({"tokens": tokens}), updated_height, 10, &headers)
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
        }
    }

    let rows = sqlx::query_as::<_, (String, Option<String>, String, i32, i32)>(queries::HOLDER_TOKENS)
        .bind(&locking_bytecode)
        .bind(limit)
        .fetch_all(state.db.pool())
        .await;

    match rows {
        Ok(rows) => {
            let updated_height = rows.first().map(|r| r.4).unwrap_or(0);
            let mut seen_categories = std::collections::HashSet::new();
            let mut tokens: Vec<_> = rows
                .into_iter()
                .map(|(category, locking_address, ft_balance, utxo_count, updated_height)| {
                    seen_categories.insert(category.clone());
                    let (u_ft, u_utxo, u_addr) = mempool_by_category
                        .get(&category)
                        .cloned()
                        .unwrap_or_else(|| ("0".to_string(), 0, None));
                    let effective_ft = parse_bigint_str(&ft_balance) + parse_bigint_str(&u_ft);
                    let effective_utxo = (utxo_count + u_utxo).max(0);
                    json!({
                        "category": category,
                        "locking_address": locking_address.or(u_addr),
                        "ft_balance": effective_ft.to_string(),
                        "utxo_count": effective_utxo,
                        "confirmed_ft_balance": ft_balance,
                        "unconfirmed_ft_delta": u_ft,
                        "effective_ft_balance": effective_ft.to_string(),
                        "confirmed_utxo_count": utxo_count,
                        "unconfirmed_utxo_delta": u_utxo,
                        "effective_utxo_count": effective_utxo,
                        "updated_height": updated_height,
                    })
                })
                .collect();
            for (category, (u_ft, u_utxo, u_addr)) in &mempool_by_category {
                if seen_categories.contains(category) {
                    continue;
                }
                let effective_ft = parse_bigint_str(u_ft);
                let effective_utxo = (*u_utxo).max(0);
                tokens.push(json!({
                    "category": category,
                    "locking_address": u_addr,
                    "ft_balance": effective_ft.to_string(),
                    "utxo_count": effective_utxo,
                    "confirmed_ft_balance": "0",
                    "unconfirmed_ft_delta": u_ft,
                    "effective_ft_balance": effective_ft.to_string(),
                    "confirmed_utxo_count": 0,
                    "unconfirmed_utxo_delta": u_utxo,
                    "effective_utxo_count": effective_utxo,
                    "updated_height": 0,
                }));
            }

            cache_and_respond(&state, &cache_key, json!({"tokens": tokens}), updated_height, 10, &headers)
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
    let n = query.n.clamp(1, 200) as usize;
    let snapshot = state.mempool_snapshot.read().await;
    let mempool_updated_at = snapshot.updated_at;
    let category_view = snapshot.categories.get(&category).cloned();
    drop(snapshot);

    match category_view {
        Some(view) => {
            let mut holders: Vec<MempoolHolderDelta> = view.holders.into_values().collect();
            holders.sort_by(|a, b| parse_bigint_str(&b.ft_delta).cmp(&parse_bigint_str(&a.ft_delta)));
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
    Path(category): Path<String>,
) -> Response {
    state.metrics.requests_total.fetch_add(1, Ordering::Relaxed);

    let summary_row = sqlx::query_as::<_, (String, String, i32, i32, i32, chrono::DateTime<chrono::Utc>)>(
        queries::TOKEN_SUMMARY,
    )
    .bind(category.clone())
    .fetch_optional(state.db.pool())
    .await;

    let Ok(Some((category_hex, total_supply, holder_count, utxo_count, updated_height, updated_at))) = summary_row else {
        return error_json(StatusCode::NOT_FOUND, "token_not_found", "Category not indexed");
    };

    let top_10_sum: String = sqlx::query_scalar(queries::TOP_N_BALANCE_SUM)
        .bind(category.clone())
        .bind(10_i64)
        .fetch_one(state.db.pool())
        .await
        .unwrap_or_else(|_| "0".to_string());

    let recent_24h_outputs: i64 = sqlx::query_scalar(queries::RECENT_ACTIVITY_BLOCKS)
        .bind(category.clone())
        .bind(144_i32)
        .fetch_one(state.db.pool())
        .await
        .unwrap_or(0);

    let total = parse_bigint_str(&total_supply);
    let top10 = parse_bigint_str(&top_10_sum);
    let top_10_share_bps = if total == num_bigint::BigInt::from(0u8) {
        0_i64
    } else {
        ((top10 * num_bigint::BigInt::from(10_000_i64)) / total)
            .to_string()
            .parse::<i64>()
            .unwrap_or(0)
    };

    let mempool = state.mempool_snapshot.read().await;
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
        .unwrap_or_else(|| json!({
            "tx_count": 0,
            "net_ft_delta": "0",
            "utxo_delta": 0,
            "nft_count": 0,
        }));

    with_common_headers(
        StatusCode::OK,
        json!({
            "category": category_hex,
            "summary": {
                "total_supply": total_supply,
                "holder_count": holder_count,
                "utxo_count": utxo_count,
                "updated_height": updated_height,
                "updated_at": updated_at,
            },
            "distribution": {
                "top_10_balance": top_10_sum,
                "top_10_share_bps": top_10_share_bps,
            },
            "activity": {
                "recent_outputs_24h_blocks": recent_24h_outputs,
            },
            "mempool_overlay": mempool_overlay,
        }),
        updated_height,
        10,
    )
}

pub fn cors_layer(config: &crate::config::Config) -> anyhow::Result<CorsLayer> {
    if config.cors_allow_origins.iter().any(|v| v == "*") {
        return Ok(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any));
    }

    let mut origins = Vec::with_capacity(config.cors_allow_origins.len());
    for origin in &config.cors_allow_origins {
        origins.push(origin.parse::<HeaderValue>()?);
    }

    Ok(CorsLayer::new().allow_origin(origins).allow_methods(Any).allow_headers(Any))
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
                if let Err(err) = redis_conn.set_ex::<_, _, ()>(redis_key, payload, ttl_secs).await {
                    warn!(error = ?err, "redis cache write failed");
                }
            });
        }
    }

    cached_to_response(&cached, request_headers, false)
}

fn cached_to_response(cached: &CachedResponse, request_headers: &HeaderMap, stale: bool) -> Response {
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
    } else if path.contains("/holder/") && path.starts_with("/v1/token/") {
        "eligibility"
    } else {
        "default"
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
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
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
    mempool_view: Option<&MempoolCategoryView>,
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

fn extract_client_ip(request: &Request<axum::body::Body>) -> String {
    if let Some(xff) = request.headers().get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
        if let Some(first) = xff.split(',').next() {
            let ip = first.trim();
            if !ip.is_empty() {
                return ip.to_string();
            }
        }
    }

    request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".to_string())
}

fn if_none_match_matches(request_headers: &HeaderMap, etag: &str) -> bool {
    request_headers
        .get(header::IF_NONE_MATCH)
        .and_then(|v| v.to_str().ok())
        .map(|v| v == etag)
        .unwrap_or(false)
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
