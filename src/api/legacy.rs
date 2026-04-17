use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::api::AppState;
use crate::db::Database;
use crate::ingest::RpcClient;

#[derive(Debug, Deserialize)]
pub struct LegacyTokenTypePath {
    category: String,
    type_key: String,
}

#[derive(Debug, Deserialize)]
pub struct LegacyCommitmentPath {
    category: String,
    commitment: String,
}

#[derive(Debug, Deserialize)]
pub struct LegacyTxoPath {
    txo: String,
}

#[derive(Debug, Deserialize, Default)]
pub struct LegacyRegistryQuery {
    include_identities: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct LegacyIdentitySnapshotQuery {
    include_token_nfts: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct LegacyNftTypesQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    paginated: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
pub struct LegacyCashTokensQuery {
    page: Option<usize>,
    include_metadata: Option<String>,
}

#[derive(Debug, sqlx::FromRow, Clone)]
struct LegacyRegistryRow {
    registry_id: i64,
    category: Option<String>,
    txid_hex: String,
    vout: i32,
    authbase_hex: Option<String>,
    source_url: String,
    content_hash_hex: Option<String>,
    claimed_hash_hex: Option<String>,
    request_status: Option<i32>,
    validity_checks: Option<Value>,
    contents: Option<Value>,
    symbol: Option<String>,
    name: Option<String>,
    description: Option<String>,
    decimals: Option<i32>,
    icon_uri: Option<String>,
    token_uri: Option<String>,
    latest_revision: Option<DateTime<Utc>>,
    identity_snapshot: Option<Value>,
    nft_types: Option<Value>,
    updated_height: Option<i32>,
    updated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, sqlx::FromRow)]
struct LegacyBlockHeightRow {
    height: i32,
}

#[derive(Debug, sqlx::FromRow, Clone)]
struct LegacyCashTokenRow {
    category: String,
    commitment: Option<String>,
    capability: Option<i16>,
    amount: String,
}

#[derive(Debug, Serialize)]
struct LegacyCashTokenResponse {
    category: String,
    commitment: Option<String>,
    capability: Option<String>,
    amount: String,
    metadata: Value,
}

pub async fn latest_block(State(state): State<Arc<AppState>>) -> Response {
    match sqlx::query_as::<_, LegacyBlockHeightRow>(
        "SELECT height FROM chain_state WHERE id = TRUE",
    )
    .fetch_optional(state.db.pool())
    .await
    {
        Ok(Some(row)) => Json(json!({ "height": row.height })).into_response(),
        Ok(None) => Json(json!({ "height": 0 })).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn token(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    legacy_token_response(state, category, None).await
}

pub async fn token_type(
    State(state): State<Arc<AppState>>,
    Path(path): Path<LegacyTokenTypePath>,
) -> Response {
    legacy_token_response(state, path.category, Some(path.type_key)).await
}

pub async fn token_icon_symbol(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let icon_uri = identity_snapshot_for_row(&row)
                .and_then(|snapshot| snapshot.get("uris").and_then(|v| v.get("icon")).cloned());
            let symbol = row.symbol.clone().map(Value::String).unwrap_or(Value::Null);
            Json(json!({
                "category": category,
                "icon_uri": icon_uri.unwrap_or(Value::Null),
                "symbol": symbol
            }))
            .into_response()
        }
        Ok(None) => match token_category_exists(&state, &category).await {
            Ok(true) => Json(json!({ "error": "bcmr is invalid" })).into_response(),
            Ok(false) => Json(json!({ "error": "category not found" })).into_response(),
            Err(err) => internal_error(err),
        },
        Err(err) => internal_error(err),
    }
}

pub async fn registries_latest(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => Json(row.contents.unwrap_or(Value::Null)).into_response(),
        Ok(None) => not_found(),
        Err(err) => internal_error(err),
    }
}

pub async fn registries_txo(
    State(state): State<Arc<AppState>>,
    Path(path): Path<LegacyTxoPath>,
) -> Response {
    let Some((txid, vout)) = parse_txo_path(&path.txo) else {
        return StatusCode::UNPROCESSABLE_ENTITY.into_response();
    };
    match fetch_registry_by_txo(&state, &txid, vout).await {
        Ok(Some(row)) => Json(row.contents.unwrap_or(Value::Null)).into_response(),
        Ok(None) => not_found(),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_contents(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => Json(row.contents.unwrap_or(Value::Null)).into_response(),
        Ok(None) => json_error(StatusCode::NOT_FOUND, json!({ "error": "Registry not found" })),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_token(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let token = identity_snapshot_for_row(&row)
                .and_then(|snapshot| snapshot.get("token").cloned());
            match token {
                Some(token) => Json(token).into_response(),
                None => json_error(StatusCode::NOT_FOUND, json!({ "error": "Token not found" })),
            }
        }
        Ok(None) => json_error(StatusCode::NOT_FOUND, json!({ "error": "Registry not found" })),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_token_nft(
    State(state): State<Arc<AppState>>,
    Path(path): Path<LegacyCommitmentPath>,
) -> Response {
    match fetch_registry_by_category(&state, &path.category).await {
        Ok(Some(row)) => {
            let nft = nft_type_from_row(&row, &path.commitment);
            match nft {
                Some(nft) => Json(nft).into_response(),
                None => json_error(
                    StatusCode::NOT_FOUND,
                    json!({ "error": format!("Nft with commitment {} not found", path.commitment) }),
                ),
            }
        }
        Ok(None) => json_error(StatusCode::NOT_FOUND, json!({ "error": "Category not found" })),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_uris(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let uris = identity_snapshot_for_row(&row)
                .and_then(|snapshot| snapshot.get("uris").cloned());
            match uris {
                Some(uris) => Json(uris).into_response(),
                None => json_error(StatusCode::NOT_FOUND, json!({ "error": "Uris not found" })),
            }
        }
        Ok(None) => json_error(StatusCode::NOT_FOUND, json!({ "error": "Registry not found" })),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_icon_uri(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let icon = identity_snapshot_for_row(&row)
                .and_then(|snapshot| snapshot.get("uris").and_then(|v| v.get("icon")).cloned());
            match icon {
                Some(icon) => Json(icon).into_response(),
                None => json_error(StatusCode::NOT_FOUND, json!({ "error": "Icon uri not found" })),
            }
        }
        Ok(None) => json_error(StatusCode::NOT_FOUND, json!({ "error": "Registry not found" })),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_published_url(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => Json(json!({ "url": row.source_url })).into_response(),
        Ok(None) => json_error(StatusCode::NOT_FOUND, json!({ "error": "Category not found" })),
        Err(err) => internal_error(err),
    }
}

pub async fn bcmr_reindex(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    let primary = requeue_bcmr_candidates(&state.db, &category).await;
    let fallback = match &state.fallback_db {
        Some(db) => requeue_bcmr_candidates(db, &category).await,
        None => Ok(0),
    };

    match (primary, fallback) {
        (Ok(_), Ok(_)) => Json(json!({ "success": "Reindexing task queued." })).into_response(),
        (Err(err), _) | (_, Err(err)) => internal_error(err),
    }
}

pub async fn authchain_head(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    let registry = match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => row,
        Ok(None) => return Json(json!({ "error": "category not found" })).into_response(),
        Err(err) => return internal_error(err),
    };

    let rpc = match rpc_for_category(&state, &category).await {
        Ok(rpc) => rpc,
        Err(err) => return internal_error(err),
    };

    let tx: Value = match rpc
        .call("getrawtransaction", json!([registry.txid_hex, true]))
        .await
    {
        Ok(tx) => tx,
        Err(err) => return internal_error(err),
    };

    let owner = tx
        .get("vout")
        .and_then(|v| v.as_array())
        .and_then(|arr| arr.first())
        .and_then(|vout| vout.get("scriptPubKey"))
        .and_then(|spk| {
            spk.get("address")
                .or_else(|| spk.get("addresses").and_then(|v| v.as_array()).and_then(|a| a.first()))
        })
        .cloned()
        .unwrap_or(Value::Null);

    Json(json!({
        "authchain_head": {
            "txid": registry.txid_hex,
            "owner": owner
        }
    }))
    .into_response()
}

pub async fn registry(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
    Query(query): Query<LegacyRegistryQuery>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let include_identities = is_true(query.include_identities.as_deref());
            Json(build_registry_summary(&row, include_identities)).into_response()
        }
        Ok(None) => Json(Value::Null).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn registry_identity_snapshot(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
    Query(query): Query<LegacyIdentitySnapshotQuery>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => match identity_snapshot_for_row(&row) {
            Some(snapshot) => {
                let body = if is_true(query.include_token_nfts.as_deref()) {
                    let mut snapshot = snapshot;
                    if let Some(obj) = snapshot.as_object_mut() {
                        obj.insert("_meta".to_string(), registry_meta(&row));
                    }
                    snapshot
                } else {
                    build_identity_snapshot_basic(&row, &snapshot)
                };
                Json(body).into_response()
            }
            None => Json(Value::Null).into_response(),
        },
        Ok(None) => Json(Value::Null).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn registry_token_category(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => Json(build_token_category_basic(&row)).into_response(),
        Ok(None) => Json(Value::Object(Map::new())).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn registry_nfts(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let nfts = identity_snapshot_for_row(&row)
                .and_then(|snapshot| snapshot.get("token").and_then(|v| v.get("nfts")).cloned());
            match nfts {
                Some(nfts) => Json(json!({ "nfts": nfts, "_meta": registry_meta(&row) })).into_response(),
                None => Json(Value::Null).into_response(),
            }
        }
        Ok(None) => Json(Value::Null).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn registry_parse_bytecode(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let bytecode = identity_snapshot_for_row(&row)
                .and_then(|snapshot| {
                    snapshot
                        .get("token")
                        .and_then(|v| v.get("nfts"))
                        .and_then(|v| v.get("parse"))
                        .and_then(|v| v.get("bytecode"))
                        .cloned()
                });
            match bytecode {
                Some(bytecode) => Json(json!({ "bytecode": bytecode, "_meta": registry_meta(&row) })).into_response(),
                None => Json(Value::Null).into_response(),
            }
        }
        Ok(None) => Json(Value::Null).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn registry_nft_types(
    State(state): State<Arc<AppState>>,
    Path(category): Path<String>,
    Query(query): Query<LegacyNftTypesQuery>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    match fetch_registry_by_category(&state, &category).await {
        Ok(Some(row)) => {
            let types = nft_types_map(&row).unwrap_or_default();
            let mut keys: Vec<String> = types.keys().cloned().collect();
            keys.sort_by(|a, b| b.cmp(a));
            let limit = query.limit.unwrap_or(10).max(1);
            let offset = query.offset.unwrap_or(0);
            let paginated = is_true(query.paginated.as_deref());
            if paginated {
                let end = (offset + limit).min(keys.len());
                let slice = if offset < keys.len() { &keys[offset..end] } else { &[] };
                let results: Vec<Value> = slice
                    .iter()
                    .map(|commitment| {
                        json!({
                            commitment: types.get(commitment).cloned().unwrap_or(Value::Null),
                            "_meta": nft_meta(&row, commitment),
                        })
                    })
                    .collect();
                let base = strip_query_from_uri(&uri);
                let next = if end < keys.len() {
                    Some(format!("{base}?paginated=true&limit={limit}&offset={end}"))
                } else {
                    None
                };
                let prev_offset = offset.saturating_sub(limit);
                let previous = if offset == 0 {
                    None
                } else if prev_offset == 0 {
                    Some(format!("{base}?paginated=true&limit={limit}"))
                } else {
                    Some(format!(
                        "{base}?paginated=true&limit={limit}&offset={prev_offset}"
                    ))
                };
                Json(json!({
                    "count": keys.len(),
                    "limit": limit,
                    "offset": offset,
                    "previous": previous,
                    "next": next,
                    "results": results
                }))
                .into_response()
            } else {
                let results: Vec<Value> = keys
                    .iter()
                    .take(limit)
                    .skip(offset)
                    .map(|commitment| {
                        json!({
                            commitment: types.get(commitment).cloned().unwrap_or(Value::Null),
                            "_meta": nft_meta(&row, commitment),
                        })
                    })
                    .collect();
                Json(Value::Array(results)).into_response()
            }
        }
        Ok(None) => Json(Value::Null).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn registry_nft_type(
    State(state): State<Arc<AppState>>,
    Path(path): Path<LegacyCommitmentPath>,
) -> Response {
    match fetch_registry_by_category(&state, &path.category).await {
        Ok(Some(row)) => {
            let nft = nft_type_from_row(&row, &path.commitment);
            match nft {
                Some(nft) => Json(json!({
                    path.commitment.clone(): nft,
                    "_meta": nft_meta(&row, &path.commitment)
                }))
                .into_response(),
                None => Json(Value::Null).into_response(),
            }
        }
        Ok(None) => Json(Value::Null).into_response(),
        Err(err) => internal_error(err),
    }
}

pub async fn cashtokens(
    State(state): State<Arc<AppState>>,
    Path(path): Path<HashMap<String, String>>,
    Query(query): Query<LegacyCashTokensQuery>,
    OriginalUri(uri): OriginalUri,
) -> Response {
    let category = path.get("category").cloned();
    let token_type = path.get("token_type").cloned();
    let commitment = path.get("commitment").cloned();
    let page = query.page.unwrap_or(1).max(1);
    let page_size = 10usize;
    let capability_filters = query_values(&uri, "capability");

    let primary = fetch_cashtokens_from_db(
        &state.db,
        category.as_deref(),
        token_type.as_deref(),
        commitment.as_deref(),
        &capability_filters,
    )
    .await;
    let rows = match primary {
        Ok(rows) => rows,
        Err(err) => return internal_error(err),
    };

    let total = rows.len();
    let start = (page - 1) * page_size;
    let end = (start + page_size).min(total);
    let page_rows = if start < total { &rows[start..end] } else { &[] };
    let include_metadata = is_true(query.include_metadata.as_deref());

    let mut results = Vec::with_capacity(page_rows.len());
    for row in page_rows {
        let metadata = if include_metadata {
            build_cash_token_metadata(&state, row).await
        } else {
            Value::String("not_set".to_string())
        };
        results.push(json!(LegacyCashTokenResponse {
            category: row.category.clone(),
            commitment: row.commitment.clone(),
            capability: row.capability.and_then(capability_name),
            amount: row.amount.clone(),
            metadata,
        }));
    }

    let base = strip_query_from_uri(&uri);
    let next = if end < total {
        Some(page_link(&base, page + 1, query.include_metadata.as_deref(), &capability_filters))
    } else {
        None
    };
    let previous = if page > 1 {
        Some(page_link(&base, page - 1, query.include_metadata.as_deref(), &capability_filters))
    } else {
        None
    };

    Json(json!({
        "count": total,
        "next": next,
        "previous": previous,
        "results": results,
        "capability": capability_filters
    }))
    .into_response()
}

async fn legacy_token_response(
    state: Arc<AppState>,
    category: String,
    type_key: Option<String>,
) -> Response {
    let exists = match token_category_exists(&state, &category).await {
        Ok(exists) => exists,
        Err(err) => return internal_error(err),
    };

    if !exists {
        return json_error(StatusCode::NOT_FOUND, json!({ "error": "category not found" }));
    }

    let registry = match fetch_registry_by_category(&state, &category).await {
        Ok(registry) => registry,
        Err(err) => return internal_error(err),
    };

    let Some(row) = registry else {
        return Json(json!({
            "category": category,
            "error": "no valid metadata found"
        }))
        .into_response();
    };

    let Some(snapshot) = identity_snapshot_for_row(&row) else {
        return Json(json!({
            "category": category,
            "error": "no valid metadata found"
        }))
        .into_response();
    };

    let is_nft = snapshot
        .get("token")
        .and_then(|token| token.get("nfts"))
        .is_some();

    Json(transform_to_legacy_token_payload(snapshot, type_key.as_deref(), is_nft)).into_response()
}

async fn token_category_exists(state: &Arc<AppState>, category: &str) -> anyhow::Result<bool> {
    let primary = token_category_exists_in_db(&state.db, category).await?;
    if primary {
        return Ok(true);
    }
    if let Some(fallback) = &state.fallback_db {
        return token_category_exists_in_db(fallback, category).await;
    }
    Ok(false)
}

async fn token_category_exists_in_db(db: &Database, category: &str) -> anyhow::Result<bool> {
    if !is_hex_64(category) {
        return Ok(false);
    }
    sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
          SELECT 1
          FROM token_stats
          WHERE category = decode($1, 'hex')
            AND (total_ft_supply > 0 OR holder_count > 0 OR utxo_count > 0)
        )
        "#,
    )
    .bind(category)
    .fetch_one(db.pool())
    .await
    .map_err(Into::into)
}

async fn fetch_registry_by_category(
    state: &Arc<AppState>,
    category: &str,
) -> anyhow::Result<Option<LegacyRegistryRow>> {
    if let Some(row) = fetch_registry_by_category_from_db(&state.db, category).await? {
        return Ok(Some(row));
    }
    if let Some(fallback) = &state.fallback_db {
        return fetch_registry_by_category_from_db(fallback, category).await;
    }
    Ok(None)
}

async fn fetch_registry_by_category_from_db(
    db: &Database,
    category: &str,
) -> anyhow::Result<Option<LegacyRegistryRow>> {
    if !is_hex_64(category) {
        return Ok(None);
    }
    sqlx::query_as::<_, LegacyRegistryRow>(
        r#"
        SELECT
          r.id AS registry_id,
          encode(COALESCE(m.category, r.category, r.authbase), 'hex') AS category,
          encode(r.txid, 'hex') AS txid_hex,
          r.vout,
          encode(COALESCE(r.authbase, r.category), 'hex') AS authbase_hex,
          r.source_url,
          r.content_hash_hex,
          r.claimed_hash_hex,
          r.request_status,
          r.validity_checks,
          r.contents,
          m.symbol,
          m.name,
          m.description,
          m.decimals,
          m.icon_uri,
          m.token_uri,
          m.latest_revision,
          m.identity_snapshot,
          m.nft_types,
          m.updated_height,
          m.updated_at
        FROM bcmr_registries r
        LEFT JOIN bcmr_category_metadata m
          ON m.registry_id = r.id
        WHERE r.category = decode($1, 'hex')
           OR r.authbase = decode($1, 'hex')
           OR m.category = decode($1, 'hex')
        ORDER BY COALESCE(m.updated_at, r.fetched_at) DESC, r.id DESC
        LIMIT 1
        "#,
    )
    .bind(category)
    .fetch_optional(db.pool())
    .await
    .map_err(Into::into)
}

async fn fetch_registry_by_txo(
    state: &Arc<AppState>,
    txid: &str,
    vout: i32,
) -> anyhow::Result<Option<LegacyRegistryRow>> {
    if let Some(row) = fetch_registry_by_txo_from_db(&state.db, txid, vout).await? {
        return Ok(Some(row));
    }
    if let Some(fallback) = &state.fallback_db {
        return fetch_registry_by_txo_from_db(fallback, txid, vout).await;
    }
    Ok(None)
}

async fn fetch_registry_by_txo_from_db(
    db: &Database,
    txid: &str,
    vout: i32,
) -> anyhow::Result<Option<LegacyRegistryRow>> {
    if !is_hex_64(txid) {
        return Ok(None);
    }
    sqlx::query_as::<_, LegacyRegistryRow>(
        r#"
        SELECT
          r.id AS registry_id,
          encode(COALESCE(m.category, r.category, r.authbase), 'hex') AS category,
          encode(r.txid, 'hex') AS txid_hex,
          r.vout,
          encode(COALESCE(r.authbase, r.category), 'hex') AS authbase_hex,
          r.source_url,
          r.content_hash_hex,
          r.claimed_hash_hex,
          r.request_status,
          r.validity_checks,
          r.contents,
          m.symbol,
          m.name,
          m.description,
          m.decimals,
          m.icon_uri,
          m.token_uri,
          m.latest_revision,
          m.identity_snapshot,
          m.nft_types,
          m.updated_height,
          m.updated_at
        FROM bcmr_registries r
        LEFT JOIN bcmr_category_metadata m
          ON m.registry_id = r.id
        WHERE r.txid = decode($1, 'hex')
          AND r.vout = $2
        ORDER BY COALESCE(m.updated_at, r.fetched_at) DESC, r.id DESC
        LIMIT 1
        "#,
    )
    .bind(txid)
    .bind(vout)
    .fetch_optional(db.pool())
    .await
    .map_err(Into::into)
}

async fn fetch_cashtokens_from_db(
    db: &Database,
    category: Option<&str>,
    token_type: Option<&str>,
    commitment: Option<&str>,
    capability: &[String],
) -> anyhow::Result<Vec<LegacyCashTokenRow>> {
    if category.is_some_and(|value| !is_hex_64(value)) {
        return Ok(Vec::new());
    }
    let rows = sqlx::query_as::<_, LegacyCashTokenRow>(
        r#"
        SELECT DISTINCT ON (
          category,
          COALESCE(encode(nft_commitment, 'hex'), ''),
          COALESCE(nft_capability, -1),
          COALESCE(ft_amount, 0)
        )
          encode(category, 'hex') AS category,
          encode(nft_commitment, 'hex') AS commitment,
          nft_capability AS capability,
          COALESCE(ft_amount, 0)::text AS amount
        FROM token_outpoints
        WHERE ($1::text IS NULL OR category = decode($1, 'hex'))
          AND spent_height IS NULL
          AND ($2::text IS NULL OR encode(nft_commitment, 'hex') = $2)
          AND (
            $3::text IS NULL
            OR ($3 = 'fts' AND nft_capability IS NULL AND COALESCE(ft_amount, 0) > 0)
            OR ($3 = 'nfts' AND nft_capability IS NOT NULL AND COALESCE(ft_amount, 0) = 0)
            OR ($3 = 'hybrids' AND nft_capability IS NOT NULL AND COALESCE(ft_amount, 0) > 0)
          )
          AND (
            COALESCE(array_length($4::text[], 1), 0) = 0
            OR ('none' = ANY($4::text[]) AND nft_capability = 0)
            OR ('mutable' = ANY($4::text[]) AND nft_capability = 1)
            OR ('minting' = ANY($4::text[]) AND nft_capability = 2)
          )
        ORDER BY
          category,
          COALESCE(encode(nft_commitment, 'hex'), '') ASC,
          COALESCE(nft_capability, -1) ASC,
          COALESCE(ft_amount, 0) ASC,
          created_height DESC
        "#,
    )
    .bind(category)
    .bind(commitment)
    .bind(token_type)
    .bind(capability)
    .fetch_all(db.pool())
    .await?;
    Ok(rows)
}

async fn build_cash_token_metadata(state: &Arc<AppState>, row: &LegacyCashTokenRow) -> Value {
    let token = match fetch_registry_by_category(state, &row.category).await {
        Ok(Some(registry)) => build_token_category_basic(&registry),
        _ => Value::Null,
    };
    let nft = match &row.commitment {
        Some(commitment) => match fetch_registry_by_category(state, &row.category).await {
            Ok(Some(registry)) => match nft_type_from_row(&registry, commitment) {
                Some(nft) => json!({
                    commitment: nft,
                    "_meta": nft_meta(&registry, commitment)
                }),
                None => Value::Null,
            },
            _ => Value::Null,
        },
        None => Value::Null,
    };
    let mut metadata = Map::new();
    if row.capability.and_then(capability_name).is_some_and(|v| v != "none") {
        metadata.insert("nft".to_string(), nft);
    }
    metadata.insert("token".to_string(), token);
    Value::Object(metadata)
}

async fn requeue_bcmr_candidates(db: &Database, category: &str) -> anyhow::Result<u64> {
    let result = sqlx::query(
        r#"
        UPDATE bcmr_candidates
        SET status = 'pending',
            next_attempt_at = now(),
            last_error = NULL
        WHERE category = decode($1, 'hex')
           OR authbase = decode($1, 'hex')
        "#,
    )
    .bind(category)
    .execute(db.pool())
    .await?;
    Ok(result.rows_affected())
}

async fn rpc_for_category(state: &Arc<AppState>, category: &str) -> anyhow::Result<RpcClient> {
    let use_fallback = if let Some(fallback) = &state.fallback_db {
        let exists_primary = token_category_exists_in_db(&state.db, category).await?;
        if exists_primary {
            false
        } else {
            token_category_exists_in_db(fallback, category).await?
        }
    } else {
        false
    };

    let cfg = if use_fallback {
        state.fallback_config.as_ref().unwrap_or(&state.config)
    } else {
        &state.config
    };

    RpcClient::new(
        cfg.rpc_url.clone(),
        cfg.rpc_user.clone(),
        cfg.rpc_pass.clone(),
        cfg.rpc_timeout_ms,
        cfg.rpc_retries,
        cfg.rpc_retry_backoff_ms,
    )
}

fn build_registry_summary(row: &LegacyRegistryRow, include_identities: bool) -> Value {
    let contents = row.contents.clone().unwrap_or(Value::Null);
    let mut out = Map::new();
    if let Some(schema) = contents.get("$schema").cloned() {
        out.insert("$schema".to_string(), schema);
    }
    for key in ["version", "latestRevision", "registryIdentity", "tags", "license", "locales", "extensions", "chains"] {
        if let Some(value) = contents.get(key).cloned() {
            out.insert(key.to_string(), value);
        }
    }
    if let Some(default_chain) = contents.get("defaultChain").cloned() {
        out.insert("defaultChain".to_string(), default_chain);
    }
    if include_identities {
        if let Some(identities) = contents.get("identities").cloned() {
            out.insert("identities".to_string(), identities);
        }
    }
    out.insert("_meta".to_string(), registry_meta(row));
    Value::Object(out)
}

fn build_token_category_basic(row: &LegacyRegistryRow) -> Value {
    json!({
        "token": {
            "symbol": row.symbol,
            "decimals": row.decimals,
            "category": row.category,
        },
        "_meta": registry_meta(row)
    })
}

fn build_identity_snapshot_basic(row: &LegacyRegistryRow, snapshot: &Value) -> Value {
    let mut out = Map::new();
    for key in [
        "name",
        "description",
        "tags",
        "migrated",
        "status",
        "splitId",
        "split_id",
        "uris",
        "extensions",
    ] {
        if let Some(value) = snapshot.get(key).cloned() {
            let normalized = if key == "split_id" {
                out.insert("splitId".to_string(), value);
                continue;
            } else {
                value
            };
            out.insert(key.to_string(), normalized);
        }
    }

    if let Some(token) = snapshot.get("token") {
        let mut token_obj = Map::new();
        for key in ["category", "symbol", "decimals"] {
            if let Some(value) = token.get(key).cloned() {
                token_obj.insert(key.to_string(), value);
            }
        }
        if let Some(bytecode) = token
            .get("nfts")
            .and_then(|n| n.get("parse"))
            .and_then(|p| p.get("bytecode"))
            .cloned()
        {
            token_obj.insert(
                "nfts".to_string(),
                json!({
                    "parse": {
                        "bytecode": bytecode
                    }
                }),
            );
        }
        out.insert("token".to_string(), Value::Object(token_obj));
    }

    out.insert("_meta".to_string(), registry_meta(row));
    Value::Object(out)
}

fn identity_snapshot_for_row(row: &LegacyRegistryRow) -> Option<Value> {
    if let Some(snapshot) = row.identity_snapshot.clone() {
        return Some(snapshot);
    }

    let contents = row.contents.as_ref()?;
    let identities = contents.get("identities")?.as_object()?;
    let identity_key = contents
        .get("registryIdentity")
        .and_then(|v| v.as_str())
        .filter(|value| identities.contains_key(*value))
        .map(str::to_string)
        .or_else(|| row.authbase_hex.clone())
        .or_else(|| identities.keys().next().cloned())?;

    let history = identities.get(&identity_key)?.as_object()?;
    let mut timestamps: Vec<&String> = history.keys().collect();
    timestamps.sort();
    let latest_key = timestamps.last()?;
    history.get(*latest_key).cloned()
}

fn transform_to_legacy_token_payload(
    mut snapshot: Value,
    type_key: Option<&str>,
    is_nft: bool,
) -> Value {
    let nft_type = if snapshot
        .get("token")
        .and_then(|v| v.get("nfts"))
        .and_then(|v| v.get("parse"))
        .and_then(|v| v.get("bytecode"))
        .is_some()
    {
        "parsable"
    } else {
        "sequential"
    };

    if let Some(type_key) = type_key {
        let normalized = match type_key {
            "empty" | "none" => "",
            other => other,
        };
        if let Some(type_metadata) = snapshot
            .get("token")
            .and_then(|v| v.get("nfts"))
            .and_then(|v| v.get("parse"))
            .and_then(|v| v.get("types"))
            .and_then(|v| v.get(normalized))
            .cloned()
        {
            if let Some(obj) = snapshot.as_object_mut() {
                obj.insert("type_metadata".to_string(), normalize_type_metadata(type_metadata));
            }
        }
    }

    if nft_type == "sequential" {
        if let Some(token) = snapshot.get_mut("token").and_then(Value::as_object_mut) {
            token.remove("nfts");
        }
    }

    if let Some(obj) = snapshot.as_object_mut() {
        obj.remove("_meta");
        obj.insert("is_nft".to_string(), Value::Bool(is_nft));
        obj.insert("nft_type".to_string(), Value::String(nft_type.to_string()));
    }

    snapshot
}

fn normalize_type_metadata(mut metadata: Value) -> Value {
    if let Some(uris) = metadata.get_mut("uris").and_then(Value::as_object_mut) {
        if !uris.contains_key("image") {
            if let Some(asset) = uris.get("asset").and_then(Value::as_str) {
                let lower = asset.to_ascii_lowercase();
                if [".jpg", ".png", ".gif", ".svg"].iter().any(|ext| lower.ends_with(ext)) {
                    uris.insert("image".to_string(), Value::String(asset.to_string()));
                }
            }
        }
        if !uris.contains_key("image") {
            if let Some(icon) = uris.get("icon").cloned() {
                uris.insert("image".to_string(), icon);
            }
        }
    }
    metadata
}

fn nft_types_map(row: &LegacyRegistryRow) -> Option<Map<String, Value>> {
    row.nft_types.clone().and_then(|value| value.as_object().cloned())
}

fn nft_type_from_row(row: &LegacyRegistryRow, commitment: &str) -> Option<Value> {
    nft_types_map(row).and_then(|types| types.get(commitment).cloned())
}

fn registry_meta(row: &LegacyRegistryRow) -> Value {
    json!({
        "registry_id": row.registry_id,
        "category": row.category,
        "authbase": row.authbase_hex,
        "identity_history": row
            .latest_revision
            .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Millis, true)),
    })
}

fn nft_meta(row: &LegacyRegistryRow, commitment: &str) -> Value {
    json!({
        "registry_id": row.registry_id,
        "commitment": commitment,
        "category": row.category,
        "authbase": row.authbase_hex,
        "identity_history": row
            .latest_revision
            .map(|ts| ts.to_rfc3339_opts(SecondsFormat::Millis, true)),
    })
}

fn capability_name(value: i16) -> Option<String> {
    match value {
        0 => Some("none".to_string()),
        1 => Some("mutable".to_string()),
        2 => Some("minting".to_string()),
        _ => None,
    }
}

fn strip_query_from_uri(uri: &axum::http::Uri) -> String {
    uri.path().to_string()
}

fn page_link(base: &str, page: usize, include_metadata: Option<&str>, capability: &[String]) -> String {
    let mut params = vec![format!("page={page}")];
    if is_true(include_metadata) {
        params.push("include_metadata=true".to_string());
    }
    for value in capability {
        params.push(format!("capability={value}"));
    }
    format!("{base}?{}", params.join("&"))
}

fn is_true(value: Option<&str>) -> bool {
    matches!(value, Some(v) if v.eq_ignore_ascii_case("true"))
}

fn is_hex_64(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn query_values(uri: &axum::http::Uri, key: &str) -> Vec<String> {
    uri.query()
        .map(|query| {
            url::form_urlencoded::parse(query.as_bytes())
                .filter_map(|(k, v)| if k == key { Some(v.into_owned()) } else { None })
                .collect()
        })
        .unwrap_or_default()
}

fn parse_txo_path(value: &str) -> Option<(String, i32)> {
    let (txid, vout) = value.split_once(':')?;
    if txid.len() != 64 || !txid.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let vout = vout.parse::<i32>().ok()?;
    if vout < 0 {
        return None;
    }
    Some((txid.to_ascii_lowercase(), vout))
}

fn json_error(status: StatusCode, body: Value) -> Response {
    (status, Json(body)).into_response()
}

fn not_found() -> Response {
    StatusCode::NOT_FOUND.into_response()
}

fn internal_error<E>(err: E) -> Response
where
    E: Into<anyhow::Error>,
{
    let err: anyhow::Error = err.into();
    json_error(
        StatusCode::INTERNAL_SERVER_ERROR,
        json!({
            "error": format!("{err}")
        }),
    )
}
