pub mod cache;
pub mod legacy;
pub mod ratelimit;
pub mod routes;

use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use dashmap::DashMap;
use moka::future::Cache;
use redis::aio::ConnectionManager;
use tokio::sync::{Mutex, RwLock};
use tower::limit::GlobalConcurrencyLimitLayer;
use tracing::warn;

use crate::api::ratelimit::IpRouteRateLimiter;
use crate::config::Config;
use crate::db::Database;
use crate::model::MempoolSnapshot;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Database,
    pub fallback_db: Option<Database>,
    pub fallback_config: Option<Config>,
    pub response_cache: Cache<String, Arc<cache::CachedResponse>>,
    pub inflight_cache_fills: Arc<DashMap<String, Arc<Mutex<()>>>>,
    pub redis_cache: Option<ConnectionManager>,
    pub rate_limiter: Arc<IpRouteRateLimiter>,
    pub mempool_snapshot: Arc<RwLock<MempoolSnapshot>>,
    pub fallback_mempool_snapshot: Option<Arc<RwLock<MempoolSnapshot>>>,
    pub api_bearer_token: Option<Arc<str>>,
    pub ip_allowlist: Arc<Vec<IpCidr>>,
    pub trusted_proxy_cidrs: Arc<Vec<IpCidr>>,
    pub metrics: Arc<ApiMetrics>,
}

impl AppState {
    pub async fn new(config: Config, db: Database) -> anyhow::Result<Self> {
        let ttl = config
            .cache_ttl_secs
            .saturating_add(config.stale_while_error_secs);
        let cache = Cache::builder()
            .max_capacity(config.cache_max_items)
            .time_to_live(Duration::from_secs(ttl))
            .build();

        let redis_cache = if let Some(redis_url) = &config.redis_url {
            match redis::Client::open(redis_url.clone()) {
                Ok(client) => match ConnectionManager::new(client).await {
                    Ok(conn) => Some(conn),
                    Err(err) => {
                        warn!(error = ?err, "redis disabled: failed to create connection manager");
                        None
                    }
                },
                Err(err) => {
                    warn!(error = ?err, "redis disabled: invalid redis url");
                    None
                }
            }
        } else {
            None
        };

        let ip_allowlist = parse_cidr_list(&config.ip_allowlist)
            .context("invalid TOKENINDEX_IP_ALLOWLIST entry")?;
        let trusted_proxy_cidrs = parse_cidr_list(&config.trusted_proxy_cidrs)
            .context("invalid TOKENINDEX_TRUSTED_PROXY_CIDRS entry")?;
        let api_bearer_token = config
            .api_bearer_token
            .as_ref()
            .map(|v| Arc::<str>::from(v.trim().to_string()))
            .filter(|v| !v.is_empty());

        Ok(Self {
            config,
            db,
            fallback_db: None,
            fallback_config: None,
            response_cache: cache,
            inflight_cache_fills: Arc::new(DashMap::new()),
            redis_cache,
            rate_limiter: Arc::new(IpRouteRateLimiter::default()),
            mempool_snapshot: Arc::new(RwLock::new(MempoolSnapshot::default())),
            fallback_mempool_snapshot: None,
            api_bearer_token,
            ip_allowlist: Arc::new(ip_allowlist),
            trusted_proxy_cidrs: Arc::new(trusted_proxy_cidrs),
            metrics: Arc::new(ApiMetrics::default()),
        })
    }
}

#[derive(Debug, Clone)]
pub struct IpCidr {
    network: IpAddr,
    prefix: u8,
}

impl IpCidr {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        let value = value.trim();
        if value.is_empty() {
            anyhow::bail!("empty CIDR");
        }
        if let Some((ip_part, prefix_part)) = value.split_once('/') {
            let ip: IpAddr = ip_part
                .parse()
                .with_context(|| format!("invalid IP '{ip_part}'"))?;
            let prefix: u8 = prefix_part
                .parse()
                .with_context(|| format!("invalid prefix '{prefix_part}'"))?;
            let max = match ip {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            if prefix > max {
                anyhow::bail!("prefix out of range for {ip}");
            }
            Ok(Self {
                network: ip,
                prefix,
            })
        } else {
            let ip: IpAddr = value
                .parse()
                .with_context(|| format!("invalid IP '{value}'"))?;
            let prefix = match ip {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            Ok(Self {
                network: ip,
                prefix,
            })
        }
    }

    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(network), IpAddr::V4(candidate)) => {
                let n = u32::from(network);
                let c = u32::from(candidate);
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                (n & mask) == (c & mask)
            }
            (IpAddr::V6(network), IpAddr::V6(candidate)) => {
                let n = u128::from(network);
                let c = u128::from(candidate);
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                (n & mask) == (c & mask)
            }
            _ => false,
        }
    }
}

fn parse_cidr_list(values: &[String]) -> anyhow::Result<Vec<IpCidr>> {
    let mut out = Vec::new();
    for value in values {
        if value.trim().is_empty() {
            continue;
        }
        out.push(IpCidr::parse(value)?);
    }
    Ok(out)
}

#[derive(Debug, Default)]
pub struct ApiMetrics {
    pub requests_total: AtomicU64,
    pub request_duration_ms_total: AtomicU64,
    pub request_duration_samples: AtomicU64,
    pub cache_hits: AtomicU64,
    pub cache_misses: AtomicU64,
    pub stale_served: AtomicU64,
    pub db_errors: AtomicU64,
    pub rate_limited: AtomicU64,
    pub redis_hits: AtomicU64,
    pub redis_misses: AtomicU64,
    pub started_at_epoch_secs: AtomicU64,
    pub ingest_blocks_applied: AtomicU64,
    pub ingest_prefetch_batches: AtomicU64,
    pub ingest_prefetch_blocks: AtomicU64,
    pub ingest_reorgs: AtomicU64,
    pub ingest_rollback_blocks: AtomicU64,
    pub ingest_spend_rows: AtomicU64,
    pub ingest_credit_rows: AtomicU64,
    pub ingest_holder_delta_rows: AtomicU64,
    pub ingest_prefetch_ms_total: AtomicU64,
    pub ingest_apply_ms_total: AtomicU64,
    pub ingest_commit_ms_total: AtomicU64,
    pub ingest_last_indexed_height: AtomicU64,
    pub ingest_last_known_tip_height: AtomicU64,
    pub ingest_last_lag_blocks: AtomicU64,
}

impl ApiMetrics {
    pub fn ensure_started(&self) {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        let _ =
            self.started_at_epoch_secs
                .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst);
    }

    pub fn render_prometheus(&self) -> String {
        let started = self.started_at_epoch_secs.load(Ordering::Relaxed);
        format!(
            "# TYPE tokenindex_requests_total counter\n\
tokenindex_requests_total {}\n\
# TYPE tokenindex_request_duration_ms_sum counter\n\
tokenindex_request_duration_ms_sum {}\n\
# TYPE tokenindex_request_duration_ms_count counter\n\
tokenindex_request_duration_ms_count {}\n\
# TYPE tokenindex_cache_hits_total counter\n\
tokenindex_cache_hits_total {}\n\
# TYPE tokenindex_cache_misses_total counter\n\
tokenindex_cache_misses_total {}\n\
# TYPE tokenindex_stale_served_total counter\n\
tokenindex_stale_served_total {}\n\
# TYPE tokenindex_db_errors_total counter\n\
tokenindex_db_errors_total {}\n\
# TYPE tokenindex_rate_limited_total counter\n\
tokenindex_rate_limited_total {}\n\
# TYPE tokenindex_redis_hits_total counter\n\
tokenindex_redis_hits_total {}\n\
# TYPE tokenindex_redis_misses_total counter\n\
tokenindex_redis_misses_total {}\n\
# TYPE tokenindex_ingest_blocks_applied_total counter\n\
tokenindex_ingest_blocks_applied_total {}\n\
# TYPE tokenindex_ingest_prefetch_batches_total counter\n\
tokenindex_ingest_prefetch_batches_total {}\n\
# TYPE tokenindex_ingest_prefetch_blocks_total counter\n\
tokenindex_ingest_prefetch_blocks_total {}\n\
# TYPE tokenindex_ingest_reorgs_total counter\n\
tokenindex_ingest_reorgs_total {}\n\
# TYPE tokenindex_ingest_rollback_blocks_total counter\n\
tokenindex_ingest_rollback_blocks_total {}\n\
# TYPE tokenindex_ingest_spend_rows_total counter\n\
tokenindex_ingest_spend_rows_total {}\n\
# TYPE tokenindex_ingest_credit_rows_total counter\n\
tokenindex_ingest_credit_rows_total {}\n\
# TYPE tokenindex_ingest_holder_delta_rows_total counter\n\
tokenindex_ingest_holder_delta_rows_total {}\n\
# TYPE tokenindex_ingest_prefetch_ms_sum counter\n\
tokenindex_ingest_prefetch_ms_sum {}\n\
# TYPE tokenindex_ingest_apply_ms_sum counter\n\
tokenindex_ingest_apply_ms_sum {}\n\
# TYPE tokenindex_ingest_commit_ms_sum counter\n\
tokenindex_ingest_commit_ms_sum {}\n\
# TYPE tokenindex_ingest_last_indexed_height gauge\n\
tokenindex_ingest_last_indexed_height {}\n\
# TYPE tokenindex_ingest_last_known_tip_height gauge\n\
tokenindex_ingest_last_known_tip_height {}\n\
# TYPE tokenindex_ingest_last_lag_blocks gauge\n\
tokenindex_ingest_last_lag_blocks {}\n\
# TYPE tokenindex_started_at_seconds gauge\n\
tokenindex_started_at_seconds {}\n",
            self.requests_total.load(Ordering::Relaxed),
            self.request_duration_ms_total.load(Ordering::Relaxed),
            self.request_duration_samples.load(Ordering::Relaxed),
            self.cache_hits.load(Ordering::Relaxed),
            self.cache_misses.load(Ordering::Relaxed),
            self.stale_served.load(Ordering::Relaxed),
            self.db_errors.load(Ordering::Relaxed),
            self.rate_limited.load(Ordering::Relaxed),
            self.redis_hits.load(Ordering::Relaxed),
            self.redis_misses.load(Ordering::Relaxed),
            self.ingest_blocks_applied.load(Ordering::Relaxed),
            self.ingest_prefetch_batches.load(Ordering::Relaxed),
            self.ingest_prefetch_blocks.load(Ordering::Relaxed),
            self.ingest_reorgs.load(Ordering::Relaxed),
            self.ingest_rollback_blocks.load(Ordering::Relaxed),
            self.ingest_spend_rows.load(Ordering::Relaxed),
            self.ingest_credit_rows.load(Ordering::Relaxed),
            self.ingest_holder_delta_rows.load(Ordering::Relaxed),
            self.ingest_prefetch_ms_total.load(Ordering::Relaxed),
            self.ingest_apply_ms_total.load(Ordering::Relaxed),
            self.ingest_commit_ms_total.load(Ordering::Relaxed),
            self.ingest_last_indexed_height.load(Ordering::Relaxed),
            self.ingest_last_known_tip_height.load(Ordering::Relaxed),
            self.ingest_last_lag_blocks.load(Ordering::Relaxed),
            started
        )
    }

    pub fn observe_request_duration_ms(&self, ms: u64) {
        self.request_duration_ms_total
            .fetch_add(ms, Ordering::Relaxed);
        self.request_duration_samples
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_ingest_prefetch(&self, batches: u64, blocks: u64, ms: u64) {
        self.ingest_prefetch_batches
            .fetch_add(batches, Ordering::Relaxed);
        self.ingest_prefetch_blocks
            .fetch_add(blocks, Ordering::Relaxed);
        self.ingest_prefetch_ms_total
            .fetch_add(ms, Ordering::Relaxed);
    }

    pub fn observe_ingest_apply(&self, ms: u64) {
        self.ingest_blocks_applied.fetch_add(1, Ordering::Relaxed);
        self.ingest_apply_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn observe_ingest_commit(&self, ms: u64) {
        self.ingest_commit_ms_total.fetch_add(ms, Ordering::Relaxed);
    }

    pub fn observe_ingest_rows(&self, spends: u64, credits: u64, holder_deltas: u64) {
        self.ingest_spend_rows.fetch_add(spends, Ordering::Relaxed);
        self.ingest_credit_rows
            .fetch_add(credits, Ordering::Relaxed);
        self.ingest_holder_delta_rows
            .fetch_add(holder_deltas, Ordering::Relaxed);
    }

    pub fn observe_ingest_tip(&self, indexed_height: i32, known_tip_height: i32) {
        self.ingest_last_indexed_height
            .store(indexed_height.max(0) as u64, Ordering::Relaxed);
        self.ingest_last_known_tip_height
            .store(known_tip_height.max(0) as u64, Ordering::Relaxed);
        self.ingest_last_lag_blocks.store(
            (known_tip_height - indexed_height).max(0) as u64,
            Ordering::Relaxed,
        );
    }
}

pub fn build_router(state: Arc<AppState>) -> anyhow::Result<Router> {
    state.metrics.ensure_started();
    let cors = routes::cors_layer(&state.config).context("invalid CORS settings")?;

    Ok(Router::new()
        .route("/health", get(routes::health))
        .route("/health/details", get(routes::health_details))
        .route("/metrics", get(routes::metrics))
        .route("/v1/tokens/known", get(routes::known_tokens))
        .route("/v1/token/{category}", get(routes::token_summary))
        .route("/v1/token/{category}/summary", get(routes::token_summary))
        .route("/v1/bcmr/{category}", get(routes::bcmr_category))
        .route("/v1/token/{category}/bcmr", get(routes::bcmr_category))
        .route("/v1/token/{category}/authchain/head", get(legacy::authchain_head))
        .route("/v1/token/{category}/authchain/head/", get(legacy::authchain_head))
        .route("/v1/token/{category}/holders/top", get(routes::top_holders))
        .route("/v1/token/{category}/holders", get(routes::paged_holders))
        .route("/v1/token/{category}/mempool", get(routes::token_mempool))
        .route("/v1/token/{category}/insights", get(routes::token_insights))
        .route(
            "/v1/token/{category}/holder/{address}",
            get(routes::holder_eligibility),
        )
        .route("/v1/address/{address}/tokens", get(routes::holder_tokens))
        .route("/api/status/latest-block/", get(legacy::latest_block))
        .route("/api/status/latest-block", get(legacy::latest_block))
        .route("/api/tokens/{category}/icon-symbol", get(legacy::token_icon_symbol))
        .route("/api/tokens/{category}/", get(legacy::token))
        .route("/api/tokens/{category}", get(legacy::token))
        .route("/api/tokens/{category}/{type_key}/", get(legacy::token_type))
        .route("/api/tokens/{category}/{type_key}", get(legacy::token_type))
        .route("/api/registries/{category}/latest/", get(legacy::registries_latest))
        .route("/api/registries/{category}/latest", get(legacy::registries_latest))
        .route("/api/registries/{txo}/", get(legacy::registries_txo))
        .route("/api/registries/{txo}", get(legacy::registries_txo))
        .route("/api/bcmr/{category}/", get(legacy::bcmr_contents))
        .route("/api/bcmr/{category}", get(legacy::bcmr_contents))
        .route("/api/bcmr/{category}/token/", get(legacy::bcmr_token))
        .route("/api/bcmr/{category}/token", get(legacy::bcmr_token))
        .route(
            "/api/bcmr/{category}/token/nfts/{commitment}/",
            get(legacy::bcmr_token_nft),
        )
        .route(
            "/api/bcmr/{category}/token/nfts/{commitment}",
            get(legacy::bcmr_token_nft),
        )
        .route("/api/bcmr/{category}/uris/", get(legacy::bcmr_uris))
        .route("/api/bcmr/{category}/uris", get(legacy::bcmr_uris))
        .route("/api/bcmr/{category}/uris/icon", get(legacy::bcmr_icon_uri))
        .route(
            "/api/bcmr/{category}/uris/published-url",
            get(legacy::bcmr_published_url),
        )
        .route("/api/bcmr/{category}/reindex/", get(legacy::bcmr_reindex))
        .route("/api/bcmr/{category}/reindex", get(legacy::bcmr_reindex))
        .route("/api/authchain/{category}/head/", get(legacy::authchain_head))
        .route("/api/authchain/{category}/head", get(legacy::authchain_head))
        .route("/api/registry/{category}/", get(legacy::registry))
        .route("/api/registry/{category}", get(legacy::registry))
        .route(
            "/api/registry/{category}/identity-snapshot/",
            get(legacy::registry_identity_snapshot),
        )
        .route(
            "/api/registry/{category}/identity-snapshot",
            get(legacy::registry_identity_snapshot),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/",
            get(legacy::registry_token_category),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category",
            get(legacy::registry_token_category),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/",
            get(legacy::registry_nfts),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts",
            get(legacy::registry_nfts),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/bytecode/",
            get(legacy::registry_parse_bytecode),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/bytecode",
            get(legacy::registry_parse_bytecode),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/types/",
            get(legacy::registry_nft_types),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/types",
            get(legacy::registry_nft_types),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/types/{commitment}/",
            get(legacy::registry_nft_type),
        )
        .route(
            "/api/registry/{category}/identity-snapshot/token-category/nfts/parse/types/{commitment}",
            get(legacy::registry_nft_type),
        )
        .route("/api/cashtokens/", get(legacy::cashtokens))
        .route("/api/cashtokens", get(legacy::cashtokens))
        .route("/api/cashtokens/{category}/", get(legacy::cashtokens))
        .route("/api/cashtokens/{category}", get(legacy::cashtokens))
        .route(
            "/api/cashtokens/{category}/{token_type}/",
            get(legacy::cashtokens),
        )
        .route(
            "/api/cashtokens/{category}/{token_type}",
            get(legacy::cashtokens),
        )
        .route(
            "/api/cashtokens/{category}/{token_type}/{commitment}/",
            get(legacy::cashtokens),
        )
        .route(
            "/api/cashtokens/{category}/{token_type}/{commitment}",
            get(legacy::cashtokens),
        )
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            routes::security_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            routes::rate_limit_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            routes::timing_middleware,
        ))
        .layer(GlobalConcurrencyLimitLayer::new(
            state.config.api_concurrency_limit,
        ))
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state))
}
