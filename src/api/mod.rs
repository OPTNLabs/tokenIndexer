pub mod cache;
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
            started
        )
    }

    pub fn observe_request_duration_ms(&self, ms: u64) {
        self.request_duration_ms_total
            .fetch_add(ms, Ordering::Relaxed);
        self.request_duration_samples
            .fetch_add(1, Ordering::Relaxed);
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
        .route("/v1/token/{category}/summary", get(routes::token_summary))
        .route("/v1/bcmr/{category}", get(routes::bcmr_category))
        .route("/v1/token/{category}/holders/top", get(routes::top_holders))
        .route("/v1/token/{category}/holders", get(routes::paged_holders))
        .route("/v1/token/{category}/mempool", get(routes::token_mempool))
        .route("/v1/token/{category}/insights", get(routes::token_insights))
        .route(
            "/v1/token/{category}/holder/{lockingBytecode}",
            get(routes::holder_eligibility),
        )
        .route(
            "/v1/holder/{lockingBytecode}/tokens",
            get(routes::holder_tokens),
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
