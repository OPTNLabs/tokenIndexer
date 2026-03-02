pub mod cache;
pub mod ratelimit;
pub mod routes;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::Context;
use axum::routing::get;
use axum::Router;
use moka::future::Cache;
use redis::aio::ConnectionManager;
use tokio::sync::RwLock;
use tower::limit::GlobalConcurrencyLimitLayer;
use tracing::warn;

use crate::config::Config;
use crate::db::Database;
use crate::model::MempoolSnapshot;
use crate::api::ratelimit::IpRouteRateLimiter;

#[derive(Clone)]
pub struct AppState {
    pub config: Config,
    pub db: Database,
    pub response_cache: Cache<String, Arc<cache::CachedResponse>>,
    pub redis_cache: Option<ConnectionManager>,
    pub rate_limiter: Arc<IpRouteRateLimiter>,
    pub mempool_snapshot: Arc<RwLock<MempoolSnapshot>>,
    pub metrics: Arc<ApiMetrics>,
}

impl AppState {
    pub async fn new(config: Config, db: Database) -> anyhow::Result<Self> {
        let ttl = config.cache_ttl_secs.saturating_add(config.stale_while_error_secs);
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

        Ok(Self {
            config,
            db,
            response_cache: cache,
            redis_cache,
            rate_limiter: Arc::new(IpRouteRateLimiter::default()),
            mempool_snapshot: Arc::new(RwLock::new(MempoolSnapshot::default())),
            metrics: Arc::new(ApiMetrics::default()),
        })
    }
}

#[derive(Debug, Default)]
pub struct ApiMetrics {
    pub requests_total: AtomicU64,
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
        let _ = self
            .started_at_epoch_secs
            .compare_exchange(0, now, Ordering::SeqCst, Ordering::SeqCst);
    }

    pub fn render_prometheus(&self) -> String {
        let started = self.started_at_epoch_secs.load(Ordering::Relaxed);
        format!(
            "# TYPE tokenindex_requests_total counter\n\
tokenindex_requests_total {}\n\
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
}

pub fn build_router(state: Arc<AppState>) -> Router {
    state.metrics.ensure_started();
    let cors = routes::cors_layer(&state.config).context("invalid CORS settings").unwrap();

    Router::new()
        .route("/health", get(routes::health))
        .route("/metrics", get(routes::metrics))
        .route("/v1/token/{category}/summary", get(routes::token_summary))
        .route("/v1/token/{category}/holders/top", get(routes::top_holders))
        .route("/v1/token/{category}/holders", get(routes::paged_holders))
        .route("/v1/token/{category}/mempool", get(routes::token_mempool))
        .route("/v1/token/{category}/insights", get(routes::token_insights))
        .route(
            "/v1/token/{category}/holder/{lockingBytecode}",
            get(routes::holder_eligibility),
        )
        .route("/v1/holder/{lockingBytecode}/tokens", get(routes::holder_tokens))
        .layer(cors)
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            routes::rate_limit_middleware,
        ))
        .layer(GlobalConcurrencyLimitLayer::new(
            state.config.api_concurrency_limit,
        ))
        .layer(tower_http::compression::CompressionLayer::new())
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .with_state(state)
}
