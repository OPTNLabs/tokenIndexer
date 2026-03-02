mod api;
mod config;
mod db;
mod ingest;
mod model;

use std::sync::Arc;
use std::net::SocketAddr;

use anyhow::Context;
use tokio::signal;
use tracing::{error, info};

use crate::api::{build_router, AppState};
use crate::config::Config;
use crate::db::Database;
use crate::ingest::{IngestWorker, MempoolWorker};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env();
    tracing_subscriber::fmt()
        .with_env_filter(config.log_level.clone())
        .json()
        .init();

    let db = Database::connect(&config.database_url, config.db_max_connections)
        .await
        .context("failed to connect to postgres")?;

    db.run_migrations().await.context("failed to run migrations")?;

    let state = Arc::new(AppState::new(config.clone(), db.clone()).await?);
    let ingest_worker = IngestWorker::new(config.clone(), db.clone());
    let mempool_worker = MempoolWorker::new(config.clone(), db.clone(), state.mempool_snapshot.clone());

    let api_listener = tokio::net::TcpListener::bind((config.api_host.as_str(), config.api_port))
        .await
        .context("failed to bind api listener")?;
    let router = build_router(state);

    info!(host = %config.api_host, port = config.api_port, "starting API server");

    tokio::select! {
        result = axum::serve(
            api_listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        ) => {
            error!(error = ?result.err(), "api server exited unexpectedly");
        }
        result = ingest_worker.run() => {
            error!(error = ?result.err(), "ingest worker exited unexpectedly");
        }
        result = async {
            if config.mempool_enabled {
                mempool_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            error!(error = ?result.err(), "mempool worker exited unexpectedly");
        }
        _ = signal::ctrl_c() => {
            info!("shutdown signal received");
        }
    }

    Ok(())
}
