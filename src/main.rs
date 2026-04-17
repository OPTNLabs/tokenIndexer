mod api;
mod bcmr;
mod config;
mod db;
mod ingest;
mod model;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use tokio::signal;
use tokio::task::JoinSet;
use tracing::{error, info};

use crate::api::{build_router, AppState};
use crate::bcmr::BcmrWorker;
use crate::config::Config;
use crate::db::Database;
use crate::ingest::{IngestWorker, MempoolWorker, ReconciliationWorker};
use crate::model::MempoolSnapshot;
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Config::from_env();
    tracing_subscriber::fmt()
        .with_env_filter(config.log_level.clone())
        .json()
        .init();

    let startup = async move {
        log_config("primary", &config);
        let mainnet_config = config.mainnet_config()?;

        let mut stacks = JoinSet::new();
        if let Some(mainnet) = mainnet_config {
            log_config("mainnet", &mainnet);
            info!(
                primary_host = %config.api_host,
                primary_port = config.api_port,
                "mainnet stack enabled; serving both chains on one API endpoint"
            );
            stacks.spawn(run_combined_stack(config, mainnet));
        } else {
            info!("single-chain stack selected");
            stacks.spawn(run_stack(config));
        }

        tokio::select! {
            maybe_result = stacks.join_next() => {
                match maybe_result {
                    Some(Ok(Ok(()))) => Err(anyhow!("stack exited unexpectedly")),
                    Some(Ok(Err(err))) => Err(err),
                    Some(Err(err)) => Err(anyhow!("stack task join error: {err}")),
                    None => Err(anyhow!("no stack tasks running")),
                }
            }
            _ = signal::ctrl_c() => {
                info!("shutdown signal received");
                stacks.abort_all();
                Ok(())
            }
        }
    }
    .await;

    if let Err(err) = &startup {
        error!(error = ?err, "tokenindex exiting after startup/runtime failure");
    }

    startup
}

fn log_config(label: &str, config: &Config) {
    let snapshot = config.startup_snapshot();
    info!(
        stack = label,
        chain = %snapshot.expected_chain,
        api_bind = %snapshot.api_bind,
        db_schema = %snapshot.db_schema,
        database_url = %snapshot.database_url,
        database_read_url = ?snapshot.database_read_url,
        rpc_url = %snapshot.rpc_url,
        redis_enabled = snapshot.redis_enabled,
        zmq_enabled = snapshot.zmq_enabled,
        mempool_enabled = snapshot.mempool_enabled,
        bcmr_enabled = snapshot.bcmr_enabled,
        reconcile_enabled = snapshot.reconcile_enabled,
        bootstrap_height = snapshot.bootstrap_height,
        db_pool_max_connections = snapshot.db_pool_max_connections,
        db_api_pool_max_connections = snapshot.db_api_pool_max_connections,
        db_ingest_pool_max_connections = snapshot.db_ingest_pool_max_connections,
        rpc_batch_size = snapshot.rpc_batch_size,
        rpc_prefetch_batches = snapshot.rpc_prefetch_batches,
        db_ingest_synchronous_commit = %snapshot.db_ingest_synchronous_commit,
        log_level = %snapshot.log_level,
        "loaded startup configuration"
    );
}

async fn run_combined_stack(primary: Config, mainnet: Config) -> anyhow::Result<()> {
    let primary_chain = primary.expected_chain.clone();
    let mainnet_chain = mainnet.expected_chain.clone();
    info!(chain = %primary_chain, "initializing primary ingest database");

    let primary_db_ingest = Database::connect(
        &primary.database_url,
        &primary.db_schema,
        primary.ingest_db_pool_size(),
        primary.db_statement_timeout_ms,
        Some(primary.db_ingest_synchronous_commit.as_str()),
    )
    .await
    .with_context(|| format!("failed to connect ingest postgres pool ({primary_chain})"))?;
    primary_db_ingest
        .run_migrations()
        .await
        .with_context(|| format!("failed to run migrations ({primary_chain})"))?;

    info!(chain = %primary_chain, "initializing primary api database");
    let primary_api_database_url = primary
        .database_read_url
        .as_deref()
        .unwrap_or(&primary.database_url);
    let primary_db_api = Database::connect(
        primary_api_database_url,
        &primary.db_schema,
        primary.api_db_pool_size(),
        primary.db_statement_timeout_ms,
        None,
    )
    .await
    .with_context(|| format!("failed to connect api postgres pool ({primary_chain})"))?;

    info!(chain = %mainnet_chain, "initializing mainnet ingest database");
    let mainnet_db_ingest = Database::connect(
        &mainnet.database_url,
        &mainnet.db_schema,
        mainnet.ingest_db_pool_size(),
        mainnet.db_statement_timeout_ms,
        Some(mainnet.db_ingest_synchronous_commit.as_str()),
    )
    .await
    .with_context(|| format!("failed to connect ingest postgres pool ({mainnet_chain})"))?;
    mainnet_db_ingest
        .run_migrations()
        .await
        .with_context(|| format!("failed to run migrations ({mainnet_chain})"))?;

    info!(chain = %mainnet_chain, "initializing mainnet api database");
    let mainnet_api_database_url = mainnet
        .database_read_url
        .as_deref()
        .unwrap_or(&mainnet.database_url);
    let mainnet_db_api = Database::connect(
        mainnet_api_database_url,
        &mainnet.db_schema,
        mainnet.api_db_pool_size(),
        mainnet.db_statement_timeout_ms,
        None,
    )
    .await
    .with_context(|| format!("failed to connect api postgres pool ({mainnet_chain})"))?;

    let mainnet_mempool_snapshot = Arc::new(RwLock::new(MempoolSnapshot::default()));

    let mut primary_state_raw = AppState::new(primary.clone(), primary_db_api.clone()).await?;
    primary_state_raw.fallback_db = Some(mainnet_db_api.clone());
    primary_state_raw.fallback_config = Some(mainnet.clone());
    primary_state_raw.fallback_mempool_snapshot = Some(mainnet_mempool_snapshot.clone());
    let primary_state = Arc::new(primary_state_raw);
    info!("application state initialized for unified stack");

    let primary_ingest_worker = IngestWorker::new(
        primary.clone(),
        primary_db_ingest.clone(),
        primary_state.metrics.clone(),
    )?;
    let primary_bcmr_worker = BcmrWorker::new(primary.clone(), primary_db_ingest.clone())?;
    let primary_reconcile_worker =
        ReconciliationWorker::new(primary.clone(), primary_db_ingest.clone());
    let primary_mempool_worker = MempoolWorker::new(
        primary.clone(),
        primary_db_ingest.clone(),
        primary_state.mempool_snapshot.clone(),
    )?;

    let mainnet_ingest_worker = IngestWorker::new(
        mainnet.clone(),
        mainnet_db_ingest.clone(),
        primary_state.metrics.clone(),
    )?;
    let mainnet_bcmr_worker = BcmrWorker::new(mainnet.clone(), mainnet_db_ingest.clone())?;
    let mainnet_reconcile_worker =
        ReconciliationWorker::new(mainnet.clone(), mainnet_db_ingest.clone());
    let mainnet_mempool_worker = MempoolWorker::new(
        mainnet.clone(),
        mainnet_db_ingest.clone(),
        mainnet_mempool_snapshot.clone(),
    )?;

    let router = build_router(primary_state)
        .with_context(|| format!("failed to build API router ({primary_chain})"))?;

    info!(chain = %primary_chain, "binding unified API listener");
    let api_listener = tokio::net::TcpListener::bind((primary.api_host.as_str(), primary.api_port))
        .await
        .with_context(|| format!("failed to bind api listener ({primary_chain})"))?;

    info!(
        host = %primary.api_host,
        port = primary.api_port,
        chip_chain = %primary_chain,
        mainnet_chain = %mainnet_chain,
        "starting unified API server"
    );

    tokio::select! {
        result = axum::serve(
            api_listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        ) => {
            result.context("api server exited unexpectedly")
        }
        result = primary_ingest_worker.run() => {
            result.context("primary ingest worker exited unexpectedly")
        }
        result = async {
            if primary.bcmr_enabled {
                primary_bcmr_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("primary BCMR worker exited unexpectedly")
        }
        result = async {
            if primary.mempool_enabled {
                primary_mempool_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("primary mempool worker exited unexpectedly")
        }
        result = async {
            if primary.reconcile_enabled {
                primary_reconcile_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("primary reconciliation worker exited unexpectedly")
        }
        result = mainnet_ingest_worker.run() => {
            result.context("mainnet ingest worker exited unexpectedly")
        }
        result = async {
            if mainnet.bcmr_enabled {
                mainnet_bcmr_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("mainnet BCMR worker exited unexpectedly")
        }
        result = async {
            if mainnet.mempool_enabled {
                mainnet_mempool_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("mainnet mempool worker exited unexpectedly")
        }
        result = async {
            if mainnet.reconcile_enabled {
                mainnet_reconcile_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("mainnet reconciliation worker exited unexpectedly")
        }
    }
}

async fn run_stack(config: Config) -> anyhow::Result<()> {
    let chain = config.expected_chain.clone();
    info!(chain = %chain, "initializing ingest database");
    let db_ingest = Database::connect(
        &config.database_url,
        &config.db_schema,
        config.ingest_db_pool_size(),
        config.db_statement_timeout_ms,
        Some(config.db_ingest_synchronous_commit.as_str()),
    )
    .await
    .with_context(|| format!("failed to connect ingest postgres pool ({chain})"))?;

    db_ingest
        .run_migrations()
        .await
        .with_context(|| format!("failed to run migrations ({chain})"))?;

    info!(chain = %chain, "initializing api database");
    let api_database_url = config
        .database_read_url
        .as_deref()
        .unwrap_or(&config.database_url);
    let db_api = Database::connect(
        api_database_url,
        &config.db_schema,
        config.api_db_pool_size(),
        config.db_statement_timeout_ms,
        None,
    )
    .await
    .with_context(|| format!("failed to connect api postgres pool ({chain})"))?;

    let state = Arc::new(AppState::new(config.clone(), db_api.clone()).await?);
    info!(chain = %chain, "application state initialized");
    let ingest_worker =
        IngestWorker::new(config.clone(), db_ingest.clone(), state.metrics.clone())?;
    let bcmr_worker = BcmrWorker::new(config.clone(), db_ingest.clone())?;
    let reconcile_worker = ReconciliationWorker::new(config.clone(), db_ingest.clone());
    let mempool_worker = MempoolWorker::new(
        config.clone(),
        db_ingest.clone(),
        state.mempool_snapshot.clone(),
    )?;

    info!(chain = %chain, "binding API listener");
    let api_listener = tokio::net::TcpListener::bind((config.api_host.as_str(), config.api_port))
        .await
        .with_context(|| format!("failed to bind api listener ({chain})"))?;
    let router =
        build_router(state).with_context(|| format!("failed to build API router ({chain})"))?;

    info!(
        chain = %chain,
        host = %config.api_host,
        port = config.api_port,
        "starting API server"
    );

    tokio::select! {
        result = axum::serve(
            api_listener,
            router.into_make_service_with_connect_info::<SocketAddr>(),
        ) => {
            result.context("api server exited unexpectedly")
        }
        result = ingest_worker.run() => {
            result.context("ingest worker exited unexpectedly")
        }
        result = async {
            if config.bcmr_enabled {
                bcmr_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("BCMR worker exited unexpectedly")
        }
        result = async {
            if config.mempool_enabled {
                mempool_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("mempool worker exited unexpectedly")
        }
        result = async {
            if config.reconcile_enabled {
                reconcile_worker.run().await
            } else {
                futures::future::pending::<anyhow::Result<()>>().await
            }
        } => {
            result.context("reconciliation worker exited unexpectedly")
        }
    }
}
