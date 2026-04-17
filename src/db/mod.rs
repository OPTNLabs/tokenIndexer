use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool};
use tracing::info;

pub mod queries;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Database {
    pool: Arc<PgPool>,
}

impl Database {
    pub async fn connect(
        database_url: &str,
        db_schema: &str,
        max_connections: u32,
        statement_timeout_ms: u64,
        synchronous_commit: Option<&str>,
    ) -> anyhow::Result<Self> {
        let statement_timeout_ms = statement_timeout_ms as i64;
        validate_schema_name(db_schema)?;
        if let Some(mode) = synchronous_commit {
            validate_synchronous_commit_mode(mode)?;
        }
        let schema = db_schema.to_ascii_lowercase();
        let schema_for_connect = schema.clone();
        let sync_commit_for_connect = synchronous_commit.map(str::to_string);
        info!(
            schema = %schema,
            max_connections,
            statement_timeout_ms,
            "opening postgres pool"
        );
        let pool = PgPoolOptions::new()
            .min_connections(1)
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(1_800))
            .after_connect(move |conn, _meta| {
                let statement_timeout_ms = statement_timeout_ms;
                let schema = schema_for_connect.clone();
                let synchronous_commit = sync_commit_for_connect.clone();
                Box::pin(async move {
                    let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {schema}");
                    sqlx::query(&create_schema).execute(&mut *conn).await?;
                    let set_search_path = format!("SET search_path TO {schema}, public");
                    sqlx::query(&set_search_path).execute(&mut *conn).await?;
                    let set_timeout = format!("SET statement_timeout = {}", statement_timeout_ms);
                    sqlx::query(&set_timeout).execute(&mut *conn).await?;
                    if let Some(mode) = synchronous_commit.as_deref() {
                        let set_sync_commit = format!("SET synchronous_commit TO {mode}");
                        sqlx::query(&set_sync_commit).execute(&mut *conn).await?;
                    }
                    Ok(())
                })
            })
            .connect(database_url)
            .await
            .with_context(|| format!("failed opening postgres pool: {database_url}"))?;

        info!(schema = %schema, "postgres pool ready");

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    pub async fn run_migrations(&self) -> anyhow::Result<()> {
        info!("running database migrations");
        MIGRATOR
            .run(self.pool.as_ref())
            .await
            .context("migrations failed")?;
        info!("database migrations complete");
        Ok(())
    }

    pub fn pool(&self) -> &PgPool {
        self.pool.as_ref()
    }
}

fn validate_schema_name(schema: &str) -> anyhow::Result<()> {
    let mut chars = schema.chars();
    let Some(first) = chars.next() else {
        anyhow::bail!("database schema cannot be empty");
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        anyhow::bail!("invalid database schema '{schema}'");
    }
    if !chars.all(|c| c.is_ascii_alphanumeric() || c == '_') {
        anyhow::bail!("invalid database schema '{schema}'");
    }
    Ok(())
}

fn validate_synchronous_commit_mode(mode: &str) -> anyhow::Result<()> {
    match mode {
        "on" | "off" | "local" | "remote_write" | "remote_apply" => Ok(()),
        _ => anyhow::bail!("invalid synchronous_commit mode '{mode}'"),
    }
}
