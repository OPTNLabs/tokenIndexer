use std::sync::Arc;

use anyhow::Context;
use sqlx::{migrate::Migrator, postgres::PgPoolOptions, PgPool};

pub mod queries;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Database {
    pool: Arc<PgPool>,
}

impl Database {
    pub async fn connect(database_url: &str, max_connections: u32) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect(database_url)
            .await
            .with_context(|| format!("failed opening postgres pool: {database_url}"))?;

        Ok(Self {
            pool: Arc::new(pool),
        })
    }

    pub async fn run_migrations(&self) -> anyhow::Result<()> {
        MIGRATOR.run(self.pool.as_ref()).await.context("migrations failed")
    }

    pub fn pool(&self) -> &PgPool {
        self.pool.as_ref()
    }
}
