use anyhow::Context;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::Config;
use crate::db::Database;

#[derive(Clone)]
pub struct ReconciliationWorker {
    config: Config,
    db: Database,
}

impl ReconciliationWorker {
    pub fn new(config: Config, db: Database) -> Self {
        Self { config, db }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!(
            interval_secs = self.config.reconcile_interval_secs,
            "starting reconciliation worker"
        );
        loop {
            if let Err(err) = self.reconcile_once().await {
                warn!(error = ?err, "reconciliation cycle failed; retrying");
            }
            sleep(Duration::from_secs(
                self.config.reconcile_interval_secs.max(1),
            ))
            .await;
        }
    }

    async fn reconcile_once(&self) -> anyhow::Result<()> {
        let height: Option<i32> =
            sqlx::query_scalar("SELECT height FROM chain_state WHERE id = TRUE")
                .fetch_optional(self.db.pool())
                .await
                .context("failed reading chain_state for reconciliation")?;

        let Some(height) = height else {
            return Ok(());
        };

        let mismatches_before = self.mismatch_count().await?;
        if mismatches_before == 0 {
            return Ok(());
        }

        let mut tx = self.db.pool().begin().await?;
        sqlx::query(
            r#"
            WITH holder_rollup AS (
              SELECT
                category,
                COALESCE(SUM(ft_balance), 0)::numeric AS total_ft_supply,
                COUNT(*) FILTER (WHERE ft_balance > 0)::integer AS holder_count,
                COALESCE(SUM(utxo_count), 0)::integer AS utxo_count
              FROM token_holders
              GROUP BY category
            )
            INSERT INTO token_stats(category, total_ft_supply, holder_count, utxo_count, updated_height, updated_at)
            SELECT
              category,
              total_ft_supply,
              holder_count,
              utxo_count,
              $1,
              now()
            FROM holder_rollup
            ON CONFLICT (category)
            DO UPDATE SET
              total_ft_supply = EXCLUDED.total_ft_supply,
              holder_count = EXCLUDED.holder_count,
              utxo_count = EXCLUDED.utxo_count,
              updated_height = GREATEST(token_stats.updated_height, EXCLUDED.updated_height),
              updated_at = now()
            "#,
        )
        .bind(height)
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            DELETE FROM token_stats s
            WHERE NOT EXISTS (
              SELECT 1
              FROM token_holders h
              WHERE h.category = s.category
            )
               OR (s.total_ft_supply = 0 AND s.holder_count = 0 AND s.utxo_count = 0)
            "#,
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        let mismatches_after = self.mismatch_count().await?;
        info!(
            height,
            mismatches_before, mismatches_after, "completed reconciliation cycle"
        );
        Ok(())
    }

    async fn mismatch_count(&self) -> anyhow::Result<i64> {
        let mismatches: i64 = sqlx::query_scalar(
            r#"
            WITH holder_rollup AS (
              SELECT
                category,
                COALESCE(SUM(ft_balance), 0)::numeric AS total_ft_supply,
                COUNT(*) FILTER (WHERE ft_balance > 0)::integer AS holder_count,
                COALESCE(SUM(utxo_count), 0)::integer AS utxo_count
              FROM token_holders
              GROUP BY category
            ),
            categories AS (
              SELECT category FROM token_stats
              UNION
              SELECT category FROM holder_rollup
            )
            SELECT COUNT(*)::bigint
            FROM categories c
            LEFT JOIN token_stats s
              ON s.category = c.category
            LEFT JOIN holder_rollup h
              ON h.category = c.category
            WHERE s.category IS NULL
               OR h.category IS NULL
               OR s.total_ft_supply <> COALESCE(h.total_ft_supply, 0)
               OR s.holder_count <> COALESCE(h.holder_count, 0)
               OR s.utxo_count <> COALESCE(h.utxo_count, 0)
            "#,
        )
        .fetch_one(self.db.pool())
        .await?;

        Ok(mismatches)
    }
}
