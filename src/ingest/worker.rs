use anyhow::{anyhow, Context};
use num_bigint::BigInt;
use num_traits::Zero;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::api::ApiMetrics;
use crate::bcmr::parse_bcmr_op_return;
use crate::config::Config;
use crate::db::Database;

use super::rpc::RpcClient;

#[derive(Clone)]
pub struct IngestWorker {
    config: Config,
    db: Database,
    rpc: RpcClient,
    metrics: Arc<ApiMetrics>,
}

#[derive(Debug, Clone)]
struct TokenOutput {
    txid_hex: String,
    vout: i32,
    category_hex: String,
    locking_bytecode_hex: String,
    locking_address: Option<String>,
    ft_amount: String,
    nft_capability: Option<i16>,
    nft_commitment_hex: Option<String>,
    satoshis: i64,
}

#[derive(Debug, Clone)]
struct SpendInput {
    prev_txid_hex: String,
    prev_vout: i32,
}

#[derive(Debug, Clone)]
struct HolderDeltaEvent {
    category_hex: String,
    locking_bytecode_hex: String,
    locking_address: Option<String>,
    ft_delta: String,
    utxo_delta: i32,
}

#[derive(Debug, Default)]
struct HolderDeltaAcc {
    locking_address: Option<String>,
    ft_delta: BigInt,
    utxo_delta: i32,
}

#[derive(Debug)]
struct FetchedBlockBatch {
    blocks: Vec<(i32, Value)>,
    fetch_ms: u64,
}

impl IngestWorker {
    pub fn new(config: Config, db: Database, metrics: Arc<ApiMetrics>) -> anyhow::Result<Self> {
        let rpc = RpcClient::new(
            config.rpc_url.clone(),
            config.rpc_user.clone(),
            config.rpc_pass.clone(),
            config.rpc_timeout_ms,
            config.rpc_retries,
            config.rpc_retry_backoff_ms,
        )?;
        Ok(Self {
            config,
            db,
            rpc,
            metrics,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!(rpc = %self.config.rpc_url, "starting ingest worker");
        self.verify_expected_chain().await?;

        loop {
            if let Err(err) = self.sync_once().await {
                warn!(error = ?err, "sync iteration failed; retrying");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }

    async fn sync_once(&self) -> anyhow::Result<()> {
        let chain_height: i64 = self.rpc.call("getblockcount", json!([])).await?;
        let chain_height = chain_height as i32;
        let mut state_height = self.current_height().await?;
        let node_recovering = state_height > chain_height;
        state_height = self
            .reconcile_state_with_node(chain_height, state_height)
            .await?;

        if state_height < self.config.bootstrap_height {
            state_height = self.config.bootstrap_height;
        }
        self.metrics.observe_ingest_tip(state_height, chain_height);

        if node_recovering {
            sleep(Duration::from_millis(1_000)).await;
            return Ok(());
        }

        if state_height >= chain_height {
            sleep(Duration::from_millis(600)).await;
            return Ok(());
        }

        let next_height = state_height + 1;
        let batch_size = self.config.rpc_batch_size.max(1) as i32;
        let prefetch_batches = self.config.rpc_prefetch_batches.max(1);
        let (tx, mut rx) = mpsc::channel::<anyhow::Result<FetchedBlockBatch>>(prefetch_batches);
        let rpc = self.rpc.clone();

        let fetch_task = tokio::spawn(async move {
            let mut batch_start = next_height;
            while batch_start <= chain_height {
                let batch_end = (batch_start + batch_size - 1).min(chain_height);
                let started = Instant::now();
                let blocks = IngestWorker::fetch_blocks_with_rpc(&rpc, batch_start, batch_end)
                    .await
                    .with_context(|| {
                        format!("failed fetching block batch {batch_start}..={batch_end}")
                    })?;
                let fetch_ms = started.elapsed().as_millis() as u64;
                if tx
                    .send(Ok(FetchedBlockBatch { blocks, fetch_ms }))
                    .await
                    .is_err()
                {
                    return Ok(());
                }
                batch_start = batch_end + 1;
            }
            Ok::<(), anyhow::Error>(())
        });

        while let Some(batch_result) = rx.recv().await {
            let batch = batch_result?;
            let batch_first_height = batch.blocks.first().map(|(height, _)| *height).unwrap_or(0);
            let batch_last_height = batch.blocks.last().map(|(height, _)| *height).unwrap_or(0);
            self.metrics
                .observe_ingest_prefetch(1, batch.blocks.len() as u64, batch.fetch_ms);
            info!(
                batch_first_height,
                batch_last_height,
                blocks = batch.blocks.len(),
                fetch_ms = batch.fetch_ms,
                "prefetched block batch"
            );

            for (height, block) in batch.blocks {
                if self.is_parent_mismatch(height, &block).await? {
                    self.handle_reorg(height).await?;
                    fetch_task.abort();
                    return Ok(());
                }

                let started = Instant::now();
                let commit_ms = self.apply_block(height, &block).await?;
                let apply_ms = started.elapsed().as_millis() as u64;
                self.metrics.observe_ingest_apply(apply_ms);
                self.metrics.observe_ingest_commit(commit_ms);
                self.metrics.observe_ingest_tip(height, chain_height);
                info!(
                    height,
                    elapsed_ms = apply_ms,
                    commit_ms,
                    lag_blocks = (chain_height - height).max(0),
                    "applied block"
                );
                if self.config.consistency_check_interval > 0
                    && height % self.config.consistency_check_interval == 0
                {
                    self.run_consistency_check(height).await?;
                }
            }
        }

        match fetch_task.await {
            Ok(result) => result?,
            Err(err) if err.is_cancelled() => {}
            Err(err) => return Err(anyhow!("prefetch task join error: {err}")),
        }
        Ok(())
    }

    async fn fetch_blocks_with_rpc(
        rpc: &RpcClient,
        start_height: i32,
        end_height: i32,
    ) -> anyhow::Result<Vec<(i32, Value)>> {
        if start_height > end_height {
            return Ok(Vec::new());
        }

        let heights: Vec<i32> = (start_height..=end_height).collect();
        let hash_params: Vec<Value> = heights.iter().map(|height| json!([height])).collect();
        let hashes: Vec<String> = if heights.len() == 1 {
            vec![rpc
                .call("getblockhash", json!([heights[0]]))
                .await
                .context("getblockhash failed")?]
        } else {
            rpc.call_batch("getblockhash", hash_params)
                .await
                .context("getblockhash batch failed")?
        };

        let block_params: Vec<Value> = hashes.iter().map(|hash| json!([hash, 2])).collect();
        let blocks: Vec<Value> = if hashes.len() == 1 {
            vec![rpc
                .call("getblock", json!([&hashes[0], 2]))
                .await
                .context("getblock verbosity=2 failed")?]
        } else {
            rpc.call_batch("getblock", block_params)
                .await
                .context("getblock batch verbosity=2 failed")?
        };

        Ok(heights.into_iter().zip(blocks).collect())
    }

    async fn reconcile_state_with_node(
        &self,
        chain_height: i32,
        mut state_height: i32,
    ) -> anyhow::Result<i32> {
        if state_height < self.config.bootstrap_height {
            return Ok(state_height);
        }

        if state_height > chain_height {
            warn!(
                state_height,
                chain_height,
                "local chain_state ahead of node tip; waiting for node recovery before reorg checks"
            );
            return Ok(state_height);
        }

        let local: Option<(i32, String)> = sqlx::query_as(
            "SELECT height, encode(blockhash, 'hex') FROM chain_state WHERE id = TRUE",
        )
        .fetch_optional(self.db.pool())
        .await
        .context("failed to read chain_state for reconciliation")?;

        let Some((local_height, local_hash)) = local else {
            return Ok(state_height);
        };

        if local_height < self.config.bootstrap_height {
            return Ok(state_height);
        }

        let canonical_hash: String = self.rpc.call("getblockhash", json!([local_height])).await?;
        if canonical_hash == local_hash {
            return Ok(state_height);
        }

        warn!(
            local_height,
            "detected chain-state divergence from node tip-at-height; attempting reorg recovery"
        );

        if let Err(err) = self.handle_reorg(local_height + 1).await {
            warn!(
                error = ?err,
                local_height,
                "reorg recovery window exhausted; resetting indexed chain state for full resync"
            );
            self.reset_indexed_state().await?;
            return Ok(self.config.bootstrap_height - 1);
        }

        state_height = self.current_height().await?;
        Ok(state_height)
    }

    async fn reset_indexed_state(&self) -> anyhow::Result<()> {
        let mut tx = self.db.pool().begin().await?;
        sqlx::query(
            r#"
            TRUNCATE TABLE
              bcmr_category_metadata,
              bcmr_registries,
              bcmr_candidates,
              bcmr_category_checks,
              token_holders,
              token_outpoints,
              token_stats,
              applied_blocks,
              chain_state
            RESTART IDENTITY
            "#,
        )
        .execute(&mut *tx)
        .await
        .context("failed to reset indexed state tables")?;
        tx.commit().await?;
        warn!(
            bootstrap_height = self.config.bootstrap_height,
            "indexed state reset complete; worker will re-bootstrap from configured height"
        );
        Ok(())
    }

    async fn verify_expected_chain(&self) -> anyhow::Result<()> {
        #[derive(serde::Deserialize)]
        struct ChainInfo {
            chain: String,
        }

        let info: ChainInfo = self.rpc.call("getblockchaininfo", json!([])).await?;
        if info.chain != self.config.expected_chain {
            return Err(anyhow!(
                "unexpected BCHN chain '{}', expected '{}'",
                info.chain,
                self.config.expected_chain
            ));
        }
        info!(chain = %info.chain, "verified BCHN expected chain");
        Ok(())
    }

    async fn current_height(&self) -> anyhow::Result<i32> {
        let row: Option<(i32,)> = sqlx::query_as("SELECT height FROM chain_state WHERE id = TRUE")
            .fetch_optional(self.db.pool())
            .await
            .context("failed to read chain_state")?;

        Ok(row.map(|r| r.0).unwrap_or(self.config.bootstrap_height - 1))
    }

    async fn is_parent_mismatch(
        &self,
        next_height: i32,
        block: &serde_json::Value,
    ) -> anyhow::Result<bool> {
        let Some(expected_prev) = block["previousblockhash"].as_str() else {
            return Ok(false);
        };

        let local: Option<(i32, String)> = sqlx::query_as(
            "SELECT height, encode(blockhash, 'hex') FROM chain_state WHERE id = TRUE",
        )
        .fetch_optional(self.db.pool())
        .await?;

        let Some((local_height, local_hash)) = local else {
            return Ok(false);
        };

        if local_height + 1 != next_height {
            return Ok(false);
        }

        Ok(local_hash != expected_prev)
    }

    async fn handle_reorg(&self, incoming_height: i32) -> anyhow::Result<()> {
        self.metrics
            .ingest_reorgs
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        warn!(
            incoming_height,
            reorg_window = self.config.reorg_window,
            "parent mismatch detected; beginning rollback scan"
        );

        let tip: (i32, String) = sqlx::query_as(
            "SELECT height, encode(blockhash, 'hex') FROM chain_state WHERE id = TRUE",
        )
        .fetch_one(self.db.pool())
        .await
        .context("missing chain_state during reorg handling")?;

        let scan_floor = (tip.0 - self.config.reorg_window).max(self.config.bootstrap_height - 1);
        let mut fork_height: Option<i32> = None;

        for h in (scan_floor..=tip.0).rev() {
            let local_hash: Option<(String,)> =
                sqlx::query_as("SELECT encode(hash, 'hex') FROM applied_blocks WHERE height = $1")
                    .bind(h)
                    .fetch_optional(self.db.pool())
                    .await?;

            let Some((local_hash,)) = local_hash else {
                continue;
            };

            let canonical_hash: String = self.rpc.call("getblockhash", json!([h])).await?;
            if local_hash == canonical_hash {
                fork_height = Some(h);
                break;
            }
        }

        let fork_height = fork_height.ok_or_else(|| {
            anyhow!(
                "unable to find common ancestor in rollback window (tip={}, floor={})",
                tip.0,
                scan_floor
            )
        })?;

        if fork_height == tip.0 {
            return Ok(());
        }

        let mut tx = self.db.pool().begin().await?;

        for h in ((fork_height + 1)..=tip.0).rev() {
            self.rollback_one_block(h, &mut tx).await?;
            self.metrics
                .ingest_rollback_blocks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let canonical_fork_hash: String =
            self.rpc.call("getblockhash", json!([fork_height])).await?;
        sqlx::query(
            r#"
            INSERT INTO chain_state(id, height, blockhash)
            VALUES(TRUE, $1, decode($2, 'hex'))
            ON CONFLICT (id)
            DO UPDATE SET
              height = EXCLUDED.height,
              blockhash = EXCLUDED.blockhash,
              updated_at = now()
            "#,
        )
        .bind(fork_height)
        .bind(canonical_fork_hash)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        warn!(
            fork_height,
            tip_height = tip.0,
            "rollback complete; will re-apply canonical blocks"
        );
        Ok(())
    }

    async fn rollback_one_block(
        &self,
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        let mut holder_deltas: HashMap<(String, String), HolderDeltaAcc> = HashMap::new();
        let mut created_txids = Vec::new();
        let mut created_vouts = Vec::new();
        let created_rows =
            sqlx::query_as::<_, (String, i32, String, String, Option<String>, String)>(
                r#"
            SELECT
              encode(txid, 'hex') AS txid,
              vout,
              encode(category, 'hex') AS category,
              encode(locking_bytecode, 'hex') AS locking_bytecode,
              locking_address,
              COALESCE(ft_amount::text, '0') AS ft_amount
            FROM token_outpoints
            WHERE created_height = $1
            "#,
            )
            .bind(height)
            .fetch_all(&mut **tx)
            .await?;

        for (txid_hex, vout, category_hex, locking_bytecode_hex, locking_address, ft_amount) in
            created_rows
        {
            accumulate_holder_delta(
                &mut holder_deltas,
                HolderDeltaEvent {
                    category_hex,
                    locking_bytecode_hex,
                    locking_address,
                    ft_delta: negate_numeric(&ft_amount),
                    utxo_delta: -1,
                },
            );
            created_txids.push(txid_hex);
            created_vouts.push(vout);
        }

        self.delete_outpoints_batch(&created_txids, &created_vouts, tx)
            .await?;

        let mut spent_txids = Vec::new();
        let mut spent_vouts = Vec::new();
        let spent_rows =
            sqlx::query_as::<_, (String, i32, String, String, Option<String>, String)>(
                r#"
            SELECT
              encode(txid, 'hex') AS txid,
              vout,
              encode(category, 'hex') AS category,
              encode(locking_bytecode, 'hex') AS locking_bytecode,
              locking_address,
              COALESCE(ft_amount::text, '0') AS ft_amount
            FROM token_outpoints
            WHERE spent_height = $1
            "#,
            )
            .bind(height)
            .fetch_all(&mut **tx)
            .await?;

        for (txid_hex, vout, category_hex, locking_bytecode_hex, locking_address, ft_amount) in
            spent_rows
        {
            accumulate_holder_delta(
                &mut holder_deltas,
                HolderDeltaEvent {
                    category_hex,
                    locking_bytecode_hex,
                    locking_address,
                    ft_delta: ft_amount,
                    utxo_delta: 1,
                },
            );
            spent_txids.push(txid_hex);
            spent_vouts.push(vout);
        }

        self.unspend_outpoints_batch(&spent_txids, &spent_vouts, tx)
            .await?;

        self.metrics.observe_ingest_rows(
            spent_txids.len() as u64,
            created_txids.len() as u64,
            holder_deltas.len() as u64,
        );
        self.apply_holder_deltas_batch(height, &holder_deltas, tx)
            .await?;

        sqlx::query("DELETE FROM applied_blocks WHERE height = $1")
            .bind(height)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    async fn apply_block(&self, height: i32, block: &serde_json::Value) -> anyhow::Result<u64> {
        let mut tx = self.db.pool().begin().await?;
        let mut holder_deltas: HashMap<(String, String), HolderDeltaAcc> = HashMap::new();
        let mut spends = Vec::new();
        let mut credits = Vec::new();

        let block_txs = block["tx"]
            .as_array()
            .ok_or_else(|| anyhow!("block missing tx array"))?;

        for block_tx in block_txs {
            let txid_hex = block_tx["txid"]
                .as_str()
                .ok_or_else(|| anyhow!("tx missing txid"))?;

            if let Some(vins) = block_tx["vin"].as_array() {
                for vin in vins {
                    if vin.get("coinbase").is_some() {
                        continue;
                    }

                    let Some(prev_txid_hex) = vin["txid"].as_str() else {
                        continue;
                    };
                    let Some(prev_vout) = vin["vout"].as_u64() else {
                        continue;
                    };

                    spends.push(SpendInput {
                        prev_txid_hex: prev_txid_hex.to_string(),
                        prev_vout: prev_vout as i32,
                    });
                }
            }

            if let Some(vouts) = block_tx["vout"].as_array() {
                for vout in vouts {
                    let script_pub_key = vout.get("scriptPubKey");
                    let is_nulldata = script_pub_key
                        .and_then(|spk| spk.get("type"))
                        .and_then(|v| v.as_str())
                        .map(|t| t == "nulldata")
                        .unwrap_or(false);

                    if is_nulldata {
                        if let Some(op_return_asm) = script_pub_key
                            .and_then(|spk| spk.get("asm"))
                            .and_then(|v| v.as_str())
                        {
                            if let Some(parsed) = parse_bcmr_op_return(op_return_asm) {
                                let vout_n = vout["n"].as_u64().unwrap_or(0) as i32;
                                self.insert_bcmr_candidate(
                                    txid_hex,
                                    vout_n,
                                    height,
                                    op_return_asm,
                                    &parsed.claimed_hash_hex,
                                    &parsed.encoded_url_hex,
                                    &parsed.decoded_url,
                                    &mut tx,
                                )
                                .await
                                .with_context(|| {
                                    format!("failed inserting BCMR candidate {txid_hex}:{vout_n}")
                                })?;
                            }
                        }
                    }
                    let Some(token_data) = vout.get("tokenData") else {
                        continue;
                    };

                    if token_data.is_null() {
                        continue;
                    }

                    let Some(parsed) = parse_token_output(txid_hex, vout, token_data) else {
                        continue;
                    };

                    credits.push(parsed);
                }
            }
        }

        let spend_deltas = self
            .apply_spends_batch(&spends, height, &mut tx)
            .await
            .with_context(|| format!("failed applying block spends at height {height}"))?;
        let spend_delta_count = spend_deltas.len() as u64;
        for delta in spend_deltas {
            accumulate_holder_delta(&mut holder_deltas, delta);
        }

        let credit_deltas = self
            .apply_credits_batch(&credits, height, &mut tx)
            .await
            .with_context(|| format!("failed applying block credits at height {height}"))?;
        let credit_delta_count = credit_deltas.len() as u64;
        for delta in credit_deltas {
            accumulate_holder_delta(&mut holder_deltas, delta);
        }

        self.metrics.observe_ingest_rows(
            spend_delta_count,
            credit_delta_count,
            holder_deltas.len() as u64,
        );

        self.apply_holder_deltas_batch(height, &holder_deltas, &mut tx)
            .await?;

        sqlx::query(
            r#"
            INSERT INTO applied_blocks(height, hash, prev_hash)
            VALUES($1, decode($2, 'hex'), decode($3, 'hex'))
            ON CONFLICT (height) DO UPDATE
              SET hash = EXCLUDED.hash,
                  prev_hash = EXCLUDED.prev_hash,
                  applied_at = now()
            "#,
        )
        .bind(height)
        .bind(block["hash"].as_str().unwrap_or_default())
        .bind(block["previousblockhash"].as_str().unwrap_or_default())
        .execute(&mut *tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO chain_state(id, height, blockhash)
            VALUES(TRUE, $1, decode($2, 'hex'))
            ON CONFLICT (id) DO UPDATE
              SET height = EXCLUDED.height,
                  blockhash = EXCLUDED.blockhash,
                  updated_at = now()
            "#,
        )
        .bind(height)
        .bind(block["hash"].as_str().unwrap_or_default())
        .execute(&mut *tx)
        .await?;

        let commit_started = Instant::now();
        tx.commit().await?;
        Ok(commit_started.elapsed().as_millis() as u64)
    }

    async fn insert_bcmr_candidate(
        &self,
        txid_hex: &str,
        vout: i32,
        block_height: i32,
        op_return: &str,
        claimed_hash_hex: &str,
        encoded_url_hex: &str,
        decoded_url: &str,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO bcmr_candidates(
              txid,
              vout,
              block_height,
              op_return,
              claimed_hash_hex,
              encoded_url_hex,
              decoded_url,
              status,
              attempts,
              next_attempt_at
            )
            VALUES(
              decode($1, 'hex'),
              $2,
              $3,
              $4,
              $5,
              $6,
              $7,
              'pending',
              0,
              now()
            )
            ON CONFLICT (txid, vout) DO NOTHING
            "#,
        )
        .bind(txid_hex)
        .bind(vout)
        .bind(block_height)
        .bind(op_return)
        .bind(claimed_hash_hex)
        .bind(encoded_url_hex)
        .bind(decoded_url)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn run_consistency_check(&self, height: i32) -> anyhow::Result<()> {
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

        if mismatches > 0 {
            warn!(
                height,
                mismatches, "consistency check found token_stats drift"
            );
        } else {
            info!(height, "consistency check passed");
        }

        Ok(())
    }

    async fn apply_spends_batch(
        &self,
        spends: &[SpendInput],
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<Vec<HolderDeltaEvent>> {
        if spends.is_empty() {
            return Ok(Vec::new());
        }

        let prev_txid_hexes: Vec<String> = spends
            .iter()
            .map(|spend| spend.prev_txid_hex.clone())
            .collect();
        let prev_vouts: Vec<i32> = spends.iter().map(|spend| spend.prev_vout).collect();

        let spent_rows = sqlx::query_as::<_, (String, String, Option<String>, String)>(
            r#"
            WITH spend_inputs AS (
              SELECT
                decode(prev_txid_hex, 'hex') AS txid,
                prev_vout AS vout
              FROM UNNEST($1::text[], $2::int[]) AS t(prev_txid_hex, prev_vout)
            ),
            spent AS (
              UPDATE token_outpoints o
              SET spent_height = $3
              FROM spend_inputs i
              WHERE o.txid = i.txid
                AND o.vout = i.vout
                AND o.spent_height IS NULL
              RETURNING
                encode(o.category, 'hex') AS category_hex,
                encode(o.locking_bytecode, 'hex') AS locking_bytecode_hex,
                o.locking_address,
                COALESCE(o.ft_amount::text, '0') AS ft_amount
            )
            SELECT category_hex, locking_bytecode_hex, locking_address, ft_amount
            FROM spent
            "#,
        )
        .bind(&prev_txid_hexes)
        .bind(&prev_vouts)
        .bind(height)
        .fetch_all(&mut **tx)
        .await?;

        Ok(spent_rows
            .into_iter()
            .map(
                |(category_hex, locking_bytecode_hex, locking_address, ft_amount)| {
                    HolderDeltaEvent {
                        category_hex,
                        locking_bytecode_hex,
                        locking_address,
                        ft_delta: negate_numeric(&ft_amount),
                        utxo_delta: -1,
                    }
                },
            )
            .collect())
    }

    async fn apply_credits_batch(
        &self,
        credits: &[TokenOutput],
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<Vec<HolderDeltaEvent>> {
        if credits.is_empty() {
            return Ok(Vec::new());
        }

        let txid_hexes: Vec<String> = credits
            .iter()
            .map(|credit| credit.txid_hex.clone())
            .collect();
        let vouts: Vec<i32> = credits.iter().map(|credit| credit.vout).collect();
        let category_hexes: Vec<String> = credits
            .iter()
            .map(|credit| credit.category_hex.clone())
            .collect();
        let locking_bytecode_hexes: Vec<String> = credits
            .iter()
            .map(|credit| credit.locking_bytecode_hex.clone())
            .collect();
        let locking_addresses: Vec<Option<String>> = credits
            .iter()
            .map(|credit| credit.locking_address.clone())
            .collect();
        let ft_amounts: Vec<String> = credits
            .iter()
            .map(|credit| credit.ft_amount.clone())
            .collect();
        let nft_capabilities: Vec<Option<i16>> =
            credits.iter().map(|credit| credit.nft_capability).collect();
        let nft_commitment_hexes: Vec<Option<String>> = credits
            .iter()
            .map(|credit| credit.nft_commitment_hex.clone())
            .collect();
        let satoshis: Vec<i64> = credits.iter().map(|credit| credit.satoshis).collect();

        let inserted_rows = sqlx::query_as::<_, (String, String, Option<String>, String)>(
            r#"
            WITH credit_rows AS (
              SELECT
                decode(txid_hex, 'hex') AS txid,
                vout,
                decode(category_hex, 'hex') AS category,
                decode(locking_bytecode_hex, 'hex') AS locking_bytecode,
                locking_address,
                ft_amount::numeric AS ft_amount,
                nft_capability,
                CASE
                  WHEN nft_commitment_hex IS NULL THEN NULL
                  ELSE decode(nft_commitment_hex, 'hex')
                END AS nft_commitment,
                satoshis
              FROM UNNEST(
                $1::text[],
                $2::int[],
                $3::text[],
                $4::text[],
                $5::text[],
                $6::text[],
                $7::smallint[],
                $8::text[],
                $9::bigint[]
              ) AS t(
                txid_hex,
                vout,
                category_hex,
                locking_bytecode_hex,
                locking_address,
                ft_amount,
                nft_capability,
                nft_commitment_hex,
                satoshis
              )
            ),
            inserted AS (
              INSERT INTO token_outpoints(
                txid,
                vout,
                category,
                locking_bytecode,
                locking_address,
                ft_amount,
                nft_capability,
                nft_commitment,
                satoshis,
                created_height,
                spent_height
              )
              SELECT
                txid,
                vout,
                category,
                locking_bytecode,
                locking_address,
                ft_amount,
                nft_capability,
                nft_commitment,
                satoshis,
                $10,
                NULL
              FROM credit_rows
              ON CONFLICT (txid, vout) DO NOTHING
              RETURNING
                encode(category, 'hex') AS category_hex,
                encode(locking_bytecode, 'hex') AS locking_bytecode_hex,
                locking_address,
                COALESCE(ft_amount::text, '0') AS ft_amount
            )
            SELECT category_hex, locking_bytecode_hex, locking_address, ft_amount
            FROM inserted
            "#,
        )
        .bind(&txid_hexes)
        .bind(&vouts)
        .bind(&category_hexes)
        .bind(&locking_bytecode_hexes)
        .bind(&locking_addresses)
        .bind(&ft_amounts)
        .bind(&nft_capabilities)
        .bind(&nft_commitment_hexes)
        .bind(&satoshis)
        .bind(height)
        .fetch_all(&mut **tx)
        .await?;

        Ok(inserted_rows
            .into_iter()
            .map(
                |(category_hex, locking_bytecode_hex, locking_address, ft_amount)| {
                    HolderDeltaEvent {
                        category_hex,
                        locking_bytecode_hex,
                        locking_address,
                        ft_delta: ft_amount,
                        utxo_delta: 1,
                    }
                },
            )
            .collect())
    }

    async fn apply_holder_deltas_batch(
        &self,
        height: i32,
        holder_deltas: &HashMap<(String, String), HolderDeltaAcc>,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        if holder_deltas.is_empty() {
            return Ok(());
        }

        let mut category_hexes = Vec::with_capacity(holder_deltas.len());
        let mut locking_bytecode_hexes = Vec::with_capacity(holder_deltas.len());
        let mut locking_addresses = Vec::with_capacity(holder_deltas.len());
        let mut ft_deltas = Vec::with_capacity(holder_deltas.len());
        let mut utxo_deltas = Vec::with_capacity(holder_deltas.len());

        for ((category_hex, locking_bytecode_hex), acc) in holder_deltas {
            category_hexes.push(category_hex.clone());
            locking_bytecode_hexes.push(locking_bytecode_hex.clone());
            locking_addresses.push(acc.locking_address.clone());
            ft_deltas.push(acc.ft_delta.to_string());
            utxo_deltas.push(acc.utxo_delta);
        }

        sqlx::query(
            r#"
            WITH holder_deltas AS (
              SELECT
                decode(category_hex, 'hex') AS category,
                decode(locking_bytecode_hex, 'hex') AS locking_bytecode,
                locking_address,
                ft_delta::numeric AS ft_delta,
                utxo_delta
              FROM UNNEST(
                $1::text[],
                $2::text[],
                $3::text[],
                $4::text[],
                $5::int[]
              ) AS t(category_hex, locking_bytecode_hex, locking_address, ft_delta, utxo_delta)
            ),
            holder_agg AS (
              SELECT
                category,
                locking_bytecode,
                MAX(locking_address) FILTER (WHERE locking_address IS NOT NULL) AS locking_address,
                SUM(ft_delta)::numeric AS ft_delta,
                SUM(utxo_delta)::integer AS utxo_delta
              FROM holder_deltas
              GROUP BY 1, 2
            ),
            holder_transition AS (
              SELECT
                a.category,
                a.locking_bytecode,
                COALESCE(a.locking_address, h.locking_address) AS locking_address,
                a.ft_delta,
                a.utxo_delta,
                CASE WHEN COALESCE(h.ft_balance, 0) > 0 THEN 1 ELSE 0 END AS old_positive,
                CASE
                  WHEN GREATEST(COALESCE(h.ft_balance, 0) + a.ft_delta, 0) > 0 THEN 1
                  ELSE 0
                END AS new_positive
              FROM holder_agg a
              LEFT JOIN token_holders h
                ON h.category = a.category
               AND h.locking_bytecode = a.locking_bytecode
            ),
            updated_holders AS (
              UPDATE token_holders h
              SET
                locking_address = COALESCE(h.locking_address, t.locking_address),
                ft_balance = GREATEST(h.ft_balance + t.ft_delta, 0),
                utxo_count = GREATEST(h.utxo_count + t.utxo_delta, 0),
                updated_height = $6
              FROM holder_transition t
              WHERE h.category = t.category
                AND h.locking_bytecode = t.locking_bytecode
              RETURNING h.category, h.locking_bytecode
            ),
            inserted_holders AS (
              INSERT INTO token_holders(category, locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)
              SELECT
                t.category,
                t.locking_bytecode,
                t.locking_address,
                GREATEST(t.ft_delta, 0),
                GREATEST(t.utxo_delta, 0),
                $6
              FROM holder_transition t
              LEFT JOIN updated_holders u
                ON u.category = t.category
               AND u.locking_bytecode = t.locking_bytecode
              WHERE u.category IS NULL
              RETURNING category, locking_bytecode
            ),
            deleted_holders AS (
              DELETE FROM token_holders h
              USING holder_agg a
              WHERE h.category = a.category
                AND h.locking_bytecode = a.locking_bytecode
                AND h.ft_balance = 0
                AND h.utxo_count = 0
              RETURNING h.category
            ),
            category_delta AS (
              SELECT
                category,
                SUM(ft_delta)::numeric AS ft_delta,
                SUM(utxo_delta)::integer AS utxo_delta,
                SUM(new_positive - old_positive)::integer AS holder_delta
              FROM holder_transition
              GROUP BY category
            ),
            updated_stats AS (
              UPDATE token_stats s
              SET
                total_ft_supply = GREATEST(s.total_ft_supply + c.ft_delta, 0),
                holder_count = GREATEST(s.holder_count + c.holder_delta, 0),
                utxo_count = GREATEST(s.utxo_count + c.utxo_delta, 0),
                updated_height = $6,
                updated_at = now()
              FROM category_delta c
              WHERE s.category = c.category
              RETURNING s.category
            ),
            inserted_stats AS (
              INSERT INTO token_stats(category, total_ft_supply, holder_count, utxo_count, updated_height, updated_at)
              SELECT
                c.category,
                GREATEST(c.ft_delta, 0),
                GREATEST(c.holder_delta, 0),
                GREATEST(c.utxo_delta, 0),
                $6,
                now()
              FROM category_delta c
              LEFT JOIN updated_stats u
                ON u.category = c.category
              WHERE u.category IS NULL
              RETURNING category
            )
            DELETE FROM token_stats s
            USING category_delta c
            WHERE s.category = c.category
              AND s.total_ft_supply = 0
              AND s.holder_count = 0
              AND s.utxo_count = 0
            "#,
        )
        .bind(&category_hexes)
        .bind(&locking_bytecode_hexes)
        .bind(&locking_addresses)
        .bind(&ft_deltas)
        .bind(&utxo_deltas)
        .bind(height)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn delete_outpoints_batch(
        &self,
        txid_hexes: &[String],
        vouts: &[i32],
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        if txid_hexes.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            DELETE FROM token_outpoints o
            USING (
              SELECT
                decode(txid_hex, 'hex') AS txid,
                vout
              FROM UNNEST($1::text[], $2::int[]) AS t(txid_hex, vout)
            ) d
            WHERE o.txid = d.txid
              AND o.vout = d.vout
            "#,
        )
        .bind(txid_hexes)
        .bind(vouts)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn unspend_outpoints_batch(
        &self,
        txid_hexes: &[String],
        vouts: &[i32],
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        if txid_hexes.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE token_outpoints o
            SET spent_height = NULL
            FROM (
              SELECT
                decode(txid_hex, 'hex') AS txid,
                vout
              FROM UNNEST($1::text[], $2::int[]) AS t(txid_hex, vout)
            ) u
            WHERE o.txid = u.txid
              AND o.vout = u.vout
            "#,
        )
        .bind(txid_hexes)
        .bind(vouts)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}

fn parse_token_output(
    txid_hex: &str,
    vout: &serde_json::Value,
    token_data: &serde_json::Value,
) -> Option<TokenOutput> {
    let vout_n = vout["n"].as_u64()? as i32;
    let category_hex = token_data["category"].as_str()?.to_ascii_lowercase();
    let ft_amount = token_data
        .get("amount")
        .and_then(|v| v.as_str())
        .unwrap_or("0")
        .to_string();

    let script_pub_key = vout.get("scriptPubKey")?;
    let locking_bytecode_hex = script_pub_key.get("hex")?.as_str()?.to_ascii_lowercase();

    let locking_address = script_pub_key
        .get("address")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            script_pub_key
                .get("addresses")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        });

    let (nft_capability, nft_commitment_hex) = parse_nft(token_data.get("nft"));

    let satoshis = parse_satoshis(vout.get("value")).unwrap_or(0);

    Some(TokenOutput {
        txid_hex: txid_hex.to_ascii_lowercase(),
        vout: vout_n,
        category_hex,
        locking_bytecode_hex,
        locking_address,
        ft_amount,
        nft_capability,
        nft_commitment_hex,
        satoshis,
    })
}

fn parse_nft(nft: Option<&serde_json::Value>) -> (Option<i16>, Option<String>) {
    let Some(nft) = nft else {
        return (None, None);
    };

    let capability = nft
        .get("capability")
        .and_then(|v| v.as_str())
        .map(|cap| match cap {
            "none" => 0,
            "mutable" => 1,
            "minting" => 2,
            _ => 0,
        });

    let commitment = nft
        .get("commitment")
        .and_then(|v| v.as_str())
        .map(|s| s.to_ascii_lowercase())
        .filter(|s| !s.is_empty());

    (capability, commitment)
}

fn parse_satoshis(value: Option<&serde_json::Value>) -> Option<i64> {
    let value = value?;
    let bch = value.as_f64()?;
    Some((bch * 100_000_000.0).round() as i64)
}

fn accumulate_holder_delta(
    deltas: &mut HashMap<(String, String), HolderDeltaAcc>,
    delta: HolderDeltaEvent,
) {
    let key = (
        delta.category_hex.clone(),
        delta.locking_bytecode_hex.clone(),
    );
    let entry = deltas.entry(key).or_default();
    if entry.locking_address.is_none() {
        entry.locking_address = delta.locking_address;
    }
    entry.ft_delta += parse_bigint(&delta.ft_delta);
    entry.utxo_delta += delta.utxo_delta;
}

fn parse_bigint(value: &str) -> BigInt {
    value.parse::<BigInt>().unwrap_or_else(|_| BigInt::zero())
}

fn negate_numeric(num: &str) -> String {
    if num.starts_with('-') {
        num.trim_start_matches('-').to_string()
    } else if num == "0" {
        "0".to_string()
    } else {
        format!("-{num}")
    }
}
