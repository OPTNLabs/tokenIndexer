use anyhow::{anyhow, Context};
use serde_json::json;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};

use crate::config::Config;
use crate::db::Database;

use super::rpc::RpcClient;

#[derive(Clone)]
pub struct IngestWorker {
    config: Config,
    db: Database,
    rpc: RpcClient,
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

impl IngestWorker {
    pub fn new(config: Config, db: Database) -> Self {
        let rpc = RpcClient::new(
            config.rpc_url.clone(),
            config.rpc_user.clone(),
            config.rpc_pass.clone(),
        );
        Self { config, db, rpc }
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
        let mut state_height = self.current_height().await?;

        if state_height < self.config.bootstrap_height {
            state_height = self.config.bootstrap_height;
        }

        if state_height >= chain_height as i32 {
            sleep(Duration::from_millis(600)).await;
            return Ok(());
        }

        let next_height = state_height + 1;
        let block_hash: String = self
            .rpc
            .call("getblockhash", json!([next_height]))
            .await
            .context("getblockhash failed")?;

        let block: serde_json::Value = self
            .rpc
            .call("getblock", json!([block_hash, 3]))
            .await
            .context("getblock verbosity=3 failed")?;

        if self.is_parent_mismatch(next_height, &block).await? {
            self.handle_reorg(next_height).await?;
            return Ok(());
        }

        self.apply_block(next_height, &block).await?;
        if next_height % 100 == 0 {
            self.run_consistency_check(next_height).await?;
        }
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

    async fn is_parent_mismatch(&self, next_height: i32, block: &serde_json::Value) -> anyhow::Result<bool> {
        let Some(expected_prev) = block["previousblockhash"].as_str() else {
            return Ok(false);
        };

        let local: Option<(i32, String)> =
            sqlx::query_as("SELECT height, encode(blockhash, 'hex') FROM chain_state WHERE id = TRUE")
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
        warn!(
            incoming_height,
            reorg_window = self.config.reorg_window,
            "parent mismatch detected; beginning rollback scan"
        );

        let tip: (i32, String) =
            sqlx::query_as("SELECT height, encode(blockhash, 'hex') FROM chain_state WHERE id = TRUE")
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
        }

        let canonical_fork_hash: String = self.rpc.call("getblockhash", json!([fork_height])).await?;
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

        warn!(fork_height, tip_height = tip.0, "rollback complete; will re-apply canonical blocks");
        Ok(())
    }

    async fn rollback_one_block(
        &self,
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        let created_rows = sqlx::query_as::<_, (String, i32, String, String, Option<String>, String)>(
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

        for (txid_hex, vout, category_hex, locking_bytecode_hex, locking_address, ft_amount) in created_rows {
            self.apply_holder_delta(
                &category_hex,
                &locking_bytecode_hex,
                locking_address.as_deref(),
                &negate_numeric(&ft_amount),
                -1,
                height,
                tx,
            )
            .await?;

            sqlx::query("DELETE FROM token_outpoints WHERE txid = decode($1, 'hex') AND vout = $2")
                .bind(txid_hex)
                .bind(vout)
                .execute(&mut **tx)
                .await?;
        }

        let spent_rows = sqlx::query_as::<_, (String, i32, String, String, Option<String>, String)>(
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

        for (txid_hex, vout, category_hex, locking_bytecode_hex, locking_address, ft_amount) in spent_rows {
            sqlx::query(
                "UPDATE token_outpoints SET spent_height = NULL WHERE txid = decode($1, 'hex') AND vout = $2",
            )
            .bind(&txid_hex)
            .bind(vout)
            .execute(&mut **tx)
            .await?;

            self.apply_holder_delta(
                &category_hex,
                &locking_bytecode_hex,
                locking_address.as_deref(),
                &ft_amount,
                1,
                height,
                tx,
            )
            .await?;
        }

        sqlx::query("DELETE FROM applied_blocks WHERE height = $1")
            .bind(height)
            .execute(&mut **tx)
            .await?;

        Ok(())
    }

    async fn apply_block(&self, height: i32, block: &serde_json::Value) -> anyhow::Result<()> {
        let mut tx = self.db.pool().begin().await?;

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

                    self.apply_spend(prev_txid_hex, prev_vout as i32, height, &mut tx)
                        .await
                        .with_context(|| {
                            format!(
                                "failed applying spend at height {height} for input {prev_txid_hex}:{prev_vout} (spending tx {txid_hex})"
                            )
                        })?;
                }
            }

            if let Some(vouts) = block_tx["vout"].as_array() {
                for vout in vouts {
                    let Some(token_data) = vout.get("tokenData") else {
                        continue;
                    };

                    if token_data.is_null() {
                        continue;
                    }

                    let Some(parsed) = parse_token_output(txid_hex, vout, token_data) else {
                        continue;
                    };

                    self.apply_credit(parsed, height, &mut tx)
                        .await
                        .with_context(|| format!("failed applying token output at height {height}"))?;
                }
            }
        }

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

        tx.commit().await?;

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
            )
            SELECT COUNT(*)::bigint
            FROM token_stats s
            LEFT JOIN holder_rollup h
              ON h.category = s.category
            WHERE s.total_ft_supply <> COALESCE(h.total_ft_supply, 0)
               OR s.holder_count <> COALESCE(h.holder_count, 0)
               OR s.utxo_count <> COALESCE(h.utxo_count, 0)
            "#,
        )
        .fetch_one(self.db.pool())
        .await?;

        if mismatches > 0 {
            warn!(height, mismatches, "consistency check found token_stats drift");
        } else {
            info!(height, "consistency check passed");
        }

        Ok(())
    }

    async fn apply_spend(
        &self,
        prev_txid_hex: &str,
        prev_vout: i32,
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        let spent = sqlx::query_as::<_, (String, String, Option<String>, Option<String>)>(
            r#"
            UPDATE token_outpoints
            SET spent_height = $3
            WHERE txid = decode($1, 'hex')
              AND vout = $2
              AND spent_height IS NULL
            RETURNING
              encode(category, 'hex') AS category,
              encode(locking_bytecode, 'hex') AS locking_bytecode,
              locking_address,
              COALESCE(ft_amount::text, '0') AS ft_amount
            "#,
        )
        .bind(prev_txid_hex)
        .bind(prev_vout)
        .bind(height)
        .fetch_optional(&mut **tx)
        .await?;

        let Some((category_hex, locking_bytecode_hex, locking_address, ft_amount_opt)) = spent else {
            return Ok(());
        };

        let ft_amount = ft_amount_opt.unwrap_or_else(|| "0".to_string());

        self.apply_holder_delta(
            &category_hex,
            &locking_bytecode_hex,
            locking_address.as_deref(),
            &negate_numeric(&ft_amount),
            -1,
            height,
            tx,
        )
        .await?;

        Ok(())
    }

    async fn apply_credit(
        &self,
        output: TokenOutput,
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
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
            VALUES(
              decode($1, 'hex'),
              $2,
              decode($3, 'hex'),
              decode($4, 'hex'),
              $5,
              $6::numeric,
              $7,
              CASE WHEN $8 IS NULL THEN NULL ELSE decode($8, 'hex') END,
              $9,
              $10,
              NULL
            )
            ON CONFLICT (txid, vout) DO NOTHING
            "#,
        )
        .bind(&output.txid_hex)
        .bind(output.vout)
        .bind(&output.category_hex)
        .bind(&output.locking_bytecode_hex)
        .bind(&output.locking_address)
        .bind(&output.ft_amount)
        .bind(output.nft_capability)
        .bind(&output.nft_commitment_hex)
        .bind(output.satoshis)
        .bind(height)
        .execute(&mut **tx)
        .await?;

        self.apply_holder_delta(
            &output.category_hex,
            &output.locking_bytecode_hex,
            output.locking_address.as_deref(),
            &output.ft_amount,
            1,
            height,
            tx,
        )
        .await?;

        Ok(())
    }

    async fn apply_holder_delta(
        &self,
        category_hex: &str,
        locking_bytecode_hex: &str,
        locking_address: Option<&str>,
        ft_delta: &str,
        utxo_delta: i32,
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        let old_balance: Option<String> = sqlx::query_scalar(
            r#"
            SELECT ft_balance::text
            FROM token_holders
            WHERE category = decode($1, 'hex')
              AND locking_bytecode = decode($2, 'hex')
            FOR UPDATE
            "#,
        )
        .bind(category_hex)
        .bind(locking_bytecode_hex)
        .fetch_optional(&mut **tx)
        .await?;

        let new_balance: String = sqlx::query_scalar(
            r#"
            INSERT INTO token_holders(category, locking_bytecode, locking_address, ft_balance, utxo_count, updated_height)
            VALUES(decode($1, 'hex'), decode($2, 'hex'), $3, $4::numeric, GREATEST($5, 0), $6)
            ON CONFLICT (category, locking_bytecode)
            DO UPDATE SET
              locking_address = COALESCE(token_holders.locking_address, EXCLUDED.locking_address),
              ft_balance = GREATEST(token_holders.ft_balance + EXCLUDED.ft_balance, 0),
              utxo_count = GREATEST(token_holders.utxo_count + $5, 0),
              updated_height = $6
            RETURNING ft_balance::text
            "#,
        )
        .bind(category_hex)
        .bind(locking_bytecode_hex)
        .bind(locking_address)
        .bind(ft_delta)
        .bind(utxo_delta)
        .bind(height)
        .fetch_one(&mut **tx)
        .await?;

        let old_positive = old_balance.as_deref().map(is_positive_numeric).unwrap_or(false);
        let new_positive = is_positive_numeric(&new_balance);
        let holder_delta = match (old_positive, new_positive) {
            (false, true) => 1,
            (true, false) => -1,
            _ => 0,
        };

        self.bump_stats(category_hex, ft_delta, utxo_delta, holder_delta, height, tx)
            .await?;

        sqlx::query(
            r#"
            DELETE FROM token_holders
            WHERE category = decode($1, 'hex')
              AND locking_bytecode = decode($2, 'hex')
              AND ft_balance = 0
              AND utxo_count = 0
            "#,
        )
        .bind(category_hex)
        .bind(locking_bytecode_hex)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn bump_stats(
        &self,
        category_hex: &str,
        ft_delta: &str,
        utxo_delta: i32,
        holder_delta: i32,
        height: i32,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO token_stats(category, total_ft_supply, holder_count, utxo_count, updated_height, updated_at)
            VALUES(decode($1, 'hex'), $2::numeric, 0, GREATEST($3, 0), $4, now())
            ON CONFLICT (category)
            DO UPDATE SET
              total_ft_supply = GREATEST(token_stats.total_ft_supply + $2::numeric, 0),
              utxo_count = GREATEST(token_stats.utxo_count + $3, 0),
              updated_height = $4,
              updated_at = now()
            "#,
        )
        .bind(category_hex)
        .bind(ft_delta)
        .bind(utxo_delta)
        .bind(height)
        .execute(&mut **tx)
        .await?;

        if holder_delta != 0 {
            sqlx::query(
                r#"
                UPDATE token_stats
                SET
                  holder_count = GREATEST(holder_count + $2, 0),
                  updated_height = $3,
                  updated_at = now()
                WHERE category = decode($1, 'hex')
                "#,
            )
            .bind(category_hex)
            .bind(holder_delta)
            .bind(height)
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }
}

fn parse_token_output(txid_hex: &str, vout: &serde_json::Value, token_data: &serde_json::Value) -> Option<TokenOutput> {
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

fn is_positive_numeric(num: &str) -> bool {
    num.trim() != "0"
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
