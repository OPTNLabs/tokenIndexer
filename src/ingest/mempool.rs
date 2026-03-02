use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Context;
use chrono::Utc;
use num_bigint::BigInt;
use num_traits::Zero;
use serde_json::json;
use tokio::sync::RwLock;
use tokio::time::{Duration, sleep};
use tracing::{info, warn};

use crate::config::Config;
use crate::db::Database;
use crate::model::{MempoolCategoryView, MempoolHolderDelta, MempoolSnapshot};

use super::rpc::RpcClient;

#[derive(Clone)]
pub struct MempoolWorker {
    config: Config,
    db: Database,
    rpc: RpcClient,
    snapshot: Arc<RwLock<MempoolSnapshot>>,
}

#[derive(Debug, Clone)]
struct ParsedTokenOutput {
    outpoint: String,
    category: String,
    locking_bytecode: String,
    locking_address: Option<String>,
    ft_amount: BigInt,
    has_nft: bool,
}

#[derive(Debug, Clone)]
struct ParsedTx {
    txid: String,
    inputs: Vec<(String, i32)>,
    outputs: Vec<ParsedTokenOutput>,
}

#[derive(Debug, Clone)]
struct OutpointTokenRef {
    category: String,
    locking_bytecode: String,
    locking_address: Option<String>,
    ft_amount: BigInt,
}

#[derive(Debug, Default)]
struct CategoryAcc {
    tx_count: u32,
    ft_credits: BigInt,
    ft_debits: BigInt,
    utxo_delta: i64,
    nft_count: u32,
    touched_txs: HashSet<String>,
    holders: HashMap<String, HolderAcc>,
}

#[derive(Debug, Default)]
struct HolderAcc {
    locking_address: Option<String>,
    ft_delta: BigInt,
    utxo_delta: i32,
}

impl MempoolWorker {
    pub fn new(config: Config, db: Database, snapshot: Arc<RwLock<MempoolSnapshot>>) -> Self {
        let rpc = RpcClient::new(
            config.rpc_url.clone(),
            config.rpc_user.clone(),
            config.rpc_pass.clone(),
        );
        Self {
            config,
            db,
            rpc,
            snapshot,
        }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!("starting mempool worker");
        loop {
            if let Err(err) = self.refresh_once().await {
                warn!(error = ?err, "mempool refresh failed; retrying");
            }
            sleep(Duration::from_millis(self.config.mempool_poll_ms)).await;
        }
    }

    async fn refresh_once(&self) -> anyhow::Result<()> {
        let mut txids: Vec<String> = self.rpc.call("getrawmempool", json!([false])).await?;
        let total_mempool_txs = txids.len() as u32;
        if txids.len() > self.config.mempool_max_txs {
            txids.truncate(self.config.mempool_max_txs);
        }

        let mut parsed_txs = Vec::with_capacity(txids.len());
        let mut mempool_outpoints: HashMap<String, OutpointTokenRef> = HashMap::new();
        for txid in &txids {
            let tx: serde_json::Value = self
                .rpc
                .call("getrawtransaction", json!([txid, true]))
                .await
                .with_context(|| format!("getrawtransaction failed for mempool tx {txid}"))?;
            let parsed = parse_tx(&tx)?;
            for output in &parsed.outputs {
                mempool_outpoints.insert(
                    output.outpoint.clone(),
                    OutpointTokenRef {
                        category: output.category.clone(),
                        locking_bytecode: output.locking_bytecode.clone(),
                        locking_address: output.locking_address.clone(),
                        ft_amount: output.ft_amount.clone(),
                    },
                );
            }
            parsed_txs.push(parsed);
        }

        let mut category_acc: HashMap<String, CategoryAcc> = HashMap::new();
        let mut prevout_cache: HashMap<String, Option<OutpointTokenRef>> = HashMap::new();

        for parsed in &parsed_txs {
            for output in &parsed.outputs {
                let acc = category_acc
                    .entry(output.category.clone())
                    .or_default();
                if acc.touched_txs.insert(parsed.txid.clone()) {
                    acc.tx_count += 1;
                }
                acc.ft_credits += &output.ft_amount;
                acc.utxo_delta += 1;
                if output.has_nft {
                    acc.nft_count += 1;
                }
                let holder = acc
                    .holders
                    .entry(output.locking_bytecode.clone())
                    .or_default();
                holder.locking_address = holder.locking_address.clone().or(output.locking_address.clone());
                holder.ft_delta += &output.ft_amount;
                holder.utxo_delta += 1;
            }

            for (prev_txid, prev_vout) in &parsed.inputs {
                let outpoint_key = format!("{prev_txid}:{prev_vout}");
                let outpoint = if let Some(value) = mempool_outpoints.get(&outpoint_key) {
                    Some(value.clone())
                } else if let Some(value) = prevout_cache.get(&outpoint_key) {
                    value.clone()
                } else {
                    let fetched = self.fetch_confirmed_token_outpoint(prev_txid, *prev_vout).await?;
                    prevout_cache.insert(outpoint_key.clone(), fetched.clone());
                    fetched
                };

                let Some(outpoint) = outpoint else {
                    continue;
                };

                let acc = category_acc
                    .entry(outpoint.category.clone())
                    .or_default();
                if acc.touched_txs.insert(parsed.txid.clone()) {
                    acc.tx_count += 1;
                }
                acc.ft_debits += &outpoint.ft_amount;
                acc.utxo_delta -= 1;
                let holder = acc
                    .holders
                    .entry(outpoint.locking_bytecode.clone())
                    .or_default();
                holder.locking_address = holder.locking_address.clone().or(outpoint.locking_address.clone());
                holder.ft_delta -= &outpoint.ft_amount;
                holder.utxo_delta -= 1;
            }
        }

        let mut snapshot = MempoolSnapshot {
            updated_at: Utc::now(),
            total_mempool_txs,
            scanned_txs: parsed_txs.len() as u32,
            categories: HashMap::new(),
        };

        for (category, acc) in category_acc {
            let mut holders = HashMap::with_capacity(acc.holders.len());
            for (locking_bytecode, holder) in acc.holders {
                holders.insert(
                    locking_bytecode.clone(),
                    MempoolHolderDelta {
                        locking_bytecode,
                        locking_address: holder.locking_address,
                        ft_delta: holder.ft_delta.to_string(),
                        utxo_delta: holder.utxo_delta,
                    },
                );
            }

            snapshot.categories.insert(
                category,
                MempoolCategoryView {
                    tx_count: acc.tx_count,
                    ft_credits: acc.ft_credits.to_string(),
                    ft_debits: acc.ft_debits.to_string(),
                    net_ft_delta: (acc.ft_credits - acc.ft_debits).to_string(),
                    utxo_delta: acc.utxo_delta,
                    nft_count: acc.nft_count,
                    holders,
                },
            );
        }

        *self.snapshot.write().await = snapshot;
        Ok(())
    }

    async fn fetch_confirmed_token_outpoint(
        &self,
        txid_hex: &str,
        vout: i32,
    ) -> anyhow::Result<Option<OutpointTokenRef>> {
        let row = sqlx::query_as::<_, (String, String, Option<String>, String)>(
            r#"
            SELECT
              encode(category, 'hex') AS category,
              encode(locking_bytecode, 'hex') AS locking_bytecode,
              locking_address,
              COALESCE(ft_amount::text, '0') AS ft_amount
            FROM token_outpoints
            WHERE txid = decode($1, 'hex')
              AND vout = $2
              AND spent_height IS NULL
            "#,
        )
        .bind(txid_hex)
        .bind(vout)
        .fetch_optional(self.db.pool())
        .await?;

        Ok(row.map(|(category, locking_bytecode, locking_address, ft_amount)| OutpointTokenRef {
            category,
            locking_bytecode,
            locking_address,
            ft_amount: parse_bigint(&ft_amount),
        }))
    }
}

fn parse_tx(tx: &serde_json::Value) -> anyhow::Result<ParsedTx> {
    let txid = tx["txid"]
        .as_str()
        .context("tx missing txid")?
        .to_ascii_lowercase();

    let mut inputs = Vec::new();
    if let Some(vins) = tx["vin"].as_array() {
        for vin in vins {
            if vin.get("coinbase").is_some() {
                continue;
            }
            let Some(prev_txid) = vin["txid"].as_str() else {
                continue;
            };
            let Some(vout) = vin["vout"].as_u64() else {
                continue;
            };
            inputs.push((prev_txid.to_ascii_lowercase(), vout as i32));
        }
    }

    let mut outputs = Vec::new();
    if let Some(vouts) = tx["vout"].as_array() {
        for vout in vouts {
            let Some(token_data) = vout.get("tokenData") else {
                continue;
            };
            if token_data.is_null() {
                continue;
            }
            let Some(category) = token_data.get("category").and_then(|v| v.as_str()) else {
                continue;
            };
            let amount = token_data
                .get("amount")
                .and_then(|v| v.as_str())
                .unwrap_or("0");
            let n = vout["n"].as_u64().unwrap_or(0);
            let script_pub_key = vout.get("scriptPubKey").unwrap_or(&serde_json::Value::Null);
            let locking_bytecode = script_pub_key
                .get("hex")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_ascii_lowercase();
            if locking_bytecode.is_empty() {
                continue;
            }
            let locking_address = script_pub_key
                .get("address")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string())
                .or_else(|| {
                    script_pub_key
                        .get("addresses")
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.first())
                        .and_then(|v| v.as_str())
                        .map(|v| v.to_string())
                });
            outputs.push(ParsedTokenOutput {
                outpoint: format!("{txid}:{n}"),
                category: category.to_ascii_lowercase(),
                locking_bytecode,
                locking_address,
                ft_amount: parse_bigint(amount),
                has_nft: token_data.get("nft").is_some(),
            });
        }
    }

    Ok(ParsedTx {
        txid,
        inputs,
        outputs,
    })
}

fn parse_bigint(value: &str) -> BigInt {
    value.parse::<BigInt>().unwrap_or_else(|_| BigInt::zero())
}

