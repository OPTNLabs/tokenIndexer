use anyhow::{anyhow, Context};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::net::IpAddr;
use tokio::net::lookup_host;
use tokio::time::{sleep, Duration};
use tracing::{info, warn};
use url::Url;

use crate::bcmr::opreturn::{decode_hex_ascii, parse_bcmr_op_return};
use crate::config::Config;
use crate::db::Database;
use crate::ingest::RpcClient;

#[derive(Clone)]
pub struct BcmrWorker {
    config: Config,
    db: Database,
    rpc: RpcClient,
    http: reqwest::Client,
}

#[derive(sqlx::FromRow, Debug)]
struct CandidateRow {
    id: i64,
    txid_hex: String,
    vout: i32,
    block_height: i32,
    op_return: String,
    claimed_hash_hex: Option<String>,
    encoded_url_hex: Option<String>,
    decoded_url: Option<String>,
    attempts: i32,
}

#[derive(sqlx::FromRow, Debug)]
struct CategoryProbeRow {
    category_hex: String,
}

struct FetchResult {
    final_url: String,
    status: u16,
    body: Option<Vec<u8>>,
}

impl BcmrWorker {
    pub fn new(config: Config, db: Database) -> anyhow::Result<Self> {
        let rpc = RpcClient::new(
            config.rpc_url.clone(),
            config.rpc_user.clone(),
            config.rpc_pass.clone(),
            config.rpc_timeout_ms,
            config.rpc_retries,
            config.rpc_retry_backoff_ms,
        )?;

        let http = reqwest::Client::builder()
            .connect_timeout(Duration::from_millis(
                config.bcmr_http_timeout_ms.min(5_000),
            ))
            .timeout(Duration::from_millis(config.bcmr_http_timeout_ms))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .context("failed to build BCMR HTTP client")?;

        Ok(Self {
            config,
            db,
            rpc,
            http,
        })
    }

    pub async fn run(self) -> anyhow::Result<()> {
        info!("starting BCMR resolver worker");
        self.requeue_processing_candidates().await?;
        if let Err(err) = self.probe_category_authbase_candidates_once().await {
            warn!(error = ?err, "initial BCMR category authbase probe failed");
        }
        if self.config.bcmr_backfill_enabled {
            if let Err(err) = self.backfill_candidates().await {
                warn!(error = ?err, "BCMR backfill failed");
            }
        }
        let mut probe_tick: u32 = 0;
        loop {
            if probe_tick == 0 {
                if let Err(err) = self.probe_category_authbase_candidates_once().await {
                    warn!(error = ?err, "periodic BCMR category authbase probe failed");
                }
            }
            if let Err(err) = self.resolve_once().await {
                warn!(error = ?err, "BCMR resolve cycle failed");
            }
            probe_tick = (probe_tick + 1) % 20;
            sleep(Duration::from_millis(self.config.bcmr_poll_ms)).await;
        }
    }

    async fn probe_category_authbase_candidates_once(&self) -> anyhow::Result<()> {
        let rows = sqlx::query_as::<_, CategoryProbeRow>(
            r#"
            SELECT encode(s.category, 'hex') AS category_hex
            FROM token_stats s
            LEFT JOIN bcmr_category_metadata m
              ON m.category = s.category
            LEFT JOIN bcmr_category_checks c
              ON c.category = s.category
            WHERE m.category IS NULL
              AND (
                c.category IS NULL
                OR c.last_result = 'error'
                OR c.last_checked_at < now() - interval '24 hours'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM bcmr_registries r
                WHERE r.category = s.category
                   OR r.authbase = s.category
              )
            ORDER BY
              c.last_checked_at NULLS FIRST,
              s.updated_height DESC,
              s.category ASC
            LIMIT $1
            "#,
        )
        .bind(self.config.bcmr_batch_size.max(1))
        .fetch_all(self.db.pool())
        .await?;

        let mut checked = 0_u64;
        let mut candidates_added = 0_u64;
        let mut no_candidate = 0_u64;
        let mut errors = 0_u64;

        for row in rows {
            checked += 1;
            match self.scan_authbase_tx_for_bcmr(&row.category_hex).await {
                Ok(found) if found > 0 => {
                    candidates_added += found;
                    self.record_category_check(&row.category_hex, "candidate_found", None, true)
                        .await?;
                }
                Ok(_) => {
                    no_candidate += 1;
                    self.record_category_check(&row.category_hex, "no_candidate", None, false)
                        .await?;
                }
                Err(err) => {
                    errors += 1;
                    self.record_category_check(
                        &row.category_hex,
                        "error",
                        Some(err.to_string()),
                        false,
                    )
                    .await?;
                }
            }
        }

        if checked > 0 {
            info!(
                checked_categories = checked,
                inserted_candidates = candidates_added,
                no_candidate,
                errors,
                "completed BCMR category authbase probe batch"
            );
        }
        Ok(())
    }

    async fn requeue_processing_candidates(&self) -> anyhow::Result<()> {
        let requeued = sqlx::query(
            r#"
            UPDATE bcmr_candidates
            SET status = 'pending',
                next_attempt_at = now()
            WHERE status = 'processing'
            "#,
        )
        .execute(self.db.pool())
        .await?
        .rows_affected();

        if requeued > 0 {
            info!(requeued, "re-queued stale BCMR processing candidates");
        }
        Ok(())
    }

    async fn backfill_candidates(&self) -> anyhow::Result<()> {
        let tip: i64 = self.rpc.call("getblockcount", json!([])).await?;
        let from = self.config.bcmr_backfill_from_height.max(0);
        let to = if self.config.bcmr_backfill_to_height <= 0 {
            tip as i32
        } else {
            self.config.bcmr_backfill_to_height.min(tip as i32)
        };
        if from > to {
            return Ok(());
        }

        let batch_blocks = self.config.bcmr_backfill_batch_blocks.max(1) as usize;
        info!(
            from_height = from,
            to_height = to,
            batch_blocks = batch_blocks,
            "starting BCMR backfill scan"
        );

        let mut inserted_total: u64 = 0;
        let mut scanned_total: u64 = 0;
        let mut start = from;
        let rpc_batch_size = self.config.rpc_batch_size.max(1) as i32;
        while start <= to {
            let end = (start + batch_blocks as i32 - 1).min(to);
            let mut chunk_start = start;
            while chunk_start <= end {
                let chunk_end = (chunk_start + rpc_batch_size - 1).min(end);
                let heights: Vec<i32> = (chunk_start..=chunk_end).collect();

                let hash_params: Vec<Value> = heights.iter().map(|h| json!([h])).collect();
                let hashes: Vec<String> = self.rpc.call_batch("getblockhash", hash_params).await?;
                let block_params: Vec<Value> = hashes.iter().map(|h| json!([h, 2])).collect();
                let blocks: Vec<Value> = self.rpc.call_batch("getblock", block_params).await?;

                for (idx, block) in blocks.iter().enumerate() {
                    let height = heights[idx];
                    scanned_total += 1;
                    inserted_total += self.scan_block_for_bcmr(height, block).await?;
                }
                chunk_start = chunk_end + 1;
            }

            start = end + 1;
        }

        info!(
            scanned_blocks = scanned_total,
            inserted_candidates = inserted_total,
            "completed BCMR backfill scan"
        );
        Ok(())
    }

    async fn scan_block_for_bcmr(&self, height: i32, block: &Value) -> anyhow::Result<u64> {
        let mut inserted = 0_u64;
        let Some(txs) = block.get("tx").and_then(|v| v.as_array()) else {
            return Ok(0);
        };

        for tx in txs {
            let Some(txid_hex) = tx.get("txid").and_then(|v| v.as_str()) else {
                continue;
            };
            let Some(vouts) = tx.get("vout").and_then(|v| v.as_array()) else {
                continue;
            };
            for vout in vouts {
                let script_pub_key = vout.get("scriptPubKey");
                let is_nulldata = script_pub_key
                    .and_then(|spk| spk.get("type"))
                    .and_then(|v| v.as_str())
                    .map(|t| t == "nulldata")
                    .unwrap_or(false);
                if !is_nulldata {
                    continue;
                }
                let Some(op_return_asm) = script_pub_key
                    .and_then(|spk| spk.get("asm"))
                    .and_then(|v| v.as_str())
                else {
                    continue;
                };
                let Some(parsed) = parse_bcmr_op_return(op_return_asm) else {
                    continue;
                };
                let vout_n = vout.get("n").and_then(|v| v.as_u64()).unwrap_or(0) as i32;
                if self
                    .insert_bcmr_candidate(
                        txid_hex,
                        vout_n,
                        height,
                        op_return_asm,
                        &parsed.claimed_hash_hex,
                        &parsed.encoded_url_hex,
                        &parsed.decoded_url,
                    )
                    .await?
                {
                    inserted += 1;
                }
            }
        }

        Ok(inserted)
    }

    async fn scan_authbase_tx_for_bcmr(&self, category_hex: &str) -> anyhow::Result<u64> {
        let tx: Value = self
            .rpc
            .call("getrawtransaction", json!([category_hex, true]))
            .await
            .with_context(|| "failed getrawtransaction for category authbase probe")?;

        let block_height = tx
            .get("blockheight")
            .and_then(|v| v.as_i64())
            .unwrap_or_default() as i32;
        let Some(vouts) = tx.get("vout").and_then(|v| v.as_array()) else {
            return Ok(0);
        };

        let mut inserted = 0_u64;
        for vout in vouts {
            let script_pub_key = vout.get("scriptPubKey");
            let is_nulldata = script_pub_key
                .and_then(|spk| spk.get("type"))
                .and_then(|v| v.as_str())
                .map(|t| t == "nulldata")
                .unwrap_or(false);
            if !is_nulldata {
                continue;
            }

            let Some(op_return_asm) = script_pub_key
                .and_then(|spk| spk.get("asm"))
                .and_then(|v| v.as_str())
            else {
                continue;
            };
            let Some(parsed) = parse_bcmr_op_return(op_return_asm) else {
                continue;
            };
            let vout_n = vout.get("n").and_then(|v| v.as_u64()).unwrap_or(0) as i32;
            if self
                .insert_bcmr_candidate(
                    category_hex,
                    vout_n,
                    block_height,
                    op_return_asm,
                    &parsed.claimed_hash_hex,
                    &parsed.encoded_url_hex,
                    &parsed.decoded_url,
                )
                .await?
            {
                inserted += 1;
            }
        }
        Ok(inserted)
    }

    async fn resolve_once(&self) -> anyhow::Result<()> {
        let rows = sqlx::query_as::<_, CandidateRow>(
            r#"
            WITH picked AS (
              SELECT id
              FROM bcmr_candidates
              WHERE status IN ('pending','retry')
                AND next_attempt_at <= now()
              ORDER BY id
              LIMIT $1
              FOR UPDATE SKIP LOCKED
            )
            UPDATE bcmr_candidates c
            SET status = 'processing',
                attempts = attempts + 1,
                last_error = NULL
            FROM picked
            WHERE c.id = picked.id
            RETURNING
              c.id,
              encode(c.txid, 'hex') AS txid_hex,
              c.vout,
              c.block_height,
              c.op_return,
              c.claimed_hash_hex,
              c.encoded_url_hex,
              c.decoded_url,
              c.attempts
            "#,
        )
        .bind(self.config.bcmr_batch_size)
        .fetch_all(self.db.pool())
        .await?;

        for row in rows {
            if let Err(err) = self.process_candidate(&row).await {
                self.mark_retry_or_failed(row.id, row.attempts, &err.to_string())
                    .await?;
            }
        }

        Ok(())
    }

    async fn process_candidate(&self, row: &CandidateRow) -> anyhow::Result<()> {
        let parsed = parse_bcmr_op_return(&row.op_return)
            .ok_or_else(|| anyhow!("invalid BCMR OP_RETURN shape"))?;

        let claimed_hash_hex = row
            .claimed_hash_hex
            .clone()
            .unwrap_or_else(|| parsed.claimed_hash_hex.clone());
        let encoded_url_hex = row
            .encoded_url_hex
            .clone()
            .unwrap_or_else(|| parsed.encoded_url_hex.clone());
        let decoded_url = row
            .decoded_url
            .clone()
            .unwrap_or_else(|| parsed.decoded_url.clone());

        let authbase = self.find_authbase(&row.txid_hex).await?;
        let fetch = self.fetch_registry(&decoded_url).await?;

        let mut validity = json!({
            "bcmr_file_accessible": fetch.status == 200,
            "bcmr_hash_match": false,
            "bcmr_format_valid": false,
            "identities_match": null
        });

        let mut content_hash_hex: Option<String> = None;
        let mut latest_revision: Option<DateTime<Utc>> = None;
        let mut contents: Option<Value> = None;

        if let Some(body) = fetch.body.as_ref() {
            let hash = sha256_hex(body);
            content_hash_hex = Some(hash.clone());

            let decoded_claimed_hash = decode_hex_ascii(&claimed_hash_hex)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let hash_match = hash == claimed_hash_hex || hash == decoded_claimed_hash;
            validity["bcmr_hash_match"] = Value::Bool(hash_match);

            if let Ok(parsed_json) = serde_json::from_slice::<Value>(body) {
                let format_valid = is_bcmr_v2_like(&parsed_json);
                validity["bcmr_format_valid"] = Value::Bool(format_valid);
                if let Some(authbase_hex) = authbase.as_deref() {
                    validity["identities_match"] =
                        Value::Bool(identities_match_for_category(&parsed_json, authbase_hex));
                }

                // Persist parsed JSON even for non-v2 variants, then project only
                // identities matching this token category in upsert_category_metadata.
                latest_revision = parsed_json
                    .get("latestRevision")
                    .and_then(|v| v.as_str())
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                    .map(|dt| dt.with_timezone(&Utc));
                contents = Some(parsed_json);
            }
        }

        let registry_id = self
            .upsert_registry(
                row,
                authbase.as_deref(),
                &fetch,
                &claimed_hash_hex,
                content_hash_hex.as_deref(),
                &validity,
                latest_revision,
                contents.as_ref(),
            )
            .await?;

        if let (Some(authbase_hex), Some(json)) = (authbase.as_deref(), contents.as_ref()) {
            self.upsert_category_metadata(
                authbase_hex,
                registry_id,
                row.block_height,
                json,
                latest_revision,
            )
            .await?;
        }

        sqlx::query(
            r#"
            UPDATE bcmr_candidates
            SET
              claimed_hash_hex = $2,
              encoded_url_hex = $3,
              decoded_url = $4,
              authbase = CASE WHEN $5 IS NULL THEN authbase ELSE decode($5, 'hex') END,
              category = CASE WHEN $5 IS NULL THEN category ELSE decode($5, 'hex') END,
              registry_id = $6,
              status = 'resolved',
              resolved_at = now(),
              last_error = NULL
            WHERE id = $1
            "#,
        )
        .bind(row.id)
        .bind(claimed_hash_hex)
        .bind(encoded_url_hex)
        .bind(decoded_url)
        .bind(authbase)
        .bind(registry_id)
        .execute(self.db.pool())
        .await?;

        Ok(())
    }

    async fn find_authbase(&self, start_txid_hex: &str) -> anyhow::Result<Option<String>> {
        let mut current = start_txid_hex.to_ascii_lowercase();

        for _ in 0..self.config.bcmr_max_authchain_depth {
            let category_exists: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM token_stats WHERE category = decode($1, 'hex'))",
            )
            .bind(&current)
            .fetch_one(self.db.pool())
            .await?;

            if category_exists {
                return Ok(Some(current));
            }

            let tx: Value = self
                .rpc
                .call("getrawtransaction", json!([current, true]))
                .await
                .with_context(|| "failed getrawtransaction while traversing authchain")?;

            let Some(vins) = tx.get("vin").and_then(|v| v.as_array()) else {
                return Ok(None);
            };

            let parent = vins.iter().find_map(|vin| {
                let out_index = vin.get("vout").and_then(|v| v.as_u64())?;
                if out_index != 0 {
                    return None;
                }
                vin.get("txid").and_then(|v| v.as_str()).map(str::to_string)
            });

            let Some(parent_txid) = parent else {
                return Ok(None);
            };
            current = parent_txid.to_ascii_lowercase();
        }

        Ok(None)
    }

    async fn fetch_registry(&self, url: &str) -> anyhow::Result<FetchResult> {
        if let Some(cid) = url.strip_prefix("ipfs://") {
            let mut attempted = 0_u32;
            for gateway in &self.config.bcmr_ipfs_gateways {
                let Some(base) = normalize_ipfs_gateway_base(gateway) else {
                    continue;
                };
                let final_url = format!("{base}{cid}");
                if !is_safe_public_url(&final_url) {
                    continue;
                }
                let Ok(parsed) = Url::parse(&final_url) else {
                    continue;
                };
                if !is_safe_public_url_resolved(&parsed).await {
                    continue;
                }
                attempted += 1;
                if let Ok(response) = self.http.get(&final_url).send().await {
                    let status = response.status().as_u16();
                    if status == 200 {
                        let body = read_limited_body(response, self.config.bcmr_max_response_bytes)
                            .await?;
                        return Ok(FetchResult {
                            final_url,
                            status,
                            body: Some(body),
                        });
                    }
                }
            }
            if attempted == 0 {
                return Err(anyhow!(
                    "ipfs source detected but no usable/safe ipfs gateways configured"
                ));
            }
            return Ok(FetchResult {
                final_url: url.to_string(),
                status: 599,
                body: None,
            });
        }

        if !is_safe_public_url(url) {
            return Err(anyhow!("unsafe bcmr url blocked"));
        }
        let parsed = Url::parse(url).map_err(|_| anyhow!("invalid bcmr url"))?;
        if !is_safe_public_url_resolved(&parsed).await {
            return Err(anyhow!("unsafe bcmr url blocked"));
        }

        let response = self.http.get(url).send().await?;
        let status = response.status().as_u16();
        let body = if status == 200 {
            Some(read_limited_body(response, self.config.bcmr_max_response_bytes).await?)
        } else {
            None
        };
        Ok(FetchResult {
            final_url: url.to_string(),
            status,
            body,
        })
    }

    async fn upsert_registry(
        &self,
        row: &CandidateRow,
        authbase_hex: Option<&str>,
        fetch: &FetchResult,
        claimed_hash_hex: &str,
        content_hash_hex: Option<&str>,
        validity: &Value,
        latest_revision: Option<DateTime<Utc>>,
        contents: Option<&Value>,
    ) -> anyhow::Result<i64> {
        let registry_id: i64 = sqlx::query_scalar(
            r#"
            INSERT INTO bcmr_registries(
              txid,
              vout,
              category,
              authbase,
              source_url,
              content_hash_hex,
              claimed_hash_hex,
              request_status,
              validity_checks,
              contents,
              latest_revision,
              fetched_at
            )
            VALUES(
              decode($1, 'hex'),
              $2,
              CASE WHEN $3 IS NULL THEN NULL ELSE decode($3, 'hex') END,
              CASE WHEN $3 IS NULL THEN NULL ELSE decode($3, 'hex') END,
              $4,
              $5,
              $6,
              $7,
              $8::jsonb,
              $9::jsonb,
              $10,
              now()
            )
            ON CONFLICT (txid, vout)
            DO UPDATE SET
              category = EXCLUDED.category,
              authbase = EXCLUDED.authbase,
              source_url = EXCLUDED.source_url,
              content_hash_hex = EXCLUDED.content_hash_hex,
              claimed_hash_hex = EXCLUDED.claimed_hash_hex,
              request_status = EXCLUDED.request_status,
              validity_checks = EXCLUDED.validity_checks,
              contents = EXCLUDED.contents,
              latest_revision = EXCLUDED.latest_revision,
              fetched_at = now()
            RETURNING id
            "#,
        )
        .bind(&row.txid_hex)
        .bind(row.vout)
        .bind(authbase_hex)
        .bind(&fetch.final_url)
        .bind(content_hash_hex)
        .bind(claimed_hash_hex)
        .bind(fetch.status as i32)
        .bind(validity)
        .bind(contents)
        .bind(latest_revision)
        .fetch_one(self.db.pool())
        .await?;

        Ok(registry_id)
    }

    async fn upsert_category_metadata(
        &self,
        category_hex: &str,
        registry_id: i64,
        updated_height: i32,
        contents: &Value,
        latest_revision: Option<DateTime<Utc>>,
    ) -> anyhow::Result<()> {
        let snapshot = extract_latest_identity_snapshot_for_category(contents, category_hex);

        let (symbol, name, description, decimals, icon_uri, token_uri, nft_types) =
            if let Some(snapshot) = snapshot.as_ref() {
                let token = snapshot.get("token");
                let symbol = token
                    .and_then(|t| t.get("symbol"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let decimals = token
                    .and_then(|t| t.get("decimals").or_else(|| t.get("decimal")))
                    .and_then(|v| v.as_i64())
                    .map(|v| v as i32);
                let name = snapshot
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let description = snapshot
                    .get("description")
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let icon_uri = snapshot
                    .get("uris")
                    .and_then(|v| v.get("icon"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let token_uri = snapshot
                    .get("uris")
                    .and_then(|v| v.get("token"))
                    .and_then(|v| v.as_str())
                    .map(str::to_string);
                let nft_types = token
                    .and_then(|t| t.get("nfts"))
                    .and_then(|n| n.get("parse"))
                    .and_then(|p| p.get("types"))
                    .cloned();

                (
                    symbol,
                    name,
                    description,
                    decimals,
                    icon_uri,
                    token_uri,
                    nft_types,
                )
            } else {
                (None, None, None, None, None, None, None)
            };

        sqlx::query(
            r#"
            INSERT INTO bcmr_category_metadata(
              category,
              registry_id,
              symbol,
              name,
              description,
              decimals,
              icon_uri,
              token_uri,
              latest_revision,
              identity_snapshot,
              nft_types,
              updated_height,
              updated_at
            )
            VALUES(
              decode($1, 'hex'),
              $2,
              $3,
              $4,
              $5,
              $6,
              $7,
              $8,
              $9,
              $10::jsonb,
              $11::jsonb,
              $12,
              now()
            )
            ON CONFLICT (category)
            DO UPDATE SET
              registry_id = EXCLUDED.registry_id,
              symbol = EXCLUDED.symbol,
              name = EXCLUDED.name,
              description = EXCLUDED.description,
              decimals = EXCLUDED.decimals,
              icon_uri = EXCLUDED.icon_uri,
              token_uri = EXCLUDED.token_uri,
              latest_revision = EXCLUDED.latest_revision,
              identity_snapshot = EXCLUDED.identity_snapshot,
              nft_types = EXCLUDED.nft_types,
              updated_height = EXCLUDED.updated_height,
              updated_at = now()
            "#,
        )
        .bind(category_hex)
        .bind(registry_id)
        .bind(symbol)
        .bind(name)
        .bind(description)
        .bind(decimals)
        .bind(icon_uri)
        .bind(token_uri)
        .bind(latest_revision)
        .bind(snapshot)
        .bind(nft_types)
        .bind(updated_height)
        .execute(self.db.pool())
        .await?;

        Ok(())
    }

    async fn mark_retry_or_failed(
        &self,
        candidate_id: i64,
        attempts: i32,
        err: &str,
    ) -> anyhow::Result<()> {
        let max_attempts = self.config.bcmr_max_attempts;
        if attempts >= max_attempts {
            sqlx::query("UPDATE bcmr_candidates SET status='failed', last_error=$2 WHERE id=$1")
                .bind(candidate_id)
                .bind(truncate_error(err))
                .execute(self.db.pool())
                .await?;
            return Ok(());
        }

        let backoff_secs =
            (self.config.bcmr_retry_backoff_secs as i32).saturating_mul(attempts.max(1));

        sqlx::query(
            r#"
            UPDATE bcmr_candidates
            SET
              status='retry',
              last_error=$2,
              next_attempt_at=now() + ($3::text || ' seconds')::interval
            WHERE id=$1
            "#,
        )
        .bind(candidate_id)
        .bind(truncate_error(err))
        .bind(backoff_secs)
        .execute(self.db.pool())
        .await?;

        Ok(())
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
    ) -> anyhow::Result<bool> {
        let result = sqlx::query(
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
        .execute(self.db.pool())
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn record_category_check(
        &self,
        category_hex: &str,
        last_result: &str,
        err: Option<String>,
        has_candidate: bool,
    ) -> anyhow::Result<()> {
        let candidate_vout = if has_candidate { Some(0_i32) } else { None };
        sqlx::query(
            r#"
            INSERT INTO bcmr_category_checks(
              category,
              last_checked_at,
              last_result,
              last_error,
              candidate_txid,
              candidate_vout
            )
            VALUES(
              decode($1, 'hex'),
              now(),
              $2,
              $3,
              CASE WHEN $4 THEN decode($1, 'hex') ELSE NULL END,
              $5
            )
            ON CONFLICT (category)
            DO UPDATE SET
              last_checked_at = now(),
              last_result = EXCLUDED.last_result,
              last_error = EXCLUDED.last_error,
              candidate_txid = EXCLUDED.candidate_txid,
              candidate_vout = EXCLUDED.candidate_vout
            "#,
        )
        .bind(category_hex)
        .bind(last_result)
        .bind(err.map(|e| truncate_error(&e)))
        .bind(has_candidate)
        .bind(candidate_vout)
        .execute(self.db.pool())
        .await?;
        Ok(())
    }
}

fn normalize_ipfs_gateway_base(input: &str) -> Option<String> {
    let mut base = input.trim().to_string();
    if base.is_empty() {
        return None;
    }

    if !base.starts_with("http://") && !base.starts_with("https://") {
        base = format!("https://{base}");
    }

    if !base.ends_with('/') {
        base.push('/');
    }

    if !base.contains("/ipfs/") {
        base.push_str("ipfs/");
    } else if !base.ends_with("/ipfs/") {
        let normalized = base.trim_end_matches('/');
        if let Some(idx) = normalized.find("/ipfs") {
            base = format!("{}/ipfs/", &normalized[..idx]);
        }
    }

    Some(base)
}

fn is_safe_public_url(input: &str) -> bool {
    let Ok(url) = Url::parse(input) else {
        return false;
    };

    match url.scheme() {
        // Keep transport secure for remote fetches.
        "https" => {}
        _ => return false,
    }

    let Some(host) = url.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".local") {
        return false;
    }

    if let Ok(ip) = host.parse::<IpAddr>() {
        return !is_private_or_local_ip(ip);
    }

    true
}

async fn is_safe_public_url_resolved(url: &Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };

    if let Ok(ip) = host.parse::<IpAddr>() {
        return !is_private_or_local_ip(ip);
    }

    let Some(port) = url.port_or_known_default() else {
        return false;
    };
    let Ok(addresses) = lookup_host((host, port)).await else {
        return false;
    };

    let mut saw_address = false;
    for addr in addresses {
        saw_address = true;
        if is_private_or_local_ip(addr.ip()) {
            return false;
        }
    }
    saw_address
}

async fn read_limited_body(
    response: reqwest::Response,
    max_bytes: usize,
) -> anyhow::Result<Vec<u8>> {
    if let Some(content_length) = response.content_length() {
        if content_length as usize > max_bytes {
            return Err(anyhow!(
                "bcmr response too large: {} > {} bytes",
                content_length,
                max_bytes
            ));
        }
    }

    let mut body = Vec::new();
    let mut stream = response.bytes_stream();
    while let Some(next) = stream.next().await {
        let chunk = next?;
        if body.len().saturating_add(chunk.len()) > max_bytes {
            return Err(anyhow!("bcmr response exceeded {} bytes", max_bytes));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn is_private_or_local_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_multicast()
                || v4.is_unspecified()
        }
        IpAddr::V6(v6) => {
            v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || v6.is_unique_local()
                || v6.is_unicast_link_local()
        }
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn truncate_error(err: &str) -> String {
    const MAX: usize = 2_000;
    if err.len() <= MAX {
        err.to_string()
    } else {
        err[..MAX].to_string()
    }
}

fn is_bcmr_v2_like(value: &Value) -> bool {
    let Some(identities) = value.get("identities").and_then(|v| v.as_object()) else {
        return false;
    };
    if identities.is_empty() {
        return false;
    }

    // Accept any schema version as long as it carries non-empty identities.
    // Some compatible registries still publish version.major = 0.
    true
}

fn identities_match_for_category(value: &Value, category_hex: &str) -> bool {
    let Some(identities) = value.get("identities").and_then(|v| v.as_object()) else {
        return false;
    };

    if identities
        .keys()
        .any(|key| key.eq_ignore_ascii_case(category_hex))
    {
        return true;
    }

    extract_latest_identity_snapshot_for_category(value, category_hex).is_some()
}

fn extract_latest_identity_snapshot_for_category(
    contents: &Value,
    category_hex: &str,
) -> Option<Value> {
    let identities = contents.get("identities")?.as_object()?;
    let mut best: Option<(DateTime<Utc>, Value)> = None;

    for history in identities.values() {
        let Some(history_map) = history.as_object() else {
            continue;
        };

        for (timestamp, snapshot) in history_map {
            let Some(token_category) = snapshot
                .get("token")
                .and_then(|t| t.get("category"))
                .and_then(|v| v.as_str())
            else {
                continue;
            };

            if !token_category.eq_ignore_ascii_case(category_hex) {
                continue;
            }

            let parsed_time = chrono::DateTime::parse_from_rfc3339(timestamp)
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(|_| Utc::now());

            match &best {
                Some((best_time, _)) if parsed_time <= *best_time => {}
                _ => best = Some((parsed_time, snapshot.clone())),
            }
        }
    }

    best.map(|(_, v)| v)
}
