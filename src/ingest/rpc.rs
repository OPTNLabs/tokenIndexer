use anyhow::Context;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct RpcClient {
    client: Client,
    url: String,
    user: String,
    pass: String,
    retries: u32,
    retry_backoff_ms: u64,
}

impl RpcClient {
    pub fn new(
        url: String,
        user: String,
        pass: String,
        timeout_ms: u64,
        retries: u32,
        retry_backoff_ms: u64,
    ) -> anyhow::Result<Self> {
        let client = Client::builder()
            .connect_timeout(Duration::from_millis(timeout_ms.min(5_000)))
            .timeout(Duration::from_millis(timeout_ms))
            .build()
            .context("failed to build RPC HTTP client")?;
        Ok(Self {
            client,
            url,
            user,
            pass,
            retries,
            retry_backoff_ms,
        })
    }

    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> anyhow::Result<T> {
        let mut attempt = 0_u32;
        let mut last_error: Option<anyhow::Error> = None;
        while attempt <= self.retries {
            match self.call_once(method, params.clone()).await {
                Ok(result) => {
                    return serde_json::from_value(result).context("failed to decode RPC result")
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt == self.retries {
                        break;
                    }
                    let backoff = self.retry_backoff_ms.saturating_mul((attempt + 1) as u64);
                    sleep(Duration::from_millis(backoff)).await;
                    attempt += 1;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("unknown RPC error")))
    }

    pub async fn call_batch<T: DeserializeOwned>(
        &self,
        method: &str,
        params_list: Vec<Value>,
    ) -> anyhow::Result<Vec<T>> {
        if params_list.is_empty() {
            return Ok(Vec::new());
        }

        let mut attempt = 0_u32;
        let mut last_error: Option<anyhow::Error> = None;
        while attempt <= self.retries {
            match self.call_batch_once(method, &params_list).await {
                Ok(results) => {
                    let mut decoded = Vec::with_capacity(results.len());
                    for result in results {
                        decoded.push(
                            serde_json::from_value(result)
                                .context("failed to decode RPC result")?,
                        );
                    }
                    return Ok(decoded);
                }
                Err(err) => {
                    last_error = Some(err);
                    if attempt == self.retries {
                        break;
                    }
                    let backoff = self.retry_backoff_ms.saturating_mul((attempt + 1) as u64);
                    sleep(Duration::from_millis(backoff)).await;
                    attempt += 1;
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("unknown RPC batch error")))
    }

    async fn call_once(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let payload = json!({
            "jsonrpc": "1.0",
            "id": "tokenindex",
            "method": method,
            "params": params,
        });

        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("RPC call failed for method {method}"))?;

        let status = resp.status();
        let body: Value = resp.json().await.context("invalid RPC JSON response")?;

        if !status.is_success() {
            anyhow::bail!("RPC http error {status}: {body}");
        }

        if !body["error"].is_null() {
            anyhow::bail!("RPC returned error: {}", body["error"]);
        }

        Ok(body["result"].clone())
    }

    async fn call_batch_once(
        &self,
        method: &str,
        params_list: &[Value],
    ) -> anyhow::Result<Vec<Value>> {
        let mut payload = Vec::with_capacity(params_list.len());
        for (idx, params) in params_list.iter().enumerate() {
            payload.push(json!({
                "jsonrpc": "1.0",
                "id": idx,
                "method": method,
                "params": params,
            }));
        }

        let resp = self
            .client
            .post(&self.url)
            .basic_auth(&self.user, Some(&self.pass))
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("RPC batch call failed for method {method}"))?;

        let status = resp.status();
        let body: Value = resp
            .json()
            .await
            .context("invalid RPC JSON batch response")?;

        if !status.is_success() {
            anyhow::bail!("RPC http error {status}: {body}");
        }

        let responses = body
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("RPC batch response is not an array"))?;

        let mut ordered: Vec<Option<Value>> = vec![None; params_list.len()];
        for item in responses {
            if !item["error"].is_null() {
                anyhow::bail!("RPC returned batch error: {}", item["error"]);
            }
            let Some(id) = item["id"].as_u64() else {
                continue;
            };
            let idx = id as usize;
            if idx >= ordered.len() {
                continue;
            }
            ordered[idx] = Some(item["result"].clone());
        }

        let mut out = Vec::with_capacity(ordered.len());
        for (idx, item) in ordered.into_iter().enumerate() {
            let value =
                item.ok_or_else(|| anyhow::anyhow!("missing RPC batch response for index {idx}"))?;
            out.push(value);
        }
        Ok(out)
    }
}
