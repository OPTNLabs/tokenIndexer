use anyhow::Context;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

#[derive(Clone)]
pub struct RpcClient {
    client: Client,
    url: String,
    user: String,
    pass: String,
}

impl RpcClient {
    pub fn new(url: String, user: String, pass: String) -> Self {
        Self {
            client: Client::new(),
            url,
            user,
            pass,
        }
    }

    pub async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> anyhow::Result<T> {
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

        serde_json::from_value(body["result"].clone()).context("failed to decode RPC result")
    }
}
