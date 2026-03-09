use std::collections::BTreeMap;

use axum::http::{header, HeaderMap, HeaderValue, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug)]
pub struct CachedResponse {
    pub status: StatusCode,
    pub body: Value,
    pub etag: String,
    pub updated_height: i32,
    pub max_age_secs: i32,
    pub cached_at_epoch_secs: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RedisCacheRecord {
    pub status: u16,
    pub body: Value,
    pub etag: String,
    pub updated_height: i32,
    pub max_age_secs: i32,
    pub cached_at_epoch_secs: u64,
}

impl CachedResponse {
    pub fn new(
        status: StatusCode,
        body: Value,
        cache_key: &str,
        updated_height: i32,
        dataset_version: &str,
        max_age_secs: i32,
    ) -> Self {
        let etag = compute_etag(cache_key, updated_height, dataset_version, &body);
        let cached_at_epoch_secs = chrono::Utc::now().timestamp().max(0) as u64;
        Self {
            status,
            body,
            etag,
            updated_height,
            max_age_secs,
            cached_at_epoch_secs,
        }
    }

    pub fn to_redis_record(&self) -> RedisCacheRecord {
        RedisCacheRecord {
            status: self.status.as_u16(),
            body: self.body.clone(),
            etag: self.etag.clone(),
            updated_height: self.updated_height,
            max_age_secs: self.max_age_secs,
            cached_at_epoch_secs: self.cached_at_epoch_secs,
        }
    }

    pub fn from_redis_record(record: RedisCacheRecord) -> Option<Self> {
        let status = StatusCode::from_u16(record.status).ok()?;
        Some(Self {
            status,
            body: record.body,
            etag: record.etag,
            updated_height: record.updated_height,
            max_age_secs: record.max_age_secs,
            cached_at_epoch_secs: record.cached_at_epoch_secs,
        })
    }

    pub fn age_secs(&self) -> u64 {
        let now = chrono::Utc::now().timestamp().max(0) as u64;
        now.saturating_sub(self.cached_at_epoch_secs)
    }

    pub fn headers(&self, stale: bool) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ETAG,
            HeaderValue::from_str(&self.etag).unwrap_or(HeaderValue::from_static("\"invalid\"")),
        );
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_str(&format!(
                "public, max-age={}, stale-while-revalidate=30",
                self.max_age_secs
            ))
            .unwrap_or(HeaderValue::from_static("public, max-age=5")),
        );
        if stale {
            headers.insert(
                header::WARNING,
                HeaderValue::from_static("110 - \"response is stale\""),
            );
        }
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        headers
    }
}

pub fn compute_etag(
    cache_key: &str,
    updated_height: i32,
    dataset_version: &str,
    body: &Value,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(cache_key.as_bytes());
    hasher.update(updated_height.to_be_bytes());
    hasher.update(dataset_version.as_bytes());

    // Use deterministic JSON ordering for stable tag material.
    let normalized = canonical_json(body);
    hasher.update(normalized.as_bytes());

    let digest = hasher.finalize();
    format!("\"{:x}\"", digest)
}

fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut sorted = BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k, canonical_json(v));
            }
            let mut parts = Vec::with_capacity(sorted.len());
            for (k, v) in sorted {
                parts.push(format!("\"{k}\":{v}"));
            }
            format!("{{{}}}", parts.join(","))
        }
        Value::Array(values) => {
            let items: Vec<String> = values.iter().map(canonical_json).collect();
            format!("[{}]", items.join(","))
        }
        _ => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{compute_etag, CachedResponse};
    use axum::http::StatusCode;
    use serde_json::json;

    #[test]
    fn etag_is_stable_for_reordered_json() {
        let a = json!({"b":1,"a":2});
        let b = json!({"a":2,"b":1});
        let etag_a = compute_etag("k", 1, "v1", &a);
        let etag_b = compute_etag("k", 1, "v1", &b);
        assert_eq!(etag_a, etag_b);
    }

    #[test]
    fn redis_roundtrip_restores_cached_response() {
        let cached = CachedResponse::new(StatusCode::OK, json!({"ok":true}), "k", 99, "v1", 10);
        let roundtrip = CachedResponse::from_redis_record(cached.to_redis_record())
            .expect("redis record should roundtrip");
        assert_eq!(roundtrip.status, StatusCode::OK);
        assert_eq!(roundtrip.updated_height, 99);
        assert_eq!(roundtrip.max_age_secs, 10);
        assert_eq!(roundtrip.body["ok"], true);
    }
}
