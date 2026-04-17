use clap::Parser;
use std::env;
use url::Url;

#[derive(Debug, Clone, Parser)]
#[command(name = "tokenindex")]
pub struct Config {
    #[arg(long, env = "TOKENINDEX_API_HOST", default_value = "0.0.0.0")]
    pub api_host: String,

    #[arg(long, env = "TOKENINDEX_API_PORT", default_value_t = 8080)]
    pub api_port: u16,

    #[arg(long, env = "TOKENINDEX_MAINNET_API_HOST")]
    pub mainnet_api_host: Option<String>,

    #[arg(long, env = "TOKENINDEX_MAINNET_API_PORT", default_value_t = 8081)]
    pub mainnet_api_port: u16,

    #[arg(long, env = "TOKENINDEX_MAINNET_DATABASE_URL")]
    pub mainnet_database_url: Option<String>,

    #[arg(long, env = "TOKENINDEX_MAINNET_DATABASE_READ_URL")]
    pub mainnet_database_read_url: Option<String>,

    #[arg(long, env = "TOKENINDEX_MAINNET_DB_SCHEMA")]
    pub mainnet_db_schema: Option<String>,

    #[arg(
        long,
        env = "TOKENINDEX_DATABASE_URL",
        default_value = "postgres://tokenindex:tokenindex@127.0.0.1:5432/tokenindex"
    )]
    pub database_url: String,

    #[arg(long, env = "TOKENINDEX_DATABASE_READ_URL")]
    pub database_read_url: Option<String>,

    #[arg(long, env = "TOKENINDEX_DB_SCHEMA", default_value = "chipnet")]
    pub db_schema: String,

    #[arg(long, env = "TOKENINDEX_DB_MAX_CONNECTIONS", default_value_t = 30)]
    pub db_max_connections: u32,

    #[arg(
        long,
        env = "TOKENINDEX_DB_STATEMENT_TIMEOUT_MS",
        default_value_t = 5000
    )]
    pub db_statement_timeout_ms: u64,

    #[arg(long, env = "TOKENINDEX_DB_API_MAX_CONNECTIONS", default_value_t = 0)]
    pub db_api_max_connections: u32,

    #[arg(
        long,
        env = "TOKENINDEX_DB_INGEST_MAX_CONNECTIONS",
        default_value_t = 0
    )]
    pub db_ingest_max_connections: u32,

    #[arg(long, env = "TOKENINDEX_REDIS_URL")]
    pub redis_url: Option<String>,

    #[arg(
        long,
        env = "TOKENINDEX_RPC_URL",
        default_value = "http://127.0.0.1:48334"
    )]
    pub rpc_url: String,

    #[arg(long, env = "TOKENINDEX_RPC_USER")]
    pub rpc_user: String,

    #[arg(long, env = "TOKENINDEX_RPC_PASS")]
    pub rpc_pass: String,

    #[arg(long, env = "TOKENINDEX_MAINNET_RPC_URL")]
    pub mainnet_rpc_url: Option<String>,

    #[arg(long, env = "TOKENINDEX_MAINNET_RPC_USER")]
    pub mainnet_rpc_user: Option<String>,

    #[arg(long, env = "TOKENINDEX_MAINNET_RPC_PASS")]
    pub mainnet_rpc_pass: Option<String>,

    #[arg(long, env = "TOKENINDEX_MAINNET_BOOTSTRAP_HEIGHT")]
    pub mainnet_bootstrap_height: Option<i32>,

    #[arg(long, env = "TOKENINDEX_EXPECTED_CHAIN", default_value = "chip")]
    pub expected_chain: String,

    #[arg(long, env = "TOKENINDEX_ZMQ_BLOCK_ENDPOINT")]
    pub zmq_block_endpoint: Option<String>,

    #[arg(long, env = "TOKENINDEX_BOOTSTRAP_HEIGHT", default_value_t = 0)]
    pub bootstrap_height: i32,

    #[arg(long, env = "TOKENINDEX_REORG_WINDOW", default_value_t = 20)]
    pub reorg_window: i32,

    #[arg(long, env = "TOKENINDEX_MEMPOOL_ENABLED", default_value_t = true)]
    pub mempool_enabled: bool,

    #[arg(long, env = "TOKENINDEX_MEMPOOL_POLL_MS", default_value_t = 1500)]
    pub mempool_poll_ms: u64,

    #[arg(long, env = "TOKENINDEX_MEMPOOL_MAX_TXS", default_value_t = 50_000)]
    pub mempool_max_txs: usize,

    #[arg(long, env = "TOKENINDEX_BCMR_ENABLED", default_value_t = true)]
    pub bcmr_enabled: bool,

    #[arg(long, env = "TOKENINDEX_BCMR_POLL_MS", default_value_t = 1500)]
    pub bcmr_poll_ms: u64,

    #[arg(long, env = "TOKENINDEX_BCMR_BATCH_SIZE", default_value_t = 32)]
    pub bcmr_batch_size: i64,

    #[arg(long, env = "TOKENINDEX_BCMR_MAX_ATTEMPTS", default_value_t = 5)]
    pub bcmr_max_attempts: i32,

    #[arg(long, env = "TOKENINDEX_BCMR_RETRY_BACKOFF_SECS", default_value_t = 30)]
    pub bcmr_retry_backoff_secs: u64,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_MAX_AUTHCHAIN_DEPTH",
        default_value_t = 512
    )]
    pub bcmr_max_authchain_depth: usize,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_HTTP_TIMEOUT_MS",
        default_value_t = 15_000
    )]
    pub bcmr_http_timeout_ms: u64,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_MAX_RESPONSE_BYTES",
        default_value_t = 1_000_000
    )]
    pub bcmr_max_response_bytes: usize,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_IPFS_GATEWAYS",
        value_delimiter = ',',
        default_value = "https://ipfs.optnlabs.com/ipfs/,https://ipfs-api.optnlabs.com/ipfs/,https://w3s.link/ipfs/,https://nftstorage.link/ipfs/,https://cf-ipfs.com/ipfs/,https://cloudflare-ipfs.com/ipfs/,https://ipfs.filebase.io/ipfs/"
    )]
    pub bcmr_ipfs_gateways: Vec<String>,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_BACKFILL_ENABLED",
        default_value_t = false
    )]
    pub bcmr_backfill_enabled: bool,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_BACKFILL_FROM_HEIGHT",
        default_value_t = 0
    )]
    pub bcmr_backfill_from_height: i32,

    #[arg(long, env = "TOKENINDEX_BCMR_BACKFILL_TO_HEIGHT", default_value_t = 0)]
    pub bcmr_backfill_to_height: i32,

    #[arg(
        long,
        env = "TOKENINDEX_BCMR_BACKFILL_BATCH_BLOCKS",
        default_value_t = 200
    )]
    pub bcmr_backfill_batch_blocks: i32,

    #[arg(
        long,
        env = "TOKENINDEX_CONSISTENCY_CHECK_INTERVAL",
        default_value_t = 100
    )]
    pub consistency_check_interval: i32,

    #[arg(long, env = "TOKENINDEX_RECONCILE_ENABLED", default_value_t = true)]
    pub reconcile_enabled: bool,

    #[arg(
        long,
        env = "TOKENINDEX_RECONCILE_INTERVAL_SECS",
        default_value_t = 120
    )]
    pub reconcile_interval_secs: u64,

    #[arg(long, env = "TOKENINDEX_RPC_TIMEOUT_MS", default_value_t = 10_000)]
    pub rpc_timeout_ms: u64,

    #[arg(long, env = "TOKENINDEX_RPC_RETRIES", default_value_t = 2)]
    pub rpc_retries: u32,

    #[arg(long, env = "TOKENINDEX_RPC_RETRY_BACKOFF_MS", default_value_t = 200)]
    pub rpc_retry_backoff_ms: u64,

    #[arg(long, env = "TOKENINDEX_RPC_BATCH_SIZE", default_value_t = 100)]
    pub rpc_batch_size: usize,

    #[arg(long, env = "TOKENINDEX_RPC_PREFETCH_BATCHES", default_value_t = 3)]
    pub rpc_prefetch_batches: usize,

    #[arg(
        long,
        env = "TOKENINDEX_DB_INGEST_SYNCHRONOUS_COMMIT",
        default_value = "on"
    )]
    pub db_ingest_synchronous_commit: String,

    #[arg(long, env = "TOKENINDEX_CACHE_MAX_ITEMS", default_value_t = 50_000)]
    pub cache_max_items: u64,

    #[arg(long, env = "TOKENINDEX_CACHE_TTL_SECS", default_value_t = 15)]
    pub cache_ttl_secs: u64,

    #[arg(long, env = "TOKENINDEX_STALE_WHILE_ERROR_SECS", default_value_t = 30)]
    pub stale_while_error_secs: u64,

    #[arg(long, env = "TOKENINDEX_DATASET_VERSION", default_value = "v1")]
    pub dataset_version: String,

    #[arg(long, env = "TOKENINDEX_API_CONCURRENCY_LIMIT", default_value_t = 2048)]
    pub api_concurrency_limit: usize,

    #[arg(long, env = "TOKENINDEX_RATE_LIMIT_DEFAULT_RPS", default_value_t = 200)]
    pub rate_limit_default_rps: u32,

    #[arg(long, env = "TOKENINDEX_RATE_LIMIT_HOLDERS_RPS", default_value_t = 60)]
    pub rate_limit_holders_rps: u32,

    #[arg(
        long,
        env = "TOKENINDEX_RATE_LIMIT_ELIGIBILITY_RPS",
        default_value_t = 400
    )]
    pub rate_limit_eligibility_rps: u32,

    #[arg(long, env = "TOKENINDEX_API_BEARER_TOKEN")]
    pub api_bearer_token: Option<String>,

    #[arg(long, env = "TOKENINDEX_IP_ALLOWLIST", value_delimiter = ',')]
    pub ip_allowlist: Vec<String>,

    #[arg(long, env = "TOKENINDEX_TRUSTED_PROXY_CIDRS", value_delimiter = ',')]
    pub trusted_proxy_cidrs: Vec<String>,

    #[arg(
        long,
        env = "TOKENINDEX_TRUST_X_FORWARDED_FOR",
        default_value_t = false
    )]
    pub trust_x_forwarded_for: bool,

    #[arg(long, env = "TOKENINDEX_MAX_CURSOR_CHARS", default_value_t = 512)]
    pub max_cursor_chars: usize,

    #[arg(
        long,
        env = "TOKENINDEX_CORS_ALLOW_ORIGINS",
        value_delimiter = ',',
        default_value = "*"
    )]
    pub cors_allow_origins: Vec<String>,

    #[arg(
        long,
        env = "TOKENINDEX_LOG_LEVEL",
        default_value = "info,tokenindex=debug"
    )]
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Self {
        apply_chipnet_aliases();
        Self::parse()
    }

    pub fn api_db_pool_size(&self) -> u32 {
        if self.db_api_max_connections == 0 {
            self.db_max_connections
        } else {
            self.db_api_max_connections
        }
    }

    pub fn ingest_db_pool_size(&self) -> u32 {
        if self.db_ingest_max_connections == 0 {
            self.db_max_connections
        } else {
            self.db_ingest_max_connections
        }
    }

    pub fn mainnet_config(&self) -> anyhow::Result<Option<Self>> {
        let mainnet_requested = self.mainnet_api_host.is_some()
            || self.mainnet_database_url.is_some()
            || self.mainnet_db_schema.is_some()
            || self.mainnet_database_read_url.is_some()
            || self.mainnet_rpc_url.is_some()
            || self.mainnet_rpc_user.is_some()
            || self.mainnet_rpc_pass.is_some();
        if !mainnet_requested {
            return Ok(None);
        }

        let rpc_url = self.mainnet_rpc_url.clone().ok_or_else(|| {
            anyhow::anyhow!("TOKENINDEX_MAINNET_RPC_URL is required when enabling mainnet stack")
        })?;

        let mut cfg = self.clone();
        cfg.api_host = self
            .mainnet_api_host
            .clone()
            .unwrap_or_else(|| self.api_host.clone());
        cfg.api_port = self.mainnet_api_port;
        cfg.database_url = self
            .mainnet_database_url
            .clone()
            .unwrap_or_else(|| self.database_url.clone());
        cfg.database_read_url = self
            .mainnet_database_read_url
            .clone()
            .or_else(|| self.database_read_url.clone());
        cfg.db_schema = self
            .mainnet_db_schema
            .clone()
            .unwrap_or_else(|| "mainnet".to_string());
        cfg.rpc_url = rpc_url;
        cfg.rpc_user = self
            .mainnet_rpc_user
            .clone()
            .unwrap_or_else(|| self.rpc_user.clone());
        cfg.rpc_pass = self
            .mainnet_rpc_pass
            .clone()
            .unwrap_or_else(|| self.rpc_pass.clone());
        cfg.bootstrap_height = self
            .mainnet_bootstrap_height
            .unwrap_or(self.bootstrap_height);
        cfg.expected_chain = "main".to_string();
        Ok(Some(cfg))
    }

    pub fn startup_snapshot(&self) -> StartupSnapshot {
        StartupSnapshot {
            api_bind: format!("{}:{}", self.api_host, self.api_port),
            db_schema: self.db_schema.clone(),
            database_url: redact_url(&self.database_url),
            database_read_url: self.database_read_url.as_deref().map(redact_url),
            rpc_url: redact_url(&self.rpc_url),
            redis_enabled: self.redis_url.is_some(),
            zmq_enabled: self.zmq_block_endpoint.is_some(),
            mempool_enabled: self.mempool_enabled,
            bcmr_enabled: self.bcmr_enabled,
            reconcile_enabled: self.reconcile_enabled,
            bootstrap_height: self.bootstrap_height,
            log_level: self.log_level.clone(),
            expected_chain: self.expected_chain.clone(),
            db_pool_max_connections: self.db_max_connections,
            db_api_pool_max_connections: self.api_db_pool_size(),
            db_ingest_pool_max_connections: self.ingest_db_pool_size(),
            rpc_batch_size: self.rpc_batch_size,
            rpc_prefetch_batches: self.rpc_prefetch_batches,
            db_ingest_synchronous_commit: self.db_ingest_synchronous_commit.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StartupSnapshot {
    pub api_bind: String,
    pub db_schema: String,
    pub database_url: String,
    pub database_read_url: Option<String>,
    pub rpc_url: String,
    pub redis_enabled: bool,
    pub zmq_enabled: bool,
    pub mempool_enabled: bool,
    pub bcmr_enabled: bool,
    pub reconcile_enabled: bool,
    pub bootstrap_height: i32,
    pub log_level: String,
    pub expected_chain: String,
    pub db_pool_max_connections: u32,
    pub db_api_pool_max_connections: u32,
    pub db_ingest_pool_max_connections: u32,
    pub rpc_batch_size: usize,
    pub rpc_prefetch_batches: usize,
    pub db_ingest_synchronous_commit: String,
}

fn redact_url(raw: &str) -> String {
    let Ok(mut parsed) = Url::parse(raw) else {
        return "<invalid-url>".to_string();
    };
    if !parsed.username().is_empty() {
        let _ = parsed.set_username("****");
    }
    if parsed.password().is_some() {
        let _ = parsed.set_password(Some("****"));
    }
    parsed.to_string()
}

fn apply_chipnet_aliases() {
    let aliases = [
        ("TOKENINDEX_CHIPNET_RPC_URL", "TOKENINDEX_RPC_URL"),
        ("TOKENINDEX_CHIPNET_RPC_USER", "TOKENINDEX_RPC_USER"),
        ("TOKENINDEX_CHIPNET_RPC_PASS", "TOKENINDEX_RPC_PASS"),
        (
            "TOKENINDEX_CHIPNET_EXPECTED_CHAIN",
            "TOKENINDEX_EXPECTED_CHAIN",
        ),
        (
            "TOKENINDEX_CHIPNET_ZMQ_BLOCK_ENDPOINT",
            "TOKENINDEX_ZMQ_BLOCK_ENDPOINT",
        ),
        ("TOKENINDEX_CHIPNET_DATABASE_URL", "TOKENINDEX_DATABASE_URL"),
        (
            "TOKENINDEX_CHIPNET_DATABASE_READ_URL",
            "TOKENINDEX_DATABASE_READ_URL",
        ),
        ("TOKENINDEX_CHIPNET_DB_SCHEMA", "TOKENINDEX_DB_SCHEMA"),
    ];

    for (chipnet_key, primary_key) in aliases {
        if env::var_os(primary_key).is_some() {
            continue;
        }
        if let Some(value) = env::var_os(chipnet_key) {
            env::set_var(primary_key, value);
        }
    }
}
