use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(name = "tokenindex")]
pub struct Config {
    #[arg(long, env = "TOKENINDEX_API_HOST", default_value = "0.0.0.0")]
    pub api_host: String,

    #[arg(long, env = "TOKENINDEX_API_PORT", default_value_t = 8080)]
    pub api_port: u16,

    #[arg(long, env = "TOKENINDEX_DATABASE_URL", default_value = "postgres://tokenindex:tokenindex@127.0.0.1:5432/tokenindex")]
    pub database_url: String,

    #[arg(long, env = "TOKENINDEX_DB_MAX_CONNECTIONS", default_value_t = 30)]
    pub db_max_connections: u32,

    #[arg(long, env = "TOKENINDEX_REDIS_URL")]
    pub redis_url: Option<String>,

    #[arg(long, env = "TOKENINDEX_RPC_URL", default_value = "http://127.0.0.1:48334")]
    pub rpc_url: String,

    #[arg(long, env = "TOKENINDEX_RPC_USER")]
    pub rpc_user: String,

    #[arg(long, env = "TOKENINDEX_RPC_PASS")]
    pub rpc_pass: String,

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

    #[arg(long, env = "TOKENINDEX_RATE_LIMIT_ELIGIBILITY_RPS", default_value_t = 400)]
    pub rate_limit_eligibility_rps: u32,

    #[arg(long, env = "TOKENINDEX_CORS_ALLOW_ORIGINS", value_delimiter = ',', default_value = "*")]
    pub cors_allow_origins: Vec<String>,

    #[arg(long, env = "TOKENINDEX_LOG_LEVEL", default_value = "info,tokenindex=debug")]
    pub log_level: String,
}

impl Config {
    pub fn from_env() -> Self {
        Self::parse()
    }
}
