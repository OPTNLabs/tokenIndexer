use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MempoolHolderDelta {
    pub locking_bytecode: String,
    pub locking_address: Option<String>,
    pub ft_delta: String,
    pub utxo_delta: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MempoolCategoryView {
    pub tx_count: u32,
    pub ft_credits: String,
    pub ft_debits: String,
    pub net_ft_delta: String,
    pub utxo_delta: i64,
    pub nft_count: u32,
    pub holders: HashMap<String, MempoolHolderDelta>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct MempoolSnapshot {
    pub updated_at: DateTime<Utc>,
    pub total_mempool_txs: u32,
    pub scanned_txs: u32,
    pub categories: HashMap<String, MempoolCategoryView>,
}
