use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenSummary {
    pub category: String,
    pub total_supply: String,
    pub holder_count: i64,
    pub utxo_count: i64,
    pub updated_height: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderBalance {
    pub locking_bytecode: String,
    pub ft_balance: String,
    pub utxo_count: i32,
    pub updated_height: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HolderTokenBalance {
    pub category: String,
    pub ft_balance: String,
    pub utxo_count: i32,
    pub updated_height: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainState {
    pub height: i32,
    pub blockhash: String,
    pub updated_at: DateTime<Utc>,
}

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
