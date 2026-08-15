use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    clickhouse::{ChAssetAmount, ChDigest, ChPrice, ChSchemaVersion, ChShares, ChUsd},
    enums::clickhouse::{
        ChAvailabilityBasis, ChExchangeEventKind, ChExchangeSide, ChExchangeVersion,
        ChExecutionParticipantRole,
    },
    types::{MarketId, TokenId},
};

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ExchangeLogRawRow {
    pub chain_id: u64,
    pub contract_key: String,
    pub exchange_version: ChExchangeVersion,
    pub contract_address: String,
    pub block_number: u64,
    pub block_hash: String,
    pub parent_block_hash: String,
    pub block_timestamp: i64,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub topic0: String,
    pub topic1: Option<String>,
    pub topic2: Option<String>,
    pub topic3: Option<String>,
    pub data: String,
    pub removed: bool,
    pub hypersync_observed_at: i64,
    pub attestor_observed_at: i64,
    pub observed_at: i64,
    pub model_available_at: i64,
    pub availability_basis: ChAvailabilityBasis,
    pub availability_policy_hash: ChDigest,
    #[serde(with = "clickhouse::serde::uuid")]
    pub chunk_id: Uuid,
    pub raw_log_hash: ChDigest,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ExchangeEventRow {
    pub event_id: ChDigest,
    pub raw_log_hash: ChDigest,
    pub event_kind: ChExchangeEventKind,
    pub contract_key: String,
    pub exchange_version: ChExchangeVersion,
    pub contract_address: String,
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: i64,
    pub transaction_hash: String,
    pub transaction_index: u64,
    pub log_index: u64,
    pub order_hash: String,
    pub maker: String,
    pub taker: Option<String>,
    pub side: ChExchangeSide,
    pub token_id: Option<String>,
    pub maker_asset_id: Option<String>,
    pub taker_asset_id: Option<String>,
    pub maker_amount: String,
    pub taker_amount: String,
    pub fee_amount: Option<String>,
    pub builder: Option<String>,
    pub metadata: Option<String>,
    pub observed_at: i64,
    pub model_available_at: i64,
    pub availability_policy_hash: ChDigest,
    #[serde(with = "clickhouse::serde::uuid")]
    pub chunk_id: Uuid,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ExchangeMatchRow {
    pub match_id: ChDigest,
    pub orders_matched_event_id: ChDigest,
    pub aggregate_taker_event_id: ChDigest,
    pub contract_key: String,
    pub exchange_version: ChExchangeVersion,
    pub transaction_hash: String,
    pub block_number: u64,
    pub block_timestamp: i64,
    pub taker_order_hash: String,
    pub taker_address: String,
    pub side: ChExchangeSide,
    pub token_id: Option<String>,
    pub maker_asset_id: Option<String>,
    pub taker_asset_id: Option<String>,
    pub maker_amount: String,
    pub taker_amount: String,
    pub maker_execution_count: u32,
    pub observed_at: i64,
    pub model_available_at: i64,
    pub availability_policy_hash: ChDigest,
    #[serde(with = "clickhouse::serde::uuid")]
    pub chunk_id: Uuid,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct MarketExecutionRow {
    pub execution_id: ChDigest,
    pub match_id: Option<ChDigest>,
    pub maker_order_filled_event_id: ChDigest,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub contract_key: String,
    pub exchange_version: ChExchangeVersion,
    pub transaction_hash: String,
    pub block_number: u64,
    pub transaction_index: u64,
    pub log_index: u64,
    pub maker_address: String,
    pub taker_address: String,
    pub side: ChExchangeSide,
    pub price: ChPrice,
    pub size_shares: ChShares,
    pub notional_usd: ChUsd,
    pub fee_amount: ChAssetAmount,
    pub fee_asset_id: String,
    pub effective_at: i64,
    pub observed_at: i64,
    pub model_available_at: i64,
    pub availability_basis: ChAvailabilityBasis,
    pub availability_policy_hash: ChDigest,
    #[serde(with = "clickhouse::serde::uuid")]
    pub chunk_id: Uuid,
    pub schema_version: ChSchemaVersion,
}

#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ExecutionParticipantRow {
    pub execution_id: ChDigest,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub participant_address: String,
    pub participant_role: ChExecutionParticipantRole,
    pub participant_notional: ChUsd,
    pub effective_at: i64,
    pub model_available_at: i64,
    pub availability_policy_hash: ChDigest,
    #[serde(with = "clickhouse::serde::uuid")]
    pub chunk_id: Uuid,
    pub schema_version: ChSchemaVersion,
}

/// Commit marker published after every fact family is durable.
///
/// Readers must join through this table so a crash between fact-family inserts
/// cannot expose a partial dual-provider-attested semantic chunk.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ExchangeHistoryAcceptanceRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub chunk_id: Uuid,
    pub frontier: String,
    pub from_block: u64,
    pub to_block: u64,
    pub log_count: u64,
    pub provider_digest: ChDigest,
    pub first_block_hash: String,
    pub last_block_hash: String,
    pub effective_through_at: i64,
    pub accepted_at: i64,
    pub active: u8,
    pub state_revision: u64,
    pub schema_version: ChSchemaVersion,
}

/// Query projection joining one economic execution with exactly one of its
/// two participant rows. This is the canonical input to participant-aware
/// finalized-execution features.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct ExecutionParticipantFactRow {
    pub execution_id: ChDigest,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub participant_address: String,
    pub participant_role: ChExecutionParticipantRole,
    pub side: ChExchangeSide,
    pub price: ChPrice,
    pub size_shares: ChShares,
    pub notional_usd: ChUsd,
    pub transaction_hash: String,
    pub effective_at: i64,
    pub observed_at: i64,
    pub model_available_at: i64,
    pub availability_policy_hash: ChDigest,
}

impl ExchangeLogRawRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);
}

impl ExchangeEventRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);
}

impl ExchangeMatchRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);
}

impl MarketExecutionRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);
}

impl ExecutionParticipantRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);
}

impl ExchangeHistoryAcceptanceRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(2);
}
