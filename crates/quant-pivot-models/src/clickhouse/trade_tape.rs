use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChBps, ChPrice, ChSchemaVersion, ChShares, ChUsd},
    enums::clickhouse::{
        ChTradeParticipantRole, ChTradeReconciliationStatus, ChTradeSide, ChTradeTapeSource,
    },
    types::{MarketId, TokenId},
};
use uuid::Uuid;

/// `ClickHouse` row for the `quant_trade_tape` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct TradeTapeRow {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub event_time: i64,
    pub ingestion_time: i64,
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub stream_session_id: Option<Uuid>,
    pub token_sequence: Option<u64>,
    pub participant_address: String,
    pub participant_role: ChTradeParticipantRole,
    pub side: ChTradeSide,
    pub price: ChPrice,
    pub size_shares: ChShares,
    pub notional_usd: ChUsd,
    pub tx_hash: Option<String>,
    pub source_event_id: String,
    pub source: ChTradeTapeSource,
    /// Bitmask describing which upstream fields were directly observed.
    pub observed_field_flags: u16,
    pub fee_rate_bps: Option<ChBps>,
    pub reconciliation_status: ChTradeReconciliationStatus,
    pub matched_source_event_id: Option<String>,
    pub revision: u32,
    pub reconciled_at: Option<i64>,
    pub raw_payload_json: Option<String>,
    pub schema_version: ChSchemaVersion,
}

impl TradeTapeRow {
    pub const SCHEMA_VERSION: ChSchemaVersion = ChSchemaVersion(1);
}
