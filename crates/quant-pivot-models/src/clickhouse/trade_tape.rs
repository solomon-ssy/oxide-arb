use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChPrice, ChSchemaVersion, ChShares, ChUsd},
    enums::clickhouse::{ChTradeParticipantRole, ChTradeSide, ChTradeTapeSource},
    types::{MarketId, TokenId},
};

/// `ClickHouse` row for the `quant_trade_tape` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct TradeTapeRow {
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub event_time: i64,
    pub ingestion_time: i64,
    pub participant_address: String,
    pub participant_role: ChTradeParticipantRole,
    pub side: ChTradeSide,
    pub price: ChPrice,
    pub size_shares: ChShares,
    pub notional_usd: ChUsd,
    pub tx_hash: Option<String>,
    pub trade_id: String,
    pub source: ChTradeTapeSource,
    /// Bitmask describing which upstream fields were directly observed.
    pub coverage_flags: u16,
    pub raw_payload_json: Option<String>,
    pub schema_version: ChSchemaVersion,
}
