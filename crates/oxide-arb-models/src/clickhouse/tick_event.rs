use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChBps, ChPrice, ChSchemaVersion, ChUsd},
    enums::clickhouse::{ChBookEventType, ChFactSource},
    types::{MarketId, TokenId},
};

/// `ClickHouse` row for `tick_events` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct TickEventRow {
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub event_type: ChBookEventType,
    pub best_bid: Option<ChPrice>,
    pub best_ask: Option<ChPrice>,
    pub last_trade_price: Option<ChPrice>,
    pub bid_depth_usd: Option<ChUsd>,
    pub ask_depth_usd: Option<ChUsd>,
    pub spread_bps: Option<ChBps>,
    pub book_version: u64,
    pub raw_payload_json: Option<String>,
    /// Business event time in epoch milliseconds.
    pub event_time: i64,
    /// Writer ingestion time in epoch milliseconds.
    pub ingestion_time: i64,
    /// Stable tie-breaker for same event/ingestion time rows.
    pub sequence: u64,
    pub source: ChFactSource,
    pub schema_version: ChSchemaVersion,
}
