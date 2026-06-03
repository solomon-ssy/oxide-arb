use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChPrice, ChSchemaVersion, ChShares},
    enums::clickhouse::{ChBookEventType, ChFactSource},
    types::{MarketId, TokenId},
};

/// `ClickHouse` row for `tick_events_l2` table — full L2 orderbook state.
///
/// Uses `Array(Decimal64(8))` columns to store price levels without JSON
/// parsing overhead. Supports both full snapshots and incremental deltas.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct TickEventL2Row {
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    /// 1 = snapshot (full book replacement), 2 = delta (incremental update).
    pub event_type: ChBookEventType,
    /// Bid prices ordered best-to-worst (descending).
    pub bid_prices: Vec<ChPrice>,
    /// Bid sizes corresponding to `bid_prices`.
    pub bid_sizes: Vec<ChShares>,
    /// Ask prices ordered best-to-worst (ascending).
    pub ask_prices: Vec<ChPrice>,
    /// Ask sizes corresponding to `ask_prices`.
    pub ask_sizes: Vec<ChShares>,
    /// For delta events: JSON-encoded changed level indices (nullable).
    pub changed_levels_json: Option<String>,
    pub book_version: u64,
    pub levels_count: u16,
    pub is_full_snapshot: bool,
    /// Business event time in epoch milliseconds.
    pub event_time: i64,
    /// Writer ingestion time in epoch milliseconds.
    pub ingestion_time: i64,
    /// Stable tie-breaker for same event/ingestion time rows.
    pub sequence: u64,
    pub source: ChFactSource,
    pub schema_version: ChSchemaVersion,
}
