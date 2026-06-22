use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChBps, ChPrice, ChSchemaVersion, ChUsd},
    enums::clickhouse::{ChFactSource, ChSnapshotReason},
    types::{MarketId, TokenId},
};

/// `ClickHouse` row for `book_snapshots` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookSnapshotRow {
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub snapshot_reason: ChSnapshotReason,
    pub top_n: u16,
    pub bids_json: String,
    pub asks_json: String,
    pub bid_depth_usd: Option<ChUsd>,
    pub ask_depth_usd: Option<ChUsd>,
    pub mid_price: Option<ChPrice>,
    pub spread_bps: Option<ChBps>,
    pub book_version: u64,
    pub levels_count: u16,
    /// Business event time in epoch milliseconds.
    pub event_time: i64,
    /// Writer ingestion time in epoch milliseconds.
    pub ingestion_time: i64,
    /// Stable tie-breaker for same event/ingestion time rows.
    pub sequence: u64,
    pub source: ChFactSource,
    pub schema_version: ChSchemaVersion,
}
