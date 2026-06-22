use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{ChPrice, ChSchemaVersion, ChShares},
    enums::clickhouse::{ChBookEventType, ChFactSource},
    types::{MarketId, TokenId},
};

/// Short-retention L2 replay fact for recent exact book reconstruction.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookL2ReplayRow {
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub event_type: ChBookEventType,
    pub bid_prices: Vec<ChPrice>,
    pub bid_sizes: Vec<ChShares>,
    pub ask_prices: Vec<ChPrice>,
    pub ask_sizes: Vec<ChShares>,
    pub book_version: u64,
    pub levels_count: u16,
    pub is_full_snapshot: bool,
    pub event_time: i64,
    pub ingestion_time: i64,
    pub sequence: u64,
    pub source: ChFactSource,
    pub feed_event_hash: Option<String>,
    pub schema_version: ChSchemaVersion,
}
