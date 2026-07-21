use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    clickhouse::{ChPrice, ChSchemaVersion, ChShares},
    enums::clickhouse::ChCanonicalBookEventType,
    types::{ContentHash, MarketId, TokenId},
};

/// Canonical, loss-intolerant L2 event used for replay and policy evidence.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookL2EventRow {
    #[serde(with = "clickhouse::serde::uuid")]
    pub stream_session_id: Uuid,
    pub shard_id: u32,
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub token_sequence: u64,
    pub event_type: ChCanonicalBookEventType,
    pub bid_prices: Vec<ChPrice>,
    pub bid_sizes: Vec<ChShares>,
    pub ask_prices: Vec<ChPrice>,
    pub ask_sizes: Vec<ChShares>,
    pub book_version: u64,
    pub old_tick_size: Option<ChPrice>,
    pub new_tick_size: Option<ChPrice>,
    pub venue_event_time: i64,
    pub ingress_time: i64,
    pub persisted_time: i64,
    pub payload_hash: ContentHash,
    pub schema_version: ChSchemaVersion,
}
