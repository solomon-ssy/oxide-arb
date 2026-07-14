use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::ChSchemaVersion,
    types::{ContentHash, MarketId, TokenId},
};
use uuid::Uuid;

/// Rebuild checkpoint anchored to one canonical L2 event.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookL2CheckpointRow {
    pub token_id: TokenId,
    pub market_id: Option<MarketId>,
    pub stream_session_id: Uuid,
    pub token_sequence: u64,
    pub bids_json: String,
    pub asks_json: String,
    pub book_version: u64,
    pub source_event_hash: ContentHash,
    pub checkpoint_hash: ContentHash,
    pub event_time: i64,
    pub created_at: i64,
    pub schema_version: ChSchemaVersion,
}
