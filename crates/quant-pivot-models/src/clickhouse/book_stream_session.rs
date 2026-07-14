use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    clickhouse::ChSchemaVersion,
    enums::clickhouse::{ChStreamSessionEndReason, ChStreamSessionState},
    types::ContentHash,
};

/// Append-only stream-session ledger record.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct BookStreamSessionRow {
    pub stream_session_id: Uuid,
    pub shard_id: u32,
    pub ledger_sequence: u32,
    pub state: ChStreamSessionState,
    pub end_reason: ChStreamSessionEndReason,
    pub subscription_token_hash: ContentHash,
    pub subscription_token_count: u32,
    pub received_sequence_json: String,
    pub persisted_sequence_json: String,
    pub opened_at: i64,
    pub recorded_at: i64,
    pub schema_version: ChSchemaVersion,
}
