use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::ChSchemaVersion,
    enums::clickhouse::ChFactSource,
    types::{MarketId, TokenId},
};

/// `ClickHouse` row for the `market_resolution_event` table — the single,
/// append-only, point-in-time settlement truth source.
///
/// The authoritative settlement key is [`Self::winning_token_id`]
/// (label-agnostic); `winning_outcome` is informational only. `resolved_at` is
/// the economic settlement time; `observed_at` is when the resolution was
/// ingested (the maturity anchor for forward-looking training labels). Both are
/// epoch milliseconds bound to `DateTime64(3, 'UTC')` columns.
#[derive(Debug, Clone, PartialEq, Eq, clickhouse::Row, Serialize, Deserialize)]
pub struct MarketResolutionRow {
    pub market_id: MarketId,
    pub winning_token_id: TokenId,
    pub winning_outcome: String,
    /// All outcome tokens of the market (for payout / completeness).
    pub asset_token_ids: Vec<TokenId>,
    /// Economic settlement (close) time, epoch milliseconds.
    pub resolved_at: i64,
    /// Writer ingestion time, epoch milliseconds (PIT maturity anchor).
    pub observed_at: i64,
    pub source: ChFactSource,
    /// Stable tie-breaker for same resolved/observed time rows.
    pub sequence: u64,
    pub schema_version: ChSchemaVersion,
}
