//! Signal candidate persistence DTOs (pre-portfolio pruning).

use crate::types::{MarketId, ModelRunId, TokenId};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Runtime signal candidate before portfolio pruning.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalCandidate {
    pub signal_candidate_id: String,
    pub model_run_id: ModelRunId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: i8,
    pub score: Decimal,
    pub confidence: Decimal,
    pub entry_price: Decimal,
    pub target_price: Decimal,
    pub stop_price: Decimal,
    pub rank_before_portfolio: u32,
    pub rejection_reason: Option<String>,
    pub as_of: DateTime<Utc>,
}
