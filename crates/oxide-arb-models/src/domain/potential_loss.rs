//! Potential loss ledger domain models.
//!
//! Tracks maximum potential loss for positions that have not yet settled.
//! Used by the risk engine to account for worst-case exposure before
//! market resolution is confirmed.

use crate::enums::common::LedgerStatus;
use crate::types::{MarketId, TokenId, Usd};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single potential-loss ledger entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PotentialLossEntry {
    pub entry_id: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    /// Original cost of the position.
    pub cost_basis: Usd,
    /// Maximum possible loss if the prediction is wrong (`cost_basis` + fees).
    pub max_loss: Usd,
    pub status: LedgerStatus,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl PotentialLossEntry {
    /// Whether this entry is still active (not resolved or expired).
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == LedgerStatus::Active
    }
}
