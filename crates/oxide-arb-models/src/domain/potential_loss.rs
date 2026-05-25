//! Potential loss ledger domain models.
//!
//! Tracks maximum potential loss for positions that have not yet settled.
//! Used by the risk engine to account for worst-case exposure before
//! market resolution is confirmed.

use crate::enums::common::LedgerStatus;
use crate::types::{LedgerId, MarketId, Price, Shares, TokenId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read model ──────────────────────────────────────────────────────

/// DB row projection for the `potential_loss_ledger` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::potential_loss_ledger::Entity")]
pub struct PotentialLossInfo {
    pub ledger_id: LedgerId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub shares: Shares,
    pub entry_price: Price,
    pub max_loss_usd: Usd,
    pub status: LedgerStatus,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

impl PotentialLossInfo {
    /// Whether this entry is still active (not resolved or expired).
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.status == LedgerStatus::Active
    }
}

info_from_model!(PotentialLossInfo, crate::entities::potential_loss_ledger::Model, {
    ledger_id, market_id, token_id, shares, entry_price, max_loss_usd,
    status, created_at, resolved_at,
});

// ── Write DTOs ──────────────────────────────────────────────────────

/// All fields required to create a new potential loss entry.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::potential_loss_ledger::ActiveModel")]
pub struct NewPotentialLoss {
    pub ledger_id: LedgerId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub shares: Shares,
    pub entry_price: Price,
    pub max_loss_usd: Usd,
}

/// Partial update for potential loss entries (resolution).
#[derive(Debug, Clone, Default)]
pub struct UpdatePotentialLoss {
    pub status: Option<LedgerStatus>,
    pub resolved_at: Option<DateTime<Utc>>,
}
