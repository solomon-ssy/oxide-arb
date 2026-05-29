//! Blacklist domain models for the risk engine.
//!
//! Tracks markets and tokens that are temporarily or permanently excluded
//! from trading due to repeated failures, data issues, or manual operator
//! action.

use crate::{
    enums::risk::{BlacklistReason, BlacklistScope},
    types::{MarketId, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read model ──────────────────────────────────────────────────────

/// DB row projection for the `blacklist_entry` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::blacklist_entry::Entity")]
pub struct BlacklistInfo {
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub scope: BlacklistScope,
    pub reason: BlacklistReason,
    pub expires_at: Option<DateTime<Utc>>,
    pub miss_count: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl BlacklistInfo {
    /// Whether this entry has expired at the given time.
    #[must_use]
    pub fn is_expired(&self, now: DateTime<Utc>) -> bool {
        self.expires_at.is_some_and(|exp| now >= exp)
    }

    /// Whether this is a permanent (never-expiring) entry.
    #[must_use]
    pub const fn is_permanent(&self) -> bool {
        self.expires_at.is_none()
    }

    /// Whether this entry blocks trading (scope >= `TradingPath`).
    #[must_use]
    pub fn blocks_trading(&self) -> bool {
        self.scope >= BlacklistScope::TradingPath
    }
}

info_from_model!(BlacklistInfo, crate::entities::blacklist_entry::Model, {
    market_id, token_id, scope, reason, expires_at, miss_count,
    created_at, updated_at,
});

// ── Write DTOs ──────────────────────────────────────────────────────

/// Upsert payload for the `blacklist_entry` table (ON CONFLICT DO UPDATE).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::blacklist_entry::ActiveModel")]
pub struct UpsertBlacklistEntry {
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub scope: BlacklistScope,
    pub reason: BlacklistReason,
    pub expires_at: Option<DateTime<Utc>>,
    pub miss_count: i32,
}
