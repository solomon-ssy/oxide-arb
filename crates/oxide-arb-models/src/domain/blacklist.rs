//! Blacklist domain models for the risk engine.
//!
//! Tracks markets and tokens that are temporarily or permanently excluded
//! from trading due to repeated failures, data issues, or manual operator
//! action.

use crate::enums::risk::{BlacklistReason, BlacklistScope};
use crate::types::{MarketId, TokenId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A single blacklist entry for a market or token.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlacklistEntry {
    pub market_id: MarketId,
    pub token_id: Option<TokenId>,
    pub scope: BlacklistScope,
    pub reason: BlacklistReason,
    /// `None` for permanent blacklist entries.
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    /// Consecutive miss count that triggered this entry (for auto-blacklist).
    pub miss_count: u32,
}

impl BlacklistEntry {
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

/// Result of checking a market/token against the blacklist.
#[derive(Debug, Clone)]
pub enum BlacklistCheckResult {
    /// Not blacklisted — proceed.
    Clear,
    /// Blacklisted — do not trade.
    Blocked {
        reason: BlacklistReason,
        scope: BlacklistScope,
        expires_at: Option<DateTime<Utc>>,
    },
}

impl BlacklistCheckResult {
    #[must_use]
    pub const fn is_clear(&self) -> bool {
        matches!(self, Self::Clear)
    }
}
