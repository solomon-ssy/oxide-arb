//! Blacklist check result enums.

use crate::enums::risk::{BlacklistReason, BlacklistScope};
use chrono::{DateTime, Utc};

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
