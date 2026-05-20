//! Market lifecycle enums for the data pipeline.

use serde::{Deserialize, Serialize};

/// Lifecycle state of a market in the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketStatus {
    /// Discovered via Gamma but not yet evaluated.
    Discovered,
    /// Active and eligible for WS subscription.
    Active,
    /// Filtered out (insufficient liquidity, too new, etc.).
    Filtered,
    /// Trading has been paused (manual or auto blacklist).
    Paused,
    /// Market has settled / resolved.
    Settled,
    /// Market is no longer listed on the exchange.
    Delisted,
}

impl std::fmt::Display for MarketStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Discovered => write!(f, "discovered"),
            Self::Active => write!(f, "active"),
            Self::Filtered => write!(f, "filtered"),
            Self::Paused => write!(f, "paused"),
            Self::Settled => write!(f, "settled"),
            Self::Delisted => write!(f, "delisted"),
        }
    }
}
