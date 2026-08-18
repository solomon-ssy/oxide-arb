//! Decision-time account capital snapshot.
//!
//! `AccountSnapshot` is a compute-domain value type, parallel to
//! [`crate::model::SignalCandidate`]: it is produced by the core
//! `AccountProvider` (which performs venue I/O) and consumed by the governed
//! planner for sizing and exposure-net projections. The position / exposure
//! value types are owned by `quant-pivot-models` (`types::account`) because they
//! also back the `quant_account_snapshot` persistence DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    enums::quant::AccountSource,
    types::{ExposureBreakdown, Usd, VenuePositionSnapshot},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Real venue account state frozen at a report's `as_of`.
///
/// `venue_net_liquidation_usd` is the uncapped venue truth. `capital_base_usd`
/// is the governed sizing anchor: `min(venue_net_liquidation_usd, budget cap)`.
/// `exposures` is derived from `positions` and is the planner's starting point
/// for per-market / per-event / per-category cap-room checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// Capital-base provenance (always the real Polymarket venue).
    pub source: AccountSource,
    /// Uncapped venue net liquidation value.
    pub venue_net_liquidation_usd: Usd,
    /// Governed strategy capital base used for sizing and drawdown.
    pub capital_base_usd: Usd,
    /// Available cash (collateral − reserved).
    pub available_usd: Usd,
    /// Capital reserved by pending intents at decision time.
    pub reserved_usd: Usd,
    /// Held positions marked to the venue price.
    pub positions: Vec<VenuePositionSnapshot>,
    /// Net exposure aggregated from `positions`.
    pub exposures: ExposureBreakdown,
}

impl AccountSnapshot {
    /// Build a snapshot, deriving `exposures` from `positions`.
    #[must_use]
    pub fn new(
        as_of: DateTime<Utc>,
        source: AccountSource,
        venue_net_liquidation_usd: Usd,
        capital_base_usd: Usd,
        available_usd: Usd,
        reserved_usd: Usd,
        positions: Vec<VenuePositionSnapshot>,
    ) -> Self {
        let exposures = ExposureBreakdown::from_positions(&positions);
        Self {
            as_of,
            source,
            venue_net_liquidation_usd,
            capital_base_usd,
            available_usd,
            reserved_usd,
            positions,
            exposures,
        }
    }
}

/// Frozen peak-to-trough strategy drawdown ratio at a decision boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AccountDrawdown {
    pub current_ratio: Decimal,
}

impl AccountDrawdown {
    #[must_use]
    pub const fn neutral() -> Self {
        Self {
            current_ratio: Decimal::ZERO,
        }
    }

    /// Preserve the most conservative ledger observation within one report run.
    #[must_use]
    pub fn conservative_max(self, other: Self) -> Self {
        Self {
            current_ratio: self.current_ratio.max(other.current_ratio),
        }
    }
}
