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
    types::{ExposureBreakdown, PositionSnapshot, Usd},
};
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
    pub positions: Vec<PositionSnapshot>,
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
        positions: Vec<PositionSnapshot>,
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
