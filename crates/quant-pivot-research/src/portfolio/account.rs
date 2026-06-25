//! Decision-time account capital snapshot (the sizing capital base).
//!
//! `AccountSnapshot` is a compute-domain value type, parallel to
//! [`crate::model::SignalCandidate`]: it is produced by the core
//! `AccountProvider` (which performs venue I/O) and consumed by the governed
//! planner for sizing and exposure-net projections. The position / exposure
//! value types are owned by `quant-pivot-models` (`types::account`) because they
//! also back the `quant_account_snapshot` persistence DTOs.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use quant_pivot_models::{
    enums::quant::AccountSource,
    types::{ExposureBreakdown, PositionSnapshot, Usd},
};

/// Real venue account state frozen at a report's `as_of`.
///
/// `equity_usd` is the sizing anchor: `min(net liquidation value, budget cap)`.
/// `exposures` is derived from `positions` and is the planner's starting point
/// for per-market / per-event / per-category cap-room checks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountSnapshot {
    /// Decision time.
    pub as_of: DateTime<Utc>,
    /// Capital-base provenance (always the real Polymarket venue).
    pub source: AccountSource,
    /// Net-liquidation capital base after the budget governance cap.
    pub equity_usd: Usd,
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
        equity_usd: Usd,
        available_usd: Usd,
        reserved_usd: Usd,
        positions: Vec<PositionSnapshot>,
    ) -> Self {
        let exposures = ExposureBreakdown::from_positions(&positions);
        Self {
            as_of,
            source,
            equity_usd,
            available_usd,
            reserved_usd,
            positions,
            exposures,
        }
    }
}
