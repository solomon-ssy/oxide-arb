//! Account capital snapshot persistence DTOs.
//!
//! Backs `quant_account_snapshot`: the decision-time venue capital base and held
//! positions that make report sizing replayable. The position / exposure value
//! types live in [`crate::types::account`] (shared with the research-plane
//! `AccountSnapshot` aggregate).

use crate::domain::pagination::PageRequest;
use crate::{
    enums::quant::AccountSource,
    types::{
        AccountPositions, AccountSnapshotId, EquitySnapshotId, ExposureBreakdown, PositionSnapshot,
        Usd,
    },
};
use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted decision-time account capital snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_account_snapshot::Entity")]
pub struct AccountSnapshotInfo {
    pub account_snapshot_id: AccountSnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub positions_json: AccountPositions,
    pub exposures_json: ExposureBreakdown,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    AccountSnapshotInfo,
    crate::entities::quant_account_snapshot::Model,
    {
        account_snapshot_id,
        as_of,
        source,
        venue_net_liquidation_usd,
        capital_base_usd,
        available_usd,
        reserved_usd,
        positions_json,
        exposures_json,
        created_at,
    }
);

/// Live venue account facts at decision time (models-owned contract).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveAccountSnapshot {
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub positions: Vec<PositionSnapshot>,
    pub exposures: ExposureBreakdown,
}

impl LiveAccountSnapshot {
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
        Self {
            as_of,
            source,
            venue_net_liquidation_usd,
            capital_base_usd,
            available_usd,
            reserved_usd,
            exposures: ExposureBreakdown::from_positions(&positions),
            positions,
        }
    }
}

/// Insert payload for `quant_account_snapshot`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_account_snapshot::ActiveModel")]
pub struct NewAccountSnapshot {
    pub account_snapshot_id: AccountSnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub positions_json: AccountPositions,
    pub exposures_json: ExposureBreakdown,
}

/// Persisted strategy-capital equity curve snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_equity_snapshot::Entity")]
pub struct EquitySnapshotInfo {
    pub equity_snapshot_id: EquitySnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub realized_pnl_cumulative_usd: Usd,
    pub unrealized_pnl_usd: Usd,
    pub high_water_mark_usd: Usd,
    pub drawdown_pct: Decimal,
    pub account_snapshot_ref: Option<AccountSnapshotId>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    EquitySnapshotInfo,
    crate::entities::quant_equity_snapshot::Model,
    {
        equity_snapshot_id,
        as_of,
        source,
        venue_net_liquidation_usd,
        capital_base_usd,
        available_usd,
        reserved_usd,
        realized_pnl_cumulative_usd,
        unrealized_pnl_usd,
        high_water_mark_usd,
        drawdown_pct,
        account_snapshot_ref,
        created_at,
    }
);

/// Insert payload for `quant_equity_snapshot`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_equity_snapshot::ActiveModel")]
pub struct NewEquitySnapshot {
    pub equity_snapshot_id: EquitySnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub realized_pnl_cumulative_usd: Usd,
    pub unrealized_pnl_usd: Usd,
    pub high_water_mark_usd: Usd,
    pub drawdown_pct: Decimal,
    pub account_snapshot_ref: Option<AccountSnapshotId>,
}

/// Equity snapshot history filters.
#[derive(Debug, Clone, Serialize, Deserialize, NormalizePageQuery)]
pub struct EquitySnapshotQuery {
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Monotonic high-water mark over strategy `capital_base_usd`.
///
/// When no prior peak exists, the current capital base seeds the mark.
#[must_use]
pub fn capital_hwm(prior_high_water_mark_usd: Option<Usd>, capital_base_usd: Usd) -> Usd {
    prior_high_water_mark_usd
        .unwrap_or(capital_base_usd)
        .max(capital_base_usd)
}

/// Monotonic HWM merge at insert when the caller already computed a candidate peak.
#[must_use]
pub fn hwm_merge(
    prior_high_water_mark_usd: Option<Usd>,
    computed_high_water_mark_usd: Usd,
    capital_base_usd: Usd,
) -> Usd {
    prior_high_water_mark_usd
        .map_or(computed_high_water_mark_usd, |prior| {
            prior.max(computed_high_water_mark_usd)
        })
        .max(capital_base_usd)
}

/// Peak-to-trough drawdown of strategy capital base relative to HWM, ∈ `[0, 1]`.
#[must_use]
pub fn capital_drawdown(capital_base_usd: Usd, high_water_mark_usd: Usd) -> Decimal {
    if high_water_mark_usd.is_zero() || capital_base_usd >= high_water_mark_usd {
        return Decimal::ZERO;
    }
    ((high_water_mark_usd - capital_base_usd).inner() / high_water_mark_usd.inner())
        .clamp(Decimal::ZERO, Decimal::ONE)
}

#[cfg(test)]
mod equity_metrics_tests {
    use super::{capital_drawdown, capital_hwm};
    use crate::types::Usd;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn first_or_new_high_water_mark_has_zero_drawdown() {
        let hwm = capital_hwm(None, Usd::new(dec!(12500)));
        assert_eq!(hwm, Usd::new(dec!(12500)));
        assert_eq!(capital_drawdown(Usd::new(dec!(12500)), hwm), Decimal::ZERO);
    }

    #[test]
    fn drawdown_is_peak_to_current_capital_base_ratio() {
        let hwm = capital_hwm(None, Usd::new(dec!(12500)));
        assert_eq!(capital_drawdown(Usd::new(dec!(10000)), hwm), dec!(0.2));
    }

    #[test]
    fn high_water_mark_is_monotonic_max() {
        let first = capital_hwm(None, Usd::new(dec!(10000)));
        let second = capital_hwm(Some(first), Usd::new(dec!(9000)));
        assert_eq!(second, Usd::new(dec!(10000)));
        let third = capital_hwm(Some(second), Usd::new(dec!(11000)));
        assert_eq!(third, Usd::new(dec!(11000)));
    }
}
