//! Account capital snapshot persistence DTOs.
//!
//! Backs `quant_account_snapshot`: the decision-time venue capital base and held
//! positions that make report sizing replayable. The position / exposure value
//! types live in [`crate::types::account`] (shared with the research-plane
//! `AccountSnapshot` aggregate).

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::pagination::PageRequest,
    entities::{quant_account_snapshot, quant_equity_snapshot},
    enums::quant::AccountSource,
    types::{
        AccountPositions, AccountSnapshotId, EquitySnapshotId, ExecutionAccountId,
        ExposureBreakdown, Usd, VenuePositionSnapshot,
    },
};

/// Persisted decision-time account capital snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_account_snapshot::Entity")]
pub struct AccountSnapshotInfo {
    pub account_snapshot_id: AccountSnapshotId,
    pub execution_account_id: ExecutionAccountId,
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
    quant_account_snapshot::Model,
    {
        account_snapshot_id,
        execution_account_id,
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
    pub positions: Vec<VenuePositionSnapshot>,
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
        positions: Vec<VenuePositionSnapshot>,
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
    pub execution_account_id: ExecutionAccountId,
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
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
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
    pub incentive_credit_cumulative_usd: Usd,
    pub unrealized_pnl_usd: Usd,
    pub high_water_mark_usd: Usd,
    pub drawdown_pct: Decimal,
    pub account_snapshot_ref: Option<AccountSnapshotId>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    EquitySnapshotInfo,
    quant_equity_snapshot::Model,
    {
        equity_snapshot_id,
        as_of,
        source,
        venue_net_liquidation_usd,
        capital_base_usd,
        available_usd,
        reserved_usd,
        realized_pnl_cumulative_usd,
        incentive_credit_cumulative_usd,
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
    pub incentive_credit_cumulative_usd: Usd,
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

impl EquitySnapshotInfo {
    /// Resolve the monotonic strategy-capital high-water mark.
    #[must_use]
    pub fn high_water_mark(prior: Option<Usd>, capital_base: Usd) -> Usd {
        prior.unwrap_or(capital_base).max(capital_base)
    }

    /// Merge a caller-computed peak with the durable peak and current capital.
    #[must_use]
    pub fn merge_high_water_mark(prior: Option<Usd>, computed: Usd, capital_base: Usd) -> Usd {
        prior
            .map_or(computed, |durable| durable.max(computed))
            .max(capital_base)
    }

    /// Compute peak-to-trough strategy-capital drawdown in `[0, 1]`.
    #[must_use]
    pub fn drawdown(capital_base: Usd, high_water_mark: Usd) -> Decimal {
        if high_water_mark.is_zero() || capital_base >= high_water_mark {
            return Decimal::ZERO;
        }
        ((high_water_mark - capital_base).inner() / high_water_mark.inner())
            .clamp(Decimal::ZERO, Decimal::ONE)
    }
}

#[cfg(test)]
mod equity_metrics_tests {
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::EquitySnapshotInfo;
    use crate::types::Usd;

    #[test]
    fn first_high_zero_drawdown() {
        let hwm = EquitySnapshotInfo::high_water_mark(None, Usd::new(dec!(12500)));
        assert_eq!(hwm, Usd::new(dec!(12500)));
        assert_eq!(
            EquitySnapshotInfo::drawdown(Usd::new(dec!(12500)), hwm),
            Decimal::ZERO
        );
    }

    #[test]
    fn drawdown_peak_capital_ratio() {
        let hwm = EquitySnapshotInfo::high_water_mark(None, Usd::new(dec!(12500)));
        assert_eq!(
            EquitySnapshotInfo::drawdown(Usd::new(dec!(10000)), hwm),
            dec!(0.2)
        );
    }

    #[test]
    fn high_water_mark_max() {
        let first = EquitySnapshotInfo::high_water_mark(None, Usd::new(dec!(10000)));
        let second = EquitySnapshotInfo::high_water_mark(Some(first), Usd::new(dec!(9000)));
        assert_eq!(second, Usd::new(dec!(10000)));
        let third = EquitySnapshotInfo::high_water_mark(Some(second), Usd::new(dec!(11000)));
        assert_eq!(third, Usd::new(dec!(11000)));
    }
}
