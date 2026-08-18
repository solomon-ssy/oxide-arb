//! Venue account HTTP contract types (live + persisted snapshots).

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use schemars::JsonSchema;
use serde::Serialize;

use crate::{
    domain::quant::{AccountSnapshotInfo, EquitySnapshotInfo, LiveAccountSnapshot},
    enums::{common::MarketCategory, quant::AccountSource},
    types::{AccountSnapshotId, EquitySnapshotId, ExposureBreakdown, Usd, VenuePositionSnapshot},
};

/// Outbound projection of one venue-held outcome position at decision time.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct VenuePositionSnapshotView {
    pub token_id: String,
    pub market_id: String,
    pub event_id: Option<String>,
    pub category: MarketCategory,
    pub outcome: String,
    pub size: String,
    pub avg_price: String,
    pub cur_price: String,
    pub current_value: String,
    pub redeemable: bool,
}

impl From<&VenuePositionSnapshot> for VenuePositionSnapshotView {
    fn from(position: &VenuePositionSnapshot) -> Self {
        Self {
            token_id: position.token_id.to_string(),
            market_id: position.market_id.to_string(),
            event_id: position.event_id.as_ref().map(ToString::to_string),
            category: position.category,
            outcome: position.outcome.clone(),
            size: position.size.to_string(),
            avg_price: position.avg_price.to_string(),
            cur_price: position.cur_price.to_string(),
            current_value: position.current_value.to_string(),
            redeemable: position.redeemable,
        }
    }
}

/// Persisted decision-time venue account snapshot (immutable audit evidence).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct AccountSnapshotView {
    pub account_snapshot_id: AccountSnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub positions: Vec<VenuePositionSnapshotView>,
    pub exposures: ExposureBreakdown,
    pub created_at: DateTime<Utc>,
}

impl From<AccountSnapshotInfo> for AccountSnapshotView {
    fn from(info: AccountSnapshotInfo) -> Self {
        Self {
            account_snapshot_id: info.account_snapshot_id,
            as_of: info.as_of,
            source: info.source,
            venue_net_liquidation_usd: info.venue_net_liquidation_usd,
            capital_base_usd: info.capital_base_usd,
            available_usd: info.available_usd,
            reserved_usd: info.reserved_usd,
            positions: info
                .positions_json
                .0
                .iter()
                .map(VenuePositionSnapshotView::from)
                .collect(),
            exposures: info.exposures_json,
            created_at: info.created_at,
        }
    }
}

/// Live venue account read (re-fetched on every request; not persisted).
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct LiveAccountView {
    pub fetched_at: DateTime<Utc>,
    pub budget_cap_usd: Usd,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub positions: Vec<VenuePositionSnapshotView>,
    pub exposures: ExposureBreakdown,
}

impl LiveAccountView {
    #[must_use]
    pub fn from_live(
        fetched_at: DateTime<Utc>,
        budget_cap_usd: Usd,
        snapshot: LiveAccountSnapshot,
    ) -> Self {
        Self {
            fetched_at,
            budget_cap_usd,
            as_of: snapshot.as_of,
            source: snapshot.source,
            venue_net_liquidation_usd: snapshot.venue_net_liquidation_usd,
            capital_base_usd: snapshot.capital_base_usd,
            available_usd: snapshot.available_usd,
            reserved_usd: snapshot.reserved_usd,
            positions: snapshot
                .positions
                .iter()
                .map(VenuePositionSnapshotView::from)
                .collect(),
            exposures: snapshot.exposures,
        }
    }
}

/// Persisted strategy-capital equity curve snapshot.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct EquitySnapshotView {
    pub equity_snapshot_id: EquitySnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub venue_net_liquidation_usd: Usd,
    pub capital_base_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub realized_pnl_cumulative_usd: Usd,
    /// Wallet-confirmed incentive credits, shown for attribution only.
    /// This amount is already reflected in venue NLV and must not be added to
    /// capital base, available cash, or realized `PnL` a second time.
    pub incentive_credit_cumulative_usd: Usd,
    pub unrealized_pnl_usd: Usd,
    pub high_water_mark_usd: Usd,
    #[serde(with = "rust_decimal::serde::str")]
    #[schemars(with = "String")]
    pub drawdown_pct: Decimal,
    pub account_snapshot_ref: Option<AccountSnapshotId>,
    pub created_at: DateTime<Utc>,
}

impl From<EquitySnapshotInfo> for EquitySnapshotView {
    fn from(info: EquitySnapshotInfo) -> Self {
        Self {
            equity_snapshot_id: info.equity_snapshot_id,
            as_of: info.as_of,
            source: info.source,
            venue_net_liquidation_usd: info.venue_net_liquidation_usd,
            capital_base_usd: info.capital_base_usd,
            available_usd: info.available_usd,
            reserved_usd: info.reserved_usd,
            realized_pnl_cumulative_usd: info.realized_pnl_cumulative_usd,
            incentive_credit_cumulative_usd: info.incentive_credit_cumulative_usd,
            unrealized_pnl_usd: info.unrealized_pnl_usd,
            high_water_mark_usd: info.high_water_mark_usd,
            drawdown_pct: info.drawdown_pct,
            account_snapshot_ref: info.account_snapshot_ref,
            created_at: info.created_at,
        }
    }
}
