//! Venue account HTTP contract types (live + persisted snapshots).

use crate::{
    domain::{AccountSnapshotInfo, LiveAccountSnapshot},
    enums::{common::MarketCategory, quant::AccountSource},
    types::{AccountSnapshotId, ExposureBreakdown, PositionSnapshot, Usd},
};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Outbound projection of one venue-held outcome position at decision time.
#[derive(Debug, Clone, Serialize)]
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

impl From<&PositionSnapshot> for VenuePositionSnapshotView {
    fn from(position: &PositionSnapshot) -> Self {
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
#[derive(Debug, Clone, Serialize)]
pub struct AccountSnapshotView {
    pub account_snapshot_id: AccountSnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub equity_usd: Usd,
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
            equity_usd: info.equity_usd,
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
#[derive(Debug, Clone, Serialize)]
pub struct LiveAccountView {
    pub fetched_at: DateTime<Utc>,
    pub budget_cap_usd: Usd,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub equity_usd: Usd,
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
            equity_usd: snapshot.equity_usd,
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
