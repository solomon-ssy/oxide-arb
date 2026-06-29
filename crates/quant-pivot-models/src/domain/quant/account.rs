//! Account capital snapshot persistence DTOs.
//!
//! Backs `quant_account_snapshot`: the decision-time venue capital base and held
//! positions that make report sizing replayable. The position / exposure value
//! types live in [`crate::types::account`] (shared with the research-plane
//! `AccountSnapshot` aggregate).

use crate::{
    enums::quant::AccountSource,
    types::{AccountPositions, AccountSnapshotId, ExposureBreakdown, PositionSnapshot, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted decision-time account capital snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_account_snapshot::Entity")]
pub struct AccountSnapshotInfo {
    pub account_snapshot_id: AccountSnapshotId,
    pub as_of: DateTime<Utc>,
    pub source: AccountSource,
    pub equity_usd: Usd,
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
        equity_usd,
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
    pub equity_usd: Usd,
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
        equity_usd: Usd,
        available_usd: Usd,
        reserved_usd: Usd,
        positions: Vec<PositionSnapshot>,
    ) -> Self {
        Self {
            as_of,
            source,
            equity_usd,
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
    pub equity_usd: Usd,
    pub available_usd: Usd,
    pub reserved_usd: Usd,
    pub positions_json: AccountPositions,
    pub exposures_json: ExposureBreakdown,
}
