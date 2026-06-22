//! Universe snapshot persistence DTOs.

use crate::{
    entities::quant_universe_snapshot::{
        UniverseExcludedMarketIds, UniverseExclusionSummary, UniverseIncludedMarketIds,
    },
    types::{EventId, MarketId, RuntimeConfigVersionId, TokenId, UniverseSnapshotId, Usd},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted universe snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_universe_snapshot::Entity")]
pub struct UniverseSnapshotInfo {
    pub universe_snapshot_id: UniverseSnapshotId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub selector_hash: String,
    pub market_count: i32,
    pub included_market_ids: UniverseIncludedMarketIds,
    pub excluded_market_ids: UniverseExcludedMarketIds,
    pub exclusion_summary: UniverseExclusionSummary,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    UniverseSnapshotInfo,
    crate::entities::quant_universe_snapshot::Model,
    {
        universe_snapshot_id,
        as_of,
        runtime_config_version_id,
        selector_hash,
        market_count,
        included_market_ids,
        excluded_market_ids,
        exclusion_summary,
        created_at,
    }
);

/// Insert payload for `quant_universe_snapshot`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_universe_snapshot::ActiveModel")]
pub struct NewUniverseSnapshot {
    pub universe_snapshot_id: UniverseSnapshotId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub selector_hash: String,
    pub market_count: i32,
    pub included_market_ids: UniverseIncludedMarketIds,
    pub excluded_market_ids: UniverseExcludedMarketIds,
    pub exclusion_summary: UniverseExclusionSummary,
}

/// Persisted queryable universe member.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_universe_member::Entity")]
pub struct UniverseMemberInfo {
    pub universe_snapshot_id: UniverseSnapshotId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: String,
    pub status: String,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
    pub reason: String,
}

info_from_model!(UniverseMemberInfo, crate::entities::quant_universe_member::Model, {
    universe_snapshot_id, market_id, event_id, category, status, primary_token_id,
    secondary_token_id, liquidity_usd, volume_24h_usd, reason,
});

/// Insert payload for `quant_universe_member`.
///
/// Covers every `ActiveModel` column (no DB-managed timestamps); `SeaORM`'s derive
/// emits a redundant `..Default::default()` that triggers `needless_update`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_universe_member::ActiveModel")]
pub struct NewUniverseMember {
    pub universe_snapshot_id: UniverseSnapshotId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: String,
    pub status: String,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
    pub reason: String,
}

/// Runtime aggregate for a selected market universe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UniverseSnapshotModel {
    pub snapshot: NewUniverseSnapshot,
    pub members: Vec<NewUniverseMember>,
}
