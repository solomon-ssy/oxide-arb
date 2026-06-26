//! Selection snapshot persistence DTOs.

use crate::{
    entities::quant_market_selection::{SelectionExcludedMarketIds, SelectionIncludedMarketIds},
    enums::{common::MarketCategory, market::MarketStatus},
    types::{
        ContentHash, EventId, MarketId, MarketSelectionId, RuntimeConfigVersionId,
        SelectionExclusionSummary, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Persisted selection snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_market_selection::Entity")]
pub struct MarketSelectionInfo {
    pub market_selection_id: MarketSelectionId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub selector_hash: ContentHash,
    pub market_count: i32,
    pub included_market_ids: SelectionIncludedMarketIds,
    pub excluded_market_ids: SelectionExcludedMarketIds,
    pub exclusion_summary: SelectionExclusionSummary,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    MarketSelectionInfo,
    crate::entities::quant_market_selection::Model,
    {
        market_selection_id,
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

/// Insert payload for `quant_market_selection`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_market_selection::ActiveModel")]
pub struct NewMarketSelection {
    pub market_selection_id: MarketSelectionId,
    pub as_of: DateTime<Utc>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub selector_hash: ContentHash,
    pub market_count: i32,
    pub included_market_ids: SelectionIncludedMarketIds,
    pub excluded_market_ids: SelectionExcludedMarketIds,
    pub exclusion_summary: SelectionExclusionSummary,
}

/// Persisted queryable selection member.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_market_selection_member::Entity")]
pub struct MarketSelectionMemberInfo {
    pub market_selection_id: MarketSelectionId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
}

info_from_model!(MarketSelectionMemberInfo, crate::entities::quant_market_selection_member::Model, {
    market_selection_id, market_id, event_id, category, status, primary_token_id,
    secondary_token_id, liquidity_usd, volume_24h_usd,
});

/// Insert payload for `quant_market_selection_member`.
///
/// Covers every `ActiveModel` column (no DB-managed timestamps); `SeaORM`'s derive
/// emits a redundant `..Default::default()` that triggers `needless_update`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_market_selection_member::ActiveModel")]
pub struct NewMarketSelectionMember {
    pub market_selection_id: MarketSelectionId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
}

/// Runtime aggregate for a selected market selection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSelectionModel {
    pub snapshot: NewMarketSelection,
    pub members: Vec<NewMarketSelectionMember>,
}
