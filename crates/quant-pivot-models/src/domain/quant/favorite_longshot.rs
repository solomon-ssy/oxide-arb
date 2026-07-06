//! Favorite-longshot bias-table artifact persistence DTOs (Phase 11.2.1).

use crate::types::{ContentHash, FavoriteLongshotBiasTableId};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Frozen, content-addressed favorite-longshot bias-table row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_favorite_longshot_bias_table::Entity")]
pub struct FavoriteLongshotBiasTableInfo {
    pub bias_table_id: FavoriteLongshotBiasTableId,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub category_count: i64,
    pub total_sample_count: i64,
    pub by_category: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    FavoriteLongshotBiasTableInfo,
    crate::entities::quant_favorite_longshot_bias_table::Model,
    {
        bias_table_id,
        content_hash,
        fit_window_start,
        fit_window_end,
        calibration_split_hash,
        category_count,
        total_sample_count,
        by_category,
        created_at,
    }
);

/// Insert payload for `quant_favorite_longshot_bias_table`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_favorite_longshot_bias_table::ActiveModel")]
pub struct NewFavoriteLongshotBiasTable {
    pub bias_table_id: FavoriteLongshotBiasTableId,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub category_count: i64,
    pub total_sample_count: i64,
    pub by_category: serde_json::Value,
}
