//! `quant_favorite_longshot_bias_table` table entity.

use crate::types::{ContentHash, FavoriteLongshotBiasTableId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_favorite_longshot_bias_table")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub bias_table_id: FavoriteLongshotBiasTableId,
    pub content_hash: ContentHash,
    pub fit_window_start: DateTime<Utc>,
    pub fit_window_end: DateTime<Utc>,
    pub calibration_split_hash: ContentHash,
    pub category_count: i64,
    pub total_sample_count: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub by_category: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
