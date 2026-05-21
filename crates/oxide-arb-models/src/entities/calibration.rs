//! `endgame_calibration_buckets` table entity.

use crate::enums::common::MarketCategory;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "endgame_calibration_bucket")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub category: MarketCategory,
    #[sea_orm(column_type = "Text")]
    pub price_zone: String,
    #[sea_orm(column_type = "Text")]
    pub duration_bucket: String,
    pub total_count: i32,
    pub correct_count: i32,
    #[sea_orm(column_type = "Text")]
    pub alpha_prior: String,
    #[sea_orm(column_type = "Text")]
    pub beta_prior: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub posterior_mean: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
