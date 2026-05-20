//! `endgame_calibration_buckets` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "endgame_calibration_bucket")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i32,
    #[sea_orm(column_type = "Text")]
    pub price_zone: String,
    #[sea_orm(column_type = "Text")]
    pub duration_bucket: String,
    #[sea_orm(column_type = "Text")]
    pub resolution_rate: String,
    pub sample_size: i32,
    #[sea_orm(column_type = "Text")]
    pub confidence_adjust: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
