//! `endgame_calibration_buckets` table entity.

use crate::enums::calibration::{DurationBucket, PriceZone};
use crate::enums::common::MarketCategory;
use crate::types::Probability;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "endgame_calibration_bucket")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub total_count: i32,
    pub correct_count: i32,
    pub alpha_prior: Probability,
    pub beta_prior: Probability,
    pub posterior_mean: Option<Probability>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
