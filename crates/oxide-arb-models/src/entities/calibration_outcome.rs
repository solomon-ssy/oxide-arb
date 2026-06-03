//! `endgame_calibration_outcomes` table entity.

use crate::{
    enums::{
        calibration::{DurationBucket, PriceZone},
        common::MarketCategory,
    },
    types::{MarketId, OpportunityId, Price, Probability, TradeId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "endgame_calibration_outcome")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub trade_id: TradeId,
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub category: MarketCategory,
    pub price_zone: PriceZone,
    pub duration_bucket: DurationBucket,
    pub predicted_yes: bool,
    pub actual_yes: Option<bool>,
    pub entry_price: Price,
    pub confidence_at_entry: Probability,
    pub convergence_secs: i32,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
