//! `endgame_calibration_outcomes` table entity.
//!
//! Records individual market resolution outcomes used to update
//! calibration bucket resolution rates.

use crate::types::MarketId;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "endgame_calibration_outcome")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i64,
    pub market_id: MarketId,
    #[sea_orm(column_type = "Text")]
    pub price_zone: String,
    #[sea_orm(column_type = "Text")]
    pub duration_bucket: String,
    pub prediction_correct: bool,
    pub settlement_price: Decimal,
    pub entry_price: Decimal,
    pub resolved_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::ConditionId"
    )]
    Market,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
