//! `events` table entity.

use crate::enums::common::MarketCategory;
use crate::types::EventId;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub title: String,
    #[sea_orm(column_type = "Text")]
    pub slug: String,
    pub category: MarketCategory,
    #[sea_orm(column_type = "Text")]
    pub status: String,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub raw_gamma: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::market::Entity")]
    Market,
    #[sea_orm(has_many = "super::trade::Entity")]
    Trade,
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl Related<super::trade::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Trade.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
