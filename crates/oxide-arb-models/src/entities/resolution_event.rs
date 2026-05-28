//! `resolution_event` table entity.

use crate::types::MarketId;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "resolution_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub resolution_id: String,
    pub market_id: MarketId,
    pub outcome: String,
    pub source: String,
    pub gamma_agrees: Option<bool>,
    pub ctf_agrees: Option<bool>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub evidence: Option<serde_json::Value>,
    pub resolved_at: DateTime<Utc>,
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
