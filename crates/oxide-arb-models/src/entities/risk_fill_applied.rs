//! `risk_fill_applied` table entity — durable risk-Fill idempotency marker.

use crate::types::TradeId;
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "risk_fill_applied")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub trade_id: TradeId,
    pub applied_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::trade::Entity",
        from = "Column::TradeId",
        to = "super::trade::Column::TradeId"
    )]
    Trade,
}

impl Related<super::trade::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Trade.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
