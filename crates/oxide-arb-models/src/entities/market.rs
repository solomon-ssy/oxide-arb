//! `markets` table entity.

use crate::enums::common::{MarketCategory, TickSize};
use crate::enums::market::MarketStatus;
use crate::types::{EventId, MarketId, TokenId};
use chrono::{DateTime, Utc};
use oxide_arb_macros::ActiveModelDefaults;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(
    Clone, Debug, PartialEq, Eq, DeriveEntityModel, ActiveModelDefaults, Serialize, Deserialize,
)]
#[sea_orm(table_name = "market")]
#[active_defaults(timestamp(created_at), timestamp(updated_at, always))]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub question: String,
    #[sea_orm(column_type = "Text")]
    pub slug: String,
    pub category: MarketCategory,
    pub status: MarketStatus,
    #[sea_orm(column_type = "Text", nullable)]
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub end_date: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::event::Entity",
        from = "Column::EventId",
        to = "super::event::Column::EventId"
    )]
    Event,
    #[sea_orm(has_many = "super::trade::Entity")]
    Trade,
    #[sea_orm(has_many = "super::position::Entity")]
    Position,
}

impl Related<super::event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Event.def()
    }
}

impl Related<super::trade::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Trade.def()
    }
}

impl Related<super::position::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Position.def()
    }
}
