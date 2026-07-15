//! `markets` table entity.

use crate::{
    enums::{
        common::{MarketCategory, TickSize},
        market::MarketStatus,
    },
    types::{EventId, MarketId, TokenId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, Serialize, Deserialize)]
#[sea_orm(table_name = "market")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub question: String,
    #[sea_orm(column_type = "Text")]
    pub slug: String,
    /// Market rules text (resolution-source grounding anchor; 11.2.2).
    #[sea_orm(column_type = "Text", nullable)]
    pub description: Option<String>,
    /// Category memberships inherited from the parent event's Gamma tags.
    pub categories: Vec<MarketCategory>,
    pub status: MarketStatus,
    #[sea_orm(column_type = "Text", nullable)]
    pub outcome: Option<String>,
    pub yes_token_id: TokenId,
    pub no_token_id: TokenId,
    pub tick_size: TickSize,
    pub neg_risk: bool,
    pub start_date: Option<DateTime<Utc>>,
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
}

impl Related<super::event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Event.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
