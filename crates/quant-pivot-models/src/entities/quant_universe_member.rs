//! `quant_universe_member` table entity.

use crate::types::{EventId, MarketId, TokenId, UniverseSnapshotId, Usd};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_universe_member")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub universe_snapshot_id: UniverseSnapshotId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    pub event_id: EventId,
    #[sea_orm(column_type = "Text")]
    pub category: String,
    #[sea_orm(column_type = "Text")]
    pub status: String,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_universe_snapshot::Entity",
        from = "Column::UniverseSnapshotId",
        to = "super::quant_universe_snapshot::Column::UniverseSnapshotId"
    )]
    UniverseSnapshot,
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
    #[sea_orm(
        belongs_to = "super::event::Entity",
        from = "Column::EventId",
        to = "super::event::Column::EventId"
    )]
    Event,
}

impl Related<super::quant_universe_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UniverseSnapshot.def()
    }
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl Related<super::event::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Event.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
