//! `quant_market_selection_member` table entity.

use crate::{
    enums::{common::MarketCategory, market::MarketStatus},
    types::{EventId, MarketId, MarketSelectionId, TokenId, Usd},
};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_market_selection_member")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_selection_id: MarketSelectionId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    pub event_id: EventId,
    pub category: MarketCategory,
    pub status: MarketStatus,
    pub primary_token_id: TokenId,
    pub secondary_token_id: Option<TokenId>,
    pub liquidity_usd: Option<Usd>,
    pub volume_24h_usd: Option<Usd>,

    #[sea_orm(
        belongs_to,
        relation_enum = "MarketSelection",
        from = "market_selection_id",
        to = "market_selection_id"
    )]
    pub market_selection: BelongsTo<super::quant_market_selection::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Event",
        from = "event_id",
        to = "event_id"
    )]
    pub event: BelongsTo<super::event::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
