//! `positions` table entity.

use crate::enums::common::Side;
use crate::types::{MarketId, Price, Shares, TokenId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "position")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub market_id: MarketId,
    #[sea_orm(primary_key, auto_increment = false)]
    pub token_id: TokenId,
    pub side: Side,
    pub size: Shares,
    pub avg_entry_price: Price,
    pub cost_basis: Usd,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
