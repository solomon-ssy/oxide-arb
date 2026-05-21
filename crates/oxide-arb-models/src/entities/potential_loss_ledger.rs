//! `potential_loss_ledger` table entity.

use crate::enums::common::LedgerStatus;
use crate::types::{MarketId, Price, Shares, TokenId, Usd};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "potential_loss_ledger")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false, column_type = "Text")]
    pub ledger_id: String,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub shares: Shares,
    pub entry_price: Price,
    pub max_loss_usd: Usd,
    pub status: LedgerStatus,
    pub created_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
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
