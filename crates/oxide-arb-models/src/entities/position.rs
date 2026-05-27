//! `positions` table entity.

use crate::{
    enums::common::{PositionStatus, Side},
    types::{MarketId, PositionId, Price, Shares, TokenId, Usd},
};
use chrono::{DateTime, Utc};
use oxide_arb_macros::ActiveModelDefaults;
use sea_orm::entity::prelude::*;

const DEFAULT_UNREALIZED_PNL: Usd = Usd::ZERO;
const DEFAULT_REALIZED_PNL: Usd = Usd::ZERO;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel, ActiveModelDefaults)]
#[sea_orm(table_name = "position")]
#[active_defaults(
    generate(position_id, PositionId::generate()),
    default(status, PositionStatus::Open),
    default(unrealized_pnl, DEFAULT_UNREALIZED_PNL),
    default(realized_pnl, DEFAULT_REALIZED_PNL),
    timestamp(opened_at)
)]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub position_id: PositionId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub unrealized_pnl: Usd,
    pub realized_pnl: Usd,
    pub status: PositionStatus,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
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
