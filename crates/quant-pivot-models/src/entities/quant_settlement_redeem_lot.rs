//! `quant_settlement_redeem_lot` table entity.

use crate::{
    enums::quant::OutcomeSide,
    types::{
        OrderIntentId, PositionId, SettlementRedeemId, SettlementRedeemLotId, Shares, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_redeem_lot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_redeem_lot_id: SettlementRedeemLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares_redeemed: Shares,
    pub cost_basis_usd: Usd,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_settlement_redeem::Entity",
        from = "Column::SettlementRedeemId",
        to = "super::quant_settlement_redeem::Column::SettlementRedeemId"
    )]
    SettlementRedeem,
    #[sea_orm(
        belongs_to = "super::quant_position::Entity",
        from = "Column::PositionId",
        to = "super::quant_position::Column::PositionId"
    )]
    Position,
    #[sea_orm(
        belongs_to = "super::quant_order_intent::Entity",
        from = "Column::OrderIntentId",
        to = "super::quant_order_intent::Column::OrderIntentId"
    )]
    OrderIntent,
}

impl Related<super::quant_settlement_redeem::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::SettlementRedeem.def()
    }
}

impl Related<super::quant_position::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Position.def()
    }
}

impl Related<super::quant_order_intent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::OrderIntent.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
