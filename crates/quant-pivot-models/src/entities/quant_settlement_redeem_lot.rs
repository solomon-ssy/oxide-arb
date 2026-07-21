//! `quant_settlement_redeem_lot` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_order_intent, quant_position, quant_settlement_redeem};
use crate::{
    enums::quant::OutcomeSide,
    types::{
        OrderIntentId, PositionId, SettlementRedeemId, SettlementRedeemLotId, Shares, TokenId, Usd,
    },
};

#[sea_orm::model]
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

    #[sea_orm(
        belongs_to,
        relation_enum = "SettlementRedeem",
        from = "settlement_redeem_id",
        to = "settlement_redeem_id"
    )]
    pub settlement_redeem: BelongsTo<quant_settlement_redeem::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Position",
        from = "position_id",
        to = "position_id"
    )]
    pub position: BelongsTo<quant_position::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<quant_order_intent::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
