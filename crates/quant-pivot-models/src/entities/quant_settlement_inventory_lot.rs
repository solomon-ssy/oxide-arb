//! `quant_settlement_inventory_lot` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_execution_account, quant_order_intent, quant_settlement_redeem,
    quant_strategy_position_lot,
};
use crate::{
    enums::quant::{ExitSettlementMode, OutcomeSide, RedeemPolicy},
    types::{
        ContentHash, ExecutionAccountId, OrderIntentId, SettlementInventoryLotId,
        SettlementRedeemId, Shares, StrategyPositionLotId, TokenId, Usd,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_inventory_lot")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_inventory_lot_id: SettlementInventoryLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub inventory_digest: ContentHash,
    pub contributor_lots_digest: ContentHash,
    pub execution_account_id: ExecutionAccountId,
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub order_intent_id: OrderIntentId,
    pub token_id: TokenId,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub cost_basis_usd: Usd,
    pub settlement_mode: ExitSettlementMode,
    pub redeem_policy: RedeemPolicy,
    pub position_version_at: DateTime<Utc>,
    pub intent_version_at: DateTime<Utc>,
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
        relation_enum = "ExecutionAccount",
        from = "execution_account_id",
        to = "execution_account_id"
    )]
    pub execution_account: BelongsTo<quant_execution_account::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Position",
        from = "strategy_position_lot_id",
        to = "strategy_position_lot_id"
    )]
    pub position: BelongsTo<quant_strategy_position_lot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "OrderIntent",
        from = "order_intent_id",
        to = "order_intent_id"
    )]
    pub order_intent: BelongsTo<quant_order_intent::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
