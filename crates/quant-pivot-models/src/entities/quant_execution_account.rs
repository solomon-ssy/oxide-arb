//! `quant_execution_account` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_account_snapshot, quant_order_intent, quant_position, quant_settlement_external_cursor,
    quant_settlement_governed_action, quant_settlement_inventory_lot, quant_settlement_redeem,
};
use crate::{
    enums::quant::ExecutionWalletKind,
    types::{ContentHash, EvmAddress, EvmCodeHash, ExecutionAccountId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_execution_account")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub execution_account_id: ExecutionAccountId,
    pub chain_id: i64,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub owner_address: EvmAddress,
    pub controller_address: EvmAddress,
    pub wallet_factory_address: Option<EvmAddress>,
    pub wallet_implementation_code_hash: Option<EvmCodeHash>,
    pub identity_digest: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "AccountSnapshot")]
    pub account_snapshot: HasMany<quant_account_snapshot::Entity>,
    #[sea_orm(has_many, relation_enum = "OrderIntent")]
    pub order_intent: HasMany<quant_order_intent::Entity>,
    #[sea_orm(has_many, relation_enum = "Position")]
    pub position: HasMany<quant_position::Entity>,
    #[sea_orm(has_many, relation_enum = "SettlementRedeem")]
    pub settlement_redeem: HasMany<quant_settlement_redeem::Entity>,
    #[sea_orm(has_many, relation_enum = "SettlementInventoryLot")]
    pub settlement_inventory_lot: HasMany<quant_settlement_inventory_lot::Entity>,
    #[sea_orm(has_many, relation_enum = "GovernedAction")]
    pub governed_action: HasMany<quant_settlement_governed_action::Entity>,
    #[sea_orm(has_many, relation_enum = "ExternalCursor")]
    pub external_cursor: HasMany<quant_settlement_external_cursor::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
