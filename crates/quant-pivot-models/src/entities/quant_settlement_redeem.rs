//! `quant_settlement_redeem` table entity.

use crate::{
    enums::{execution::SettlementRedeemState, quant::ExecutionWalletKind},
    types::{
        EvmAddress, EvmTransactionHash, MarketId, SettlementBalanceEvidence,
        SettlementPayoutVector, SettlementRedeemId, SettlementRedeemIndexSets, Usd,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_redeem")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub funder_address: EvmAddress,
    pub wallet_kind: ExecutionWalletKind,
    pub state: SettlementRedeemState,
    pub tx_hash: Option<EvmTransactionHash>,
    pub index_sets_json: SettlementRedeemIndexSets,
    pub payout_vector_json: SettlementPayoutVector,
    pub balance_before_json: SettlementBalanceEvidence,
    pub balance_after_json: Option<SettlementBalanceEvidence>,
    pub payout_usd: Usd,
    pub gas_fee_pol: Option<Decimal>,
    pub attempt_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "RedeemLot")]
    pub redeem_lot: HasMany<super::quant_settlement_redeem_lot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Market",
        from = "market_id",
        to = "market_id"
    )]
    pub market: BelongsTo<super::market::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
