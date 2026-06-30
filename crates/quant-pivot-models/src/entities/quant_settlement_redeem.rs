//! `quant_settlement_redeem` table entity.

use crate::{
    enums::{execution::SettlementRedeemState, quant::ExecutionWalletKind},
    types::{
        MarketId, SettlementBalanceEvidence, SettlementPayoutVector, SettlementRedeemId,
        SettlementRedeemIndexSets, Usd,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_settlement_redeem")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub funder_address: String,
    pub wallet_kind: ExecutionWalletKind,
    pub state: SettlementRedeemState,
    pub tx_hash: Option<String>,
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
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(has_many = "super::quant_settlement_redeem_lot::Entity")]
    RedeemLot,
    #[sea_orm(
        belongs_to = "super::market::Entity",
        from = "Column::MarketId",
        to = "super::market::Column::MarketId"
    )]
    Market,
}

impl Related<super::quant_settlement_redeem_lot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RedeemLot.def()
    }
}

impl Related<super::market::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Market.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
