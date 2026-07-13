//! Settlement redemption persistence DTOs.

use crate::{
    domain::PositionExit,
    entities::{quant_settlement_redeem, quant_settlement_redeem_lot},
    enums::{
        execution::SettlementRedeemState,
        quant::{ExecutionWalletKind, OutcomeSide},
    },
    types::{
        MarketId, OrderIntentId, PositionId, SettlementBalanceEvidence, SettlementPayoutVector,
        SettlementRedeemId, SettlementRedeemIndexSets, SettlementRedeemLotId, Shares, TokenId, Usd,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// One on-chain CTF redemption batch for a `(condition_id, funder)` pair.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_settlement_redeem::Entity")]
pub struct SettlementRedeemInfo {
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

info_from_model!(
    SettlementRedeemInfo,
    quant_settlement_redeem::Model,
    {
        settlement_redeem_id, market_id, funder_address, wallet_kind, state,
        tx_hash, index_sets_json, payout_vector_json, balance_before_json,
        balance_after_json, payout_usd, gas_fee_pol, attempt_count,
        next_attempt_at, last_error, submitted_at, confirmed_at, failed_at,
        created_at, updated_at,
    }
);

/// Insert payload for `quant_settlement_redeem`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_redeem::ActiveModel")]
pub struct NewSettlementRedeem {
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
}

/// Per-lot allocation within a settlement redemption batch.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_settlement_redeem_lot::Entity")]
pub struct SettlementRedeemLotInfo {
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

info_from_model!(
    SettlementRedeemLotInfo,
    quant_settlement_redeem_lot::Model,
    {
        settlement_redeem_lot_id, settlement_redeem_id, position_id,
        order_intent_id, token_id, side, shares_redeemed, cost_basis_usd,
        payout_usd, realized_pnl_usd, created_at,
    }
);

/// Insert payload for `quant_settlement_redeem_lot`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_settlement_redeem_lot::ActiveModel")]
pub struct NewSettlementRedeemLot {
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
}

/// One position-lot close applied by a confirmed settlement redeem transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettlementRedeemLotWrite {
    pub lot: NewSettlementRedeemLot,
    pub position_exit: PositionExit,
}

/// Atomic ledger write for a confirmed settlement redeem transaction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfirmSettlementRedeem {
    pub settlement_redeem_id: SettlementRedeemId,
    pub balance_after_json: SettlementBalanceEvidence,
    pub payout_usd: Usd,
    pub gas_fee_pol: Option<Decimal>,
    pub confirmed_at: DateTime<Utc>,
    pub lots: Vec<SettlementRedeemLotWrite>,
}
