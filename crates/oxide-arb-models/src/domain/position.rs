//! Position tracking domain models.
//!
//! `NewPosition` derives `DeriveIntoActiveModel` for clean DTO→ActiveModel
//! conversion. System fields (`position_id`, status, timestamps, `PnL` defaults)
//! are populated by `ActiveModelBehavior::before_save`.

use crate::{
    enums::{
        common::{
            PositionStatus, RedeemStatus, SettlementAccountingStatus, SettlementTrigger, Side,
        },
        risk::ReservationStatus,
    },
    types::{MarketId, PositionId, Price, ReservationId, Shares, TokenId, TradeId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read model ──────────────────────────────────────────────────────

/// DB row projection for the `position` table.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::position::Entity")]
pub struct PositionInfo {
    pub position_id: PositionId,
    pub trade_id: TradeId,
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
    pub winning_token_id: Option<TokenId>,
    pub settlement_payout_usd: Option<Usd>,
    pub redeem_tx_hash: Option<String>,
    pub redeem_status: RedeemStatus,
    pub redeem_attempts: i32,
    pub oracle_verdict: Option<serde_json::Value>,
    pub settlement_trigger: Option<SettlementTrigger>,
    pub settlement_accounting_status: SettlementAccountingStatus,
    pub settlement_accounting_error: Option<String>,
    pub settlement_accounted_at: Option<DateTime<Utc>>,
    pub redeem_terminal_reason: Option<String>,
}

info_from_model!(PositionInfo, crate::entities::position::Model, {
    position_id, trade_id, market_id, token_id, side, shares, avg_entry_price,
    total_cost_usd, total_fees_usd, unrealized_pnl, realized_pnl,
    status, opened_at, closed_at, settled_at, winning_token_id,
    settlement_payout_usd, redeem_tx_hash, redeem_status, redeem_attempts,
    oracle_verdict, settlement_trigger,
    settlement_accounting_status, settlement_accounting_error,
    settlement_accounted_at, redeem_terminal_reason,
});

/// Capital reservation for a pending trade execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExposureReservation {
    pub reservation_id: ReservationId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub trade_id: TradeId,
    pub reserved_usd: Usd,
    pub status: ReservationStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// ── Repository Write DTOs ────────────────────────────────────────────

/// All fields required to open a new position.
///
/// Derives `DeriveIntoActiveModel` — system fields (`position_id`, status,
/// `opened_at`, unrealized/realized `PnL` defaults) are filled by the entity's
/// `ActiveModelBehavior::before_save`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::position::ActiveModel")]
pub struct NewPosition {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub redeem_status: RedeemStatus,
}

/// Fields that can change when a position is updated (add/reduce/close/settle).
#[derive(Debug, Clone, Default)]
pub struct UpdatePosition {
    pub shares: Option<Shares>,
    pub avg_entry_price: Option<Price>,
    pub total_cost_usd: Option<Usd>,
    pub total_fees_usd: Option<Usd>,
    pub unrealized_pnl: Option<Usd>,
    pub realized_pnl: Option<Usd>,
    pub status: Option<PositionStatus>,
    pub closed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
    pub winning_token_id: Option<TokenId>,
    pub settlement_payout_usd: Option<Usd>,
    pub redeem_tx_hash: Option<String>,
    pub redeem_status: Option<RedeemStatus>,
    pub redeem_attempts: Option<i32>,
    pub oracle_verdict: Option<serde_json::Value>,
    pub settlement_trigger: Option<SettlementTrigger>,
    pub settlement_accounting_status: Option<SettlementAccountingStatus>,
    pub settlement_accounting_error: Option<String>,
    pub settlement_accounted_at: Option<DateTime<Utc>>,
    pub redeem_terminal_reason: Option<String>,
}

/// Atomic payload for closing the open-position lifecycle at market settlement.
#[derive(Debug, Clone)]
pub struct SettlePositionParams {
    pub winning_token_id: TokenId,
    pub settlement_payout_usd: Usd,
    pub realized_pnl: Decimal,
    pub redeem_tx_hash: Option<String>,
    pub redeem_status: RedeemStatus,
    pub settlement_trigger: SettlementTrigger,
    pub oracle_verdict: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct MarkRedeemedParams {
    pub winning_token_id: TokenId,
    pub settlement_payout_usd: Usd,
    pub realized_pnl: Usd,
    pub redeem_tx_hash: Option<String>,
    pub redeem_status: RedeemStatus,
    pub settlement_trigger: SettlementTrigger,
    pub redeem_terminal_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettledPositionStats {
    pub realized_pnl: Usd,
    pub total_payout: Usd,
    pub total_cost: Usd,
    pub total_fees: Usd,
    pub settled_position_count: u32,
    pub winning_position_count: u32,
    pub losing_position_count: u32,
    pub unsettled_position_count: u32,
    pub failed_accounting_count: u32,
    pub largest_single_profit: Usd,
    pub largest_single_loss: Usd,
}
