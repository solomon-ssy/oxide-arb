//! Position tracking domain models.
//!
//! `NewPosition` derives `DeriveIntoActiveModel` for clean DTO→ActiveModel
//! conversion. Database defaults own insert-time timestamps; patch DTOs keep
//! nullable clear semantics explicit.

use crate::{
    domain::{NullablePatch, Patch},
    enums::{
        common::{
            ExecutionMode, PositionStatus, RedeemStatus, SettlementAccountingStatus,
            SettlementTrigger, Side,
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
    pub execution_mode: ExecutionMode,
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
    position_id, trade_id, market_id, token_id, side, execution_mode, shares, avg_entry_price,
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
/// Derives `DeriveIntoActiveModel`; database defaults fill insert-only state
/// such as `opened_at` and `PnL` defaults.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::position::ActiveModel")]
pub struct NewPosition {
    pub position_id: PositionId,
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub execution_mode: ExecutionMode,
    pub shares: Shares,
    pub avg_entry_price: Price,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub redeem_status: RedeemStatus,
}

/// Fields that can change when a position is updated (add/reduce/close/settle).
#[derive(Debug, Clone, Default, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::position::ActiveModel")]
pub struct PositionPatch {
    pub shares: Patch<Shares>,
    pub avg_entry_price: Patch<Price>,
    pub total_cost_usd: Patch<Usd>,
    pub total_fees_usd: Patch<Usd>,
    pub unrealized_pnl: Patch<Usd>,
    pub realized_pnl: Patch<Usd>,
    pub status: Patch<PositionStatus>,
    pub closed_at: NullablePatch<DateTime<Utc>>,
    pub settled_at: NullablePatch<DateTime<Utc>>,
    pub winning_token_id: NullablePatch<TokenId>,
    pub settlement_payout_usd: NullablePatch<Usd>,
    pub redeem_tx_hash: NullablePatch<String>,
    pub redeem_status: Patch<RedeemStatus>,
    pub redeem_attempts: Patch<i32>,
    pub oracle_verdict: NullablePatch<serde_json::Value>,
    pub settlement_trigger: NullablePatch<SettlementTrigger>,
    pub settlement_accounting_status: Patch<SettlementAccountingStatus>,
    pub settlement_accounting_error: NullablePatch<String>,
    pub settlement_accounted_at: NullablePatch<DateTime<Utc>>,
    pub redeem_terminal_reason: NullablePatch<String>,
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

#[cfg(test)]
mod tests {
    use super::PositionPatch;
    use crate::domain::{NullablePatch, Patch};
    use sea_orm::{ActiveValue, IntoActiveModel};

    #[test]
    fn position_patch_maps_keep_set_and_clear_to_active_values() {
        let active = PositionPatch {
            redeem_attempts: Patch::set(3),
            redeem_tx_hash: NullablePatch::clear(),
            ..Default::default()
        }
        .into_active_model();

        assert!(matches!(active.shares, ActiveValue::NotSet));
        assert!(matches!(active.redeem_attempts, ActiveValue::Set(3)));
        assert!(matches!(active.redeem_tx_hash, ActiveValue::Set(None)));
    }
}
