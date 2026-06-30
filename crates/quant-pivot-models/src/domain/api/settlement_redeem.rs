//! Settlement redeem HTTP contract types.

use crate::{
    domain::{SettlementRedeemInfo, SettlementRedeemLotInfo, pagination::PageRequest},
    enums::{execution::SettlementRedeemState, quant::ExecutionWalletKind, quant::OutcomeSide},
    types::{
        MarketId, OrderIntentId, PositionId, SettlementRedeemId, SettlementRedeemLotId, Shares, Usd,
    },
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Outbound projection of one settlement redeem lot row.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementRedeemLotView {
    pub settlement_redeem_lot_id: SettlementRedeemLotId,
    pub settlement_redeem_id: SettlementRedeemId,
    pub position_id: PositionId,
    pub order_intent_id: OrderIntentId,
    pub token_id: crate::types::TokenId,
    pub side: OutcomeSide,
    pub shares_redeemed: Shares,
    pub cost_basis_usd: Usd,
    pub payout_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub created_at: DateTime<Utc>,
}

impl From<SettlementRedeemLotInfo> for SettlementRedeemLotView {
    fn from(info: SettlementRedeemLotInfo) -> Self {
        Self {
            settlement_redeem_lot_id: info.settlement_redeem_lot_id,
            settlement_redeem_id: info.settlement_redeem_id,
            position_id: info.position_id,
            order_intent_id: info.order_intent_id,
            token_id: info.token_id,
            side: info.side,
            shares_redeemed: info.shares_redeemed,
            cost_basis_usd: info.cost_basis_usd,
            payout_usd: info.payout_usd,
            realized_pnl_usd: info.realized_pnl_usd,
            created_at: info.created_at,
        }
    }
}

/// Outbound projection of one settlement redeem batch.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementRedeemView {
    pub settlement_redeem_id: SettlementRedeemId,
    pub market_id: MarketId,
    pub funder_address: String,
    pub wallet_kind: ExecutionWalletKind,
    pub state: SettlementRedeemState,
    pub tx_hash: Option<String>,
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

impl From<SettlementRedeemInfo> for SettlementRedeemView {
    fn from(info: SettlementRedeemInfo) -> Self {
        Self {
            settlement_redeem_id: info.settlement_redeem_id,
            market_id: info.market_id,
            funder_address: info.funder_address,
            wallet_kind: info.wallet_kind,
            state: info.state,
            tx_hash: info.tx_hash,
            payout_usd: info.payout_usd,
            gas_fee_pol: info.gas_fee_pol,
            attempt_count: info.attempt_count,
            next_attempt_at: info.next_attempt_at,
            last_error: info.last_error,
            submitted_at: info.submitted_at,
            confirmed_at: info.confirmed_at,
            failed_at: info.failed_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Read-port aggregate: one redeem batch with its lot allocations.
#[derive(Debug, Clone)]
pub struct SettlementRedeemDetail {
    pub redeem: SettlementRedeemInfo,
    pub lots: Vec<SettlementRedeemLotInfo>,
}

/// Settlement redeem detail including per-lot allocations.
#[derive(Debug, Clone, Serialize)]
pub struct SettlementRedeemDetailView {
    #[serde(flatten)]
    pub redeem: SettlementRedeemView,
    pub lots: Vec<SettlementRedeemLotView>,
}

/// Paginated filter for listing settlement redeem batches.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SettlementRedeemListQuery {
    pub state: Option<SettlementRedeemState>,
    pub market_id: Option<MarketId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl SettlementRedeemListQuery {
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}
