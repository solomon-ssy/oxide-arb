//! Position API contract: inbound list query + outbound response view.
//!
//! [`PositionView`] is the outbound wire projection of a position row. It omits
//! the raw `oracle_verdict` JSON and the redeem/accounting retry bookkeeping,
//! which are settlement-internal concerns.

use crate::{
    domain::{PositionInfo, pagination::PageRequest},
    enums::common::{ExecutionMode, PositionStatus, RedeemStatus, Side},
    types::{MarketId, PositionId, Price, Shares, TokenId, TradeId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Filter + pagination query for the positions list endpoint.
///
/// Filters are optional and AND-combined. The window is hardened via
/// [`PositionPageQuery::normalized`] before reaching SQL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PositionPageQuery {
    pub market_id: Option<MarketId>,
    pub status: Option<PositionStatus>,
    #[serde(flatten)]
    pub page: PageRequest,
}

impl PositionPageQuery {
    /// Return a copy with a normalized (safe) pagination window.
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}

/// Outbound projection of a position row for the web dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct PositionView {
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
    pub redeem_status: RedeemStatus,
    pub settlement_payout_usd: Option<Usd>,
    pub winning_token_id: Option<TokenId>,
    pub redeem_tx_hash: Option<String>,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
    pub settled_at: Option<DateTime<Utc>>,
}

impl From<PositionInfo> for PositionView {
    fn from(p: PositionInfo) -> Self {
        Self {
            position_id: p.position_id,
            trade_id: p.trade_id,
            market_id: p.market_id,
            token_id: p.token_id,
            side: p.side,
            execution_mode: p.execution_mode,
            shares: p.shares,
            avg_entry_price: p.avg_entry_price,
            total_cost_usd: p.total_cost_usd,
            total_fees_usd: p.total_fees_usd,
            unrealized_pnl: p.unrealized_pnl,
            realized_pnl: p.realized_pnl,
            status: p.status,
            redeem_status: p.redeem_status,
            settlement_payout_usd: p.settlement_payout_usd,
            winning_token_id: p.winning_token_id,
            redeem_tx_hash: p.redeem_tx_hash,
            opened_at: p.opened_at,
            closed_at: p.closed_at,
            settled_at: p.settled_at,
        }
    }
}
