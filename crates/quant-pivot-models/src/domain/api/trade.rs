//! Trade API contract: inbound list query + outbound response views.
//!
//! [`TradeView`] is the outbound wire projection of a persisted trade row. It
//! deliberately omits the heavy `scored_snapshot` JSON blob and the relay's
//! internal lease bookkeeping (`post_trade_*`), which are forensic/persistence
//! concerns the dashboard never needs.

use crate::{
    domain::{TradeInfo, pagination::PageRequest},
    enums::common::{
        ExecutionMode, MarketCategory, NetProfitKind, Side, TradeBusinessOutcome,
        TradeReconcileResolution, TradeState,
    },
    types::{Bps, EventId, MarketId, OpportunityId, OrderId, Price, Shares, TokenId, TradeId, Usd},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use validator::Validate;

/// Filter + pagination query for the trades list endpoint.
///
/// All filters are optional and AND-combined; `from`/`to` bound `created_at`.
/// The window is hardened via [`TradePageQuery::normalized`] before reaching SQL.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradePageQuery {
    pub market_id: Option<MarketId>,
    pub side: Option<Side>,
    pub state: Option<TradeState>,
    pub business_outcome: Option<TradeBusinessOutcome>,
    pub execution_mode: Option<ExecutionMode>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Operator request to record a manual reconciliation conclusion.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct ReconcileTradeRequest {
    pub resolution: TradeReconcileResolution,
    #[validate(length(min = 1, max = 1024))]
    pub note: String,
}

impl TradePageQuery {
    /// Return a copy with a normalized (safe) pagination window.
    #[must_use]
    pub fn normalized(self) -> Self {
        Self {
            page: self.page.normalized(),
            ..self
        }
    }
}

/// Outbound projection of a trade row for the web dashboard.
#[derive(Debug, Clone, Serialize)]
pub struct TradeView {
    pub trade_id: TradeId,
    /// Originating opportunity — the join key into the audit trail
    /// (`GET /opportunities/{id}`) for the decision-chain timeline.
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub side: Side,
    pub shares: Shares,
    pub price: Price,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub detected_edge_bps: Option<Bps>,
    pub detected_profit_usd: Option<Usd>,
    pub net_profit_usd: Option<Usd>,
    /// Semantic kind of [`Self::net_profit_usd`] — fill-time EV, not realized `PnL`.
    pub net_profit_kind: NetProfitKind,
    pub state: TradeState,
    pub business_outcome: Option<TradeBusinessOutcome>,
    pub category: MarketCategory,
    pub needs_reconcile: bool,
    pub reconcile_resolution: Option<TradeReconcileResolution>,
    pub reconciled_at: Option<DateTime<Utc>>,
    pub reconcile_note: Option<String>,
    pub execution_mode: ExecutionMode,
    pub order_id: Option<OrderId>,
    pub tx_hash: Option<String>,
    pub latency_ms: Option<i32>,
    pub error_message: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TradeInfo> for TradeView {
    fn from(t: TradeInfo) -> Self {
        Self {
            trade_id: t.trade_id,
            opportunity_id: t.opportunity_id,
            market_id: t.market_id,
            event_id: t.event_id,
            token_id: t.token_id,
            side: t.side,
            shares: t.shares,
            price: t.price,
            cost_usd: t.cost_usd,
            fee_usd: t.fee_usd,
            detected_edge_bps: t.detected_edge_bps,
            detected_profit_usd: t.detected_profit_usd,
            net_profit_usd: t.net_profit_usd,
            net_profit_kind: NetProfitKind::for_net_profit(&t.net_profit_usd),
            state: t.state,
            business_outcome: t.business_outcome,
            category: t.category,
            needs_reconcile: t.needs_reconcile,
            reconcile_resolution: t.reconcile_resolution,
            reconciled_at: t.reconciled_at,
            reconcile_note: t.reconcile_note,
            execution_mode: t.execution_mode,
            order_id: t.order_id,
            tx_hash: t.tx_hash,
            latency_ms: t.latency_ms,
            error_message: t.error_message,
            submitted_at: t.submitted_at,
            confirmed_at: t.confirmed_at,
            created_at: t.created_at,
            updated_at: t.updated_at,
        }
    }
}
