//! Trade record domain objects.
//!
//! - `TradeInfo` is the 1:1 DB projection returned by `TradeRepository`.
//! - `PostTradeInput` is the risk engine's view of a completed trade.
//! - `NewTrade` / `TradeObservation` are write DTOs.

use crate::{
    domain::{ScoredOpportunitySnapshot, SettledPositionStats},
    enums::{
        common::{
            ExecutionMode, MarketCategory, Side, TradeBusinessOutcome, TradeReconcileResolution,
            TradeState,
        },
        report::ReportSchemaVersion,
    },
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, ReservationId, Shares,
        TokenId, TradeId, Usd,
    },
};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

// ── Read models ─────────────────────────────────────────────────────

/// DB row projection matching `entities::trade::Model` columns exactly.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::trade::Entity")]
pub struct TradeInfo {
    pub trade_id: TradeId,
    pub execution_id: ExecutionId,
    pub reservation_id: ReservationId,
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
    pub order_id: Option<OrderId>,
    pub tx_hash: Option<String>,
    /// Lifecycle state machine — single source of truth for the trade row.
    pub state: TradeState,
    /// Business-outcome bucket maintained by repository state transitions.
    pub business_outcome: Option<TradeBusinessOutcome>,
    /// Frozen scored-opportunity snapshot captured at dispatch (post-trade audit).
    pub scored_snapshot: serde_json::Value,
    pub category: MarketCategory,
    pub needs_reconcile: bool,
    pub reconcile_resolution: Option<TradeReconcileResolution>,
    pub reconciled_at: Option<DateTime<Utc>>,
    pub reconcile_note: Option<String>,
    pub post_trade_claim_owner: Option<String>,
    pub post_trade_claimed_at: Option<DateTime<Utc>>,
    pub post_trade_attempts: i32,
    pub execution_mode: ExecutionMode,
    pub latency_ms: Option<i32>,
    pub error_message: Option<String>,
    pub submitted_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(TradeInfo, crate::entities::trade::Model, {
    trade_id, execution_id, reservation_id, opportunity_id, market_id, event_id, token_id,
    side, shares, price, cost_usd, fee_usd, detected_edge_bps,
    detected_profit_usd, net_profit_usd, order_id, tx_hash, state,
    business_outcome, scored_snapshot, category, needs_reconcile,
    reconcile_resolution, reconciled_at, reconcile_note, post_trade_claim_owner,
    post_trade_claimed_at, post_trade_attempts, execution_mode, latency_ms,
    error_message, submitted_at, confirmed_at, created_at, updated_at,
});

impl TradeInfo {
    pub fn scored_opportunity_snapshot(
        &self,
    ) -> Result<ScoredOpportunitySnapshot, serde_json::Error> {
        serde_json::from_value(self.scored_snapshot.clone())
    }
}

/// Risk engine's view of a completed trade — minimal fields needed for
/// post-trade accounting, blacklist logic, and potential-loss tracking.
///
/// Constructed by the execution pipeline (from `TradeInfo`) or directly
/// from execution results. Not a DB projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostTradeInput {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub side: Side,
    pub outcome: TradeBusinessOutcome,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub net_profit_usd: Option<Usd>,
    pub shares: Shares,
    pub entry_price: Price,
}

impl PostTradeInput {
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.outcome == TradeBusinessOutcome::Success
    }

    #[must_use]
    pub fn is_miss(&self) -> bool {
        self.outcome == TradeBusinessOutcome::Miss
    }

    /// Build the risk-engine input from a persisted trade row.
    ///
    /// Returns `None` for in-flight rows (`Intent`/`Submitted`) that have no
    /// business outcome yet — the relay only calls this for `*_observed` rows.
    #[must_use]
    pub fn from_trade_info(t: &TradeInfo) -> Option<Self> {
        Some(Self {
            trade_id: t.trade_id.clone(),
            market_id: t.market_id.clone(),
            token_id: t.token_id.clone(),
            side: t.side,
            outcome: t.business_outcome?,
            cost_usd: t.cost_usd,
            fee_usd: t.fee_usd,
            net_profit_usd: t.net_profit_usd,
            shares: t.shares,
            entry_price: t.price,
        })
    }
}

// ── Repository Write DTOs ────────────────────────────────────────────

/// All fields required to record a new trade at creation time (state = `Intent`).
///
/// Derives `DeriveIntoActiveModel` — calling `.into_active_model()` produces
/// an `ActiveModel` with these fields `Set(...)` and all others `NotSet`.
/// The repository starts new rows in `Intent`; `business_outcome` remains `None`
/// until an observed/terminal state transition sets it.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::trade::ActiveModel")]
pub struct NewTrade {
    pub trade_id: TradeId,
    pub execution_id: ExecutionId,
    pub reservation_id: ReservationId,
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
    /// Frozen scored-opportunity snapshot, captured at dispatch.
    pub scored_snapshot: serde_json::Value,
    pub category: MarketCategory,
    pub execution_mode: ExecutionMode,
}

/// Venue-result observation written when an order outcome becomes known.
///
/// Transitions the row to one of the `*_observed` states and records the raw
/// execution economics. The relay later applies side-effects and advances to a
/// terminal state. `state` must be one of `FillObserved`/`MissObserved`/`FailObserved`.
#[derive(Debug, Clone)]
pub struct TradeObservation {
    pub state: TradeState,
    pub shares: Shares,
    pub price: Price,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub order_id: Option<OrderId>,
    pub tx_hash: Option<String>,
    pub net_profit_usd: Option<Usd>,
    pub latency_ms: Option<i32>,
    pub error_message: Option<String>,
    pub confirmed_at: DateTime<Utc>,
}

// ── Reporting ────────────────────────────────────────────────────────

/// Daily accounting summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DailyReport {
    pub date: NaiveDate,
    pub schema_version: ReportSchemaVersion,
    pub generated_at: DateTime<Utc>,
    pub period_start: NaiveDate,
    pub period_end: NaiveDate,
    pub settled_pnl: SettledPnlStats,
    pub execution: ReportTradeStats,
    pub risk: ReportRiskSummary,
    pub total_pnl: Usd,
    pub total_fees_paid: Usd,
    /// Actual gas paid by redemption transactions. Currently zero until redeem
    /// gas persistence is implemented.
    pub total_gas_paid: Usd,
    pub trade_count: u32,
    pub success_count: u32,
    pub miss_count: u32,
    pub largest_single_loss: Usd,
    pub largest_single_profit: Usd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SettledPnlStats {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportTradeStats {
    pub trade_count: u32,
    pub success_count: u32,
    pub miss_count: u32,
    pub failed_count: u32,
    pub total_fill_cost: Usd,
    pub total_fill_fees: Usd,
    pub fill_expected_pnl: Usd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportRiskSummary {
    pub daily_pnl: Usd,
    pub daily_loss: Usd,
    pub weekly_loss: Usd,
    pub total_exposure: Usd,
    pub open_position_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeeklyReport {
    pub week_start: NaiveDate,
    pub week_end: NaiveDate,
    pub schema_version: ReportSchemaVersion,
    pub generated_at: DateTime<Utc>,
    pub settled_pnl: SettledPnlStats,
    pub execution: ReportTradeStats,
    pub risk: ReportRiskSummary,
    pub daily_reports: Vec<DailyReport>,
}

impl From<&SettledPositionStats> for SettledPnlStats {
    fn from(stats: &SettledPositionStats) -> Self {
        Self {
            realized_pnl: stats.realized_pnl,
            total_payout: stats.total_payout,
            total_cost: stats.total_cost,
            total_fees: stats.total_fees,
            settled_position_count: stats.settled_position_count,
            winning_position_count: stats.winning_position_count,
            losing_position_count: stats.losing_position_count,
            unsettled_position_count: stats.unsettled_position_count,
            failed_accounting_count: stats.failed_accounting_count,
            largest_single_profit: stats.largest_single_profit,
            largest_single_loss: stats.largest_single_loss,
        }
    }
}
