//! Trade record domain objects.
//!
//! - `TradeInfo` is the 1:1 DB projection returned by `TradeRepository`.
//! - `PostTradeInput` is the risk engine's view of a completed trade.
//! - `NewTrade` / `UpdateTradeOutcome` are write DTOs.

use crate::{
    domain::SettledPositionStats,
    enums::{
        common::{ExecutionMode, Side, TradeOutcome},
        report::ReportSchemaVersion,
    },
    types::{
        Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, Shares, TokenId,
        TradeId, Usd,
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
    pub outcome: TradeOutcome,
    pub execution_mode: ExecutionMode,
    pub latency_ms: Option<i32>,
    pub error_message: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(TradeInfo, crate::entities::trade::Model, {
    trade_id, execution_id, opportunity_id, market_id, event_id, token_id,
    side, shares, price, cost_usd, fee_usd, detected_edge_bps,
    detected_profit_usd, net_profit_usd, order_id, tx_hash, outcome,
    execution_mode, latency_ms, error_message, confirmed_at,
    created_at, updated_at,
});

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
    pub outcome: TradeOutcome,
    pub cost_usd: Usd,
    pub fee_usd: Usd,
    pub net_profit_usd: Option<Usd>,
    pub shares: Shares,
    pub entry_price: Price,
}

impl PostTradeInput {
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.outcome == TradeOutcome::Success
    }

    #[must_use]
    pub fn is_miss(&self) -> bool {
        self.outcome == TradeOutcome::Miss
    }

    #[must_use]
    pub fn is_system_error(&self) -> bool {
        self.outcome == TradeOutcome::SystemError
    }

    #[must_use]
    pub fn is_trade_failed(&self) -> bool {
        self.outcome == TradeOutcome::TradeFailed
    }

    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.outcome == TradeOutcome::Stale
    }

    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(
            self.outcome,
            TradeOutcome::Miss
                | TradeOutcome::Stale
                | TradeOutcome::TradeFailed
                | TradeOutcome::SystemError
        )
    }
}

impl From<&TradeInfo> for PostTradeInput {
    fn from(t: &TradeInfo) -> Self {
        Self {
            trade_id: t.trade_id.clone(),
            market_id: t.market_id.clone(),
            token_id: t.token_id.clone(),
            outcome: t.outcome,
            cost_usd: t.cost_usd,
            fee_usd: t.fee_usd,
            net_profit_usd: t.net_profit_usd,
            shares: t.shares,
            entry_price: t.price,
        }
    }
}

// ── Repository Write DTOs ────────────────────────────────────────────

/// All fields required to record a new trade at creation time.
///
/// Derives `DeriveIntoActiveModel` — calling `.into_active_model()` produces
/// an `ActiveModel` with these fields `Set(...)` and all others `NotSet`.
/// The entity's `ActiveModelBehavior::before_save` fills in `outcome`,
/// timestamps, and nullable defaults automatically.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::trade::ActiveModel")]
pub struct NewTrade {
    pub trade_id: TradeId,
    pub execution_id: ExecutionId,
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
    pub execution_mode: ExecutionMode,
}

/// Fields that can be updated after trade creation (execution result).
#[derive(Debug, Clone)]
pub struct UpdateTradeOutcome {
    pub outcome: TradeOutcome,
    pub shares: Option<Shares>,
    pub price: Option<Price>,
    pub cost_usd: Option<Usd>,
    pub fee_usd: Option<Usd>,
    pub order_id: Option<OrderId>,
    pub tx_hash: Option<String>,
    pub net_profit_usd: Option<Usd>,
    pub latency_ms: Option<i32>,
    pub error_message: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
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
