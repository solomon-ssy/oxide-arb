//! Trade record domain objects.
//!
//! - `TradeInfo` is the 1:1 DB projection returned by `TradeRepository`.
//! - `PostTradeInput` is the risk engine's view of a completed trade.
//! - `NewTrade` / `UpdateTradeOutcome` are write DTOs.

use crate::domain::PositionInfo;
use crate::enums::common::{ExecutionMode, MarketCategory, Side, TradeOutcome};
use crate::types::{
    Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, Shares, TokenId, TradeId,
    Usd,
};
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;
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
        }
    }
}

// ── Repository Write DTOs ────────────────────────────────────────────

/// All fields required to record a new trade at creation time.
///
/// Derives `DeriveIntoActiveModel` — calling `.into_active_model()` produces
/// an `ActiveModel` with these fields `Set(...)` and all others `NotSet`.
/// The entity's `ActiveModelBehavior::before_save` fills in `trade_id`,
/// `outcome`, timestamps, and nullable defaults automatically.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "super::super::entities::trade::ActiveModel")]
pub struct NewTrade {
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
    pub total_pnl: Usd,
    pub total_fees_paid: Usd,
    pub total_gas_paid: Usd,
    pub trade_count: u32,
    pub success_count: u32,
    pub miss_count: u32,
    pub largest_single_loss: Usd,
    pub largest_single_profit: Usd,
}

/// Wallet balance cache snapshot (not persisted, runtime-only).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletBalanceSnapshot {
    pub raw_balance: Usd,
    pub reserved: Usd,
    pub available: Usd,
    pub queried_at: DateTime<Utc>,
}

/// Per-market position summary cache DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummary {
    pub market_id: MarketId,
    pub open_positions: Vec<PositionInfo>,
    pub total_exposure_usd: Usd,
    pub position_count: usize,
    pub summarized_at: DateTime<Utc>,
}

/// Fee params cache DTO.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedFeeParams {
    pub category: MarketCategory,
    pub fee_rate: Decimal,
    pub exponent: Decimal,
    pub cached_at: DateTime<Utc>,
}
