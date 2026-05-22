//! Trade record domain objects.
//!
//! Strong-typed throughout: `TradeId` / `MarketId` / `Bps` / `Usd` /
//! `DateTime<Utc>`. These are pure domain types — persistence mapping
//! is handled by the entity and repository layers.
//!
//! The `NewTrade` DTO derives `DeriveIntoActiveModel` for zero-boilerplate
//! conversion to `SeaORM` `ActiveModel`. System-generated fields (`trade_id`,
//! timestamps, default outcome) are populated by `ActiveModelBehavior::before_save`.

use crate::enums::common::{ExecutionMode, Side, TradeOutcome};
use crate::types::{
    Bps, EventId, ExecutionId, MarketId, OpportunityId, OrderId, Price, Shares, TokenId, TradeId,
    Usd,
};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::DeriveIntoActiveModel;
use serde::{Deserialize, Serialize};

/// Risk/accounting read model for a completed trade.
///
/// This is the **business-layer** view consumed by `RiskEngine` and accounting
/// subsystems. It is intentionally distinct from `entities::trade::Model`
/// (the `SeaORM` persistence entity) — the two have different field sets:
///
/// - **Entity** has execution-level fields (`side`, `shares`, `price`,
///   `execution_id`, `order_id`) that risk doesn't need.
/// - **`TradeRecord`** has accounting aggregates (`total_cost_usd`, `total_gas_usd`,
///   `opportunity_snapshot`) that the entity stores differently.
///
/// The mapping `impl From<trade::Model> for TradeRecord` (or a dedicated
/// assembler) belongs to the core crate and is planned for Phase 4.2.
/// Until then, `TradeRecord` is constructed only in test harnesses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub event_id: EventId,
    pub token_id: TokenId,
    pub status: TradeOutcome,
    pub detected_edge_bps: Bps,
    pub detected_profit_usd: Usd,
    pub total_cost_usd: Usd,
    pub total_fees_usd: Usd,
    pub total_gas_usd: Usd,
    pub net_profit_usd: Usd,
    /// Projected `PnL` awaiting market settlement.
    pub net_profit_projected_usd: Usd,
    pub detection_to_exec_ms: Option<i32>,
    pub tx_hash: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub opportunity_snapshot: String,
    pub validation_snapshot: Option<String>,
    pub execution_record: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TradeRecord {
    /// Whether the trade was successfully filled.
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.status == TradeOutcome::Success
    }

    /// Whether the FOK order was not filled (book moved or insufficient depth).
    #[must_use]
    pub fn is_miss(&self) -> bool {
        self.status == TradeOutcome::Miss
    }

    /// Whether the trade failed due to an internal pipeline error.
    #[must_use]
    pub fn is_system_error(&self) -> bool {
        self.status == TradeOutcome::SystemError
    }

    /// Whether the trade failed at the venue level.
    #[must_use]
    pub fn is_trade_failed(&self) -> bool {
        self.status == TradeOutcome::TradeFailed
    }

    /// Whether the trade was rejected at validation due to stale data.
    #[must_use]
    pub fn is_stale(&self) -> bool {
        self.status == TradeOutcome::Stale
    }

    /// Whether the trade reached a terminal failure state (miss, stale, failed, error).
    #[must_use]
    pub const fn is_failure(&self) -> bool {
        matches!(
            self.status,
            TradeOutcome::Miss
                | TradeOutcome::Stale
                | TradeOutcome::TradeFailed
                | TradeOutcome::SystemError
        )
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
