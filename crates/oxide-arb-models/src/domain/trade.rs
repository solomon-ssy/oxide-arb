//! Trade record domain objects.
//!
//! Strong-typed throughout: `TradeId` / `MarketId` / `Bps` / `Usd` /
//! `DateTime<Utc>`. These are pure domain types — persistence mapping
//! is handled by the entity and repository layers.
//!
//! The `NewTrade` DTO derives `DeriveIntoActiveModel` for zero-boilerplate
//! conversion to `SeaORM` `ActiveModel`. System-generated fields (`trade_id`,
//! timestamps, default outcome) are populated by `ActiveModelBehavior::before_save`.

use crate::enums::common::{Side, TradeOutcome};
use crate::types::{
    Bps, EventId, ExecutionId, MarketId, OpportunityId, Price, Shares, TokenId, TradeId, Usd,
};
use chrono::{DateTime, NaiveDate, Utc};
use sea_orm::DeriveIntoActiveModel;
use serde::{Deserialize, Serialize};

/// Full trade record used across the business layer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeRecord {
    pub trade_id: TradeId,
    pub market_id: MarketId,
    pub event_id: EventId,
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
    pub execution_mode: String,
}

/// Fields that can be updated after trade creation (execution result).
#[derive(Debug, Clone)]
pub struct UpdateTradeOutcome {
    pub outcome: TradeOutcome,
    pub order_id: Option<String>,
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
