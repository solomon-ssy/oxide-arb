//! Trade record domain objects.
//!
//! Strong-typed throughout: `TradeId` / `MarketId` / `Bps` / `Usd` /
//! `DateTime<Utc>`. These are pure domain types — persistence mapping
//! is handled by the entity and repository layers.

use crate::enums::common::TradeOutcome;
use crate::types::{Bps, EventId, MarketId, TradeId, Usd};
use chrono::{DateTime, NaiveDate, Utc};
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

/// DTO for inserting a new trade record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewTrade {
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
    pub net_profit_projected_usd: Usd,
    pub detection_to_exec_ms: Option<i32>,
    pub tx_hash: Option<String>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub opportunity_snapshot: String,
    pub validation_snapshot: Option<String>,
    pub execution_record: Option<String>,
}

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
