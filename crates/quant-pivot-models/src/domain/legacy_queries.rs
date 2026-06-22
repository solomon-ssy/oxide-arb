//! Legacy repository page-query DTOs (Phase 1 removal).
//!
//! Retained so Postgres repositories for historical `trade` / `position` /
//! control-factor tables continue to compile while core/web no longer expose
//! these routes.

use crate::{
    domain::PageRequest,
    enums::{
        common::{PositionStatus, Side, TradeBusinessOutcome, TradeState},
        control_factor::MaterializationRunStatus,
        legacy::LegacyExecutionMode,
    },
    types::{MarketId, Usd},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Paginated trade list filter used by legacy repository reads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TradePageQuery {
    #[serde(flatten)]
    pub page: PageRequest,
    pub market_id: Option<MarketId>,
    pub side: Option<Side>,
    pub state: Option<TradeState>,
    pub business_outcome: Option<TradeBusinessOutcome>,
    pub execution_mode: Option<LegacyExecutionMode>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
}

/// Paginated position list filter used by legacy repository reads.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PositionPageQuery {
    #[serde(flatten)]
    pub page: PageRequest,
    pub market_id: Option<MarketId>,
    pub status: Option<PositionStatus>,
}

/// Paginated materialization-run list filter (legacy control-plane tables).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReplayPageQuery {
    #[serde(flatten)]
    pub page: PageRequest,
    pub status: Option<MaterializationRunStatus>,
}

/// Detected-edge histogram bucket for legacy analytics queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeBucket {
    pub label: &'static str,
    pub count: u64,
}

/// Per-market performance aggregate row for legacy analytics queries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketPerformanceRow {
    pub market_id: MarketId,
    pub trade_count: u64,
    pub success_count: u64,
    pub net_profit_usd: Usd,
    pub total_cost_usd: Usd,
    pub avg_edge_bps: Option<Decimal>,
}
