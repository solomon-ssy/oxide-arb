//! Analytics API contract: detected-edge histogram + per-market performance.
//!
//! These outbound rows are computed by SQL aggregation in the repository layer
//! (`TradeRepository::edge_histogram` / `market_performance`) and serialized
//! directly to the dashboard — there is no separate persistence projection.

use crate::types::{MarketId, Usd};
use serde::Serialize;

/// A single detected-edge histogram bucket over a trade-history window.
#[derive(Debug, Clone, Serialize)]
pub struct EdgeBucket {
    /// Stable bucket label (basis-point range), e.g. `"0-50"`.
    pub label: &'static str,
    /// Number of trades whose detected edge fell in this bucket.
    pub count: u64,
}

/// Per-market performance aggregate over a trade-history window.
#[derive(Debug, Clone, Serialize)]
pub struct MarketPerformanceRow {
    pub market_id: MarketId,
    pub trade_count: u64,
    pub success_count: u64,
    pub net_profit_usd: Usd,
    pub total_cost_usd: Usd,
}
