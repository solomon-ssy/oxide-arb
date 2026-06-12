//! Opportunity API contract: outbound feed projection.
//!
//! [`OpportunityView`] is the single slim wire shape consumed by the dashboard
//! opportunity feed, shared by the WebSocket `opportunity.detected` push and
//! the `sync` snapshot's `recent_opportunities` section. It deliberately strips
//! the detection internals (calibration snapshot, score components, book
//! context) that the feed never renders, so both real-time and snapshot paths
//! agree on one contract.

use crate::{
    clickhouse::OpportunityDetectionRow,
    domain::Opportunity,
    types::{Bps, MarketId, OpportunityId, Usd},
};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Outbound projection of a detected opportunity for the dashboard feed.
#[derive(Debug, Clone, Serialize)]
pub struct OpportunityView {
    pub opportunity_id: OpportunityId,
    pub market_id: MarketId,
    pub edge_bps: Bps,
    /// Calibration-adjusted expected net profit at detection time.
    pub expected_net_profit_usd: Usd,
    pub detected_at: DateTime<Utc>,
}

impl From<&Opportunity> for OpportunityView {
    fn from(opportunity: &Opportunity) -> Self {
        Self {
            opportunity_id: opportunity.opportunity_id.clone(),
            market_id: opportunity.market_id.clone(),
            edge_bps: opportunity.edge_bps,
            expected_net_profit_usd: opportunity.expected_net_profit,
            detected_at: opportunity.detected_at,
        }
    }
}

impl From<&OpportunityDetectionRow> for OpportunityView {
    fn from(row: &OpportunityDetectionRow) -> Self {
        Self {
            opportunity_id: row.opportunity_id.clone(),
            market_id: row.market_id.clone(),
            edge_bps: row.edge_bps.to_bps(),
            expected_net_profit_usd: row.expected_net_profit_usd.to_usd(),
            // Persisted rows always carry in-range epoch millis; epoch zero is
            // an unreachable fallback that keeps this panic-free.
            detected_at: DateTime::from_timestamp_millis(row.detected_at).unwrap_or_default(),
        }
    }
}
