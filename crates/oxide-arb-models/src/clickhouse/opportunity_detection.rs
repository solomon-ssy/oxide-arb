use crate::domain::opportunity::Opportunity;
use num_traits::ToPrimitive;
use serde::{Deserialize, Serialize};

/// `ClickHouse` row for the `opportunity_detection` table — scanner funnel analytics.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct OpportunityDetectionRow {
    pub opportunity_id: String,
    pub market_id: String,
    pub event_id: String,
    pub token_id: String,
    pub side: String,
    pub entry_price: f64,
    pub edge_bps: u32,
    pub net_profit: f64,
    pub resolution_prob: f64,
    pub confidence: f64,
    pub category: String,
    pub price_zone: String,
    pub duration_bucket: String,
    pub detected_at: i64,
}

impl From<&Opportunity> for OpportunityDetectionRow {
    fn from(opp: &Opportunity) -> Self {
        Self {
            opportunity_id: opp.opportunity_id.to_string(),
            market_id: opp.market_id.to_string(),
            event_id: opp.event_id.to_string(),
            token_id: opp.token_id.to_string(),
            side: opp.side.to_string(),
            entry_price: opp.entry_price.inner().to_f64().unwrap_or(0.0),
            edge_bps: opp.edge_bps.inner().to_u32().unwrap_or(0),
            net_profit: opp.expected_net_profit.inner().to_f64().unwrap_or(0.0),
            resolution_prob: opp.resolution_adjust.to_f64().unwrap_or(0.0),
            confidence: opp.meta.confidence.to_f64().unwrap_or(0.0),
            category: opp.category.to_string(),
            price_zone: opp.meta.price_zone.to_string(),
            duration_bucket: opp.meta.duration_bucket.to_string(),
            detected_at: opp.detected_at.timestamp_millis(),
        }
    }
}
