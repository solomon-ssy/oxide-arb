use serde::{Deserialize, Serialize};

/// `ClickHouse` row for `opportunity_audit` table.
#[derive(Debug, Clone, clickhouse::Row, Serialize, Deserialize)]
pub struct OpportunityAuditRow {
    pub opportunity_id: String,
    pub market_id: String,
    pub event_id: String,
    pub side: String,
    pub entry_price: f64,
    pub shares: f64,
    pub total_cost_usd: f64,
    pub total_fees_usd: f64,
    pub net_profit_usd: f64,
    pub expected_profit: f64,
    pub edge_bps: u32,
    pub resolution_prob: f64,
    pub confidence: f64,
    pub convergence_secs: u32,
    pub price_zone: String,
    pub duration_bucket: String,
    pub depth_used_pct: f64,
    pub staleness: String,
    pub category: String,
    pub outcome: Option<String>,
    pub detected_at: i64,
}
