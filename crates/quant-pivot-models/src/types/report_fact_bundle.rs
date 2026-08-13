//! Immutable report fact bundle written before the `PostgreSQL` report commit.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    clickhouse::{QuantReportRecommendationFactRow, ReportMarketFunnelRow},
    enums::quant::{EmptyReportReason, OutcomeSide, QuantRuntimeMode, ReportKind},
    runtime_config::{BuyModelRoute, ReportDeliveryPolicy},
    types::{Bps, ContentHash, RecommendationReportId, Usd},
};

pub const REPORT_FACT_BUNDLE_FORMAT_VERSION: u32 = 2;

/// Stable commitment for one `ClickHouse` table inside a report bundle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReportFactTableCommitment {
    pub table: String,
    pub row_count: u64,
    pub row_chain_hash: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportFactNotificationRecommendationV2 {
    pub market_id: String,
    pub outcome_side: OutcomeSide,
    pub route: BuyModelRoute,
    pub profit_probability_bps: Bps,
    pub robust_expected_net_usd: Usd,
    pub marginal_portfolio_value_usd: Usd,
    pub suggested_usd: Usd,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportFactNotificationV2 {
    pub kind: ReportKind,
    pub status: String,
    pub runtime_mode: QuantRuntimeMode,
    pub published_count: u32,
    pub total_suggested_usd: Usd,
    pub top3: Vec<ReportFactNotificationRecommendationV2>,
    pub warnings: Vec<String>,
    pub empty_reason: Option<EmptyReportReason>,
}

/// Complete two-table fact payload for one report.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReportFactBundleV2 {
    pub format_version: u32,
    pub recommendation_report_id: RecommendationReportId,
    pub created_at: DateTime<Utc>,
    pub delivery_policy: ReportDeliveryPolicy,
    pub notify_operators: bool,
    pub notification: ReportFactNotificationV2,
    pub recommendation_commitment: ReportFactTableCommitment,
    pub funnel_commitment: ReportFactTableCommitment,
    pub recommendation_rows: Vec<QuantReportRecommendationFactRow>,
    pub funnel_rows: Vec<ReportMarketFunnelRow>,
}
