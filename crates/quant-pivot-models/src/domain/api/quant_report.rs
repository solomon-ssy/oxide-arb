//! Quant recommendation report HTTP contract types.

use crate::{
    domain::RecommendationReportInfo, enums::quant::ReportKind, types::RecommendationReportId,
};
use chrono::{DateTime, Utc};
use serde::Serialize;

/// Outbound projection for a published recommendation report.
#[derive(Debug, Clone, Serialize)]
pub struct QuantReportView {
    pub recommendation_report_id: RecommendationReportId,
    pub report_kind: ReportKind,
    pub status: String,
    pub published_at: Option<DateTime<Utc>>,
    pub top_n: u32,
}

impl From<RecommendationReportInfo> for QuantReportView {
    fn from(info: RecommendationReportInfo) -> Self {
        Self {
            recommendation_report_id: info.recommendation_report_id,
            report_kind: info.report_kind,
            status: info.status.as_str().to_owned(),
            published_at: info.published_at,
            top_n: u32::try_from(info.top_n).unwrap_or(0),
        }
    }
}

/// List query for recommendation reports.
#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct QuantReportListQuery {
    pub kind: Option<ReportKind>,
    pub limit: Option<u32>,
}
