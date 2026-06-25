//! Pairwise model-comparison report HTTP view (Phase 3.6, §5.6).
//!
//! `GET /research/comparison-reports/{id}` returns this; it is also embedded by
//! reference (`comparison_report_id`) on the candidate's [`BacktestReportView`]
//! when a backtest runs in pair mode.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::Serialize;

use crate::{
    domain::ModelComparisonReportInfo,
    types::{BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId},
};

/// Stored pairwise comparison of a candidate model against a baseline.
#[derive(Debug, Clone, Serialize)]
pub struct ModelComparisonReportView {
    pub comparison_report_id: ModelComparisonReportId,
    pub baseline_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub baseline_report_id: BacktestReportId,
    pub candidate_report_id: BacktestReportId,
    pub model_run_id: ModelRunId,
    pub rank_ic_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: i64,
    pub category_breakdown_diff: serde_json::Value,
    pub comparison_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<ModelComparisonReportInfo> for ModelComparisonReportView {
    fn from(info: ModelComparisonReportInfo) -> Self {
        Self {
            comparison_report_id: info.comparison_report_id,
            baseline_model_version_id: info.baseline_model_version_id,
            candidate_model_version_id: info.candidate_model_version_id,
            baseline_report_id: info.baseline_report_id,
            candidate_report_id: info.candidate_report_id,
            model_run_id: info.model_run_id,
            rank_ic_delta: info.rank_ic_delta,
            hit_rate_delta: info.hit_rate_delta,
            realized_pnl_delta: info.realized_pnl_delta,
            score_correlation: info.score_correlation,
            side_disagreement_rate: info.side_disagreement_rate,
            common_samples: info.common_samples,
            category_breakdown_diff: info.category_breakdown_diff,
            comparison_hash: info.comparison_hash,
            created_at: info.created_at,
        }
    }
}
