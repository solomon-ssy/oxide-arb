//! Pairwise model-comparison report persistence DTOs.

use crate::{
    entities::quant_model_comparison_report,
    types::{BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId},
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// Frozen, content-addressed pairwise model-comparison report row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_model_comparison_report::Entity")]
pub struct ModelComparisonReportInfo {
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

info_from_model!(
    ModelComparisonReportInfo,
    quant_model_comparison_report::Model,
    {
        comparison_report_id,
        baseline_model_version_id,
        candidate_model_version_id,
        baseline_report_id,
        candidate_report_id,
        model_run_id,
        rank_ic_delta,
        hit_rate_delta,
        realized_pnl_delta,
        score_correlation,
        side_disagreement_rate,
        common_samples,
        category_breakdown_diff,
        comparison_hash,
        created_at,
    }
);

/// Insert payload for `quant_model_comparison_report`.
///
/// Covers every `ActiveModel` column except the DB-managed `created_at`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_comparison_report::ActiveModel")]
pub struct NewModelComparisonReport {
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
}
