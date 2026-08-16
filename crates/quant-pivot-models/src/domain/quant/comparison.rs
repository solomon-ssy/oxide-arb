//! Pairwise model-comparison report persistence DTOs.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::quant::BacktestReportInfo,
    entities::quant_model_comparison_report,
    types::{
        BacktestReportId, ContentHash, ModelComparisonReportId, ModelRunId, ModelVersionId,
        backtest::{
            BACKTEST_METRIC_SCALE, CategoryRealizedReturnRankCorrelationDelta,
            CategoryRealizedReturnRankCorrelationDeltas, ModelComparisonHashInput,
        },
    },
};

/// Frozen, content-addressed pairwise model-comparison report row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_model_comparison_report::Entity")]
pub struct ModelComparisonReportInfo {
    pub comparison_report_id: ModelComparisonReportId,
    pub baseline_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub baseline_report_id: BacktestReportId,
    pub candidate_report_id: BacktestReportId,
    pub model_run_id: ModelRunId,
    pub realized_return_rank_correlation_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: i64,
    pub category_breakdown_diff: CategoryRealizedReturnRankCorrelationDeltas,
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
        realized_return_rank_correlation_delta,
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

impl ModelComparisonReportInfo {
    /// Recompute the canonical comparison hash against the exact two report
    /// content hashes.
    pub fn recomputed_hash(
        &self,
        baseline_report_hash: &ContentHash,
        candidate_report_hash: &ContentHash,
    ) -> Result<ContentHash, String> {
        ModelComparisonHashInput::try_from((self, baseline_report_hash, candidate_report_hash))?
            .content_hash()
            .map_err(|error| format!("model comparison hash failed: {error}"))
    }
}

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
    pub realized_return_rank_correlation_delta: Decimal,
    pub hit_rate_delta: Decimal,
    pub realized_pnl_delta: Decimal,
    pub score_correlation: Decimal,
    pub side_disagreement_rate: Decimal,
    pub common_samples: i64,
    pub category_breakdown_diff: CategoryRealizedReturnRankCorrelationDeltas,
    pub comparison_hash: ContentHash,
}

impl NewModelComparisonReport {
    /// Recompute the canonical comparison hash against the exact two report
    /// content hashes.
    pub fn recomputed_hash(
        &self,
        baseline_report_hash: &ContentHash,
        candidate_report_hash: &ContentHash,
    ) -> Result<ContentHash, String> {
        ModelComparisonHashInput::try_from((self, baseline_report_hash, candidate_report_hash))?
            .content_hash()
            .map_err(|error| format!("model comparison hash failed: {error}"))
    }

    /// Verify all report-derived fields and the canonical comparison hash.
    pub fn validate_against_reports(
        &self,
        baseline: &BacktestReportInfo,
        candidate: &BacktestReportInfo,
    ) -> Result<(), String> {
        baseline.verify_hash()?;
        candidate.verify_hash()?;
        if self.baseline_report_id != baseline.backtest_report_id
            || self.candidate_report_id != candidate.backtest_report_id
            || self.baseline_model_version_id != baseline.model_version_id
            || self.candidate_model_version_id != candidate.model_version_id
            || baseline.evaluation_dataset_id != candidate.evaluation_dataset_id
            || baseline.decision_policy_snapshot_id != candidate.decision_policy_snapshot_id
            || baseline.window_start != candidate.window_start
            || baseline.window_end != candidate.window_end
        {
            return Err(
                "comparison reports must have exact ids/models and a common Dataset/policy/window"
                    .to_owned(),
            );
        }
        if self.baseline_model_version_id == self.candidate_model_version_id
            || self.baseline_report_id == self.candidate_report_id
        {
            return Err("comparison requires distinct baseline and candidate subjects".to_owned());
        }
        let expected_realized_return_rank_correlation = (candidate
            .realized_return_rank_correlation
            - baseline.realized_return_rank_correlation)
            .round_dp(BACKTEST_METRIC_SCALE);
        let expected_hit_rate = (candidate.hit_rate.inner() - baseline.hit_rate.inner())
            .round_dp(BACKTEST_METRIC_SCALE);
        let expected_realized_pnl = (candidate.report_pnl_simulation.realized_pnl_usd
            - baseline.report_pnl_simulation.realized_pnl_usd)
            .round_dp(BACKTEST_METRIC_SCALE);
        if self.realized_return_rank_correlation_delta != expected_realized_return_rank_correlation
            || self.hit_rate_delta != expected_hit_rate
            || self.realized_pnl_delta != expected_realized_pnl
        {
            return Err("comparison scalar deltas do not match the two reports".to_owned());
        }
        if self.score_correlation < -Decimal::ONE
            || self.score_correlation > Decimal::ONE
            || self.side_disagreement_rate < Decimal::ZERO
            || self.side_disagreement_rate > Decimal::ONE
        {
            return Err("comparison correlation or disagreement rate is outside range".to_owned());
        }
        let common_samples = u64::try_from(self.common_samples)
            .map_err(|error| format!("comparison common_samples must be non-negative: {error}"))?;
        let baseline_samples = u64::try_from(baseline.sample_count)
            .map_err(|error| format!("baseline sample_count must be non-negative: {error}"))?;
        let candidate_samples = u64::try_from(candidate.sample_count)
            .map_err(|error| format!("candidate sample_count must be non-negative: {error}"))?;
        if common_samples > baseline_samples.min(candidate_samples) {
            return Err("comparison common_samples exceeds either report sample count".to_owned());
        }
        let expected_categories =
            CategoryRealizedReturnRankCorrelationDeltas::between(baseline, candidate);
        if self.category_breakdown_diff != expected_categories {
            return Err("comparison category deltas do not match the two reports".to_owned());
        }
        let recomputed = self.recomputed_hash(&baseline.report_hash, &candidate.report_hash)?;
        if self.comparison_hash != recomputed {
            return Err(format!(
                "model comparison hash mismatch: stored {}, recomputed {recomputed}",
                self.comparison_hash
            ));
        }
        Ok(())
    }
}

impl<'a>
    TryFrom<(
        &'a ModelComparisonReportInfo,
        &'a ContentHash,
        &'a ContentHash,
    )> for ModelComparisonHashInput<'a>
{
    type Error = String;

    fn try_from(
        (report, baseline_report_hash, candidate_report_hash): (
            &'a ModelComparisonReportInfo,
            &'a ContentHash,
            &'a ContentHash,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            baseline_model_version_id: &report.baseline_model_version_id,
            candidate_model_version_id: &report.candidate_model_version_id,
            baseline_report_hash,
            candidate_report_hash,
            realized_return_rank_correlation_delta: report.realized_return_rank_correlation_delta,
            hit_rate_delta: report.hit_rate_delta,
            realized_pnl_delta: report.realized_pnl_delta,
            score_correlation: report.score_correlation,
            side_disagreement_rate: report.side_disagreement_rate,
            common_samples: u64::try_from(report.common_samples).map_err(|error| {
                format!("comparison common_samples must be non-negative: {error}")
            })?,
            category_breakdown_diff: &report.category_breakdown_diff,
        })
    }
}

impl<'a>
    TryFrom<(
        &'a NewModelComparisonReport,
        &'a ContentHash,
        &'a ContentHash,
    )> for ModelComparisonHashInput<'a>
{
    type Error = String;

    fn try_from(
        (report, baseline_report_hash, candidate_report_hash): (
            &'a NewModelComparisonReport,
            &'a ContentHash,
            &'a ContentHash,
        ),
    ) -> Result<Self, Self::Error> {
        Ok(Self {
            baseline_model_version_id: &report.baseline_model_version_id,
            candidate_model_version_id: &report.candidate_model_version_id,
            baseline_report_hash,
            candidate_report_hash,
            realized_return_rank_correlation_delta: report.realized_return_rank_correlation_delta,
            hit_rate_delta: report.hit_rate_delta,
            realized_pnl_delta: report.realized_pnl_delta,
            score_correlation: report.score_correlation,
            side_disagreement_rate: report.side_disagreement_rate,
            common_samples: u64::try_from(report.common_samples).map_err(|error| {
                format!("comparison common_samples must be non-negative: {error}")
            })?,
            category_breakdown_diff: &report.category_breakdown_diff,
        })
    }
}

impl CategoryRealizedReturnRankCorrelationDeltas {
    fn between(baseline: &BacktestReportInfo, candidate: &BacktestReportInfo) -> Self {
        let baseline_by_category = baseline
            .category_breakdown
            .iter()
            .map(|metric| (metric.category, metric.realized_return_rank_correlation))
            .collect::<BTreeMap<_, _>>();
        let candidate_by_category = candidate
            .category_breakdown
            .iter()
            .map(|metric| (metric.category, metric.realized_return_rank_correlation))
            .collect::<BTreeMap<_, _>>();
        let mut categories = baseline_by_category
            .keys()
            .chain(candidate_by_category.keys())
            .copied()
            .collect::<Vec<_>>();
        categories.sort_by_key(|category| category.as_str());
        categories.dedup();
        categories
            .into_iter()
            .map(|category| {
                let baseline_realized_return_rank_correlation = baseline_by_category
                    .get(&category)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                let candidate_realized_return_rank_correlation = candidate_by_category
                    .get(&category)
                    .copied()
                    .unwrap_or(Decimal::ZERO);
                CategoryRealizedReturnRankCorrelationDelta {
                    category,
                    baseline_realized_return_rank_correlation,
                    candidate_realized_return_rank_correlation,
                    realized_return_rank_correlation_delta:
                        (candidate_realized_return_rank_correlation
                            - baseline_realized_return_rank_correlation)
                            .round_dp(BACKTEST_METRIC_SCALE),
                }
            })
            .collect::<Vec<_>>()
            .into()
    }
}
