//! Pairwise model comparison over a shared backtest window (Phase 3.6, §5.6).
//!
//! Given a baseline and a candidate model replayed over the **same** PIT
//! cross-sections, [`compare_reports`] computes the head-to-head divergence the
//! Admin surface (and Phase 04 promotion gate) needs: the rank-IC / hit-rate /
//! realized-PnL deltas, the per-`(as_of, market, token)` composite-score
//! correlation and side-disagreement rate, and a per-category rank-IC diff. The
//! report is content-addressed so a comparison is reproducible and auditable.

use quant_pivot_models::{
    enums::common::MarketCategory,
    types::{ContentHash, ModelVersionId},
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use crate::{
    backtest::{BacktestRunResult, SampleOutcome},
    hashing::ResearchHasher,
    precision::RESEARCH_DECIMAL_SCALE,
    stats,
};
use quant_pivot_error::QuantResult;

/// Per-category rank-IC divergence between the candidate and the baseline.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CategoryRankIcDelta {
    /// Market category.
    pub category: MarketCategory,
    /// Baseline rank IC in this category.
    pub baseline_rank_ic: Decimal,
    /// Candidate rank IC in this category.
    pub candidate_rank_ic: Decimal,
    /// `candidate − baseline` rank-IC delta.
    pub rank_ic_delta: Decimal,
}

/// Head-to-head comparison of a candidate against a baseline over one window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelComparisonReport {
    /// Baseline model version.
    pub baseline_model_version_id: ModelVersionId,
    /// Candidate model version.
    pub candidate_model_version_id: ModelVersionId,
    /// Baseline report content hash (provenance).
    pub baseline_report_hash: ContentHash,
    /// Candidate report content hash (provenance).
    pub candidate_report_hash: ContentHash,
    /// `candidate − baseline` rank IC over all resolved samples.
    pub rank_ic_delta: Decimal,
    /// `candidate − baseline` hit rate.
    pub hit_rate_delta: Decimal,
    /// `candidate − baseline` realized `PnL` (USD).
    pub realized_pnl_delta: Decimal,
    /// Pearson correlation of composite scores over markets both models scored.
    pub score_correlation: Decimal,
    /// Fraction of common markets where the two models took opposite sides.
    pub side_disagreement_rate: Decimal,
    /// Number of `(as_of, market, token)` keys both models resolved.
    pub common_samples: u64,
    /// Per-category rank-IC diff (union of both reports' categories).
    pub category_breakdown_diff: Vec<CategoryRankIcDelta>,
    /// Canonical hash over every field above.
    pub comparison_hash: ContentHash,
}

/// Compute the head-to-head comparison of `candidate` against `baseline`.
///
/// Both runs must come from the same replay window; samples are joined on
/// `(as_of, market, token)` for the correlation / disagreement metrics.
///
/// # Errors
///
/// Propagates canonical-hash serialization failures.
pub fn compare_reports(
    baseline: &BacktestRunResult,
    candidate: &BacktestRunResult,
) -> QuantResult<ModelComparisonReport> {
    let rank_ic_delta =
        (candidate.report.rank_ic - baseline.report.rank_ic).round_dp(RESEARCH_DECIMAL_SCALE);
    let hit_rate_delta = (candidate.report.hit_rate.inner() - baseline.report.hit_rate.inner())
        .round_dp(RESEARCH_DECIMAL_SCALE);
    let realized_pnl_delta = (candidate.report.report_pnl_simulation.realized_pnl_usd
        - baseline.report.report_pnl_simulation.realized_pnl_usd)
        .round_dp(RESEARCH_DECIMAL_SCALE);

    let (score_correlation, side_disagreement_rate, common_samples) =
        joined_divergence(&baseline.sample_outcomes, &candidate.sample_outcomes);

    let category_breakdown_diff = category_diff(baseline, candidate);

    let comparison_hash = ResearchHasher::canonical(&ComparisonHashInput {
        baseline_model_version_id: &baseline.report.model_version_id,
        candidate_model_version_id: &candidate.report.model_version_id,
        baseline_report_hash: &baseline.report.report_hash,
        candidate_report_hash: &candidate.report.report_hash,
        rank_ic_delta,
        hit_rate_delta,
        realized_pnl_delta,
        score_correlation,
        side_disagreement_rate,
        common_samples,
        category_breakdown_diff: &category_breakdown_diff,
    })?;

    Ok(ModelComparisonReport {
        baseline_model_version_id: baseline.report.model_version_id.clone(),
        candidate_model_version_id: candidate.report.model_version_id.clone(),
        baseline_report_hash: baseline.report.report_hash.clone(),
        candidate_report_hash: candidate.report.report_hash.clone(),
        rank_ic_delta,
        hit_rate_delta,
        realized_pnl_delta,
        score_correlation,
        side_disagreement_rate,
        common_samples,
        category_breakdown_diff,
        comparison_hash,
    })
}

/// Composite-score correlation + side-disagreement over the common sample keys.
fn joined_divergence(
    baseline: &[SampleOutcome],
    candidate: &[SampleOutcome],
) -> (Decimal, Decimal, u64) {
    let baseline_index: BTreeMap<SampleKey, &SampleOutcome> =
        baseline.iter().map(|s| (SampleKey::of(s), s)).collect();

    let mut baseline_scores = Vec::new();
    let mut candidate_scores = Vec::new();
    let mut disagreements: u64 = 0;
    let mut common: u64 = 0;
    for sample in candidate {
        let Some(base) = baseline_index.get(&SampleKey::of(sample)) else {
            continue;
        };
        common += 1;
        baseline_scores.push(base.composite_score.inner());
        candidate_scores.push(sample.composite_score.inner());
        if base.outcome_side != sample.outcome_side {
            disagreements += 1;
        }
    }

    let score_correlation = if common >= 2 {
        stats::pearson(&baseline_scores, &candidate_scores).round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    let side_disagreement_rate = if common > 0 {
        (Decimal::from(disagreements) / Decimal::from(common)).round_dp(RESEARCH_DECIMAL_SCALE)
    } else {
        Decimal::ZERO
    };
    (score_correlation, side_disagreement_rate, common)
}

/// Per-category rank-IC diff over the union of both reports' categories.
fn category_diff(
    baseline: &BacktestRunResult,
    candidate: &BacktestRunResult,
) -> Vec<CategoryRankIcDelta> {
    let baseline_by_cat: BTreeMap<MarketCategory, Decimal> = baseline
        .report
        .category_breakdown
        .iter()
        .map(|c| (c.category, c.rank_ic))
        .collect();
    let candidate_by_cat: BTreeMap<MarketCategory, Decimal> = candidate
        .report
        .category_breakdown
        .iter()
        .map(|c| (c.category, c.rank_ic))
        .collect();

    let mut categories: Vec<MarketCategory> = baseline_by_cat
        .keys()
        .chain(candidate_by_cat.keys())
        .copied()
        .collect();
    categories.sort_by_key(|category| category.as_str());
    categories.dedup();

    categories
        .into_iter()
        .map(|category| {
            let baseline_rank_ic = baseline_by_cat
                .get(&category)
                .copied()
                .unwrap_or(Decimal::ZERO);
            let candidate_rank_ic = candidate_by_cat
                .get(&category)
                .copied()
                .unwrap_or(Decimal::ZERO);
            CategoryRankIcDelta {
                category,
                baseline_rank_ic,
                candidate_rank_ic,
                rank_ic_delta: (candidate_rank_ic - baseline_rank_ic)
                    .round_dp(RESEARCH_DECIMAL_SCALE),
            }
        })
        .collect()
}

/// Join key for matching the same decision across the two models.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SampleKey {
    as_of: i64,
    market_id: String,
    token_id: String,
}

impl SampleKey {
    fn of(sample: &SampleOutcome) -> Self {
        Self {
            as_of: sample.as_of.timestamp_millis(),
            market_id: sample.market_id.as_str().to_owned(),
            token_id: sample.token_id.as_str().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compare_reports;
    use chrono::{TimeZone, Utc};
    use quant_pivot_models::{
        enums::{
            common::MarketCategory,
            quant::{DataQualityStatus, OutcomeSide},
        },
        types::{
            BacktestReportId, ContentHash, MarketId, ModelVersionId, Probability,
            RuntimeConfigVersionId, TokenId, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::backtest::{
        BacktestReport, BacktestRunResult, CategoryMetric, ExpectedVsRealized, PnlCurvePoint,
        PnlSimulation, SampleOutcome,
    };

    fn hash(seed: &str) -> ContentHash {
        ContentHash::parse(format!("blake3:{seed:0>64}")).expect("hash")
    }

    fn sample(
        idx: i64,
        score: Decimal,
        realized: Decimal,
        outcome_side: OutcomeSide,
    ) -> SampleOutcome {
        SampleOutcome {
            as_of: Utc.timestamp_opt(1_700_000_000 + idx, 0).unwrap(),
            market_id: MarketId::new(format!("0x{idx}")),
            token_id: TokenId::new("yes"),
            category: MarketCategory::Crypto,
            outcome_side,
            composite_score: Probability::new(score),
            confidence: Probability::new(dec!(1)),
            expected_return_bps: dec!(100),
            realized_return_bps: realized,
            allocated_usd: Usd::new(dec!(100)),
            liquidity_feasible: true,
            data_quality: DataQualityStatus::Fresh,
            liquidity_usd: None,
            time_to_resolution_secs: None,
            prediction_horizon_secs: 0,
            substitutions: Vec::new(),
        }
    }

    fn result(
        report_seed: &str,
        rank_ic: Decimal,
        realized_pnl: Decimal,
        samples: Vec<SampleOutcome>,
    ) -> BacktestRunResult {
        let report = BacktestReport {
            backtest_report_id: BacktestReportId::from_v7(),
            model_version_id: ModelVersionId::from_v7(),
            runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
            window_start: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
            window_end: Utc.timestamp_opt(1_700_100_000, 0).unwrap(),
            coverage: dec!(1),
            sample_count: samples.len() as u64,
            missing_feature_count: 0,
            rank_ic,
            hit_rate: Probability::new(dec!(0.5)),
            expected_vs_realized: ExpectedVsRealized {
                mean_expected_bps: dec!(0),
                mean_realized_bps: dec!(0),
                correlation: dec!(0),
                bias_bps: dec!(0),
            },
            max_drawdown: dec!(0),
            turnover: dec!(0),
            liquidity_feasibility: Probability::new(dec!(1)),
            category_breakdown: vec![CategoryMetric {
                category: MarketCategory::Crypto,
                sample_count: samples.len() as u64,
                rank_ic,
                hit_rate: Probability::new(dec!(0.5)),
                mean_realized_bps: dec!(0),
            }],
            tail_loss: dec!(0),
            report_pnl_simulation: PnlSimulation {
                total_allocated_usd: dec!(1000),
                realized_pnl_usd: realized_pnl,
                gross_return: dec!(0),
                pnl_curve: vec![PnlCurvePoint {
                    as_of: Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
                    cumulative_realized_pnl_usd: realized_pnl,
                }],
            },
            report_hash: hash(report_seed),
        };
        BacktestRunResult {
            report,
            sample_outcomes: samples,
        }
    }

    #[test]
    fn comparison_reports_capture_deltas_and_disagreement() {
        let baseline = result(
            "aa",
            dec!(0.10),
            dec!(100),
            vec![
                sample(0, dec!(0.8), dec!(50), OutcomeSide::Yes),
                sample(1, dec!(0.2), dec!(-20), OutcomeSide::Yes),
            ],
        );
        // Candidate scores correlate but flips the side on market 1.
        let candidate = result(
            "bb",
            dec!(0.25),
            dec!(180),
            vec![
                sample(0, dec!(0.85), dec!(50), OutcomeSide::Yes),
                sample(1, dec!(0.25), dec!(-20), OutcomeSide::No),
            ],
        );

        let comparison = compare_reports(&baseline, &candidate).expect("compare");
        assert_eq!(
            comparison.rank_ic_delta,
            dec!(0.15),
            "candidate − baseline rank IC"
        );
        assert_eq!(comparison.realized_pnl_delta, dec!(80));
        assert_eq!(comparison.common_samples, 2);
        assert_eq!(
            comparison.side_disagreement_rate,
            dec!(0.5),
            "one of two flipped"
        );
        assert!(comparison.score_correlation > dec!(0.9), "scores track");
        assert_eq!(comparison.category_breakdown_diff.len(), 1);
        assert_eq!(
            comparison.category_breakdown_diff[0].rank_ic_delta,
            dec!(0.15)
        );
        assert!(comparison.comparison_hash.as_str().starts_with("blake3:"));
    }
}

/// Canonical-hash projection of every comparison field except the hash itself.
#[derive(Serialize)]
struct ComparisonHashInput<'a> {
    baseline_model_version_id: &'a ModelVersionId,
    candidate_model_version_id: &'a ModelVersionId,
    baseline_report_hash: &'a ContentHash,
    candidate_report_hash: &'a ContentHash,
    rank_ic_delta: Decimal,
    hit_rate_delta: Decimal,
    realized_pnl_delta: Decimal,
    score_correlation: Decimal,
    side_disagreement_rate: Decimal,
    common_samples: u64,
    category_breakdown_diff: &'a [CategoryRankIcDelta],
}
