//! Return-model + governed-multiplier calibration from realized backtest
//! outcomes (Phase 3.6, §1.1 / §4).
//!
//! After a candidate model is backtested, this module fits a **monotone**
//! [`ReturnModelSpec::Calibrated`] curve from realized vs. predicted outcomes
//! (replacing the 3.4 `Heuristic` default), and tightens every governed score
//! multiplier — data-quality, liquidity, horizon — plus the substitution
//! confidence penalties from realized **stratified** performance.
//!
//! Every calibration is **fail-closed**: insufficient evidence keeps the
//! conservative baseline, a stratum that lost money on average collapses to the
//! harshest multiplier, and a calibrated multiplier is **never** more optimistic
//! than the governed baseline it refines. The unified [`calibrate_weighted_artifact`]
//! returns `None` (keep the artifact verbatim) unless there is enough evidence to
//! fit a real return curve, so a thin backtest can never silently relax governance.

use quant_pivot_models::enums::quant::DataQualityStatus;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    features::NullReason,
    model::artifact::{
        CalibratedReturnModel, DataQualityMultipliers, HorizonMultipliers, LiquidityMultipliers,
        LiquidityTier, ReturnCurvePoint, ReturnModelSpec, ScoreMultiplierSpec,
        SubstitutionConfidenceRules, WeightedFactorModelArtifact,
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// One realized backtest outcome used for calibration.
///
/// Carries the full stratum context each sample was scored under — never assumed
/// (the backtest runner records the PIT-resolved data-quality / liquidity /
/// horizon / substitution context per sample).
#[derive(Debug, Clone)]
pub struct CalibrationSample {
    /// The candidate's composite ranking score in `[0, 1]`.
    pub composite_score: Decimal,
    /// Realized return in basis points (settlement payoff vs. entry, sided).
    pub realized_return_bps: Decimal,
    /// Data-quality stratum the candidate was scored under.
    pub data_quality: DataQualityStatus,
    /// Visible liquidity (USD) at decision time, when known (liquidity stratum).
    pub liquidity_usd: Option<Decimal>,
    /// Seconds until resolution at decision time, when known (horizon stratum).
    pub time_to_resolution_secs: Option<u64>,
    /// The model's frozen prediction horizon (seconds); the horizon-ratio denominator.
    pub prediction_horizon_secs: u64,
    /// Distinct substitution reasons applied to the scored vector (substitution stratum).
    pub substitution_reasons: Vec<NullReason>,
}

/// Number of equal-width `[0, 1]` score buckets.
const SCORE_BUCKETS: usize = 10;

/// Minimum samples in a bucket / stratum for it to inform calibration.
const MIN_BUCKET_SAMPLES: usize = 3;

/// Minimum total samples to attempt return-curve calibration.
const MIN_TOTAL_SAMPLES: usize = 30;

/// One calibrated stratum's provenance (sample count + mean realized return),
/// emitted into the run's `metrics_json` for auditability.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StratumFit {
    /// Human-readable stratum label (e.g. `"fresh"`, `"liquidity>=10000"`).
    pub label: String,
    /// Samples in the stratum.
    pub sample_count: u64,
    /// Mean realized return (bps) in the stratum.
    pub mean_realized_bps: Decimal,
}

/// Stratified calibration provenance report (serialized into `metrics_json`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CalibrationReport {
    /// Total realized samples the calibration consumed.
    pub total_samples: u64,
    /// Number of knots in the fitted expected-return curve.
    pub return_curve_points: u64,
    /// Per data-quality stratum fit.
    pub data_quality_strata: Vec<StratumFit>,
    /// Per liquidity-tier stratum fit.
    pub liquidity_strata: Vec<StratumFit>,
    /// Per horizon-bucket stratum fit.
    pub horizon_strata: Vec<StratumFit>,
    /// Per substitution-reason stratum fit (plus the clean baseline).
    pub substitution_strata: Vec<StratumFit>,
}

/// The full calibration of a weighted artifact: the refined return model, the
/// tightened score multipliers, the tightened substitution rules, and the
/// stratified provenance report.
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    /// Calibrated return / downside curve.
    pub return_model: ReturnModelSpec,
    /// Tightened governed score multipliers (data-quality / liquidity / horizon).
    pub multipliers: ScoreMultiplierSpec,
    /// Tightened governed substitution confidence penalties.
    pub substitution_rules: SubstitutionConfidenceRules,
    /// Stratified provenance for the run's `metrics_json`.
    pub report: CalibrationReport,
}

/// Calibrate a weighted artifact end-to-end from realized outcomes.
///
/// Returns `None` (keep the baseline artifact verbatim) when there is
/// insufficient evidence to fit a return curve — the conservative governed
/// defaults stay in force. When `Some`, the return model is calibrated and every
/// governed multiplier is tightened (never loosened) by stratified performance.
#[must_use]
pub fn calibrate_weighted_artifact(
    samples: &[CalibrationSample],
    baseline: &WeightedFactorModelArtifact,
) -> Option<CalibrationResult> {
    let curve = calibrate_return_model(samples)?;
    let return_curve_points = curve.expected_return_curve.len() as u64;

    let (data_quality, data_quality_strata) =
        calibrate_data_quality(samples, &baseline.multipliers.data_quality);
    let (liquidity, liquidity_strata) =
        calibrate_liquidity_multipliers(samples, &baseline.multipliers.liquidity);
    let (horizon, horizon_strata) =
        calibrate_horizon_multipliers(samples, &baseline.multipliers.horizon);
    let (substitution_rules, substitution_strata) =
        calibrate_substitution_rules(samples, &baseline.substitution_confidence_rules);

    Some(CalibrationResult {
        return_model: ReturnModelSpec::Calibrated(curve),
        multipliers: ScoreMultiplierSpec {
            data_quality,
            liquidity,
            horizon,
        },
        substitution_rules,
        report: CalibrationReport {
            total_samples: samples.len() as u64,
            return_curve_points,
            data_quality_strata,
            liquidity_strata,
            horizon_strata,
            substitution_strata,
        },
    })
}

/// Fit a monotone calibrated return model from realized outcomes.
///
/// Returns `None` (keep the heuristic) when there is insufficient evidence
/// (`< MIN_TOTAL_SAMPLES`, or fewer than two populated score buckets).
#[must_use]
pub fn calibrate_return_model(samples: &[CalibrationSample]) -> Option<CalibratedReturnModel> {
    if samples.len() < MIN_TOTAL_SAMPLES {
        return None;
    }

    // Bucket by composite score; collect mean realized return and mean downside
    // (loss magnitude) per populated bucket.
    let mut expected_knots: Vec<(Decimal, Decimal)> = Vec::new();
    let mut downside_knots: Vec<(Decimal, Decimal)> = Vec::new();
    for bucket in 0..SCORE_BUCKETS {
        let lo = Decimal::from(bucket as u64) / Decimal::from(SCORE_BUCKETS as u64);
        let hi = Decimal::from((bucket + 1) as u64) / Decimal::from(SCORE_BUCKETS as u64);
        let midpoint = ((lo + hi) / Decimal::from(2)).round_dp(RESEARCH_DECIMAL_SCALE);
        let in_bucket: Vec<&CalibrationSample> = samples
            .iter()
            .filter(|s| {
                let score = s.composite_score;
                if bucket + 1 == SCORE_BUCKETS {
                    score >= lo && score <= hi
                } else {
                    score >= lo && score < hi
                }
            })
            .collect();
        if in_bucket.len() < MIN_BUCKET_SAMPLES {
            continue;
        }
        let count = Decimal::from(in_bucket.len() as u64);
        let mean_return: Decimal = in_bucket
            .iter()
            .map(|s| s.realized_return_bps)
            .sum::<Decimal>()
            / count;
        let mean_downside: Decimal = in_bucket
            .iter()
            .map(|s| (-s.realized_return_bps).max(Decimal::ZERO))
            .sum::<Decimal>()
            / count;
        expected_knots.push((midpoint, mean_return.round_dp(RESEARCH_DECIMAL_SCALE)));
        downside_knots.push((midpoint, mean_downside.round_dp(RESEARCH_DECIMAL_SCALE)));
    }

    if expected_knots.len() < 2 {
        return None;
    }

    // Enforce monotonicity: expected return non-decreasing in score, downside
    // non-increasing in score (higher conviction ⇒ more upside, less downside).
    let expected_values =
        pava_non_decreasing(&expected_knots.iter().map(|(_, v)| *v).collect::<Vec<_>>());
    let downside_values =
        pava_non_increasing(&downside_knots.iter().map(|(_, v)| *v).collect::<Vec<_>>());

    let expected_return_curve = expected_knots
        .iter()
        .zip(&expected_values)
        .map(|((score, _), bps)| ReturnCurvePoint {
            score: *score,
            bps: bps.round_dp(RESEARCH_DECIMAL_SCALE),
        })
        .collect();
    let downside_curve = downside_knots
        .iter()
        .zip(&downside_values)
        .map(|((score, _), bps)| ReturnCurvePoint {
            score: *score,
            bps: bps.round_dp(RESEARCH_DECIMAL_SCALE),
        })
        .collect();

    Some(CalibratedReturnModel {
        expected_return_curve,
        downside_curve,
        fit_sample_size: samples.len() as u64,
    })
}

/// Tighten the governed data-quality multipliers from realized performance.
///
/// Fail-closed: a stratum with insufficient samples keeps the baseline, and no
/// calibrated multiplier is ever more optimistic than the baseline it refines.
#[must_use]
pub fn calibrate_score_multipliers(
    samples: &[CalibrationSample],
    baseline: &DataQualityMultipliers,
) -> DataQualityMultipliers {
    calibrate_data_quality(samples, baseline).0
}

/// Data-quality multiplier calibration + the per-stratum provenance fit.
fn calibrate_data_quality(
    samples: &[CalibrationSample],
    baseline: &DataQualityMultipliers,
) -> (DataQualityMultipliers, Vec<StratumFit>) {
    let strata = [
        DataQualityStatus::Fresh,
        DataQualityStatus::Acceptable,
        DataQualityStatus::Degraded,
        DataQualityStatus::Stale,
        DataQualityStatus::Insufficient,
    ];
    let means: Vec<Option<Decimal>> = strata
        .iter()
        .map(|status| stratum_mean(samples.iter().filter(|s| s.data_quality == *status)))
        .collect();
    let best = best_of(&means);
    let fits = strata
        .iter()
        .zip(&means)
        .map(|(status, mean)| StratumFit {
            label: status.as_str().to_owned(),
            sample_count: count_in(samples.iter().filter(|s| s.data_quality == *status)),
            mean_realized_bps: mean.unwrap_or(Decimal::ZERO),
        })
        .collect();
    let calibrated = DataQualityMultipliers {
        fresh: fail_closed(baseline.fresh, means[0], best),
        acceptable: fail_closed(baseline.acceptable, means[1], best),
        degraded: fail_closed(baseline.degraded, means[2], best),
        stale: fail_closed(baseline.stale, means[3], best),
        insufficient: fail_closed(baseline.insufficient, means[4], best),
    };
    (calibrated, fits)
}

/// Tighten the liquidity step-function multipliers from realized performance per
/// liquidity tier (tier thresholds are preserved; only multipliers tighten).
#[must_use]
pub fn calibrate_liquidity_multipliers(
    samples: &[CalibrationSample],
    baseline: &LiquidityMultipliers,
) -> (LiquidityMultipliers, Vec<StratumFit>) {
    // Tier means (index-aligned with `baseline.tiers`) plus the floor stratum
    // (unknown / below-lowest-tier liquidity).
    let tier_means: Vec<Option<Decimal>> =
        (0..baseline.tiers.len())
            .map(|tier| {
                stratum_mean(samples.iter().filter(|s| {
                    liquidity_tier_index(&baseline.tiers, s.liquidity_usd) == Some(tier)
                }))
            })
            .collect();
    let floor_mean = stratum_mean(
        samples
            .iter()
            .filter(|s| liquidity_tier_index(&baseline.tiers, s.liquidity_usd).is_none()),
    );

    let mut all_means = tier_means.clone();
    all_means.push(floor_mean);
    let best = best_of(&all_means);

    let tiers = baseline
        .tiers
        .iter()
        .zip(&tier_means)
        .map(|(tier, mean)| LiquidityTier {
            min_liquidity_usd: tier.min_liquidity_usd,
            multiplier: fail_closed(tier.multiplier, *mean, best),
        })
        .collect();
    let calibrated = LiquidityMultipliers {
        tiers,
        floor: fail_closed(baseline.floor, floor_mean, best),
    };

    let mut fits: Vec<StratumFit> =
        baseline
            .tiers
            .iter()
            .zip(&tier_means)
            .enumerate()
            .map(|(idx, (tier, mean))| StratumFit {
                label: format!("liquidity>={}", tier.min_liquidity_usd),
                sample_count: count_in(samples.iter().filter(|s| {
                    liquidity_tier_index(&baseline.tiers, s.liquidity_usd) == Some(idx)
                })),
                mean_realized_bps: mean.unwrap_or(Decimal::ZERO),
            })
            .collect();
    fits.push(StratumFit {
        label: "liquidity_floor".to_owned(),
        sample_count: count_in(
            samples
                .iter()
                .filter(|s| liquidity_tier_index(&baseline.tiers, s.liquidity_usd).is_none()),
        ),
        mean_realized_bps: floor_mean.unwrap_or(Decimal::ZERO),
    });
    (calibrated, fits)
}

/// Tighten the horizon multipliers from realized performance per horizon bucket
/// (the acceptable-window ratio bounds are preserved; only multipliers tighten).
#[must_use]
pub fn calibrate_horizon_multipliers(
    samples: &[CalibrationSample],
    baseline: &HorizonMultipliers,
) -> (HorizonMultipliers, Vec<StratumFit>) {
    let bucket_of = |s: &CalibrationSample| horizon_bucket(s, baseline);
    let too_soon = stratum_mean(
        samples
            .iter()
            .filter(|s| bucket_of(s) == HorizonBucket::TooSoon),
    );
    let in_window = stratum_mean(
        samples
            .iter()
            .filter(|s| bucket_of(s) == HorizonBucket::InWindow),
    );
    let too_late = stratum_mean(
        samples
            .iter()
            .filter(|s| bucket_of(s) == HorizonBucket::TooLate),
    );

    let best = best_of(&[too_soon, in_window, too_late]);
    let calibrated = HorizonMultipliers {
        in_window: fail_closed(baseline.in_window, in_window, best),
        too_soon: fail_closed(baseline.too_soon, too_soon, best),
        too_late: fail_closed(baseline.too_late, too_late, best),
        min_ratio: baseline.min_ratio,
        max_ratio: baseline.max_ratio,
    };
    let fits = vec![
        horizon_fit(
            "horizon_too_soon",
            samples,
            baseline,
            HorizonBucket::TooSoon,
        ),
        horizon_fit(
            "horizon_in_window",
            samples,
            baseline,
            HorizonBucket::InWindow,
        ),
        horizon_fit(
            "horizon_too_late",
            samples,
            baseline,
            HorizonBucket::TooLate,
        ),
    ];
    (calibrated, fits)
}

/// Tighten the substitution confidence penalties from realized performance.
///
/// Every reason's realized performance is compared against the best stratum
/// (clean baseline + each reason), so a substitution that on average hurt the
/// realized return collapses its confidence multiplier — never above baseline.
#[must_use]
pub fn calibrate_substitution_rules(
    samples: &[CalibrationSample],
    baseline: &SubstitutionConfidenceRules,
) -> (SubstitutionConfidenceRules, Vec<StratumFit>) {
    let reasons = [
        NullReason::SourceUnavailable,
        NullReason::StaleBeyondPolicy,
        NullReason::OutOfValidRange,
        NullReason::InsufficientHistory,
        NullReason::NotApplicable,
        NullReason::LegBookMissing,
        NullReason::TradeTapeUnavailable,
        NullReason::InsufficientTradeTape,
        NullReason::InsufficientRoleCoverage,
        NullReason::DomainSourceUnavailable,
        NullReason::LinkageUnresolved,
    ];
    let clean = stratum_mean(samples.iter().filter(|s| s.substitution_reasons.is_empty()));
    let reason_means: Vec<Option<Decimal>> = reasons
        .iter()
        .map(|reason| stratum_mean(samples.iter().filter(|s| has_reason(s, *reason))))
        .collect();

    let mut all_means = reason_means.clone();
    all_means.push(clean);
    let best = best_of(&all_means);

    let calibrated = SubstitutionConfidenceRules {
        source_unavailable: fail_closed(baseline.source_unavailable, reason_means[0], best),
        stale_beyond_policy: fail_closed(baseline.stale_beyond_policy, reason_means[1], best),
        out_of_valid_range: fail_closed(baseline.out_of_valid_range, reason_means[2], best),
        insufficient_history: fail_closed(baseline.insufficient_history, reason_means[3], best),
        not_applicable: fail_closed(baseline.not_applicable, reason_means[4], best),
        leg_book_missing: fail_closed(baseline.leg_book_missing, reason_means[5], best),
        trade_tape_unavailable: fail_closed(baseline.trade_tape_unavailable, reason_means[6], best),
        insufficient_trade_tape: fail_closed(
            baseline.insufficient_trade_tape,
            reason_means[7],
            best,
        ),
        insufficient_role_coverage: fail_closed(
            baseline.insufficient_role_coverage,
            reason_means[8],
            best,
        ),
        domain_source_unavailable: fail_closed(
            baseline.domain_source_unavailable,
            reason_means[9],
            best,
        ),
        linkage_unresolved: fail_closed(baseline.linkage_unresolved, reason_means[10], best),
    };
    let mut fits: Vec<StratumFit> = reasons
        .iter()
        .zip(&reason_means)
        .map(|(reason, mean)| StratumFit {
            label: format!("substitution_{}", reason_label(*reason)),
            sample_count: count_in(samples.iter().filter(|s| has_reason(s, *reason))),
            mean_realized_bps: mean.unwrap_or(Decimal::ZERO),
        })
        .collect();
    fits.push(StratumFit {
        label: "substitution_none".to_owned(),
        sample_count: count_in(samples.iter().filter(|s| s.substitution_reasons.is_empty())),
        mean_realized_bps: clean.unwrap_or(Decimal::ZERO),
    });
    (calibrated, fits)
}

/// A market's horizon stratum relative to the model's prediction horizon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HorizonBucket {
    /// Resolves sooner than `min_ratio × prediction_horizon`.
    TooSoon,
    /// Resolves within `[min_ratio, max_ratio] × prediction_horizon` (or unknown).
    InWindow,
    /// Resolves later than `max_ratio × prediction_horizon`.
    TooLate,
}

/// Classify a sample into its horizon bucket (unknown horizon ⇒ in-window, the
/// same rule [`HorizonMultipliers::multiplier_for`] applies online).
fn horizon_bucket(sample: &CalibrationSample, baseline: &HorizonMultipliers) -> HorizonBucket {
    let (Some(ttr), true) = (
        sample.time_to_resolution_secs,
        sample.prediction_horizon_secs > 0,
    ) else {
        return HorizonBucket::InWindow;
    };
    let ratio = Decimal::from(ttr) / Decimal::from(sample.prediction_horizon_secs);
    if ratio < baseline.min_ratio {
        HorizonBucket::TooSoon
    } else if ratio > baseline.max_ratio {
        HorizonBucket::TooLate
    } else {
        HorizonBucket::InWindow
    }
}

/// One horizon-bucket provenance fit.
fn horizon_fit(
    label: &str,
    samples: &[CalibrationSample],
    baseline: &HorizonMultipliers,
    bucket: HorizonBucket,
) -> StratumFit {
    let subset = samples
        .iter()
        .filter(|s| horizon_bucket(s, baseline) == bucket);
    StratumFit {
        label: label.to_owned(),
        sample_count: count_in(
            samples
                .iter()
                .filter(|s| horizon_bucket(s, baseline) == bucket),
        ),
        mean_realized_bps: stratum_mean(subset).unwrap_or(Decimal::ZERO),
    }
}

/// The highest tier whose `min_liquidity_usd` bound is `≤` the liquidity, or
/// `None` for unknown / below-lowest liquidity (the floor stratum).
fn liquidity_tier_index(tiers: &[LiquidityTier], liquidity: Option<Decimal>) -> Option<usize> {
    let liquidity = liquidity?;
    tiers
        .iter()
        .enumerate()
        .rev()
        .find(|(_, tier)| liquidity >= tier.min_liquidity_usd)
        .map(|(idx, _)| idx)
}

/// Whether a sample carried a given substitution reason.
fn has_reason(sample: &CalibrationSample, reason: NullReason) -> bool {
    sample.substitution_reasons.contains(&reason)
}

/// Stable wire label for a substitution reason (provenance only).
const fn reason_label(reason: NullReason) -> &'static str {
    match reason {
        NullReason::SourceUnavailable => "source_unavailable",
        NullReason::StaleBeyondPolicy => "stale_beyond_policy",
        NullReason::OutOfValidRange => "out_of_valid_range",
        NullReason::InsufficientHistory => "insufficient_history",
        NullReason::NotApplicable => "not_applicable",
        NullReason::LegBookMissing => "leg_book_missing",
        NullReason::TradeTapeUnavailable => "trade_tape_unavailable",
        NullReason::InsufficientTradeTape => "insufficient_trade_tape",
        NullReason::InsufficientRoleCoverage => "insufficient_role_coverage",
        NullReason::DomainSourceUnavailable => "domain_source_unavailable",
        NullReason::LinkageUnresolved => "linkage_unresolved",
    }
}

/// Fail-closed multiplier: the calibrated value is the baseline tightened toward
/// the stratum's realized performance relative to the best stratum, and is never
/// more optimistic than the baseline. A stratum below the sample floor, or no
/// positive best stratum, keeps the baseline.
fn fail_closed(base: Decimal, stratum_mean: Option<Decimal>, best: Decimal) -> Decimal {
    if best <= Decimal::ZERO {
        return base;
    }
    stratum_mean.map_or(base, |mean| {
        let ratio = (mean.max(Decimal::ZERO) / best).clamp(Decimal::ZERO, Decimal::ONE);
        base.min(ratio).round_dp(RESEARCH_DECIMAL_SCALE)
    })
}

/// Mean realized return over a sample subset, or `None` below the sample floor.
fn stratum_mean<'a>(samples: impl Iterator<Item = &'a CalibrationSample>) -> Option<Decimal> {
    let realized: Vec<Decimal> = samples.map(|s| s.realized_return_bps).collect();
    if realized.len() < MIN_BUCKET_SAMPLES {
        return None;
    }
    let count = Decimal::from(realized.len() as u64);
    Some(realized.iter().sum::<Decimal>() / count)
}

/// Count of samples in a subset (provenance only; no floor applied).
fn count_in<'a>(samples: impl Iterator<Item = &'a CalibrationSample>) -> u64 {
    samples.count() as u64
}

/// The best (highest) populated stratum mean, or zero when none qualify.
fn best_of(means: &[Option<Decimal>]) -> Decimal {
    means
        .iter()
        .filter_map(|m| *m)
        .fold(Decimal::ZERO, Decimal::max)
}

/// Pool-adjacent-violators isotonic regression producing a non-decreasing series.
fn pava_non_decreasing(values: &[Decimal]) -> Vec<Decimal> {
    // Each pool: (weighted sum, count).
    let mut pools: Vec<(Decimal, u64)> = Vec::with_capacity(values.len());
    for &value in values {
        pools.push((value, 1));
        // Merge while the last pool's mean violates monotonicity.
        while pools.len() >= 2 {
            let (sum_b, n_b) = pools[pools.len() - 1];
            let (sum_a, n_a) = pools[pools.len() - 2];
            let mean_a = sum_a / Decimal::from(n_a);
            let mean_b = sum_b / Decimal::from(n_b);
            if mean_a <= mean_b {
                break;
            }
            pools.pop();
            pools.pop();
            pools.push((sum_a + sum_b, n_a + n_b));
        }
    }
    expand_pools(&pools, values.len())
}

/// Isotonic regression producing a non-increasing series (PAVA on the reverse).
fn pava_non_increasing(values: &[Decimal]) -> Vec<Decimal> {
    let reversed: Vec<Decimal> = values.iter().rev().copied().collect();
    let mut out = pava_non_decreasing(&reversed);
    out.reverse();
    out
}

/// Expand merged pools back into a per-knot series of pool means.
fn expand_pools(pools: &[(Decimal, u64)], len: usize) -> Vec<Decimal> {
    let mut out = Vec::with_capacity(len);
    for &(sum, count) in pools {
        let mean = (sum / Decimal::from(count)).round_dp(RESEARCH_DECIMAL_SCALE);
        for _ in 0..count {
            out.push(mean);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{
        CalibrationSample, calibrate_liquidity_multipliers, calibrate_return_model,
        calibrate_score_multipliers, calibrate_substitution_rules,
    };
    use quant_pivot_models::enums::quant::DataQualityStatus;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use crate::{
        features::NullReason,
        model::artifact::{
            DataQualityMultipliers, LiquidityMultipliers, SubstitutionConfidenceRules,
        },
    };

    /// Build a sample with only the score / realized / data-quality stratum set
    /// (the liquidity / horizon / substitution strata default to "unknown").
    fn dq_sample(score: Decimal, realized: Decimal, dq: DataQualityStatus) -> CalibrationSample {
        CalibrationSample {
            composite_score: score,
            realized_return_bps: realized,
            data_quality: dq,
            liquidity_usd: None,
            time_to_resolution_secs: None,
            prediction_horizon_secs: 0,
            substitution_reasons: Vec::new(),
        }
    }

    /// Higher-score candidates realize higher returns ⇒ a monotone, calibrated
    /// expected-return curve.
    #[test]
    fn calibration_curve_is_monotone() {
        let samples: Vec<CalibrationSample> = (0..200)
            .map(|i| {
                let score = Decimal::from(i % 100) / dec!(100);
                let realized = score * dec!(400) - dec!(100) + Decimal::from(i % 3) * dec!(5);
                dq_sample(score, realized, DataQualityStatus::Fresh)
            })
            .collect();

        let model = calibrate_return_model(&samples).expect("calibrated");
        assert!(model.fit_sample_size >= 30);
        assert!(model.expected_return_curve.len() >= 2);
        for window in model.expected_return_curve.windows(2) {
            assert!(window[0].score < window[1].score, "scores ascending");
            assert!(window[0].bps <= window[1].bps, "expected return monotone");
        }
    }

    #[test]
    fn insufficient_samples_keep_heuristic() {
        let samples: Vec<CalibrationSample> = (0..5)
            .map(|i| {
                dq_sample(
                    Decimal::from(i) / dec!(10),
                    dec!(10),
                    DataQualityStatus::Fresh,
                )
            })
            .collect();
        assert!(calibrate_return_model(&samples).is_none());
    }

    /// A data-quality stratum that loses money collapses to a harsher multiplier;
    /// no stratum ever exceeds the governed baseline (fail-closed).
    #[test]
    fn calibration_multipliers_fail_closed() {
        let baseline = DataQualityMultipliers::conservative();
        // Fresh wins big; degraded loses; stale unobserved.
        let mut samples = Vec::new();
        for _ in 0..10 {
            samples.push(dq_sample(dec!(0.8), dec!(300), DataQualityStatus::Fresh));
            samples.push(dq_sample(
                dec!(0.4),
                dec!(-200),
                DataQualityStatus::Degraded,
            ));
        }
        let calibrated = calibrate_score_multipliers(&samples, &baseline);
        assert!(calibrated.fresh <= baseline.fresh, "never exceeds baseline");
        assert!(
            calibrated.degraded <= baseline.degraded,
            "losing stratum tightened"
        );
        assert_eq!(
            calibrated.degraded,
            dec!(0),
            "a money-losing stratum collapses to zero"
        );
        // Unobserved stratum keeps the conservative baseline.
        assert_eq!(calibrated.stale, baseline.stale);
    }

    /// A substitution reason whose samples underperform the clean baseline gets a
    /// tighter confidence multiplier, never above the governed baseline.
    #[test]
    fn calibration_substitution_never_exceeds_baseline() {
        let baseline = SubstitutionConfidenceRules::conservative();
        let mut samples = Vec::new();
        for _ in 0..10 {
            // Clean samples win; substituted samples lose.
            samples.push(CalibrationSample {
                composite_score: dec!(0.7),
                realized_return_bps: dec!(250),
                data_quality: DataQualityStatus::Fresh,
                liquidity_usd: None,
                time_to_resolution_secs: None,
                prediction_horizon_secs: 0,
                substitution_reasons: Vec::new(),
            });
            samples.push(CalibrationSample {
                composite_score: dec!(0.6),
                realized_return_bps: dec!(-150),
                data_quality: DataQualityStatus::Fresh,
                liquidity_usd: None,
                time_to_resolution_secs: None,
                prediction_horizon_secs: 0,
                substitution_reasons: vec![NullReason::SourceUnavailable],
            });
        }
        let (calibrated, fits) = calibrate_substitution_rules(&samples, &baseline);
        assert!(
            calibrated.source_unavailable <= baseline.source_unavailable,
            "tightened, never loosened"
        );
        assert_eq!(
            calibrated.source_unavailable,
            dec!(0),
            "a money-losing substitution collapses to zero"
        );
        // Unobserved reasons keep the conservative baseline.
        assert_eq!(calibrated.leg_book_missing, baseline.leg_book_missing);
        assert!(fits.iter().any(|f| f.label == "substitution_none"));
    }

    /// The liquidity tiers are stratified by realized performance; deeper books
    /// keep the higher multiplier when they realize the best returns.
    #[test]
    fn calibration_liquidity_tiers_are_stratified() {
        let baseline = LiquidityMultipliers::conservative();
        let mut samples = Vec::new();
        for _ in 0..10 {
            // Deep books win; thin books lose; unknown liquidity loses most.
            samples.push(sample_with_liquidity(dec!(300), Some(dec!(100000))));
            samples.push(sample_with_liquidity(dec!(-50), Some(dec!(500))));
            samples.push(sample_with_liquidity(dec!(-200), None));
        }
        let (calibrated, fits) = calibrate_liquidity_multipliers(&samples, &baseline);
        for (tier, base_tier) in calibrated.tiers.iter().zip(&baseline.tiers) {
            assert!(tier.multiplier <= base_tier.multiplier, "fail-closed tiers");
        }
        assert!(calibrated.floor <= baseline.floor, "fail-closed floor");
        assert_eq!(
            fits.len(),
            baseline.tiers.len() + 1,
            "one fit per tier + floor"
        );
    }

    fn sample_with_liquidity(realized: Decimal, liquidity: Option<Decimal>) -> CalibrationSample {
        CalibrationSample {
            composite_score: dec!(0.5),
            realized_return_bps: realized,
            data_quality: DataQualityStatus::Fresh,
            liquidity_usd: liquidity,
            time_to_resolution_secs: None,
            prediction_horizon_secs: 0,
            substitution_reasons: Vec::new(),
        }
    }
}
