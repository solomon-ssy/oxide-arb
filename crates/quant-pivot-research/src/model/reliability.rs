//! [`ReliabilityReport`]: Brier score, log-loss, and ECE diagnostics.
//!
//! Also per-bin reliability-diagram data for a fitted
//! [`crate::model::calibrator::ProbabilityCalibrator`].
//!
//! Also the source of the per-calibrated-probability-bucket
//! `mean_adverse_excursion_bps` the `Calibrated` return model's
//! `DownsideSource::MfeMae` reads at serving time — computed on the
//! **same** independent calibration split the probability mapping was fit
//! on, never re-derived from training data.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::{
    Probability,
    calibration::{MonotoneMapping, ReliabilityBin, ReliabilityReport},
};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    model::{
        CancellationProbe,
        calibrator::{apply_validated_mapping, validate_mapping_cancellable},
    },
    precision::RESEARCH_DECIMAL_SCALE,
    stats::{count_f64, wilson_interval, wilson_z},
};

/// Number of equal-width `[0, 1]` calibrated-probability buckets in a
/// reliability report.
const RELIABILITY_BINS: usize = 10;
/// `f64` floor/ceiling probabilities never touched exactly (avoids `ln(0)`).
const LOG_LOSS_EPS: f64 = 1e-12;
const CANCELLATION_INTERVAL: usize = 1_024;

/// One calibration-split observation feeding a [`ReliabilityReport`].
pub struct ReliabilitySample {
    pub score: Decimal,
    pub won: bool,
    pub max_adverse_excursion_bps: Option<Decimal>,
}

/// Compute the full reliability report for a fitted mapping over its
/// calibration-split samples.
///
/// # Errors
///
/// Propagates mapping validation, cooperative cancellation, and numeric
/// conversion failures. An empty split yields a zeroed report; the caller's
/// sample-count gate upstream is what fails closed.
pub fn compute_reliability(
    mapping: &MonotoneMapping,
    samples: &[ReliabilitySample],
    ci_confidence: Decimal,
    cancellation: &CancellationProbe,
) -> QuantResult<ReliabilityReport> {
    cancellation.check("reliability start")?;
    let n = samples.len();
    if n == 0 {
        return Ok(ReliabilityReport {
            bins: Vec::new(),
            brier_score: Decimal::ZERO,
            log_loss: Decimal::ZERO,
            ece: Decimal::ZERO,
            n_samples: 0,
        });
    }
    validate_mapping_cancellable(mapping, cancellation)?;

    let mut calibrated = Vec::with_capacity(samples.len());
    for (index, sample) in samples.iter().enumerate() {
        if index % CANCELLATION_INTERVAL == 0 {
            cancellation.check("reliability mapping application")?;
        }
        calibrated.push(apply_validated_mapping(mapping, sample.score)?);
    }

    let sample_count = u64::try_from(n).map_err(|error| ResearchError::ValidationMethodology {
        detail: format!("reliability sample count exceeds u64: {error}"),
    })?;
    let mut brier_sum = Decimal::ZERO;
    let mut log_loss_sum = Decimal::ZERO;
    for (index, (sample, probability)) in samples.iter().zip(&calibrated).enumerate() {
        if index % CANCELLATION_INTERVAL == 0 {
            cancellation.check("reliability metric accumulation")?;
        }
        let observed = if sample.won {
            Decimal::ONE
        } else {
            Decimal::ZERO
        };
        brier_sum += (probability.inner() - observed) * (probability.inner() - observed);
        log_loss_sum += log_loss_term(probability.inner(), sample.won)?;
    }
    let denominator = Decimal::from(sample_count);
    let brier_score = (brier_sum / denominator).round_dp(RESEARCH_DECIMAL_SCALE);
    let log_loss = (log_loss_sum / denominator).round_dp(RESEARCH_DECIMAL_SCALE);

    let z = wilson_z(ci_confidence)?;
    let bins = build_bins(samples, &calibrated, z, cancellation)?;
    let ece = expected_calibration_error(&bins, n);

    Ok(ReliabilityReport {
        bins,
        brier_score,
        log_loss,
        ece,
        n_samples: sample_count,
    })
}

/// Partition samples into `RELIABILITY_BINS` equal-width buckets over the
/// **calibrated probability** axis (`calibrated[i]`), not the raw pre-
/// calibration `score` — the standard ECE bucketing (Naeini et al.; sklearn
/// `calibration_curve`). Binning on the raw score instead would make ECE, the
/// reliability diagram, and the `mean_adverse_excursion_bps` bucket lookup
/// depend on the (arbitrary, method-specific) score→probability curvature
/// rather than the probability itself.
fn build_bins(
    samples: &[ReliabilitySample],
    calibrated: &[Probability],
    z: f64,
    cancellation: &CancellationProbe,
) -> QuantResult<Vec<ReliabilityBin>> {
    let width = Decimal::ONE / Decimal::from(RELIABILITY_BINS as u64);
    let mut bins = Vec::new();
    for index in 0..RELIABILITY_BINS {
        let lo = width * Decimal::from(index as u64);
        let top = index + 1 == RELIABILITY_BINS;
        let hi = if top {
            Decimal::ONE
        } else {
            width * Decimal::from((index + 1) as u64)
        };
        let mut member_count = 0_u64;
        let mut wins = 0_u64;
        let mut predicted_sum = Decimal::ZERO;
        let mut mae_sum = Decimal::ZERO;
        let mut mae_count = 0_u64;
        for (sample_index, probability) in calibrated.iter().enumerate() {
            if sample_index % CANCELLATION_INTERVAL == 0 {
                cancellation.check("reliability bin scan")?;
            }
            let value = probability.inner();
            if value >= lo && (value < hi || (top && value <= hi)) {
                member_count = member_count.checked_add(1).ok_or_else(|| {
                    ResearchError::ValidationMethodology {
                        detail: "reliability-bin sample count overflowed".to_owned(),
                    }
                })?;
                wins = wins
                    .checked_add(u64::from(samples[sample_index].won))
                    .ok_or_else(|| ResearchError::ValidationMethodology {
                        detail: "reliability-bin win count overflowed".to_owned(),
                    })?;
                predicted_sum += value;
                if let Some(mae) = samples[sample_index].max_adverse_excursion_bps {
                    mae_sum += mae;
                    mae_count = mae_count.checked_add(1).ok_or_else(|| {
                        ResearchError::ValidationMethodology {
                            detail: "reliability-bin downside count overflowed".to_owned(),
                        }
                    })?;
                }
            }
        }
        if member_count == 0 {
            continue;
        }
        let mean_predicted =
            (predicted_sum / Decimal::from(member_count)).round_dp(RESEARCH_DECIMAL_SCALE);
        let p_hat = count_f64(wins)? / count_f64(member_count)?;
        let empirical_frequency =
            Decimal::from_f64(p_hat).ok_or_else(|| ResearchError::ValidationMethodology {
                detail: "empirical reliability frequency is not representable as Decimal"
                    .to_owned(),
            })?;
        let (ci_lo, ci_hi) = wilson_interval(p_hat, member_count, z, RESEARCH_DECIMAL_SCALE)?;
        let mean_adverse_excursion_bps = if mae_count == 0 {
            None
        } else {
            Some((mae_sum / Decimal::from(mae_count)).round_dp(RESEARCH_DECIMAL_SCALE))
        };
        bins.push(ReliabilityBin {
            predicted_lo: lo,
            predicted_hi: hi,
            sample_count: member_count,
            mean_predicted: Probability::new(mean_predicted.round_dp(RESEARCH_DECIMAL_SCALE)),
            empirical_frequency: Probability::new(
                empirical_frequency.round_dp(RESEARCH_DECIMAL_SCALE),
            ),
            wilson_ci: (Probability::new(ci_lo), Probability::new(ci_hi)),
            mean_adverse_excursion_bps,
        });
    }
    cancellation.check("reliability bins completion")?;
    Ok(bins)
}

/// `Σ (bin_count / n) * |mean_predicted - empirical_frequency|`.
fn expected_calibration_error(bins: &[ReliabilityBin], n: usize) -> Decimal {
    if n == 0 {
        return Decimal::ZERO;
    }
    let total = Decimal::from(n as u64);
    bins.iter()
        .map(|bin| {
            let weight = Decimal::from(bin.sample_count) / total;
            weight * (bin.mean_predicted.inner() - bin.empirical_frequency.inner()).abs()
        })
        .sum::<Decimal>()
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

/// `-[y*ln(p) + (1-y)*ln(1-p)]`, clamped away from `{0, 1}` to avoid `ln(0)`.
fn log_loss_term(p: Decimal, won: bool) -> QuantResult<Decimal> {
    let clamped = p
        .to_f64()
        .ok_or_else(|| ResearchError::ValidationMethodology {
            detail: format!("calibrated probability {p} is not representable as f64"),
        })?
        .clamp(LOG_LOSS_EPS, 1.0 - LOG_LOSS_EPS);
    let term = if won {
        -clamped.ln()
    } else {
        -(1.0 - clamped).ln()
    };
    Decimal::from_f64(term).ok_or_else(|| {
        ResearchError::ValidationMethodology {
            detail: format!("log-loss term {term} is not representable as Decimal"),
        }
        .into()
    })
}

#[cfg(test)]
fn mean_decimal(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    (values.iter().sum::<Decimal>() / Decimal::from(values.len() as u64))
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use quant_pivot_models::types::calibration::{IsotonicKnot, MonotoneMapping};
    use rust_decimal::{Decimal, prelude::ToPrimitive};
    use rust_decimal_macros::dec;

    use super::{ReliabilitySample, compute_reliability, mean_decimal};
    use crate::model::{
        CancellationProbe,
        calibrator::{ProbabilityCalibrator, isotonic::IsotonicCalibrator},
    };

    #[test]
    fn truly_calibrated_zero_ece() {
        // Ten groups, one per reliability bin, each carrying a score that
        // *is* its own bin midpoint and an empirical win rate exactly equal
        // to that midpoint. A perfect isotonic fit reproduces `calibrated ==
        // score` here (the group means are already non-decreasing), so
        // `mean_predicted` must match `empirical_frequency` almost exactly
        // in every bin — a real ECE-near-zero assertion, unlike the previous
        // `i % 2` alternating-outcome fixture (which is not perfectly
        // calibrated at all) with a vacuous `ece <= 0.5` bound that nearly
        // any report would satisfy.
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for bucket in 0..10_i64 {
            let target_p = (Decimal::from(bucket) + dec!(0.5)) / dec!(10);
            let score = target_p;
            let positives = (target_p * dec!(100)).round().to_i64().unwrap_or(0);
            for i in 0..100_i64 {
                scores.push(score);
                outcomes.push(i < positives);
            }
        }
        let calibrator = IsotonicCalibrator::new(10);
        let mapping = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("fit");
        let samples: Vec<ReliabilitySample> = scores
            .iter()
            .zip(&outcomes)
            .map(|(&score, &won)| ReliabilitySample {
                score,
                won,
                max_adverse_excursion_bps: Some(dec!(-150)),
            })
            .collect();
        let report = compute_reliability(
            &mapping,
            &samples,
            dec!(0.95),
            &CancellationProbe::default(),
        )
        .expect("reliability");
        assert_eq!(report.n_samples, 1000);
        assert_eq!(report.bins.len(), 10, "{:?}", report.bins);
        assert!(
            report.ece <= dec!(0.001),
            "truly calibrated data must yield near-zero ECE, got {}",
            report.ece
        );
        for bin in &report.bins {
            assert_eq!(bin.mean_adverse_excursion_bps, Some(dec!(-150)));
        }
    }

    #[test]
    fn reliability_ece_not_score() {
        // A steep Platt sigmoid (a=-10, b=5) maps score=0.55 to a calibrated
        // probability of ≈0.622. Binning on the *raw score* would place this
        // sample in bucket 5 (`[0.5, 0.6)`); binning on the *calibrated
        // probability* (the correct, standard-ECE behavior) must place it in
        // bucket 6 (`[0.6, 0.7)`) instead.
        let mapping = MonotoneMapping::Platt {
            a: dec!(-10),
            b: dec!(5),
        };
        let mut scores = Vec::new();
        for i in 0..=20_i64 {
            scores.push(Decimal::from(i) / dec!(20));
        }
        let samples: Vec<ReliabilitySample> = scores
            .iter()
            .map(|&score| ReliabilitySample {
                score,
                won: true,
                max_adverse_excursion_bps: None,
            })
            .collect();
        let report = compute_reliability(
            &mapping,
            &samples,
            dec!(0.95),
            &CancellationProbe::default(),
        )
        .expect("reliability");
        let bin_with_score_point_five_five = report
            .bins
            .iter()
            .find(|bin| dec!(0.622) >= bin.predicted_lo && dec!(0.622) < bin.predicted_hi)
            .expect("a bin covering the calibrated probability at score=0.55");
        // The correct calibrated-probability bucket is [0.6, 0.7); binning on
        // the raw score would have (incorrectly) placed it in [0.5, 0.6).
        assert_eq!(bin_with_score_point_five_five.predicted_lo, dec!(0.6));
        assert_eq!(bin_with_score_point_five_five.predicted_hi, dec!(0.7));
    }

    #[test]
    fn log_loss_never_probabilities() {
        let mapping = MonotoneMapping::Isotonic {
            knots: vec![
                IsotonicKnot {
                    score: dec!(0),
                    probability: dec!(0),
                },
                IsotonicKnot {
                    score: dec!(1),
                    probability: dec!(1),
                },
            ],
        };
        let samples = vec![
            ReliabilitySample {
                score: dec!(0),
                won: true,
                max_adverse_excursion_bps: None,
            },
            ReliabilitySample {
                score: dec!(1),
                won: false,
                max_adverse_excursion_bps: None,
            },
        ];
        let report = compute_reliability(
            &mapping,
            &samples,
            dec!(0.95),
            &CancellationProbe::default(),
        )
        .expect("reliability");
        // `p=0`/`p=1` is clamped to `LOG_LOSS_EPS` before taking `ln`, so the
        // term is a large-but-bounded finite number (`-ln(1e-12) ≈ 27.63`),
        // never `+inf`/`NaN` (which `Decimal` cannot even represent, but an
        // unclamped `f64::ln(0.0)` boundary would silently poison the mean
        // via `Decimal::from_f64` returning `None` -> `unwrap_or(ZERO)`,
        // masking the failure instead of bounding it).
        assert!(
            report.log_loss > dec!(20) && report.log_loss < dec!(30),
            "log_loss={}",
            report.log_loss
        );
    }

    #[test]
    fn brier_log_loss_form() {
        // An identity mapping (knots at (0,0)/(1,1), exact linear
        // interpolation) makes `calibrated == score`, so Brier/log-loss can
        // be hand-verified against a fixed, independently-computed golden
        // value (Python: `sum((p-y)**2)/n` / clamped cross-entropy) rather
        // than only a relative "not worse than raw" comparison.
        let identity = MonotoneMapping::Isotonic {
            knots: vec![
                IsotonicKnot {
                    score: dec!(0),
                    probability: dec!(0),
                },
                IsotonicKnot {
                    score: dec!(1),
                    probability: dec!(1),
                },
            ],
        };
        let scores = [dec!(0.1), dec!(0.4), dec!(0.6), dec!(0.9)];
        let outcomes = [false, false, true, true];
        let samples: Vec<ReliabilitySample> = scores
            .iter()
            .zip(outcomes)
            .map(|(&score, won)| ReliabilitySample {
                score,
                won,
                max_adverse_excursion_bps: None,
            })
            .collect();
        let report = compute_reliability(
            &identity,
            &samples,
            dec!(0.95),
            &CancellationProbe::default(),
        )
        .expect("reliability");
        // Golden: brier = ((0.1-0)^2+(0.4-0)^2+(0.6-1)^2+(0.9-1)^2)/4 = 0.085 exactly.
        assert_eq!(report.brier_score, dec!(0.085));
        // Golden (Python, natural log, no clamping triggered): 0.30809306971190853.
        let log_loss_f64 = report.log_loss.to_f64().expect("f64");
        assert!(
            (log_loss_f64 - 0.308_093_069_711_908_53).abs() < 1e-6,
            "log_loss={log_loss_f64}"
        );
    }

    #[test]
    fn empty_split_yields_report() {
        let mapping = MonotoneMapping::Isotonic { knots: Vec::new() };
        let report = compute_reliability(&mapping, &[], dec!(0.95), &CancellationProbe::default())
            .expect("reliability");
        assert_eq!(report.n_samples, 0);
        assert!(report.bins.is_empty());
    }

    #[test]
    fn isotonic_calibration_improves_uncalibrated() {
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for i in 0..300_i32 {
            let score = Decimal::from(i) / dec!(300);
            scores.push(score);
            outcomes.push(i % 3 == 0);
        }
        let samples: Vec<ReliabilitySample> = scores
            .iter()
            .zip(&outcomes)
            .map(|(&score, &won)| ReliabilitySample {
                score,
                won,
                max_adverse_excursion_bps: None,
            })
            .collect();
        let raw_brier = mean_decimal(
            &samples
                .iter()
                .map(|s| {
                    let y = if s.won { Decimal::ONE } else { Decimal::ZERO };
                    let p = s.score.clamp(Decimal::ZERO, Decimal::ONE);
                    (p - y) * (p - y)
                })
                .collect::<Vec<_>>(),
        );
        let calibrator = IsotonicCalibrator::new(10);
        let mapping = calibrator
            .fit(&scores, &outcomes, &CancellationProbe::default())
            .expect("fit");
        let calibrated = compute_reliability(
            &mapping,
            &samples,
            dec!(0.95),
            &CancellationProbe::default(),
        )
        .expect("calibrated");
        assert!(
            calibrated.brier_score <= raw_brier,
            "isotonic calibration must not worsen Brier: raw={raw_brier} calibrated={}",
            calibrated.brier_score
        );
    }

    #[test]
    fn isotonic_reliability_scales() {
        let sample_count = 20_000_u64;
        let knots = (0..sample_count)
            .map(|index| IsotonicKnot {
                score: Decimal::from(index) / Decimal::from(sample_count),
                probability: Decimal::from(index) / Decimal::from(sample_count),
            })
            .collect::<Vec<_>>();
        let mapping = MonotoneMapping::Isotonic { knots };
        let samples = (0..sample_count)
            .map(|index| ReliabilitySample {
                score: Decimal::from(index) / Decimal::from(sample_count),
                won: index % 2 == 0,
                max_adverse_excursion_bps: Some(dec!(-10)),
            })
            .collect::<Vec<_>>();
        let report = compute_reliability(
            &mapping,
            &samples,
            dec!(0.95),
            &CancellationProbe::default(),
        )
        .expect("large isotonic reliability report");
        assert_eq!(report.n_samples, sample_count);
    }

    #[test]
    fn reliability_observes_cancellation() {
        let checks = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&checks);
        let cancellation =
            CancellationProbe::new(move || observed.fetch_add(1, Ordering::Relaxed) >= 4);
        let knots = (0..20_000_u64)
            .map(|index| IsotonicKnot {
                score: Decimal::from(index) / Decimal::from(20_000_u64),
                probability: Decimal::from(index) / Decimal::from(20_000_u64),
            })
            .collect::<Vec<_>>();
        let samples = (0..20_000_u64)
            .map(|index| ReliabilitySample {
                score: Decimal::from(index) / Decimal::from(20_000_u64),
                won: index % 2 == 0,
                max_adverse_excursion_bps: None,
            })
            .collect::<Vec<_>>();
        let result = compute_reliability(
            &MonotoneMapping::Isotonic { knots },
            &samples,
            dec!(0.95),
            &cancellation,
        );
        assert!(
            result.is_err(),
            "running reliability kernel must observe cancellation"
        );
        assert!(checks.load(Ordering::Relaxed) > 4);
    }
}
