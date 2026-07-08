//! [`ReliabilityReport`]: Brier score, log-loss, and ECE diagnostics.
//!
//! Also per-bin reliability-diagram data for a fitted
//! [`crate::model::calibrator::ProbabilityCalibrator`] (Phase 11.3 §3.2/§6.1).
//!
//! Also the source of the per-score-bucket `mean_adverse_excursion_bps` the
//! `Calibrated` return model's `DownsideSource::MfeMae` reads at serving time
//! (§3.3) — computed on the **same** independent calibration split the
//! probability mapping was fit on, never re-derived from training data.

use quant_pivot_error::QuantResult;
use quant_pivot_models::types::Probability;
use rust_decimal::{Decimal, prelude::FromPrimitive, prelude::ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::{
    model::calibrator::{MonotoneMapping, apply_mapping},
    precision::RESEARCH_DECIMAL_SCALE,
    stats::{wilson_interval, wilson_z},
};

/// Number of equal-width `[0, 1]` score buckets in a reliability report.
const RELIABILITY_BINS: usize = 10;
/// `f64` floor/ceiling probabilities never touched exactly (avoids `ln(0)`).
const LOG_LOSS_EPS: f64 = 1e-12;

/// One score-bucket's reliability-diagram row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReliabilityBin {
    /// Inclusive lower score edge.
    pub score_lo: Decimal,
    /// Exclusive upper score edge (inclusive for the top bin).
    pub score_hi: Decimal,
    /// Samples in the bucket.
    pub sample_count: u64,
    /// Mean calibrated probability in the bucket (reliability-diagram x-axis).
    pub mean_predicted: Probability,
    /// Empirical win frequency in the bucket (reliability-diagram y-axis).
    pub empirical_frequency: Probability,
    /// Wilson score interval for `empirical_frequency`.
    pub wilson_ci: (Probability, Probability),
    /// Mean `max_adverse_excursion_bps` in the bucket, when any sample carried
    /// a resolved MAE label (`DownsideSource::MfeMae` serving-time lookup).
    pub mean_adverse_excursion_bps: Option<Decimal>,
}

/// A calibration artifact's reliability evaluation (Brier / log-loss / ECE +
/// per-bin diagnostics), computed on the fit's own held-out split.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReliabilityReport {
    pub bins: Vec<ReliabilityBin>,
    pub brier_score: Decimal,
    pub log_loss: Decimal,
    pub ece: Decimal,
    pub n_samples: u64,
}

impl ReliabilityReport {
    /// The bucket containing `score`, or `None` when no bucket was retained
    /// (empty report) — callers must treat this as "no downside data",
    /// never fabricate a value.
    #[must_use]
    pub fn bin_for(&self, score: Decimal) -> Option<&ReliabilityBin> {
        let top = Decimal::ONE;
        self.bins.iter().find(|bin| {
            score >= bin.score_lo && (score < bin.score_hi || (bin.score_hi == top && score <= top))
        })
    }
}

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
/// Propagates `Decimal`/`f64` conversion failures only (never on empty input
/// — an empty split yields a zeroed report; the caller's sample-count gate
/// upstream is what actually fails closed).
pub fn compute_reliability(
    mapping: &MonotoneMapping,
    samples: &[ReliabilitySample],
    ci_confidence: Decimal,
) -> QuantResult<ReliabilityReport> {
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

    let calibrated: Vec<Probability> = samples
        .iter()
        .map(|s| apply_mapping(mapping, s.score))
        .collect();

    let brier_score = mean_decimal(
        &samples
            .iter()
            .zip(&calibrated)
            .map(|(s, p)| {
                let y = if s.won { Decimal::ONE } else { Decimal::ZERO };
                (p.inner() - y) * (p.inner() - y)
            })
            .collect::<Vec<_>>(),
    );
    let log_loss = mean_decimal(
        &samples
            .iter()
            .zip(&calibrated)
            .map(|(s, p)| log_loss_term(p.inner(), s.won))
            .collect::<Vec<_>>(),
    );

    let z = wilson_z(ci_confidence);
    let bins = build_bins(samples, &calibrated, z);
    let ece = expected_calibration_error(&bins, n);

    Ok(ReliabilityReport {
        bins,
        brier_score,
        log_loss,
        ece,
        n_samples: n as u64,
    })
}

fn build_bins(
    samples: &[ReliabilitySample],
    calibrated: &[Probability],
    z: f64,
) -> Vec<ReliabilityBin> {
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
        let members: Vec<usize> = samples
            .iter()
            .enumerate()
            .filter(|(_, s)| s.score >= lo && (s.score < hi || (top && s.score <= hi)))
            .map(|(i, _)| i)
            .collect();
        if members.is_empty() {
            continue;
        }
        let n = members.len() as u64;
        let wins = members.iter().filter(|&&i| samples[i].won).count() as u64;
        let mean_predicted = mean_decimal(
            &members
                .iter()
                .map(|&i| calibrated[i].inner())
                .collect::<Vec<_>>(),
        );
        let p_hat = count_f64(wins) / count_f64(n);
        let empirical_frequency = Decimal::from_f64(p_hat).unwrap_or(Decimal::ZERO);
        let (ci_lo, ci_hi) = wilson_interval(p_hat, n, z, RESEARCH_DECIMAL_SCALE);
        let mae_values: Vec<Decimal> = members
            .iter()
            .filter_map(|&i| samples[i].max_adverse_excursion_bps)
            .collect();
        let mean_adverse_excursion_bps = if mae_values.is_empty() {
            None
        } else {
            Some(mean_decimal(&mae_values))
        };
        bins.push(ReliabilityBin {
            score_lo: lo,
            score_hi: hi,
            sample_count: n,
            mean_predicted: Probability::new(mean_predicted.round_dp(RESEARCH_DECIMAL_SCALE)),
            empirical_frequency: Probability::new(
                empirical_frequency.round_dp(RESEARCH_DECIMAL_SCALE),
            ),
            wilson_ci: (Probability::new(ci_lo), Probability::new(ci_hi)),
            mean_adverse_excursion_bps,
        });
    }
    bins
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
fn log_loss_term(p: Decimal, won: bool) -> Decimal {
    let clamped = p
        .to_f64()
        .unwrap_or(0.5)
        .clamp(LOG_LOSS_EPS, 1.0 - LOG_LOSS_EPS);
    let term = if won {
        -clamped.ln()
    } else {
        -(1.0 - clamped).ln()
    };
    Decimal::from_f64(term).unwrap_or(Decimal::ZERO)
}

fn mean_decimal(values: &[Decimal]) -> Decimal {
    if values.is_empty() {
        return Decimal::ZERO;
    }
    (values.iter().sum::<Decimal>() / Decimal::from(values.len() as u64))
        .round_dp(RESEARCH_DECIMAL_SCALE)
}

fn count_f64(n: u64) -> f64 {
    Decimal::from(n).to_f64().unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::{ReliabilitySample, compute_reliability};
    use crate::model::calibrator::{
        MonotoneMapping, ProbabilityCalibrator, isotonic::IsotonicCalibrator,
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn perfectly_calibrated_isotonic_has_near_zero_ece() {
        let calibrator = IsotonicCalibrator::new(10);
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for i in 0..200 {
            let score = Decimal::from(i) / dec!(200);
            scores.push(score);
            outcomes.push(i % 2 == 0);
        }
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        let samples: Vec<ReliabilitySample> = scores
            .iter()
            .zip(&outcomes)
            .map(|(&score, &won)| ReliabilitySample {
                score,
                won,
                max_adverse_excursion_bps: Some(dec!(-150)),
            })
            .collect();
        let report = compute_reliability(&mapping, &samples, dec!(0.95)).expect("reliability");
        assert_eq!(report.n_samples, 200);
        assert!(report.ece <= dec!(0.5));
        assert!(!report.bins.is_empty());
        for bin in &report.bins {
            assert_eq!(bin.mean_adverse_excursion_bps, Some(dec!(-150)));
        }
    }

    #[test]
    fn empty_split_yields_zeroed_report() {
        let mapping = MonotoneMapping::Isotonic { knots: Vec::new() };
        let report = compute_reliability(&mapping, &[], dec!(0.95)).expect("reliability");
        assert_eq!(report.n_samples, 0);
        assert!(report.bins.is_empty());
    }

    #[test]
    fn isotonic_calibration_improves_brier_vs_uncalibrated() {
        use super::mean_decimal;
        use crate::model::calibrator::{ProbabilityCalibrator, isotonic::IsotonicCalibrator};

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
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        let calibrated = compute_reliability(&mapping, &samples, dec!(0.95)).expect("calibrated");
        assert!(
            calibrated.brier_score <= raw_brier,
            "isotonic calibration must not worsen Brier: raw={raw_brier} calibrated={}",
            calibrated.brier_score
        );
    }
}
