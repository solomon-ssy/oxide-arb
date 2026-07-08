//! Platt scaling: fit `P(win) = 1 / (1 + exp(A*score + B))`.
//!
//! Uses the Lin–Lin–Weng (2007) Newton's-method-with-backtracking algorithm
//! ("A Note on Platt's Probabilistic Outputs for Support Vector Machines",
//! Machine Learning 68(3)) — the standard, numerically robust fit `libsvm`
//! ships, including Platt's original label-smoothing prior (`t+`/`t-`) that
//! guards against overfitting the sigmoid to a small calibration split.
//!
//! Self-contained (no `argmin`/optimizer crate): calibration is a fail-closed,
//! money-critical production path and must not depend on the optional
//! `optimize` feature.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{enums::quant::CalibrationMethod, types::Probability};
use rust_decimal::{Decimal, prelude::FromPrimitive, prelude::ToPrimitive};

use super::{MonotoneMapping, ProbabilityCalibrator};

/// Minimum paired samples for a numerically meaningful 2-parameter fit.
const MIN_SAMPLES: usize = 10;
const MAX_ITERATIONS: usize = 100;
const MIN_STEP: f64 = 1e-10;
const SIGMA: f64 = 1e-12;
const STOPPING_GRAD: f64 = 1e-5;
/// Backtracking halving steps: `2^-step_idx` reaches `MIN_STEP` well before 40.
const MAX_BACKTRACK_STEPS: u32 = 40;

/// Platt's Newton loop counts samples as `f64`; cap at `u32::MAX` to avoid
/// lossy `usize` casts on wide platforms.
fn sample_count_as_f64(len: usize) -> f64 {
    f64::from(u32::try_from(len).unwrap_or(u32::MAX))
}

/// Platt's label-smoothing prior for the positive class count.
fn prior_positive_target(won_count: f64) -> f64 {
    (won_count + 1.0) / (won_count + 2.0)
}

/// Platt's label-smoothing prior for the negative class count.
fn prior_negative_target(lost_count: f64) -> f64 {
    1.0 / (lost_count + 2.0)
}

/// Two-parameter Platt-scaling probability calibrator.
pub struct PlattCalibrator;

impl ProbabilityCalibrator for PlattCalibrator {
    fn method(&self) -> CalibrationMethod {
        CalibrationMethod::Platt
    }

    fn fit(&self, scores: &[Decimal], outcomes: &[bool]) -> QuantResult<MonotoneMapping> {
        if scores.len() != outcomes.len() || scores.len() < MIN_SAMPLES {
            return Err(QuantError::from(ResearchError::DatasetBuild {
                detail: format!(
                    "platt calibration requires >= {MIN_SAMPLES} paired samples, got {}",
                    scores.len()
                ),
            }));
        }
        let scores_f64: Vec<f64> = scores.iter().map(|s| s.to_f64().unwrap_or(0.0)).collect();
        let (param_a, param_b) = fit_platt(&scores_f64, outcomes)?;
        Ok(MonotoneMapping::Platt {
            a: Decimal::from_f64(param_a).unwrap_or(Decimal::ZERO),
            b: Decimal::from_f64(param_b).unwrap_or(Decimal::ZERO),
        })
    }

    fn calibrate(&self, mapping: &MonotoneMapping, score: Decimal) -> Probability {
        super::apply_mapping(mapping, score)
    }
}

/// Evaluate the calibrated sigmoid `1 / (1 + exp(a*score + b))` at `score`,
/// numerically stable for large `|a*score+b|`.
#[must_use]
pub fn sigmoid(a: Decimal, b: Decimal, score: Decimal) -> Decimal {
    let linear_term = (a.to_f64().unwrap_or(0.0))
        .mul_add(score.to_f64().unwrap_or(0.0), b.to_f64().unwrap_or(0.0));
    let prob = if linear_term >= 0.0 {
        (-linear_term).exp() / (1.0 + (-linear_term).exp())
    } else {
        1.0 / (1.0 + linear_term.exp())
    };
    Decimal::from_f64(prob).unwrap_or(Decimal::ZERO)
}

/// Stable sigmoid probability and its complement at `linear_term = a*score + b`.
fn sigmoid_pair(linear_term: f64) -> (f64, f64) {
    if linear_term >= 0.0 {
        let exp_neg = (-linear_term).exp();
        (exp_neg / (1.0 + exp_neg), 1.0 / (1.0 + exp_neg))
    } else {
        let exp_pos = linear_term.exp();
        (1.0 / (1.0 + exp_pos), exp_pos / (1.0 + exp_pos))
    }
}

/// Lin–Lin–Weng Algorithm 1: Newton's method with backtracking line search.
fn fit_platt(scores: &[f64], outcomes: &[bool]) -> QuantResult<(f64, f64)> {
    let sample_len = scores.len();
    let won_count = outcomes.iter().filter(|&&won| won).count();
    let won_count_f64 = f64::from(u32::try_from(won_count).unwrap_or(u32::MAX));
    let total_count_f64 = sample_count_as_f64(sample_len);
    let lost_count_f64 = total_count_f64 - won_count_f64;
    if lost_count_f64 <= 0.0 || won_count_f64 <= 0.0 {
        return Err(QuantError::from(ResearchError::DatasetBuild {
            detail: "platt calibration requires both won and lost outcomes in the split".to_owned(),
        }));
    }

    let targets: Vec<f64> = outcomes
        .iter()
        .map(|&won| {
            if won {
                prior_positive_target(won_count_f64)
            } else {
                prior_negative_target(lost_count_f64)
            }
        })
        .collect();

    let mut param_a = 0.0_f64;
    let mut param_b = ((lost_count_f64 + 1.0) / (won_count_f64 + 1.0)).ln();
    let mut neg_ll = neg_log_likelihood(scores, &targets, param_a, param_b);

    for _ in 0..MAX_ITERATIONS {
        let mut hess_aa = SIGMA;
        let mut hess_bb = SIGMA;
        let mut hess_cross = 0.0_f64;
        let mut grad_a = 0.0_f64;
        let mut grad_b = 0.0_f64;
        for idx in 0..sample_len {
            let linear_term = param_a.mul_add(scores[idx], param_b);
            let (sigmoid_prob, sigmoid_complement) = sigmoid_pair(linear_term);
            let hess_term = sigmoid_prob * sigmoid_complement;
            hess_aa = (scores[idx] * scores[idx]).mul_add(hess_term, hess_aa);
            hess_bb += hess_term;
            hess_cross = scores[idx].mul_add(hess_term, hess_cross);
            let likelihood_grad = targets[idx] - sigmoid_prob;
            grad_a = scores[idx].mul_add(likelihood_grad, grad_a);
            grad_b += likelihood_grad;
        }
        if grad_a.abs() < STOPPING_GRAD && grad_b.abs() < STOPPING_GRAD {
            break;
        }
        let hess_det = hess_aa.mul_add(hess_bb, -(hess_cross * hess_cross));
        if hess_det.abs() < f64::EPSILON {
            break;
        }
        let step_a = -(hess_bb.mul_add(grad_a, -(hess_cross * grad_b))) / hess_det;
        let step_b = -((-hess_cross).mul_add(grad_a, hess_aa * grad_b)) / hess_det;
        let grad_dot_step = grad_a.mul_add(step_a, grad_b * step_b);

        let mut updated = false;
        for step_idx in 0..MAX_BACKTRACK_STEPS {
            let step = 2.0_f64.powi(-i32::try_from(step_idx).unwrap_or(i32::MAX));
            if step < MIN_STEP {
                break;
            }
            let trial_a = step.mul_add(step_a, param_a);
            let trial_b = step.mul_add(step_b, param_b);
            let trial_neg_ll = neg_log_likelihood(scores, &targets, trial_a, trial_b);
            if trial_neg_ll < 0.0001_f64.mul_add(step * grad_dot_step, neg_ll) {
                param_a = trial_a;
                param_b = trial_b;
                neg_ll = trial_neg_ll;
                updated = true;
                break;
            }
        }
        if !updated {
            break;
        }
    }
    Ok((param_a, param_b))
}

/// Negative log-likelihood at `(param_a, param_b)`, numerically stable for either
/// sign of `linear_term` (Lin–Lin–Weng eq. 4).
fn neg_log_likelihood(scores: &[f64], targets: &[f64], param_a: f64, param_b: f64) -> f64 {
    let mut total = 0.0_f64;
    for idx in 0..scores.len() {
        let linear_term = param_a.mul_add(scores[idx], param_b);
        total += if linear_term >= 0.0 {
            targets[idx].mul_add(linear_term, (-linear_term).exp().ln_1p())
        } else {
            (targets[idx] - 1.0).mul_add(linear_term, linear_term.exp().ln_1p())
        };
    }
    total
}

#[cfg(test)]
mod tests {
    use super::{PlattCalibrator, fit_platt};
    use crate::model::calibrator::{MonotoneMapping, ProbabilityCalibrator};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn fit_fails_closed_without_both_classes() {
        let scores = vec![dec!(0.1); 20];
        let outcomes = vec![true; 20];
        assert!(PlattCalibrator.fit(&scores, &outcomes).is_err());
    }

    #[test]
    fn fit_recovers_monotone_increasing_sigmoid() {
        // Perfectly separable-ish data: high score -> win.
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for i in 0..200_i32 {
            let score = f64::from(i - 100) / 50.0;
            scores.push(score);
            outcomes.push(score > 0.0);
        }
        let (param_a, param_b) = fit_platt(&scores, &outcomes).expect("fit");
        // P(win) increasing in score requires param_a < 0 (since P = 1/(1+exp(a*f+b))).
        // The magnitude check (not just the sign) guards against a degenerate fit that
        // "converges" to ~0 after a single corrupted Newton step — the exact failure
        // mode of a `mul_add` accumulator-order regression: the sign check alone
        // passed for `param_a ≈ -1e-7` while the true fit on this data is `≈ -6.5`.
        assert!(
            param_a < -1.0,
            "param_a={param_a} param_b={param_b} (near-zero param_a means the sigmoid \
             barely depends on score — a degenerate, uninformative calibration)"
        );
    }

    #[test]
    fn platt_calibration_matches_known_closed_form() {
        // Golden (A, B) independently computed in Python from the same
        // Lin-Lin-Weng (2007) Newton-with-backtracking algorithm (correct
        // `Σ f_i·d_i`-style accumulation, not the buggy `mul_add` order this
        // test guards against regressing to) on this exact fixed dataset.
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for i in 0..40_i32 {
            let score = f64::from(i - 20) / 10.0;
            scores.push(score);
            let base = score > 0.0;
            outcomes.push(if i % 6 == 0 { !base } else { base });
        }
        let (param_a, param_b) = fit_platt(&scores, &outcomes).expect("fit");
        let golden_a = -1.026_860_098_423_914;
        let golden_b = -0.051_343_004_921_195_81;
        assert!(
            (param_a - golden_a).abs() < 1e-6,
            "param_a={param_a} golden={golden_a}"
        );
        assert!(
            (param_b - golden_b).abs() < 1e-6,
            "param_b={param_b} golden={golden_b}"
        );
    }

    #[test]
    fn calibrate_is_monotone_in_score() {
        let calibrator = PlattCalibrator;
        let mut scores = Vec::new();
        let mut outcomes = Vec::new();
        for i in 0..100_i32 {
            let score = Decimal::from(i) / dec!(100);
            scores.push(score);
            outcomes.push(i >= 50);
        }
        let mapping = calibrator.fit(&scores, &outcomes).expect("fit");
        let MonotoneMapping::Platt { .. } = &mapping else {
            panic!("expected platt mapping");
        };
        let low = calibrator.calibrate(&mapping, dec!(0.05));
        let mid = calibrator.calibrate(&mapping, dec!(0.5));
        let high = calibrator.calibrate(&mapping, dec!(0.95));
        assert!(low.inner() < mid.inner());
        assert!(mid.inner() < high.inner());
    }
}
