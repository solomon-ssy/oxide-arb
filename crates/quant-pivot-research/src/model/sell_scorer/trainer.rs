//! [`SellScorerTrainer`]: fits a Sell-side hold-vs-exit scorer (Phase 06.1).
//!
//! Reuses the deterministic rank-IC simplex fit shared with the Buy-side
//! weighted trainer.
//!
//! The ranking formula is identical (`net = Σ weightᵢ · signedᵢ`); only the
//! supervised label (`hold_vs_exit_alpha_bps`) and the artifact body differ.
//! Determinism is a money-critical invariant: the same `(examples, label, seed,
//! output_spec, header)` yields a byte-identical artifact hash.

use quant_pivot_error::QuantResult;
use quant_pivot_models::types::ContentHash;
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    features::FeatureName,
    model::{
        artifact::{
            FactorWeight, ModelArtifact, ModelArtifactHeader, SellScorerArtifact,
            SellScorerOutputSpec,
        },
        trainer::{
            LabelSelector, Regularization, TrainedModelArtifact, ValidationSpec,
            fit_simplex_weights, signed_contribution,
        },
    },
    training::TrainingExample,
};

/// Lowest / highest exit-alpha scale (bps at `net = 1`) calibration will emit,
/// so a degenerate fit cannot produce a zero or absurd alpha scale.
const MIN_ALPHA_SCALE_BPS: i64 = 1;
const MAX_ALPHA_SCALE_BPS: i64 = 10_000;

/// Candidate logistic gains searched during `p_exit_better` calibration.
const GAIN_CANDIDATES: [f64; 9] = [0.5, 1.0, 2.0, 3.0, 4.0, 6.0, 8.0, 10.0, 12.0];

/// Request to train a Sell-side hold-vs-exit scorer from frozen examples.
///
/// Mirrors [`TrainModelRequest`] but carries the Sell governance (`output_spec`
/// + `label_schema_hash`) instead of the Buy multipliers / return model.
#[derive(Debug, Clone)]
pub struct TrainSellScorerRequest {
    /// Frozen, point-in-time exit-decision training examples.
    pub examples: Vec<crate::training::TrainingExample>,
    /// Supervised target label (`hold_vs_exit_alpha_bps`).
    pub label: LabelSelector,
    /// Initial weights / candidate factor set (market + position-state).
    pub seed_weights: Vec<FactorWeight>,
    /// Weight regularization.
    pub regularization: Regularization,
    /// Rolling validation split.
    pub validation: ValidationSpec,
    /// Frozen artifact header (`model_family = HoldVsExitWeighted`).
    pub header: ModelArtifactHeader,
    /// Model-intrinsic hold-vs-exit horizon, in seconds.
    pub prediction_horizon_secs: u64,
    /// Governed net → business-output mapping.
    pub output_spec: SellScorerOutputSpec,
    /// Hold-vs-exit label-schema hash the scorer is trained against.
    pub label_schema_hash: ContentHash,
    /// Features the scorer requires (eligibility / audit).
    pub required_features: Vec<FeatureName>,
}

/// The Sell-side hold-vs-exit trainer.
#[derive(Debug, Clone, Default)]
pub struct SellScorerTrainer;

impl SellScorerTrainer {
    /// Construct the trainer.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    /// Train a Sell scorer artifact (pure, deterministic).
    ///
    /// # Errors
    ///
    /// Propagates the shared simplex fit (empty seed set, no resolved samples,
    /// too few folds) and the artifact structural validation.
    pub fn train_sell_scorer(
        &self,
        request: &TrainSellScorerRequest,
    ) -> QuantResult<TrainedModelArtifact> {
        let fit = fit_simplex_weights(
            &request.examples,
            &request.label,
            &request.seed_weights,
            request.regularization,
            request.validation,
        )?;

        // Calibrate the net→business-output mapping on the training labels so
        // `exit_alpha_bps` / `p_exit_better` reflect the realized hold-vs-exit
        // alpha rather than hand-authored constants. The governed `output_spec`
        // is the fail-closed fallback (degenerate / single-class calibration).
        let output_spec = calibrate_output_spec(
            &request.examples,
            &request.label,
            &fit.factor_weights,
            &request.output_spec,
        );

        let artifact = SellScorerArtifact {
            header: request.header.clone(),
            weights: fit.factor_weights,
            prediction_horizon_secs: request.prediction_horizon_secs,
            output_spec,
            label_schema_hash: request.label_schema_hash.clone(),
            required_features: request.required_features.clone(),
            objective_report: Some(fit.objective_report.clone()),
        };
        artifact.validate()?;
        let model_artifact = ModelArtifact::SellScorer(Box::new(artifact));
        let artifact_hash = model_artifact.content_hash()?;

        Ok(TrainedModelArtifact {
            artifact: model_artifact,
            artifact_hash,
            in_sample_metrics: fit.objective_report,
            validation_metrics: fit.validation,
        })
    }
}

/// Calibrate the `net → (exit_alpha_bps, p_exit_better)` mapping on the training
/// labels: `max_exit_alpha_bps` is the origin OLS slope of realized exit alpha on
/// the fitted net, and `p_exit_gain` is the logistic gain maximizing the Bernoulli
/// likelihood of `label > 0`. `default_sell_pct` stays the governed policy floor.
/// Falls back to `fallback` whenever calibration is degenerate.
fn calibrate_output_spec(
    examples: &[TrainingExample],
    label: &LabelSelector,
    weights: &[FactorWeight],
    fallback: &SellScorerOutputSpec,
) -> SellScorerOutputSpec {
    let pairs = net_label_pairs(examples, label, weights);
    if pairs.len() < 2 {
        return fallback.clone();
    }
    SellScorerOutputSpec {
        max_exit_alpha_bps: calibrate_alpha_scale(&pairs).unwrap_or(fallback.max_exit_alpha_bps),
        p_exit_gain: calibrate_logistic_gain(&pairs).unwrap_or(fallback.p_exit_gain),
        default_sell_pct: fallback.default_sell_pct,
    }
}

/// `(net, label_bps)` over every resolved example, where `net` uses the fitted
/// weights and the same signed contributions the runtime scores with.
fn net_label_pairs(
    examples: &[TrainingExample],
    label: &LabelSelector,
    weights: &[FactorWeight],
) -> Vec<(Decimal, Decimal)> {
    let mut pairs = Vec::new();
    for example in examples {
        let Some(label_value) = example
            .labels
            .iter()
            .find(|row| (&row.label_name, row.horizon_secs) == (&label.name, label.horizon_secs))
            .map(|row| row.value)
        else {
            continue;
        };
        let net: Decimal = weights
            .iter()
            .map(|w| w.weight * signed_contribution(example, &w.factor))
            .sum();
        pairs.push((net, label_value));
    }
    pairs
}

/// Origin OLS slope `Σ(net·label) / Σ(net²)` of realized alpha on net, clamped to
/// a sane positive band. `None` when net has no variance or the slope is not a
/// usable positive scale (the quality gate rejects such a model on rank IC).
fn calibrate_alpha_scale(pairs: &[(Decimal, Decimal)]) -> Option<Decimal> {
    let mut numerator = Decimal::ZERO;
    let mut denominator = Decimal::ZERO;
    for (net, label) in pairs {
        numerator += *net * *label;
        denominator += *net * *net;
    }
    if denominator <= Decimal::ZERO {
        return None;
    }
    let slope = numerator / denominator;
    if slope <= Decimal::ZERO {
        return None;
    }
    Some(slope.clamp(
        Decimal::from(MIN_ALPHA_SCALE_BPS),
        Decimal::from(MAX_ALPHA_SCALE_BPS),
    ))
}

/// Logistic gain (`net → P(exit_better)`) maximizing the Bernoulli likelihood of
/// `label > 0` over a fixed candidate grid. `None` when a class is absent or nets
/// are degenerate (the governed default gain then applies).
fn calibrate_logistic_gain(pairs: &[(Decimal, Decimal)]) -> Option<Decimal> {
    let observations: Vec<(f64, f64)> = pairs
        .iter()
        .filter_map(|(net, label)| {
            let net = net.to_f64()?;
            let y = if *label > Decimal::ZERO { 1.0 } else { 0.0 };
            Some((net, y))
        })
        .collect();
    if observations.len() < 2 {
        return None;
    }
    let positives = observations.iter().filter(|(_, y)| *y > 0.5).count();
    if positives == 0 || positives == observations.len() {
        return None;
    }
    let mut best_gain = None;
    let mut best_ll = f64::NEG_INFINITY;
    for &gain in &GAIN_CANDIDATES {
        let mut log_likelihood = 0.0;
        for (net, y) in &observations {
            let p = (1.0 / (1.0 + (-(gain * net)).exp())).clamp(1e-6, 1.0 - 1e-6);
            log_likelihood += (1.0 - y).mul_add((1.0 - p).ln(), y * p.ln());
        }
        if log_likelihood > best_ll {
            best_ll = log_likelihood;
            best_gain = Some(gain);
        }
    }
    best_gain.and_then(Decimal::from_f64)
}

#[cfg(test)]
mod tests {
    use super::{GAIN_CANDIDATES, calibrate_alpha_scale, calibrate_logistic_gain};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    #[test]
    fn alpha_scale_is_the_origin_ols_slope() {
        // label = 200 · net exactly ⇒ slope 200.
        let pairs = vec![
            (dec!(0.10), dec!(20)),
            (dec!(0.50), dec!(100)),
            (dec!(0.80), dec!(160)),
        ];
        assert_eq!(calibrate_alpha_scale(&pairs), Some(dec!(200)));
    }

    #[test]
    fn alpha_scale_none_on_degenerate_or_negative() {
        // No net variance ⇒ None (denominator zero).
        let flat = vec![(dec!(0), dec!(50)), (dec!(0), dec!(-30))];
        assert_eq!(calibrate_alpha_scale(&flat), None);
        // Net anti-correlated with alpha ⇒ negative slope ⇒ None (fail to fallback).
        let inverted = vec![(dec!(0.5), dec!(-100)), (dec!(0.8), dec!(-160))];
        assert_eq!(calibrate_alpha_scale(&inverted), None);
    }

    #[test]
    fn alpha_scale_clamps_into_band() {
        // Huge slope clamps to the 10_000 bps ceiling.
        let pairs = vec![(dec!(0.01), dec!(1000)), (dec!(0.02), dec!(2000))];
        assert_eq!(calibrate_alpha_scale(&pairs), Some(Decimal::from(10_000)));
    }

    #[test]
    fn logistic_gain_prefers_separating_gain() {
        // Positive nets ⇒ exit-better; negative nets ⇒ hold. A higher gain
        // maximizes the Bernoulli likelihood of the clean separation.
        let pairs = vec![
            (dec!(0.9), dec!(50)),
            (dec!(0.8), dec!(40)),
            (dec!(-0.9), dec!(-50)),
            (dec!(-0.8), dec!(-40)),
        ];
        let gain = calibrate_logistic_gain(&pairs).expect("gain");
        let max = Decimal::from_f64_retain(*GAIN_CANDIDATES.last().unwrap()).unwrap();
        assert_eq!(
            gain, max,
            "cleanly separable labels pick the strongest gain"
        );
    }

    #[test]
    fn logistic_gain_none_on_single_class() {
        // Every label positive ⇒ no class contrast ⇒ None (governed default used).
        let pairs = vec![(dec!(0.5), dec!(10)), (dec!(-0.5), dec!(20))];
        assert_eq!(calibrate_logistic_gain(&pairs), None);
    }
}
