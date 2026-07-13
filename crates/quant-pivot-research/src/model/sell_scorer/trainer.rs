//! [`SellScorerTrainer`]: fits a Sell-side hold-vs-exit scorer (Phase 06.1).
//!
//! Reuses the deterministic LTR simplex fit shared with the Buy-side
//! weighted trainer.
//!
//! The ranking formula is identical (`net = Σ weightᵢ · signedᵢ`); only the
//! supervised label (`hold_vs_exit_alpha_bps`) and the artifact body differ.
//! Determinism is a money-critical invariant: the same `(examples, label, seed,
//! output_spec, header)` yields a byte-identical artifact hash.

use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::types::{ContentHash, ModelInputContract};
use rust_decimal::{
    Decimal,
    prelude::{FromPrimitive, ToPrimitive},
};

use crate::{
    model::{
        TrainingObjectiveSpec,
        artifact::{
            FactorWeight, ModelArtifact, ModelArtifactHeader, SellScorerArtifact,
            SellScorerOutputSpec, model_input_contract_hash,
        },
        trainer::{
            LabelSelector, TrainedModelArtifact, ValidationSpec, fit_simplex_weights,
            signed_contribution, weighted_training_input_hash,
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
    pub examples: Vec<TrainingExample>,
    /// Supervised target label (`hold_vs_exit_alpha_bps`).
    pub label: LabelSelector,
    /// Initial weights / candidate factor set (market + position-state).
    pub seed_weights: Vec<FactorWeight>,
    /// Governed training objective snapshot.
    pub objective: TrainingObjectiveSpec,
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
    /// Semantic hash of the exact frozen training dataset/partition.
    pub training_dataset_hash: ContentHash,
    /// Exact ordered raw-input contract frozen by the owning model spec.
    pub input_contract: ModelInputContract,
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
            &request.objective,
            request.validation,
            None,
        )?;

        // Calibrate the net→business-output mapping on the training labels so
        // `exit_alpha_bps` / `p_exit_better` reflect the realized hold-vs-exit
        // alpha rather than hand-authored constants. Degenerate or single-class
        // calibration fails training; hand-authored scales must never
        // masquerade as a fitted business-output mapping.
        let output_spec = calibrate_output_spec(
            &request.examples,
            &request.label,
            &fit.factor_weights,
            &request.output_spec,
        )?;
        let factors = fit
            .factor_weights
            .iter()
            .map(|weight| weight.factor.clone())
            .collect::<Vec<_>>();
        let training_input_hash = weighted_training_input_hash(
            &request.examples,
            &request.label,
            &factors,
            &fit.frozen_reference_quantiles,
            None,
        )?;
        let input_contract_hash = model_input_contract_hash(&request.input_contract)?;

        let artifact = SellScorerArtifact {
            header: request.header.clone(),
            weights: fit.factor_weights,
            prediction_horizon_secs: request.prediction_horizon_secs,
            output_spec,
            label_schema_hash: request.label_schema_hash.clone(),
            training_dataset_hash: request.training_dataset_hash.clone(),
            training_input_hash,
            input_contract: request.input_contract.clone(),
            input_contract_hash,
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
/// Degenerate calibration is a typed training failure.
fn calibrate_output_spec(
    examples: &[TrainingExample],
    label: &LabelSelector,
    weights: &[FactorWeight],
    governed: &SellScorerOutputSpec,
) -> QuantResult<SellScorerOutputSpec> {
    let pairs = net_label_pairs(examples, label, weights);
    if pairs.len() < 2 {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "sell scorer calibration requires at least two resolved training labels"
                .to_owned(),
        }
        .into());
    }
    Ok(SellScorerOutputSpec {
        max_exit_alpha_bps: calibrate_alpha_scale(&pairs)?,
        p_exit_gain: calibrate_logistic_gain(&pairs)?,
        default_sell_pct: governed.default_sell_pct,
    })
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
/// a sane positive band. Missing variance or a non-positive slope fails the fit.
fn calibrate_alpha_scale(pairs: &[(Decimal, Decimal)]) -> QuantResult<Decimal> {
    let mut numerator = Decimal::ZERO;
    let mut denominator = Decimal::ZERO;
    for (net, label) in pairs {
        numerator += *net * *label;
        denominator += *net * *net;
    }
    if denominator <= Decimal::ZERO {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "sell scorer alpha calibration has zero net-score variance".to_owned(),
        }
        .into());
    }
    let slope = numerator / denominator;
    if slope <= Decimal::ZERO {
        return Err(ResearchError::InvalidModelArtifact {
            detail: format!("sell scorer alpha calibration slope must be positive, got {slope}"),
        }
        .into());
    }
    Ok(slope.clamp(
        Decimal::from(MIN_ALPHA_SCALE_BPS),
        Decimal::from(MAX_ALPHA_SCALE_BPS),
    ))
}

/// Logistic gain (`net → P(exit_better)`) maximizing the Bernoulli likelihood of
/// `label > 0` over a fixed candidate grid. A missing class or degenerate input
/// fails the fit. Decimal conversion failures are never discarded.
fn calibrate_logistic_gain(pairs: &[(Decimal, Decimal)]) -> QuantResult<Decimal> {
    let observations: Vec<(f64, f64)> = pairs
        .iter()
        .map(|(net, label)| {
            let net = net
                .to_f64()
                .filter(|value| value.is_finite())
                .ok_or_else(|| ResearchError::InvalidModelArtifact {
                    detail: format!(
                        "sell scorer calibration net score `{net}` is not a finite f64"
                    ),
                })?;
            let y = if *label > Decimal::ZERO { 1.0 } else { 0.0 };
            Ok((net, y))
        })
        .collect::<Result<_, ResearchError>>()?;
    if observations.len() < 2 {
        return Err(ResearchError::InvalidModelArtifact {
            detail: "sell scorer probability calibration requires at least two observations"
                .to_owned(),
        }
        .into());
    }
    let positives = observations.iter().filter(|(_, y)| *y > 0.5).count();
    if positives == 0 || positives == observations.len() {
        return Err(ResearchError::InvalidModelArtifact {
            detail:
                "sell scorer probability calibration requires both positive and non-positive labels"
                    .to_owned(),
        }
        .into());
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
    let best_gain = best_gain.ok_or_else(|| ResearchError::InvalidModelArtifact {
        detail: "sell scorer probability calibration produced no gain candidate".to_owned(),
    })?;
    Decimal::from_f64(best_gain).ok_or_else(|| {
        ResearchError::InvalidModelArtifact {
            detail: format!("sell scorer calibration gain `{best_gain}` is not a Decimal"),
        }
        .into()
    })
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
        assert_eq!(
            calibrate_alpha_scale(&pairs).expect("positive fitted slope"),
            dec!(200)
        );
    }

    #[test]
    fn alpha_scale_rejects_degenerate_or_negative() {
        // No net variance is a hard calibration failure.
        let flat = vec![(dec!(0), dec!(50)), (dec!(0), dec!(-30))];
        assert!(calibrate_alpha_scale(&flat).is_err());
        // Net anti-correlated with alpha is also rejected.
        let inverted = vec![(dec!(0.5), dec!(-100)), (dec!(0.8), dec!(-160))];
        assert!(calibrate_alpha_scale(&inverted).is_err());
    }

    #[test]
    fn alpha_scale_clamps_into_band() {
        // Huge slope clamps to the 10_000 bps ceiling.
        let pairs = vec![(dec!(0.01), dec!(1000)), (dec!(0.02), dec!(2000))];
        assert_eq!(
            calibrate_alpha_scale(&pairs).expect("clamped fitted slope"),
            Decimal::from(10_000)
        );
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
    fn logistic_gain_rejects_single_class() {
        // Every label positive means there is no probability calibration signal.
        let pairs = vec![(dec!(0.5), dec!(10)), (dec!(-0.5), dec!(20))];
        assert!(calibrate_logistic_gain(&pairs).is_err());
    }
}
