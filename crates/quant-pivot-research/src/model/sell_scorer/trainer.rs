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

use crate::{
    features::FeatureName,
    model::{
        artifact::{
            FactorWeight, ModelArtifact, ModelArtifactHeader, SellScorerArtifact,
            SellScorerOutputSpec,
        },
        trainer::{
            LabelSelector, Regularization, TrainedModelArtifact, ValidationSpec,
            fit_simplex_weights,
        },
    },
};

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

        let artifact = SellScorerArtifact {
            header: request.header.clone(),
            weights: fit.factor_weights,
            prediction_horizon_secs: request.prediction_horizon_secs,
            output_spec: request.output_spec.clone(),
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
