//! [`WeightedFactorRuntime`]: the first-class, fully-explainable weighted factor
//! scorer.
//!
//! It consumes a [`FactorInferenceTable`] of already-normalized factor values and
//! produces ranked [`SignalCandidate`]s. The score separates a **ranking model**
//! from a **return/risk mapping**, both frozen in the artifact:
//!
//! ```text
//! signedᵢ        = dir_signᵢ · normalizedᵢ · confidenceᵢ        (∈ [-1, 1])
//! net            = Σ weightᵢ · signedᵢ                          (∈ [-1, 1])
//! outcome_side   = sign(net)  → Yes (>0) / No (<0)             (0 ⇒ no candidate)
//! conviction     = |net|
//! composite      = clamp₀₁(conviction · dq_mult · liq_mult · horizon_mult)
//! confidence     = clamp₀₁(weighted_mean(confidenceᵢ) · Π substitution_penalty)
//! (return,down)  = artifact.return_model.estimate(composite, confidence)
//! ```
//!
//! Weights are non-negative and sum to 1 (validated on construction); missing
//! factors carry `confidence = 0` so they contribute nothing yet remain auditable.
//! Cross-sectional `net` uses `ndarray` batch reduction; money fields remain
//! `Decimal` / newtypes after quantization.

use std::{cmp::Reverse, collections::BTreeMap, time::Instant};
mod batch;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        quant::{DataQualityStatus, ModelWeightSource, OutcomeSide},
    },
    runtime_config::FactorCrossSectionConfig,
    types::{
        ContentHash, ModelRunId, ModelVersionId, Price, Probability, SignalCandidateId, TokenId,
        Usd,
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    factors::{FactorName, FrozenReferenceQuantiles, NormalizedFactor},
    features::FeatureName,
    model::{
        artifact::WeightedFactorModelArtifact,
        calibrator::ResolvedCalibration,
        overlay::WeightOverlay,
        runtime::{
            FactorInferenceRow, FactorInferenceTable, MarketInferenceContext, ModelFamily,
            ModelInputAuditRow, ModelInputAuditState, ModelRuntimeInput, ModelRuntimeMetrics,
            ModelRuntimeOutput, QuantModelRuntime,
        },
        signal::{FactorContribution, ModelExplanation, SignalCandidate, SignalWarning},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

use batch::ScoringBatchLayout;

/// How many positive / negative contributions the explanation surfaces.
const EXPLANATION_TOP_K: usize = 3;

/// A governed weighted-factor scorer bound to one frozen artifact.
pub struct WeightedFactorRuntime {
    artifact: WeightedFactorModelArtifact,
    weights: BTreeMap<FactorName, Decimal>,
    batch_layout: ScoringBatchLayout,
    weight_source: ModelWeightSource,
    /// Resolved `ProbabilityCalibrator` data when `artifact.return_model` is
    /// `Calibrated` (Phase 11.3 §5) — bound once at load time by the factory,
    /// never re-fetched per candidate. `None` for `Heuristic`.
    calibration: Option<ResolvedCalibration>,
}

impl WeightedFactorRuntime {
    /// Build a runtime from a validated weighted artifact, optionally applying a
    /// non-persisted [`WeightOverlay`] over a non-published candidate / shadow
    /// version, and the resolved calibrator when the artifact's return model is
    /// `Calibrated` (the loader — `quant-pivot-core`'s
    /// `DefaultModelRuntimeFactory` — fails the load closed if it cannot
    /// resolve a `Calibrated` artifact's `calibrator_ref`, so this is `None`
    /// only for `Heuristic`).
    ///
    /// When `overlay` is `Some`, it **replaces** the artifact's weight table
    /// (fail-closed: it must cover exactly the artifact's factor set) and the
    /// runtime records [`ModelWeightSource::ConfigOverlay`]. The overlay never alters
    /// the artifact bytes or its content hash. When `None`, the frozen artifact
    /// weights are used ([`ModelWeightSource::Artifact`]).
    ///
    /// # Errors
    ///
    /// Propagates [`WeightedFactorModelArtifact::validate`] (unnormalized or
    /// negative weights, empty weight set) and overlay resolution (unknown or
    /// missing factor).
    pub fn new(
        artifact: WeightedFactorModelArtifact,
        overlay: Option<WeightOverlay>,
        calibration: Option<ResolvedCalibration>,
    ) -> QuantResult<Self> {
        artifact.validate()?;
        let artifact_weights = artifact.weight_index();
        let (weights, weight_source) = match overlay {
            Some(overlay) => (
                overlay.resolve_against(&artifact_weights)?,
                ModelWeightSource::ConfigOverlay,
            ),
            None => (artifact_weights, ModelWeightSource::Artifact),
        };
        let batch_layout = ScoringBatchLayout::from_weights(&weights)?;
        Ok(Self {
            artifact,
            weights,
            batch_layout,
            weight_source,
            calibration,
        })
    }

    /// Collect per-factor signed contributions for one row (explanations + confidence).
    fn row_contributions(
        row: &FactorInferenceRow,
        weights: &BTreeMap<FactorName, Decimal>,
    ) -> (Decimal, Decimal, Vec<FactorContribution>) {
        let mut confidence_mass = Decimal::ZERO;
        let mut confidence_weighted = Decimal::ZERO;
        let mut contributions: Vec<FactorContribution> = Vec::new();

        for factor in &row.factors {
            let Some(weight) = weights.get(&factor.name) else {
                continue;
            };
            if weight.is_zero() {
                continue;
            }
            let confidence = factor.confidence.inner();
            // Only a scored factor contributes to net / confidence mass; a
            // missing-input or indeterminate factor is surfaced in the breakdown
            // with a zero contribution (never a fabricated neutral score).
            let contribution = factor.normalized_score().map_or(Decimal::ZERO, |score| {
                let direction = Decimal::from(factor.direction.as_i8());
                let signed = direction * score.inner() * confidence;
                (*weight * signed).round_dp(RESEARCH_DECIMAL_SCALE)
            });
            if factor.is_scored() && confidence > Decimal::ZERO {
                confidence_mass += weight;
                confidence_weighted += *weight * confidence;
            }
            contributions.push(FactorContribution {
                definition_id: factor.definition_id.clone(),
                name: factor.name.clone(),
                family: factor.family,
                value_state: factor.value_state(),
                raw_value: factor.raw_value,
                normalized_score: factor.normalized_score(),
                normalization_source: factor.normalization_source(),
                indeterminate_reason: factor.indeterminate_reason(),
                weight: *weight,
                contribution,
                confidence: factor.confidence,
                direction: factor.direction,
                explanation: factor.explanation.headline.clone(),
                source_refs: factor
                    .input_feature_refs
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            });
        }

        (confidence_mass, confidence_weighted, contributions)
    }

    /// Score one market row from a precomputed directional net, or `None` when
    /// there is no directional signal or the chosen side cannot be priced.
    fn score_row_with_net(
        &self,
        row: &FactorInferenceRow,
        net: Decimal,
        model_run_id: &ModelRunId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<SignalCandidate>> {
        if net.is_zero() {
            return Ok(None);
        }

        let outcome_side = if net.is_sign_positive() {
            OutcomeSide::Yes
        } else {
            OutcomeSide::No
        };
        let Some((token_id, entry_price_ref)) = Self::resolve_entry(row, outcome_side) else {
            return Ok(None);
        };

        let (confidence_mass, confidence_weighted, contributions) =
            Self::row_contributions(row, &self.weights);

        let conviction = net.abs();
        let composite_score = self.apply_multipliers(conviction, &row.context);
        let confidence = self.apply_confidence(confidence_mass, confidence_weighted, &row.context);
        let data_quality_score = clamp_unit(
            self.artifact
                .multipliers
                .data_quality
                .multiplier_for(row.context.data_quality),
        );
        let liquidity_score = clamp_unit(
            self.artifact
                .multipliers
                .liquidity
                .multiplier_for(row.context.liquidity_usd.map(Usd::inner)),
        );

        let estimate = self.artifact.return_model.estimate(
            composite_score.inner(),
            confidence.inner(),
            entry_price_ref,
            self.calibration.as_ref(),
        )?;

        let (top_positive, top_negative) = split_contributions(&contributions);
        let provenance = if estimate.calibrated {
            "calibrated"
        } else {
            "heuristic"
        };
        let headline = format!(
            "buy {outcome_side}: composite {composite_score}, confidence {confidence} ({provenance} return)"
        );

        Ok(Some(SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: model_run_id.clone(),
            market_id: row.market_id.clone(),
            token_id,
            outcome_side,
            composite_score,
            confidence,
            expected_return_bps: estimate.expected_return_bps,
            downside_bps: estimate.downside_bps,
            win_probability: estimate.win_probability,
            entry_price_ref,
            suggested_horizon_secs: self.artifact.prediction_horizon_secs,
            factor_breakdown: contributions,
            model_explanation: ModelExplanation {
                headline,
                top_positive,
                top_negative,
            },
            rejection_warnings: context_warnings(&row.context, &self.artifact),
            rank_before_portfolio: 0,
            liquidity_score,
            data_quality_score,
            model_score_percentile: Probability::ZERO,
            decision_at: as_of,
        }))
    }

    /// Resolve the target token + executable entry price for the chosen outcome.
    /// `OutcomeSide::No` requires both a NO token and its PIT-resolved executable ask.
    fn resolve_entry(
        row: &FactorInferenceRow,
        outcome_side: OutcomeSide,
    ) -> Option<(TokenId, Price)> {
        match outcome_side {
            OutcomeSide::Yes => Some((row.token_id.clone(), row.context.yes_price)),
            OutcomeSide::No => {
                let token = row.context.secondary_token_id.clone()?;
                let price = row.context.no_price?;
                Some((token, price))
            }
        }
    }

    /// Apply the governed data-quality / liquidity / horizon multipliers and
    /// clamp the result into `[0, 1]`.
    fn apply_multipliers(
        &self,
        conviction: Decimal,
        context: &MarketInferenceContext,
    ) -> Probability {
        let multipliers = &self.artifact.multipliers;
        let dq = multipliers
            .data_quality
            .multiplier_for(context.data_quality);
        let liquidity = multipliers
            .liquidity
            .multiplier_for(context.liquidity_usd.map(Usd::inner));
        let horizon = multipliers.horizon.multiplier_for(
            context.time_to_resolution_secs,
            self.artifact.prediction_horizon_secs,
        );
        clamp_unit(conviction * dq * liquidity * horizon)
    }

    /// Compute the confidence aggregate and apply the governed substitution
    /// penalty for every audited imputation.
    fn apply_confidence(
        &self,
        confidence_mass: Decimal,
        confidence_weighted: Decimal,
        context: &MarketInferenceContext,
    ) -> Probability {
        let base = if confidence_mass > Decimal::ZERO {
            confidence_weighted / confidence_mass
        } else {
            Decimal::ZERO
        };
        let penalty = context
            .substitution_reasons
            .iter()
            .fold(Decimal::ONE, |acc, reason| {
                acc * self
                    .artifact
                    .substitution_confidence_rules
                    .multiplier_for(*reason)
            });
        clamp_unit(base * penalty)
    }

    fn input_audit(&self, table: &FactorInferenceTable) -> QuantResult<Vec<ModelInputAuditRow>> {
        let input_contract_hash = self.artifact.input_contract_hash.clone();
        let transform_hash = self.artifact.input_transform_hash()?;
        let training_input_hash = self.artifact.training_input_hash.clone();
        let row_count = table
            .rows
            .iter()
            .try_fold(0usize, |count, row| count.checked_add(row.factors.len()))
            .ok_or_else(|| ResearchError::Inference {
                detail: "weighted model-input audit row count overflow".to_owned(),
            })?;
        let mut audit = Vec::with_capacity(row_count);
        for row in &table.rows {
            for factor in &row.factors {
                let (raw_state, score) = match &factor.normalization {
                    NormalizedFactor::Scored { score, .. } => {
                        (ModelInputAuditState::Scored, Some(score.inner()))
                    }
                    NormalizedFactor::MissingInput => (ModelInputAuditState::MissingInput, None),
                    NormalizedFactor::NotApplicable => (ModelInputAuditState::NotApplicable, None),
                    NormalizedFactor::Indeterminate { .. } => {
                        (ModelInputAuditState::Indeterminate, None)
                    }
                };
                let encoded_value_bits = score
                    .map(|value| {
                        value
                            .to_f64()
                            .filter(|value| value.is_finite())
                            .ok_or_else(|| ResearchError::Inference {
                                detail: format!(
                                    "weighted factor `{}` normalized score cannot be represented as finite f64",
                                    factor.name
                                ),
                            })
                            .map(f64::to_bits)
                    })
                    .transpose()?;
                audit.push(ModelInputAuditRow {
                    model_version_id: self.artifact.header.model_version_id.clone(),
                    model_family: self.artifact.header.model_family,
                    market_id: row.market_id.clone(),
                    raw_input_name: factor.name.to_string(),
                    raw_state,
                    raw_value: factor.raw_value.map(|value| value.to_string()),
                    encoded_column: format!("{}.normalized_score", factor.name),
                    encoded_value_bits,
                    input_contract_hash: input_contract_hash.clone(),
                    transform_hash: transform_hash.clone(),
                    training_input_hash: training_input_hash.clone(),
                });
            }
        }
        Ok(audit)
    }
}

#[async_trait]
impl QuantModelRuntime for WeightedFactorRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.artifact.header.model_version_id.clone()
    }

    fn model_family(&self) -> ModelFamily {
        ModelFamily::WeightedFactor
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.artifact.header.feature_schema_hash.clone()
    }

    fn required_features(&self) -> Vec<FeatureName> {
        self.artifact.required_features()
    }

    fn category_scope(&self) -> Option<MarketCategory> {
        self.artifact.category_scope
    }

    fn weight_source(&self) -> ModelWeightSource {
        self.weight_source
    }

    fn factor_cross_section(&self) -> Option<&FactorCrossSectionConfig> {
        Some(&self.artifact.factor_cross_section)
    }

    fn frozen_reference_quantiles(&self) -> Option<&FrozenReferenceQuantiles> {
        Some(&self.artifact.frozen_reference_quantiles)
    }

    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
        let started = Instant::now();
        let table = expect_factor_table(input)?;
        let markets_scored =
            u32::try_from(table.rows.len()).map_err(|error| ResearchError::Inference {
                detail: format!("weighted market count does not fit u32: {error}"),
            })?;
        let input_audit = self.input_audit(&table)?;

        let nets = self.batch_layout.compute_nets(&table.rows)?;
        let mut candidates: Vec<SignalCandidate> = table
            .rows
            .iter()
            .zip(nets)
            .map(|(row, net)| {
                self.score_row_with_net(row, net, &table.model_run_id, table.decision_at)
            })
            .collect::<QuantResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        candidates.sort_by(|left, right| {
            right
                .composite_score
                .inner()
                .cmp(&left.composite_score.inner())
                .then_with(|| left.market_id.as_str().cmp(right.market_id.as_str()))
        });
        for (index, candidate) in candidates.iter_mut().enumerate() {
            candidate.rank_before_portfolio =
                u32::try_from(index + 1).map_err(|error| ResearchError::Inference {
                    detail: format!("weighted candidate rank does not fit u32: {error}"),
                })?;
        }

        let candidates_emitted =
            u32::try_from(candidates.len()).map_err(|error| ResearchError::Inference {
                detail: format!("weighted candidate count does not fit u32: {error}"),
            })?;
        let inference_duration_ms =
            u64::try_from(started.elapsed().as_millis()).map_err(|error| {
                ResearchError::Inference {
                    detail: format!("weighted inference duration does not fit u64: {error}"),
                }
            })?;

        Ok(ModelRuntimeOutput {
            candidates,
            runtime_metrics: ModelRuntimeMetrics {
                markets_scored,
                candidates_emitted,
                inference_duration_ms,
            },
            input_audit,
        })
    }
}

/// Extract the factor table, rejecting a feature-matrix input (classical only).
fn expect_factor_table(input: ModelRuntimeInput) -> QuantResult<FactorInferenceTable> {
    match input {
        ModelRuntimeInput::FactorTable(table) => Ok(table),
        ModelRuntimeInput::FeatureMatrix(_) => Err(ResearchError::Inference {
            detail: "weighted runtime requires a factor table, got a feature matrix".to_owned(),
        }
        .into()),
    }
}

/// Clamp a decimal into `[0, 1]` and build a [`Probability`] at the scorer scale.
fn clamp_unit(value: Decimal) -> Probability {
    Probability::new(
        value
            .clamp(Decimal::ZERO, Decimal::ONE)
            .round_dp(RESEARCH_DECIMAL_SCALE),
    )
}

/// Split contributions into the strongest positive and negative drivers.
fn split_contributions(
    contributions: &[FactorContribution],
) -> (Vec<FactorContribution>, Vec<FactorContribution>) {
    let mut positive: Vec<FactorContribution> = contributions
        .iter()
        .filter(|c| c.contribution > Decimal::ZERO)
        .cloned()
        .collect();
    let mut negative: Vec<FactorContribution> = contributions
        .iter()
        .filter(|c| c.contribution < Decimal::ZERO)
        .cloned()
        .collect();
    positive.sort_by_key(|item| Reverse(item.contribution));
    negative.sort_by_key(|item| item.contribution);
    positive.truncate(EXPLANATION_TOP_K);
    negative.truncate(EXPLANATION_TOP_K);
    (positive, negative)
}

/// Non-fatal, context-derived warnings attached to a candidate.
fn context_warnings(
    context: &MarketInferenceContext,
    artifact: &WeightedFactorModelArtifact,
) -> Vec<SignalWarning> {
    let mut warnings = Vec::new();
    let liquidity_multiplier = artifact
        .multipliers
        .liquidity
        .multiplier_for(context.liquidity_usd.map(Usd::inner));
    if liquidity_multiplier <= artifact.multipliers.liquidity.floor {
        warnings.push(SignalWarning::ThinLiquidity);
    }
    if matches!(
        context.data_quality,
        DataQualityStatus::Degraded | DataQualityStatus::Stale | DataQualityStatus::Insufficient
    ) {
        warnings.push(SignalWarning::StaleFeatures);
    }
    warnings
}

#[cfg(test)]
mod tests {
    use super::WeightedFactorRuntime;
    use chrono::Utc;
    use quant_pivot_models::{
        enums::{
            factor::{FactorFamily, FactorIndeterminateReason},
            quant::{DataQualityStatus, FactorDirection, OutcomeSide},
        },
        runtime_config::FactorCrossSectionConfig,
        types::{
            FactorDefinitionId, MarketId, ModelInputContract, ModelRunId, ModelVersionId, Price,
            Probability, TokenId, Usd, builtin_research_profiles,
        },
    };
    use rust_decimal_macros::dec;

    use crate::{
        factors::{
            FactorExplanation, FactorName, FactorValue, FrozenReferenceQuantiles, NormalizedFactor,
            names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        },
        model::{
            ReturnModelSpec,
            artifact::{
                FactorWeight, ModelArtifactHeader, ScoreMultiplierSpec,
                SubstitutionConfidenceRules, WeightedFactorModelArtifact,
                model_input_contract_hash,
            },
            runtime::{
                FactorInferenceRow, FactorInferenceTable, MarketInferenceContext, ModelFamily,
                ModelInputAuditState, ModelRuntimeInput, QuantModelRuntime,
            },
        },
        test_support::content_hash as hash,
    };

    fn factor(
        name: FactorName,
        normalized: Probability,
        direction: FactorDirection,
    ) -> FactorValue {
        FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name,
            family: FactorFamily::Liquidity,
            raw_value: Some(dec!(1)),
            normalization: NormalizedFactor::cross_section(normalized),
            direction,
            confidence: Probability::new(dec!(0.9)),
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        }
    }

    fn artifact() -> WeightedFactorModelArtifact {
        let input_contract = ModelInputContract::single_required("book.mid");
        let input_contract_hash =
            model_input_contract_hash(&input_contract).expect("input contract hash");
        WeightedFactorModelArtifact {
            header: ModelArtifactHeader {
                model_version_id: ModelVersionId::from_v7(),
                model_spec_definition_hash: hash("spec"),
                profile_ref: builtin_research_profiles()
                    .expect("built-in profiles")
                    .remove(0)
                    .profile_ref,
                model_family: ModelFamily::WeightedFactor,
                feature_schema_hash: hash("aa"),
                factor_schema_hash: hash("bb"),
                trade_policy_artifact_id: None,
                trade_policy_hash: None,
            },
            training_dataset_hash: hash("cc"),
            training_input_hash: hash("dd"),
            input_contract,
            input_contract_hash,
            weights: vec![
                FactorWeight {
                    factor: LIQUIDITY_DEPTH,
                    weight: dec!(0.5),
                },
                FactorWeight {
                    factor: MOMENTUM_ROC,
                    weight: dec!(0.5),
                },
            ],
            prediction_horizon_secs: 86_400,
            multipliers: ScoreMultiplierSpec::conservative(),
            substitution_confidence_rules: SubstitutionConfidenceRules::conservative(),
            return_model: ReturnModelSpec::heuristic_default(),
            factor_cross_section: FactorCrossSectionConfig::default(),
            frozen_reference_quantiles: FrozenReferenceQuantiles::empty(),
            objective_report: None,
            category_scope: None,
        }
    }

    fn context() -> MarketInferenceContext {
        MarketInferenceContext {
            secondary_token_id: Some(TokenId::new("no")),
            yes_price: Price::new(dec!(0.5)),
            no_price: Some(Price::new(dec!(0.52))),
            liquidity_usd: Some(Usd::new(dec!(60000))),
            data_quality: DataQualityStatus::Fresh,
            time_to_resolution_secs: Some(86_400),
            substitution_reasons: Vec::new(),
        }
    }

    fn row(market: &str, bullish: bool) -> FactorInferenceRow {
        let direction = if bullish {
            FactorDirection::Positive
        } else {
            FactorDirection::Negative
        };
        FactorInferenceRow {
            market_id: MarketId::new(market),
            token_id: TokenId::new("yes"),
            factors: vec![
                factor(LIQUIDITY_DEPTH, Probability::new(dec!(0.8)), direction),
                factor(MOMENTUM_ROC, Probability::new(dec!(0.6)), direction),
            ],
            context: context(),
        }
    }

    #[tokio::test]
    async fn weighted_runtime_scores_candidates_from_factor_table() {
        let expected_contract_hash =
            model_input_contract_hash(&ModelInputContract::single_required("book.mid"))
                .expect("contract hash");
        let runtime = WeightedFactorRuntime::new(artifact(), None, None).expect("runtime");
        let table = FactorInferenceTable {
            model_run_id: ModelRunId::from_v7(),
            decision_at: Utc::now(),
            rows: vec![row("0xstrong", true), row("0xbear", false)],
        };
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(table))
            .await
            .expect("infer");

        assert_eq!(output.candidates.len(), 2);
        let bull = output
            .candidates
            .iter()
            .find(|c| c.market_id.as_str() == "0xstrong")
            .expect("bull candidate");
        let bear = output
            .candidates
            .iter()
            .find(|c| c.market_id.as_str() == "0xbear")
            .expect("bear candidate");
        assert_eq!(bull.outcome_side, OutcomeSide::Yes);
        assert_eq!(bull.entry_price_ref.inner(), dec!(0.5));
        assert_eq!(bear.outcome_side, OutcomeSide::No);
        assert_eq!(bear.token_id.as_str(), "no");
        assert_eq!(bear.entry_price_ref.inner(), dec!(0.52));
        assert_ne!(
            bear.entry_price_ref.inner(),
            dec!(1) - bull.entry_price_ref.inner(),
            "weighted runtime must never synthesize the NO ask"
        );
        for candidate in &output.candidates {
            assert!(candidate.composite_score.inner() >= dec!(0));
            assert!(candidate.composite_score.inner() <= dec!(1));
            assert!(candidate.rank_before_portfolio >= 1);
        }
        assert_eq!(output.runtime_metrics.markets_scored, 2);
        assert_eq!(output.input_audit.len(), 4);
        assert!(output.input_audit.iter().all(|row| {
            row.raw_state == ModelInputAuditState::Scored
                && row.raw_value.as_deref() == Some("1")
                && row
                    .encoded_value_bits
                    .is_some_and(|bits| f64::from_bits(bits).is_finite())
                && row.input_contract_hash == expected_contract_hash
                && !row.transform_hash.as_str().is_empty()
                && !row.training_input_hash.as_str().is_empty()
        }));
    }

    #[tokio::test]
    async fn weighted_runtime_never_fabricates_a_missing_no_quote() {
        let runtime = WeightedFactorRuntime::new(artifact(), None, None).expect("runtime");
        let mut bearish = row("0xmissing-no", false);
        bearish.context.no_price = None;
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(FactorInferenceTable {
                model_run_id: ModelRunId::from_v7(),
                decision_at: Utc::now(),
                rows: vec![bearish],
            }))
            .await
            .expect("missing quote is an explicit market-level rejection");
        assert!(output.candidates.is_empty());
        assert_eq!(output.runtime_metrics.markets_scored, 1);
    }

    #[tokio::test]
    async fn weighted_audit_keeps_missing_factor_without_numeric_sentinel() {
        let runtime = WeightedFactorRuntime::new(artifact(), None, None).expect("runtime");
        let mut missing = factor(
            LIQUIDITY_DEPTH,
            Probability::new(dec!(0.8)),
            FactorDirection::Positive,
        );
        missing.raw_value = None;
        missing.normalization = NormalizedFactor::MissingInput;
        let table = FactorInferenceTable {
            model_run_id: ModelRunId::from_v7(),
            decision_at: Utc::now(),
            rows: vec![FactorInferenceRow {
                market_id: MarketId::new("0xmissing"),
                token_id: TokenId::new("yes"),
                factors: vec![
                    missing,
                    factor(
                        MOMENTUM_ROC,
                        Probability::new(dec!(0.6)),
                        FactorDirection::Positive,
                    ),
                ],
                context: context(),
            }],
        };
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(table))
            .await
            .expect("infer");
        let missing = output
            .input_audit
            .iter()
            .find(|row| row.raw_input_name == LIQUIDITY_DEPTH.as_str())
            .expect("missing factor audit");
        assert_eq!(missing.raw_state, ModelInputAuditState::MissingInput);
        assert!(missing.raw_value.is_none());
        assert!(
            missing.encoded_value_bits.is_none(),
            "missing factor must not be written as a numeric zero"
        );
    }

    #[tokio::test]
    async fn neutral_factors_emit_no_candidate() {
        let runtime = WeightedFactorRuntime::new(artifact(), None, None).expect("runtime");
        let neutral_row = FactorInferenceRow {
            market_id: MarketId::new("0xflat"),
            token_id: TokenId::new("yes"),
            factors: vec![
                factor(
                    LIQUIDITY_DEPTH,
                    Probability::new(dec!(0.5)),
                    FactorDirection::Neutral,
                ),
                factor(
                    MOMENTUM_ROC,
                    Probability::new(dec!(0.5)),
                    FactorDirection::Neutral,
                ),
            ],
            context: context(),
        };
        let table = FactorInferenceTable {
            model_run_id: ModelRunId::from_v7(),
            decision_at: Utc::now(),
            rows: vec![neutral_row],
        };
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(table))
            .await
            .expect("infer");
        assert!(
            output.candidates.is_empty(),
            "a neutral net signal must not emit a candidate"
        );
    }

    #[tokio::test]
    async fn indeterminate_factor_contributes_nothing() {
        // A market whose momentum factor is indeterminate (no usable cross-section)
        // still scores off the remaining scored factor; the indeterminate factor
        // is surfaced in the breakdown with a zero contribution and no fabricated
        // normalized score — never a silent neutral.
        let runtime = WeightedFactorRuntime::new(artifact(), None, None).expect("runtime");
        let indeterminate = FactorValue {
            definition_id: FactorDefinitionId::from_v7(),
            name: MOMENTUM_ROC,
            family: FactorFamily::Momentum,
            raw_value: Some(dec!(1)),
            normalization: NormalizedFactor::Indeterminate {
                reason: FactorIndeterminateReason::CrossSectionTooSmall,
            },
            direction: FactorDirection::Positive,
            confidence: Probability::ZERO,
            explanation: FactorExplanation {
                headline: "indeterminate".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        };
        let row = FactorInferenceRow {
            market_id: MarketId::new("0xmixed"),
            token_id: TokenId::new("yes"),
            factors: vec![
                factor(
                    LIQUIDITY_DEPTH,
                    Probability::new(dec!(0.8)),
                    FactorDirection::Positive,
                ),
                indeterminate,
            ],
            context: context(),
        };
        let table = FactorInferenceTable {
            model_run_id: ModelRunId::from_v7(),
            decision_at: Utc::now(),
            rows: vec![row],
        };
        let output = runtime
            .infer_batch(ModelRuntimeInput::FactorTable(table))
            .await
            .expect("infer");
        let candidate = output
            .candidates
            .iter()
            .find(|c| c.market_id.as_str() == "0xmixed")
            .expect("the scored factor still yields a candidate");
        let momentum = candidate
            .factor_breakdown
            .iter()
            .find(|entry| entry.name == MOMENTUM_ROC)
            .expect("indeterminate factor is surfaced in the breakdown");
        assert!(
            momentum.normalized_score.is_none(),
            "indeterminate factor carries no normalized score"
        );
        assert_eq!(
            momentum.contribution,
            dec!(0),
            "indeterminate factor contributes nothing"
        );
        assert_eq!(
            momentum.indeterminate_reason,
            Some(FactorIndeterminateReason::CrossSectionTooSmall)
        );
    }
}
