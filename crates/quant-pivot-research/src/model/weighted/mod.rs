//! [`WeightedFactorRuntime`]: the first-class, fully-explainable weighted factor
//! scorer.
//!
//! It consumes a [`FactorInferenceTable`] of already-normalized factor values and
//! produces ranked [`SignalCandidate`]s. The score separates a **ranking model**
//! from a **return/risk mapping**, both frozen in the artifact:
//!
//! ```text
//! signedᵢ = dir_signᵢ · normalizedᵢ · confidenceᵢ (∈ [-1, 1])
//! net = Σ weightᵢ · signedᵢ (∈ [-1, 1])
//! outcome_side = sign(net) → Yes (>0) / No (<0) (0 ⇒ no candidate)
//! conviction = |net|
//! composite = clamp₀₁(conviction · dq_mult · liq_mult · horizon_mult)
//! confidence = clamp₀₁(weighted_mean(confidenceᵢ) · Π substitution_penalty)
//! (return,down) = artifact.return_model.estimate(composite, confidence)
//! ```
//!
//! Weights are non-negative and sum to 1 (validated on construction); missing
//! factors carry `confidence = 0` so they contribute nothing yet remain auditable.
//! Cross-sectional `net` uses `ndarray` batch reduction; money fields remain
//! `Decimal` / newtypes after quantization.

use std::{cmp::Reverse, collections::HashMap, time::Instant};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use quant_pivot_error::{QuantResult, research::ResearchError};
use quant_pivot_models::{
    enums::{
        common::MarketCategory,
        factor::FactorFamily,
        model::ModelFamily,
        quant::{DataQualityStatus, ModelWeightSource, OutcomeSide},
    },
    runtime_config::FactorCrossSectionConfig,
    types::{
        ContentHash, ModelRunId, ModelVersionId, OutcomeTokenBinding, Price, Probability,
        SignalCandidateId, TokenId,
        factor::{FactorAlphaOrientation, FactorServingPlane},
    },
};
use rust_decimal::{Decimal, prelude::ToPrimitive};

use crate::{
    factors::{FrozenReferenceQuantiles, NormalizedFactor, value::FactorScoringProjection},
    features::FeatureName,
    model::{
        artifact::{ModelArtifact, ModelArtifactHeader, ModelPayload, WeightedFactorModelPayload},
        calibrator::ResolvedCalibration,
        factor_heads::score_factor_heads,
        runtime::{
            FactorInferenceRow, FactorInferenceTable, MarketInferenceContext, ModelInputAuditRow,
            ModelInputAuditState, ModelRuntimeInput, ModelRuntimeMetrics, ModelRuntimeOutput,
            QuantModelRuntime,
        },
        signal::{FactorContribution, ModelExplanation, SignalCandidate, SignalWarning},
    },
    precision::RESEARCH_DECIMAL_SCALE,
};

/// How many positive / negative contributions the explanation surfaces.
const EXPLANATION_TOP_K: usize = 3;

/// A governed weighted-factor scorer bound to one frozen artifact.
pub struct WeightedFactorRuntime {
    header: ModelArtifactHeader,
    payload: WeightedFactorModelPayload,
    /// Resolved `ProbabilityCalibrator` data when `artifact.return_model` is
    /// `Calibrated` — bound once from the verified serving preimage,
    /// never re-fetched per candidate. `None` for `Heuristic`.
    calibration: Option<ResolvedCalibration>,
}

impl WeightedFactorRuntime {
    /// Build a runtime from a validated weighted artifact and the resolved
    /// calibrator when the payload's return model is `Calibrated` (the loader —
    /// `quant-pivot-core`'s
    /// the verified serving-preimage owner fails the load closed if it cannot
    /// resolve a `Calibrated` artifact's `calibrator_ref`, so this is `None`
    /// only for `Heuristic`).
    ///
    /// # Errors
    ///
    /// Propagates [`ModelArtifact::validate`] for any contract/payload mismatch.
    pub fn new(
        artifact: ModelArtifact,
        calibration: Option<ResolvedCalibration>,
    ) -> QuantResult<Self> {
        let (header, payload) = artifact.into_parts()?;
        let ModelPayload::WeightedFactor(payload) = payload else {
            return Err(ResearchError::InvalidModelArtifact {
                detail: "weighted runtime requires a WeightedFactor payload".to_owned(),
            }
            .into());
        };
        Ok(Self {
            header,
            payload: *payload,
            calibration,
        })
    }

    /// Collect canonical alpha and multiplicative context contributions.
    fn row_contributions(
        &self,
        row: &FactorInferenceRow,
        outcome_binding: &OutcomeTokenBinding,
    ) -> QuantResult<Vec<FactorContribution>> {
        let plane = &self.header.serving_contract().bindings().factors.plane;
        let alpha_weights = self
            .payload
            .factor_head
            .alpha_weights
            .iter()
            .map(|weight| (weight.factor_definition_id, weight.weight))
            .collect::<HashMap<_, _>>();
        let context_weights = self
            .payload
            .factor_head
            .context_weights
            .iter()
            .map(|weight| (weight.factor_definition_id, weight))
            .collect::<HashMap<_, _>>();
        let mut contributions = Vec::with_capacity(plane.definitions().len());
        for revision in plane.definitions() {
            let factor = row
                .factors
                .iter()
                .find(|factor| factor.definition_id == revision.factor_definition_id())
                .ok_or_else(|| ResearchError::Inference {
                    detail: format!(
                        "factor row omitted sealed revision {}",
                        revision.factor_definition_id()
                    ),
                })?;
            let projection = factor.scoring_projection(revision)?;
            let (weight, contribution) = match projection {
                Some(FactorScoringProjection::OutcomeAlpha {
                    orientation,
                    strength,
                    confidence,
                }) => {
                    let weight = alpha_weights
                        .get(&revision.factor_definition_id())
                        .copied()
                        .ok_or_else(|| ResearchError::Inference {
                            detail: format!(
                                "alpha head omitted sealed revision {}",
                                revision.factor_definition_id()
                            ),
                        })?;
                    let yes_strength = match orientation {
                        FactorAlphaOrientation::FeatureToken => {
                            Decimal::from(outcome_binding.feature_to_yes_sign()) * strength
                        }
                        FactorAlphaOrientation::CanonicalYes => strength,
                    };
                    (
                        weight,
                        (weight * confidence.inner() * yes_strength)
                            .round_dp(RESEARCH_DECIMAL_SCALE),
                    )
                }
                Some(FactorScoringProjection::Context {
                    adequacy,
                    confidence,
                }) => {
                    let weight = context_weights
                        .get(&revision.factor_definition_id())
                        .copied()
                        .ok_or_else(|| ResearchError::Inference {
                            detail: format!(
                                "context head omitted sealed revision {}",
                                revision.factor_definition_id()
                            ),
                        })?;
                    let penalty = weight.penalty_strength
                        * confidence.inner()
                        * (Decimal::ONE - adequacy.inner());
                    (
                        weight.penalty_strength,
                        (-penalty).round_dp(RESEARCH_DECIMAL_SCALE),
                    )
                }
                Some(FactorScoringProjection::Diagnostic { .. }) | None => {
                    (Decimal::ZERO, Decimal::ZERO)
                }
            };
            contributions.push(FactorContribution {
                definition_id: factor.definition_id,
                name: factor.name.clone(),
                family: factor.family,
                value_state: factor.value_state(),
                raw_value: factor.raw_value,
                normalized_score: factor.normalized_score(),
                normalization_source: factor.normalization_source(),
                indeterminate_reason: factor.indeterminate_reason(),
                weight,
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
        Ok(contributions)
    }

    /// Score one market row through the canonical two-head algebra.
    fn score_row(
        &self,
        row: &FactorInferenceRow,
        model_run_id: &ModelRunId,
        as_of: DateTime<Utc>,
    ) -> QuantResult<Option<SignalCandidate>> {
        let outcome_binding = Self::outcome_binding(row)?;
        let substitution_reliability = self.substitution_reliability(&row.context);
        let horizon_multiplier = self.payload.horizon_multipliers.multiplier_for(
            row.context.time_to_resolution_secs,
            self.header.prediction_horizon_secs(),
        );
        let score = score_factor_heads(
            &row.factors,
            &self.header.serving_contract().bindings().factors.plane,
            &self.payload.factor_head,
            &outcome_binding,
            substitution_reliability,
            horizon_multiplier,
        )?;
        let Some(outcome_side) = score.outcome_side else {
            return Ok(None);
        };
        let Some((token_id, entry_price_ref)) = Self::resolve_entry(row, outcome_side) else {
            return Ok(None);
        };
        let contributions = self.row_contributions(row, &outcome_binding)?;
        let composite_score = clamp_unit(score.composite_score);
        let confidence = clamp_unit(score.reliability);
        let data_quality_score = self.family_adequacy(row, FactorFamily::DataQuality)?;
        let liquidity_score = self.family_adequacy(row, FactorFamily::Liquidity)?;
        let estimate = self.payload.return_model.estimate(
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
            "buy {outcome_side}: alpha {}, composite {composite_score}, reliability {confidence} ({provenance} return)",
            score.yes_alpha
        );

        Ok(Some(SignalCandidate {
            signal_candidate_id: SignalCandidateId::from_v7(),
            model_run_id: *model_run_id,
            market_id: row.market_id.clone(),
            token_id,
            outcome_side,
            composite_score,
            confidence,
            expected_return_bps: estimate.expected_return_bps,
            downside_bps: estimate.downside_bps,
            win_probability: estimate.win_probability,
            entry_price_ref,
            suggested_horizon_secs: self.header.prediction_horizon_secs(),
            factor_breakdown: contributions,
            model_explanation: ModelExplanation {
                headline,
                top_positive,
                top_negative,
            },
            rejection_warnings: context_warnings(&row.context, liquidity_score, data_quality_score),
            rank_before_portfolio: 0,
            liquidity_score,
            data_quality_score,
            model_score_percentile: Probability::ZERO,
            decision_at: as_of,
        }))
    }

    fn outcome_binding(row: &FactorInferenceRow) -> QuantResult<OutcomeTokenBinding> {
        let no_token_id =
            row.context
                .secondary_token_id
                .clone()
                .ok_or_else(|| ResearchError::Inference {
                    detail: format!(
                        "market {} has no canonical NO token for factor-head scoring",
                        row.market_id
                    ),
                })?;
        OutcomeTokenBinding::try_new(
            row.market_id.clone(),
            row.token_id.clone(),
            no_token_id,
            row.token_id.clone(),
            OutcomeSide::Yes,
        )
        .map_err(|error| {
            ResearchError::Inference {
                detail: format!("invalid outcome-token binding: {error}"),
            }
            .into()
        })
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

    fn substitution_reliability(&self, context: &MarketInferenceContext) -> Decimal {
        context
            .substitution_reasons
            .iter()
            .fold(Decimal::ONE, |acc, reason| {
                acc * self
                    .payload
                    .substitution_confidence_rules
                    .multiplier_for(*reason)
            })
    }

    fn family_adequacy(
        &self,
        row: &FactorInferenceRow,
        family: FactorFamily,
    ) -> QuantResult<Probability> {
        let plane = &self.header.serving_contract().bindings().factors.plane;
        let context_weights = self
            .payload
            .factor_head
            .context_weights
            .iter()
            .map(|weight| (weight.factor_definition_id, weight))
            .collect::<HashMap<_, _>>();
        let mut numerator = Decimal::ZERO;
        let mut denominator = Decimal::ZERO;
        for revision in plane
            .definitions()
            .iter()
            .filter(|revision| revision.definition().family == family)
        {
            let factor = row
                .factors
                .iter()
                .find(|factor| factor.definition_id == revision.factor_definition_id())
                .ok_or_else(|| ResearchError::Inference {
                    detail: format!(
                        "factor row omitted sealed revision {}",
                        revision.factor_definition_id()
                    ),
                })?;
            if factor.is_not_applicable() {
                factor.validate_against(revision)?;
                continue;
            }
            let Some(weight) = context_weights
                .get(&revision.factor_definition_id())
                .copied()
            else {
                continue;
            };
            denominator += weight.coverage_weight;
            if let Some(FactorScoringProjection::Context {
                adequacy,
                confidence,
            }) = factor.scoring_projection(revision)?
            {
                numerator += weight.coverage_weight * confidence.inner() * adequacy.inner();
            }
        }
        if denominator.is_zero() {
            Ok(Probability::ZERO)
        } else {
            Ok(clamp_unit(numerator / denominator))
        }
    }

    fn input_audit(&self, table: &FactorInferenceTable) -> QuantResult<Vec<ModelInputAuditRow>> {
        let transform = &self.header.serving_contract().bindings().transform;
        let input_contract_hash = transform.input_contract_hash;
        let transform_hash = transform.input_transform_hash;
        let training_input_hash = transform.training_input_hash;
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
                    model_version_id: self.header.model_version_id(),
                    model_family: self.header.model_family(),
                    market_id: row.market_id.clone(),
                    raw_input_name: factor.name.to_string(),
                    raw_state,
                    raw_value: factor.raw_value.map(|value| value.to_string()),
                    encoded_column: format!("{}.normalized_score", factor.name),
                    encoded_value_bits,
                    input_contract_hash,
                    transform_hash,
                    training_input_hash,
                });
            }
        }
        Ok(audit)
    }
}

#[async_trait]
impl QuantModelRuntime for WeightedFactorRuntime {
    fn model_version_id(&self) -> ModelVersionId {
        self.header.model_version_id()
    }

    fn model_family(&self) -> ModelFamily {
        ModelFamily::WeightedFactor
    }

    fn feature_schema_hash(&self) -> ContentHash {
        self.header.feature_schema_hash()
    }

    fn required_features(&self) -> Vec<FeatureName> {
        self.payload.required_features()
    }

    fn category_scope(&self) -> Option<MarketCategory> {
        self.header.category_scope()
    }

    fn weight_source(&self) -> ModelWeightSource {
        ModelWeightSource::Artifact
    }

    fn factor_cross_section(&self) -> Option<&FactorCrossSectionConfig> {
        Some(&self.payload.factor_cross_section)
    }

    fn factor_serving_plane(&self) -> Option<&FactorServingPlane> {
        Some(&self.header.serving_contract().bindings().factors.plane)
    }

    fn frozen_reference_quantiles(&self) -> Option<&FrozenReferenceQuantiles> {
        Some(&self.payload.frozen_reference_quantiles)
    }

    async fn infer_batch(&self, input: ModelRuntimeInput) -> QuantResult<ModelRuntimeOutput> {
        let started = Instant::now();
        let table = (input).expect_factor_table()?;
        let markets_scored =
            u32::try_from(table.rows.len()).map_err(|error| ResearchError::Inference {
                detail: format!("weighted market count does not fit u32: {error}"),
            })?;
        let input_audit = self.input_audit(&table)?;

        let mut candidates: Vec<SignalCandidate> = table
            .rows
            .iter()
            .map(|row| self.score_row(row, &table.model_run_id, table.decision_at))
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

impl ModelRuntimeInput {
    /// Extract the factor table, rejecting a feature-matrix input (classical only).
    fn expect_factor_table(self) -> QuantResult<FactorInferenceTable> {
        match self {
            Self::FactorTable(table) => Ok(table),
            Self::FeatureMatrix(_) => Err(ResearchError::Inference {
                detail: "weighted runtime requires a factor table, got a feature matrix".to_owned(),
            }
            .into()),
        }
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
    liquidity_score: Probability,
    data_quality_score: Probability,
) -> Vec<SignalWarning> {
    let mut warnings = Vec::new();
    if context.liquidity_usd.is_none() || liquidity_score.inner() < Decimal::new(5, 1) {
        warnings.push(SignalWarning::ThinLiquidity);
    }
    if data_quality_score.inner() < Decimal::new(5, 1)
        || matches!(
            context.data_quality,
            DataQualityStatus::Degraded
                | DataQualityStatus::Stale
                | DataQualityStatus::Insufficient
        )
    {
        warnings.push(SignalWarning::StaleFeatures);
    }
    warnings
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use quant_pivot_models::{
        enums::{
            factor::FactorIndeterminateReason,
            quant::{DataQualityStatus, FactorDirection, OutcomeSide},
        },
        types::{
            MarketId, ModelInputContract, ModelRunId, Price, Probability, TokenId, Usd,
            factor::FactorExplanation,
        },
    };
    use rust_decimal::{Decimal, prelude::ToPrimitive};
    use rust_decimal_macros::dec;

    use super::WeightedFactorRuntime;
    use crate::{
        factors::{
            FactorName, FactorValue, NormalizedFactor,
            names::{LIQUIDITY_DEPTH, MOMENTUM_ROC},
        },
        model::{
            artifact::{ModelArtifact, model_input_contract_hash},
            runtime::{
                FactorInferenceRow, FactorInferenceTable, MarketInferenceContext,
                ModelInputAuditState, ModelRuntimeInput, QuantModelRuntime,
            },
        },
        test_support::weighted_factor_plane,
    };

    fn factor(
        name: &FactorName,
        normalized: Probability,
        direction: FactorDirection,
    ) -> FactorValue {
        let plane = weighted_factor_plane();
        let revision = plane
            .definitions()
            .iter()
            .find(|revision| revision.factor_name() == name)
            .expect("factor fixture revision");
        let is_alpha = revision.definition().is_outcome_alpha();
        let raw_value = if is_alpha {
            match direction {
                FactorDirection::Positive => dec!(1),
                FactorDirection::Negative => dec!(-1),
                FactorDirection::Neutral => Decimal::ZERO,
            }
        } else {
            dec!(1)
        };
        let direction = revision
            .definition()
            .contribution_direction(raw_value)
            .expect("fixture factor direction");
        let normalized = if is_alpha && direction == FactorDirection::Neutral {
            Probability::ZERO
        } else {
            normalized
        };
        let confidence = if is_alpha && direction == FactorDirection::Neutral {
            Probability::ZERO
        } else {
            Probability::new(dec!(0.9))
        };
        FactorValue {
            definition_id: revision.factor_definition_id(),
            name: revision.factor_name().clone(),
            family: revision.definition().family,
            raw_value: Some(raw_value),
            normalization: NormalizedFactor::cross_section(normalized),
            direction,
            confidence,
            explanation: FactorExplanation {
                headline: "t".to_owned(),
                drivers: Vec::new(),
            },
            input_feature_refs: Vec::new(),
        }
    }

    impl MarketInferenceContext {
        fn weighted_fixture() -> Self {
            Self {
                secondary_token_id: Some(TokenId::new("no")),
                yes_price: Price::new(dec!(0.5)),
                no_price: Some(Price::new(dec!(0.52))),
                liquidity_usd: Some(Usd::new(dec!(60000))),
                data_quality: DataQualityStatus::Fresh,
                time_to_resolution_secs: Some(86_400),
                substitution_reasons: Vec::new(),
            }
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
                factor(&LIQUIDITY_DEPTH, Probability::new(dec!(0.8)), direction),
                factor(&MOMENTUM_ROC, Probability::new(dec!(0.6)), direction),
            ],
            context: MarketInferenceContext::weighted_fixture(),
        }
    }

    #[tokio::test]
    async fn weighted_runtime_scores_table() {
        let expected_contract_hash =
            model_input_contract_hash(&ModelInputContract::single_required("book.mid"))
                .expect("contract hash");
        let runtime =
            WeightedFactorRuntime::new(ModelArtifact::weighted_fixture(), None).expect("runtime");
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
        let liquidity_name = LIQUIDITY_DEPTH;
        let momentum_name = MOMENTUM_ROC;
        let expected_audit = [
            ("0xstrong", liquidity_name.as_str(), "1", dec!(0.8)),
            ("0xstrong", momentum_name.as_str(), "1", dec!(0.6)),
            ("0xbear", liquidity_name.as_str(), "1", dec!(0.8)),
            ("0xbear", momentum_name.as_str(), "-1", dec!(0.6)),
        ];
        for (audit, (market, factor, raw_value, encoded_value)) in
            output.input_audit.iter().zip(expected_audit)
        {
            assert_eq!(audit.market_id.as_str(), market);
            assert_eq!(audit.raw_input_name, factor);
            assert_eq!(audit.raw_state, ModelInputAuditState::Scored);
            assert_eq!(audit.raw_value.as_deref(), Some(raw_value));
            assert_eq!(
                audit.encoded_value_bits,
                encoded_value.to_f64().map(f64::to_bits)
            );
            assert_eq!(audit.input_contract_hash, expected_contract_hash);
            assert!(
                audit
                    .transform_hash
                    .as_bytes()
                    .iter()
                    .any(|byte| *byte != 0)
            );
            assert!(
                audit
                    .training_input_hash
                    .as_bytes()
                    .iter()
                    .any(|byte| *byte != 0)
            );
        }
    }

    #[tokio::test]
    async fn weighted_never_missing_no() {
        let runtime =
            WeightedFactorRuntime::new(ModelArtifact::weighted_fixture(), None).expect("runtime");
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
    async fn weighted_keeps_missing_without() {
        let runtime =
            WeightedFactorRuntime::new(ModelArtifact::weighted_fixture(), None).expect("runtime");
        let mut missing = factor(
            &LIQUIDITY_DEPTH,
            Probability::new(dec!(0.8)),
            FactorDirection::Positive,
        );
        missing.raw_value = None;
        missing.normalization = NormalizedFactor::MissingInput;
        missing.direction = FactorDirection::Neutral;
        missing.confidence = Probability::ZERO;
        let table = FactorInferenceTable {
            model_run_id: ModelRunId::from_v7(),
            decision_at: Utc::now(),
            rows: vec![FactorInferenceRow {
                market_id: MarketId::new("0xmissing"),
                token_id: TokenId::new("yes"),
                factors: vec![
                    missing,
                    factor(
                        &MOMENTUM_ROC,
                        Probability::new(dec!(0.6)),
                        FactorDirection::Positive,
                    ),
                ],
                context: MarketInferenceContext::weighted_fixture(),
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
    async fn neutral_factors_no_candidate() {
        let runtime =
            WeightedFactorRuntime::new(ModelArtifact::weighted_fixture(), None).expect("runtime");
        let neutral_row = FactorInferenceRow {
            market_id: MarketId::new("0xflat"),
            token_id: TokenId::new("yes"),
            factors: vec![
                factor(
                    &LIQUIDITY_DEPTH,
                    Probability::new(dec!(0.5)),
                    FactorDirection::Neutral,
                ),
                factor(
                    &MOMENTUM_ROC,
                    Probability::new(dec!(0.5)),
                    FactorDirection::Neutral,
                ),
            ],
            context: MarketInferenceContext::weighted_fixture(),
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
        let runtime =
            WeightedFactorRuntime::new(ModelArtifact::weighted_fixture(), None).expect("runtime");
        let plane = weighted_factor_plane();
        let momentum_revision = plane
            .definitions()
            .iter()
            .find(|revision| revision.factor_name() == &MOMENTUM_ROC)
            .expect("momentum fixture revision");
        let indeterminate = FactorValue {
            definition_id: momentum_revision.factor_definition_id(),
            name: momentum_revision.factor_name().clone(),
            family: momentum_revision.definition().family,
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
                    &LIQUIDITY_DEPTH,
                    Probability::new(dec!(0.8)),
                    FactorDirection::Positive,
                ),
                indeterminate,
            ],
            context: MarketInferenceContext::weighted_fixture(),
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
        assert!(
            output.candidates.is_empty(),
            "context evidence alone cannot choose an outcome side"
        );
        let momentum = output
            .input_audit
            .iter()
            .find(|entry| entry.raw_input_name == MOMENTUM_ROC.as_str())
            .expect("indeterminate factor audit");
        assert_eq!(momentum.raw_state, ModelInputAuditState::Indeterminate);
        assert!(momentum.encoded_value_bits.is_none());
    }
}
