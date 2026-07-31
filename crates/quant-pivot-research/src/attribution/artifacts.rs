use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::AttributionSubject,
    enums::quant::{AttributionArtifactKind, AttributionCohort},
    hashing::CanonicalDigest,
    types::{
        Bps, ContentHash, FeedbackCycleId, MarketId, ModelVersionId, OrderIntentId,
        OutcomeTokenBinding, Price, RecommendationId, TokenId,
        factor::{FactorAlphaOrientation, FactorOutputSemantics, FactorServingPlane},
    },
};
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorValue, value::FactorScoringProjection},
    model::factor_heads::{FactorHeadSpec, score_factor_heads},
};

use super::{TreeEnsembleInput, TreeEnsembleSpec, TreeShapExplainer};

const ATTRIBUTION_SCHEMA_VERSION: u32 = 2;
const MAX_EFFICIENCY_RESIDUAL: Decimal = Decimal::from_parts(1, 0, 0, false, 12);
const ASSOCIATION_ESTIMATOR_DOMAIN: &str = "quant-pivot/attribution-association-estimator";
const ASSOCIATION_COHORT_DOMAIN: &str = "quant-pivot/attribution-association-cohort";
const ASSOCIATION_EXPLANATIONS_DOMAIN: &str = "quant-pivot/attribution-explanation-set";
const ASSOCIATION_RESOLUTIONS_DOMAIN: &str = "quant-pivot/attribution-resolution-set";
const ASSOCIATION_EXECUTIONS_DOMAIN: &str = "quant-pivot/attribution-execution-set";
const ASSOCIATION_VERSION: u32 = 1;
const DECISION_INTERVENTION_DOMAIN: &str = "quant-pivot/decision-intervention-policy";
const DECISION_DEPENDENCY_DOMAIN: &str = "quant-pivot/decision-dependency-graph";
const DECISION_UNIVERSE_DOMAIN: &str = "quant-pivot/decision-candidate-universe";
const DECISION_VERSION: u32 = 2;
const DECISION_INTERVENTION_POLICY: &str =
    "versioned_encoded_input_intervention_replay_model_economics_rank_topn_v2";
const DECISION_DEPENDENCY_NODES: [&str; 5] = [
    "composite_score",
    "economic_projection",
    "model_output",
    "model_rank",
    "model_top_n",
];

fn invalid(detail: impl Into<String>) -> QuantError {
    ResearchError::InvalidModelArtifact {
        detail: detail.into(),
    }
    .into()
}

fn invalid_method(detail: impl Into<String>) -> QuantError {
    ResearchError::ValidationMethodology {
        detail: detail.into(),
    }
    .into()
}

fn leakage(detail: impl Into<String>) -> QuantError {
    ResearchError::LeakageDetected {
        detail: detail.into(),
    }
    .into()
}

/// Common PIT and cycle lineage carried inside every attribution payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttributionLineage {
    pub schema_version: u32,
    pub source_feedback_cycle_id: FeedbackCycleId,
    pub source_cohort: AttributionCohort,
    pub source_cutoff: DateTime<Utc>,
    pub generated_at: DateTime<Utc>,
    pub source_evidence_hashes: Vec<ContentHash>,
}

impl AttributionLineage {
    pub fn try_new(
        source_feedback_cycle_id: FeedbackCycleId,
        source_cohort: AttributionCohort,
        source_cutoff: DateTime<Utc>,
        generated_at: DateTime<Utc>,
        mut source_evidence_hashes: Vec<ContentHash>,
    ) -> QuantResult<Self> {
        source_evidence_hashes.sort_unstable();
        source_evidence_hashes.dedup();
        let lineage = Self {
            schema_version: ATTRIBUTION_SCHEMA_VERSION,
            source_feedback_cycle_id,
            source_cohort,
            source_cutoff,
            generated_at,
            source_evidence_hashes,
        };
        lineage.validate()?;
        Ok(lineage)
    }

    pub fn validate(&self) -> QuantResult<()> {
        if self.schema_version != ATTRIBUTION_SCHEMA_VERSION {
            return Err(invalid(format!(
                "unsupported attribution schema version {}",
                self.schema_version
            )));
        }
        if self.source_cutoff.timestamp_millis() <= 0 || self.generated_at < self.source_cutoff {
            return Err(leakage(
                "attribution generation must occur at or after the frozen source cutoff",
            ));
        }
        if self.source_evidence_hashes.is_empty() || !strictly_sorted(&self.source_evidence_hashes)
        {
            return Err(invalid(
                "attribution source evidence hashes must be non-empty, unique, and sorted",
            ));
        }
        Ok(())
    }
}

/// Exact output whose value is being allocated by a prediction explanation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictionOutputKind {
    CanonicalYesAlpha,
    CalibratedWinProbability,
    ClassicalRawPrediction,
}

/// One exact feature/factor allocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictionContribution {
    pub input_name: String,
    pub input_value: Option<Decimal>,
    pub contribution: Decimal,
}

/// Input for the exact affine decomposition used by weighted models.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedExplanationInput {
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub model_artifact_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub output_kind: PredictionOutputKind,
    pub intercept: Decimal,
    pub terms: Vec<WeightedTerm>,
}

/// Exact weighted-factor runtime preimage used for one additive alpha
/// explanation.
#[derive(Debug, Clone, Copy)]
pub struct WeightedFactorExplanationInput<'a> {
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub model_artifact_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub factors: &'a [FactorValue],
    pub plane: &'a FactorServingPlane,
    pub spec: &'a FactorHeadSpec,
    pub outcome_binding: &'a OutcomeTokenBinding,
}

/// One affine `weight × encoded_value` term.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WeightedTerm {
    pub input_name: String,
    pub encoded_value: Decimal,
    pub weight: Decimal,
}

/// Immutable explanation of one model output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PredictionExplanationArtifact {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub model_artifact_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub output_kind: PredictionOutputKind,
    pub method: PredictionExplanationMethod,
    pub baseline_output: Decimal,
    pub predicted_output: Decimal,
    pub contributions: Vec<PredictionContribution>,
    pub efficiency_residual: Decimal,
}

/// Closed explanation-method vocabulary. Neither variant implies causality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictionExplanationMethod {
    WeightedClosedForm,
    ExactTreeShap,
}

impl PredictionExplanationArtifact {
    pub fn weighted(
        lineage: AttributionLineage,
        input: WeightedExplanationInput,
    ) -> QuantResult<Self> {
        if input.terms.is_empty() {
            return Err(invalid("weighted explanation requires at least one term"));
        }
        let mut contributions = input
            .terms
            .into_iter()
            .map(|term| PredictionContribution {
                input_name: term.input_name,
                input_value: Some(term.encoded_value),
                contribution: term.weight * term.encoded_value,
            })
            .collect::<Vec<_>>();
        contributions.sort_by(|left, right| left.input_name.cmp(&right.input_name));
        let predicted_output = input.intercept
            + contributions
                .iter()
                .map(|term| term.contribution)
                .sum::<Decimal>();
        let artifact = Self {
            lineage,
            model_version_id: input.model_version_id,
            recommendation_id: input.recommendation_id,
            model_artifact_hash: input.model_artifact_hash,
            input_contract_hash: input.input_contract_hash,
            output_kind: input.output_kind,
            method: PredictionExplanationMethod::WeightedClosedForm,
            baseline_output: input.intercept,
            predicted_output,
            contributions,
            efficiency_residual: Decimal::ZERO,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    /// Decompose the exact canonical-YES alpha emitted by the weighted factor
    /// head. Context multipliers remain outside this additive explanation
    /// because their algebra is multiplicative, not an affine allocation.
    pub fn weighted_factor(
        lineage: AttributionLineage,
        input: WeightedFactorExplanationInput<'_>,
    ) -> QuantResult<Self> {
        let score = score_factor_heads(
            input.factors,
            input.plane,
            input.spec,
            input.outcome_binding,
            Decimal::ONE,
            Decimal::ONE,
        )?;
        let mut denominator = Decimal::ZERO;
        let mut contributions = Vec::with_capacity(input.spec.alpha_weights.len());
        for weight in &input.spec.alpha_weights {
            let revision = input
                .plane
                .definitions()
                .iter()
                .find(|revision| revision.factor_definition_id() == weight.factor_definition_id)
                .ok_or_else(|| {
                    invalid(format!(
                        "weighted explanation cannot resolve factor revision {}",
                        weight.factor_definition_id
                    ))
                })?;
            if !matches!(
                revision.definition().output,
                FactorOutputSemantics::OutcomeAlpha { .. }
            ) {
                return Err(invalid(format!(
                    "weighted explanation factor `{}` is not an OutcomeAlpha input",
                    revision.factor_name()
                )));
            }
            let value = input
                .factors
                .iter()
                .find(|value| value.definition_id == weight.factor_definition_id)
                .ok_or_else(|| {
                    invalid(format!(
                        "weighted explanation cannot resolve factor value {}",
                        weight.factor_definition_id
                    ))
                })?;
            let projection = value.scoring_projection(revision)?;
            if !value.is_not_applicable() {
                denominator += weight.weight;
            }
            let (input_value, numerator) = match projection {
                Some(FactorScoringProjection::OutcomeAlpha {
                    orientation,
                    strength,
                    confidence,
                }) => {
                    let yes_strength = match orientation {
                        FactorAlphaOrientation::FeatureToken => {
                            Decimal::from(input.outcome_binding.feature_to_yes_sign()) * strength
                        }
                        FactorAlphaOrientation::CanonicalYes => strength,
                    };
                    let encoded = confidence.inner() * yes_strength;
                    (Some(encoded), weight.weight * encoded)
                }
                Some(
                    FactorScoringProjection::Context { .. }
                    | FactorScoringProjection::Diagnostic { .. },
                ) => {
                    return Err(invalid(format!(
                        "weighted explanation factor `{}` projected into the wrong head",
                        revision.factor_name()
                    )));
                }
                None => (None, Decimal::ZERO),
            };
            contributions.push(PredictionContribution {
                input_name: revision.factor_name().to_string(),
                input_value,
                contribution: numerator,
            });
        }
        if denominator.is_zero() {
            return Err(invalid_method(
                "weighted explanation has no applicable OutcomeAlpha weight",
            ));
        }
        for contribution in &mut contributions {
            contribution.contribution /= denominator;
        }
        contributions.sort_by(|left, right| left.input_name.cmp(&right.input_name));
        let allocated = contributions
            .iter()
            .map(|contribution| contribution.contribution)
            .sum::<Decimal>();
        let artifact = Self {
            lineage,
            model_version_id: input.model_version_id,
            recommendation_id: input.recommendation_id,
            model_artifact_hash: input.model_artifact_hash,
            input_contract_hash: input.input_contract_hash,
            output_kind: PredictionOutputKind::CanonicalYesAlpha,
            method: PredictionExplanationMethod::WeightedClosedForm,
            baseline_output: Decimal::ZERO,
            predicted_output: score.yes_alpha,
            contributions,
            efficiency_residual: score.yes_alpha - allocated,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn tree_shap(
        lineage: AttributionLineage,
        model_version_id: ModelVersionId,
        recommendation_id: RecommendationId,
        model_artifact_hash: ContentHash,
        spec: &TreeEnsembleSpec,
        input: &TreeEnsembleInput,
    ) -> QuantResult<Self> {
        let explanation = TreeShapExplainer::explain(spec, input)?;
        let artifact = Self {
            lineage,
            model_version_id,
            recommendation_id,
            model_artifact_hash,
            input_contract_hash: spec.input_contract_hash,
            output_kind: PredictionOutputKind::ClassicalRawPrediction,
            method: PredictionExplanationMethod::ExactTreeShap,
            baseline_output: explanation.baseline_output,
            predicted_output: explanation.predicted_output,
            contributions: explanation.contributions,
            efficiency_residual: explanation.efficiency_residual,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        validate_contributions(&self.contributions)?;
        let allocated = self.baseline_output
            + self
                .contributions
                .iter()
                .map(|term| term.contribution)
                .sum::<Decimal>();
        let residual = self.predicted_output - allocated;
        if residual != self.efficiency_residual || residual.abs() > MAX_EFFICIENCY_RESIDUAL {
            return Err(invalid_method(format!(
                "prediction explanation violates efficiency: residual {residual}"
            )));
        }
        Ok(())
    }
}

/// One admissible intervention and the dependency closure it invalidates.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CounterfactualIntervention {
    pub input_name: String,
    pub observed_value: Decimal,
    pub intervened_value: Decimal,
    pub affected_nodes: Vec<String>,
}

/// Candidate score used by deterministic rank/TopN replay.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionCandidateKey {
    pub market_id: MarketId,
    pub token_id: TokenId,
}

/// Candidate score used by deterministic rank/TopN replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionCandidateScore {
    pub key: DecisionCandidateKey,
    pub score: Decimal,
    pub confidence: Decimal,
}

/// Versioned rank and selection policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplayPolicy {
    pub policy_hash: ContentHash,
    pub admissible_intervention_policy_hash: ContentHash,
    pub dependency_graph_hash: ContentHash,
    pub candidate_universe_hash: ContentHash,
    pub candidate_score_floor: Decimal,
    pub minimum_confidence: Decimal,
    pub top_n: u32,
}

impl DecisionReplayPolicy {
    #[must_use]
    pub const fn affected_nodes() -> &'static [&'static str] {
        &DECISION_DEPENDENCY_NODES
    }

    pub fn try_new(
        policy_hash: ContentHash,
        universe: &[DecisionCandidateScore],
        candidate_score_floor: Decimal,
        minimum_confidence: Decimal,
        top_n: u32,
    ) -> QuantResult<Self> {
        let mut canonical_universe = universe.to_vec();
        canonical_universe.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        if canonical_universe.is_empty()
            || canonical_universe
                .windows(2)
                .any(|pair| pair[0].key == pair[1].key)
        {
            return Err(invalid_method(
                "decision replay universe must be non-empty with unique candidate keys",
            ));
        }
        let policy = Self {
            policy_hash,
            admissible_intervention_policy_hash: CanonicalDigest::content_hash_typed(
                DECISION_INTERVENTION_DOMAIN,
                DECISION_VERSION,
                &DECISION_INTERVENTION_POLICY,
            )?,
            dependency_graph_hash: CanonicalDigest::content_hash_typed(
                DECISION_DEPENDENCY_DOMAIN,
                DECISION_VERSION,
                &DECISION_DEPENDENCY_NODES,
            )?,
            candidate_universe_hash: CanonicalDigest::content_hash_typed(
                DECISION_UNIVERSE_DOMAIN,
                DECISION_VERSION,
                &canonical_universe,
            )?,
            candidate_score_floor,
            minimum_confidence,
            top_n,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> QuantResult<()> {
        if self.top_n == 0
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.candidate_score_floor)
            || !(Decimal::ZERO..=Decimal::ONE).contains(&self.minimum_confidence)
        {
            return Err(invalid_method(
                "decision counterfactual floors must be in [0, 1] and top_n must be positive",
            ));
        }
        Ok(())
    }
}

/// Scope of the deterministic replay. This is not the portfolio allocator,
/// execution policy, or a real-world causal intervention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReplayScope {
    AdmittedModelRankTopN,
}

/// Result of replaying score, rank, `TopN`, and decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplay {
    pub score: Decimal,
    pub rank: u32,
    pub model_top_n_selected: bool,
}

/// Immutable model/policy counterfactual. It is deliberately not named or
/// represented as a causal effect on the real world.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionCounterfactualArtifact {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub target_key: DecisionCandidateKey,
    pub prediction_explanation_hash: ContentHash,
    pub scope: DecisionReplayScope,
    pub policy: DecisionReplayPolicy,
    pub interventions: Vec<CounterfactualIntervention>,
    pub baseline: DecisionReplay,
    pub counterfactual: DecisionReplay,
}

/// Complete input to one deterministic decision counterfactual replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionCounterfactualInput {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub target_key: DecisionCandidateKey,
    pub prediction_explanation_hash: ContentHash,
    pub policy: DecisionReplayPolicy,
    pub interventions: Vec<CounterfactualIntervention>,
    pub baseline_score: Decimal,
    pub counterfactual_score: Decimal,
    pub target_confidence: Decimal,
    pub peer_scores: Vec<DecisionCandidateScore>,
}

impl DecisionCounterfactualArtifact {
    pub fn replay(input: DecisionCounterfactualInput) -> QuantResult<Self> {
        input.policy.validate()?;
        let mut universe = input.peer_scores.clone();
        universe.push(DecisionCandidateScore {
            key: input.target_key.clone(),
            score: input.baseline_score,
            confidence: input.target_confidence,
        });
        universe.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        let expected_universe = CanonicalDigest::content_hash_typed(
            DECISION_UNIVERSE_DOMAIN,
            DECISION_VERSION,
            &universe,
        )?;
        let expected_intervention = CanonicalDigest::content_hash_typed(
            DECISION_INTERVENTION_DOMAIN,
            DECISION_VERSION,
            &DECISION_INTERVENTION_POLICY,
        )?;
        let expected_dependency = CanonicalDigest::content_hash_typed(
            DECISION_DEPENDENCY_DOMAIN,
            DECISION_VERSION,
            &DECISION_DEPENDENCY_NODES,
        )?;
        let required_lineage = [
            input.prediction_explanation_hash,
            input.policy.policy_hash,
            input.policy.admissible_intervention_policy_hash,
            input.policy.dependency_graph_hash,
            input.policy.candidate_universe_hash,
        ];
        if input.policy.candidate_universe_hash != expected_universe
            || input.policy.admissible_intervention_policy_hash != expected_intervention
            || input.policy.dependency_graph_hash != expected_dependency
            || required_lineage.iter().any(|hash| {
                input
                    .lineage
                    .source_evidence_hashes
                    .binary_search(hash)
                    .is_err()
            })
        {
            return Err(invalid_method(
                "decision replay policy, universe, dependency graph, or lineage is inconsistent",
            ));
        }
        let baseline = replay_decision(
            &input.target_key,
            input.baseline_score,
            input.target_confidence,
            &input.peer_scores,
            &input.policy,
        )?;
        let counterfactual = replay_decision(
            &input.target_key,
            input.counterfactual_score,
            input.target_confidence,
            &input.peer_scores,
            &input.policy,
        )?;
        let artifact = Self {
            lineage: input.lineage,
            model_version_id: input.model_version_id,
            recommendation_id: input.recommendation_id,
            target_key: input.target_key,
            prediction_explanation_hash: input.prediction_explanation_hash,
            scope: DecisionReplayScope::AdmittedModelRankTopN,
            policy: input.policy,
            interventions: input.interventions,
            baseline,
            counterfactual,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        self.policy.validate()?;
        if self.scope != DecisionReplayScope::AdmittedModelRankTopN || self.interventions.is_empty()
        {
            return Err(invalid_method(
                "decision counterfactual requires an admissible intervention",
            ));
        }
        let mut names = BTreeSet::new();
        for intervention in &self.interventions {
            if intervention.input_name.trim().is_empty()
                || intervention.observed_value == intervention.intervened_value
                || intervention.affected_nodes.is_empty()
                || !strictly_sorted(&intervention.affected_nodes)
                || !names.insert(intervention.input_name.as_str())
            {
                return Err(invalid_method(
                    "counterfactual interventions must be unique, material, and dependency-complete",
                ));
            }
        }
        Ok(())
    }
}

/// Statistical interpretation is fixed to non-causal conditional association.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssociationInterpretation {
    ConditionalNonCausal,
}

/// Outcome variable used by one association analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutcomeAssociationTarget {
    FinalTokenPayoutRatio,
}

/// One uncertainty-bearing association estimate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssociationEstimate {
    pub input_name: String,
    pub estimate: Decimal,
    pub standard_error: Decimal,
    pub confidence_level: Decimal,
    pub confidence_interval_low: Decimal,
    pub confidence_interval_high: Decimal,
    pub sample_count: u64,
}

/// Cohort-level association between model explanations and final outcome truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OutcomeAssociationArtifact {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub interpretation: AssociationInterpretation,
    pub target: OutcomeAssociationTarget,
    pub estimator_contract_hash: ContentHash,
    pub conditioning_policy_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub explanation_set_hash: ContentHash,
    pub resolution_set_hash: ContentHash,
    pub execution_rollup_set_hash: ContentHash,
    pub included_recommendation_count: u64,
    pub estimates: Vec<AssociationEstimate>,
}

/// One mature recommendation admitted to a conditional association estimate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeAssociationSample {
    pub recommendation_id: RecommendationId,
    pub explanation_hash: ContentHash,
    pub outcome_hash: ContentHash,
    pub outcome: Decimal,
    pub contributions: Vec<PredictionContribution>,
}

/// Complete immutable preimage for one cohort-level association artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutcomeAssociationInput {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub target: OutcomeAssociationTarget,
    pub estimator_contract_hash: ContentHash,
    pub conditioning_policy_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub explanation_set_hash: ContentHash,
    pub resolution_set_hash: ContentHash,
    pub execution_rollup_set_hash: ContentHash,
    pub samples: Vec<OutcomeAssociationSample>,
}

impl OutcomeAssociationInput {
    fn fit_estimates(&self, input_names: Vec<String>) -> QuantResult<Vec<AssociationEstimate>> {
        let sample_count = u64::try_from(self.samples.len())
            .map_err(|error| invalid_method(format!("sample count overflow: {error}")))?;
        let count = Decimal::from(sample_count);
        let degrees_of_freedom = count - Decimal::from(2_u32);
        let confidence_level = Decimal::new(95, 2);
        let z = Decimal::new(1_959_963_984_540_054_i64, 15);
        let mut estimates = Vec::with_capacity(input_names.len());
        for (index, input_name) in input_names.into_iter().enumerate() {
            let x_mean = self
                .samples
                .iter()
                .map(|sample| sample.contributions[index].contribution)
                .sum::<Decimal>()
                / count;
            let y_mean = self
                .samples
                .iter()
                .map(|sample| sample.outcome)
                .sum::<Decimal>()
                / count;
            let sxx = self
                .samples
                .iter()
                .map(|sample| {
                    let centered = sample.contributions[index].contribution - x_mean;
                    centered * centered
                })
                .sum::<Decimal>();
            if sxx.is_zero() {
                continue;
            }
            let sxy = self
                .samples
                .iter()
                .map(|sample| {
                    (sample.contributions[index].contribution - x_mean) * (sample.outcome - y_mean)
                })
                .sum::<Decimal>();
            let estimate = sxy / sxx;
            let squared_error = self
                .samples
                .iter()
                .map(|sample| {
                    let fitted =
                        y_mean + estimate * (sample.contributions[index].contribution - x_mean);
                    let residual = sample.outcome - fitted;
                    residual * residual
                })
                .sum::<Decimal>();
            let variance = squared_error / degrees_of_freedom / sxx;
            let standard_error = variance.sqrt().ok_or_else(|| {
                invalid_method(format!(
                    "outcome association `{input_name}` has an invalid variance"
                ))
            })?;
            let margin = z * standard_error;
            estimates.push(AssociationEstimate {
                input_name,
                estimate,
                standard_error,
                confidence_level,
                confidence_interval_low: estimate - margin,
                confidence_interval_high: estimate + margin,
                sample_count,
            });
        }
        Ok(estimates)
    }
}

impl OutcomeAssociationArtifact {
    /// Fit independent cohort-conditional OLS slopes with classical 95%
    /// uncertainty. The result is descriptive association only; no feature
    /// contribution is multiplied by `PnL` and no causal effect is asserted.
    pub fn estimate(mut input: OutcomeAssociationInput) -> QuantResult<Self> {
        if input.samples.len() < 3 {
            return Err(invalid_method(
                "outcome association requires at least three mature recommendations",
            ));
        }
        input
            .samples
            .sort_by_key(|sample| sample.recommendation_id.as_uuid());
        if input
            .samples
            .windows(2)
            .any(|pair| pair[0].recommendation_id == pair[1].recommendation_id)
        {
            return Err(invalid_method(
                "outcome association contains duplicate recommendations",
            ));
        }
        let recommendation_ids = input
            .samples
            .iter()
            .map(|sample| sample.recommendation_id)
            .collect::<Vec<_>>();
        let explanation_hashes = input
            .samples
            .iter()
            .map(|sample| sample.explanation_hash)
            .collect::<Vec<_>>();
        let resolution_hashes = input
            .samples
            .iter()
            .map(|sample| sample.outcome_hash)
            .collect::<Vec<_>>();
        let expected_estimator = CanonicalDigest::content_hash_typed(
            ASSOCIATION_ESTIMATOR_DOMAIN,
            ASSOCIATION_VERSION,
            &"univariate_ols_classical_95pct_noncausal",
        )?;
        let expected_cohort = CanonicalDigest::content_hash_typed(
            ASSOCIATION_COHORT_DOMAIN,
            ASSOCIATION_VERSION,
            &recommendation_ids,
        )?;
        let expected_explanations = CanonicalDigest::content_hash_typed(
            ASSOCIATION_EXPLANATIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &explanation_hashes,
        )?;
        let expected_resolutions = CanonicalDigest::content_hash_typed(
            ASSOCIATION_RESOLUTIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &resolution_hashes,
        )?;
        let expected_executions = CanonicalDigest::content_hash_typed(
            ASSOCIATION_EXECUTIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &Vec::<ContentHash>::new(),
        )?;
        let required_lineage = [
            expected_cohort,
            expected_explanations,
            expected_resolutions,
            expected_executions,
        ];
        if input.estimator_contract_hash != expected_estimator
            || input.cohort_manifest_hash != expected_cohort
            || input.explanation_set_hash != expected_explanations
            || input.resolution_set_hash != expected_resolutions
            || input.execution_rollup_set_hash != expected_executions
            || required_lineage.iter().any(|hash| {
                input
                    .lineage
                    .source_evidence_hashes
                    .binary_search(hash)
                    .is_err()
            })
        {
            return Err(invalid_method(
                "outcome association set hashes or lineage differ from its samples",
            ));
        }
        let input_names = input
            .samples
            .first()
            .ok_or_else(|| invalid_method("outcome association sample set is empty"))?
            .contributions
            .iter()
            .map(|contribution| contribution.input_name.clone())
            .collect::<Vec<_>>();
        if input_names.is_empty()
            || input.samples.iter().any(|sample| {
                validate_contributions(&sample.contributions).is_err()
                    || sample
                        .contributions
                        .iter()
                        .map(|contribution| &contribution.input_name)
                        .ne(input_names.iter())
            })
        {
            return Err(invalid_method(
                "outcome association explanations do not share one canonical input plane",
            ));
        }
        let estimates = input.fit_estimates(input_names)?;
        let artifact = Self {
            lineage: input.lineage,
            model_version_id: input.model_version_id,
            interpretation: AssociationInterpretation::ConditionalNonCausal,
            target: input.target,
            estimator_contract_hash: input.estimator_contract_hash,
            conditioning_policy_hash: input.conditioning_policy_hash,
            cohort_manifest_hash: input.cohort_manifest_hash,
            explanation_set_hash: input.explanation_set_hash,
            resolution_set_hash: input.resolution_set_hash,
            execution_rollup_set_hash: input.execution_rollup_set_hash,
            included_recommendation_count: u64::try_from(input.samples.len())
                .map_err(|error| invalid_method(format!("sample count overflow: {error}")))?,
            estimates,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        if self.interpretation != AssociationInterpretation::ConditionalNonCausal
            || self.included_recommendation_count < 3
            || self.estimates.is_empty()
        {
            return Err(invalid_method(
                "outcome association requires a multi-observation cohort and non-causal semantics",
            ));
        }
        let mut names = BTreeSet::new();
        for estimate in &self.estimates {
            if estimate.input_name.trim().is_empty()
                || !names.insert(estimate.input_name.as_str())
                || estimate.sample_count != self.included_recommendation_count
                || estimate.standard_error < Decimal::ZERO
                || !(Decimal::ZERO..Decimal::ONE).contains(&estimate.confidence_level)
                || estimate.confidence_interval_low > estimate.estimate
                || estimate.confidence_interval_high < estimate.estimate
            {
                return Err(invalid_method(
                    "outcome association estimate has invalid uncertainty or cohort binding",
                ));
            }
        }
        Ok(())
    }
}

/// One PIT executable-price observation used by an attempt trajectory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryPoint {
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub executable_exit_price: Price,
    pub source_fact_hash: ContentHash,
}

/// Immutable per-attempt trajectory. MAE/MFE are derived here and never
/// backfilled into the WORM execution-attempt outcome.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionTrajectoryArtifact {
    pub lineage: AttributionLineage,
    pub recommendation_id: RecommendationId,
    pub order_intent_id: OrderIntentId,
    pub attempt_outcome_hash: ContentHash,
    pub pit_book_contract_hash: ContentHash,
    pub entry_at: DateTime<Utc>,
    pub entry_price: Price,
    pub horizon_end: DateTime<Utc>,
    pub points: Vec<TrajectoryPoint>,
    pub max_adverse_excursion_bps: Bps,
    pub max_favorable_excursion_bps: Bps,
}

/// Complete immutable source binding for one attempt trajectory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionTrajectoryInput {
    pub lineage: AttributionLineage,
    pub recommendation_id: RecommendationId,
    pub order_intent_id: OrderIntentId,
    pub attempt_outcome_hash: ContentHash,
    pub pit_book_contract_hash: ContentHash,
    pub entry_at: DateTime<Utc>,
    pub entry_price: Price,
    pub horizon_end: DateTime<Utc>,
    pub points: Vec<TrajectoryPoint>,
}

impl ExecutionTrajectoryArtifact {
    pub fn try_new(input: ExecutionTrajectoryInput) -> QuantResult<Self> {
        if input.entry_price <= Price::ZERO {
            return Err(invalid_method(
                "execution trajectory requires a positive entry price",
            ));
        }
        let (max_adverse_excursion_bps, max_favorable_excursion_bps) =
            trajectory_excursions(input.entry_price, &input.points)?;
        let artifact = Self {
            lineage: input.lineage,
            recommendation_id: input.recommendation_id,
            order_intent_id: input.order_intent_id,
            attempt_outcome_hash: input.attempt_outcome_hash,
            pit_book_contract_hash: input.pit_book_contract_hash,
            entry_at: input.entry_at,
            entry_price: input.entry_price,
            horizon_end: input.horizon_end,
            points: input.points,
            max_adverse_excursion_bps,
            max_favorable_excursion_bps,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        if self.entry_at >= self.horizon_end
            || self.horizon_end > self.lineage.source_cutoff
            || self.entry_price <= Price::ZERO
            || self.points.is_empty()
        {
            return Err(leakage(
                "execution trajectory window is empty, inverted, or not mature by cutoff",
            ));
        }
        let mut prior = None;
        let mut hashes = BTreeSet::new();
        let entry_at = self.entry_at;
        let horizon_end = self.horizon_end;
        let source_cutoff = self.lineage.source_cutoff;
        for point in &self.points {
            if point.observed_at < entry_at
                || point.observed_at > horizon_end
                || point.available_at < point.observed_at
                || point.available_at > source_cutoff
                || !(Price::ZERO..=Price::ONE).contains(&point.executable_exit_price)
                || prior.is_some_and(|timestamp| timestamp >= point.observed_at)
                || !hashes.insert(point.source_fact_hash)
            {
                return Err(leakage(
                    "trajectory points must be unique, ordered PIT facts inside the mature horizon",
                ));
            }
            prior = Some(point.observed_at);
        }
        let (adverse, favorable) = trajectory_excursions(self.entry_price, &self.points)?;
        if adverse != self.max_adverse_excursion_bps
            || favorable != self.max_favorable_excursion_bps
        {
            return Err(invalid_method(
                "stored trajectory excursions differ from the PIT replay",
            ));
        }
        Ok(())
    }
}

/// Explicit alternative exit/barrier policy used for a counterfactual outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum AlternativeExitPolicy {
    LatestExecutableAtOrBeforeHorizon,
    FirstBarrier {
        take_profit_bps: Bps,
        stop_loss_bps: Bps,
        max_holding_secs: u64,
    },
}

impl AlternativeExitPolicy {
    fn validate(self) -> QuantResult<()> {
        if let Self::FirstBarrier {
            take_profit_bps,
            stop_loss_bps,
            max_holding_secs,
        } = self
            && (take_profit_bps <= Bps::ZERO || stop_loss_bps >= Bps::ZERO || max_holding_secs == 0)
        {
            return Err(invalid_method(
                "first-barrier policy requires positive take-profit, negative stop-loss, and duration",
            ));
        }
        Ok(())
    }
}

/// Alternative-policy replay bound to one immutable attempt trajectory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyCounterfactualOutcome {
    pub lineage: AttributionLineage,
    pub recommendation_id: RecommendationId,
    pub order_intent_id: OrderIntentId,
    pub trajectory_artifact_hash: ContentHash,
    pub alternative_policy_hash: ContentHash,
    pub alternative_policy: AlternativeExitPolicy,
    pub counterfactual_exit_at: DateTime<Utc>,
    pub counterfactual_exit_price: Price,
    pub counterfactual_gross_return_bps: Bps,
    pub baseline_realized_return_bps: Option<Bps>,
    pub gross_return_delta_bps: Option<Bps>,
}

impl PolicyCounterfactualOutcome {
    pub fn replay(
        trajectory: &ExecutionTrajectoryArtifact,
        trajectory_artifact_hash: ContentHash,
        alternative_policy_hash: ContentHash,
        alternative_policy: AlternativeExitPolicy,
        baseline_realized_return_bps: Option<Bps>,
    ) -> QuantResult<Self> {
        trajectory.validate()?;
        alternative_policy.validate()?;
        let selected = match alternative_policy {
            AlternativeExitPolicy::LatestExecutableAtOrBeforeHorizon => trajectory.points.last(),
            AlternativeExitPolicy::FirstBarrier {
                take_profit_bps,
                stop_loss_bps,
                max_holding_secs,
            } => {
                let maximum_at = trajectory.entry_at
                    + Duration::seconds(i64::try_from(max_holding_secs).map_err(|error| {
                        invalid_method(format!("holding period exceeds chrono range: {error}"))
                    })?);
                trajectory
                    .points
                    .iter()
                    .find(|point| {
                        let excursion = Bps::relative(
                            point.executable_exit_price.inner() - trajectory.entry_price.inner(),
                            trajectory.entry_price.inner(),
                        );
                        point.observed_at >= maximum_at
                            || excursion.is_some_and(|value| {
                                value >= take_profit_bps || value <= stop_loss_bps
                            })
                    })
                    .or_else(|| trajectory.points.last())
            }
        }
        .ok_or_else(|| invalid_method("counterfactual trajectory has no exit observation"))?;
        let counterfactual_gross_return_bps = Bps::relative(
            selected.executable_exit_price.inner() - trajectory.entry_price.inner(),
            trajectory.entry_price.inner(),
        )
        .ok_or_else(|| invalid_method("counterfactual return denominator is zero"))?;
        let gross_return_delta_bps =
            baseline_realized_return_bps.map(|baseline| counterfactual_gross_return_bps - baseline);
        let artifact = Self {
            lineage: trajectory.lineage.clone(),
            recommendation_id: trajectory.recommendation_id,
            order_intent_id: trajectory.order_intent_id,
            trajectory_artifact_hash,
            alternative_policy_hash,
            alternative_policy,
            counterfactual_exit_at: selected.observed_at,
            counterfactual_exit_price: selected.executable_exit_price,
            counterfactual_gross_return_bps,
            baseline_realized_return_bps,
            gross_return_delta_bps,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        self.alternative_policy.validate()?;
        if self.counterfactual_exit_at > self.lineage.source_cutoff
            || !(Price::ZERO..=Price::ONE).contains(&self.counterfactual_exit_price)
            || self.gross_return_delta_bps
                != self
                    .baseline_realized_return_bps
                    .map(|baseline| self.counterfactual_gross_return_bps - baseline)
        {
            return Err(invalid_method(
                "policy counterfactual result is inconsistent with its baseline or cutoff",
            ));
        }
        Ok(())
    }
}

/// Closed payload vocabulary persisted under the attribution artifact index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "artifact_kind", content = "artifact")]
pub enum AttributionArtifact {
    PredictionExplanation(Box<PredictionExplanationArtifact>),
    DecisionCounterfactual(Box<DecisionCounterfactualArtifact>),
    OutcomeAssociation(Box<OutcomeAssociationArtifact>),
    ExecutionTrajectory(Box<ExecutionTrajectoryArtifact>),
    PolicyCounterfactualOutcome(Box<PolicyCounterfactualOutcome>),
}

impl AttributionArtifact {
    pub fn validate(&self) -> QuantResult<()> {
        match self {
            Self::PredictionExplanation(artifact) => artifact.validate(),
            Self::DecisionCounterfactual(artifact) => artifact.validate(),
            Self::OutcomeAssociation(artifact) => artifact.validate(),
            Self::ExecutionTrajectory(artifact) => artifact.validate(),
            Self::PolicyCounterfactualOutcome(artifact) => artifact.validate(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> AttributionArtifactKind {
        match self {
            Self::PredictionExplanation(_) => AttributionArtifactKind::PredictionExplanation,
            Self::DecisionCounterfactual(_) => AttributionArtifactKind::DecisionCounterfactual,
            Self::OutcomeAssociation(_) => AttributionArtifactKind::OutcomeAssociation,
            Self::ExecutionTrajectory(_) => AttributionArtifactKind::ExecutionTrajectory,
            Self::PolicyCounterfactualOutcome(_) => {
                AttributionArtifactKind::PolicyCounterfactualOutcome
            }
        }
    }

    #[must_use]
    pub const fn lineage(&self) -> &AttributionLineage {
        match self {
            Self::PredictionExplanation(artifact) => &artifact.lineage,
            Self::DecisionCounterfactual(artifact) => &artifact.lineage,
            Self::OutcomeAssociation(artifact) => &artifact.lineage,
            Self::ExecutionTrajectory(artifact) => &artifact.lineage,
            Self::PolicyCounterfactualOutcome(artifact) => &artifact.lineage,
        }
    }

    #[must_use]
    pub const fn subject(&self) -> AttributionSubject {
        match self {
            Self::PredictionExplanation(artifact) => AttributionSubject::Prediction {
                model_version_id: artifact.model_version_id,
                recommendation_id: artifact.recommendation_id,
            },
            Self::DecisionCounterfactual(artifact) => AttributionSubject::Decision {
                model_version_id: artifact.model_version_id,
                recommendation_id: artifact.recommendation_id,
            },
            Self::OutcomeAssociation(artifact) => AttributionSubject::Outcome {
                model_version_id: artifact.model_version_id,
            },
            Self::ExecutionTrajectory(artifact) => AttributionSubject::Execution {
                recommendation_id: artifact.recommendation_id,
                order_intent_id: artifact.order_intent_id,
            },
            Self::PolicyCounterfactualOutcome(artifact) => {
                AttributionSubject::PolicyCounterfactual {
                    recommendation_id: artifact.recommendation_id,
                    order_intent_id: artifact.order_intent_id,
                }
            }
        }
    }
}

/// Canonical JSON codec. Decoding rejects semantically valid but non-canonical
/// bytes so a content address has one representation.
pub struct AttributionArtifactCodec;

impl AttributionArtifactCodec {
    pub fn encode(artifact: &AttributionArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<AttributionArtifact> {
        let artifact = serde_json::from_slice::<AttributionArtifact>(bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("decode attribution artifact: {error}"),
            }
        })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(ResearchError::Serialization {
                detail: "attribution artifact is not canonical JSON".to_owned(),
            }
            .into());
        }
        Ok(artifact)
    }

    #[must_use]
    pub fn hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }
}

fn replay_decision(
    target_key: &DecisionCandidateKey,
    score: Decimal,
    confidence: Decimal,
    peer_scores: &[DecisionCandidateScore],
    policy: &DecisionReplayPolicy,
) -> QuantResult<DecisionReplay> {
    policy.validate()?;
    if !(Decimal::ZERO..=Decimal::ONE).contains(&score)
        || !(Decimal::ZERO..=Decimal::ONE).contains(&confidence)
        || peer_scores.iter().any(|candidate| {
            !(Decimal::ZERO..=Decimal::ONE).contains(&candidate.score)
                || !(Decimal::ZERO..=Decimal::ONE).contains(&candidate.confidence)
        })
    {
        return Err(invalid_method(
            "decision replay scores and confidence must remain in [0, 1]",
        ));
    }
    if peer_scores
        .iter()
        .any(|candidate| &candidate.key == target_key)
    {
        return Err(invalid_method(
            "decision replay peers contain the target candidate",
        ));
    }
    let unique = peer_scores
        .iter()
        .map(|candidate| &candidate.key)
        .collect::<BTreeSet<_>>();
    if unique.len() != peer_scores.len() {
        return Err(invalid_method(
            "decision replay peer candidates are duplicated",
        ));
    }
    let mut candidates = peer_scores
        .iter()
        .filter(|candidate| {
            candidate.score >= policy.candidate_score_floor
                && candidate.confidence >= policy.minimum_confidence
        })
        .cloned()
        .collect::<Vec<_>>();
    candidates.push(DecisionCandidateScore {
        key: target_key.clone(),
        score,
        confidence,
    });
    candidates.sort_by(|left, right| {
        right
            .score
            .cmp(&left.score)
            .then_with(|| left.key.cmp(&right.key))
    });
    let position = candidates
        .iter()
        .position(|candidate| &candidate.key == target_key)
        .ok_or_else(|| invalid_method("decision replay lost the target recommendation"))?;
    let rank = u32::try_from(position + 1)
        .map_err(|error| invalid_method(format!("decision replay rank overflow: {error}")))?;
    Ok(DecisionReplay {
        score,
        rank,
        model_top_n_selected: score >= policy.candidate_score_floor
            && confidence >= policy.minimum_confidence
            && rank <= policy.top_n,
    })
}

fn validate_contributions(contributions: &[PredictionContribution]) -> QuantResult<()> {
    if contributions.is_empty() {
        return Err(invalid("prediction explanation has no contributions"));
    }
    let mut prior = None;
    for contribution in contributions {
        if contribution.input_name.trim().is_empty()
            || prior.is_some_and(|name: &str| name >= contribution.input_name.as_str())
        {
            return Err(invalid(
                "prediction contributions must be uniquely sorted by input name",
            ));
        }
        prior = Some(contribution.input_name.as_str());
    }
    Ok(())
}

fn trajectory_excursions(
    entry_price: Price,
    points: &[TrajectoryPoint],
) -> QuantResult<(Bps, Bps)> {
    let excursions = points
        .iter()
        .map(|point| {
            Bps::relative(
                point.executable_exit_price.inner() - entry_price.inner(),
                entry_price.inner(),
            )
            .ok_or_else(|| invalid_method("trajectory excursion denominator is zero"))
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let adverse = excursions
        .iter()
        .copied()
        .min()
        .unwrap_or(Bps::ZERO)
        .min(Bps::ZERO);
    let favorable = excursions
        .iter()
        .copied()
        .max()
        .unwrap_or(Bps::ZERO)
        .max(Bps::ZERO);
    Ok((adverse, favorable))
}

fn strictly_sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::AttributionCohort,
        hashing::CanonicalDigest,
        types::{
            Bps, ContentHash, FeedbackCycleId, MarketId, ModelVersionId, OrderIntentId, Price,
            RecommendationId, TokenId,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        ASSOCIATION_COHORT_DOMAIN, ASSOCIATION_ESTIMATOR_DOMAIN, ASSOCIATION_EXECUTIONS_DOMAIN,
        ASSOCIATION_EXPLANATIONS_DOMAIN, ASSOCIATION_RESOLUTIONS_DOMAIN, ASSOCIATION_VERSION,
        AlternativeExitPolicy, AssociationInterpretation, AttributionArtifact,
        AttributionArtifactCodec, AttributionLineage, CounterfactualIntervention,
        DecisionCandidateKey, DecisionCandidateScore, DecisionCounterfactualArtifact,
        DecisionCounterfactualInput, DecisionReplayPolicy, ExecutionTrajectoryArtifact,
        ExecutionTrajectoryInput, OutcomeAssociationArtifact, OutcomeAssociationInput,
        OutcomeAssociationSample, OutcomeAssociationTarget, PolicyCounterfactualOutcome,
        PredictionContribution, PredictionExplanationArtifact, PredictionOutputKind,
        TrajectoryPoint, WeightedExplanationInput, WeightedTerm,
    };

    fn hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("fixture hash")
    }

    impl AttributionLineage {
        fn fixture() -> Self {
            let cutoff = Utc
                .with_ymd_and_hms(2026, 7, 30, 12, 0, 0)
                .single()
                .expect("fixture cutoff");
            Self::try_new(
                FeedbackCycleId::from_v7(),
                AttributionCohort::Evaluation,
                cutoff,
                cutoff + Duration::seconds(1),
                vec![hash("evidence")],
            )
            .expect("valid lineage")
        }
    }

    #[test]
    fn weighted_efficiency_is_exact() {
        let explanation = PredictionExplanationArtifact::weighted(
            AttributionLineage::fixture(),
            WeightedExplanationInput {
                model_version_id: ModelVersionId::from_v7(),
                recommendation_id: RecommendationId::from_v7(),
                model_artifact_hash: hash("model"),
                input_contract_hash: hash("input"),
                output_kind: PredictionOutputKind::CanonicalYesAlpha,
                intercept: dec!(0.1),
                terms: vec![
                    WeightedTerm {
                        input_name: "momentum".to_owned(),
                        encoded_value: dec!(0.5),
                        weight: dec!(0.4),
                    },
                    WeightedTerm {
                        input_name: "liquidity".to_owned(),
                        encoded_value: dec!(0.2),
                        weight: dec!(-0.25),
                    },
                ],
            },
        )
        .expect("exact weighted explanation");
        assert_eq!(explanation.predicted_output, dec!(0.25));
        assert_eq!(explanation.efficiency_residual, Decimal::ZERO);

        let payload = AttributionArtifact::PredictionExplanation(Box::new(explanation));
        let bytes = AttributionArtifactCodec::encode(&payload).expect("encode");
        assert_eq!(
            AttributionArtifactCodec::decode(&bytes).expect("decode"),
            payload
        );
    }

    #[test]
    fn association_is_noncausal_uncertain() {
        let mut samples = [
            (dec!(-0.2), dec!(0)),
            (dec!(0), dec!(0.5)),
            (dec!(0.2), dec!(1)),
        ]
        .into_iter()
        .map(|(contribution, outcome)| OutcomeAssociationSample {
            recommendation_id: RecommendationId::from_v7(),
            explanation_hash: hash(&format!("explanation-{contribution}")),
            outcome_hash: hash(&format!("outcome-{outcome}")),
            outcome,
            contributions: vec![PredictionContribution {
                input_name: "canonical_alpha".to_owned(),
                input_value: Some(contribution),
                contribution,
            }],
        })
        .collect::<Vec<_>>();
        samples.sort_by_key(|sample| sample.recommendation_id.as_uuid());
        let recommendation_ids = samples
            .iter()
            .map(|sample| sample.recommendation_id)
            .collect::<Vec<_>>();
        let explanation_hashes = samples
            .iter()
            .map(|sample| sample.explanation_hash)
            .collect::<Vec<_>>();
        let resolution_hashes = samples
            .iter()
            .map(|sample| sample.outcome_hash)
            .collect::<Vec<_>>();
        let estimator_contract_hash = CanonicalDigest::content_hash_typed(
            ASSOCIATION_ESTIMATOR_DOMAIN,
            ASSOCIATION_VERSION,
            &"univariate_ols_classical_95pct_noncausal",
        )
        .expect("estimator hash");
        let cohort_manifest_hash = CanonicalDigest::content_hash_typed(
            ASSOCIATION_COHORT_DOMAIN,
            ASSOCIATION_VERSION,
            &recommendation_ids,
        )
        .expect("cohort hash");
        let explanation_set_hash = CanonicalDigest::content_hash_typed(
            ASSOCIATION_EXPLANATIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &explanation_hashes,
        )
        .expect("explanation set");
        let resolution_set_hash = CanonicalDigest::content_hash_typed(
            ASSOCIATION_RESOLUTIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &resolution_hashes,
        )
        .expect("resolution set");
        let execution_rollup_set_hash = CanonicalDigest::content_hash_typed(
            ASSOCIATION_EXECUTIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &Vec::<ContentHash>::new(),
        )
        .expect("execution set");
        let base = AttributionLineage::fixture();
        let lineage = AttributionLineage::try_new(
            base.source_feedback_cycle_id,
            base.source_cohort,
            base.source_cutoff,
            base.generated_at,
            vec![
                cohort_manifest_hash,
                explanation_set_hash,
                resolution_set_hash,
                execution_rollup_set_hash,
            ],
        )
        .expect("association lineage");
        let artifact = OutcomeAssociationArtifact::estimate(OutcomeAssociationInput {
            lineage,
            model_version_id: ModelVersionId::from_v7(),
            target: OutcomeAssociationTarget::FinalTokenPayoutRatio,
            estimator_contract_hash,
            conditioning_policy_hash: hash("conditioning"),
            cohort_manifest_hash,
            explanation_set_hash,
            resolution_set_hash,
            execution_rollup_set_hash,
            samples,
        })
        .expect("association");
        assert_eq!(
            artifact.interpretation,
            AssociationInterpretation::ConditionalNonCausal
        );
        assert_eq!(artifact.estimates[0].estimate, dec!(2.5));
        assert_eq!(artifact.estimates[0].standard_error, Decimal::ZERO);
    }

    #[test]
    fn decision_replay_is_scoped() {
        let target_key = DecisionCandidateKey {
            market_id: MarketId::new("target-market"),
            token_id: TokenId::new("target-token"),
        };
        let peer = DecisionCandidateScore {
            key: DecisionCandidateKey {
                market_id: MarketId::new("peer-market"),
                token_id: TokenId::new("peer-token"),
            },
            score: dec!(0.7),
            confidence: dec!(0.9),
        };
        let mut universe = vec![
            peer.clone(),
            DecisionCandidateScore {
                key: target_key.clone(),
                score: dec!(0.8),
                confidence: dec!(0.9),
            },
        ];
        universe.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        let policy_hash = hash("policy");
        let policy = DecisionReplayPolicy::try_new(policy_hash, &universe, dec!(0.5), dec!(0.5), 1)
            .expect("decision policy");
        let prediction_explanation_hash = hash("prediction");
        let base = AttributionLineage::fixture();
        let lineage = AttributionLineage::try_new(
            base.source_feedback_cycle_id,
            base.source_cohort,
            base.source_cutoff,
            base.generated_at,
            vec![
                prediction_explanation_hash,
                policy_hash,
                policy.admissible_intervention_policy_hash,
                policy.dependency_graph_hash,
                policy.candidate_universe_hash,
            ],
        )
        .expect("decision lineage");
        let artifact = DecisionCounterfactualArtifact::replay(DecisionCounterfactualInput {
            lineage,
            model_version_id: ModelVersionId::from_v7(),
            recommendation_id: RecommendationId::from_v7(),
            target_key,
            prediction_explanation_hash,
            policy,
            interventions: vec![CounterfactualIntervention {
                input_name: "canonical_alpha".to_owned(),
                observed_value: dec!(0.4),
                intervened_value: Decimal::ZERO,
                affected_nodes: vec![
                    "composite_score".to_owned(),
                    "economic_projection".to_owned(),
                    "model_output".to_owned(),
                    "model_rank".to_owned(),
                    "model_top_n".to_owned(),
                ],
            }],
            baseline_score: dec!(0.8),
            counterfactual_score: dec!(0.4),
            target_confidence: dec!(0.9),
            peer_scores: vec![peer],
        })
        .expect("decision replay");
        assert!(artifact.baseline.model_top_n_selected);
        assert!(!artifact.counterfactual.model_top_n_selected);
    }

    #[test]
    fn trajectory_is_derived_only() {
        let lineage = AttributionLineage::fixture();
        let entry_at = lineage.source_cutoff - Duration::hours(2);
        let trajectory = ExecutionTrajectoryArtifact::try_new(ExecutionTrajectoryInput {
            lineage,
            recommendation_id: RecommendationId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            attempt_outcome_hash: hash("attempt"),
            pit_book_contract_hash: hash("pit-book"),
            entry_at,
            entry_price: Price::new(dec!(0.5)),
            horizon_end: entry_at + Duration::hours(1),
            points: vec![
                TrajectoryPoint {
                    observed_at: entry_at + Duration::minutes(10),
                    available_at: entry_at + Duration::minutes(10),
                    executable_exit_price: Price::new(dec!(0.4)),
                    source_fact_hash: hash("point-1"),
                },
                TrajectoryPoint {
                    observed_at: entry_at + Duration::minutes(20),
                    available_at: entry_at + Duration::minutes(20),
                    executable_exit_price: Price::new(dec!(0.65)),
                    source_fact_hash: hash("point-2"),
                },
            ],
        })
        .expect("valid trajectory");
        assert_eq!(trajectory.max_adverse_excursion_bps, Bps::new(dec!(-2000)));
        assert_eq!(trajectory.max_favorable_excursion_bps, Bps::new(dec!(3000)));

        let counterfactual = PolicyCounterfactualOutcome::replay(
            &trajectory,
            hash("trajectory"),
            hash("exit-policy"),
            AlternativeExitPolicy::FirstBarrier {
                take_profit_bps: Bps::new(dec!(2500)),
                stop_loss_bps: Bps::new(dec!(-2500)),
                max_holding_secs: 3600,
            },
            Some(Bps::new(dec!(1000))),
        )
        .expect("policy replay");
        assert_eq!(
            counterfactual.counterfactual_exit_price,
            Price::new(dec!(0.65))
        );
        assert_eq!(
            counterfactual.gross_return_delta_bps,
            Some(Bps::new(dec!(2000)))
        );
    }

    #[test]
    fn rejects_future_trajectory_fact() {
        let lineage = AttributionLineage::fixture();
        let entry_at = lineage.source_cutoff - Duration::hours(1);
        let result = ExecutionTrajectoryArtifact::try_new(ExecutionTrajectoryInput {
            lineage: lineage.clone(),
            recommendation_id: RecommendationId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            attempt_outcome_hash: hash("attempt"),
            pit_book_contract_hash: hash("pit-book"),
            entry_at,
            entry_price: Price::new(dec!(0.5)),
            horizon_end: lineage.source_cutoff,
            points: vec![TrajectoryPoint {
                observed_at: entry_at + Duration::minutes(1),
                available_at: lineage.source_cutoff + Duration::seconds(1),
                executable_exit_price: Price::new(dec!(0.6)),
                source_fact_hash: hash("future"),
            }],
        });
        assert!(result.is_err());
    }
}
