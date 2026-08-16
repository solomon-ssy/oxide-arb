use std::collections::BTreeSet;

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::quant::AttributionSubject,
    enums::quant::{AttributionArtifactKind, AttributionCohort},
    hashing::CanonicalDigest,
    types::{
        Bps, ContentHash, FeedbackCycleId, MarketId, ModelVersionId, OrderIntentId,
        OutcomeTokenBinding, Price, RecommendationId, Shares, TokenId, Usd,
        factor::{FactorAlphaOrientation, FactorOutputSemantics, FactorServingPlane},
    },
};
use rust_decimal::{Decimal, MathematicalOps};
use serde::{Deserialize, Serialize};

use crate::{
    factors::{FactorValue, value::FactorScoringProjection},
    model::factor_heads::{FactorHeadSpec, score_factor_heads},
};

const ATTRIBUTION_SCHEMA_VERSION: u32 = 2;
const MAX_EFFICIENCY_RESIDUAL: Decimal = Decimal::from_parts(1, 0, 0, false, 12);
const ASSOCIATION_ESTIMATOR_DOMAIN: &str = "quant-pivot/attribution-association-estimator";
const ASSOCIATION_COHORT_DOMAIN: &str = "quant-pivot/attribution-association-cohort";
const ASSOCIATION_EXPLANATIONS_DOMAIN: &str = "quant-pivot/attribution-explanation-set";
const ASSOCIATION_RESOLUTIONS_DOMAIN: &str = "quant-pivot/attribution-resolution-set";
const ASSOCIATION_EXECUTIONS_DOMAIN: &str = "quant-pivot/attribution-execution-set";
const ASSOCIATION_VERSION: u32 = 1;
const DECISION_INTERVENTION_DOMAIN: &str = "quant-pivot/decision-intervention-policy";
const DECISION_GRAPH_CONTRACT_DOMAIN: &str = "quant-pivot/decision-computation-graph-contract";
const DECISION_GRAPH_DOMAIN: &str = "quant-pivot/decision-computation-graph";
const DECISION_GRAPH_NODE_DOMAIN: &str = "quant-pivot/decision-computation-node";
const DECISION_VERSION: u32 = 4;
const DECISION_INTERVENTION_POLICY: &str =
    "feature_specific_domain_admissible_route_model_intervention_replay_v4";
const DECISION_GRAPH_CONTRACT: &str =
    "feature_transform_route_model_output_global_economic_reoptimization_boundary_v1";

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

/// Candidate identity used by deterministic rank/TopN replay.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionCandidateKey {
    pub market_id: MarketId,
    pub token_id: TokenId,
}

/// Semantic role of one node in the persisted decision computation graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionGraphNodeKind {
    FeatureInput,
    Transform,
    ModelOutput,
    EconomicReoptimizationBoundary,
}

/// One content-bound node in the exact computation graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionGraphNode {
    pub node_id: String,
    pub kind: DecisionGraphNodeKind,
    pub input_name: Option<String>,
    pub contract_hash: ContentHash,
}

/// One directed dependency edge in the exact computation graph.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionGraphEdge {
    pub from_node_id: String,
    pub to_node_id: String,
}

/// Exact directed path invalidated by one evaluated feature intervention.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionGraphPath {
    pub node_ids: Vec<String>,
}

/// Persisted directed graph from governed feature input to the boundary where
/// calibrated economics and the global MILP must be recomputed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionComputationGraph {
    pub contract_hash: ContentHash,
    pub nodes: Vec<DecisionGraphNode>,
    pub edges: Vec<DecisionGraphEdge>,
    pub graph_hash: ContentHash,
}

#[derive(Serialize)]
struct DecisionGraphNodePreimage<'a> {
    kind: DecisionGraphNodeKind,
    node_id: &'a str,
    binding_hashes: &'a [ContentHash],
}

#[derive(Serialize)]
struct DecisionGraphPreimage<'a> {
    contract_hash: ContentHash,
    nodes: &'a [DecisionGraphNode],
    edges: &'a [DecisionGraphEdge],
}

impl DecisionComputationGraph {
    fn try_new(
        input_names: &[String],
        model_artifact_hash: ContentHash,
        input_contract_hash: ContentHash,
        input_transform_hash: ContentHash,
        policy: &DecisionReplayPolicy,
    ) -> QuantResult<Self> {
        let mut names = input_names.to_vec();
        names.sort();
        if names.is_empty()
            || names.iter().any(|name| name.trim().is_empty())
            || !strictly_sorted(&names)
        {
            return Err(invalid_method(
                "decision computation graph requires non-empty unique feature inputs",
            ));
        }
        let contract_hash = DecisionReplayPolicy::graph_contract_hash()?;
        let mut nodes = Vec::with_capacity(names.len().saturating_mul(2).saturating_add(2));
        let mut edges = Vec::with_capacity(names.len().saturating_mul(2).saturating_add(1));
        for input_name in &names {
            let feature_id = Self::feature_id(input_name);
            let transform_id = Self::transform_id(input_name);
            nodes.push(Self::node(
                feature_id.clone(),
                DecisionGraphNodeKind::FeatureInput,
                Some(input_name.clone()),
                &[input_contract_hash],
            )?);
            nodes.push(Self::node(
                transform_id.clone(),
                DecisionGraphNodeKind::Transform,
                Some(input_name.clone()),
                &[input_transform_hash],
            )?);
            edges.push(DecisionGraphEdge {
                from_node_id: feature_id,
                to_node_id: transform_id.clone(),
            });
            edges.push(DecisionGraphEdge {
                from_node_id: transform_id,
                to_node_id: "model_output".to_owned(),
            });
        }
        nodes.extend([
            Self::node(
                "model_output".to_owned(),
                DecisionGraphNodeKind::ModelOutput,
                None,
                &[model_artifact_hash],
            )?,
            Self::node(
                "global_economic_reoptimization".to_owned(),
                DecisionGraphNodeKind::EconomicReoptimizationBoundary,
                None,
                &[model_artifact_hash, policy.policy_hash],
            )?,
        ]);
        edges.push(DecisionGraphEdge {
            from_node_id: "model_output".to_owned(),
            to_node_id: "global_economic_reoptimization".to_owned(),
        });
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        edges.sort();
        let graph_hash = CanonicalDigest::content_hash_typed(
            DECISION_GRAPH_DOMAIN,
            DECISION_VERSION,
            &DecisionGraphPreimage {
                contract_hash,
                nodes: &nodes,
                edges: &edges,
            },
        )?;
        Ok(Self {
            contract_hash,
            nodes,
            edges,
            graph_hash,
        })
    }

    fn node(
        node_id: String,
        kind: DecisionGraphNodeKind,
        input_name: Option<String>,
        binding_hashes: &[ContentHash],
    ) -> QuantResult<DecisionGraphNode> {
        let contract_hash = CanonicalDigest::content_hash_typed(
            DECISION_GRAPH_NODE_DOMAIN,
            DECISION_VERSION,
            &DecisionGraphNodePreimage {
                kind,
                node_id: &node_id,
                binding_hashes,
            },
        )?;
        Ok(DecisionGraphNode {
            node_id,
            kind,
            input_name,
            contract_hash,
        })
    }

    fn path(input_name: &str) -> DecisionGraphPath {
        DecisionGraphPath {
            node_ids: vec![
                Self::feature_id(input_name),
                Self::transform_id(input_name),
                "model_output".to_owned(),
                "global_economic_reoptimization".to_owned(),
            ],
        }
    }

    fn feature_id(input_name: &str) -> String {
        format!("feature:{input_name}")
    }

    fn transform_id(input_name: &str) -> String {
        format!("transform:{input_name}")
    }
}

/// Versioned policy binding for route-local model interventions.
///
/// It deliberately carries no score floors, confidence gates, or `TopN` knobs:
/// final selection belongs exclusively to calibrated economics and the global
/// portfolio optimizer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplayPolicy {
    pub policy_hash: ContentHash,
    pub admissible_intervention_policy_hash: ContentHash,
    pub computation_graph_contract_hash: ContentHash,
}

impl DecisionReplayPolicy {
    pub fn graph_contract_hash() -> QuantResult<ContentHash> {
        CanonicalDigest::content_hash_typed(
            DECISION_GRAPH_CONTRACT_DOMAIN,
            DECISION_VERSION,
            &DECISION_GRAPH_CONTRACT,
        )
        .map_err(Into::into)
    }

    pub fn try_new(policy_hash: ContentHash) -> QuantResult<Self> {
        let policy = Self {
            policy_hash,
            admissible_intervention_policy_hash: CanonicalDigest::content_hash_typed(
                DECISION_INTERVENTION_DOMAIN,
                DECISION_VERSION,
                &DECISION_INTERVENTION_POLICY,
            )?,
            computation_graph_contract_hash: Self::graph_contract_hash()?,
        };
        policy.validate()?;
        Ok(policy)
    }

    fn validate(&self) -> QuantResult<()> {
        if self.computation_graph_contract_hash != Self::graph_contract_hash()? {
            return Err(invalid_method(
                "decision replay policy has an invalid computation graph contract",
            ));
        }
        Ok(())
    }
}

/// Scope of the deterministic replay. The boundary is explicit: an
/// intervention never claims a changed recommendation until calibration,
/// executable cashflows, scenarios, and the global MILP are rerun.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionReplayScope {
    RouteModelOutputToEconomicBoundary,
}

/// Route-local model result at the global economic reoptimization boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionReplay {
    pub model_output: Decimal,
    pub global_economic_reoptimization_required: bool,
}

/// Frozen admissible interval for one model-ready feature input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionInterventionSupport {
    pub minimum: Decimal,
    pub maximum: Decimal,
}

impl DecisionInterventionSupport {
    pub fn try_new(minimum: Decimal, maximum: Decimal) -> QuantResult<Self> {
        if minimum > maximum {
            return Err(invalid_method(
                "decision intervention support minimum exceeds maximum",
            ));
        }
        Ok(Self { minimum, maximum })
    }

    #[must_use]
    pub fn contains(self, value: Decimal) -> bool {
        (self.minimum..=self.maximum).contains(&value)
    }
}

/// Typed reason why a proposed feature intervention was not replayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecisionInterventionNotEvaluableReason {
    MissingObservedValue,
    NoMaterialModelContribution,
    ObservedValueOutOfSupport,
    ProposedValueOutOfSupport,
    NoMaterialInputChange,
    DeadbandWouldSuppressSignal,
    OutcomeSideFlipNotAdmissible,
    ProjectionNotAdmissible,
    TokenSideFlipNotAdmissible,
    NoMaterialModelOutputChange,
}

/// Materializer result used to construct one immutable intervention outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecisionInterventionEvaluation {
    Evaluated {
        intervened_model_output: Decimal,
    },
    NotEvaluable {
        reason: DecisionInterventionNotEvaluableReason,
    },
}

/// Complete materializer input for one feature-specific intervention attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionInterventionAttempt {
    pub input_name: String,
    pub model_contribution: Decimal,
    pub observed_value: Option<Decimal>,
    pub proposed_value: Option<Decimal>,
    pub support: DecisionInterventionSupport,
    pub evaluation: DecisionInterventionEvaluation,
}

/// Persisted result for one feature-specific intervention attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum DecisionInterventionOutcome {
    Evaluated {
        affected_paths: Vec<DecisionGraphPath>,
        replay: DecisionReplay,
    },
    NotEvaluable {
        reason: DecisionInterventionNotEvaluableReason,
    },
}

/// One persisted intervention and its domain/support audit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionIntervention {
    pub input_name: String,
    pub model_contribution: Decimal,
    pub observed_value: Option<Decimal>,
    pub proposed_value: Option<Decimal>,
    pub support: DecisionInterventionSupport,
    pub outcome: DecisionInterventionOutcome,
}

/// Immutable model intervention replay up to the global economic
/// reoptimization boundary. It is neither a causal effect nor a shortcut for
/// recalibrating probabilities and rerunning the portfolio MILP.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionInterventionReplayArtifact {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub target_key: DecisionCandidateKey,
    pub prediction_explanation_hash: ContentHash,
    pub model_artifact_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub scope: DecisionReplayScope,
    pub policy: DecisionReplayPolicy,
    pub computation_graph: DecisionComputationGraph,
    pub baseline: DecisionReplay,
    pub interventions: Vec<DecisionIntervention>,
}

/// Complete input to one deterministic model-to-decision intervention replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecisionInterventionReplayInput {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub recommendation_id: RecommendationId,
    pub target_key: DecisionCandidateKey,
    pub prediction_explanation_hash: ContentHash,
    pub model_artifact_hash: ContentHash,
    pub input_contract_hash: ContentHash,
    pub input_transform_hash: ContentHash,
    pub policy: DecisionReplayPolicy,
    pub interventions: Vec<DecisionInterventionAttempt>,
    pub baseline_model_output: Decimal,
}

impl DecisionInterventionReplayArtifact {
    pub fn replay(input: DecisionInterventionReplayInput) -> QuantResult<Self> {
        input.policy.validate()?;
        let expected_intervention = CanonicalDigest::content_hash_typed(
            DECISION_INTERVENTION_DOMAIN,
            DECISION_VERSION,
            &DECISION_INTERVENTION_POLICY,
        )?;
        let expected_graph_contract = DecisionReplayPolicy::graph_contract_hash()?;
        let required_lineage = [
            input.prediction_explanation_hash,
            input.model_artifact_hash,
            input.input_contract_hash,
            input.input_transform_hash,
            input.policy.policy_hash,
            input.policy.admissible_intervention_policy_hash,
            input.policy.computation_graph_contract_hash,
        ];
        if input.policy.admissible_intervention_policy_hash != expected_intervention
            || input.policy.computation_graph_contract_hash != expected_graph_contract
            || required_lineage.iter().any(|hash| {
                input
                    .lineage
                    .source_evidence_hashes
                    .binary_search(hash)
                    .is_err()
            })
        {
            return Err(invalid_method(
                "model intervention policy, computation graph, or lineage is inconsistent",
            ));
        }
        let mut attempts = input.interventions;
        attempts.sort_by(|left, right| left.input_name.cmp(&right.input_name));
        let input_names = attempts
            .iter()
            .map(|attempt| attempt.input_name.clone())
            .collect::<Vec<_>>();
        let computation_graph = DecisionComputationGraph::try_new(
            &input_names,
            input.model_artifact_hash,
            input.input_contract_hash,
            input.input_transform_hash,
            &input.policy,
        )?;
        let baseline = replay_model_output(input.baseline_model_output);
        let interventions = attempts
            .into_iter()
            .map(|attempt| {
                let outcome = match attempt.evaluation {
                    DecisionInterventionEvaluation::Evaluated {
                        intervened_model_output,
                    } => DecisionInterventionOutcome::Evaluated {
                        affected_paths: vec![DecisionComputationGraph::path(&attempt.input_name)],
                        replay: replay_model_output(intervened_model_output),
                    },
                    DecisionInterventionEvaluation::NotEvaluable { reason } => {
                        DecisionInterventionOutcome::NotEvaluable { reason }
                    }
                };
                Ok(DecisionIntervention {
                    input_name: attempt.input_name,
                    model_contribution: attempt.model_contribution,
                    observed_value: attempt.observed_value,
                    proposed_value: attempt.proposed_value,
                    support: attempt.support,
                    outcome,
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let artifact = Self {
            lineage: input.lineage,
            model_version_id: input.model_version_id,
            recommendation_id: input.recommendation_id,
            target_key: input.target_key,
            prediction_explanation_hash: input.prediction_explanation_hash,
            model_artifact_hash: input.model_artifact_hash,
            input_contract_hash: input.input_contract_hash,
            input_transform_hash: input.input_transform_hash,
            scope: DecisionReplayScope::RouteModelOutputToEconomicBoundary,
            policy: input.policy,
            computation_graph,
            baseline,
            interventions,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        self.policy.validate()?;
        if self.scope != DecisionReplayScope::RouteModelOutputToEconomicBoundary
            || self.interventions.is_empty()
        {
            return Err(invalid_method(
                "decision intervention replay requires at least one feature attempt",
            ));
        }
        if self.baseline != replay_model_output(self.baseline.model_output) {
            return Err(invalid_method(
                "model intervention baseline does not stop at the economic boundary",
            ));
        }
        self.validate_lineage()?;
        for intervention in &self.interventions {
            DecisionInterventionSupport::try_new(
                intervention.support.minimum,
                intervention.support.maximum,
            )?;
            let observed_in_support = intervention
                .observed_value
                .is_some_and(|value| intervention.support.contains(value));
            let proposed_in_support = intervention
                .proposed_value
                .is_some_and(|value| intervention.support.contains(value));
            match &intervention.outcome {
                DecisionInterventionOutcome::Evaluated {
                    affected_paths,
                    replay,
                } => {
                    if intervention.model_contribution.is_zero()
                        || !observed_in_support
                        || !proposed_in_support
                        || intervention.observed_value == intervention.proposed_value
                        || affected_paths
                            != &[DecisionComputationGraph::path(&intervention.input_name)]
                        || replay != &replay_model_output(replay.model_output)
                    {
                        return Err(invalid_method(
                            "evaluated intervention is not domain-admissible or replay-exact",
                        ));
                    }
                }
                DecisionInterventionOutcome::NotEvaluable { reason } => {
                    let valid_reason = match reason {
                        DecisionInterventionNotEvaluableReason::MissingObservedValue => {
                            intervention.observed_value.is_none()
                        }
                        DecisionInterventionNotEvaluableReason::NoMaterialModelContribution => {
                            intervention.model_contribution.is_zero()
                        }
                        DecisionInterventionNotEvaluableReason::ObservedValueOutOfSupport => {
                            intervention.observed_value.is_some() && !observed_in_support
                        }
                        DecisionInterventionNotEvaluableReason::ProposedValueOutOfSupport => {
                            intervention.proposed_value.is_some() && !proposed_in_support
                        }
                        DecisionInterventionNotEvaluableReason::NoMaterialInputChange => {
                            intervention.observed_value.is_some()
                                && intervention.observed_value == intervention.proposed_value
                        }
                        DecisionInterventionNotEvaluableReason::DeadbandWouldSuppressSignal
                        | DecisionInterventionNotEvaluableReason::OutcomeSideFlipNotAdmissible
                        | DecisionInterventionNotEvaluableReason::ProjectionNotAdmissible
                        | DecisionInterventionNotEvaluableReason::TokenSideFlipNotAdmissible
                        | DecisionInterventionNotEvaluableReason::NoMaterialModelOutputChange => {
                            !intervention.model_contribution.is_zero()
                                && observed_in_support
                                && proposed_in_support
                                && intervention.observed_value != intervention.proposed_value
                        }
                    };
                    if !valid_reason {
                        return Err(invalid_method(
                            "not-evaluable intervention reason contradicts its frozen inputs",
                        ));
                    }
                }
            }
            if intervention.input_name.trim().is_empty() {
                return Err(invalid_method(
                    "decision intervention input name must not be empty",
                ));
            }
        }
        Ok(())
    }

    fn validate_lineage(&self) -> QuantResult<()> {
        let required = [
            self.prediction_explanation_hash,
            self.model_artifact_hash,
            self.input_contract_hash,
            self.input_transform_hash,
            self.policy.policy_hash,
            self.policy.admissible_intervention_policy_hash,
            self.policy.computation_graph_contract_hash,
        ];
        if required.iter().any(|hash| {
            self.lineage
                .source_evidence_hashes
                .binary_search(hash)
                .is_err()
        }) {
            return Err(invalid_method(
                "decision replay lineage omits an exact model, policy, or graph contract",
            ));
        }
        let names = self
            .interventions
            .iter()
            .map(|intervention| intervention.input_name.clone())
            .collect::<Vec<_>>();
        if !strictly_sorted(&names) {
            return Err(invalid_method(
                "decision intervention attempts must be unique and sorted",
            ));
        }
        let expected = DecisionComputationGraph::try_new(
            &names,
            self.model_artifact_hash,
            self.input_contract_hash,
            self.input_transform_hash,
            &self.policy,
        )?;
        if self.computation_graph != expected {
            return Err(invalid_method(
                "decision computation graph differs from its exact contracts",
            ));
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
pub enum ResolutionOutcomeAssociationTarget {
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

fn validate_estimates(
    estimates: &[AssociationEstimate],
    included_recommendation_count: u64,
) -> QuantResult<()> {
    let mut names = BTreeSet::new();
    for estimate in estimates {
        if estimate.input_name.trim().is_empty()
            || !names.insert(estimate.input_name.as_str())
            || estimate.sample_count != included_recommendation_count
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

/// Cohort-level association between model explanations and final outcome truth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolutionOutcomeAssociationArtifact {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub interpretation: AssociationInterpretation,
    pub target: ResolutionOutcomeAssociationTarget,
    pub estimator_contract_hash: ContentHash,
    pub conditioning_policy_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub explanation_set_hash: ContentHash,
    pub resolution_set_hash: ContentHash,
    pub included_recommendation_count: u64,
    pub estimates: Vec<AssociationEstimate>,
}

/// One mature recommendation admitted to a conditional association estimate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionOutcomeAssociationSample {
    pub recommendation_id: RecommendationId,
    pub explanation_hash: ContentHash,
    pub outcome_hash: ContentHash,
    pub outcome: Decimal,
    pub contributions: Vec<PredictionContribution>,
}

/// Complete immutable preimage for one cohort-level association artifact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolutionOutcomeAssociationInput {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub target: ResolutionOutcomeAssociationTarget,
    pub estimator_contract_hash: ContentHash,
    pub conditioning_policy_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub explanation_set_hash: ContentHash,
    pub resolution_set_hash: ContentHash,
    pub samples: Vec<ResolutionOutcomeAssociationSample>,
}

trait AssociationRegressionSample {
    fn outcome(&self) -> Decimal;
    fn contributions(&self) -> &[PredictionContribution];
}

impl AssociationRegressionSample for ResolutionOutcomeAssociationSample {
    fn outcome(&self) -> Decimal {
        self.outcome
    }

    fn contributions(&self) -> &[PredictionContribution] {
        &self.contributions
    }
}

fn fit_association_estimates(
    samples: &[impl AssociationRegressionSample],
    input_names: Vec<String>,
) -> QuantResult<Vec<AssociationEstimate>> {
    let sample_count = u64::try_from(samples.len())
        .map_err(|error| invalid_method(format!("sample count overflow: {error}")))?;
    let count = Decimal::from(sample_count);
    let degrees_of_freedom = count - Decimal::from(2_u32);
    let confidence_level = Decimal::new(95, 2);
    let z = Decimal::new(1_959_963_984_540_054_i64, 15);
    let mut estimates = Vec::with_capacity(input_names.len());
    for (index, input_name) in input_names.into_iter().enumerate() {
        let x_mean = samples
            .iter()
            .map(|sample| sample.contributions()[index].contribution)
            .sum::<Decimal>()
            / count;
        let y_mean = samples
            .iter()
            .map(AssociationRegressionSample::outcome)
            .sum::<Decimal>()
            / count;
        let sxx = samples
            .iter()
            .map(|sample| {
                let centered = sample.contributions()[index].contribution - x_mean;
                centered * centered
            })
            .sum::<Decimal>();
        if sxx.is_zero() {
            continue;
        }
        let sxy = samples
            .iter()
            .map(|sample| {
                (sample.contributions()[index].contribution - x_mean) * (sample.outcome() - y_mean)
            })
            .sum::<Decimal>();
        let estimate = sxy / sxx;
        let squared_error = samples
            .iter()
            .map(|sample| {
                let fitted =
                    y_mean + estimate * (sample.contributions()[index].contribution - x_mean);
                let residual = sample.outcome() - fitted;
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

impl ResolutionOutcomeAssociationArtifact {
    /// Fit independent cohort-conditional OLS slopes with classical 95%
    /// uncertainty. The result is descriptive association only; no feature
    /// contribution is multiplied by `PnL` and no causal effect is asserted.
    pub fn estimate(mut input: ResolutionOutcomeAssociationInput) -> QuantResult<Self> {
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
        let required_lineage = [expected_cohort, expected_explanations, expected_resolutions];
        if input.estimator_contract_hash != expected_estimator
            || input.cohort_manifest_hash != expected_cohort
            || input.explanation_set_hash != expected_explanations
            || input.resolution_set_hash != expected_resolutions
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
        let estimates = fit_association_estimates(&input.samples, input_names)?;
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
        validate_estimates(&self.estimates, self.included_recommendation_count)
    }
}

/// Net execution variable used by one association analysis.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionOutcomeAssociationTarget {
    RealizedNetPnlUsd,
}

/// Complete terminal execution rollup identity and economics admitted to an
/// execution association.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionOutcomeBinding {
    pub recommendation_id: RecommendationId,
    pub rollup_hash: ContentHash,
    pub attempt_set_hash: ContentHash,
    pub intent_count: i32,
    pub attempt_count: i32,
    pub total_filled_shares: Shares,
    pub total_entry_fee_usd: Option<Usd>,
    pub total_exit_fee_usd: Option<Usd>,
    pub total_realized_pnl_usd: Usd,
    pub terminal_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
}

impl ExecutionOutcomeBinding {
    fn validate(&self) -> QuantResult<()> {
        if self.intent_count < 0
            || self.attempt_count < 0
            || self.attempt_count > self.intent_count
            || self.total_filled_shares.is_negative()
            || self
                .total_entry_fee_usd
                .is_some_and(|fee| fee.is_negative())
            || self.total_exit_fee_usd.is_some_and(|fee| fee.is_negative())
            || self.terminal_at > self.available_at
        {
            return Err(invalid_method(
                "execution association binding has invalid counts, economics, or timeline",
            ));
        }
        Ok(())
    }
}

/// One terminal execution rollup admitted to a non-causal association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcomeAssociationSample {
    pub explanation_hash: ContentHash,
    pub binding: ExecutionOutcomeBinding,
    pub contributions: Vec<PredictionContribution>,
}

impl AssociationRegressionSample for ExecutionOutcomeAssociationSample {
    fn outcome(&self) -> Decimal {
        self.binding.total_realized_pnl_usd.inner()
    }

    fn contributions(&self) -> &[PredictionContribution] {
        &self.contributions
    }
}

/// Complete immutable preimage for one execution-outcome association.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionOutcomeAssociationInput {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub target: ExecutionOutcomeAssociationTarget,
    pub estimator_contract_hash: ContentHash,
    pub conditioning_policy_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub explanation_set_hash: ContentHash,
    pub execution_rollup_set_hash: ContentHash,
    pub samples: Vec<ExecutionOutcomeAssociationSample>,
}

/// Cohort-level association between model explanations and terminal execution
/// truth. The estimate remains descriptive and explicitly non-causal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionOutcomeAssociationArtifact {
    pub lineage: AttributionLineage,
    pub model_version_id: ModelVersionId,
    pub interpretation: AssociationInterpretation,
    pub target: ExecutionOutcomeAssociationTarget,
    pub estimator_contract_hash: ContentHash,
    pub conditioning_policy_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub explanation_set_hash: ContentHash,
    pub execution_rollup_set_hash: ContentHash,
    pub included_recommendation_count: u64,
    pub estimates: Vec<AssociationEstimate>,
}

impl ExecutionOutcomeAssociationArtifact {
    pub fn estimate(mut input: ExecutionOutcomeAssociationInput) -> QuantResult<Self> {
        if input.samples.len() < 3 {
            return Err(invalid_method(
                "execution outcome association requires at least three terminal rollups",
            ));
        }
        input
            .samples
            .sort_by_key(|sample| sample.binding.recommendation_id.as_uuid());
        if input
            .samples
            .windows(2)
            .any(|pair| pair[0].binding.recommendation_id == pair[1].binding.recommendation_id)
        {
            return Err(invalid_method(
                "execution outcome association contains duplicate recommendations",
            ));
        }
        for sample in &input.samples {
            sample.binding.validate()?;
        }
        let recommendation_ids = input
            .samples
            .iter()
            .map(|sample| sample.binding.recommendation_id)
            .collect::<Vec<_>>();
        let explanation_hashes = input
            .samples
            .iter()
            .map(|sample| sample.explanation_hash)
            .collect::<Vec<_>>();
        let bindings = input
            .samples
            .iter()
            .map(|sample| &sample.binding)
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
        let expected_executions = CanonicalDigest::content_hash_typed(
            ASSOCIATION_EXECUTIONS_DOMAIN,
            ASSOCIATION_VERSION,
            &bindings,
        )?;
        if input.estimator_contract_hash != expected_estimator
            || input.cohort_manifest_hash != expected_cohort
            || input.explanation_set_hash != expected_explanations
            || input.execution_rollup_set_hash != expected_executions
            || [expected_cohort, expected_explanations, expected_executions]
                .iter()
                .any(|hash| {
                    input
                        .lineage
                        .source_evidence_hashes
                        .binary_search(hash)
                        .is_err()
                })
        {
            return Err(invalid_method(
                "execution association set hashes or lineage differ from terminal rollups",
            ));
        }
        let input_names = input
            .samples
            .first()
            .ok_or_else(|| invalid_method("execution association sample set is empty"))?
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
                "execution association explanations do not share one canonical input plane",
            ));
        }
        let estimates = fit_association_estimates(&input.samples, input_names)?;
        let artifact = Self {
            lineage: input.lineage,
            model_version_id: input.model_version_id,
            interpretation: AssociationInterpretation::ConditionalNonCausal,
            target: input.target,
            estimator_contract_hash: input.estimator_contract_hash,
            conditioning_policy_hash: input.conditioning_policy_hash,
            cohort_manifest_hash: input.cohort_manifest_hash,
            explanation_set_hash: input.explanation_set_hash,
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
            || self.execution_rollup_set_hash == ContentHash::from_bytes([0; 32])
        {
            return Err(invalid_method(
                "execution association requires non-empty terminal rollups and non-causal semantics",
            ));
        }
        validate_estimates(&self.estimates, self.included_recommendation_count)
    }
}

/// Why the immutable actual execution facts cannot support a numeric return
/// baseline. A non-evaluable baseline is evidence, never an implicit zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActualBaselineNotEvaluableReason {
    MissingEntryFee,
    MissingExitFee,
    MissingRealizedPnl,
}

/// Gross and net actual economics derived from one terminal execution attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum ActualExecutionBaseline {
    Evaluated {
        entry_fee_usd: Usd,
        exit_fee_usd: Usd,
        entry_cash_outlay_usd: Usd,
        actual_gross_pnl_usd: Usd,
        actual_net_pnl_usd: Usd,
        actual_gross_return_bps: Bps,
        actual_net_return_bps: Bps,
    },
    NotEvaluable {
        reason: ActualBaselineNotEvaluableReason,
    },
}

/// Point-specific source defect that prevents executable economics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrajectoryPointNotEvaluableReason {
    NoBidDepth,
    InvalidBookDepth,
    FeeScheduleUnavailable,
    InvalidFeeSchedule,
}

/// Size-specific executable economics for one PIT L2 snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum TrajectoryPointEconomics {
    Executable {
        filled_shares: Shares,
        remaining_shares: Shares,
        depth_levels_consumed: u32,
        best_bid_price: Price,
        executable_exit_price: Price,
        gross_exit_proceeds_usd: Usd,
        exit_fee_usd: Usd,
        net_exit_proceeds_usd: Usd,
        fee_schedule_hash: ContentHash,
        slippage_bps: Bps,
    },
    InsufficientDepth {
        filled_shares: Shares,
        remaining_shares: Shares,
        depth_levels_consumed: u32,
        best_bid_price: Price,
        partial_vwap: Price,
        partial_gross_proceeds_usd: Usd,
        partial_exit_fee_usd: Usd,
        partial_net_proceeds_usd: Usd,
        fee_schedule_hash: ContentHash,
        partial_slippage_bps: Bps,
    },
    NotEvaluable {
        reason: TrajectoryPointNotEvaluableReason,
    },
}

/// One PIT full-depth observation used by an attempt trajectory.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrajectoryPoint {
    pub observed_at: DateTime<Utc>,
    pub available_at: DateTime<Utc>,
    pub requested_shares: Shares,
    pub economics: TrajectoryPointEconomics,
    pub source_fact_hash: ContentHash,
}

impl TrajectoryPoint {
    fn validate(&self) -> QuantResult<()> {
        if !self.requested_shares.is_positive() {
            return Err(invalid_method(
                "trajectory point requires a positive requested size",
            ));
        }
        match &self.economics {
            TrajectoryPointEconomics::Executable {
                filled_shares,
                remaining_shares,
                depth_levels_consumed,
                best_bid_price,
                executable_exit_price,
                gross_exit_proceeds_usd,
                exit_fee_usd,
                net_exit_proceeds_usd,
                fee_schedule_hash,
                slippage_bps,
            } => PointEconomicsValidation {
                requested_shares: self.requested_shares,
                filled_shares: *filled_shares,
                remaining_shares: *remaining_shares,
                depth_levels_consumed: *depth_levels_consumed,
                best_bid_price: *best_bid_price,
                vwap: *executable_exit_price,
                gross_proceeds_usd: *gross_exit_proceeds_usd,
                fee_usd: *exit_fee_usd,
                net_proceeds_usd: *net_exit_proceeds_usd,
                fee_schedule_hash: *fee_schedule_hash,
                slippage_bps: *slippage_bps,
                requires_complete: true,
            }
            .validate(),
            TrajectoryPointEconomics::InsufficientDepth {
                filled_shares,
                remaining_shares,
                depth_levels_consumed,
                best_bid_price,
                partial_vwap,
                partial_gross_proceeds_usd,
                partial_exit_fee_usd,
                partial_net_proceeds_usd,
                fee_schedule_hash,
                partial_slippage_bps,
            } => PointEconomicsValidation {
                requested_shares: self.requested_shares,
                filled_shares: *filled_shares,
                remaining_shares: *remaining_shares,
                depth_levels_consumed: *depth_levels_consumed,
                best_bid_price: *best_bid_price,
                vwap: *partial_vwap,
                gross_proceeds_usd: *partial_gross_proceeds_usd,
                fee_usd: *partial_exit_fee_usd,
                net_proceeds_usd: *partial_net_proceeds_usd,
                fee_schedule_hash: *fee_schedule_hash,
                slippage_bps: *partial_slippage_bps,
                requires_complete: false,
            }
            .validate(),
            TrajectoryPointEconomics::NotEvaluable { .. } => Ok(()),
        }
    }

    const fn executable_net_proceeds(&self) -> Option<Usd> {
        match self.economics {
            TrajectoryPointEconomics::Executable {
                net_exit_proceeds_usd,
                ..
            } => Some(net_exit_proceeds_usd),
            TrajectoryPointEconomics::InsufficientDepth { .. }
            | TrajectoryPointEconomics::NotEvaluable { .. } => None,
        }
    }
}

/// Typed MAE/MFE state. Numeric excursions only exist when both the actual
/// baseline and at least one full-size executable observation are available.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum TrajectoryExcursionEvaluation {
    Evaluated {
        max_adverse_excursion_bps: Bps,
        max_favorable_excursion_bps: Bps,
    },
    ActualBaselineUnavailable {
        reason: ActualBaselineNotEvaluableReason,
    },
    NoExecutableObservation,
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
    pub entry_shares: Shares,
    pub entry_price: Price,
    pub entry_principal_usd: Usd,
    pub actual_baseline: ActualExecutionBaseline,
    pub horizon_end: DateTime<Utc>,
    pub points: Vec<TrajectoryPoint>,
    pub excursions: TrajectoryExcursionEvaluation,
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
    pub entry_shares: Shares,
    pub entry_price: Price,
    pub actual_baseline: ActualExecutionBaseline,
    pub horizon_end: DateTime<Utc>,
    pub points: Vec<TrajectoryPoint>,
}

impl ExecutionTrajectoryArtifact {
    pub fn try_new(input: ExecutionTrajectoryInput) -> QuantResult<Self> {
        if input.entry_price <= Price::ZERO || input.entry_shares <= Shares::ZERO {
            return Err(invalid_method(
                "execution trajectory requires positive entry price and shares",
            ));
        }
        let entry_principal_usd = input.entry_shares * input.entry_price;
        validate_actual_baseline(
            &input.actual_baseline,
            entry_principal_usd,
            input.entry_shares,
        )?;
        let excursions = trajectory_excursions(&input.actual_baseline, &input.points)?;
        let artifact = Self {
            lineage: input.lineage,
            recommendation_id: input.recommendation_id,
            order_intent_id: input.order_intent_id,
            attempt_outcome_hash: input.attempt_outcome_hash,
            pit_book_contract_hash: input.pit_book_contract_hash,
            entry_at: input.entry_at,
            entry_shares: input.entry_shares,
            entry_price: input.entry_price,
            entry_principal_usd,
            actual_baseline: input.actual_baseline,
            horizon_end: input.horizon_end,
            points: input.points,
            excursions,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        if self.entry_at >= self.horizon_end
            || self.horizon_end > self.lineage.source_cutoff
            || self.entry_price <= Price::ZERO
            || self.entry_shares <= Shares::ZERO
            || self.entry_principal_usd != self.entry_shares * self.entry_price
        {
            return Err(leakage(
                "execution trajectory window, entry economics, or maturity is invalid",
            ));
        }
        validate_actual_baseline(
            &self.actual_baseline,
            self.entry_principal_usd,
            self.entry_shares,
        )?;
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
                || point.requested_shares != self.entry_shares
                || prior.is_some_and(|timestamp| timestamp >= point.observed_at)
                || !hashes.insert(point.source_fact_hash)
            {
                return Err(leakage(
                    "trajectory points must be unique, ordered PIT facts for the exact entry size",
                ));
            }
            point.validate()?;
            prior = Some(point.observed_at);
        }
        if trajectory_excursions(&self.actual_baseline, &self.points)? != self.excursions {
            return Err(invalid_method(
                "stored trajectory excursions differ from the size-specific net replay",
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

/// Why an approved alternative policy cannot produce a numeric estimate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicyCounterfactualNotEvaluableReason {
    ActualBaselineUnavailable,
    NoTrajectoryObservation,
    NoFullyExecutableObservation,
    IncompleteFirstBarrierPath,
}

/// Typed result of an alternative-policy replay. Numeric estimates exist only
/// in the `Evaluated` variant and therefore cannot be confused with zeros.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "status")]
pub enum PolicyCounterfactualEvaluation {
    Evaluated {
        counterfactual_exit_at: DateTime<Utc>,
        counterfactual_exit_price: Price,
        counterfactual_gross_proceeds_usd: Usd,
        counterfactual_exit_fee_usd: Usd,
        counterfactual_net_proceeds_usd: Usd,
        actual_gross_return_bps: Bps,
        actual_net_return_bps: Bps,
        counterfactual_gross_return_bps: Bps,
        counterfactual_net_return_bps: Bps,
        missed_return_bps: Bps,
    },
    NotEvaluable {
        reason: PolicyCounterfactualNotEvaluableReason,
    },
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
    pub evaluation: PolicyCounterfactualEvaluation,
}

impl PolicyCounterfactualOutcome {
    pub fn replay(
        trajectory: &ExecutionTrajectoryArtifact,
        trajectory_artifact_hash: ContentHash,
        alternative_policy_hash: ContentHash,
        alternative_policy: AlternativeExitPolicy,
    ) -> QuantResult<Self> {
        trajectory.validate()?;
        alternative_policy.validate()?;
        let evaluation = replay_policy(trajectory, alternative_policy)?;
        let artifact = Self {
            lineage: trajectory.lineage.clone(),
            recommendation_id: trajectory.recommendation_id,
            order_intent_id: trajectory.order_intent_id,
            trajectory_artifact_hash,
            alternative_policy_hash,
            alternative_policy,
            evaluation,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn validate(&self) -> QuantResult<()> {
        self.lineage.validate()?;
        self.alternative_policy.validate()?;
        if let PolicyCounterfactualEvaluation::Evaluated {
            counterfactual_exit_at,
            counterfactual_exit_price,
            counterfactual_gross_proceeds_usd,
            counterfactual_exit_fee_usd,
            counterfactual_net_proceeds_usd,
            actual_net_return_bps,
            counterfactual_net_return_bps,
            missed_return_bps,
            ..
        } = self.evaluation
            && (counterfactual_exit_at > self.lineage.source_cutoff
                || !(Price::ZERO..=Price::ONE).contains(&counterfactual_exit_price)
                || counterfactual_gross_proceeds_usd < counterfactual_exit_fee_usd
                || counterfactual_net_proceeds_usd
                    != counterfactual_gross_proceeds_usd - counterfactual_exit_fee_usd
                || missed_return_bps != counterfactual_net_return_bps - actual_net_return_bps)
        {
            return Err(invalid_method(
                "policy counterfactual result is inconsistent with its net economics or cutoff",
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
    DecisionInterventionReplay(Box<DecisionInterventionReplayArtifact>),
    ResolutionOutcomeAssociation(Box<ResolutionOutcomeAssociationArtifact>),
    ExecutionOutcomeAssociation(Box<ExecutionOutcomeAssociationArtifact>),
    ExecutionTrajectory(Box<ExecutionTrajectoryArtifact>),
    PolicyCounterfactualOutcome(Box<PolicyCounterfactualOutcome>),
}

impl AttributionArtifact {
    pub fn validate(&self) -> QuantResult<()> {
        match self {
            Self::PredictionExplanation(artifact) => artifact.validate(),
            Self::DecisionInterventionReplay(artifact) => artifact.validate(),
            Self::ResolutionOutcomeAssociation(artifact) => artifact.validate(),
            Self::ExecutionOutcomeAssociation(artifact) => artifact.validate(),
            Self::ExecutionTrajectory(artifact) => artifact.validate(),
            Self::PolicyCounterfactualOutcome(artifact) => artifact.validate(),
        }
    }

    #[must_use]
    pub const fn kind(&self) -> AttributionArtifactKind {
        match self {
            Self::PredictionExplanation(_) => AttributionArtifactKind::PredictionExplanation,
            Self::DecisionInterventionReplay(_) => {
                AttributionArtifactKind::DecisionInterventionReplay
            }
            Self::ResolutionOutcomeAssociation(_) => {
                AttributionArtifactKind::ResolutionOutcomeAssociation
            }
            Self::ExecutionOutcomeAssociation(_) => {
                AttributionArtifactKind::ExecutionOutcomeAssociation
            }
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
            Self::DecisionInterventionReplay(artifact) => &artifact.lineage,
            Self::ResolutionOutcomeAssociation(artifact) => &artifact.lineage,
            Self::ExecutionOutcomeAssociation(artifact) => &artifact.lineage,
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
            Self::DecisionInterventionReplay(artifact) => AttributionSubject::Decision {
                model_version_id: artifact.model_version_id,
                recommendation_id: artifact.recommendation_id,
            },
            Self::ResolutionOutcomeAssociation(artifact) => AttributionSubject::ResolutionOutcome {
                model_version_id: artifact.model_version_id,
            },
            Self::ExecutionOutcomeAssociation(artifact) => AttributionSubject::ExecutionOutcome {
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

const fn replay_model_output(model_output: Decimal) -> DecisionReplay {
    DecisionReplay {
        model_output,
        global_economic_reoptimization_required: true,
    }
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

fn validate_actual_baseline(
    baseline: &ActualExecutionBaseline,
    entry_principal_usd: Usd,
    entry_shares: Shares,
) -> QuantResult<()> {
    if !entry_principal_usd.is_positive() || !entry_shares.is_positive() {
        return Err(invalid_method(
            "actual baseline requires positive entry principal and shares",
        ));
    }
    let ActualExecutionBaseline::Evaluated {
        entry_fee_usd,
        exit_fee_usd,
        entry_cash_outlay_usd,
        actual_gross_pnl_usd,
        actual_net_pnl_usd,
        actual_gross_return_bps,
        actual_net_return_bps,
    } = baseline
    else {
        return Ok(());
    };
    let expected_cash_outlay = entry_principal_usd + *entry_fee_usd;
    let expected_gross_pnl = *actual_net_pnl_usd + *entry_fee_usd + *exit_fee_usd;
    let expected_gross_return =
        Bps::relative(expected_gross_pnl.inner(), entry_principal_usd.inner())
            .ok_or_else(|| invalid_method("actual gross return denominator is zero"))?;
    let expected_net_return =
        Bps::relative(actual_net_pnl_usd.inner(), expected_cash_outlay.inner())
            .ok_or_else(|| invalid_method("actual net return denominator is zero"))?;
    if entry_fee_usd.is_negative()
        || exit_fee_usd.is_negative()
        || *entry_cash_outlay_usd != expected_cash_outlay
        || *actual_gross_pnl_usd != expected_gross_pnl
        || *actual_gross_return_bps != expected_gross_return
        || *actual_net_return_bps != expected_net_return
    {
        return Err(invalid_method(
            "actual baseline gross/net economics are internally inconsistent",
        ));
    }
    Ok(())
}

struct PointEconomicsValidation {
    requested_shares: Shares,
    filled_shares: Shares,
    remaining_shares: Shares,
    depth_levels_consumed: u32,
    best_bid_price: Price,
    vwap: Price,
    gross_proceeds_usd: Usd,
    fee_usd: Usd,
    net_proceeds_usd: Usd,
    fee_schedule_hash: ContentHash,
    slippage_bps: Bps,
    requires_complete: bool,
}

impl PointEconomicsValidation {
    fn validate(self) -> QuantResult<()> {
        let completion_is_valid = if self.requires_complete {
            self.filled_shares == self.requested_shares && self.remaining_shares == Shares::ZERO
        } else {
            self.filled_shares.is_positive()
                && self.filled_shares < self.requested_shares
                && self.remaining_shares == self.requested_shares - self.filled_shares
        };
        let expected_vwap =
            Price::new(self.gross_proceeds_usd.inner() / self.filled_shares.inner());
        let expected_slippage = Bps::relative(
            self.best_bid_price.inner() - self.vwap.inner(),
            self.best_bid_price.inner(),
        )
        .ok_or_else(|| invalid_method("trajectory slippage denominator is zero"))?;
        if !completion_is_valid
            || self.depth_levels_consumed == 0
            || !(Price::ZERO..=Price::ONE).contains(&self.best_bid_price)
            || self.best_bid_price.is_zero()
            || !(Price::ZERO..=Price::ONE).contains(&self.vwap)
            || self.vwap.is_zero()
            || self.best_bid_price < self.vwap
            || self.gross_proceeds_usd <= Usd::ZERO
            || self.fee_usd.is_negative()
            || self.gross_proceeds_usd < self.fee_usd
            || self.net_proceeds_usd != self.gross_proceeds_usd - self.fee_usd
            || self.vwap != expected_vwap
            || self.slippage_bps != expected_slippage
            || self.fee_schedule_hash == ContentHash::from_bytes([0; 32])
        {
            return Err(invalid_method(
                "trajectory point depth, fee, VWAP, or slippage economics are inconsistent",
            ));
        }
        Ok(())
    }
}

fn trajectory_excursions(
    baseline: &ActualExecutionBaseline,
    points: &[TrajectoryPoint],
) -> QuantResult<TrajectoryExcursionEvaluation> {
    let entry_cash_outlay_usd = match baseline {
        ActualExecutionBaseline::Evaluated {
            entry_cash_outlay_usd,
            ..
        } => entry_cash_outlay_usd,
        ActualExecutionBaseline::NotEvaluable { reason } => {
            return Ok(TrajectoryExcursionEvaluation::ActualBaselineUnavailable {
                reason: *reason,
            });
        }
    };
    let excursions = points
        .iter()
        .filter_map(|point| point.executable_net_proceeds().map(|net| (point, net)))
        .map(|(_, net)| {
            Bps::relative(
                (net - *entry_cash_outlay_usd).inner(),
                entry_cash_outlay_usd.inner(),
            )
            .ok_or_else(|| invalid_method("trajectory excursion denominator is zero"))
        })
        .collect::<QuantResult<Vec<_>>>()?;
    if excursions.is_empty() {
        return Ok(TrajectoryExcursionEvaluation::NoExecutableObservation);
    }
    let max_adverse_excursion_bps = excursions
        .iter()
        .copied()
        .min()
        .unwrap_or(Bps::ZERO)
        .min(Bps::ZERO);
    let max_favorable_excursion_bps = excursions
        .iter()
        .copied()
        .max()
        .unwrap_or(Bps::ZERO)
        .max(Bps::ZERO);
    Ok(TrajectoryExcursionEvaluation::Evaluated {
        max_adverse_excursion_bps,
        max_favorable_excursion_bps,
    })
}

fn replay_policy(
    trajectory: &ExecutionTrajectoryArtifact,
    policy: AlternativeExitPolicy,
) -> QuantResult<PolicyCounterfactualEvaluation> {
    let ActualExecutionBaseline::Evaluated { .. } = trajectory.actual_baseline else {
        return Ok(PolicyCounterfactualEvaluation::NotEvaluable {
            reason: PolicyCounterfactualNotEvaluableReason::ActualBaselineUnavailable,
        });
    };
    if trajectory.points.is_empty() {
        return Ok(PolicyCounterfactualEvaluation::NotEvaluable {
            reason: PolicyCounterfactualNotEvaluableReason::NoTrajectoryObservation,
        });
    }
    let selected = match policy {
        AlternativeExitPolicy::LatestExecutableAtOrBeforeHorizon => trajectory
            .points
            .iter()
            .rev()
            .find(|point| point.executable_net_proceeds().is_some()),
        AlternativeExitPolicy::FirstBarrier {
            take_profit_bps,
            stop_loss_bps,
            max_holding_secs,
        } => {
            let holding_secs = i64::try_from(max_holding_secs).map_err(|error| {
                invalid_method(format!("holding period exceeds chrono range: {error}"))
            })?;
            let maximum_at = trajectory
                .entry_at
                .checked_add_signed(Duration::seconds(holding_secs))
                .ok_or_else(|| invalid_method("holding period exceeds timestamp range"))?
                .min(trajectory.horizon_end);
            let mut latest = None;
            for point in trajectory
                .points
                .iter()
                .take_while(|point| point.observed_at <= maximum_at)
            {
                let Some(net_proceeds) = point.executable_net_proceeds() else {
                    return Ok(PolicyCounterfactualEvaluation::NotEvaluable {
                        reason: PolicyCounterfactualNotEvaluableReason::IncompleteFirstBarrierPath,
                    });
                };
                latest = Some(point);
                let net_return = counterfactual_net_return(trajectory, net_proceeds)?;
                if net_return >= take_profit_bps || net_return <= stop_loss_bps {
                    break;
                }
            }
            latest
        }
    };
    let Some(selected) = selected else {
        return Ok(PolicyCounterfactualEvaluation::NotEvaluable {
            reason: PolicyCounterfactualNotEvaluableReason::NoFullyExecutableObservation,
        });
    };
    evaluated_policy_replay(trajectory, selected)
}

fn evaluated_policy_replay(
    trajectory: &ExecutionTrajectoryArtifact,
    selected: &TrajectoryPoint,
) -> QuantResult<PolicyCounterfactualEvaluation> {
    let ActualExecutionBaseline::Evaluated {
        actual_gross_return_bps,
        actual_net_return_bps,
        ..
    } = trajectory.actual_baseline
    else {
        return Err(invalid_method(
            "evaluated policy replay lost its actual execution baseline",
        ));
    };
    let TrajectoryPointEconomics::Executable {
        executable_exit_price,
        gross_exit_proceeds_usd,
        exit_fee_usd,
        net_exit_proceeds_usd,
        ..
    } = selected.economics
    else {
        return Err(invalid_method(
            "evaluated policy replay selected a non-executable point",
        ));
    };
    let counterfactual_gross_return_bps = Bps::relative(
        (gross_exit_proceeds_usd - trajectory.entry_principal_usd).inner(),
        trajectory.entry_principal_usd.inner(),
    )
    .ok_or_else(|| invalid_method("counterfactual gross return denominator is zero"))?;
    let counterfactual_net_return_bps =
        counterfactual_net_return(trajectory, net_exit_proceeds_usd)?;
    Ok(PolicyCounterfactualEvaluation::Evaluated {
        counterfactual_exit_at: selected.observed_at,
        counterfactual_exit_price: executable_exit_price,
        counterfactual_gross_proceeds_usd: gross_exit_proceeds_usd,
        counterfactual_exit_fee_usd: exit_fee_usd,
        counterfactual_net_proceeds_usd: net_exit_proceeds_usd,
        actual_gross_return_bps,
        actual_net_return_bps,
        counterfactual_gross_return_bps,
        counterfactual_net_return_bps,
        missed_return_bps: counterfactual_net_return_bps - actual_net_return_bps,
    })
}

fn counterfactual_net_return(
    trajectory: &ExecutionTrajectoryArtifact,
    net_proceeds: Usd,
) -> QuantResult<Bps> {
    let ActualExecutionBaseline::Evaluated {
        entry_cash_outlay_usd,
        ..
    } = trajectory.actual_baseline
    else {
        return Err(invalid_method(
            "counterfactual net return requires an evaluated actual baseline",
        ));
    };
    Bps::relative(
        (net_proceeds - entry_cash_outlay_usd).inner(),
        entry_cash_outlay_usd.inner(),
    )
    .ok_or_else(|| invalid_method("counterfactual net return denominator is zero"))
}

fn strictly_sorted<T: Ord>(values: &[T]) -> bool {
    values.windows(2).all(|pair| pair[0] < pair[1])
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone, Utc};
    use quant_pivot_models::{
        enums::quant::AttributionCohort,
        hashing::CanonicalDigest,
        types::{
            Bps, ContentHash, FeedbackCycleId, MarketId, ModelVersionId, OrderIntentId, Price,
            RecommendationId, Shares, TokenId, Usd,
        },
    };
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::{
        ASSOCIATION_COHORT_DOMAIN, ASSOCIATION_ESTIMATOR_DOMAIN, ASSOCIATION_EXPLANATIONS_DOMAIN,
        ASSOCIATION_RESOLUTIONS_DOMAIN, ASSOCIATION_VERSION, ActualExecutionBaseline,
        AlternativeExitPolicy, AssociationInterpretation, AttributionArtifact,
        AttributionArtifactCodec, AttributionLineage, DecisionCandidateKey,
        DecisionInterventionAttempt, DecisionInterventionEvaluation,
        DecisionInterventionNotEvaluableReason, DecisionInterventionOutcome,
        DecisionInterventionReplayArtifact, DecisionInterventionReplayInput,
        DecisionInterventionSupport, DecisionReplayPolicy, ExecutionTrajectoryArtifact,
        ExecutionTrajectoryInput, PolicyCounterfactualEvaluation, PolicyCounterfactualOutcome,
        PredictionContribution, PredictionExplanationArtifact, PredictionOutputKind,
        ResolutionOutcomeAssociationArtifact, ResolutionOutcomeAssociationInput,
        ResolutionOutcomeAssociationSample, ResolutionOutcomeAssociationTarget,
        TrajectoryExcursionEvaluation, TrajectoryPoint, TrajectoryPointEconomics,
        WeightedExplanationInput, WeightedTerm,
    };

    fn hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("fixture hash")
    }

    impl ActualExecutionBaseline {
        fn fixture() -> Self {
            Self::Evaluated {
                entry_fee_usd: Usd::ZERO,
                exit_fee_usd: Usd::ZERO,
                entry_cash_outlay_usd: Usd::new(dec!(50)),
                actual_gross_pnl_usd: Usd::new(dec!(5)),
                actual_net_pnl_usd: Usd::new(dec!(5)),
                actual_gross_return_bps: Bps::new(dec!(1000)),
                actual_net_return_bps: Bps::new(dec!(1000)),
            }
        }
    }

    fn executable_point(
        observed_at: DateTime<Utc>,
        available_at: DateTime<Utc>,
        price: Price,
        source: &str,
    ) -> TrajectoryPoint {
        let shares = Shares::new(dec!(100));
        let proceeds = shares * price;
        TrajectoryPoint {
            observed_at,
            available_at,
            requested_shares: shares,
            economics: TrajectoryPointEconomics::Executable {
                filled_shares: shares,
                remaining_shares: Shares::ZERO,
                depth_levels_consumed: 1,
                best_bid_price: price,
                executable_exit_price: price,
                gross_exit_proceeds_usd: proceeds,
                exit_fee_usd: Usd::ZERO,
                net_exit_proceeds_usd: proceeds,
                fee_schedule_hash: hash("fee-schedule"),
                slippage_bps: Bps::ZERO,
            },
            source_fact_hash: hash(source),
        }
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
        .map(
            |(contribution, outcome)| ResolutionOutcomeAssociationSample {
                recommendation_id: RecommendationId::from_v7(),
                explanation_hash: hash(&format!("explanation-{contribution}")),
                outcome_hash: hash(&format!("outcome-{outcome}")),
                outcome,
                contributions: vec![PredictionContribution {
                    input_name: "canonical_alpha".to_owned(),
                    input_value: Some(contribution),
                    contribution,
                }],
            },
        )
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
            ],
        )
        .expect("association lineage");
        let artifact =
            ResolutionOutcomeAssociationArtifact::estimate(ResolutionOutcomeAssociationInput {
                lineage,
                model_version_id: ModelVersionId::from_v7(),
                target: ResolutionOutcomeAssociationTarget::FinalTokenPayoutRatio,
                estimator_contract_hash,
                conditioning_policy_hash: hash("conditioning"),
                cohort_manifest_hash,
                explanation_set_hash,
                resolution_set_hash,
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
    fn model_intervention_stops_economics() {
        let target_key = DecisionCandidateKey {
            market_id: MarketId::new("target-market"),
            token_id: TokenId::new("target-token"),
        };
        let policy_hash = hash("policy");
        let policy = DecisionReplayPolicy::try_new(policy_hash).expect("intervention policy");
        let prediction_explanation_hash = hash("prediction");
        let model_artifact_hash = hash("model-artifact");
        let input_contract_hash = hash("input-contract");
        let input_transform_hash = hash("input-transform");
        let base = AttributionLineage::fixture();
        let lineage = AttributionLineage::try_new(
            base.source_feedback_cycle_id,
            base.source_cohort,
            base.source_cutoff,
            base.generated_at,
            vec![
                prediction_explanation_hash,
                model_artifact_hash,
                input_contract_hash,
                input_transform_hash,
                policy_hash,
                policy.admissible_intervention_policy_hash,
                policy.computation_graph_contract_hash,
            ],
        )
        .expect("decision lineage");
        let artifact =
            DecisionInterventionReplayArtifact::replay(DecisionInterventionReplayInput {
                lineage,
                model_version_id: ModelVersionId::from_v7(),
                recommendation_id: RecommendationId::from_v7(),
                target_key,
                prediction_explanation_hash,
                model_artifact_hash,
                input_contract_hash,
                input_transform_hash,
                policy,
                interventions: vec![DecisionInterventionAttempt {
                    input_name: "canonical_alpha".to_owned(),
                    model_contribution: dec!(0.4),
                    observed_value: Some(dec!(0.4)),
                    proposed_value: Some(Decimal::ZERO),
                    support: DecisionInterventionSupport::try_new(-Decimal::ONE, Decimal::ONE)
                        .expect("support"),
                    evaluation: DecisionInterventionEvaluation::Evaluated {
                        intervened_model_output: dec!(0.4),
                    },
                }],
                baseline_model_output: dec!(0.8),
            })
            .expect("decision replay");
        assert!(artifact.baseline.global_economic_reoptimization_required);
        let DecisionInterventionOutcome::Evaluated {
            affected_paths,
            replay,
        } = &artifact.interventions[0].outcome
        else {
            panic!("fixture intervention must be evaluated");
        };
        assert!(replay.global_economic_reoptimization_required);
        assert_eq!(
            affected_paths[0].node_ids,
            [
                "feature:canonical_alpha",
                "transform:canonical_alpha",
                "model_output",
                "global_economic_reoptimization",
            ]
        );

        let mut out_of_support = artifact.clone();
        let intervention = out_of_support
            .interventions
            .first_mut()
            .expect("fixture intervention");
        intervention.observed_value = Some(dec!(2));
        intervention.outcome = DecisionInterventionOutcome::NotEvaluable {
            reason: DecisionInterventionNotEvaluableReason::ObservedValueOutOfSupport,
        };
        out_of_support
            .validate()
            .expect("typed out-of-support outcome");

        let mut contradictory = out_of_support.clone();
        contradictory
            .interventions
            .first_mut()
            .expect("fixture intervention")
            .outcome = DecisionInterventionOutcome::NotEvaluable {
            reason: DecisionInterventionNotEvaluableReason::NoMaterialModelOutputChange,
        };
        assert!(contradictory.validate().is_err());

        let mut tampered_graph = artifact;
        tampered_graph
            .computation_graph
            .edges
            .first_mut()
            .expect("fixture graph edge")
            .to_node_id = "unbound_decision".to_owned();
        assert!(tampered_graph.validate().is_err());
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
            entry_shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.5)),
            actual_baseline: ActualExecutionBaseline::fixture(),
            horizon_end: entry_at + Duration::hours(1),
            points: vec![
                executable_point(
                    entry_at + Duration::minutes(10),
                    entry_at + Duration::minutes(10),
                    Price::new(dec!(0.4)),
                    "point-1",
                ),
                executable_point(
                    entry_at + Duration::minutes(20),
                    entry_at + Duration::minutes(20),
                    Price::new(dec!(0.65)),
                    "point-2",
                ),
            ],
        })
        .expect("valid trajectory");
        assert_eq!(
            trajectory.excursions,
            TrajectoryExcursionEvaluation::Evaluated {
                max_adverse_excursion_bps: Bps::new(dec!(-2000)),
                max_favorable_excursion_bps: Bps::new(dec!(3000)),
            }
        );

        let counterfactual = PolicyCounterfactualOutcome::replay(
            &trajectory,
            hash("trajectory"),
            hash("exit-policy"),
            AlternativeExitPolicy::FirstBarrier {
                take_profit_bps: Bps::new(dec!(2500)),
                stop_loss_bps: Bps::new(dec!(-2500)),
                max_holding_secs: 3600,
            },
        )
        .expect("policy replay");
        let PolicyCounterfactualEvaluation::Evaluated {
            counterfactual_exit_price,
            counterfactual_net_return_bps,
            missed_return_bps,
            ..
        } = counterfactual.evaluation
        else {
            panic!("expected evaluated policy replay");
        };
        assert_eq!(counterfactual_exit_price, Price::new(dec!(0.65)));
        assert_eq!(counterfactual_net_return_bps, Bps::new(dec!(3000)));
        assert_eq!(missed_return_bps, Bps::new(dec!(2000)));
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
            entry_shares: Shares::new(dec!(100)),
            entry_price: Price::new(dec!(0.5)),
            actual_baseline: ActualExecutionBaseline::fixture(),
            horizon_end: lineage.source_cutoff,
            points: vec![executable_point(
                entry_at + Duration::minutes(1),
                lineage.source_cutoff + Duration::seconds(1),
                Price::new(dec!(0.6)),
                "future",
            )],
        });
        assert!(result.is_err());
    }
}
