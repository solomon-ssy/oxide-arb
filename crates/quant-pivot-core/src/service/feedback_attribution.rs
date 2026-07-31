//! Production materialization of immutable attribution artifacts.

use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    fmt::Display,
    sync::Arc,
};

use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, research::ResearchError,
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, QuantFeatureEventRow, QuantModelInputEventRow,
        QuantServingEvidenceCompletionRow,
    },
    domain::{
        ports::FeedbackAttributionPlanJobParams,
        quant::{
            AttributionArtifactInfo, ExecutionAttemptOutcomeInfo, FactorDefinitionInfo,
            FactorValueInfo, FeatureVectorInfo, FeedbackCohortCandidate, FeedbackCohortDecision,
            FeedbackCohortPageQuery, FeedbackRecommendationContext, JobProgressSink,
            MarketSelectionMemberInfo, NewAttributionArtifact,
        },
    },
    enums::{
        model::ModelFamily,
        quant::{AttributionCohort, FeedbackCohort, OutcomeSide},
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, FactorDefinitionId, FeatureVectorId, ModelRunId, ModelVersionId,
        OutcomeTokenBinding, Price, ResearchJobProgress, Shares,
        factor::{FactorDefinitionRef, FactorServingPlane},
    },
};
use quant_pivot_repository::traits::{
    AttributionArtifactRepository, AttributionArtifactWriteOutcome,
    ExecutionAttemptOutcomeRepository, FactorRepository, FeatureRepository,
    FeedbackCohortRepository, MarketSelectionRepository, ModelRegistryRepository, PolicyRepository,
    QuantFactReadRepository, ServingEvidenceRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    attribution::{
        AlternativeExitPolicy, AttributionArtifact, AttributionArtifactCodec, AttributionLineage,
        CounterfactualIntervention, DecisionCandidateKey, DecisionCandidateScore,
        DecisionCounterfactualArtifact, DecisionCounterfactualInput, DecisionReplayPolicy,
        ExecutionTrajectoryArtifact, ExecutionTrajectoryInput, OutcomeAssociationArtifact,
        OutcomeAssociationInput, OutcomeAssociationSample, OutcomeAssociationTarget,
        PolicyCounterfactualOutcome, PredictionContribution, PredictionExplanationArtifact,
        TrajectoryPoint, TreeEnsembleInput, WeightedFactorExplanationInput,
    },
    factors::FactorValue,
    features::FeatureVector,
    model::{
        ModelArtifact,
        artifact::{ClassicalModelPayload, ModelPayload, WeightedFactorModelPayload},
        factor_heads::score_factor_heads,
        model_input_contract_hash,
    },
    precision::RESEARCH_DECIMAL_SCALE,
    selection::SelectedMarket,
};
#[cfg(feature = "ml-classical")]
use quant_pivot_research::{
    attribution::TreeEnsembleSpec,
    model::{ClassicalDecisionProjection, InferenceMatrixRow},
};
use rust_decimal::{Decimal, prelude::FromPrimitive};
use tokio_util::sync::CancellationToken;

use crate::{
    observability::{metrics_hub::MetricsHub, serving_evidence::verify_completion},
    projection::inference_context::build_market_inference_context,
    service::feedback_cohort::evaluate_feedback_cohort,
};

const FACTOR_VALUES_DOMAIN: &str = "quant-pivot/attribution-factor-values";
const FACTOR_VALUES_VERSION: u32 = 1;
const ASSOCIATION_ESTIMATOR_DOMAIN: &str = "quant-pivot/attribution-association-estimator";
const ASSOCIATION_CONDITIONING_DOMAIN: &str = "quant-pivot/attribution-association-conditioning";
const ASSOCIATION_COHORT_DOMAIN: &str = "quant-pivot/attribution-association-cohort";
const ASSOCIATION_EXPLANATIONS_DOMAIN: &str = "quant-pivot/attribution-explanation-set";
const ASSOCIATION_RESOLUTIONS_DOMAIN: &str = "quant-pivot/attribution-resolution-set";
const ASSOCIATION_EXECUTIONS_DOMAIN: &str = "quant-pivot/attribution-execution-set";
const ASSOCIATION_VERSION: u32 = 1;
const TRAJECTORY_CONTRACT_DOMAIN: &str = "quant-pivot/execution-trajectory-contract";
const TRAJECTORY_CONTRACT_VERSION: u32 = 1;
const EXIT_POLICY_DOMAIN: &str = "quant-pivot/policy-counterfactual-exit";
const EXIT_POLICY_VERSION: u32 = 2;
const ATTRIBUTION_PAGE_LIMIT: u32 = 1_000;

/// Dependencies for [`FeedbackAttributionMaterializer`].
pub struct FeedbackAttributionDeps {
    pub cohorts: Arc<dyn FeedbackCohortRepository>,
    pub factors: Arc<dyn FactorRepository>,
    pub features: Arc<dyn FeatureRepository>,
    pub models: Arc<dyn ModelRegistryRepository>,
    pub selections: Arc<dyn MarketSelectionRepository>,
    pub policies: Arc<dyn PolicyRepository>,
    pub attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    pub facts: Arc<dyn QuantFactReadRepository>,
    pub serving_evidence: Arc<dyn ServingEvidenceRepository>,
    pub index: Arc<dyn AttributionArtifactRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub metrics: Arc<MetricsHub>,
}

/// Materialization summary reported by the `AttributionPlan` job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackAttributionSummary {
    pub prediction_explanations: u64,
    pub decision_counterfactuals: u64,
    pub outcome_associations: u64,
    pub execution_trajectories: u64,
    pub policy_counterfactuals: u64,
}

struct TrajectorySeed {
    context: FeedbackRecommendationContext,
    attempt: ExecutionAttemptOutcomeInfo,
    rollup_hash: ContentHash,
    entry_at: DateTime<Utc>,
    entry_price: Price,
    horizon_end: DateTime<Utc>,
}

struct MaterializedPrediction {
    context: FeedbackRecommendationContext,
    explanation: PredictionExplanationArtifact,
    explanation_hash: ContentHash,
    outcome_hash: ContentHash,
    outcome: Decimal,
}

struct DecisionUniverse {
    scores: Vec<DecisionCandidateScore>,
    policy: DecisionReplayPolicy,
    replay: DecisionReplayModel,
}

enum DecisionReplayModel {
    Weighted(BTreeMap<DecisionCandidateKey, WeightedReplayState>),
    #[cfg(feature = "ml-classical")]
    Tree(Box<TreeReplayModel>),
}

struct WeightedReplayState {
    yes_alpha: Decimal,
    score_multiplier: Decimal,
    alpha_deadband: Decimal,
}

struct WeightedUniverseEvidence<'a> {
    context: &'a FeedbackRecommendationContext,
    artifact: &'a ModelArtifact,
    payload: &'a WeightedFactorModelPayload,
    plane: &'a FactorServingPlane,
    factor_rows: &'a [FactorValueInfo],
    definitions: &'a HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    features: &'a HashMap<FeatureVectorId, FeatureVectorInfo>,
}

struct WeightedCandidateReplay {
    score: DecisionCandidateScore,
    state: WeightedReplayState,
}

#[cfg(feature = "ml-classical")]
struct TreeReplayModel {
    payload: ClassicalModelPayload,
    prediction_horizon_secs: u64,
    ensemble: TreeEnsembleSpec,
    candidates: BTreeMap<DecisionCandidateKey, TreeReplayState>,
}

#[cfg(feature = "ml-classical")]
struct TreeReplayState {
    input: TreeEnsembleInput,
    row: InferenceMatrixRow,
}

#[cfg(feature = "ml-classical")]
struct TreeUniverseEvidence<'a> {
    context: &'a FeedbackRecommendationContext,
    artifact: &'a ModelArtifact,
    payload: &'a ClassicalModelPayload,
    run_inputs: &'a [QuantModelInputEventRow],
    features: &'a HashMap<FeatureVectorId, FeatureVectorInfo>,
}

#[cfg(feature = "ml-classical")]
struct TreeCandidateReplay {
    score: DecisionCandidateScore,
    state: TreeReplayState,
}

struct CounterfactualReplay {
    score: Decimal,
    interventions: Vec<CounterfactualIntervention>,
}

impl DecisionUniverse {
    fn counterfactual(
        &self,
        prediction: &MaterializedPrediction,
        target_key: &DecisionCandidateKey,
        baseline_score: Decimal,
    ) -> QuantResult<Option<CounterfactualReplay>> {
        match &self.replay {
            DecisionReplayModel::Weighted(states) => {
                let state = states.get(target_key).ok_or_else(|| {
                    FeedbackAttributionMaterializer::invalid(format!(
                        "weighted replay state omitted {}/{}",
                        target_key.market_id, target_key.token_id
                    ))
                })?;
                for contribution in Self::material_contributions(prediction) {
                    let Some(observed_value) = contribution.input_value else {
                        continue;
                    };
                    let counterfactual_alpha =
                        prediction.explanation.predicted_output - contribution.contribution;
                    if counterfactual_alpha.abs() <= state.alpha_deadband
                        || counterfactual_alpha.is_sign_positive()
                            != state.yes_alpha.is_sign_positive()
                    {
                        continue;
                    }
                    let score = (counterfactual_alpha.abs() * state.score_multiplier)
                        .round_dp(RESEARCH_DECIMAL_SCALE)
                        .clamp(Decimal::ZERO, Decimal::ONE);
                    if score == baseline_score {
                        continue;
                    }
                    return Ok(Some(CounterfactualReplay {
                        score,
                        interventions: vec![CounterfactualIntervention {
                            input_name: contribution.input_name.clone(),
                            observed_value,
                            intervened_value: Decimal::ZERO,
                            affected_nodes: DecisionReplayPolicy::affected_nodes()
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                        }],
                    }));
                }
                Ok(None)
            }
            #[cfg(feature = "ml-classical")]
            DecisionReplayModel::Tree(model) => {
                let state = model.candidates.get(target_key).ok_or_else(|| {
                    FeedbackAttributionMaterializer::invalid(format!(
                        "GBDT replay state omitted {}/{}",
                        target_key.market_id, target_key.token_id
                    ))
                })?;
                for contribution in Self::material_contributions(prediction) {
                    let Some(observed_value) = contribution.input_value else {
                        continue;
                    };
                    let Some(feature_index) = model
                        .ensemble
                        .feature_names
                        .iter()
                        .position(|name| name == &contribution.input_name)
                    else {
                        return Err(FeedbackAttributionMaterializer::invalid(format!(
                            "GBDT explanation input {} is absent from its ensemble",
                            contribution.input_name
                        )));
                    };
                    if observed_value.is_zero() {
                        continue;
                    }
                    let mut input = state.input.clone();
                    input.values[feature_index] = Some(Decimal::ZERO);
                    let raw_prediction = model.ensemble.predict(&input)?;
                    let Some(projected) = ClassicalDecisionProjection::try_project(
                        &model.payload,
                        model.prediction_horizon_secs,
                        raw_prediction,
                        &state.row,
                    )?
                    else {
                        continue;
                    };
                    if projected.token_id != target_key.token_id
                        || projected.composite_score.inner() == baseline_score
                    {
                        continue;
                    }
                    return Ok(Some(CounterfactualReplay {
                        score: projected.composite_score.inner(),
                        interventions: vec![CounterfactualIntervention {
                            input_name: contribution.input_name.clone(),
                            observed_value,
                            intervened_value: Decimal::ZERO,
                            affected_nodes: DecisionReplayPolicy::affected_nodes()
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                        }],
                    }));
                }
                Ok(None)
            }
        }
    }

    fn material_contributions(prediction: &MaterializedPrediction) -> Vec<&PredictionContribution> {
        let mut contributions = prediction
            .explanation
            .contributions
            .iter()
            .filter(|contribution| !contribution.contribution.is_zero())
            .collect::<Vec<_>>();
        contributions.sort_by(|left, right| {
            right
                .contribution
                .abs()
                .cmp(&left.contribution.abs())
                .then_with(|| left.input_name.cmp(&right.input_name))
        });
        contributions
    }
}

struct ServingEvidencePage {
    completion_hashes: HashMap<ModelRunId, ContentHash>,
    model_inputs: HashMap<ModelRunId, Vec<QuantModelInputEventRow>>,
}

struct TreeInputEvidenceContract<'a> {
    feature_names: &'a [String],
    model_family: ModelFamily,
    model_version_id: ModelVersionId,
    input_contract_hash: ContentHash,
    input_transform_hash: ContentHash,
    training_input_hash: ContentHash,
}

impl TreeInputEvidenceContract<'_> {
    fn encode(
        &self,
        rows: &[&QuantModelInputEventRow],
        model_run_id: ModelRunId,
    ) -> QuantResult<TreeEnsembleInput> {
        if rows.len() != self.feature_names.len() {
            return Err(FeedbackAttributionMaterializer::invalid(format!(
                "model run {model_run_id} has {} encoded inputs but GBDT requires {}",
                rows.len(),
                self.feature_names.len()
            )));
        }
        let mut encoded = BTreeMap::new();
        for row in rows {
            if row.model_run_id != model_run_id
                || row.model_version_id != self.model_version_id
                || row.model_family != self.model_family.to_string()
                || row.input_contract_hash != self.input_contract_hash.to_string()
                || row.transform_hash != self.input_transform_hash.to_string()
                || row.training_input_hash != self.training_input_hash.to_string()
            {
                return Err(FeedbackAttributionMaterializer::invalid(format!(
                    "model input {} for run {model_run_id} differs from its serving contract",
                    row.encoded_column
                )));
            }
            if encoded.insert(row.encoded_column.as_str(), *row).is_some() {
                return Err(FeedbackAttributionMaterializer::invalid(format!(
                    "model run {model_run_id} contains duplicate encoded column {}",
                    row.encoded_column
                )));
            }
        }
        let values = self
            .feature_names
            .iter()
            .map(|name| {
                let row = encoded.get(name.as_str()).ok_or_else(|| {
                    FeedbackAttributionMaterializer::invalid(format!(
                        "model run {model_run_id} omitted GBDT encoded column {name}"
                    ))
                })?;
                let bits = row.encoded_value_bits.ok_or_else(|| {
                    FeedbackAttributionMaterializer::invalid(format!(
                        "GBDT encoded column {name} has no numeric value"
                    ))
                })?;
                let value = f64::from_bits(bits);
                if !value.is_finite() {
                    return Err(FeedbackAttributionMaterializer::invalid(format!(
                        "GBDT encoded column {name} is non-finite"
                    )));
                }
                Decimal::from_f64(value).map(Some).ok_or_else(|| {
                    FeedbackAttributionMaterializer::invalid(format!(
                        "GBDT encoded column {name} cannot be represented as Decimal"
                    ))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(TreeEnsembleInput { values })
    }
}

struct PredictionPageEvidence<'a> {
    params: &'a FeedbackAttributionPlanJobParams,
    factor_rows: &'a [FactorValueInfo],
    definitions: &'a HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    selection_members: &'a [MarketSelectionMemberInfo],
    serving: &'a ServingEvidencePage,
}

/// Builds explanation evidence from the exact frozen production preimages.
pub struct FeedbackAttributionMaterializer {
    cohorts: Arc<dyn FeedbackCohortRepository>,
    factors: Arc<dyn FactorRepository>,
    features: Arc<dyn FeatureRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    selections: Arc<dyn MarketSelectionRepository>,
    policies: Arc<dyn PolicyRepository>,
    attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    facts: Arc<dyn QuantFactReadRepository>,
    serving_evidence: Arc<dyn ServingEvidenceRepository>,
    index: Arc<dyn AttributionArtifactRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    metrics: Arc<MetricsHub>,
}

impl FeedbackAttributionMaterializer {
    #[must_use]
    pub fn new(deps: FeedbackAttributionDeps) -> Self {
        Self {
            cohorts: deps.cohorts,
            factors: deps.factors,
            features: deps.features,
            models: deps.models,
            selections: deps.selections,
            policies: deps.policies,
            attempts: deps.attempts,
            facts: deps.facts,
            serving_evidence: deps.serving_evidence,
            index: deps.index,
            artifacts: deps.artifacts,
            metrics: deps.metrics,
        }
    }

    pub async fn materialize(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackAttributionSummary> {
        let cohort_snapshot = params.cohort_snapshot()?;
        let mut cursor = None;
        let mut prediction_explanations = 0_u64;
        let mut association_samples =
            HashMap::<ModelVersionId, Vec<OutcomeAssociationSample>>::new();
        let mut decision_predictions = Vec::new();
        let mut model_cache = HashMap::new();
        loop {
            Self::require_active(cancel)?;
            let query = FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ModelLearning,
                cohort_snapshot.clone(),
                cursor,
                ATTRIBUTION_PAGE_LIMIT,
            )
            .map_err(Self::invalid_contract)?;
            let page = self.cohorts.list_page(query).await?;
            let eligible = page
                .candidates()
                .iter()
                .filter_map(|candidate| {
                    match evaluate_feedback_cohort(
                        FeedbackCohort::ModelLearning,
                        &cohort_snapshot,
                        candidate.context(),
                        candidate.resolution_outcome(),
                        None,
                    ) {
                        Ok(FeedbackCohortDecision::Eligible(_)) => Some(Ok(candidate.clone())),
                        Ok(
                            FeedbackCohortDecision::Censored(_)
                            | FeedbackCohortDecision::Excluded(_),
                        ) => None,
                        Err(error) => Some(Err(Self::invalid_contract(error))),
                    }
                })
                .collect::<QuantResult<Vec<_>>>()?;
            let page_samples = self
                .materialize_page(params, eligible, &mut model_cache)
                .await?;
            prediction_explanations =
                prediction_explanations.saturating_add(u64::try_from(page_samples.len()).map_err(
                    |error| Self::invalid(format!("attribution page size overflow: {error}")),
                )?);
            for prediction in page_samples {
                let model_version_id = prediction.context.model_version_id();
                association_samples
                    .entry(model_version_id)
                    .or_default()
                    .push(OutcomeAssociationSample {
                        recommendation_id: prediction.context.recommendation_id(),
                        explanation_hash: prediction.explanation_hash,
                        outcome_hash: prediction.outcome_hash,
                        outcome: prediction.outcome,
                        contributions: prediction.explanation.contributions.clone(),
                    });
                decision_predictions.push(prediction);
            }
            progress.report(ResearchJobProgress::indeterminate(
                format!("attribution-predictions:{prediction_explanations}"),
                prediction_explanations,
            ));
            cursor = page.next_cursor();
            if cursor.is_none() {
                break;
            }
        }
        let outcome_associations = self
            .materialize_associations(params, association_samples)
            .await?;
        let decision_counterfactuals = self
            .materialize_decisions(params, &decision_predictions, &model_cache)
            .await?;
        let (execution_trajectories, policy_counterfactuals) = self
            .materialize_trajectories(params, progress, cancel)
            .await?;
        Ok(FeedbackAttributionSummary {
            prediction_explanations,
            decision_counterfactuals,
            outcome_associations,
            execution_trajectories,
            policy_counterfactuals,
        })
    }

    async fn materialize_page(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        candidates: Vec<FeedbackCohortCandidate>,
        model_cache: &mut HashMap<ModelVersionId, ModelArtifact>,
    ) -> QuantResult<Vec<MaterializedPrediction>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let feature_vector_ids = candidates
            .iter()
            .map(|candidate| candidate.context().feature_vector_id())
            .collect::<Vec<_>>();
        let factor_rows = self
            .factors
            .find_values_by_vectors(&feature_vector_ids)
            .await?;
        let factor_definition_ids = candidates
            .iter()
            .flat_map(|candidate| candidate.context().factor_definition_versions())
            .copied()
            .collect::<Vec<_>>();
        let definitions = self
            .factors
            .find_definitions_by_ids(&factor_definition_ids)
            .await?;
        let selection_ids = candidates
            .iter()
            .map(|candidate| candidate.context().market_selection_id())
            .collect::<Vec<_>>();
        let selection_members = self
            .selections
            .list_snapshot_members(&selection_ids)
            .await?;
        let definition_index = definitions
            .into_iter()
            .map(|definition| (definition.factor_definition_id, definition))
            .collect::<HashMap<_, _>>();
        let mut serving_run_ids = candidates
            .iter()
            .map(|candidate| candidate.context().model_run_id())
            .collect::<Vec<_>>();
        serving_run_ids.sort_by_key(|run_id| run_id.as_uuid());
        serving_run_ids.dedup();
        let serving = self
            .load_serving_evidence(&serving_run_ids, params.cutoff)
            .await?;
        for candidate in &candidates {
            let context = candidate.context();
            if !serving
                .model_inputs
                .get(&context.model_run_id())
                .is_some_and(|rows| {
                    rows.iter()
                        .any(|row| row.feature_vector_id == context.feature_vector_id())
                })
            {
                return Err(Self::invalid(format!(
                    "model run {} completion omitted recommendation vector {}",
                    context.model_run_id(),
                    context.feature_vector_id()
                )));
            }
        }
        let page_evidence = PredictionPageEvidence {
            params,
            factor_rows: &factor_rows,
            definitions: &definition_index,
            selection_members: &selection_members,
            serving: &serving,
        };
        let mut samples = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let context = candidate.context();
            let model_version_id = context.model_version_id();
            if let Entry::Vacant(entry) = model_cache.entry(model_version_id) {
                let version = self
                    .models
                    .find_model_version(&model_version_id)
                    .await?
                    .ok_or_else(|| {
                        Self::invalid(format!(
                            "attribution model version {model_version_id} does not exist"
                        ))
                    })?;
                let artifact =
                    ModelArtifact::load_verified(self.artifacts.as_ref(), &version).await?;
                entry.insert(artifact);
            }
            let artifact = model_cache.get(&model_version_id).ok_or_else(|| {
                Self::invalid(format!(
                    "attribution model cache lost version {model_version_id}"
                ))
            })?;
            let resolution_hash = candidate
                .resolution_outcome()
                .ok_or_else(|| {
                    Self::invalid("eligible model-learning candidate lost resolution truth")
                })?
                .outcome_hash;
            let explanation =
                self.build_prediction(&page_evidence, &candidate, artifact, resolution_hash)?;
            let persisted = self
                .persist(AttributionArtifact::PredictionExplanation(Box::new(
                    explanation.clone(),
                )))
                .await?;
            let resolution = candidate.resolution_outcome().ok_or_else(|| {
                Self::invalid("eligible model-learning candidate lost resolution truth")
            })?;
            samples.push(MaterializedPrediction {
                context: context.clone(),
                explanation,
                explanation_hash: persisted.artifact_hash,
                outcome_hash: resolution.outcome_hash,
                outcome: resolution.token_payout_ratio.inner(),
            });
        }
        Ok(samples)
    }

    fn build_prediction(
        &self,
        evidence: &PredictionPageEvidence<'_>,
        candidate: &FeedbackCohortCandidate,
        artifact: &ModelArtifact,
        resolution_hash: ContentHash,
    ) -> QuantResult<PredictionExplanationArtifact> {
        match artifact.payload() {
            ModelPayload::WeightedFactor(payload) => {
                self.weighted_prediction(evidence, candidate, artifact, payload, resolution_hash)
            }
            ModelPayload::Classical(payload) => {
                self.tree_prediction(evidence, candidate, artifact, payload, resolution_hash)
            }
            ModelPayload::SellScorer(_) => Err(ResearchError::NotEligible {
                code: "exact_prediction_explanation_unavailable",
                detail: format!(
                    "sell model {} is not part of the recommendation attribution cohort",
                    candidate.context().model_version_id()
                ),
            }
            .into()),
        }
    }

    fn weighted_prediction(
        &self,
        evidence: &PredictionPageEvidence<'_>,
        candidate: &FeedbackCohortCandidate,
        artifact: &ModelArtifact,
        payload: &WeightedFactorModelPayload,
        resolution_hash: ContentHash,
    ) -> QuantResult<PredictionExplanationArtifact> {
        let context = candidate.context();
        let plane = &artifact
            .header()
            .serving_contract()
            .bindings()
            .factors
            .plane;
        let values = Self::factor_values(
            candidate,
            evidence.factor_rows,
            evidence.definitions,
            plane
                .definitions()
                .iter()
                .map(FactorDefinitionRef::factor_definition_id),
        )?;
        let member = evidence
            .selection_members
            .iter()
            .find(|member| {
                member.market_selection_id == context.market_selection_id()
                    && &member.market_id == context.market_id()
            })
            .ok_or_else(|| {
                Self::invalid(format!(
                    "attribution selection {} omitted market {}",
                    context.market_selection_id(),
                    context.market_id()
                ))
            })?;
        let no_token_id = member.secondary_token_id.clone().ok_or_else(|| {
            Self::invalid(format!(
                "attribution market {} is not an exact binary token pair",
                context.market_id()
            ))
        })?;
        let expected_token_id = match context.outcome_side() {
            OutcomeSide::Yes => &member.primary_token_id,
            OutcomeSide::No => &no_token_id,
        };
        if context.token_id() != expected_token_id {
            return Err(Self::invalid(format!(
                "recommendation {} token differs from its frozen selection orientation",
                context.recommendation_id()
            )));
        }
        let binding = OutcomeTokenBinding::try_new(
            context.market_id().clone(),
            member.primary_token_id.clone(),
            no_token_id,
            member.primary_token_id.clone(),
            OutcomeSide::Yes,
        )
        .map_err(Self::invalid_contract)?;
        let factor_values_hash = CanonicalDigest::content_hash_typed(
            FACTOR_VALUES_DOMAIN,
            FACTOR_VALUES_VERSION,
            &evidence
                .factor_rows
                .iter()
                .filter(|row| {
                    row.feature_vector_id == context.feature_vector_id()
                        && row.model_run_id == context.model_run_id()
                        && &row.market_id == context.market_id()
                })
                .collect::<Vec<_>>(),
        )?;
        let completion_hash = evidence
            .serving
            .completion_hashes
            .get(&context.model_run_id())
            .copied()
            .ok_or_else(|| {
                Self::invalid(format!(
                    "model run {} has no verified serving completion",
                    context.model_run_id()
                ))
            })?;
        let lineage = AttributionLineage::try_new(
            evidence.params.feedback_cycle_id,
            AttributionCohort::Evaluation,
            evidence.params.cutoff,
            evidence.params.generated_at,
            vec![
                evidence.params.truth_artifact.content_hash,
                artifact.content_hash()?,
                artifact.header().serving_contract().contract_hash(),
                factor_values_hash,
                completion_hash,
                resolution_hash,
            ],
        )?;
        PredictionExplanationArtifact::weighted_factor(
            lineage,
            WeightedFactorExplanationInput {
                model_version_id: context.model_version_id(),
                recommendation_id: context.recommendation_id(),
                model_artifact_hash: artifact.content_hash()?,
                input_contract_hash: model_input_contract_hash(&payload.input_contract)?,
                factors: &values,
                plane,
                spec: &payload.factor_head,
                outcome_binding: &binding,
            },
        )
        .inspect_err(|error| {
            if matches!(
                error,
                QuantError::Research(ResearchError::ValidationMethodology { detail })
                    if detail.contains("prediction explanation violates efficiency")
            ) {
                self.metrics
                    .record_attribution_efficiency_failure("weighted_closed_form");
            }
        })
    }

    fn tree_prediction(
        &self,
        evidence: &PredictionPageEvidence<'_>,
        candidate: &FeedbackCohortCandidate,
        artifact: &ModelArtifact,
        payload: &ClassicalModelPayload,
        resolution_hash: ContentHash,
    ) -> QuantResult<PredictionExplanationArtifact> {
        let context = candidate.context();
        let tree_shap = payload
            .tree_shap
            .as_ref()
            .ok_or_else(|| ResearchError::NotEligible {
                code: "exact_prediction_explanation_unavailable",
                detail: format!(
                    "classical model {} has no exact local explanation contract",
                    context.model_version_id()
                ),
            })?;
        let completion_hash = evidence
            .serving
            .completion_hashes
            .get(&context.model_run_id())
            .copied()
            .ok_or_else(|| {
                Self::invalid(format!(
                    "model run {} has no verified serving completion",
                    context.model_run_id()
                ))
            })?;
        let run_rows = evidence
            .serving
            .model_inputs
            .get(&context.model_run_id())
            .ok_or_else(|| {
                Self::invalid(format!(
                    "model run {} has no verified model inputs",
                    context.model_run_id()
                ))
            })?;
        let rows = run_rows
            .iter()
            .filter(|row| {
                row.model_version_id == context.model_version_id()
                    && &row.market_id == context.market_id()
                    && row.feature_vector_id == context.feature_vector_id()
            })
            .collect::<Vec<_>>();
        let input = Self::tree_input(payload, artifact, &rows, context.model_run_id())?;
        let lineage = AttributionLineage::try_new(
            evidence.params.feedback_cycle_id,
            AttributionCohort::Evaluation,
            evidence.params.cutoff,
            evidence.params.generated_at,
            vec![
                evidence.params.truth_artifact.content_hash,
                artifact.content_hash()?,
                artifact.header().serving_contract().contract_hash(),
                tree_shap.ensemble_hash,
                completion_hash,
                resolution_hash,
            ],
        )?;
        PredictionExplanationArtifact::tree_shap(
            lineage,
            context.model_version_id(),
            context.recommendation_id(),
            artifact.content_hash()?,
            &tree_shap.ensemble,
            &input,
        )
        .inspect_err(|_| {
            self.metrics
                .record_attribution_efficiency_failure("exact_tree_shap");
        })
    }

    async fn load_serving_evidence(
        &self,
        run_ids: &[ModelRunId],
        cutoff: DateTime<Utc>,
    ) -> QuantResult<ServingEvidencePage> {
        let mut run_ids = run_ids.to_vec();
        run_ids.sort_by_key(|run_id| run_id.as_uuid());
        run_ids.dedup();
        let cutoff_millis = cutoff.timestamp_millis();
        let completions = Self::canonical_completions(
            self.serving_evidence
                .completions_for_runs(&run_ids)
                .await?
                .into_iter()
                .filter(|row| row.ingestion_time <= cutoff_millis)
                .collect(),
        )?;
        if completions.len() != run_ids.len() {
            return Err(Self::invalid(format!(
                "attribution requires {} serving completions visible by cutoff, found {}",
                run_ids.len(),
                completions.len()
            )));
        }
        let mut vector_ids = Vec::new();
        for run_id in &run_ids {
            let marker = completions.get(run_id).ok_or_else(|| {
                Self::invalid(format!(
                    "model run {run_id} has no serving completion visible by cutoff"
                ))
            })?;
            let mut marker_vectors = serde_json::from_str::<Vec<_>>(
                &marker.feature_vector_ids_json,
            )
            .map_err(|error| {
                Self::invalid(format!(
                    "model run {run_id} has invalid completion vector ids: {error}"
                ))
            })?;
            vector_ids.append(&mut marker_vectors);
        }
        vector_ids.sort_by_key(ToString::to_string);
        vector_ids.dedup();
        let inputs = Self::canonical_model_inputs(
            self.serving_evidence
                .model_inputs_for_runs(&run_ids)
                .await?
                .into_iter()
                .filter(|row| row.ingestion_time <= cutoff_millis)
                .collect(),
        )?;
        let features = Self::canonical_feature_rows(
            self.serving_evidence
                .feature_cells_for_vectors(&vector_ids)
                .await?
                .into_iter()
                .filter(|row| row.ingestion_time <= cutoff_millis)
                .collect(),
        )?;
        let mut inputs_by_run = HashMap::new();
        for row in inputs {
            inputs_by_run
                .entry(row.model_run_id)
                .or_insert_with(Vec::new)
                .push(row);
        }
        let mut completion_hashes = HashMap::new();
        for run_id in run_ids {
            let marker = completions.get(&run_id).ok_or_else(|| {
                Self::invalid(format!("model run {run_id} lost its canonical completion"))
            })?;
            let run_inputs = inputs_by_run.get(&run_id).ok_or_else(|| {
                Self::invalid(format!("model run {run_id} has no durable encoded inputs"))
            })?;
            let marker_vectors =
                serde_json::from_str::<HashSet<FeatureVectorId>>(&marker.feature_vector_ids_json)
                    .map_err(|error| {
                    Self::invalid(format!(
                        "model run {run_id} has invalid completion vector ids: {error}"
                    ))
                })?;
            let run_features = features
                .iter()
                .filter(|row| marker_vectors.contains(&row.feature_vector_id))
                .cloned()
                .collect::<Vec<_>>();
            verify_completion(marker, &run_features, run_inputs)?;
            completion_hashes.insert(
                run_id,
                marker
                    .completion_hash
                    .parse()
                    .map_err(Self::invalid_contract)?,
            );
        }
        Ok(ServingEvidencePage {
            completion_hashes,
            model_inputs: inputs_by_run,
        })
    }

    fn canonical_completions(
        rows: Vec<QuantServingEvidenceCompletionRow>,
    ) -> QuantResult<HashMap<ModelRunId, QuantServingEvidenceCompletionRow>> {
        let mut canonical = HashMap::<ModelRunId, QuantServingEvidenceCompletionRow>::new();
        for row in rows {
            if let Some(previous) = canonical.get(&row.model_run_id) {
                if previous.completion_hash != row.completion_hash {
                    return Err(Self::invalid(format!(
                        "conflicting serving completion retries for run {}",
                        row.model_run_id
                    )));
                }
                continue;
            }
            canonical.insert(row.model_run_id, row);
        }
        Ok(canonical)
    }

    fn canonical_model_inputs(
        rows: Vec<QuantModelInputEventRow>,
    ) -> QuantResult<Vec<QuantModelInputEventRow>> {
        let mut canonical = BTreeMap::<String, QuantModelInputEventRow>::new();
        for row in rows {
            let key = format!(
                "{}/{}/{}/{}/{}/{}",
                row.model_run_id,
                row.model_version_id,
                row.market_id,
                row.feature_vector_id,
                row.raw_input_name,
                row.encoded_column
            );
            if let Some(previous) = canonical.get(&key) {
                if previous.audit_fingerprint != row.audit_fingerprint {
                    return Err(Self::invalid(format!(
                        "conflicting model-input retries for key {key}"
                    )));
                }
                continue;
            }
            canonical.insert(key, row);
        }
        Ok(canonical.into_values().collect())
    }

    fn canonical_feature_rows(
        rows: Vec<QuantFeatureEventRow>,
    ) -> QuantResult<Vec<QuantFeatureEventRow>> {
        let mut canonical = BTreeMap::<String, QuantFeatureEventRow>::new();
        for row in rows {
            let key = format!("{}/{}", row.feature_vector_id, row.feature_name);
            if let Some(previous) = canonical.get(&key) {
                if previous.audit_fingerprint != row.audit_fingerprint {
                    return Err(Self::invalid(format!(
                        "conflicting feature-evidence retries for key {key}"
                    )));
                }
                continue;
            }
            canonical.insert(key, row);
        }
        Ok(canonical.into_values().collect())
    }

    async fn materialize_associations(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        mut groups: HashMap<ModelVersionId, Vec<OutcomeAssociationSample>>,
    ) -> QuantResult<u64> {
        let mut count = 0_u64;
        for (model_version_id, samples) in &mut groups {
            samples.sort_by_key(|sample| sample.recommendation_id.as_uuid());
            if samples.len() < 3 || !Self::association_varies(samples) {
                continue;
            }
            let estimator_contract_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_ESTIMATOR_DOMAIN,
                ASSOCIATION_VERSION,
                &"univariate_ols_classical_95pct_noncausal",
            )?;
            let conditioning_policy_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_CONDITIONING_DOMAIN,
                ASSOCIATION_VERSION,
                &(
                    &params.window,
                    *model_version_id,
                    OutcomeAssociationTarget::FinalTokenPayoutRatio,
                ),
            )?;
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
            let cohort_manifest_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_COHORT_DOMAIN,
                ASSOCIATION_VERSION,
                &recommendation_ids,
            )?;
            let explanation_set_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_EXPLANATIONS_DOMAIN,
                ASSOCIATION_VERSION,
                &explanation_hashes,
            )?;
            let resolution_set_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_RESOLUTIONS_DOMAIN,
                ASSOCIATION_VERSION,
                &resolution_hashes,
            )?;
            let execution_rollup_set_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_EXECUTIONS_DOMAIN,
                ASSOCIATION_VERSION,
                &Vec::<ContentHash>::new(),
            )?;
            let lineage = AttributionLineage::try_new(
                params.feedback_cycle_id,
                AttributionCohort::Evaluation,
                params.cutoff,
                params.generated_at,
                vec![
                    params.truth_artifact.content_hash,
                    estimator_contract_hash,
                    conditioning_policy_hash,
                    cohort_manifest_hash,
                    explanation_set_hash,
                    resolution_set_hash,
                    execution_rollup_set_hash,
                ],
            )?;
            let artifact = OutcomeAssociationArtifact::estimate(OutcomeAssociationInput {
                lineage,
                model_version_id: *model_version_id,
                target: OutcomeAssociationTarget::FinalTokenPayoutRatio,
                estimator_contract_hash,
                conditioning_policy_hash,
                cohort_manifest_hash,
                explanation_set_hash,
                resolution_set_hash,
                execution_rollup_set_hash,
                samples: samples.clone(),
            })?;
            self.persist(AttributionArtifact::OutcomeAssociation(Box::new(artifact)))
                .await?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn association_varies(samples: &[OutcomeAssociationSample]) -> bool {
        samples.first().is_some_and(|first| {
            first
                .contributions
                .iter()
                .enumerate()
                .any(|(index, contribution)| {
                    samples.iter().skip(1).any(|sample| {
                        sample.contributions.get(index).is_some_and(|candidate| {
                            candidate.input_name == contribution.input_name
                                && candidate.contribution != contribution.contribution
                        })
                    })
                })
        })
    }

    async fn materialize_decisions(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        predictions: &[MaterializedPrediction],
        model_cache: &HashMap<ModelVersionId, ModelArtifact>,
    ) -> QuantResult<u64> {
        let mut grouped = HashMap::<ModelRunId, Vec<&MaterializedPrediction>>::new();
        for prediction in predictions {
            grouped
                .entry(prediction.context.model_run_id())
                .or_default()
                .push(prediction);
        }
        let mut run_ids = grouped.keys().copied().collect::<Vec<_>>();
        run_ids.sort_by_key(|model_run_id| model_run_id.as_uuid());
        let mut count = 0_u64;
        for model_run_id in run_ids {
            let predictions = grouped.get_mut(&model_run_id).ok_or_else(|| {
                Self::invalid(format!("decision replay group {model_run_id} disappeared"))
            })?;
            predictions.sort_by_key(|prediction| prediction.context.recommendation_id().as_uuid());
            let first = predictions
                .first()
                .ok_or_else(|| Self::invalid("decision replay group is empty"))?;
            let model_version_id = first.context.model_version_id();
            let artifact = model_cache.get(&model_version_id).ok_or_else(|| {
                Self::invalid(format!(
                    "decision replay model cache lost version {model_version_id}"
                ))
            })?;
            let universe = self
                .build_decision_universe(params, first, artifact)
                .await?;
            for prediction in predictions.iter() {
                let target_key = DecisionCandidateKey {
                    market_id: prediction.context.market_id().clone(),
                    token_id: prediction.context.token_id().clone(),
                };
                let baseline = universe
                    .scores
                    .iter()
                    .find(|candidate| candidate.key == target_key)
                    .ok_or_else(|| {
                        Self::invalid(format!(
                            "published recommendation {} is absent from its replayed model universe",
                            prediction.context.recommendation_id()
                        ))
                    })?;
                if baseline.score != prediction.context.composite_score().inner()
                    || baseline.confidence != prediction.context.confidence().inner()
                {
                    return Err(Self::invalid(format!(
                        "published recommendation {} differs from its exact model replay",
                        prediction.context.recommendation_id()
                    )));
                }
                let Some(replay) =
                    universe.counterfactual(prediction, &target_key, baseline.score)?
                else {
                    continue;
                };
                let lineage = AttributionLineage::try_new(
                    params.feedback_cycle_id,
                    AttributionCohort::Evaluation,
                    params.cutoff,
                    params.generated_at,
                    vec![
                        params.truth_artifact.content_hash,
                        prediction.explanation_hash,
                        universe.policy.policy_hash,
                        universe.policy.admissible_intervention_policy_hash,
                        universe.policy.dependency_graph_hash,
                        universe.policy.candidate_universe_hash,
                        artifact.content_hash()?,
                    ],
                )?;
                let counterfactual =
                    DecisionCounterfactualArtifact::replay(DecisionCounterfactualInput {
                        lineage,
                        model_version_id,
                        recommendation_id: prediction.context.recommendation_id(),
                        target_key: target_key.clone(),
                        prediction_explanation_hash: prediction.explanation_hash,
                        policy: universe.policy.clone(),
                        interventions: replay.interventions,
                        baseline_score: baseline.score,
                        counterfactual_score: replay.score,
                        target_confidence: baseline.confidence,
                        peer_scores: universe
                            .scores
                            .iter()
                            .filter(|candidate| candidate.key != target_key)
                            .cloned()
                            .collect(),
                    })?;
                self.persist(AttributionArtifact::DecisionCounterfactual(Box::new(
                    counterfactual,
                )))
                .await?;
                count = count.saturating_add(1);
            }
        }
        Ok(count)
    }

    async fn build_decision_universe(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        prediction: &MaterializedPrediction,
        artifact: &ModelArtifact,
    ) -> QuantResult<DecisionUniverse> {
        match artifact.payload() {
            ModelPayload::WeightedFactor(payload) => {
                self.build_weighted_universe(prediction, artifact, payload)
                    .await
            }
            ModelPayload::Classical(payload) => {
                #[cfg(feature = "ml-classical")]
                {
                    self.build_tree_universe(params, prediction, artifact, payload)
                        .await
                }
                #[cfg(not(feature = "ml-classical"))]
                {
                    Self::unavailable_tree_universe(params, prediction, artifact, payload)
                }
            }
            ModelPayload::SellScorer(_) => Err(ResearchError::NotEligible {
                code: "exact_decision_counterfactual_unavailable",
                detail: format!(
                    "sell model {} is outside recommendation counterfactual replay",
                    prediction.context.model_version_id()
                ),
            }
            .into()),
        }
    }

    async fn build_weighted_universe(
        &self,
        prediction: &MaterializedPrediction,
        artifact: &ModelArtifact,
        payload: &WeightedFactorModelPayload,
    ) -> QuantResult<DecisionUniverse> {
        let context = &prediction.context;
        let plane = &artifact
            .header()
            .serving_contract()
            .bindings()
            .factors
            .plane;
        let factor_rows = self
            .factors
            .list_values_for_run(&context.model_run_id())
            .await?;
        let definition_ids = plane
            .definitions()
            .iter()
            .map(FactorDefinitionRef::factor_definition_id)
            .collect::<Vec<_>>();
        let definitions = self
            .factors
            .find_definitions_by_ids(&definition_ids)
            .await?
            .into_iter()
            .map(|definition| (definition.factor_definition_id, definition))
            .collect::<HashMap<_, _>>();
        let mut vector_ids = factor_rows
            .iter()
            .map(|row| row.feature_vector_id)
            .collect::<Vec<_>>();
        vector_ids.sort_by_key(|feature_vector_id| feature_vector_id.as_uuid());
        vector_ids.dedup();
        let features = self
            .features
            .find_by_ids(&vector_ids)
            .await?
            .into_iter()
            .map(|feature| (feature.feature_vector_id, feature))
            .collect::<HashMap<_, _>>();
        let members = self
            .selections
            .list_members(&context.market_selection_id())
            .await?;
        let policy = self
            .policies
            .load_snapshot(&context.decision_policy_snapshot_id())
            .await?
            .ok_or_else(|| {
                Self::invalid(format!(
                    "decision policy snapshot {} does not exist",
                    context.decision_policy_snapshot_id()
                ))
            })?;
        let mut scores = Vec::new();
        let mut replay = BTreeMap::new();
        let evidence = WeightedUniverseEvidence {
            context,
            artifact,
            payload,
            plane,
            factor_rows: &factor_rows,
            definitions: &definitions,
            features: &features,
        };
        for member in members {
            let Some(candidate) = Self::weighted_candidate(&evidence, &member)? else {
                continue;
            };
            if replay
                .insert(candidate.score.key.clone(), candidate.state)
                .is_some()
            {
                return Err(Self::invalid(format!(
                    "weighted replay duplicated market {} candidate",
                    member.market_id
                )));
            }
            scores.push(candidate.score);
        }
        scores.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        if scores.is_empty() || scores.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(Self::invalid(format!(
                "model run {} replayed an empty or duplicate candidate universe",
                context.model_run_id()
            )));
        }
        let top_n = u32::try_from(context.top_n())
            .map_err(|error| Self::invalid(format!("report top_n is invalid: {error}")))?;
        let candidate_score_floor = policy
            .snapshot
            .model_routing
            .model
            .candidate_score_floor
            .value();
        let minimum_confidence = policy
            .snapshot
            .model_routing
            .model
            .min_model_confidence
            .value();
        Ok(DecisionUniverse {
            policy: DecisionReplayPolicy::try_new(
                policy.snapshot_hash,
                &scores,
                candidate_score_floor,
                minimum_confidence,
                top_n,
            )?,
            scores,
            replay: DecisionReplayModel::Weighted(replay),
        })
    }

    fn weighted_candidate(
        evidence: &WeightedUniverseEvidence<'_>,
        member: &MarketSelectionMemberInfo,
    ) -> QuantResult<Option<WeightedCandidateReplay>> {
        let rows = evidence
            .factor_rows
            .iter()
            .filter(|row| row.market_id == member.market_id)
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Err(Self::invalid(format!(
                "model run {} omitted factor evidence for selected market {}",
                evidence.context.model_run_id(),
                member.market_id
            )));
        }
        let feature_vector_id = rows[0].feature_vector_id;
        if rows
            .iter()
            .any(|row| row.feature_vector_id != feature_vector_id)
        {
            return Err(Self::invalid(format!(
                "model run {} mixes feature vectors for market {}",
                evidence.context.model_run_id(),
                member.market_id
            )));
        }
        let feature = evidence.features.get(&feature_vector_id).ok_or_else(|| {
            Self::invalid(format!(
                "model run {} feature vector {feature_vector_id} is missing",
                evidence.context.model_run_id()
            ))
        })?;
        let vector = FeatureVector::try_from(feature)?;
        let selected = SelectedMarket {
            market_id: member.market_id.clone(),
            event_id: member.event_id.clone(),
            category: member.category,
            primary_token_id: member.primary_token_id.clone(),
            secondary_token_id: member.secondary_token_id.clone(),
            liquidity_usd: member.liquidity_usd,
            volume_24h_usd: member.volume_24h_usd,
            source_refs: Vec::new(),
        };
        let Some(inference_context) = build_market_inference_context(&vector, &selected) else {
            return Ok(None);
        };
        let values = evidence
            .plane
            .definitions()
            .iter()
            .map(|revision| {
                let row = rows
                    .iter()
                    .find(|row| row.factor_definition_id == revision.factor_definition_id())
                    .ok_or_else(|| {
                        Self::invalid(format!(
                            "model run {} market {} omitted factor {}",
                            evidence.context.model_run_id(),
                            member.market_id,
                            revision.factor_definition_id()
                        ))
                    })?;
                let definition = evidence
                    .definitions
                    .get(&revision.factor_definition_id())
                    .ok_or_else(|| {
                        Self::invalid(format!(
                            "factor revision {} is absent from the registry",
                            revision.factor_definition_id()
                        ))
                    })?;
                FactorValue::try_from_persistence(row, definition)
            })
            .collect::<QuantResult<Vec<_>>>()?;
        for (revision, value) in evidence.plane.definitions().iter().zip(&values) {
            if !value.is_not_applicable()
                && revision.definition().required
                && value.scoring_projection(revision)?.is_none()
            {
                return Ok(None);
            }
        }
        let Some(no_token_id) = member.secondary_token_id.clone() else {
            return Err(Self::invalid(format!(
                "weighted model run {} contains non-binary market {}",
                evidence.context.model_run_id(),
                member.market_id
            )));
        };
        let binding = OutcomeTokenBinding::try_new(
            member.market_id.clone(),
            member.primary_token_id.clone(),
            no_token_id.clone(),
            member.primary_token_id.clone(),
            OutcomeSide::Yes,
        )
        .map_err(Self::invalid_contract)?;
        let substitution_reliability =
            inference_context
                .substitution_reasons
                .iter()
                .fold(Decimal::ONE, |product, reason| {
                    product
                        * evidence
                            .payload
                            .substitution_confidence_rules
                            .multiplier_for(*reason)
                });
        let horizon_multiplier = evidence.payload.horizon_multipliers.multiplier_for(
            inference_context.time_to_resolution_secs,
            evidence.artifact.header().prediction_horizon_secs(),
        );
        let score = score_factor_heads(
            &values,
            evidence.plane,
            &evidence.payload.factor_head,
            &binding,
            substitution_reliability,
            horizon_multiplier,
        )?;
        let Some(outcome_side) = score.outcome_side else {
            return Ok(None);
        };
        let token_id = match outcome_side {
            OutcomeSide::Yes => member.primary_token_id.clone(),
            OutcomeSide::No => no_token_id,
        };
        let key = DecisionCandidateKey {
            market_id: member.market_id.clone(),
            token_id,
        };
        let score_multiplier = score.context_multiplier * horizon_multiplier;
        let replayed_score = (score.yes_alpha.abs() * score_multiplier)
            .round_dp(RESEARCH_DECIMAL_SCALE)
            .clamp(Decimal::ZERO, Decimal::ONE);
        if replayed_score != score.composite_score {
            return Err(Self::invalid(format!(
                "weighted replay multiplier does not reproduce market {} score",
                member.market_id
            )));
        }
        Ok(Some(WeightedCandidateReplay {
            score: DecisionCandidateScore {
                key,
                score: score.composite_score,
                confidence: score.reliability,
            },
            state: WeightedReplayState {
                yes_alpha: score.yes_alpha,
                score_multiplier,
                alpha_deadband: evidence.payload.factor_head.alpha_deadband,
            },
        }))
    }

    #[cfg(feature = "ml-classical")]
    async fn build_tree_universe(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        prediction: &MaterializedPrediction,
        artifact: &ModelArtifact,
        payload: &ClassicalModelPayload,
    ) -> QuantResult<DecisionUniverse> {
        let context = &prediction.context;
        let tree_shap = payload
            .tree_shap
            .as_ref()
            .ok_or_else(|| ResearchError::NotEligible {
                code: "exact_decision_counterfactual_unavailable",
                detail: format!(
                    "classical model {} has no exact TreeSHAP replay contract",
                    context.model_version_id()
                ),
            })?;
        let serving = self
            .load_serving_evidence(&[context.model_run_id()], params.cutoff)
            .await?;
        let run_inputs = serving
            .model_inputs
            .get(&context.model_run_id())
            .ok_or_else(|| {
                Self::invalid(format!(
                    "model run {} has no verified model inputs",
                    context.model_run_id()
                ))
            })?;
        let mut vector_ids = run_inputs
            .iter()
            .map(|row| row.feature_vector_id)
            .collect::<Vec<_>>();
        vector_ids.sort_by_key(|vector_id| vector_id.as_uuid());
        vector_ids.dedup();
        let features = self
            .features
            .find_by_ids(&vector_ids)
            .await?
            .into_iter()
            .map(|feature| (feature.feature_vector_id, feature))
            .collect::<HashMap<_, _>>();
        let members = self
            .selections
            .list_members(&context.market_selection_id())
            .await?;
        let policy = self
            .policies
            .load_snapshot(&context.decision_policy_snapshot_id())
            .await?
            .ok_or_else(|| {
                Self::invalid(format!(
                    "decision policy snapshot {} does not exist",
                    context.decision_policy_snapshot_id()
                ))
            })?;
        let mut scores = Vec::new();
        let mut candidates = BTreeMap::new();
        let evidence = TreeUniverseEvidence {
            context,
            artifact,
            payload,
            run_inputs,
            features: &features,
        };
        for member in members {
            let Some(candidate) = Self::tree_candidate(&evidence, &member)? else {
                continue;
            };
            if candidates
                .insert(candidate.score.key.clone(), candidate.state)
                .is_some()
            {
                return Err(Self::invalid(format!(
                    "GBDT replay duplicated candidate {}/{}",
                    candidate.score.key.market_id, candidate.score.key.token_id
                )));
            }
            scores.push(candidate.score);
        }
        scores.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        if scores.is_empty() {
            return Err(Self::invalid(format!(
                "GBDT model run {} replayed an empty candidate universe",
                context.model_run_id()
            )));
        }
        let top_n = u32::try_from(context.top_n())
            .map_err(|error| Self::invalid(format!("report top_n is invalid: {error}")))?;
        let candidate_score_floor = policy
            .snapshot
            .model_routing
            .model
            .candidate_score_floor
            .value();
        let minimum_confidence = policy
            .snapshot
            .model_routing
            .model
            .min_model_confidence
            .value();
        Ok(DecisionUniverse {
            policy: DecisionReplayPolicy::try_new(
                policy.snapshot_hash,
                &scores,
                candidate_score_floor,
                minimum_confidence,
                top_n,
            )?,
            scores,
            replay: DecisionReplayModel::Tree(Box::new(TreeReplayModel {
                payload: payload.clone(),
                prediction_horizon_secs: artifact.header().prediction_horizon_secs(),
                ensemble: tree_shap.ensemble.clone(),
                candidates,
            })),
        })
    }

    #[cfg(feature = "ml-classical")]
    fn tree_candidate(
        evidence: &TreeUniverseEvidence<'_>,
        member: &MarketSelectionMemberInfo,
    ) -> QuantResult<Option<TreeCandidateReplay>> {
        let rows = evidence
            .run_inputs
            .iter()
            .filter(|row| {
                row.model_version_id == evidence.context.model_version_id()
                    && row.market_id == member.market_id
            })
            .collect::<Vec<_>>();
        if rows.is_empty() {
            return Ok(None);
        }
        let feature_vector_id = rows[0].feature_vector_id;
        if rows
            .iter()
            .any(|row| row.feature_vector_id != feature_vector_id)
        {
            return Err(Self::invalid(format!(
                "model run {} mixes feature vectors for market {}",
                evidence.context.model_run_id(),
                member.market_id
            )));
        }
        let feature = evidence.features.get(&feature_vector_id).ok_or_else(|| {
            Self::invalid(format!(
                "model run {} feature vector {feature_vector_id} is missing",
                evidence.context.model_run_id()
            ))
        })?;
        let vector = FeatureVector::try_from(feature)?;
        let selected = SelectedMarket {
            market_id: member.market_id.clone(),
            event_id: member.event_id.clone(),
            category: member.category,
            primary_token_id: member.primary_token_id.clone(),
            secondary_token_id: member.secondary_token_id.clone(),
            liquidity_usd: member.liquidity_usd,
            volume_24h_usd: member.volume_24h_usd,
            source_refs: Vec::new(),
        };
        let Some(inference_context) = build_market_inference_context(&vector, &selected) else {
            return Ok(None);
        };
        let input = Self::tree_input(
            evidence.payload,
            evidence.artifact,
            &rows,
            evidence.context.model_run_id(),
        )?;
        let tree_shap = evidence.payload.tree_shap.as_ref().ok_or_else(|| {
            Self::invalid("classical decision replay lost its verified TreeSHAP contract")
        })?;
        let raw_prediction = tree_shap.ensemble.predict(&input)?;
        let row = InferenceMatrixRow {
            market_id: member.market_id.clone(),
            token_id: member.primary_token_id.clone(),
            features: Vec::new(),
            context: inference_context,
        };
        let Some(projected) = ClassicalDecisionProjection::try_project(
            evidence.payload,
            evidence.artifact.header().prediction_horizon_secs(),
            raw_prediction,
            &row,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(TreeCandidateReplay {
            score: DecisionCandidateScore {
                key: DecisionCandidateKey {
                    market_id: member.market_id.clone(),
                    token_id: projected.token_id,
                },
                score: projected.composite_score.inner(),
                confidence: projected.confidence.inner(),
            },
            state: TreeReplayState { input, row },
        }))
    }

    #[cfg(not(feature = "ml-classical"))]
    fn unavailable_tree_universe(
        _params: &FeedbackAttributionPlanJobParams,
        prediction: &MaterializedPrediction,
        _artifact: &ModelArtifact,
        _payload: &ClassicalModelPayload,
    ) -> QuantResult<DecisionUniverse> {
        Err(ResearchError::NotEligible {
            code: "exact_decision_counterfactual_unavailable",
            detail: format!(
                "classical model {} replay requires the ml-classical runtime",
                prediction.context.model_version_id()
            ),
        }
        .into())
    }

    fn tree_input(
        payload: &ClassicalModelPayload,
        artifact: &ModelArtifact,
        rows: &[&QuantModelInputEventRow],
        model_run_id: ModelRunId,
    ) -> QuantResult<TreeEnsembleInput> {
        let tree_shap = payload
            .tree_shap
            .as_ref()
            .ok_or_else(|| ResearchError::NotEligible {
                code: "exact_prediction_explanation_unavailable",
                detail: format!(
                    "classical model {} has no exact local explanation contract",
                    artifact.header().model_version_id()
                ),
            })?;
        let bindings = artifact.header().serving_contract().bindings();
        TreeInputEvidenceContract {
            feature_names: &tree_shap.ensemble.feature_names,
            model_family: bindings.model.model_family,
            model_version_id: bindings.model.model_version_id,
            input_contract_hash: bindings.transform.input_contract_hash,
            input_transform_hash: bindings.transform.input_transform_hash,
            training_input_hash: bindings.transform.training_input_hash,
        }
        .encode(rows, model_run_id)
    }

    async fn materialize_trajectories(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<(u64, u64)> {
        let cohort_snapshot = params.cohort_snapshot()?;
        let mut cursor = None;
        let mut trajectory_count = 0_u64;
        let mut counterfactual_count = 0_u64;
        loop {
            Self::require_active(cancel)?;
            let query = FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ExecutionLearning,
                cohort_snapshot.clone(),
                cursor,
                ATTRIBUTION_PAGE_LIMIT,
            )
            .map_err(Self::invalid_contract)?;
            let page = self.cohorts.list_page(query).await?;
            let candidates = page
                .candidates()
                .iter()
                .filter_map(|candidate| {
                    match evaluate_feedback_cohort(
                        FeedbackCohort::ExecutionLearning,
                        &cohort_snapshot,
                        candidate.context(),
                        None,
                        candidate.execution_rollup(),
                    ) {
                        Ok(FeedbackCohortDecision::Eligible(_)) => Some(Ok(candidate.clone())),
                        Ok(
                            FeedbackCohortDecision::Censored(_)
                            | FeedbackCohortDecision::Excluded(_),
                        ) => None,
                        Err(error) => Some(Err(Self::invalid_contract(error))),
                    }
                })
                .collect::<QuantResult<Vec<_>>>()?;
            let (trajectories, counterfactuals) = self
                .materialize_trajectory_page(params, &candidates)
                .await?;
            trajectory_count = trajectory_count.saturating_add(trajectories);
            counterfactual_count = counterfactual_count.saturating_add(counterfactuals);
            progress.report(ResearchJobProgress::indeterminate(
                format!(
                    "attribution-trajectories:{trajectory_count}:counterfactuals:{counterfactual_count}"
                ),
                trajectory_count.saturating_add(counterfactual_count),
            ));
            cursor = page.next_cursor();
            if cursor.is_none() {
                break;
            }
        }
        Ok((trajectory_count, counterfactual_count))
    }

    async fn materialize_trajectory_page(
        &self,
        params: &FeedbackAttributionPlanJobParams,
        candidates: &[FeedbackCohortCandidate],
    ) -> QuantResult<(u64, u64)> {
        if candidates.is_empty() {
            return Ok((0, 0));
        }
        let recommendation_ids = candidates
            .iter()
            .map(|candidate| candidate.context().recommendation_id())
            .collect::<Vec<_>>();
        let attempts = self
            .attempts
            .list_by_recommendations(&recommendation_ids, params.cutoff)
            .await?;
        let seeds = Self::trajectory_seeds(candidates, attempts, params.cutoff)?;
        if seeds.is_empty() {
            return Ok((0, 0));
        }
        let mut token_ids = seeds
            .iter()
            .map(|seed| seed.attempt.token_id.clone())
            .collect::<Vec<_>>();
        token_ids.sort();
        token_ids.dedup();
        let from = seeds
            .iter()
            .map(|seed| seed.entry_at)
            .min()
            .ok_or_else(|| Self::invalid("trajectory seed set has no entry frontier"))?;
        let until = seeds
            .iter()
            .map(|seed| seed.horizon_end)
            .max()
            .ok_or_else(|| Self::invalid("trajectory seed set has no horizon frontier"))?;
        let rows = self
            .facts
            .book_ledger_snapshots_between(
                token_ids,
                from.timestamp_millis(),
                until.timestamp_millis(),
                params.cutoff.timestamp_millis(),
            )
            .await?;
        let pit_book_contract_hash = CanonicalDigest::content_hash_typed(
            TRAJECTORY_CONTRACT_DOMAIN,
            TRAJECTORY_CONTRACT_VERSION,
            &"canonical_l2_snapshot_best_bid_visible_by_cutoff",
        )?;
        let alternative_policy = AlternativeExitPolicy::LatestExecutableAtOrBeforeHorizon;
        let alternative_policy_hash = CanonicalDigest::content_hash_typed(
            EXIT_POLICY_DOMAIN,
            EXIT_POLICY_VERSION,
            &alternative_policy,
        )?;
        let mut trajectory_count = 0_u64;
        let mut counterfactual_count = 0_u64;
        for seed in seeds {
            let points = Self::trajectory_points(&seed, &rows, params.cutoff)?;
            let mut source_hashes = points
                .iter()
                .map(|point| point.source_fact_hash)
                .collect::<Vec<_>>();
            source_hashes.extend([
                params.truth_artifact.content_hash,
                seed.attempt.outcome_hash,
                seed.rollup_hash,
                pit_book_contract_hash,
            ]);
            let lineage = AttributionLineage::try_new(
                params.feedback_cycle_id,
                AttributionCohort::Evaluation,
                params.cutoff,
                params.generated_at,
                source_hashes,
            )?;
            let trajectory = ExecutionTrajectoryArtifact::try_new(ExecutionTrajectoryInput {
                lineage,
                recommendation_id: seed.context.recommendation_id(),
                order_intent_id: seed.attempt.order_intent_id,
                attempt_outcome_hash: seed.attempt.outcome_hash,
                pit_book_contract_hash,
                entry_at: seed.entry_at,
                entry_price: seed.entry_price,
                horizon_end: seed.horizon_end,
                points,
            })?;
            let persisted = self
                .persist(AttributionArtifact::ExecutionTrajectory(Box::new(
                    trajectory.clone(),
                )))
                .await?;
            trajectory_count = trajectory_count.saturating_add(1);
            let counterfactual = PolicyCounterfactualOutcome::replay(
                &trajectory,
                persisted.artifact_hash,
                alternative_policy_hash,
                alternative_policy,
                None,
            )?;
            self.persist(AttributionArtifact::PolicyCounterfactualOutcome(Box::new(
                counterfactual,
            )))
            .await?;
            counterfactual_count = counterfactual_count.saturating_add(1);
        }
        Ok((trajectory_count, counterfactual_count))
    }

    fn trajectory_seeds(
        candidates: &[FeedbackCohortCandidate],
        attempts: Vec<ExecutionAttemptOutcomeInfo>,
        cutoff: DateTime<Utc>,
    ) -> QuantResult<Vec<TrajectorySeed>> {
        let contexts = candidates
            .iter()
            .map(|candidate| {
                (
                    candidate.context().recommendation_id(),
                    (
                        candidate.context(),
                        candidate
                            .execution_rollup()
                            .map(|rollup| rollup.rollup_hash),
                    ),
                )
            })
            .collect::<HashMap<_, _>>();
        let mut seeds = Vec::new();
        for attempt in attempts {
            attempt.validate().map_err(Self::invalid_contract)?;
            let Some((context, rollup_hash)) = contexts.get(&attempt.recommendation_id) else {
                return Err(Self::invalid(format!(
                    "attempt {} is outside the frozen execution cohort",
                    attempt.order_intent_id
                )));
            };
            let rollup_hash = rollup_hash.ok_or_else(|| {
                Self::invalid("eligible execution candidate lost its final rollup")
            })?;
            let (Some(entry_at), Some(entry_price)) =
                (attempt.entry_filled_at, attempt.entry_avg_price)
            else {
                continue;
            };
            if attempt.filled_shares <= Shares::ZERO {
                continue;
            }
            let horizon = Duration::seconds(context.horizon_secs());
            let horizon_end = entry_at.checked_add_signed(horizon).ok_or_else(|| {
                Self::invalid(format!(
                    "recommendation {} trajectory horizon overflows",
                    context.recommendation_id()
                ))
            })?;
            if horizon_end > cutoff {
                continue;
            }
            seeds.push(TrajectorySeed {
                context: (*context).clone(),
                attempt,
                rollup_hash,
                entry_at,
                entry_price,
                horizon_end,
            });
        }
        Ok(seeds)
    }

    fn trajectory_points(
        seed: &TrajectorySeed,
        rows: &[BookL2LedgerRow],
        cutoff: DateTime<Utc>,
    ) -> QuantResult<Vec<TrajectoryPoint>> {
        let mut points = BTreeMap::<DateTime<Utc>, (&BookL2LedgerRow, DateTime<Utc>)>::new();
        for row in rows.iter().filter(|row| {
            row.token_id == seed.attempt.token_id
                && row.venue_event_time >= seed.entry_at.timestamp_millis()
                && row.venue_event_time <= seed.horizon_end.timestamp_millis()
                && row.persisted_time <= cutoff.timestamp_millis()
                && !row.bid_prices.is_empty()
        }) {
            let observed_at =
                DateTime::from_timestamp_millis(row.venue_event_time).ok_or_else(|| {
                    Self::invalid(format!(
                        "book event {} has an invalid venue timestamp",
                        ContentHash::from(row.event_hash)
                    ))
                })?;
            let available_at =
                DateTime::from_timestamp_millis(row.persisted_time).ok_or_else(|| {
                    Self::invalid(format!(
                        "book event {} has an invalid persisted timestamp",
                        ContentHash::from(row.event_hash)
                    ))
                })?;
            match points.get(&observed_at) {
                Some((prior, prior_available))
                    if (*prior_available, prior.token_sequence)
                        >= (available_at, row.token_sequence) => {}
                Some(_) | None => {
                    points.insert(observed_at, (row, available_at));
                }
            }
        }
        if points.is_empty() {
            return Err(ResearchError::NotEligible {
                code: "execution_trajectory_evidence_missing",
                detail: format!(
                    "attempt {} has no PIT-visible executable bid snapshots in its mature horizon",
                    seed.attempt.order_intent_id
                ),
            }
            .into());
        }
        points
            .into_iter()
            .map(|(observed_at, (row, available_at))| {
                let executable_exit_price = row
                    .bid_prices
                    .first()
                    .copied()
                    .map(Into::into)
                    .ok_or_else(|| {
                        Self::invalid(format!(
                            "book event {} lost its executable bid",
                            ContentHash::from(row.event_hash)
                        ))
                    })?;
                Ok(TrajectoryPoint {
                    observed_at,
                    available_at,
                    executable_exit_price,
                    source_fact_hash: ContentHash::from(row.event_hash),
                })
            })
            .collect()
    }

    fn factor_values(
        candidate: &FeedbackCohortCandidate,
        rows: &[FactorValueInfo],
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
        expected_ids: impl Iterator<Item = FactorDefinitionId>,
    ) -> QuantResult<Vec<FactorValue>> {
        let context = candidate.context();
        let expected_ids = expected_ids.collect::<Vec<_>>();
        let rows = rows
            .iter()
            .filter(|row| {
                row.feature_vector_id == context.feature_vector_id()
                    && row.model_run_id == context.model_run_id()
                    && &row.market_id == context.market_id()
            })
            .collect::<Vec<_>>();
        if rows.len() != expected_ids.len() {
            return Err(Self::invalid(format!(
                "recommendation {} has {} factor rows but its serving plane requires {}",
                context.recommendation_id(),
                rows.len(),
                expected_ids.len()
            )));
        }
        expected_ids
            .into_iter()
            .map(|factor_definition_id| {
                let row = rows
                    .iter()
                    .find(|row| row.factor_definition_id == factor_definition_id)
                    .ok_or_else(|| {
                        Self::invalid(format!(
                            "recommendation {} omitted factor revision {factor_definition_id}",
                            context.recommendation_id()
                        ))
                    })?;
                let definition = definitions.get(&factor_definition_id).ok_or_else(|| {
                    Self::invalid(format!(
                        "factor revision {factor_definition_id} is absent from the registry"
                    ))
                })?;
                FactorValue::try_from_persistence(row, definition)
            })
            .collect()
    }

    async fn persist(&self, artifact: AttributionArtifact) -> QuantResult<AttributionArtifactInfo> {
        let bytes = AttributionArtifactCodec::encode(&artifact)?;
        let artifact_hash = AttributionArtifactCodec::hash(&bytes);
        let key = ArtifactKey::new(ArtifactNamespace::Attribution, artifact_hash.hex(), "json")?;
        let uri = self.artifacts.put(key, &bytes).await?;
        let persisted = self.artifacts.get(&uri).await?;
        if AttributionArtifactCodec::hash(&persisted) != artifact_hash {
            return Err(ResearchError::ArtifactHashMismatch {
                expected: artifact_hash.to_string(),
                actual: AttributionArtifactCodec::hash(&persisted).to_string(),
            }
            .into());
        }
        AttributionArtifactCodec::decode(&persisted)?;
        let lineage = artifact.lineage();
        let insert = NewAttributionArtifact::try_new(
            lineage.source_cohort,
            lineage.source_feedback_cycle_id,
            artifact.subject(),
            uri,
            artifact_hash,
            lineage.source_cutoff,
        )
        .map_err(Self::invalid_contract)?;
        match self.index.insert(insert).await? {
            AttributionArtifactWriteOutcome::Inserted(info)
            | AttributionArtifactWriteOutcome::AlreadyPresent(info) => Ok(info),
        }
    }

    fn require_active(cancel: &CancellationToken) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "feedback attribution materialization cancelled".to_owned(),
            }
            .into());
        }
        Ok(())
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        FeedbackError::InvalidJobContract {
            detail: detail.into(),
        }
        .into()
    }

    fn invalid_contract(error: impl Display) -> QuantError {
        Self::invalid(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        clickhouse::QuantModelInputEventRow,
        enums::model::ModelFamily,
        hashing::CanonicalDigest,
        types::{ContentHash, FeatureVectorId, MarketId, ModelRunId, ModelVersionId},
    };
    use rust_decimal::Decimal;

    use super::TreeInputEvidenceContract;

    fn hash(seed: &str) -> ContentHash {
        CanonicalDigest::content_hash_json(&seed).expect("fixture content hash")
    }

    fn input_row(
        contract: &TreeInputEvidenceContract<'_>,
        model_run_id: ModelRunId,
        encoded_column: &str,
        encoded_value: f64,
    ) -> QuantModelInputEventRow {
        QuantModelInputEventRow {
            event_time: 1,
            decision_at: 1,
            knowledge_cutoff: 1,
            model_run_id,
            model_version_id: contract.model_version_id,
            recommendation_report_id: None,
            market_id: MarketId::new("0xattribution"),
            feature_vector_id: FeatureVectorId::from_v7(),
            model_family: contract.model_family.to_string(),
            raw_input_name: encoded_column.to_owned(),
            raw_state: "observed".to_owned(),
            raw_value: Some(encoded_value.to_string()),
            encoded_column: encoded_column.to_owned(),
            encoded_value_bits: Some(encoded_value.to_bits()),
            input_contract_hash: contract.input_contract_hash.to_string(),
            transform_hash: contract.input_transform_hash.to_string(),
            training_input_hash: contract.training_input_hash.to_string(),
            audit_fingerprint: format!("fingerprint-{encoded_column}"),
            ingestion_time: 2,
        }
    }

    #[test]
    fn tree_input_preserves_order() {
        let feature_names = vec!["feature.z".to_owned(), "feature.a".to_owned()];
        let contract = TreeInputEvidenceContract {
            feature_names: &feature_names,
            model_family: ModelFamily::ClassicalGradientBoostedTrees,
            model_version_id: ModelVersionId::from_v7(),
            input_contract_hash: hash("input-contract"),
            input_transform_hash: hash("input-transform"),
            training_input_hash: hash("training-input"),
        };
        let model_run_id = ModelRunId::from_v7();
        let second = input_row(&contract, model_run_id, "feature.a", -2.0);
        let first = input_row(&contract, model_run_id, "feature.z", 1.5);

        let encoded = contract
            .encode(&[&second, &first], model_run_id)
            .expect("encoded input");

        assert_eq!(
            encoded.values,
            vec![Some(Decimal::new(15, 1)), Some(Decimal::new(-20, 1))]
        );
    }

    #[test]
    fn tree_input_rejects_drift() {
        let feature_names = vec!["feature.value".to_owned()];
        let contract = TreeInputEvidenceContract {
            feature_names: &feature_names,
            model_family: ModelFamily::ClassicalGradientBoostedTrees,
            model_version_id: ModelVersionId::from_v7(),
            input_contract_hash: hash("input-contract"),
            input_transform_hash: hash("input-transform"),
            training_input_hash: hash("training-input"),
        };
        let model_run_id = ModelRunId::from_v7();
        let mut row = input_row(&contract, model_run_id, "feature.value", 1.0);
        row.training_input_hash = hash("different-training-input").to_string();

        let error = contract
            .encode(&[&row], model_run_id)
            .expect_err("contract drift must fail closed");

        assert!(
            error
                .to_string()
                .contains("differs from its serving contract")
        );
    }

    #[test]
    fn tree_input_rejects_nonfinite() {
        let feature_names = vec!["feature.value".to_owned()];
        let contract = TreeInputEvidenceContract {
            feature_names: &feature_names,
            model_family: ModelFamily::ClassicalGradientBoostedTrees,
            model_version_id: ModelVersionId::from_v7(),
            input_contract_hash: hash("input-contract"),
            input_transform_hash: hash("input-transform"),
            training_input_hash: hash("training-input"),
        };
        let model_run_id = ModelRunId::from_v7();
        let row = input_row(&contract, model_run_id, "feature.value", f64::NAN);

        let error = contract
            .encode(&[&row], model_run_id)
            .expect_err("non-finite input must fail closed");

        assert!(error.to_string().contains("non-finite"));
    }
}
