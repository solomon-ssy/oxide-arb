//! Production materialization of immutable attribution artifacts.

use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    fmt::Display,
    sync::Arc,
    time::Duration as StdDuration,
};

use chrono::{DateTime, Duration, Utc};
use futures_util::{StreamExt, stream};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{
    QuantError, QuantResult, feedback::FeedbackError, infra::InfraError, research::ResearchError,
};
use quant_pivot_models::{
    clickhouse::{
        BookL2LedgerRow, QuantFeatureEventRow, QuantModelInputEventRow,
        QuantServingEvidenceCompletionRow,
    },
    config::FeedbackAttributionComputeConfig,
    domain::{
        market::book::BookLevel,
        ports::FeedbackAttributionJobParams,
        quant::{
            AttributionArtifactInfo, ExecutionAttemptOutcomeInfo, FactorDefinitionInfo,
            FactorValueInfo, FeatureVectorInfo, FeedbackCohortCandidate, FeedbackCohortDecision,
            FeedbackCohortPageQuery, FeedbackRecommendationContext, JobProgressSink,
            MarketSelectionMemberInfo, NewAttributionArtifact,
        },
    },
    enums::quant::{AttributionCohort, FeedbackCohort, FillRequirement, OutcomeSide},
    hashing::CanonicalDigest,
    types::{
        Bps, ClobMarketInfoVersion, ContentHash, FactorDefinitionId, FeatureVectorId, ModelRunId,
        ModelVersionId, OutcomeTokenBinding, Price, ResearchJobProgress, Shares, Usd,
        factor::{FactorDefinitionRef, FactorServingPlane},
    },
};
use quant_pivot_repository::traits::{
    AttributionArtifactRepository, AttributionArtifactWriteOutcome, ClobMarketInfoRepository,
    ExecutionAttemptOutcomeRepository, FactorRepository, FeatureRepository,
    FeedbackCohortRepository, MarketSelectionRepository, ModelRegistryRepository, PolicyRepository,
    QuantFactReadRepository, ServingEvidenceRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    attribution::{
        ActualBaselineNotEvaluableReason, ActualExecutionBaseline, AlternativeExitPolicy,
        AttributionArtifact, AttributionArtifactCodec, AttributionLineage, DecisionCandidateKey,
        DecisionInterventionAttempt, DecisionInterventionEvaluation,
        DecisionInterventionNotEvaluableReason, DecisionInterventionReplayArtifact,
        DecisionInterventionReplayInput, DecisionInterventionSupport, DecisionReplayPolicy,
        ExecutionOutcomeAssociationArtifact, ExecutionOutcomeAssociationInput,
        ExecutionOutcomeAssociationSample, ExecutionOutcomeAssociationTarget,
        ExecutionOutcomeBinding, ExecutionTrajectoryArtifact, ExecutionTrajectoryInput,
        PolicyCounterfactualOutcome, PredictionContribution, PredictionExplanationArtifact,
        ResolutionOutcomeAssociationArtifact, ResolutionOutcomeAssociationInput,
        ResolutionOutcomeAssociationSample, ResolutionOutcomeAssociationTarget, TrajectoryPoint,
        TrajectoryPointEconomics, TrajectoryPointNotEvaluableReason,
        WeightedFactorExplanationInput,
    },
    execution_semantics::{BookWalkOutcome, LiquidityRole, PitFeeSchedule, walk_sell_exact_shares},
    factors::FactorValue,
    features::FeatureVector,
    model::{
        ModelArtifact,
        artifact::{ModelPayload, WeightedFactorModelPayload},
        factor_heads::score_factor_heads,
        model_input_contract_hash,
    },
    selection::SelectedMarket,
};
use rust_decimal::Decimal;
use tokio::sync::Semaphore;
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
const TRAJECTORY_CONTRACT_VERSION: u32 = 2;
const EXIT_POLICY_DOMAIN: &str = "quant-pivot/policy-counterfactual-exit";
const EXIT_POLICY_VERSION: u32 = 3;
const ATTRIBUTION_MAX_PAGE_SIZE: u32 = 1_000;

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
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub serving_evidence: Arc<dyn ServingEvidenceRepository>,
    pub index: Arc<dyn AttributionArtifactRepository>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub metrics: Arc<MetricsHub>,
    pub compute: Arc<ComputeExecutor>,
    pub compute_budget: FeedbackAttributionComputeConfig,
}

/// Materialization summary reported by the `AttributionManifest` job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackAttributionSummary {
    pub prediction_explanations: u64,
    pub decision_intervention_replays: u64,
    pub resolution_outcome_associations: u64,
    pub execution_outcome_associations: u64,
    pub execution_trajectories: u64,
    pub policy_counterfactuals: u64,
}

struct TrajectorySeed {
    context: FeedbackRecommendationContext,
    attempt: ExecutionAttemptOutcomeInfo,
    rollup_hash: ContentHash,
    entry_at: DateTime<Utc>,
    entry_shares: Shares,
    entry_price: Price,
    actual_baseline: ActualExecutionBaseline,
    horizon_end: DateTime<Utc>,
}

#[derive(Clone)]
struct MaterializedPrediction {
    context: FeedbackRecommendationContext,
    explanation: PredictionExplanationArtifact,
    explanation_hash: ContentHash,
    outcome_hash: ContentHash,
    outcome: Decimal,
}

#[derive(Clone)]
struct DecisionUniverse {
    policy: DecisionReplayPolicy,
    replay: DecisionReplayModel,
    model_artifact_hash: ContentHash,
    input_contract_hash: ContentHash,
    input_transform_hash: ContentHash,
}

#[derive(Clone)]
enum DecisionReplayModel {
    Weighted(BTreeMap<DecisionCandidateKey, WeightedReplayState>),
}

#[derive(Clone)]
struct WeightedReplayState {
    yes_alpha: Decimal,
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
    key: DecisionCandidateKey,
    state: WeightedReplayState,
}

impl DecisionUniverse {
    fn replay_prediction(
        &self,
        params: &FeedbackAttributionJobParams,
        prediction: &MaterializedPrediction,
    ) -> QuantResult<DecisionInterventionReplayArtifact> {
        let target_key = DecisionCandidateKey {
            market_id: prediction.context.market_id().clone(),
            token_id: prediction.context.token_id().clone(),
        };
        let target_present = match &self.replay {
            DecisionReplayModel::Weighted(states) => states.contains_key(&target_key),
        };
        if !target_present {
            return Err(FeedbackAttributionMaterializer::invalid(format!(
                "published recommendation {} is absent from its Route model replay",
                prediction.context.recommendation_id()
            )));
        }
        let interventions = self.interventions(prediction, &target_key)?;
        let lineage = AttributionLineage::try_new(
            params.feedback_cycle_id,
            AttributionCohort::Evaluation,
            params.cutoff,
            params.generated_at,
            vec![
                params.truth_artifact.content_hash,
                prediction.explanation_hash,
                self.model_artifact_hash,
                self.input_contract_hash,
                self.input_transform_hash,
                self.policy.policy_hash,
                self.policy.admissible_intervention_policy_hash,
                self.policy.computation_graph_contract_hash,
            ],
        )?;
        DecisionInterventionReplayArtifact::replay(DecisionInterventionReplayInput {
            lineage,
            model_version_id: prediction.context.model_version_id(),
            recommendation_id: prediction.context.recommendation_id(),
            target_key,
            prediction_explanation_hash: prediction.explanation_hash,
            model_artifact_hash: self.model_artifact_hash,
            input_contract_hash: self.input_contract_hash,
            input_transform_hash: self.input_transform_hash,
            policy: self.policy.clone(),
            interventions,
            baseline_model_output: prediction.explanation.predicted_output,
        })
    }

    fn interventions(
        &self,
        prediction: &MaterializedPrediction,
        target_key: &DecisionCandidateKey,
    ) -> QuantResult<Vec<DecisionInterventionAttempt>> {
        match &self.replay {
            DecisionReplayModel::Weighted(states) => {
                Self::weighted_interventions(states, prediction, target_key)
            }
        }
    }

    fn weighted_interventions(
        states: &BTreeMap<DecisionCandidateKey, WeightedReplayState>,
        prediction: &MaterializedPrediction,
        target_key: &DecisionCandidateKey,
    ) -> QuantResult<Vec<DecisionInterventionAttempt>> {
        let state = states.get(target_key).ok_or_else(|| {
            FeedbackAttributionMaterializer::invalid(format!(
                "weighted replay state omitted {}/{}",
                target_key.market_id, target_key.token_id
            ))
        })?;
        let support = DecisionInterventionSupport::try_new(-Decimal::ONE, Decimal::ONE)?;
        Self::ordered_contributions(prediction)
            .into_iter()
            .map(|contribution| {
                let proposed_value = Some(Decimal::ZERO);
                let evaluation = Self::blocked_reason(contribution, support, proposed_value)
                    .map_or_else(
                        || {
                            let counterfactual_alpha =
                                prediction.explanation.predicted_output - contribution.contribution;
                            if counterfactual_alpha.abs() <= state.alpha_deadband {
                                DecisionInterventionEvaluation::NotEvaluable {
                                    reason: DecisionInterventionNotEvaluableReason::
                                        DeadbandWouldSuppressSignal,
                                }
                            } else if counterfactual_alpha.is_sign_positive()
                                != state.yes_alpha.is_sign_positive()
                            {
                                DecisionInterventionEvaluation::NotEvaluable {
                                    reason: DecisionInterventionNotEvaluableReason::
                                        OutcomeSideFlipNotAdmissible,
                                }
                            } else if counterfactual_alpha
                                == prediction.explanation.predicted_output
                            {
                                DecisionInterventionEvaluation::NotEvaluable {
                                    reason: DecisionInterventionNotEvaluableReason::
                                        NoMaterialModelOutputChange,
                                }
                            } else {
                                DecisionInterventionEvaluation::Evaluated {
                                    intervened_model_output: counterfactual_alpha,
                                }
                            }
                        },
                        |reason| DecisionInterventionEvaluation::NotEvaluable { reason },
                    );
                Ok(DecisionInterventionAttempt {
                    input_name: contribution.input_name.clone(),
                    model_contribution: contribution.contribution,
                    observed_value: contribution.input_value,
                    proposed_value,
                    support,
                    evaluation,
                })
            })
            .collect()
    }

    fn blocked_reason(
        contribution: &PredictionContribution,
        support: DecisionInterventionSupport,
        proposed_value: Option<Decimal>,
    ) -> Option<DecisionInterventionNotEvaluableReason> {
        let Some(observed_value) = contribution.input_value else {
            return Some(DecisionInterventionNotEvaluableReason::MissingObservedValue);
        };
        if !support.contains(observed_value) {
            return Some(DecisionInterventionNotEvaluableReason::ObservedValueOutOfSupport);
        }
        let Some(proposed_value) = proposed_value else {
            return Some(DecisionInterventionNotEvaluableReason::ProposedValueOutOfSupport);
        };
        if !support.contains(proposed_value) {
            return Some(DecisionInterventionNotEvaluableReason::ProposedValueOutOfSupport);
        }
        if contribution.contribution.is_zero() {
            return Some(DecisionInterventionNotEvaluableReason::NoMaterialModelContribution);
        }
        (observed_value == proposed_value)
            .then_some(DecisionInterventionNotEvaluableReason::NoMaterialInputChange)
    }

    fn ordered_contributions(prediction: &MaterializedPrediction) -> Vec<&PredictionContribution> {
        let mut contributions = prediction
            .explanation
            .contributions
            .iter()
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

#[derive(Clone)]
struct ServingEvidencePage {
    completion_hashes: HashMap<ModelRunId, ContentHash>,
    model_inputs: HashMap<ModelRunId, Vec<QuantModelInputEventRow>>,
}

struct PredictionPageEvidence<'a> {
    params: &'a FeedbackAttributionJobParams,
    factor_rows: &'a [FactorValueInfo],
    definitions: &'a HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    selection_members: &'a [MarketSelectionMemberInfo],
    serving: &'a ServingEvidencePage,
}

/// Builds explanation evidence from the exact frozen production preimages.
#[derive(Clone)]
pub struct FeedbackAttributionMaterializer {
    cohorts: Arc<dyn FeedbackCohortRepository>,
    factors: Arc<dyn FactorRepository>,
    features: Arc<dyn FeatureRepository>,
    models: Arc<dyn ModelRegistryRepository>,
    selections: Arc<dyn MarketSelectionRepository>,
    policies: Arc<dyn PolicyRepository>,
    attempts: Arc<dyn ExecutionAttemptOutcomeRepository>,
    facts: Arc<dyn QuantFactReadRepository>,
    clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    serving_evidence: Arc<dyn ServingEvidenceRepository>,
    index: Arc<dyn AttributionArtifactRepository>,
    artifacts: Arc<dyn ArtifactStore>,
    metrics: Arc<MetricsHub>,
    compute: Arc<ComputeExecutor>,
    compute_memory: OfflineMemory,
    compute_slots: Arc<Semaphore>,
    compute_budget: FeedbackAttributionComputeConfig,
}

impl FeedbackAttributionMaterializer {
    pub fn try_new(deps: FeedbackAttributionDeps) -> QuantResult<Self> {
        let budget = deps.compute_budget;
        if !(1..=ATTRIBUTION_MAX_PAGE_SIZE).contains(&budget.page_size)
            || !(1..=32).contains(&budget.max_concurrency)
            || !(1_048_576..=10_737_418_240).contains(&budget.max_working_set_bytes)
            || !(1..=86_400).contains(&budget.deadline_secs)
        {
            return Err(InfraError::Misconfigured {
                detail: "feedback attribution page size, concurrency, or deadline is invalid"
                    .to_owned(),
            }
            .into());
        }
        let working_set = usize::try_from(budget.max_working_set_bytes).map_err(|error| {
            InfraError::Misconfigured {
                detail: format!("feedback attribution working-set bytes do not fit usize: {error}"),
            }
        })?;
        let compute_memory = OfflineMemory::try_bytes(working_set)?;
        Ok(Self {
            cohorts: deps.cohorts,
            factors: deps.factors,
            features: deps.features,
            models: deps.models,
            selections: deps.selections,
            policies: deps.policies,
            attempts: deps.attempts,
            facts: deps.facts,
            clob_market_info: deps.clob_market_info,
            serving_evidence: deps.serving_evidence,
            index: deps.index,
            artifacts: deps.artifacts,
            metrics: deps.metrics,
            compute: deps.compute,
            compute_memory,
            compute_slots: Arc::new(Semaphore::new(budget.max_concurrency)),
            compute_budget: budget,
        })
    }

    pub async fn materialize(
        &self,
        params: &FeedbackAttributionJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackAttributionSummary> {
        let bounded_cancel = cancel.child_token();
        let materialization = self.materialize_bounded(params, progress, &bounded_cancel);
        tokio::time::timeout(
            StdDuration::from_secs(self.compute_budget.deadline_secs),
            materialization,
        )
        .await
        .unwrap_or_else(|_| {
            bounded_cancel.cancel();
            Err(ResearchError::ComputeDeadlineExceeded {
                operation: "feedback_attribution",
                deadline_secs: self.compute_budget.deadline_secs,
            }
            .into())
        })
    }

    async fn materialize_bounded(
        &self,
        params: &FeedbackAttributionJobParams,
        progress: &dyn JobProgressSink,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackAttributionSummary> {
        let cohort_snapshot = params.cohort_snapshot()?;
        let mut cursor = None;
        let mut prediction_explanations = 0_u64;
        let mut association_samples =
            HashMap::<ModelVersionId, Vec<ResolutionOutcomeAssociationSample>>::new();
        let mut decision_predictions = Vec::new();
        let mut model_cache = HashMap::new();
        loop {
            Self::require_active(cancel)?;
            let query = FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ModelLearning,
                cohort_snapshot.clone(),
                cursor,
                self.compute_budget.page_size,
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
                .materialize_page(params, eligible, &mut model_cache, cancel)
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
                    .push(ResolutionOutcomeAssociationSample {
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
        let execution_samples = self
            .load_execution_samples(params, &decision_predictions)
            .await?;
        let resolution_outcome_associations = self
            .materialize_resolution_associations(params, association_samples, cancel)
            .await?;
        let execution_outcome_associations = self
            .materialize_execution_associations(params, execution_samples, cancel)
            .await?;
        let decision_intervention_replays = self
            .materialize_decisions(params, &decision_predictions, &model_cache, cancel)
            .await?;
        let (execution_trajectories, policy_counterfactuals) = self
            .materialize_trajectories(params, progress, cancel)
            .await?;
        Ok(FeedbackAttributionSummary {
            prediction_explanations,
            decision_intervention_replays,
            resolution_outcome_associations,
            execution_outcome_associations,
            execution_trajectories,
            policy_counterfactuals,
        })
    }

    async fn run_compute<T, F>(&self, cancel: &CancellationToken, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(ResearchError::Cancelled {
                    detail: "cancelled while waiting for attribution compute capacity".to_owned(),
                }
                .into());
            }
            permit = Arc::clone(&self.compute_slots).acquire_owned() => {
                permit.map_err(|_| InfraError::ComputeExecution {
                    detail: "feedback attribution compute semaphore closed".to_owned(),
                })?
            }
        };
        self.compute
            .run_offline_cancellable(self.compute_memory, cancel, move || {
                let _permit = permit;
                work()
            })
            .await
    }

    async fn materialize_page(
        &self,
        params: &FeedbackAttributionJobParams,
        candidates: Vec<FeedbackCohortCandidate>,
        model_cache: &mut HashMap<ModelVersionId, ModelArtifact>,
        cancel: &CancellationToken,
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
        Self::validate_serving_page(&candidates, &serving)?;
        let mut model_version_ids = candidates
            .iter()
            .map(|candidate| candidate.context().model_version_id())
            .collect::<Vec<_>>();
        model_version_ids.sort_by_key(|model_version_id| model_version_id.as_uuid());
        model_version_ids.dedup();
        for model_version_id in &model_version_ids {
            if let Entry::Vacant(entry) = model_cache.entry(*model_version_id) {
                let version = self
                    .models
                    .find_model_version(model_version_id)
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
        }
        let page_models = model_version_ids
            .into_iter()
            .map(|model_version_id| {
                let artifact = model_cache.get(&model_version_id).cloned().ok_or_else(|| {
                    Self::invalid(format!(
                        "attribution model cache lost version {model_version_id}"
                    ))
                })?;
                Ok((model_version_id, artifact))
            })
            .collect::<QuantResult<HashMap<_, _>>>()?;
        let worker = self.clone();
        let compute_params = params.clone();
        let compute_cancel = cancel.clone();
        let pending = self
            .run_compute(cancel, move || {
                let page_evidence = PredictionPageEvidence {
                    params: &compute_params,
                    factor_rows: &factor_rows,
                    definitions: &definition_index,
                    selection_members: &selection_members,
                    serving: &serving,
                };
                candidates
                    .into_iter()
                    .map(|candidate| {
                        Self::require_active(&compute_cancel)?;
                        let context = candidate.context();
                        let model_version_id = context.model_version_id();
                        let artifact = page_models.get(&model_version_id).ok_or_else(|| {
                            Self::invalid(format!(
                                "attribution page model cache lost version {model_version_id}"
                            ))
                        })?;
                        let resolution = candidate.resolution_outcome().ok_or_else(|| {
                            Self::invalid("eligible model-learning candidate lost resolution truth")
                        })?;
                        let explanation = worker.build_prediction(
                            &page_evidence,
                            &candidate,
                            artifact,
                            resolution.outcome_hash,
                        )?;
                        Ok((
                            context.clone(),
                            explanation,
                            resolution.outcome_hash,
                            resolution.token_payout_ratio.inner(),
                        ))
                    })
                    .collect::<QuantResult<Vec<_>>>()
            })
            .await?;
        let mut samples = Vec::with_capacity(pending.len());
        for (context, explanation, outcome_hash, outcome) in pending {
            let persisted = self
                .persist(AttributionArtifact::PredictionExplanation(Box::new(
                    explanation.clone(),
                )))
                .await?;
            samples.push(MaterializedPrediction {
                context,
                explanation,
                explanation_hash: persisted.artifact_hash,
                outcome_hash,
                outcome,
            });
        }
        Ok(samples)
    }

    fn validate_serving_page(
        candidates: &[FeedbackCohortCandidate],
        serving: &ServingEvidencePage,
    ) -> QuantResult<()> {
        for candidate in candidates {
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
        Ok(())
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
            ModelPayload::Classical(_) => Err(ResearchError::NotEligible {
                code: "exact_prediction_explanation_unavailable",
                detail: format!(
                    "shadow-only classical model {} cannot enter recommendation attribution",
                    candidate.context().model_version_id()
                ),
            }
            .into()),
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

    async fn load_execution_samples(
        &self,
        params: &FeedbackAttributionJobParams,
        predictions: &[MaterializedPrediction],
    ) -> QuantResult<HashMap<ModelVersionId, Vec<ExecutionOutcomeAssociationSample>>> {
        let prediction_index = predictions
            .iter()
            .map(|prediction| (prediction.context.recommendation_id(), prediction))
            .collect::<HashMap<_, _>>();
        if prediction_index.is_empty() {
            return Ok(HashMap::new());
        }
        let cohort_snapshot = params.cohort_snapshot()?;
        let mut groups = HashMap::<ModelVersionId, Vec<ExecutionOutcomeAssociationSample>>::new();
        let mut included = HashSet::new();
        let mut cursor = None;
        loop {
            let query = FeedbackCohortPageQuery::try_new(
                FeedbackCohort::ExecutionLearning,
                cohort_snapshot.clone(),
                cursor,
                self.compute_budget.page_size,
            )
            .map_err(Self::invalid_contract)?;
            let page = self.cohorts.list_page(query).await?;
            for candidate in page.candidates() {
                let decision = evaluate_feedback_cohort(
                    FeedbackCohort::ExecutionLearning,
                    &cohort_snapshot,
                    candidate.context(),
                    None,
                    candidate.execution_rollup(),
                )
                .map_err(Self::invalid_contract)?;
                if !matches!(decision, FeedbackCohortDecision::Eligible(_)) {
                    continue;
                }
                let recommendation_id = candidate.context().recommendation_id();
                let Some(prediction) = prediction_index.get(&recommendation_id) else {
                    continue;
                };
                if !included.insert(recommendation_id) {
                    return Err(Self::invalid(format!(
                        "execution association contains duplicate recommendation {recommendation_id}"
                    )));
                }
                let rollup = candidate.execution_rollup().ok_or_else(|| {
                    Self::invalid("eligible execution candidate lost its terminal rollup")
                })?;
                rollup.validate().map_err(Self::invalid_contract)?;
                let binding = ExecutionOutcomeBinding {
                    recommendation_id,
                    rollup_hash: rollup.rollup_hash,
                    attempt_set_hash: rollup.attempt_set_hash,
                    intent_count: rollup.intent_count,
                    attempt_count: rollup.attempt_count,
                    total_filled_shares: rollup.total_filled_shares,
                    total_entry_fee_usd: rollup.total_entry_fee_usd,
                    total_exit_fee_usd: rollup.total_exit_fee_usd,
                    total_realized_pnl_usd: rollup.total_realized_pnl_usd,
                    terminal_at: rollup.terminal_at,
                    available_at: rollup.available_at,
                };
                groups
                    .entry(prediction.context.model_version_id())
                    .or_default()
                    .push(ExecutionOutcomeAssociationSample {
                        explanation_hash: prediction.explanation_hash,
                        binding,
                        contributions: prediction.explanation.contributions.clone(),
                    });
            }
            cursor = page.next_cursor();
            if cursor.is_none() {
                break;
            }
        }
        Ok(groups)
    }

    async fn materialize_execution_associations(
        &self,
        params: &FeedbackAttributionJobParams,
        mut groups: HashMap<ModelVersionId, Vec<ExecutionOutcomeAssociationSample>>,
        cancel: &CancellationToken,
    ) -> QuantResult<u64> {
        let mut count = 0_u64;
        for (model_version_id, samples) in &mut groups {
            Self::require_active(cancel)?;
            samples.sort_by_key(|sample| sample.binding.recommendation_id.as_uuid());
            if samples.len() < 3 || !Self::execution_association_varies(samples) {
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
                    ExecutionOutcomeAssociationTarget::RealizedNetPnlUsd,
                ),
            )?;
            let recommendation_ids = samples
                .iter()
                .map(|sample| sample.binding.recommendation_id)
                .collect::<Vec<_>>();
            let explanation_hashes = samples
                .iter()
                .map(|sample| sample.explanation_hash)
                .collect::<Vec<_>>();
            let bindings = samples
                .iter()
                .map(|sample| &sample.binding)
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
            let execution_rollup_set_hash = CanonicalDigest::content_hash_typed(
                ASSOCIATION_EXECUTIONS_DOMAIN,
                ASSOCIATION_VERSION,
                &bindings,
            )?;
            let mut source_hashes = vec![
                params.truth_artifact.content_hash,
                estimator_contract_hash,
                conditioning_policy_hash,
                cohort_manifest_hash,
                explanation_set_hash,
                execution_rollup_set_hash,
            ];
            source_hashes.extend(
                samples.iter().flat_map(|sample| {
                    [sample.binding.rollup_hash, sample.binding.attempt_set_hash]
                }),
            );
            let lineage = AttributionLineage::try_new(
                params.feedback_cycle_id,
                AttributionCohort::Evaluation,
                params.cutoff,
                params.generated_at,
                source_hashes,
            )?;
            let association_input = ExecutionOutcomeAssociationInput {
                lineage,
                model_version_id: *model_version_id,
                target: ExecutionOutcomeAssociationTarget::RealizedNetPnlUsd,
                estimator_contract_hash,
                conditioning_policy_hash,
                cohort_manifest_hash,
                explanation_set_hash,
                execution_rollup_set_hash,
                samples: samples.clone(),
            };
            let artifact = self
                .run_compute(cancel, move || {
                    ExecutionOutcomeAssociationArtifact::estimate(association_input)
                })
                .await?;
            self.persist(AttributionArtifact::ExecutionOutcomeAssociation(Box::new(
                artifact,
            )))
            .await?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    async fn materialize_resolution_associations(
        &self,
        params: &FeedbackAttributionJobParams,
        mut groups: HashMap<ModelVersionId, Vec<ResolutionOutcomeAssociationSample>>,
        cancel: &CancellationToken,
    ) -> QuantResult<u64> {
        let mut count = 0_u64;
        for (model_version_id, samples) in &mut groups {
            Self::require_active(cancel)?;
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
                    ResolutionOutcomeAssociationTarget::FinalTokenPayoutRatio,
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
                ],
            )?;
            let association_input = ResolutionOutcomeAssociationInput {
                lineage,
                model_version_id: *model_version_id,
                target: ResolutionOutcomeAssociationTarget::FinalTokenPayoutRatio,
                estimator_contract_hash,
                conditioning_policy_hash,
                cohort_manifest_hash,
                explanation_set_hash,
                resolution_set_hash,
                samples: samples.clone(),
            };
            let artifact = self
                .run_compute(cancel, move || {
                    ResolutionOutcomeAssociationArtifact::estimate(association_input)
                })
                .await?;
            self.persist(AttributionArtifact::ResolutionOutcomeAssociation(Box::new(
                artifact,
            )))
            .await?;
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn association_varies(samples: &[ResolutionOutcomeAssociationSample]) -> bool {
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

    fn execution_association_varies(samples: &[ExecutionOutcomeAssociationSample]) -> bool {
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
                && samples.iter().skip(1).any(|sample| {
                    sample.binding.total_realized_pnl_usd != first.binding.total_realized_pnl_usd
                })
        })
    }

    async fn materialize_decisions(
        &self,
        params: &FeedbackAttributionJobParams,
        predictions: &[MaterializedPrediction],
        model_cache: &HashMap<ModelVersionId, ModelArtifact>,
        cancel: &CancellationToken,
    ) -> QuantResult<u64> {
        let mut grouped = HashMap::<ModelRunId, Vec<MaterializedPrediction>>::new();
        for prediction in predictions {
            grouped
                .entry(prediction.context.model_run_id())
                .or_default()
                .push(prediction.clone());
        }
        let mut run_ids = grouped.keys().copied().collect::<Vec<_>>();
        run_ids.sort_by_key(|model_run_id| model_run_id.as_uuid());
        let mut groups = Vec::with_capacity(run_ids.len());
        for model_run_id in run_ids {
            Self::require_active(cancel)?;
            let mut predictions = grouped.remove(&model_run_id).ok_or_else(|| {
                Self::invalid(format!("decision replay group {model_run_id} disappeared"))
            })?;
            predictions.sort_by_key(|prediction| prediction.context.recommendation_id().as_uuid());
            let first = predictions
                .first()
                .ok_or_else(|| Self::invalid("decision replay group is empty"))?;
            let model_version_id = first.context.model_version_id();
            let artifact = model_cache.get(&model_version_id).cloned().ok_or_else(|| {
                Self::invalid(format!(
                    "decision replay model cache lost version {model_version_id}"
                ))
            })?;
            groups.push((predictions, artifact));
        }

        let concurrency = self.compute_budget.max_concurrency;
        let mut pending = stream::iter(groups.into_iter().map(|(predictions, artifact)| {
            let materializer = self.clone();
            let params = params.clone();
            let cancel = cancel.clone();
            async move {
                materializer
                    .materialize_decision_group(&params, predictions, &artifact, &cancel)
                    .await
            }
        }))
        .buffer_unordered(concurrency);
        let mut count = 0_u64;
        while let Some(result) = pending.next().await {
            count = count.saturating_add(result?);
        }
        Ok(count)
    }

    async fn materialize_decision_group(
        &self,
        params: &FeedbackAttributionJobParams,
        predictions: Vec<MaterializedPrediction>,
        artifact: &ModelArtifact,
        cancel: &CancellationToken,
    ) -> QuantResult<u64> {
        Self::require_active(cancel)?;
        let first = predictions
            .first()
            .ok_or_else(|| Self::invalid("decision replay group is empty"))?;
        let universe = self.build_decision_universe(first, artifact).await?;
        let count = u64::try_from(predictions.len()).map_err(|error| {
            Self::invalid(format!("decision replay group size overflow: {error}"))
        })?;
        let compute_params = params.clone();
        let replays = self
            .run_compute(cancel, move || {
                predictions
                    .iter()
                    .map(|prediction| universe.replay_prediction(&compute_params, prediction))
                    .collect::<QuantResult<Vec<_>>>()
            })
            .await?;
        for replay in replays {
            Self::require_active(cancel)?;
            self.persist(AttributionArtifact::DecisionInterventionReplay(Box::new(
                replay,
            )))
            .await?;
        }
        Ok(count)
    }

    async fn build_decision_universe(
        &self,
        prediction: &MaterializedPrediction,
        artifact: &ModelArtifact,
    ) -> QuantResult<DecisionUniverse> {
        match artifact.payload() {
            ModelPayload::WeightedFactor(payload) => {
                self.build_weighted_universe(prediction, artifact, payload)
                    .await
            }
            ModelPayload::Classical(_) => Err(ResearchError::NotEligible {
                code: "exact_decision_intervention_replay_unavailable",
                detail: format!(
                    "shadow-only classical model {} cannot enter recommendation replay",
                    prediction.context.model_version_id()
                ),
            }
            .into()),
            ModelPayload::SellScorer(_) => Err(ResearchError::NotEligible {
                code: "exact_decision_intervention_replay_unavailable",
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
            if replay.insert(candidate.key, candidate.state).is_some() {
                return Err(Self::invalid(format!(
                    "weighted replay duplicated market {} candidate",
                    member.market_id
                )));
            }
        }
        if replay.is_empty() {
            return Err(Self::invalid(format!(
                "model run {} replayed no Route model states",
                context.model_run_id()
            )));
        }
        let input_contract_hash = model_input_contract_hash(&payload.input_contract)?;
        if input_contract_hash != prediction.explanation.input_contract_hash {
            return Err(Self::invalid(format!(
                "weighted replay input contract differs from explanation for model {}",
                context.model_version_id()
            )));
        }
        Ok(DecisionUniverse {
            policy: DecisionReplayPolicy::try_new(policy.snapshot_hash)?,
            replay: DecisionReplayModel::Weighted(replay),
            model_artifact_hash: artifact.content_hash()?,
            input_contract_hash,
            input_transform_hash: payload.input_transform_hash()?,
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
        Ok(Some(WeightedCandidateReplay {
            key,
            state: WeightedReplayState {
                yes_alpha: score.yes_alpha,
                alpha_deadband: evidence.payload.factor_head.alpha_deadband,
            },
        }))
    }

    async fn materialize_trajectories(
        &self,
        params: &FeedbackAttributionJobParams,
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
                self.compute_budget.page_size,
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
                .materialize_trajectory_page(params, &candidates, cancel)
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
        params: &FeedbackAttributionJobParams,
        candidates: &[FeedbackCohortCandidate],
        cancel: &CancellationToken,
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
        let mut market_ids = seeds
            .iter()
            .map(|seed| seed.context.market_id().clone())
            .collect::<Vec<_>>();
        market_ids.sort();
        market_ids.dedup();
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
        let market_info_until = until
            .checked_add_signed(Duration::milliseconds(1))
            .ok_or_else(|| {
                Self::invalid("trajectory market-info window exceeds timestamp range")
            })?;
        let market_info = self
            .clob_market_info
            .window(&market_ids, from, market_info_until, params.cutoff)
            .await?;
        let pit_book_contract_hash = CanonicalDigest::content_hash_typed(
            TRAJECTORY_CONTRACT_DOMAIN,
            TRAJECTORY_CONTRACT_VERSION,
            &"canonical_full_l2_size_specific_sell_walk_with_pit_fee_schedule",
        )?;
        let alternative_policy = AlternativeExitPolicy::LatestExecutableAtOrBeforeHorizon;
        let alternative_policy_hash = CanonicalDigest::content_hash_typed(
            EXIT_POLICY_DOMAIN,
            EXIT_POLICY_VERSION,
            &alternative_policy,
        )?;
        let compute_params = params.clone();
        let compute_cancel = cancel.clone();
        let trajectories = self
            .run_compute(cancel, move || {
                seeds
                    .into_iter()
                    .map(|seed| {
                        Self::require_active(&compute_cancel)?;
                        let points = Self::trajectory_points(
                            &seed,
                            &rows,
                            &market_info,
                            compute_params.cutoff,
                        )?;
                        let mut source_hashes = points
                            .iter()
                            .map(|point| point.source_fact_hash)
                            .collect::<Vec<_>>();
                        source_hashes.extend(points.iter().filter_map(Self::trajectory_fee_hash));
                        source_hashes.extend([
                            compute_params.truth_artifact.content_hash,
                            seed.attempt.outcome_hash,
                            seed.rollup_hash,
                            pit_book_contract_hash,
                        ]);
                        let lineage = AttributionLineage::try_new(
                            compute_params.feedback_cycle_id,
                            AttributionCohort::Evaluation,
                            compute_params.cutoff,
                            compute_params.generated_at,
                            source_hashes,
                        )?;
                        ExecutionTrajectoryArtifact::try_new(ExecutionTrajectoryInput {
                            lineage,
                            recommendation_id: seed.context.recommendation_id(),
                            order_intent_id: seed.attempt.order_intent_id,
                            attempt_outcome_hash: seed.attempt.outcome_hash,
                            pit_book_contract_hash,
                            entry_at: seed.entry_at,
                            entry_shares: seed.entry_shares,
                            entry_price: seed.entry_price,
                            actual_baseline: seed.actual_baseline,
                            horizon_end: seed.horizon_end,
                            points,
                        })
                    })
                    .collect::<QuantResult<Vec<_>>>()
            })
            .await?;
        let mut trajectory_count = 0_u64;
        let mut counterfactual_count = 0_u64;
        for trajectory in trajectories {
            Self::require_active(cancel)?;
            let persisted = self
                .persist(AttributionArtifact::ExecutionTrajectory(Box::new(
                    trajectory.clone(),
                )))
                .await?;
            trajectory_count = trajectory_count.saturating_add(1);
            let replay_trajectory = trajectory.clone();
            let trajectory_hash = persisted.artifact_hash;
            let counterfactual = self
                .run_compute(cancel, move || {
                    PolicyCounterfactualOutcome::replay(
                        &replay_trajectory,
                        trajectory_hash,
                        alternative_policy_hash,
                        alternative_policy,
                    )
                })
                .await?;
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
            let entry_shares = attempt.filled_shares;
            let actual_baseline =
                Self::actual_execution_baseline(&attempt, entry_shares, entry_price)?;
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
                entry_shares,
                entry_price,
                actual_baseline,
                horizon_end,
            });
        }
        Ok(seeds)
    }

    fn trajectory_points(
        seed: &TrajectorySeed,
        rows: &[BookL2LedgerRow],
        market_info: &[ClobMarketInfoVersion],
        cutoff: DateTime<Utc>,
    ) -> QuantResult<Vec<TrajectoryPoint>> {
        let mut points = BTreeMap::<DateTime<Utc>, (&BookL2LedgerRow, DateTime<Utc>)>::new();
        for row in rows.iter().filter(|row| {
            row.token_id == seed.attempt.token_id
                && row.venue_event_time >= seed.entry_at.timestamp_millis()
                && row.venue_event_time <= seed.horizon_end.timestamp_millis()
                && row.persisted_time <= cutoff.timestamp_millis()
        }) {
            if row
                .market_id
                .as_ref()
                .is_some_and(|market_id| market_id != seed.context.market_id())
            {
                return Err(Self::invalid(format!(
                    "book event {} binds token {} to market {:?}, expected {}",
                    ContentHash::from(row.event_hash),
                    row.token_id,
                    row.market_id,
                    seed.context.market_id()
                )));
            }
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
        points
            .into_iter()
            .map(|(observed_at, (row, available_at))| {
                Ok(TrajectoryPoint {
                    observed_at,
                    available_at,
                    requested_shares: seed.entry_shares,
                    economics: Self::trajectory_point_economics(
                        seed,
                        row,
                        observed_at,
                        market_info,
                    )?,
                    source_fact_hash: ContentHash::from(row.event_hash),
                })
            })
            .collect()
    }

    fn actual_execution_baseline(
        attempt: &ExecutionAttemptOutcomeInfo,
        entry_shares: Shares,
        entry_price: Price,
    ) -> QuantResult<ActualExecutionBaseline> {
        let Some(actual_net_pnl_usd) = attempt.realized_pnl_usd else {
            return Ok(ActualExecutionBaseline::NotEvaluable {
                reason: ActualBaselineNotEvaluableReason::MissingRealizedPnl,
            });
        };
        let Some(entry_fee_usd) = attempt.entry_fee_usd else {
            return Ok(ActualExecutionBaseline::NotEvaluable {
                reason: ActualBaselineNotEvaluableReason::MissingEntryFee,
            });
        };
        let exit_fee_usd = if attempt.exit_filled_shares.is_some() {
            let Some(exit_fee_usd) = attempt.exit_fee_usd else {
                return Ok(ActualExecutionBaseline::NotEvaluable {
                    reason: ActualBaselineNotEvaluableReason::MissingExitFee,
                });
            };
            exit_fee_usd
        } else {
            Usd::ZERO
        };
        let entry_principal_usd = entry_shares * entry_price;
        let entry_cash_outlay_usd = entry_principal_usd + entry_fee_usd;
        let actual_gross_pnl_usd = actual_net_pnl_usd + entry_fee_usd + exit_fee_usd;
        let actual_gross_return_bps =
            Bps::relative(actual_gross_pnl_usd.inner(), entry_principal_usd.inner())
                .ok_or_else(|| Self::invalid("actual gross return has a zero entry principal"))?;
        let actual_net_return_bps =
            Bps::relative(actual_net_pnl_usd.inner(), entry_cash_outlay_usd.inner())
                .ok_or_else(|| Self::invalid("actual net return has a zero entry cash outlay"))?;
        Ok(ActualExecutionBaseline::Evaluated {
            entry_fee_usd,
            exit_fee_usd,
            entry_cash_outlay_usd,
            actual_gross_pnl_usd,
            actual_net_pnl_usd,
            actual_gross_return_bps,
            actual_net_return_bps,
        })
    }

    fn trajectory_point_economics(
        seed: &TrajectorySeed,
        row: &BookL2LedgerRow,
        observed_at: DateTime<Utc>,
        market_info: &[ClobMarketInfoVersion],
    ) -> QuantResult<TrajectoryPointEconomics> {
        let Some(bids) = Self::trajectory_bids(row) else {
            let reason = if row.bid_prices.is_empty() && row.bid_sizes.is_empty() {
                TrajectoryPointNotEvaluableReason::NoBidDepth
            } else {
                TrajectoryPointNotEvaluableReason::InvalidBookDepth
            };
            return Ok(TrajectoryPointEconomics::NotEvaluable { reason });
        };
        let Some(fee_schedule) = Self::trajectory_fee_schedule(seed, observed_at, market_info)
        else {
            return Ok(TrajectoryPointEconomics::NotEvaluable {
                reason: TrajectoryPointNotEvaluableReason::FeeScheduleUnavailable,
            });
        };
        let Ok(fee_schedule) = fee_schedule else {
            return Ok(TrajectoryPointEconomics::NotEvaluable {
                reason: TrajectoryPointNotEvaluableReason::InvalidFeeSchedule,
            });
        };
        let best_bid_price = bids[0].price_decimal();
        let limit_price = bids
            .last()
            .map(|level| level.price_decimal())
            .ok_or_else(|| Self::invalid("validated trajectory bid ladder became empty"))?;
        let Ok(fill) = walk_sell_exact_shares(
            &bids,
            seed.entry_shares,
            limit_price,
            FillRequirement::AllowPartial,
            &fee_schedule,
            LiquidityRole::Taker,
            observed_at,
        ) else {
            return Ok(TrajectoryPointEconomics::NotEvaluable {
                reason: TrajectoryPointNotEvaluableReason::InvalidFeeSchedule,
            });
        };
        let depth_levels_consumed = Self::depth_levels_consumed(&bids, seed.entry_shares)?;
        match fill.outcome {
            BookWalkOutcome::Filled => {
                let executable_exit_price = fill
                    .vwap
                    .ok_or_else(|| Self::invalid("filled trajectory walk omitted its VWAP"))?;
                let slippage_bps = Bps::relative(
                    best_bid_price.inner() - executable_exit_price.inner(),
                    best_bid_price.inner(),
                )
                .ok_or_else(|| Self::invalid("trajectory best bid is zero"))?;
                Ok(TrajectoryPointEconomics::Executable {
                    filled_shares: fill.filled_shares,
                    remaining_shares: fill.unfilled_shares,
                    depth_levels_consumed,
                    best_bid_price,
                    executable_exit_price,
                    gross_exit_proceeds_usd: fill.immediate_cost.principal_usd,
                    exit_fee_usd: fill.immediate_cost.total_fee_usd(),
                    net_exit_proceeds_usd: Usd::new(fill.account_cash_delta_usd),
                    fee_schedule_hash: fee_schedule.schedule_hash,
                    slippage_bps,
                })
            }
            BookWalkOutcome::Partial => {
                let partial_vwap = fill
                    .vwap
                    .ok_or_else(|| Self::invalid("partial trajectory walk omitted its VWAP"))?;
                let partial_slippage_bps = Bps::relative(
                    best_bid_price.inner() - partial_vwap.inner(),
                    best_bid_price.inner(),
                )
                .ok_or_else(|| Self::invalid("trajectory best bid is zero"))?;
                Ok(TrajectoryPointEconomics::InsufficientDepth {
                    filled_shares: fill.filled_shares,
                    remaining_shares: fill.unfilled_shares,
                    depth_levels_consumed,
                    best_bid_price,
                    partial_vwap,
                    partial_gross_proceeds_usd: fill.immediate_cost.principal_usd,
                    partial_exit_fee_usd: fill.immediate_cost.total_fee_usd(),
                    partial_net_proceeds_usd: Usd::new(fill.account_cash_delta_usd),
                    fee_schedule_hash: fee_schedule.schedule_hash,
                    partial_slippage_bps,
                })
            }
            BookWalkOutcome::Unfilled => Ok(TrajectoryPointEconomics::NotEvaluable {
                reason: TrajectoryPointNotEvaluableReason::NoBidDepth,
            }),
        }
    }

    fn trajectory_bids(row: &BookL2LedgerRow) -> Option<Vec<BookLevel>> {
        if row.bid_prices.is_empty() || row.bid_prices.len() != row.bid_sizes.len() {
            return None;
        }
        let bids = row
            .bid_prices
            .iter()
            .copied()
            .zip(row.bid_sizes.iter().copied())
            .map(|(price, size)| BookLevel::from_decimal(price.into(), size.into()).ok())
            .collect::<Option<Vec<_>>>()?;
        if bids.iter().any(|level| !level.size_decimal().is_positive())
            || !bids
                .windows(2)
                .all(|pair| pair[0].price_decimal() >= pair[1].price_decimal())
        {
            return None;
        }
        Some(bids)
    }

    fn trajectory_fee_schedule(
        seed: &TrajectorySeed,
        observed_at: DateTime<Utc>,
        market_info: &[ClobMarketInfoVersion],
    ) -> Option<Result<PitFeeSchedule, ()>> {
        let version = market_info
            .iter()
            .filter(|version| {
                version.market_id == *seed.context.market_id()
                    && version
                        .tokens
                        .iter()
                        .any(|token| token.token_id == seed.attempt.token_id)
                    && version.effective_at <= observed_at
                    && version.available_at <= observed_at
            })
            .max_by(|left, right| {
                (left.effective_at, left.available_at, left.payload_hash).cmp(&(
                    right.effective_at,
                    right.available_at,
                    right.payload_hash,
                ))
            })?;
        if version.validate().is_err() {
            return Some(Err(()));
        }
        Some(PitFeeSchedule::from_market_fee_schedule(&version.fee_schedule()).map_err(|_| ()))
    }

    fn depth_levels_consumed(bids: &[BookLevel], target: Shares) -> QuantResult<u32> {
        let mut remaining = target;
        let mut consumed = 0_usize;
        for level in bids {
            if !remaining.is_positive() {
                break;
            }
            let level_shares = level.size_decimal().min(remaining);
            if level_shares.is_positive() {
                remaining -= level_shares;
                consumed = consumed.saturating_add(1);
            }
        }
        u32::try_from(consumed)
            .map_err(|error| Self::invalid(format!("trajectory depth exceeds u32: {error}")))
    }

    const fn trajectory_fee_hash(point: &TrajectoryPoint) -> Option<ContentHash> {
        match point.economics {
            TrajectoryPointEconomics::Executable {
                fee_schedule_hash, ..
            }
            | TrajectoryPointEconomics::InsufficientDepth {
                fee_schedule_hash, ..
            } => Some(fee_schedule_hash),
            TrajectoryPointEconomics::NotEvaluable { .. } => None,
        }
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
