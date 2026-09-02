//! Durable serving-evidence replay for feature/model parity runs.
//!
//! Online evidence is read only from the append-only serving ledgers. Replay
//! resolves catalog and market facts again through the historical PIT path and
//! loads the exact frozen model artifact. The two sides never share computed
//! values, which prevents a self-comparison from reporting a false pass.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{
    QuantError, QuantResult, infra::InfraError, research::ResearchError, storage::StorageError,
};
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
    },
    config::FeatureParityComputeConfig,
    domain::{
        data_plane::{
            DecisionBoundary, DecisionClock, DecisionSource, ExchangeHistoryFrontier,
            HistorySealChunkRef, HistoryServingHeadSeal,
        },
        quant::{
            FactorValueInfo, FeatureParityRunInfo, FeatureVectorInfo, FrozenFeatureParitySubject,
            FrozenFeatureParitySubjectId, MarketSelectionInfo, MarketSelectionMemberInfo,
            ModelRunInfo, ModelRunParityEvidence, ModelVersionInfo, RecommendationReportInfo,
            ReportDataQualitySnapshotInfo, ReportRouteRunInfo, ReportRunInfo, RepresentedRouteSet,
            RouteHistoryLineage, RouteModelLineage, parity_candidate_membership_hash,
            parity_selection_hash, report_parity_evidence_hash, report_parity_generation_hash,
        },
    },
    enums::{
        clickhouse::ChFeatureCellState,
        model::ModelFamily,
        quant::{
            DataQualityStatus, FeatureCellState, FeatureParityRunKind, FeatureParityStage,
            ModelRunKind, ModelRunStatus,
        },
    },
    runtime_config::{BuyModelRoute, DecisionPolicySnapshot},
    types::{
        ContentHash, DecisionCaptureEvidence, DecisionPolicySnapshotId, FeatureParityDetailSource,
        FeatureVectorId, FinalizedExecutionEvidence, HistoryServingHeadSealId, MarketId,
        MarketSelectionId, ModelRunId, ModelVersionId, RecommendationReportId,
        SelectionExclusionSummary, SelectorHashEvidence, SelectorParityEvidence, Usd,
        factor::FactorServingPlane, stable_name::FeatureName,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    ExchangeHistoryRepository, FactorRepository, FeatureParityRepository, FeatureRepository,
    MarketLinkageRepository, MarketSelectionRepository, ModelRunRepository, PolicyRepository,
    QuantFactReadRepository, RecommendationReportRepository, ReportRunRepository,
    ServingEvidenceRepository,
};
use quant_pivot_research::{
    factors::{FactorEngine, FactorValue, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, ExecutableFeatureSchema, FeatureVector, MarketDecisionCapture,
        feature_events,
    },
    hashing::ResearchHasher,
    model::{
        ModelCalibrationScore, ModelInputAuditRow, ModelRuntimeMetrics, ModelRuntimeOutput,
        SignalCandidate, canonical_business_prediction_hash, finalize_candidates,
    },
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelectionSnapshot,
        MarketSelector, ModelFeatureRequirements, RouteAvailabilityContract, SelectedMarket,
    },
};
use serde::Serialize;
use tokio::{runtime::Handle, sync::Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    observability::serving_evidence::{ModelInputEvidenceBatch, verify_completion},
    pit::platform::ch_historical::DurablePitSource,
    prefetch::{
        historical_window::{
            HistoricalWindowLoader, ReplaySample, WindowSpec, historical_window_from_prefetched,
        },
        market_candidates::MarketCandidateProvider,
    },
    projection::inference_batch::build_runtime_input,
    report::universe::{ReportUniverseContract, ReportUniverseRoute},
    service::{
        bias_table_fit::resolve_frozen_bias_table,
        feature_parity_executor::{
            FeatureParityCandidate, FeatureParityComparison, FeatureParityEvidence,
            FeatureParityInputWitness, FeatureParityReplayAttempt, FeatureParityReplaySource,
            FeatureParitySubject, PendingFeatureParityComparison,
        },
        historical_replay::{
            CrossSectionRequest, ReplayCaptureKey, ReplayConfig, ReplayCrossSection,
            ReplayExecutionSource, ReplayFactorMode, materialize_cross_section,
        },
        model_serving_generation::{
            ModelServingGenerationRequest, ModelServingGenerationStore, ModelServingRouteSnapshot,
        },
        report_boundary::ReportBoundaryEvidence,
    },
};
/// Process-lifetime dependencies of the production parity source.
#[derive(Clone)]
pub struct DurableFeatureParityDeps {
    pub parity: Arc<dyn FeatureParityRepository>,
    pub model_runs: Arc<dyn ModelRunRepository>,
    pub serving_generations: Arc<ModelServingGenerationStore>,
    pub runtime_configs: Arc<dyn PolicyRepository>,
    pub selections: Arc<dyn MarketSelectionRepository>,
    pub feature_vectors: Arc<dyn FeatureRepository>,
    pub factors: Arc<dyn FactorRepository>,
    pub reports: Arc<dyn RecommendationReportRepository>,
    pub report_runs: Arc<dyn ReportRunRepository>,
    pub serving_evidence: Arc<dyn ServingEvidenceRepository>,
    pub fact_read: Arc<dyn QuantFactReadRepository>,
    pub catalog: Arc<dyn CatalogLedgerRepository>,
    pub clob_market_info: Arc<dyn ClobMarketInfoRepository>,
    pub linkages: Arc<dyn MarketLinkageRepository>,
    pub calibration_artifacts: Arc<dyn CalibrationArtifactRepository>,
    pub exchange_history: Arc<dyn ExchangeHistoryRepository>,
    pub compute: Arc<ComputeExecutor>,
    pub compute_budget: FeatureParityComputeConfig,
}

/// Replays successful serving runs from durable PIT data and frozen artifacts.
#[derive(Clone)]
pub struct DurableFeatureParitySource {
    deps: DurableFeatureParityDeps,
    compute_boundary: ParityComputeBoundary,
}

#[derive(Clone)]
struct ParityComputeBoundary {
    executor: Arc<ComputeExecutor>,
    memory: OfflineMemory,
    slots: Arc<Semaphore>,
}

impl ParityComputeBoundary {
    fn new(executor: Arc<ComputeExecutor>, memory: OfflineMemory, concurrency: usize) -> Self {
        Self {
            executor,
            memory,
            slots: Arc::new(Semaphore::new(concurrency)),
        }
    }

    async fn run<T, F>(&self, cancel: &CancellationToken, work: F) -> QuantResult<T>
    where
        T: Send + 'static,
        F: FnOnce() -> QuantResult<T> + Send + 'static,
    {
        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(ResearchError::Cancelled {
                    detail: "cancelled while waiting for feature parity compute capacity".to_owned(),
                }
                .into());
            }
            permit = Arc::clone(&self.slots).acquire_owned() => {
                permit.map_err(|_| InfraError::ComputeExecution {
                    detail: "feature parity compute semaphore closed".to_owned(),
                })?
            }
        };
        self.executor
            .run_offline_cancellable(self.memory, cancel, move || {
                let _permit = permit;
                work()
            })
            .await
    }
}

struct DurableReplayEvidence {
    completion_by_run: HashMap<ModelRunId, QuantServingEvidenceCompletionRow>,
    inputs_by_run: HashMap<ModelRunId, Vec<QuantModelInputEventRow>>,
    features_by_vector: HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    info_by_id: HashMap<FeatureVectorId, FeatureVectorInfo>,
}

#[derive(Clone)]
struct ReplayRunContext {
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    config: DecisionPolicySnapshot,
    boundary: DecisionBoundary,
    report_id: Option<RecommendationReportId>,
    history: Option<RouteHistoryLineage>,
    represented_routes: Option<RepresentedRouteSet>,
    report_selector: Option<ReportSelectorBinding>,
    selection: MarketSelectionInfo,
    members: Vec<MarketSelectionMemberInfo>,
    samples: Vec<ReplaySample>,
    finalized_execution_evidences: HashMap<MarketId, FinalizedExecutionEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReplayHistoryMode {
    NotRequired,
    RuntimeDisabled,
    RuntimeEnabled {
        accepted_through_block: u64,
        accepted_through_at: DateTime<Utc>,
    },
    Materialized {
        available_by: DateTime<Utc>,
    },
}

struct ReplayHistoryWindow {
    chunks: Vec<HistorySealChunkRef>,
    load_execution_history: bool,
    materialized_available_by: Option<DateTime<Utc>>,
}

#[derive(Clone)]
struct ReportReplayContext {
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    config: DecisionPolicySnapshot,
    boundary: DecisionBoundary,
    represented_routes: RepresentedRouteSet,
    selector_binding: ReportSelectorBinding,
    selection: MarketSelectionInfo,
    members: Vec<MarketSelectionMemberInfo>,
}

#[derive(Clone)]
enum ReportSelectorBinding {
    Runtime {
        serving_head_seal_id: HistoryServingHeadSealId,
        serving_head_seal_hash: ContentHash,
        universe_plan_hash: ContentHash,
    },
    Materialized,
}

impl ReportSelectorBinding {
    const fn new(history: &RouteHistoryLineage, universe_plan_hash: ContentHash) -> Self {
        match history {
            RouteHistoryLineage::Runtime {
                serving_head_seal_id,
                serving_head_seal_hash,
            } => Self::Runtime {
                serving_head_seal_id: *serving_head_seal_id,
                serving_head_seal_hash: *serving_head_seal_hash,
                universe_plan_hash,
            },
            RouteHistoryLineage::Materialized { .. } => Self::Materialized,
        }
    }

    fn verify_universe(
        &self,
        contract: ReportUniverseContract,
    ) -> QuantResult<ReplaySelectionContract> {
        if !matches!(
            self,
            Self::Runtime { universe_plan_hash, .. }
                if *universe_plan_hash == contract.availability.universe_plan_hash
        ) {
            return Err(determinism(
                "report selector universe differs from its frozen all-active-route lineage"
                    .to_owned(),
            ));
        }
        Ok(ReplaySelectionContract {
            model_requirements: contract.requirements,
            route_availability: Some(contract.availability),
        })
    }
}

struct ReplaySelectionContract {
    model_requirements: ModelFeatureRequirements,
    route_availability: Option<RouteAvailabilityContract>,
}

struct MaterializedRunReplay {
    builder: ConfiguredFeatureBuilder,
    factor_engine: FactorEngine,
    bias_table_hash: Option<ContentHash>,
    selection: MarketSelectionSnapshot,
    cross_section: ReplayCrossSection,
    serving: ModelServingRouteSnapshot,
}

struct MaterializedSelectionReplay {
    builder: ConfiguredFeatureBuilder,
    factor_engine: FactorEngine,
    bias_table_hash: Option<ContentHash>,
    selection: MarketSelectionSnapshot,
    replay_config: ReplayConfig,
    required_features: Vec<FeatureName>,
    serving: ModelServingRouteSnapshot,
}

struct ModelRouteReplayRequest<'a> {
    run: &'a ModelRunInfo,
    config: &'a DecisionPolicySnapshot,
    boundary: &'a DecisionBoundary,
    online_inputs: &'a [QuantModelInputEventRow],
    markets: &'a [SelectedMarket],
    vectors: &'a [FeatureVector],
    vector_binding: &'a HashMap<MarketId, FeatureVectorId>,
    factor_engine: &'a FactorEngine,
    bias_table_hash: Option<ContentHash>,
    serving: &'a ModelServingRouteSnapshot,
    cancel: &'a CancellationToken,
}

struct ReplayRunInput<'a> {
    candidate: &'a FeatureParityCandidate,
    run: &'a ModelRunInfo,
    completion: &'a QuantServingEvidenceCompletionRow,
    online_inputs: &'a [QuantModelInputEventRow],
    online_features: &'a HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: &'a HashMap<FeatureVectorId, FeatureVectorInfo>,
    cancel: &'a CancellationToken,
}

struct FinalRunComparisonInput {
    candidate: FeatureParityCandidate,
    run: ModelRunInfo,
    context: ReplayRunContext,
    online_inputs: Vec<QuantModelInputEventRow>,
    replay: ReplayedModelOutput,
    online_factors: Box<[FactorValueInfo]>,
    comparisons: Vec<FeatureParityComparison>,
}

struct ModelRouteInputs {
    markets: Vec<SelectedMarket>,
    vectors: Vec<FeatureVector>,
}

#[derive(Clone, Copy)]
struct ComparisonSubject<'a> {
    report: Option<&'a RecommendationReportId>,
    model_run: Option<&'a ModelRunId>,
    model_version: Option<&'a ModelVersionId>,
}

#[derive(Clone, Copy)]
struct FeatureComparisonInputs<'a> {
    online_features: &'a HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: &'a HashMap<FeatureVectorId, FeatureVectorInfo>,
    replay_by_market: &'a HashMap<MarketId, FeatureVector>,
    replay_captures: &'a HashMap<ReplayCaptureKey, MarketDecisionCapture>,
    vector_binding: &'a HashMap<MarketId, FeatureVectorId>,
    boundary: &'a DecisionBoundary,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    schema: &'a ExecutableFeatureSchema,
}

struct PreparedReportReplay {
    report_id: RecommendationReportId,
    context: ReportReplayContext,
}

#[derive(Default)]
struct CandidateSubjects {
    model_runs: Vec<ModelRunInfo>,
    reports: Vec<RecommendationReportInfo>,
}

impl DurableFeatureParitySource {
    pub fn try_new(deps: DurableFeatureParityDeps) -> QuantResult<Self> {
        let budget = deps.compute_budget;
        if !(1..=1_000).contains(&budget.page_size)
            || budget.max_concurrency != 1
            || !(1_048_576..=10_737_418_240).contains(&budget.max_working_set_bytes)
            || !(1..=86_400).contains(&budget.deadline_secs)
        {
            return Err(InfraError::Misconfigured {
                detail:
                    "feature parity page size, concurrency, working set, or deadline is invalid"
                        .to_owned(),
            }
            .into());
        }
        let working_set = usize::try_from(budget.max_working_set_bytes).map_err(|error| {
            InfraError::Misconfigured {
                detail: format!("feature parity working-set bytes do not fit usize: {error}"),
            }
        })?;
        let compute_boundary = ParityComputeBoundary::new(
            Arc::clone(&deps.compute),
            OfflineMemory::try_bytes(working_set)?,
            budget.max_concurrency,
        );
        Ok(Self {
            deps,
            compute_boundary,
        })
    }

    fn require_active(cancel: &CancellationToken) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "feature parity replay cancelled inside a compute kernel".to_owned(),
            }
            .into());
        }
        Ok(())
    }
}

#[async_trait]
impl FeatureParityReplaySource for DurableFeatureParitySource {
    async fn list_candidates(
        &self,
        run: &FeatureParityRunInfo,
        cancel: &CancellationToken,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        let bounded_cancel = cancel.child_token();
        let discovery = async {
            let candidates = self.candidate_pool(run).await?;
            self.qualify_candidates(run, candidates, &bounded_cancel)
                .await
        };
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                bounded_cancel.cancel();
                Err(ResearchError::Cancelled {
                    detail: "feature parity discovery cancelled".to_owned(),
                }.into())
            }
            result = tokio::time::timeout(
                Duration::from_secs(self.deps.compute_budget.deadline_secs), Box::pin(discovery),
            ) => result.unwrap_or_else(|_| {
                bounded_cancel.cancel();
                Err(ResearchError::ComputeDeadlineExceeded {
                    operation: "feature_parity_discovery",
                    deadline_secs: self.deps.compute_budget.deadline_secs,
                }.into())
            }),
        }
    }

    async fn replay(
        &self,
        _parity_run: &FeatureParityRunInfo,
        candidates: &[FeatureParityCandidate],
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityReplayAttempt> {
        validate_witness_states(candidates)?;
        let bounded_cancel = cancel.child_token();
        let replay = Box::pin(self.replay_pages(candidates, &bounded_cancel));
        Box::pin(tokio::time::timeout(
            Duration::from_secs(self.deps.compute_budget.deadline_secs),
            replay,
        ))
        .await
        .unwrap_or_else(|_| {
            bounded_cancel.cancel();
            Err(ResearchError::ComputeDeadlineExceeded {
                operation: "feature_parity_replay",
                deadline_secs: self.deps.compute_budget.deadline_secs,
            }
            .into())
        })
    }
}

impl DurableFeatureParitySource {
    async fn candidate_pool(
        &self,
        run: &FeatureParityRunInfo,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        let frozen = self.deps.parity.load_frozen_subjects(&run.run_id).await?;
        if !frozen.is_empty() {
            return self.frozen_candidates(run, frozen).await;
        }
        if run.kind == FeatureParityRunKind::Sampled
            || (run.kind == FeatureParityRunKind::Full
                && run.report_id.is_none()
                && run.model_version_id.is_none()
                && run.training_dataset_id.is_none())
        {
            return Err(determinism(format!(
                "parity run {} has no atomically frozen serving subjects",
                run.run_id
            )));
        }
        let subjects = self.candidate_subjects(run).await?;
        for row in &subjects.model_runs {
            validate_candidate_run(row, run)?;
        }
        let mut candidates = self.candidates_for_runs(subjects.model_runs).await?;
        candidates.extend(self.candidates_for_reports(subjects.reports, run).await?);
        Ok(candidates)
    }

    async fn qualify_candidates(
        &self,
        run: &FeatureParityRunInfo,
        mut candidates: Vec<FeatureParityCandidate>,
        cancel: &CancellationToken,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        let page_size = usize::try_from(self.deps.compute_budget.page_size).map_err(|error| {
            InfraError::Misconfigured {
                detail: format!("feature parity page size does not fit usize: {error}"),
            }
        })?;
        let mut indices_by_model = HashMap::<ModelRunId, Vec<usize>>::new();
        for (index, candidate) in candidates.iter().enumerate() {
            if let FeatureParitySubject::ModelRun(id) = &candidate.subject {
                indices_by_model.entry(*id).or_default().push(index);
            }
        }
        let mut model_runs = indices_by_model.keys().copied().collect::<Vec<_>>();
        model_runs.sort_unstable_by_key(|id| id.as_uuid());
        for page in model_runs.chunks(page_size) {
            Self::require_active(cancel)?;
            let evidence = self.load_replay_evidence(page, cancel).await?;
            for model_run_id in page {
                let indices = indices_by_model.get(model_run_id).ok_or_else(|| {
                    determinism(format!(
                        "model {model_run_id} lost its qualification indices"
                    ))
                })?;
                let witness = {
                    let candidate = indices.first().and_then(|index| candidates.get(*index))
                        .ok_or_else(|| determinism(format!(
                            "model {model_run_id} lost its frozen candidates during qualification"
                        )))?;
                    self.model_input_witnesses(run, candidate, model_run_id, &evidence, cancel)
                        .await?
                };
                for index in indices {
                    let candidate = candidates.get_mut(*index).ok_or_else(|| {
                        determinism(format!(
                            "model {model_run_id} has an invalid qualification index"
                        ))
                    })?;
                    candidate.input_witness = match &witness {
                        None => FeatureParityInputWitness::PendingServingEvidence,
                        Some(bindings) => candidate
                            .market_id
                            .as_ref()
                            .and_then(|market| bindings.get(market))
                            .map_or(FeatureParityInputWitness::SelectionOnly, |vector_id| {
                                FeatureParityInputWitness::VerifiedModelInput {
                                    feature_vector_id: *vector_id,
                                }
                            }),
                    };
                }
            }
        }
        Self::require_active(cancel)?;
        Ok(candidates)
    }

    async fn model_input_witnesses(
        &self,
        parity_run: &FeatureParityRunInfo,
        candidate: &FeatureParityCandidate,
        model_run_id: &ModelRunId,
        evidence: &DurableReplayEvidence,
        cancel: &CancellationToken,
    ) -> QuantResult<Option<HashMap<MarketId, FeatureVectorId>>> {
        let Some(completion) = evidence.completion_by_run.get(model_run_id) else {
            return Ok(None);
        };
        Self::require_active(cancel)?;
        let run = self
            .deps
            .model_runs
            .find_by_id(model_run_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_model_run", model_run_id))?;
        validate_candidate_run(&run, parity_run)?;
        let inputs = validate_run_completion(
            model_run_id,
            candidate,
            completion,
            &evidence.inputs_by_run,
            &evidence.features_by_vector,
        )?;
        let vector_ids = completion_vector_ids(completion)?;
        let features = vector_ids
            .iter()
            .map(|id| {
                evidence
                    .features_by_vector
                    .get(id)
                    .cloned()
                    .map(|rows| (*id, rows))
                    .ok_or_else(|| {
                        determinism(format!(
                            "model {model_run_id} has no committed feature vector {id}"
                        ))
                    })
            })
            .collect::<QuantResult<HashMap<_, _>>>()?;
        let infos = vector_ids
            .iter()
            .map(|id| {
                evidence
                    .info_by_id
                    .get(id)
                    .cloned()
                    .map(|info| (*id, info))
                    .ok_or_else(|| {
                        determinism(format!(
                            "model {model_run_id} has no Postgres feature vector {id}"
                        ))
                    })
            })
            .collect::<QuantResult<HashMap<_, _>>>()?;
        let context = self
            .prepare_replay_run(candidate, &run, completion, inputs, &features, &infos)
            .await?;
        let all_vectors = feature_vector_binding(&features)?;
        let route = route_for_model(&context.config, context.model_version_id)?;
        let route_vectors = online_route_binding(&all_vectors, &features, &context.members, route)?;
        let bindings = verified_input_bindings(&run, inputs)?;
        validate_input_population(&bindings, &route_vectors)?;
        Self::require_active(cancel)?;
        Ok(Some(bindings))
    }
    async fn replay_pages(
        &self,
        candidates: &[FeatureParityCandidate],
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityReplayAttempt> {
        let page_size = usize::try_from(self.deps.compute_budget.page_size).map_err(|error| {
            InfraError::Misconfigured {
                detail: format!("feature parity page size does not fit usize: {error}"),
            }
        })?;
        let mut combined = FeatureParityReplayAttempt::default();
        for page in candidates.chunks(page_size) {
            if cancel.is_cancelled() {
                return Err(ResearchError::Cancelled {
                    detail: "feature parity replay cancelled between bounded pages".to_owned(),
                }
                .into());
            }
            let (ready, pending) = partition_witness_candidates(page)?;
            combined.pending.extend(pending);
            if ready.is_empty() {
                continue;
            }
            let run_ids = unique_run_ids(&ready);
            let evidence = self.load_replay_evidence(&run_ids, cancel).await?;
            let mut attempt = self
                .replay_candidate_groups(&ready, &evidence, cancel)
                .await?;
            let report_attempt = self.replay_report_candidate_groups(&ready, cancel).await?;
            attempt.comparisons.extend(report_attempt.comparisons);
            attempt.pending.extend(report_attempt.pending);
            combined.comparisons.extend(attempt.comparisons);
            combined.pending.extend(attempt.pending);
        }
        Ok(combined)
    }

    async fn frozen_candidates(
        &self,
        run: &FeatureParityRunInfo,
        frozen: Vec<FrozenFeatureParitySubject>,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        let mut candidates = Vec::new();
        for subject in frozen {
            self.validate_frozen_subject(run, &subject).await?;
            let subject_label = match &subject.subject_id {
                FrozenFeatureParitySubjectId::ModelRun(id) => id.to_string(),
                FrozenFeatureParitySubjectId::RecommendationReport(id) => id.to_string(),
                FrozenFeatureParitySubjectId::ModelVersion { .. } => {
                    return Err(determinism(format!(
                        "offline model-version proof {} cannot enter serving replay",
                        run.run_id
                    )));
                }
            };
            let owner = match subject.subject_id {
                FrozenFeatureParitySubjectId::ModelRun(id) => FeatureParitySubject::ModelRun(id),
                FrozenFeatureParitySubjectId::RecommendationReport(id) => {
                    FeatureParitySubject::RecommendationReport(id)
                }
                FrozenFeatureParitySubjectId::ModelVersion { .. } => {
                    return Err(determinism(format!(
                        "offline model-version proof {} cannot enter serving replay",
                        run.run_id
                    )));
                }
            };
            let decision_at = subject.decision_at.ok_or_else(|| {
                determinism(format!(
                    "serving parity subject {subject_label} has no decision time"
                ))
            })?;
            if subject.candidates.is_empty() {
                if matches!(owner, FeatureParitySubject::ModelRun(_)) {
                    return Err(determinism(format!(
                        "frozen model-run subject {subject_label} has no market membership"
                    )));
                }
                candidates.push(FeatureParityCandidate {
                    sampling_key: format!("report/{subject_label}/selection"),
                    subject: owner,
                    market_id: None,
                    decision_at,
                    input_witness: FeatureParityInputWitness::SelectionOnly,
                });
                continue;
            }
            for candidate in subject.candidates {
                candidates.push(FeatureParityCandidate {
                    sampling_key: format!("{subject_label}/{}", candidate.market_id),
                    subject: owner.clone(),
                    market_id: Some(candidate.market_id),
                    decision_at,
                    input_witness: FeatureParityInputWitness::SelectionOnly,
                });
            }
        }
        Ok(candidates)
    }

    async fn validate_frozen_subject(
        &self,
        parity_run: &FeatureParityRunInfo,
        subject: &FrozenFeatureParitySubject,
    ) -> QuantResult<()> {
        if matches!(
            subject.subject_id,
            FrozenFeatureParitySubjectId::ModelVersion { .. }
        ) {
            return Err(determinism(format!(
                "offline model-version proof {} cannot enter serving replay",
                parity_run.run_id
            )));
        }
        let (selection_id, decision_at) =
            self.validate_frozen_selection(parity_run, subject).await?;

        match &subject.subject_id {
            FrozenFeatureParitySubjectId::ModelRun(model_run_id) => {
                let model_run = self
                    .deps
                    .model_runs
                    .find_by_id(model_run_id)
                    .await?
                    .ok_or_else(|| StorageError::not_found("quant_model_run", model_run_id))?;
                let output_hash = model_run.output_hash.as_ref().ok_or_else(|| {
                    determinism(format!(
                        "frozen model run {model_run_id} no longer has an output hash"
                    ))
                })?;
                let evidence_hash = ModelRunParityEvidence {
                    model_run_id: &model_run.model_run_id,
                    input_hash: &model_run.input_hash,
                    output_hash,
                    model_version_id: &model_run.model_version_id,
                    decision_policy_snapshot_id: &model_run.decision_policy_snapshot_id,
                }
                .content_hash()?;
                if model_run.run_kind != ModelRunKind::LiveInference
                    || model_run.status != ModelRunStatus::Succeeded
                    || model_run.market_selection_id.as_ref() != Some(selection_id)
                    || model_run.window_start != decision_at
                    || output_hash != &subject.subject_generation
                    || evidence_hash != subject.evidence_hash
                {
                    return Err(determinism(format!(
                        "frozen model-run evidence drifted for parity run {} subject {model_run_id}",
                        parity_run.run_id
                    )));
                }
                if let Some(report_id) = parity_run.report_id.as_ref() {
                    let report =
                        self.deps
                            .reports
                            .find_by_id(report_id)
                            .await?
                            .ok_or_else(|| {
                                StorageError::not_found("quant_recommendation_report", report_id)
                            })?;
                    let route_run = self
                        .deps
                        .reports
                        .find_model_route_run(model_run_id)
                        .await?
                        .ok_or_else(|| {
                            StorageError::not_found(
                                "quant_report_route_run.model_run_id",
                                model_run_id,
                            )
                        })?;
                    if route_run.report_run_id != report.report_run_id
                        || route_run.model_run_id.as_ref() != Some(model_run_id)
                        || route_run.model_version_id != model_run.model_version_id
                    {
                        return Err(determinism(format!(
                            "parity run {} report Route lineage does not bind frozen model run {model_run_id}",
                            parity_run.run_id
                        )));
                    }
                }
            }
            FrozenFeatureParitySubjectId::RecommendationReport(report_id) => {
                let report = self
                    .deps
                    .reports
                    .find_by_id(report_id)
                    .await?
                    .ok_or_else(|| {
                        StorageError::not_found("quant_recommendation_report", report_id)
                    })?;
                let generation = report_parity_generation_hash(
                    &report.recommendation_report_id,
                    report.decision_at,
                    report.created_at,
                )?;
                let evidence_hash = report_parity_evidence_hash(
                    &generation,
                    &report.represented_routes_json,
                    &report.scenario_artifact_hash,
                    &report.decision_policy_snapshot_id,
                    &report.market_selection_id,
                    &report.data_quality_snapshot_ref,
                    &report.portfolio_plan_id,
                )?;
                if &report.market_selection_id != selection_id
                    || report.decision_at != decision_at
                    || generation != subject.subject_generation
                    || evidence_hash != subject.evidence_hash
                    || parity_run
                        .report_id
                        .as_ref()
                        .is_some_and(|bound| bound != report_id)
                {
                    return Err(determinism(format!(
                        "frozen report evidence drifted for parity run {} subject {report_id}: \
                         selection_match={} decision_match={} generation_expected={} \
                         generation_frozen={} evidence_expected={} evidence_frozen={} \
                         run_report_id={:?}",
                        parity_run.run_id,
                        &report.market_selection_id == selection_id,
                        report.decision_at == decision_at,
                        generation,
                        subject.subject_generation,
                        evidence_hash,
                        subject.evidence_hash,
                        parity_run.report_id,
                    )));
                }
            }
            FrozenFeatureParitySubjectId::ModelVersion { .. } => {
                return Err(determinism(format!(
                    "offline model-version proof {} cannot enter serving replay",
                    parity_run.run_id
                )));
            }
        }
        Ok(())
    }

    async fn validate_frozen_selection<'a>(
        &self,
        parity_run: &FeatureParityRunInfo,
        subject: &'a FrozenFeatureParitySubject,
    ) -> QuantResult<(&'a MarketSelectionId, DateTime<Utc>)> {
        let selection_id = subject.market_selection_id.as_ref().ok_or_else(|| {
            determinism(format!(
                "serving parity run {} has no frozen market selection",
                parity_run.run_id
            ))
        })?;
        let decision_at = subject.decision_at.ok_or_else(|| {
            determinism(format!(
                "serving parity run {} has no frozen decision time",
                parity_run.run_id
            ))
        })?;
        let selection_hash = subject.selection_hash.as_ref().ok_or_else(|| {
            determinism(format!(
                "serving parity run {} has no frozen selection hash",
                parity_run.run_id
            ))
        })?;
        let selection = self
            .deps
            .selections
            .find_by_id(selection_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_market_selection", selection_id))?;
        let mut members = self.deps.selections.list_members(selection_id).await?;
        members.sort_by(|left, right| left.market_id.cmp(&right.market_id));
        let market_ids = members
            .iter()
            .map(|member| member.market_id.clone())
            .collect::<Vec<_>>();
        let expected_selection_hash = parity_selection_hash(
            &selection.market_selection_id,
            &selection.selector_hash,
            &market_ids,
        )?;
        if &expected_selection_hash != selection_hash
            || usize::try_from(selection.market_count).ok() != Some(members.len())
            || subject.candidates.len() != members.len()
        {
            return Err(determinism(format!(
                "frozen selection evidence drifted for parity run {} subject {:?}",
                parity_run.run_id, subject.subject_id
            )));
        }
        for (ordinal, (candidate, member)) in subject.candidates.iter().zip(&members).enumerate() {
            let ordinal = i32::try_from(ordinal)
                .map_err(|_| determinism("frozen selection exceeds i32 capacity".to_owned()))?;
            let expected_membership =
                parity_candidate_membership_hash(selection_hash, &member.market_id, ordinal)?;
            if candidate.ordinal != ordinal
                || candidate.market_id != member.market_id
                || candidate.membership_hash != expected_membership
            {
                return Err(determinism(format!(
                    "frozen candidate membership drifted for parity run {} subject {:?}",
                    parity_run.run_id, subject.subject_id
                )));
            }
        }
        Ok((selection_id, decision_at))
    }

    async fn candidate_subjects(
        &self,
        run: &FeatureParityRunInfo,
    ) -> QuantResult<CandidateSubjects> {
        match run.kind {
            FeatureParityRunKind::Sampled => {
                let report_id = run.report_id.as_ref().ok_or_else(|| {
                    determinism("sampled parity run has no bound recommendation report".to_owned())
                })?;
                let report = self
                    .deps
                    .reports
                    .find_by_id(report_id)
                    .await?
                    .ok_or_else(|| StorageError::not_found("recommendation_report", report_id))?;
                validate_report_subject(&report, run)?;
                let route_runs = self
                    .deps
                    .reports
                    .find_route_runs(&[report.report_run_id])
                    .await?;
                let mut model_runs = Vec::new();
                for route_run in route_runs {
                    let Some(model_run_id) = route_run.model_run_id.as_ref() else {
                        continue;
                    };
                    let model_run = self
                        .deps
                        .model_runs
                        .find_by_id(model_run_id)
                        .await?
                        .ok_or_else(|| StorageError::not_found("quant_model_run", model_run_id))?;
                    validate_route_run_binding(&report, &route_run, &model_run)?;
                    model_runs.push(model_run);
                }
                Ok(CandidateSubjects {
                    model_runs,
                    reports: vec![report],
                })
            }
            FeatureParityRunKind::Full => {
                let mut reports = self
                    .deps
                    .reports
                    .list_committed_between(run.window_start, run.window_end)
                    .await?;
                for report in &reports {
                    validate_report_subject(report, run)?;
                }
                let mut model_runs = self
                    .deps
                    .model_runs
                    .list_succeeded_live_between(run.window_start, run.window_end)
                    .await?;
                model_runs.sort_by_key(|row| (row.window_start, row.model_run_id.to_string()));
                reports.sort_by_key(|report| {
                    (
                        report.decision_at,
                        report.recommendation_report_id.to_string(),
                    )
                });
                Ok(CandidateSubjects {
                    model_runs,
                    reports,
                })
            }
        }
    }

    async fn candidates_for_runs(
        &self,
        rows: Vec<ModelRunInfo>,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        let mut candidates = Vec::new();
        for row in rows {
            let mut markets = self.candidate_markets(&row).await?;
            markets.sort_by(|left, right| left.as_str().cmp(right.as_str()));
            markets.dedup();
            if markets.is_empty() {
                return Err(determinism(format!(
                    "serving run {} has no market rows to replay",
                    row.model_run_id
                )));
            }
            candidates.extend(markets.into_iter().map(|market_id| FeatureParityCandidate {
                sampling_key: format!("{}/{}", row.model_run_id, market_id),
                subject: FeatureParitySubject::ModelRun(row.model_run_id),
                market_id: Some(market_id),
                decision_at: row.window_start,
                input_witness: FeatureParityInputWitness::SelectionOnly,
            }));
        }
        Ok(candidates)
    }

    async fn candidates_for_reports(
        &self,
        reports: Vec<RecommendationReportInfo>,
        parity_run: &FeatureParityRunInfo,
    ) -> QuantResult<Vec<FeatureParityCandidate>> {
        let mut candidates = Vec::new();
        for report in reports {
            validate_report_subject(&report, parity_run)?;
            let members = self
                .deps
                .selections
                .list_members(&report.market_selection_id)
                .await?;
            let subject =
                FeatureParitySubject::RecommendationReport(report.recommendation_report_id);
            if members.is_empty() {
                candidates.push(FeatureParityCandidate {
                    sampling_key: format!("report/{}/selection", report.recommendation_report_id),
                    subject,
                    market_id: None,
                    decision_at: report.decision_at,
                    input_witness: FeatureParityInputWitness::SelectionOnly,
                });
                continue;
            }
            for member in members {
                candidates.push(FeatureParityCandidate {
                    sampling_key: format!(
                        "report/{}/{}",
                        report.recommendation_report_id, member.market_id
                    ),
                    subject: subject.clone(),
                    market_id: Some(member.market_id),
                    decision_at: report.decision_at,
                    input_witness: FeatureParityInputWitness::SelectionOnly,
                });
            }
        }
        Ok(candidates)
    }

    async fn candidate_markets(&self, run: &ModelRunInfo) -> QuantResult<Vec<MarketId>> {
        let selection_id = run.market_selection_id.as_ref().ok_or_else(|| {
            determinism(format!(
                "serving run {} has no selection for parity enumeration",
                run.model_run_id
            ))
        })?;
        Ok(self
            .deps
            .selections
            .list_members(selection_id)
            .await?
            .into_iter()
            .map(|member| member.market_id)
            .collect())
    }

    async fn load_replay_evidence(
        &self,
        run_ids: &[ModelRunId],
        cancel: &CancellationToken,
    ) -> QuantResult<DurableReplayEvidence> {
        let completion_rows = self
            .deps
            .serving_evidence
            .completions_for_runs(run_ids)
            .await?;
        let input_rows = self
            .deps
            .serving_evidence
            .model_inputs_for_runs(run_ids)
            .await?;
        let prepare_cancel = cancel.clone();
        let (completion_by_run, online_inputs, feature_ids) = self
            .compute_boundary
            .run(cancel, move || {
                Self::require_active(&prepare_cancel)?;
                let completion_by_run = dedupe_completions(completion_rows)?
                    .into_iter()
                    .map(|row| (row.model_run_id, row))
                    .collect::<HashMap<_, _>>();
                let online_inputs = dedupe_model_input_rows(input_rows)?;
                let feature_ids = completion_by_run
                    .values()
                    .map(completion_vector_ids)
                    .collect::<QuantResult<Vec<_>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                Ok((completion_by_run, online_inputs, feature_ids))
            })
            .await?;
        let feature_rows = self
            .deps
            .serving_evidence
            .feature_cells_for_vectors(&feature_ids)
            .await?;
        let feature_infos = self.deps.feature_vectors.find_by_ids(&feature_ids).await?;
        let finish_cancel = cancel.clone();
        self.compute_boundary
            .run(cancel, move || {
                Self::require_active(&finish_cancel)?;
                let online_features = dedupe_feature_rows(feature_rows)?;
                Ok(DurableReplayEvidence {
                    completion_by_run,
                    inputs_by_run: group_model_inputs(online_inputs),
                    features_by_vector: group_feature_rows(online_features),
                    info_by_id: feature_infos
                        .into_iter()
                        .map(|info| (info.feature_vector_id, info))
                        .collect(),
                })
            })
            .await
    }

    async fn replay_candidate_groups(
        &self,
        candidates: &[FeatureParityCandidate],
        evidence: &DurableReplayEvidence,
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityReplayAttempt> {
        let mut attempt = FeatureParityReplayAttempt::default();
        for (run_id, run_candidates) in group_candidates_by_run(candidates) {
            let candidate = representative_candidate(&run_id.to_string(), &run_candidates)?;
            let run = self
                .deps
                .model_runs
                .find_by_id(&run_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_model_run", run_id))?;
            if run.window_start != candidate.decision_at {
                return Err(determinism(format!(
                    "candidate {} decision time changed from {} to {}",
                    run_id, candidate.decision_at, run.window_start
                )));
            }
            let Some(completion) = evidence.completion_by_run.get(&run_id) else {
                return Err(determinism(format!(
                    "qualified model {run_id} lost its committed serving completion"
                )));
            };
            let run_inputs = validate_run_completion(
                &run_id,
                candidate,
                completion,
                &evidence.inputs_by_run,
                &evidence.features_by_vector,
            )?;
            validate_replay_witnesses(&run, &run_candidates, run_inputs)?;
            let vector_ids = completion_vector_ids(completion)?;
            let run_features = vector_ids
                .iter()
                .map(|vector_id| {
                    evidence
                        .features_by_vector
                        .get(vector_id)
                        .cloned()
                        .map(|rows| (*vector_id, rows))
                        .ok_or_else(|| {
                            determinism(format!(
                                "serving completion for {run_id} references missing vector {vector_id}"
                            ))
                        })
                })
                .collect::<QuantResult<HashMap<_, _>>>()?;
            let run_infos = vector_ids
                .iter()
                .map(|vector_id| {
                    evidence
                        .info_by_id
                        .get(vector_id)
                        .cloned()
                        .map(|info| (*vector_id, info))
                        .ok_or_else(|| {
                            determinism(format!(
                                "serving completion for {run_id} has no Postgres vector {vector_id}"
                            ))
                        })
                })
                .collect::<QuantResult<HashMap<_, _>>>()?;
            let comparisons = Box::pin(self.replay_run(ReplayRunInput {
                candidate,
                run: &run,
                completion,
                online_inputs: run_inputs,
                online_features: &run_features,
                feature_infos: &run_infos,
                cancel,
            }))
            .await?;
            attempt.comparisons.extend(select_comparisons(
                &run_id.to_string(),
                &run_candidates,
                &comparisons,
            )?);
        }
        Ok(attempt)
    }

    async fn replay_report_candidate_groups(
        &self,
        candidates: &[FeatureParityCandidate],
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityReplayAttempt> {
        let mut attempt = FeatureParityReplayAttempt::default();
        for (report_id, report_candidates) in group_candidates_by_report(candidates) {
            let prepared = self
                .prepare_report_replay(&report_id, &report_candidates)
                .await?;
            let comparisons = self
                .compare_report_selection(
                    representative_candidate(&report_id.to_string(), &report_candidates)?,
                    &prepared,
                    cancel,
                )
                .await?;
            attempt.comparisons.extend(select_comparisons(
                &report_id.to_string(),
                &report_candidates,
                &comparisons,
            )?);
        }
        Ok(attempt)
    }

    async fn prepare_report_replay(
        &self,
        report_id: &RecommendationReportId,
        candidates: &[&FeatureParityCandidate],
    ) -> QuantResult<Box<PreparedReportReplay>> {
        let candidate = representative_candidate(&report_id.to_string(), candidates)?;
        let report = self
            .deps
            .reports
            .find_by_id(report_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_recommendation_report", report_id))?;
        let report_run = self.load_report_run(&report).await?;
        if report.decision_at != candidate.decision_at {
            return Err(determinism(format!(
                "report {} decision time changed from {} to {}",
                report_id, candidate.decision_at, report.decision_at
            )));
        }
        let selection = self
            .deps
            .selections
            .find_by_id(&report.market_selection_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("quant_market_selection", report.market_selection_id)
            })?;
        let members = self
            .deps
            .selections
            .list_members(&report.market_selection_id)
            .await?;
        let dq = self
            .deps
            .reports
            .find_data_quality_snapshot(report_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_report_data_quality_snapshot",
                    report.data_quality_snapshot_ref,
                )
            })?;
        validate_quality_evidence(&report, &members, &dq)?;
        let config_info = self
            .deps
            .runtime_configs
            .load_snapshot(&report.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    report.decision_policy_snapshot_id,
                )
            })?;
        let snapshot_hash = config_info.snapshot_hash;
        let route_runs = self
            .deps
            .reports
            .find_route_runs(&[report.report_run_id])
            .await?;
        let evidence =
            ReportBoundaryEvidence::try_new(&report, &report_run, &config_info, &dq, &route_runs)?;
        let vectors = self
            .deps
            .feature_vectors
            .find_by_ids(evidence.feature_ids())
            .await?;
        let expected_boundary = evidence
            .restore(
                &vectors,
                self.deps.exchange_history.as_ref(),
                self.deps.fact_read.as_ref(),
            )
            .await?;
        let selector_binding =
            ReportSelectorBinding::new(evidence.history(), evidence.universe_plan_hash());
        let config = config_info.snapshot;
        validate_report_selection_binding(&report, &selection)?;
        Ok(Box::new(PreparedReportReplay {
            report_id: *report_id,
            context: ReportReplayContext {
                decision_policy_snapshot_id: report.decision_policy_snapshot_id,
                snapshot_hash,
                config,
                boundary: expected_boundary,
                represented_routes: report.represented_routes_json,
                selector_binding,
                selection,
                members,
            },
        }))
    }

    async fn load_report_run(
        &self,
        report: &RecommendationReportInfo,
    ) -> QuantResult<ReportRunInfo> {
        let run = self
            .deps
            .report_runs
            .find_by_output_report(&report.recommendation_report_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    "quant_report_run.output_report_id",
                    report.recommendation_report_id,
                )
            })?;
        if run.decision_policy_snapshot_id.as_ref() != Some(&report.decision_policy_snapshot_id)
            || run.decision_at != Some(report.decision_at)
        {
            return Err(determinism(format!(
                "report {} is not bound to the exact successful report run",
                report.recommendation_report_id
            )));
        }
        Ok(run)
    }

    async fn compare_report_selection(
        &self,
        candidate: &FeatureParityCandidate,
        prepared: &PreparedReportReplay,
        cancel: &CancellationToken,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let context = prepared.context.clone();
        let report_id = prepared.report_id;
        let candidate = candidate.clone();
        let replay = Box::pin(self.materialize_report_selection(&context, cancel)).await?;
        let kernel_cancel = cancel.clone();
        self.compute_boundary
            .run(cancel, move || {
                Self::require_active(&kernel_cancel)?;
                selection_comparisons(
                    &candidate,
                    ComparisonSubject {
                        report: Some(&report_id),
                        model_run: None,
                        model_version: None,
                    },
                    &context.selection,
                    &context.members,
                    &replay,
                    &context.boundary,
                )
            })
            .await
    }

    async fn prepare_replay_run(
        &self,
        candidate: &FeatureParityCandidate,
        run: &ModelRunInfo,
        completion: &QuantServingEvidenceCompletionRow,
        online_inputs: &[QuantModelInputEventRow],
        online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
        feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
    ) -> QuantResult<ReplayRunContext> {
        let model_version_id = run.model_version_id.ok_or_else(|| {
            determinism(format!(
                "live run {} has no model version",
                run.model_run_id
            ))
        })?;
        let selection_id = run.market_selection_id.ok_or_else(|| {
            determinism(format!("live run {} has no selection", run.model_run_id))
        })?;
        let config_info = self
            .deps
            .runtime_configs
            .load_snapshot(&run.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found("decision_policy_snapshot", run.decision_policy_snapshot_id)
            })?;
        let snapshot_hash = config_info.snapshot_hash;
        let config = config_info.snapshot;
        let route = route_for_model(&config, model_version_id)?;
        let boundary =
            boundary_from_online(completion, online_inputs, online_features, feature_infos)?;
        if boundary.decision_at() != candidate.decision_at {
            return Err(determinism(format!(
                "serving evidence boundary {} does not match candidate {}",
                boundary.decision_at(),
                candidate.decision_at
            )));
        }
        let (report_id, represented_routes, history, report_selector) = if let Some(route_run) =
            self.deps
                .reports
                .find_model_route_run(&run.model_run_id)
                .await?
        {
            if route_run.route != route {
                return Err(determinism(format!(
                    "report Route {:?} differs from model {model_version_id} frozen Route {route:?}",
                    route_run.route
                )));
            }
            let report = self
                .deps
                .reports
                .find_by_report_run(&route_run.report_run_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found(
                        "quant_recommendation_report.report_run_id",
                        route_run.report_run_id,
                    )
                })?;
            let lineage = validate_route_run_binding(&report, &route_run, run)?;
            (
                Some(report.recommendation_report_id),
                Some(report.represented_routes_json),
                Some(lineage.history.clone()),
                Some(ReportSelectorBinding::new(
                    &lineage.history,
                    lineage.report_universe_plan_hash,
                )),
            )
        } else {
            (None, None, None, None)
        };

        let selection = self
            .deps
            .selections
            .find_by_id(&selection_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_market_selection", selection_id))?;
        if selection.decision_at != boundary.decision_at()
            || selection.decision_policy_snapshot_id != run.decision_policy_snapshot_id
        {
            return Err(determinism(format!(
                "selection {selection_id} is not bound to model run {} decision/config",
                run.model_run_id
            )));
        }
        let members = self.deps.selections.list_members(&selection_id).await?;
        let samples = replay_feature_population(
            &selection_id,
            &boundary,
            route,
            &members,
            online_features,
            feature_infos,
        )?;
        let finalized_execution_evidences =
            frozen_finalized_execution_evidences(&boundary, online_features, feature_infos)?;
        Ok(ReplayRunContext {
            model_version_id,
            decision_policy_snapshot_id: run.decision_policy_snapshot_id,
            snapshot_hash,
            config,
            boundary,
            report_id,
            history,
            represented_routes,
            report_selector,
            selection,
            members,
            samples,
            finalized_execution_evidences,
        })
    }

    fn replay_history_mode(
        builder_requires_history: bool,
        evidences: &HashMap<MarketId, FinalizedExecutionEvidence>,
    ) -> QuantResult<ReplayHistoryMode> {
        let mut mode = None;
        for evidence in evidences.values() {
            let current = match evidence {
                FinalizedExecutionEvidence::NotRequired if !builder_requires_history => {
                    ReplayHistoryMode::NotRequired
                }
                FinalizedExecutionEvidence::Runtime {
                    history_enabled: false,
                    accepted_through_block: None,
                    accepted_through_at: None,
                } if builder_requires_history => ReplayHistoryMode::RuntimeDisabled,
                FinalizedExecutionEvidence::Runtime {
                    history_enabled: true,
                    accepted_through_block: Some(accepted_through_block),
                    accepted_through_at: Some(accepted_through_at),
                } if builder_requires_history => ReplayHistoryMode::RuntimeEnabled {
                    accepted_through_block: *accepted_through_block,
                    accepted_through_at: *accepted_through_at,
                },
                FinalizedExecutionEvidence::Materialized { available_by }
                    if builder_requires_history =>
                {
                    ReplayHistoryMode::Materialized {
                        available_by: *available_by,
                    }
                }
                _ => {
                    return Err(determinism(
                        "feature parity builder and finalized-history evidence disagree".to_owned(),
                    ));
                }
            };
            if mode.as_ref().is_some_and(|mode| mode != &current) {
                return Err(determinism(
                    "feature parity run contains mixed finalized-history evidence".to_owned(),
                ));
            }
            mode = Some(current);
        }
        mode.ok_or_else(|| determinism("feature parity run has no history evidence".to_owned()))
    }

    async fn replay_history_window(
        &self,
        context: &ReplayRunContext,
        builder_requires_history: bool,
    ) -> QuantResult<ReplayHistoryWindow> {
        match Self::replay_history_mode(
            builder_requires_history,
            &context.finalized_execution_evidences,
        )? {
            ReplayHistoryMode::NotRequired | ReplayHistoryMode::RuntimeDisabled => {
                Ok(ReplayHistoryWindow {
                    chunks: Vec::new(),
                    load_execution_history: false,
                    materialized_available_by: None,
                })
            }
            ReplayHistoryMode::RuntimeEnabled {
                accepted_through_block,
                accepted_through_at,
            } => {
                let head = self
                    .replay_history_head(context, accepted_through_block, accepted_through_at)
                    .await?;
                Ok(ReplayHistoryWindow {
                    chunks: head.chunks,
                    load_execution_history: true,
                    materialized_available_by: None,
                })
            }
            ReplayHistoryMode::Materialized { available_by } => {
                let Some(RouteHistoryLineage::Materialized {
                    available_by: bound_available_by,
                    chunks,
                }) = context.history.as_ref()
                else {
                    return Err(determinism(
                        "materialized feature parity has no exact Route history lineage".to_owned(),
                    ));
                };
                if *bound_available_by != available_by || chunks.is_empty() {
                    return Err(determinism(
                        "materialized feature parity Route lineage differs from frozen evidence"
                            .to_owned(),
                    ));
                }
                self.deps
                    .fact_read
                    .validate_execution_history_chunks(chunks.clone())
                    .await?;
                Ok(ReplayHistoryWindow {
                    chunks: chunks.clone(),
                    load_execution_history: true,
                    materialized_available_by: Some(available_by),
                })
            }
        }
    }

    async fn replay_history_head(
        &self,
        context: &ReplayRunContext,
        expected_block: u64,
        expected_at: DateTime<Utc>,
    ) -> QuantResult<HistoryServingHeadSeal> {
        let decision_at = context.boundary.decision_at();
        let head = if let Some(history) = context.history.as_ref() {
            let RouteHistoryLineage::Runtime {
                serving_head_seal_id,
                serving_head_seal_hash,
            } = history
            else {
                return Err(determinism(
                    "runtime feature parity is bound to materialized Route history".to_owned(),
                ));
            };
            let head = self
                .deps
                .exchange_history
                .validate_serving_head(*serving_head_seal_id, *serving_head_seal_hash)
                .await?;
            if head.seal.serving_head_seal_id != *serving_head_seal_id
                || head.seal.seal_hash != *serving_head_seal_hash
            {
                return Err(determinism(
                    "parity repository returned a different Route lineage seal".to_owned(),
                ));
            }
            head
        } else {
            let head = self
                .deps
                .exchange_history
                .serving_head_at(ExchangeHistoryFrontier::Activation, decision_at)
                .await?
                .ok_or_else(|| {
                    determinism(
                        "parity replay has no serving head at its decision boundary".to_owned(),
                    )
                })?;
            self.deps
                .exchange_history
                .validate_serving_head(head.seal.serving_head_seal_id, head.seal.seal_hash)
                .await?
        };
        if head.seal.frontier != ExchangeHistoryFrontier::Activation
            || head.seal.created_at > decision_at
        {
            return Err(determinism(format!(
                "parity serving head {} is not decision-visible Activation history",
                head.seal.serving_head_seal_id
            )));
        }
        let accepted_block = u64::try_from(head.seal.accepted_through_block).map_err(|error| {
            determinism(format!(
                "parity serving head has invalid accepted block: {error}"
            ))
        })?;
        if accepted_block != expected_block || head.seal.effective_through_at != expected_at {
            return Err(determinism(format!(
                "parity finalized-execution evidence disagrees with serving head {}",
                head.seal.serving_head_seal_id
            )));
        }
        Ok(head)
    }

    async fn materialize_run_replay(
        &self,
        subject: &str,
        context: &ReplayRunContext,
        cancel: &CancellationToken,
    ) -> QuantResult<MaterializedRunReplay> {
        let config = &context.config;
        let boundary = &context.boundary;
        let selection_replay = Box::pin(self.materialize_selection_replay(context, cancel)).await?;
        let samples = context.samples.clone();
        if samples.is_empty() {
            return Err(determinism(format!(
                "committed serving feature population is empty for {subject}"
            )));
        }
        let prefetch_lookback = Duration::from_secs(
            config
                .profile_artifacts
                .features
                .definition
                .max_lookback_secs(),
        );
        let max_book_staleness = Duration::from_millis(
            config
                .profile_artifacts
                .research_method
                .training
                .max_book_staleness_ms,
        );
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.catalog),
            Arc::clone(&self.deps.clob_market_info),
            Arc::clone(&self.deps.linkages),
            Arc::clone(&self.deps.calibration_artifacts),
            max_book_staleness,
        );
        let window_end = boundary
            .decision_at()
            .checked_add_signed(ChronoDuration::milliseconds(1))
            .ok_or_else(|| determinism("parity window end is outside chrono range".to_owned()))?;
        let ReplayHistoryWindow {
            chunks,
            load_execution_history,
            materialized_available_by,
        } = self
            .replay_history_window(context, selection_replay.builder.needs_execution_history())
            .await?;
        let prefetched = loader
            .prefetch(&WindowSpec {
                window_start: boundary.decision_at(),
                window_end,
                available_by: window_end,
                samples: samples.clone(),
                lookback: prefetch_lookback,
                knowledge_lag: boundary.knowledge_lag(),
                max_horizon_secs: 0,
                domain: config.profile_artifacts.domain.definition.clone(),
                feature_contract: selection_replay.replay_config.feature_contract,
                execution_history_chunks: chunks,
                requires_execution_history: load_execution_history,
            })
            .await?;
        Self::require_active(cancel)?;
        let finalized_execution_evidences = context.finalized_execution_evidences.clone();
        let boundary = boundary.clone();
        let subject = subject.to_owned();
        let kernel_cancel = cancel.clone();
        let runtime = Handle::current();
        let (selection_replay, cross) = self
            .compute_boundary
            .run(cancel, move || {
                Self::require_active(&kernel_cancel)?;
                let window = historical_window_from_prefetched(prefetched, max_book_staleness)?;
                let execution_source = materialized_available_by.map_or(
                    ReplayExecutionSource::FrozenRuntime(&finalized_execution_evidences),
                    |available_by| ReplayExecutionSource::Materialized { available_by },
                );
                let cross = runtime
                    .block_on(materialize_cross_section(
                        &selection_replay.builder,
                        ReplayFactorMode::FactorNative {
                            engine: &selection_replay.factor_engine,
                        },
                        &selection_replay.replay_config,
                        &CrossSectionRequest {
                            pit: &window.pit,
                            prefetched: &window.prefetched,
                            finalized_execution_evidence: execution_source,
                            boundary: &boundary,
                            group: &samples,
                            required_features: &selection_replay.required_features,
                            category_scope: selection_replay
                                .serving
                                .active_version()
                                .category_scope,
                        },
                    ))?
                    .ok_or_else(|| {
                        determinism(format!(
                            "durable replay resolved no catalog rows for {subject}"
                        ))
                    })?;
                Ok((selection_replay, cross))
            })
            .await?;
        Ok(MaterializedRunReplay {
            builder: selection_replay.builder,
            factor_engine: selection_replay.factor_engine,
            bias_table_hash: selection_replay.bias_table_hash,
            selection: selection_replay.selection,
            cross_section: cross,
            serving: selection_replay.serving,
        })
    }

    async fn materialize_selection_replay(
        &self,
        context: &ReplayRunContext,
        cancel: &CancellationToken,
    ) -> QuantResult<MaterializedSelectionReplay> {
        let config = &context.config;
        let boundary = &context.boundary;
        let bias_table = resolve_frozen_bias_table(
            self.deps.calibration_artifacts.as_ref(),
            &config.profile_artifacts.scoring.definition,
        )
        .await?;
        let serving = self.replay_serving(context).await?;
        let selector = match (&context.report_selector, &context.represented_routes) {
            (Some(binding), Some(represented_routes)) => {
                self.report_selector_contract(
                    ModelServingGenerationRequest {
                        decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                        snapshot_hash: context.snapshot_hash,
                        snapshot: config,
                    },
                    represented_routes,
                    binding,
                )
                .await?
            }
            (None, None) => ReplaySelectionContract {
                model_requirements: serving.model_requirements(),
                route_availability: None,
            },
            _ => {
                return Err(determinism(
                    "model replay has incomplete report selector lineage".to_owned(),
                ));
            }
        };
        let durable_pit = Arc::new(DurablePitSource::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.catalog),
            Arc::clone(&self.deps.clob_market_info),
        ));
        let candidate_provider = MarketCandidateProvider::new(
            durable_pit,
            Arc::clone(&self.deps.linkages),
            Arc::clone(&self.deps.fact_read),
        );
        let candidate_batch = candidate_provider
            .candidates(boundary, &config.profile_artifacts.domain.definition)
            .await?;
        Self::require_active(cancel)?;
        let config = config.clone();
        let boundary = boundary.clone();
        let decision_policy_snapshot_id = context.decision_policy_snapshot_id;
        let kernel_cancel = cancel.clone();
        let runtime = Handle::current();
        self.compute_boundary
            .run(cancel, move || {
                Self::require_active(&kernel_cancel)?;
                let feature_contract = serving
                    .active_version()
                    .profile_ref
                    .resolve_builtin_research_profile()
                    .map_err(determinism)?
                    .spec
                    .feature_contract;
                let replay_config = ReplayConfig {
                    features: config.profile_artifacts.features.definition.clone(),
                    factors: config.profile_artifacts.scoring.definition.clone(),
                    domain: config.profile_artifacts.domain.definition.clone(),
                    data_quality: config.recommendation.data_quality.clone(),
                    liquidity_cap_usd: Usd::new(
                        config
                            .execution_risk
                            .portfolio
                            .exposure_limits
                            .max_single_recommendation_usd
                            .value,
                    ),
                    feature_contract,
                    bias_table: bias_table.as_ref().map(Arc::clone),
                };
                let builder = ConfiguredFeatureBuilder::new_for_contract(
                    &config.profile_artifacts.features.definition,
                    &config.profile_artifacts.domain.definition,
                    feature_contract,
                )?;
                let factor_engine = FactorEngine::for_model_scope(
                    &config.profile_artifacts.scoring.definition,
                    &config.profile_artifacts.features.definition,
                    &config.profile_artifacts.domain.definition,
                    feature_contract,
                    serving.active_version().category_scope,
                    bias_table.clone(),
                );
                let bias_table_hash = bias_table.as_ref().map(|table| table.content_hash);
                let feature_schema_hash = ResearchHasher::feature_schema(builder.schema())?;
                let factor_plane = factor_engine.serving_plane()?;
                verify_replay_contract(
                    serving.active_version(),
                    feature_schema_hash,
                    factor_plane,
                    bias_table_hash,
                )?;
                let required_features = serving.model_requirements().union_all();
                let selection =
                    runtime.block_on(ConfiguredMarketSelector::new().build_snapshot(
                        MarketSelectionBuildRequest {
                            decision_at: boundary.decision_at(),
                            decision_policy_snapshot_id,
                            selection: config.recommendation.selection.clone(),
                            data_quality: config.recommendation.data_quality.clone(),
                            features: config.profile_artifacts.features.definition.clone(),
                            model_requirements: selector.model_requirements,
                            knowledge_lag_secs: boundary.knowledge_lag_secs(),
                            route_availability: selector.route_availability,
                        },
                        candidate_batch.candidates,
                    ))?;
                Ok(MaterializedSelectionReplay {
                    builder,
                    factor_engine,
                    bias_table_hash,
                    selection,
                    replay_config,
                    required_features,
                    serving,
                })
            })
            .await
    }

    async fn materialize_report_selection(
        &self,
        context: &ReportReplayContext,
        cancel: &CancellationToken,
    ) -> QuantResult<MarketSelectionSnapshot> {
        let selector = self
            .report_selector_contract(
                ModelServingGenerationRequest {
                    decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                    snapshot_hash: context.snapshot_hash,
                    snapshot: &context.config,
                },
                &context.represented_routes,
                &context.selector_binding,
            )
            .await?;
        let durable_pit = Arc::new(DurablePitSource::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.catalog),
            Arc::clone(&self.deps.clob_market_info),
        ));
        let candidate_provider = MarketCandidateProvider::new(
            durable_pit,
            Arc::clone(&self.deps.linkages),
            Arc::clone(&self.deps.fact_read),
        );
        let candidates = candidate_provider
            .candidates(
                &context.boundary,
                &context.config.profile_artifacts.domain.definition,
            )
            .await?;
        Self::require_active(cancel)?;
        let context = context.clone();
        let kernel_cancel = cancel.clone();
        let runtime = Handle::current();
        self.compute_boundary
            .run(cancel, move || {
                Self::require_active(&kernel_cancel)?;
                runtime.block_on(ConfiguredMarketSelector::new().build_snapshot(
                    MarketSelectionBuildRequest {
                        decision_at: context.boundary.decision_at(),
                        decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                        selection: context.config.recommendation.selection.clone(),
                        data_quality: context.config.recommendation.data_quality.clone(),
                        features: context.config.profile_artifacts.features.definition.clone(),
                        model_requirements: selector.model_requirements,
                        knowledge_lag_secs: context.boundary.knowledge_lag_secs(),
                        route_availability: selector.route_availability,
                    },
                    candidates.candidates,
                ))
            })
            .await
    }

    async fn report_selector_contract(
        &self,
        request: ModelServingGenerationRequest<'_>,
        represented_routes: &RepresentedRouteSet,
        binding: &ReportSelectorBinding,
    ) -> QuantResult<ReplaySelectionContract> {
        if let ReportSelectorBinding::Runtime {
            serving_head_seal_id,
            serving_head_seal_hash,
            ..
        } = binding
        {
            let serving_routes = self
                .deps
                .serving_generations
                .resolve_available_routes(request)
                .await?;
            for serving in &serving_routes {
                serving.validate_active()?;
            }
            let contract = ReportUniverseContract::try_new(
                request.decision_policy_snapshot_id,
                request.snapshot_hash,
                serving_routes
                    .iter()
                    .map(ReportUniverseRoute::from)
                    .collect(),
                *serving_head_seal_id,
                *serving_head_seal_hash,
            )?;
            if represented_routes
                .routes
                .iter()
                .any(|route| !contract.availability.active_routes.contains(route))
            {
                return Err(determinism(
                    "report represented Routes escaped its frozen selector universe".to_owned(),
                ));
            }
            return binding.verify_universe(contract);
        }
        let mut model_requirements = ModelFeatureRequirements::default();
        let serving_routes = self
            .deps
            .serving_generations
            .resolve_routes(request, represented_routes)
            .await?;
        for serving in serving_routes {
            model_requirements.merge(serving.model_requirements());
        }
        Ok(ReplaySelectionContract {
            model_requirements,
            route_availability: None,
        })
    }

    async fn replay_serving(
        &self,
        context: &ReplayRunContext,
    ) -> QuantResult<ModelServingRouteSnapshot> {
        let serving = self
            .deps
            .serving_generations
            .resolve_route(
                ModelServingGenerationRequest {
                    decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                    snapshot_hash: context.snapshot_hash,
                    snapshot: &context.config,
                },
                route_for_model(&context.config, context.model_version_id)?,
            )
            .await?;
        if serving.champion_model_version_id() != context.model_version_id {
            return Err(determinism(format!(
                "frozen exact route model {} differs from serving subject model {}",
                serving.champion_model_version_id(),
                context.model_version_id,
            )));
        }
        Ok(serving)
    }

    async fn replay_run(
        &self,
        input: ReplayRunInput<'_>,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let ReplayRunInput {
            candidate,
            run,
            completion,
            online_inputs,
            online_features,
            feature_infos,
            cancel,
        } = input;
        let context = self
            .prepare_replay_run(
                candidate,
                run,
                completion,
                online_inputs,
                online_features,
                feature_infos,
            )
            .await?;
        let replay = Box::pin(self.materialize_run_replay(
            &format!("serving run {}", run.model_run_id),
            &context,
            cancel,
        ))
        .await?;

        let kernel_candidate = candidate.clone();
        let kernel_run = run.clone();
        let kernel_context = context.clone();
        let kernel_inputs = online_inputs.to_vec();
        let kernel_features = online_features.clone();
        let kernel_infos = feature_infos.clone();
        let kernel_cancel = cancel.clone();
        let (replay, comparisons, admission_matches, route_vector_binding) = self
            .compute_boundary
            .run(cancel, move || {
                Self::require_active(&kernel_cancel)?;
                let comparison_subject = ComparisonSubject {
                    report: kernel_context.report_id.as_ref(),
                    model_run: Some(&kernel_run.model_run_id),
                    model_version: Some(&kernel_context.model_version_id),
                };
                let model_vector_binding = vector_binding(&kernel_inputs)?;
                let all_vector_binding = feature_vector_binding(&kernel_features)?;
                let route_vector_binding = online_route_binding(
                    &all_vector_binding,
                    &kernel_features,
                    &kernel_context.members,
                    replay.serving.route(),
                )?;
                validate_input_population(&model_vector_binding, &route_vector_binding)?;
                let replay_by_market = replay.cross_section.replay_vectors_by_market();
                let mut comparisons = selection_comparisons(
                    &kernel_candidate,
                    comparison_subject,
                    &kernel_context.selection,
                    &kernel_context.members,
                    &replay.selection,
                    &kernel_context.boundary,
                )?;
                comparisons.extend(snapshot_and_feature_comparisons(
                    &kernel_candidate,
                    comparison_subject,
                    FeatureComparisonInputs {
                        online_features: &kernel_features,
                        feature_infos: &kernel_infos,
                        replay_by_market: &replay_by_market,
                        replay_captures: &replay.cross_section.captures,
                        vector_binding: &all_vector_binding,
                        boundary: &kernel_context.boundary,
                        decision_policy_snapshot_id: &kernel_run.decision_policy_snapshot_id,
                        schema: replay.builder.schema(),
                    },
                )?);
                let admission_matches = route_admission_matches(
                    &route_vector_binding,
                    &replay.cross_section,
                    replay.serving.route(),
                );
                comparisons.push(data_quality_comparison(
                    &kernel_candidate,
                    comparison_subject,
                    &kernel_features,
                    &replay_by_market,
                    &kernel_context.boundary,
                )?);
                Ok((replay, comparisons, admission_matches, route_vector_binding))
            })
            .await?;
        if !admission_matches {
            return Ok(comparisons);
        }

        let runtime = Handle::current();
        let runtime_run = run.clone();
        let runtime_context = context.clone();
        let runtime_inputs = online_inputs.to_vec();
        let runtime_cancel = cancel.clone();
        let replay_outputs = self
            .compute_boundary
            .run(cancel, move || {
                Self::require_active(&runtime_cancel)?;
                runtime.block_on(Self::replay_model_routes(ModelRouteReplayRequest {
                    run: &runtime_run,
                    config: &runtime_context.config,
                    boundary: &runtime_context.boundary,
                    online_inputs: &runtime_inputs,
                    markets: &replay.cross_section.markets,
                    vectors: &replay.cross_section.vectors,
                    vector_binding: &route_vector_binding,
                    factor_engine: &replay.factor_engine,
                    bias_table_hash: replay.bias_table_hash,
                    serving: &replay.serving,
                    cancel: &runtime_cancel,
                }))
            })
            .await?;
        let online_factors = self
            .deps
            .factors
            .list_values_for_run(&run.model_run_id)
            .await?
            .into_boxed_slice();
        let final_candidate = candidate.clone();
        let final_run = run.clone();
        let final_context = context;
        let final_inputs = online_inputs.to_vec();
        let final_cancel = cancel.clone();
        Box::pin(self.compute_boundary.run(cancel, move || {
            Self::require_active(&final_cancel)?;
            Self::finish_run_comparisons(FinalRunComparisonInput {
                candidate: final_candidate,
                run: final_run,
                context: final_context,
                online_inputs: final_inputs,
                replay: replay_outputs,
                online_factors,
                comparisons,
            })
        }))
        .await
    }

    fn finish_run_comparisons(
        input: FinalRunComparisonInput,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let FinalRunComparisonInput {
            candidate,
            run,
            context,
            online_inputs,
            replay,
            online_factors,
            mut comparisons,
        } = input;
        comparisons.extend(model_input_comparisons(
            &candidate,
            &run.model_run_id,
            context.report_id.as_ref(),
            &online_inputs,
            &replay.input_rows,
        )?);
        comparisons.extend(Self::factor_comparisons(
            &candidate,
            &run,
            context.report_id.as_ref(),
            &replay.factor_outcomes,
            &context.boundary,
            online_factors,
        )?);
        comparisons.push(prediction_comparison(
            &candidate,
            &run,
            context.report_id,
            &replay.runtime_output,
            &context.boundary,
        )?);
        Ok(comparisons)
    }

    async fn replay_model_routes(
        request: ModelRouteReplayRequest<'_>,
    ) -> QuantResult<ReplayedModelOutput> {
        let feature_contract = request
            .serving
            .active_version()
            .profile_ref
            .resolve_builtin_research_profile()
            .map_err(determinism)?
            .spec
            .feature_contract;
        let feature_schema_hash = ResearchHasher::feature_schema(&ExecutableFeatureSchema::build(
            &request.config.profile_artifacts.features.definition,
            feature_contract,
        )?)?;
        let factor_plane = request.factor_engine.serving_plane()?;
        let market_by_id = request
            .markets
            .iter()
            .map(|market| (market.market_id.clone(), market))
            .collect::<HashMap<_, _>>();
        let vector_by_id = request
            .vectors
            .iter()
            .map(|vector| (vector.market_id.clone(), vector))
            .collect::<HashMap<_, _>>();
        let version_id = request.serving.champion_model_version_id();
        for row in request.online_inputs {
            Self::require_active(request.cancel)?;
            if row.model_version_id != version_id {
                return Err(determinism(format!(
                    "online input model {} differs from pinned exact route model {version_id}",
                    row.model_version_id
                )));
            }
        }
        let market_ids = request.vector_binding.keys().cloned().collect();
        verify_replay_contract(
            request.serving.active_version(),
            feature_schema_hash,
            factor_plane,
            request.bias_table_hash,
        )?;
        let runtime = request.serving.active_runtime().runtime();
        let route = resolve_route_inputs(
            market_ids,
            &market_by_id,
            &vector_by_id,
            request.vector_binding,
        )?;
        let outcomes = if runtime.model_family() == ModelFamily::WeightedFactor {
            let mut factor_config = request.config.profile_artifacts.scoring.definition.clone();
            factor_config.cross_section =
                runtime.factor_cross_section().cloned().ok_or_else(|| {
                    determinism(format!(
                        "weighted runtime {version_id} lacks cross-section policy"
                    ))
                })?;
            let references = runtime.frozen_reference_quantiles().ok_or_else(|| {
                determinism(format!(
                    "weighted runtime {version_id} lacks reference CDFs"
                ))
            })?;
            request.factor_engine.compute_batch_with_refs(
                &route.vectors,
                &factor_config,
                references,
            )?
        } else {
            Vec::new()
        };
        let input = build_runtime_input(
            runtime.as_ref(),
            &request.run.model_run_id,
            request.boundary.decision_at(),
            &route.markets,
            &route.vectors,
            &outcomes,
        );
        let output = runtime.infer_batch(input).await?;
        finish_replayed_model_output(
            &request,
            &route.vectors,
            output.candidates,
            output.input_audit,
            outcomes,
        )
    }

    fn factor_comparisons(
        candidate: &FeatureParityCandidate,
        run: &ModelRunInfo,
        report_id: Option<&RecommendationReportId>,
        replay_outcomes: &[MarketFactorOutcome],
        boundary: &DecisionBoundary,
        online: Box<[FactorValueInfo]>,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        if online.is_empty() && replay_outcomes.is_empty() {
            return Ok(vec![comparison(ComparisonInput {
                candidate,
                stage: FeatureParityStage::Factor,
                report_id: report_id.copied(),
                model_run_id: Some(run.model_run_id),
                model_version_id: run.model_version_id,
                market_id: None,
                feature_name: Some("classical_factor_bypass".to_owned()),
                online: canonical_evidence(&"classical_factor_bypass", None, boundary)?,
                replay: canonical_evidence(&"classical_factor_bypass", None, boundary)?,
                transform_hash: None,
                detail: FeatureParityDetailSource::FactorClassicalBypass,
            })]);
        }
        let mut online_projection = online
            .into_vec()
            .into_iter()
            .map(|row| FactorProjection::from_online(&row))
            .collect::<Vec<_>>();
        online_projection.sort();
        let mut replay_projection = replay_outcomes
            .iter()
            .filter(|outcome| outcome.eligibility.is_eligible())
            .flat_map(|outcome| {
                outcome
                    .factors
                    .iter()
                    .map(|factor| FactorProjection::from_replay(&outcome.market_id, &factor.value))
            })
            .collect::<Vec<_>>();
        replay_projection.sort();
        Ok(vec![comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::Factor,
            report_id: report_id.copied(),
            model_run_id: Some(run.model_run_id),
            model_version_id: run.model_version_id,
            market_id: None,
            feature_name: None,
            online: canonical_evidence(&online_projection, None, boundary)?,
            replay: canonical_evidence(&replay_projection, None, boundary)?,
            transform_hash: None,
            detail: FeatureParityDetailSource::FactorCounts {
                online_count: count(online_projection.len(), "factor online count")?,
                replay_count: count(replay_projection.len(), "factor replay count")?,
            },
        })])
    }
}

fn route_for_model(
    config: &DecisionPolicySnapshot,
    model_version_id: ModelVersionId,
) -> QuantResult<BuyModelRoute> {
    let mut routes = config
        .model_routing
        .model
        .buy_routes
        .iter()
        .filter_map(|(route, binding)| {
            (binding.champion.model_version_id == model_version_id).then_some(*route)
        });
    let route = routes.next().ok_or_else(|| {
        determinism(format!(
            "model {model_version_id} is not an active Route champion in the frozen policy"
        ))
    })?;
    if routes.next().is_some() {
        return Err(determinism(format!(
            "model {model_version_id} is bound as champion for more than one Route"
        )));
    }
    Ok(route)
}

fn resolve_route_inputs(
    market_ids: BTreeSet<MarketId>,
    market_by_id: &HashMap<MarketId, &SelectedMarket>,
    vector_by_id: &HashMap<MarketId, &FeatureVector>,
    vector_binding: &HashMap<MarketId, FeatureVectorId>,
) -> QuantResult<ModelRouteInputs> {
    let mut markets = Vec::with_capacity(market_ids.len());
    let mut vectors = Vec::with_capacity(market_ids.len());
    for market_id in market_ids {
        markets.push(
            (*market_by_id.get(&market_id).ok_or_else(|| {
                determinism(format!("replay has no selected market {market_id}"))
            })?)
            .clone(),
        );
        vectors.push(
            (*vector_by_id
                .get(&market_id)
                .ok_or_else(|| determinism(format!("replay has no feature vector {market_id}")))?)
            .clone(),
        );
        if !vector_binding.contains_key(&market_id) {
            return Err(determinism(format!(
                "online input has no vector binding for {market_id}"
            )));
        }
    }
    Ok(ModelRouteInputs { markets, vectors })
}

fn finish_replayed_model_output(
    request: &ModelRouteReplayRequest<'_>,
    vectors: &[FeatureVector],
    mut candidates: Vec<SignalCandidate>,
    input_audit: Vec<ModelInputAuditRow>,
    factor_outcomes: Vec<MarketFactorOutcome>,
) -> QuantResult<ReplayedModelOutput> {
    finalize_candidates(&mut candidates)?;
    let calibration_scores = candidates.iter().map(ModelCalibrationScore::from).collect();
    let runtime_output = ModelRuntimeOutput {
        calibration_scores,
        rank_scores: Vec::new(),
        candidates,
        runtime_metrics: ModelRuntimeMetrics {
            markets_scored: 0,
            candidates_emitted: 0,
            inference_duration_ms: 0,
        },
        input_audit,
    };
    let input_rows = project_replay_rows(
        &request.run.model_run_id,
        request.boundary,
        vectors,
        request.vector_binding,
        &runtime_output.input_audit,
    )?;
    Ok(ReplayedModelOutput {
        runtime_output,
        input_rows,
        factor_outcomes,
    })
}

fn project_replay_rows(
    model_run_id: &ModelRunId,
    boundary: &DecisionBoundary,
    vectors: &[FeatureVector],
    vector_binding: &HashMap<MarketId, FeatureVectorId>,
    audit: &[ModelInputAuditRow],
) -> QuantResult<Vec<QuantModelInputEventRow>> {
    let vector_ids = vectors
        .iter()
        .map(|vector| {
            vector_binding
                .get(&vector.market_id)
                .copied()
                .ok_or_else(|| {
                    determinism(format!("no serving vector id for {}", vector.market_id))
                })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    ModelInputEvidenceBatch::try_new(vectors, &vector_ids)?.project(
        model_run_id,
        boundary,
        audit,
        boundary.decision_at().timestamp_millis(),
    )
}

struct ReplayedModelOutput {
    runtime_output: ModelRuntimeOutput,
    input_rows: Vec<QuantModelInputEventRow>,
    factor_outcomes: Vec<MarketFactorOutcome>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct FactorProjection {
    market_id: String,
    factor_definition_id: String,
    state: String,
    raw_value: Option<String>,
    normalized_score: Option<String>,
    normalization_source: Option<String>,
    confidence: String,
}

impl FactorProjection {
    fn from_online(value: &FactorValueInfo) -> Self {
        Self {
            market_id: value.market_id.to_string(),
            factor_definition_id: value.factor_definition_id.to_string(),
            state: value.value_state.to_string(),
            raw_value: value.raw_value.map(|value| value.normalize().to_string()),
            normalized_score: value
                .normalized_score
                .map(|value| value.normalized().to_string()),
            normalization_source: value.normalization_source.map(|value| value.to_string()),
            confidence: value.confidence.normalized().to_string(),
        }
    }

    fn from_replay(market_id: &MarketId, value: &FactorValue) -> Self {
        Self {
            market_id: market_id.to_string(),
            factor_definition_id: value.definition_id.to_string(),
            state: value.value_state().to_string(),
            raw_value: value.raw_value.map(|raw| raw.normalize().to_string()),
            normalized_score: value
                .normalized_score()
                .map(|score| score.normalized().to_string()),
            normalization_source: value
                .normalization_source()
                .map(|source| source.to_string()),
            confidence: value.confidence.normalized().to_string(),
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct SelectionMemberProjection {
    market_id: String,
    event_id: String,
    category: String,
    primary_token_id: String,
    secondary_token_id: Option<String>,
    liquidity_usd: Option<String>,
    volume_24h_usd: Option<String>,
}

#[derive(Serialize)]
struct SelectionHeaderProjection<'a> {
    decision_at: DateTime<Utc>,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    selector_hash: &'a ContentHash,
    selector_evidence: &'a SelectorHashEvidence,
    market_count: usize,
    exclusion_summary: SelectionExclusionSummary,
    membership_hash: ContentHash,
}

struct SelectorBinding<'a> {
    provenance: &'static str,
    selection_id: MarketSelectionId,
    selector_hash: ContentHash,
    evidence: &'a SelectorHashEvidence,
}

impl SelectorBinding<'_> {
    fn validate(self) -> QuantResult<()> {
        if self.evidence.selector_hash == self.selector_hash {
            return Ok(());
        }
        Err(determinism(format!(
            "{} selection {} selector evidence root {} does not match selector_hash {}",
            self.provenance, self.selection_id, self.evidence.selector_hash, self.selector_hash
        )))
    }
}

#[derive(Serialize)]
struct SelectionProjection<'a> {
    header: &'a SelectionHeaderProjection<'a>,
    member: Option<&'a SelectionMemberProjection>,
}

fn selection_comparisons(
    candidate: &FeatureParityCandidate,
    subject: ComparisonSubject<'_>,
    online_selection: &MarketSelectionInfo,
    online_members: &[MarketSelectionMemberInfo],
    replay: &MarketSelectionSnapshot,
    boundary: &DecisionBoundary,
) -> QuantResult<Vec<FeatureParityComparison>> {
    SelectorBinding {
        provenance: "persisted",
        selection_id: online_selection.market_selection_id,
        selector_hash: online_selection.selector_hash,
        evidence: &online_selection.selector_evidence,
    }
    .validate()?;
    SelectorBinding {
        provenance: "replayed",
        selection_id: replay.market_selection_id,
        selector_hash: replay.selector_hash,
        evidence: &replay.selector_evidence,
    }
    .validate()?;
    let online_market_count = usize::try_from(online_selection.market_count).map_err(|error| {
        determinism(format!(
            "selection {} has invalid market_count {}: {error}",
            online_selection.market_selection_id, online_selection.market_count
        ))
    })?;
    let mut online_projection_members = online_members
        .iter()
        .map(|row| SelectionMemberProjection {
            market_id: row.market_id.to_string(),
            event_id: row.event_id.to_string(),
            category: row.category.to_string(),
            primary_token_id: row.primary_token_id.to_string(),
            secondary_token_id: row.secondary_token_id.as_ref().map(ToString::to_string),
            liquidity_usd: row
                .liquidity_usd
                .map(|value| value.normalized().to_string()),
            volume_24h_usd: row
                .volume_24h_usd
                .map(|value| value.normalized().to_string()),
        })
        .collect::<Vec<_>>();
    online_projection_members.sort();
    let mut replay_members = replay
        .included
        .iter()
        .map(|row| SelectionMemberProjection {
            market_id: row.market_id.to_string(),
            event_id: row.event_id.to_string(),
            category: row.category.to_string(),
            primary_token_id: row.primary_token_id.to_string(),
            secondary_token_id: row.secondary_token_id.as_ref().map(ToString::to_string),
            liquidity_usd: row
                .liquidity_usd
                .map(|value| value.normalized().to_string()),
            volume_24h_usd: row
                .volume_24h_usd
                .map(|value| value.normalized().to_string()),
        })
        .collect::<Vec<_>>();
    replay_members.sort();
    if online_market_count != online_projection_members.len() {
        return Err(determinism(format!(
            "selection {} market_count {} differs from its {} persisted members",
            online_selection.market_selection_id,
            online_market_count,
            online_projection_members.len()
        )));
    }
    let online_market_ids = online_projection_members
        .iter()
        .map(|member| member.market_id.as_str())
        .collect::<BTreeSet<_>>();
    let replay_market_ids = replay_members
        .iter()
        .map(|member| member.market_id.as_str())
        .collect::<BTreeSet<_>>();
    if online_market_ids.len() != online_projection_members.len()
        || replay_market_ids.len() != replay_members.len()
    {
        return Err(determinism(
            "selection parity encountered duplicate market membership".to_owned(),
        ));
    }
    let online_header = SelectionHeaderProjection {
        decision_at: online_selection.decision_at,
        decision_policy_snapshot_id: &online_selection.decision_policy_snapshot_id,
        selector_hash: &online_selection.selector_hash,
        selector_evidence: &online_selection.selector_evidence,
        market_count: online_market_count,
        exclusion_summary: online_selection.exclusion_summary,
        membership_hash: ResearchHasher::canonical(&online_projection_members)?,
    };
    let replay_header = SelectionHeaderProjection {
        decision_at: replay.decision_at,
        decision_policy_snapshot_id: &replay.decision_policy_snapshot_id,
        selector_hash: &replay.selector_hash,
        selector_evidence: &replay.selector_evidence,
        market_count: replay.included.len(),
        exclusion_summary: replay.exclusion_summary,
        membership_hash: ResearchHasher::canonical(&replay_members)?,
    };
    let online_count = count(online_projection_members.len(), "selection online count")?;
    let replay_count = count(replay_members.len(), "selection replay count")?;
    let replay_excluded_count = count(replay.excluded.len(), "selection replay excluded count")?;
    let replay_by_market = replay_members
        .iter()
        .map(|member| (member.market_id.as_str(), member))
        .collect::<BTreeMap<_, _>>();
    let project = |market_id: Option<MarketId>,
                   online_member: Option<&SelectionMemberProjection>,
                   replay_member: Option<&SelectionMemberProjection>|
     -> QuantResult<FeatureParityComparison> {
        let online_projection = SelectionProjection {
            header: &online_header,
            member: online_member,
        };
        let replay_projection = SelectionProjection {
            header: &replay_header,
            member: replay_member,
        };
        Ok(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::Selection,
            report_id: subject.report.copied(),
            model_run_id: subject.model_run.copied(),
            model_version_id: subject.model_version.copied(),
            market_id,
            feature_name: None,
            online: canonical_evidence(&online_projection, None, boundary)?,
            replay: canonical_evidence(&replay_projection, None, boundary)?,
            transform_hash: None,
            detail: FeatureParityDetailSource::Selection {
                online_count,
                replay_count,
                selector_evidence: Box::new(SelectorParityEvidence {
                    online: online_selection.selector_evidence,
                    replay: replay.selector_evidence,
                }),
                replay_excluded_count,
            },
        }))
    };
    if online_projection_members.is_empty() {
        return Ok(vec![project(None, None, None)?]);
    }
    online_projection_members
        .iter()
        .map(|member| {
            project(
                Some(MarketId::new(&member.market_id)),
                Some(member),
                replay_by_market.get(member.market_id.as_str()).copied(),
            )
        })
        .collect()
}

fn data_quality_comparison(
    candidate: &FeatureParityCandidate,
    subject: ComparisonSubject<'_>,
    online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    replay_by_market: &HashMap<MarketId, FeatureVector>,
    boundary: &DecisionBoundary,
) -> QuantResult<FeatureParityComparison> {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
    struct DataQualityProjection {
        market_id: String,
        data_quality: String,
        admitted: bool,
    }

    let mut online = Vec::with_capacity(online_features.len());
    for (vector_id, rows) in online_features {
        let first = rows
            .first()
            .ok_or_else(|| determinism(format!("feature evidence group {vector_id} is empty")))?;
        let data_quality = evidence_data_quality(vector_id, rows)?;
        online.push(DataQualityProjection {
            market_id: first.market_id.to_string(),
            data_quality: data_quality.to_string(),
            admitted: data_quality != DataQualityStatus::Insufficient,
        });
    }
    online.sort();

    let mut replay = replay_by_market
        .values()
        .map(|vector| DataQualityProjection {
            market_id: vector.market_id.to_string(),
            data_quality: vector.data_quality.to_string(),
            admitted: vector.data_quality != DataQualityStatus::Insufficient,
        })
        .collect::<Vec<_>>();
    replay.sort();

    Ok(comparison(ComparisonInput {
        candidate,
        stage: FeatureParityStage::DataQuality,
        report_id: subject.report.copied(),
        model_run_id: subject.model_run.copied(),
        model_version_id: subject.model_version.copied(),
        market_id: None,
        feature_name: None,
        online: canonical_evidence(&online, None, boundary)?,
        replay: canonical_evidence(&replay, None, boundary)?,
        transform_hash: None,
        detail: FeatureParityDetailSource::DataQuality {
            online_count: count(online.len(), "data-quality online count")?,
            replay_count: count(replay.len(), "data-quality replay count")?,
            online_admitted_count: count(
                online.iter().filter(|row| row.admitted).count(),
                "data-quality online admitted count",
            )?,
            replay_admitted_count: count(
                replay.iter().filter(|row| row.admitted).count(),
                "data-quality replay admitted count",
            )?,
        },
    }))
}

impl ReplayCrossSection {
    fn replay_vectors_by_market(&self) -> HashMap<MarketId, FeatureVector> {
        self.vectors
            .iter()
            .chain(&self.rejected_vectors)
            .cloned()
            .map(|vector| (vector.market_id.clone(), vector))
            .collect()
    }
}

fn evidence_data_quality(
    vector_id: &FeatureVectorId,
    rows: &[QuantFeatureEventRow],
) -> QuantResult<DataQualityStatus> {
    let first = rows
        .first()
        .ok_or_else(|| determinism(format!("feature evidence group {vector_id} is empty")))?;
    if rows
        .iter()
        .any(|row| row.feature_vector_id != *vector_id || row.data_quality != first.data_quality)
    {
        return Err(determinism(format!(
            "feature evidence group {vector_id} has inconsistent vector/data-quality evidence"
        )));
    }
    first
        .data_quality
        .parse::<DataQualityStatus>()
        .map_err(|error| {
            determinism(format!(
                "feature evidence group {vector_id} has invalid data-quality state `{}`: {error}",
                first.data_quality
            ))
        })
}

fn online_route_binding(
    all_vectors: &HashMap<MarketId, FeatureVectorId>,
    online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    members: &[MarketSelectionMemberInfo],
    route: BuyModelRoute,
) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
    let mut member_by_market = HashMap::with_capacity(members.len());
    for member in members {
        if member_by_market
            .insert(member.market_id.clone(), member)
            .is_some()
        {
            return Err(determinism(format!(
                "selection contains duplicate market {}",
                member.market_id
            )));
        }
    }

    let mut binding = HashMap::new();
    for (market_id, vector_id) in all_vectors {
        let member = member_by_market.get(market_id).ok_or_else(|| {
            determinism(format!(
                "feature vector {vector_id} market {market_id} is absent from the frozen selection"
            ))
        })?;
        if BuyModelRoute::from(member.category) != route {
            continue;
        }
        let rows = online_features
            .get(vector_id)
            .ok_or_else(|| determinism(format!("online feature group {vector_id} disappeared")))?;
        if evidence_data_quality(vector_id, rows)? != DataQualityStatus::Insufficient {
            binding.insert(market_id.clone(), *vector_id);
        }
    }
    if binding.is_empty() {
        return Err(determinism(format!(
            "successful serving run for route {} has no admitted feature-vector population",
            route.as_str()
        )));
    }
    Ok(binding)
}

fn validate_input_population(
    model_inputs: &HashMap<MarketId, FeatureVectorId>,
    route_vectors: &HashMap<MarketId, FeatureVectorId>,
) -> QuantResult<()> {
    for (market_id, vector_id) in model_inputs {
        match route_vectors.get(market_id) {
            Some(expected) if expected == vector_id => {}
            Some(expected) => {
                return Err(determinism(format!(
                    "model-input market {market_id} uses vector {vector_id}, but its admitted route vector is {expected}"
                )));
            }
            None => {
                return Err(determinism(format!(
                    "model-input market {market_id} is outside the admitted route population"
                )));
            }
        }
    }
    Ok(())
}

fn route_admission_matches(
    online: &HashMap<MarketId, FeatureVectorId>,
    replay: &ReplayCrossSection,
    route: BuyModelRoute,
) -> bool {
    online.keys().collect::<BTreeSet<_>>()
        == replay
            .markets
            .iter()
            .filter(|market| BuyModelRoute::from(market.category) == route)
            .map(|market| &market.market_id)
            .collect::<BTreeSet<_>>()
}

fn replay_feature_population(
    selection_id: &MarketSelectionId,
    boundary: &DecisionBoundary,
    route: BuyModelRoute,
    members: &[MarketSelectionMemberInfo],
    online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
) -> QuantResult<Vec<ReplaySample>> {
    let mut member_by_market = HashMap::with_capacity(members.len());
    for member in members {
        if member.market_selection_id != *selection_id
            || member_by_market
                .insert(member.market_id.clone(), member)
                .is_some()
        {
            return Err(determinism(format!(
                "selection {selection_id} contains a duplicate or foreign member {}",
                member.market_id
            )));
        }
    }
    let mut samples_by_market = HashMap::with_capacity(feature_infos.len());
    for (vector_id, persisted) in feature_infos {
        let persisted_boundary = &persisted.decision_boundary;
        persisted_boundary.validate()?;
        if persisted_boundary != boundary {
            return Err(determinism(format!(
                "Postgres feature vector {vector_id} boundary does not match serving evidence"
            )));
        }
        let rows = online_features.get(vector_id).ok_or_else(|| {
            determinism(format!("feature rows disappeared for vector {vector_id}"))
        })?;
        let first = rows.first().ok_or_else(|| {
            determinism(format!("feature row group is empty for vector {vector_id}"))
        })?;
        let token_id = first
            .token_id
            .as_ref()
            .ok_or_else(|| determinism(format!("serving vector {vector_id} has no token id")))?;
        let member = member_by_market.get(&first.market_id).ok_or_else(|| {
            determinism(format!(
                "feature vector {vector_id} market {} is absent from selection {selection_id}",
                first.market_id
            ))
        })?;
        if BuyModelRoute::from(member.category) != route {
            return Err(determinism(format!(
                "feature vector {vector_id} market {} is outside frozen Route {route:?}",
                first.market_id
            )));
        }
        let valid_binding = member.primary_token_id == *token_id
            && persisted.market_id == first.market_id
            && persisted.token_id.as_ref() == Some(token_id)
            && samples_by_market
                .insert(
                    first.market_id.clone(),
                    ReplaySample {
                        market_id: first.market_id.clone(),
                        token_id: token_id.clone(),
                    },
                )
                .is_none();
        if !valid_binding {
            return Err(determinism(format!(
                "feature vector {vector_id} has a duplicate or inconsistent selection/market/token binding"
            )));
        }
    }
    let expected_count = members
        .iter()
        .filter(|member| BuyModelRoute::from(member.category) == route)
        .count();
    if samples_by_market.len() != expected_count {
        return Err(determinism(format!(
            "selection {selection_id} Route {route:?} expects {expected_count} feature vectors, but its committed population has {}",
            samples_by_market.len()
        )));
    }
    members
        .iter()
        .filter(|member| BuyModelRoute::from(member.category) == route)
        .map(|member| {
            samples_by_market.remove(&member.market_id).ok_or_else(|| {
                determinism(format!(
                    "selection {selection_id} member {} has no committed feature vector",
                    member.market_id
                ))
            })
        })
        .collect()
}

fn frozen_finalized_execution_evidences(
    boundary: &DecisionBoundary,
    online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
) -> QuantResult<HashMap<MarketId, FinalizedExecutionEvidence>> {
    let mut by_market = HashMap::with_capacity(feature_infos.len());
    for (vector_id, info) in feature_infos {
        let rows = online_features.get(vector_id).ok_or_else(|| {
            determinism(format!(
                "feature rows disappeared while freezing finalized-execution evidence for vector {vector_id}"
            ))
        })?;
        let capture = persisted_capture(info, rows, boundary)?;
        if by_market
            .insert(
                info.market_id.clone(),
                capture.finalized_execution_evidence.clone(),
            )
            .is_some()
        {
            return Err(determinism(format!(
                "serving evidence contains duplicate finalized-execution source snapshots for market {}",
                info.market_id
            )));
        }
    }
    Ok(by_market)
}

fn snapshot_and_feature_comparisons(
    candidate: &FeatureParityCandidate,
    subject: ComparisonSubject<'_>,
    inputs: FeatureComparisonInputs<'_>,
) -> QuantResult<Vec<FeatureParityComparison>> {
    let mut comparisons = Vec::new();
    for (market_id, vector_id) in inputs.vector_binding {
        let online_rows = inputs
            .online_features
            .get(vector_id)
            .ok_or_else(|| determinism(format!("online feature group {vector_id} disappeared")))?;
        let online_info = inputs.feature_infos.get(vector_id).ok_or_else(|| {
            determinism(format!("Postgres feature vector {vector_id} is missing"))
        })?;
        let replay_vector = inputs
            .replay_by_market
            .get(market_id)
            .ok_or_else(|| determinism(format!("replay dropped serving market {market_id}")))?;
        let replay_token_id = replay_vector.token_id.as_ref().ok_or_else(|| {
            determinism(format!(
                "replay feature vector for market {market_id} has no token binding"
            ))
        })?;
        let online_capture = persisted_capture(online_info, online_rows, inputs.boundary)?;
        let replay_capture = inputs
            .replay_captures
            .get(&ReplayCaptureKey::new(market_id, replay_token_id))
            .ok_or_else(|| {
                determinism(format!(
                    "replay dropped decision capture for market/token \
                     {market_id}/{replay_token_id}"
                ))
            })?;
        let replay_capture_evidence = replay_capture.evidence();
        let replay_capture_hash = replay_capture.evidence_hash()?;
        let replay_info = replay_feature_info(
            vector_id,
            replay_vector,
            inputs.boundary,
            &replay_capture_evidence,
            &replay_capture_hash,
            online_info.created_at,
        )?;
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::Snapshot,
            report_id: subject.report.copied(),
            model_run_id: subject.model_run.copied(),
            model_version_id: subject.model_version.copied(),
            market_id: Some(market_id.clone()),
            feature_name: None,
            online: canonical_evidence(&online_capture.snapshot, None, inputs.boundary)?,
            replay: canonical_evidence(&replay_capture_evidence.snapshot, None, inputs.boundary)?,
            transform_hash: None,
            detail: FeatureParityDetailSource::Snapshot {
                feature_vector_id: *vector_id,
                online_catalog_change_id: online_capture.snapshot.catalog.market_change_id,
                replay_catalog_change_id: replay_capture_evidence.snapshot.catalog.market_change_id,
                online_book_ref: online_capture.snapshot.book_snapshot_ref.clone(),
                replay_book_ref: replay_capture_evidence.snapshot.book_snapshot_ref.clone(),
            },
        }));
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::Capture,
            report_id: subject.report.copied(),
            model_run_id: subject.model_run.copied(),
            model_version_id: subject.model_version.copied(),
            market_id: Some(market_id.clone()),
            feature_name: None,
            online: canonical_evidence(&online_capture, None, inputs.boundary)?,
            replay: canonical_evidence(&replay_capture_evidence, None, inputs.boundary)?,
            transform_hash: None,
            detail: FeatureParityDetailSource::Capture {
                feature_vector_id: *vector_id,
                online_capture_hash: online_info.decision_capture_hash,
                replay_capture_hash,
            },
        }));
        let replay_rows = feature_events(
            replay_vector,
            &replay_info,
            inputs.boundary,
            inputs.decision_policy_snapshot_id,
            inputs.schema,
            inputs.boundary.decision_at().timestamp_millis(),
        )?;
        let replay_by_name = replay_rows
            .into_iter()
            .map(|row| (row.feature_name.clone(), row))
            .collect::<HashMap<_, _>>();
        comparisons.extend(feature_cell_comparisons(
            candidate,
            subject,
            market_id,
            vector_id,
            online_rows,
            &replay_by_name,
        )?);
    }
    Ok(comparisons)
}

fn feature_cell_comparisons(
    candidate: &FeatureParityCandidate,
    subject: ComparisonSubject<'_>,
    market_id: &MarketId,
    vector_id: &FeatureVectorId,
    online_rows: &[QuantFeatureEventRow],
    replay_by_name: &HashMap<String, QuantFeatureEventRow>,
) -> QuantResult<Vec<FeatureParityComparison>> {
    let mut comparisons = Vec::with_capacity(online_rows.len());
    for online in online_rows {
        let replay = replay_by_name.get(&online.feature_name).ok_or_else(|| {
            determinism(format!(
                "replay vector {vector_id} omitted feature {}",
                online.feature_name
            ))
        })?;
        if online.audit_fingerprint != replay.audit_fingerprint {
            tracing::error!(
                feature_vector_id = %vector_id,
                feature_name = %online.feature_name,
                differing_fields = ?feature_row_diff(online, replay),
                "feature parity feature-row audit fields differ",
            );
        }
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::FeatureCell,
            report_id: subject.report.copied(),
            model_run_id: subject.model_run.copied(),
            model_version_id: subject.model_version.copied(),
            market_id: Some(market_id.clone()),
            feature_name: Some(online.feature_name.clone()),
            online: feature_row_evidence(online)?,
            replay: feature_row_evidence(replay)?,
            transform_hash: None,
            detail: FeatureParityDetailSource::FeatureCell {
                feature_vector_id: *vector_id,
            },
        }));
    }
    Ok(comparisons)
}

fn feature_row_diff(
    online: &QuantFeatureEventRow,
    replay: &QuantFeatureEventRow,
) -> Vec<&'static str> {
    [
        ("event_time", online.event_time != replay.event_time),
        (
            "feature_vector_id",
            online.feature_vector_id != replay.feature_vector_id,
        ),
        (
            "decision_policy_snapshot_id",
            online.decision_policy_snapshot_id != replay.decision_policy_snapshot_id,
        ),
        ("decision_at", online.decision_at != replay.decision_at),
        (
            "knowledge_cutoff",
            online.knowledge_cutoff != replay.knowledge_cutoff,
        ),
        (
            "per_source_cutoffs_json",
            online.per_source_cutoffs_json != replay.per_source_cutoffs_json,
        ),
        ("market_id", online.market_id != replay.market_id),
        ("token_id", online.token_id != replay.token_id),
        (
            "feature_schema_version",
            online.feature_schema_version != replay.feature_schema_version,
        ),
        (
            "feature_schema_hash",
            online.feature_schema_hash != replay.feature_schema_hash,
        ),
        ("feature_hash", online.feature_hash != replay.feature_hash),
        (
            "decision_capture_hash",
            online.decision_capture_hash != replay.decision_capture_hash,
        ),
        ("feature_name", online.feature_name != replay.feature_name),
        ("cell_state", online.cell_state != replay.cell_state),
        ("raw_value", online.raw_value != replay.raw_value),
        ("value_kind", online.value_kind != replay.value_kind),
        ("source_kind", online.source_kind != replay.source_kind),
        (
            "evidence_source_kind",
            online.evidence_source_kind != replay.evidence_source_kind,
        ),
        (
            "evidence_reference",
            online.evidence_reference != replay.evidence_reference,
        ),
        (
            "evidence_effective_at",
            online.evidence_effective_at != replay.evidence_effective_at,
        ),
        (
            "evidence_available_at",
            online.evidence_available_at != replay.evidence_available_at,
        ),
        ("reason", online.reason != replay.reason),
        ("staleness_ms", online.staleness_ms != replay.staleness_ms),
        ("data_quality", online.data_quality != replay.data_quality),
    ]
    .into_iter()
    .filter_map(|(field, differs)| differs.then_some(field))
    .collect()
}

fn replay_feature_info(
    vector_id: &FeatureVectorId,
    replay_vector: &FeatureVector,
    boundary: &DecisionBoundary,
    capture: &DecisionCaptureEvidence,
    capture_hash: &ContentHash,
    created_at: DateTime<Utc>,
) -> QuantResult<FeatureVectorInfo> {
    let replay_new = replay_vector.try_to_new(boundary, capture)?;
    if &replay_new.decision_capture_hash != capture_hash {
        return Err(determinism(format!(
            "replay capture hash changed for feature vector {vector_id}"
        )));
    }
    Ok(FeatureVectorInfo {
        feature_vector_id: *vector_id,
        market_id: replay_new.market_id,
        token_id: replay_new.token_id,
        decision_at: replay_new.decision_at,
        decision_boundary: replay_new.decision_boundary,
        feature_schema_version: replay_new.feature_schema_version,
        feature_hash: replay_new.feature_hash,
        data_quality: replay_new.data_quality,
        staleness_ms: replay_new.staleness_ms,
        payload: replay_new.payload,
        source_refs: replay_new.source_refs,
        decision_capture: replay_new.decision_capture,
        decision_capture_hash: replay_new.decision_capture_hash,
        created_at,
    })
}

pub(crate) fn persisted_capture(
    info: &FeatureVectorInfo,
    rows: &[QuantFeatureEventRow],
    boundary: &DecisionBoundary,
) -> QuantResult<DecisionCaptureEvidence> {
    let capture = info.decision_capture.clone();
    let expected_hash = &info.decision_capture_hash;
    let expected_hash_text = expected_hash.canonical_text();
    let actual_hash = ResearchHasher::canonical(&capture)?;
    if actual_hash != *expected_hash
        || capture.snapshot.boundary != *boundary
        || capture.snapshot.market_id != info.market_id
        || Some(&capture.snapshot.token_id) != info.token_id.as_ref()
        || rows
            .iter()
            .any(|row| row.decision_capture_hash.as_bytes() != expected_hash_text.as_bytes())
    {
        return Err(determinism(format!(
            "feature vector {} decision capture does not match its hash/boundary/identity commitment",
            info.feature_vector_id
        )));
    }
    Ok(capture)
}

fn model_input_comparisons(
    candidate: &FeatureParityCandidate,
    model_run_id: &ModelRunId,
    report_id: Option<&RecommendationReportId>,
    online: &[QuantModelInputEventRow],
    replay: &[QuantModelInputEventRow],
) -> QuantResult<Vec<FeatureParityComparison>> {
    let replay_by_key = replay
        .iter()
        .map(|row| (model_input_key(row), row))
        .collect::<BTreeMap<_, _>>();
    let mut comparisons = Vec::with_capacity(online.len());
    for row in online {
        let key = model_input_key(row);
        let replay_row = replay_by_key
            .get(&key)
            .ok_or_else(|| determinism(format!("replay omitted model input {key}")))?;
        let transform_hash = Some(ContentHash::parse(&row.transform_hash)?);
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::ModelInput,
            report_id: report_id.copied(),
            model_run_id: Some(*model_run_id),
            model_version_id: Some(row.model_version_id),
            market_id: Some(row.market_id.clone()),
            feature_name: Some(row.encoded_column.clone()),
            online: model_input_evidence(row)?,
            replay: model_input_evidence(replay_row)?,
            transform_hash,
            detail: FeatureParityDetailSource::ModelInput {
                raw_input_name: FeatureName::new(row.raw_input_name.clone()),
                feature_vector_id: row.feature_vector_id,
            },
        }));
    }
    if replay_by_key.len() != online.len() {
        return Err(determinism(format!(
            "replay model-input width {} differs from online {}",
            replay_by_key.len(),
            online.len()
        )));
    }
    Ok(comparisons)
}

fn prediction_comparison(
    candidate: &FeatureParityCandidate,
    run: &ModelRunInfo,
    report_id: Option<RecommendationReportId>,
    replay: &ModelRuntimeOutput,
    boundary: &DecisionBoundary,
) -> QuantResult<FeatureParityComparison> {
    let online_hash = run.output_hash.ok_or_else(|| {
        determinism(format!(
            "successful run {} has no output hash",
            run.model_run_id
        ))
    })?;
    let replay_hash = canonical_business_prediction_hash(&replay.candidates)?;
    Ok(comparison(ComparisonInput {
        candidate,
        stage: FeatureParityStage::Prediction,
        report_id,
        model_run_id: Some(run.model_run_id),
        model_version_id: run.model_version_id,
        market_id: None,
        feature_name: None,
        online: canonical_evidence(&online_hash, None, boundary)?,
        replay: canonical_evidence(&replay_hash, None, boundary)?,
        transform_hash: None,
        detail: FeatureParityDetailSource::Prediction {
            candidate_count: count(replay.candidates.len(), "prediction candidate count")?,
        },
    }))
}

struct ComparisonInput<'a> {
    candidate: &'a FeatureParityCandidate,
    stage: FeatureParityStage,
    report_id: Option<RecommendationReportId>,
    model_run_id: Option<ModelRunId>,
    model_version_id: Option<ModelVersionId>,
    market_id: Option<MarketId>,
    feature_name: Option<String>,
    online: FeatureParityEvidence,
    replay: FeatureParityEvidence,
    transform_hash: Option<ContentHash>,
    detail: FeatureParityDetailSource,
}

fn comparison(input: ComparisonInput<'_>) -> FeatureParityComparison {
    FeatureParityComparison {
        sampling_key: input.candidate.sampling_key.clone(),
        decision_at: input.candidate.decision_at,
        stage: input.stage,
        report_id: input.report_id,
        model_run_id: input.model_run_id,
        model_version_id: input.model_version_id,
        training_dataset_id: None,
        market_id: input.market_id,
        feature_name: input.feature_name,
        reason: None,
        online: input.online,
        replay: input.replay,
        transform_hash: input.transform_hash,
        detail: input.detail,
    }
}

fn count(value: usize, field: &str) -> QuantResult<u64> {
    u64::try_from(value)
        .map_err(|error| determinism(format!("feature parity {field} does not fit u64: {error}")))
}

fn canonical_evidence<T: Serialize>(
    value: &T,
    state: Option<FeatureCellState>,
    boundary: &DecisionBoundary,
) -> QuantResult<FeatureParityEvidence> {
    Ok(FeatureParityEvidence {
        state,
        value: Some(serde_json::to_string(value).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("serialize parity evidence: {error}"),
            }
        })?),
        effective_at: None,
        available_at: None,
        cutoff: Some(boundary.knowledge_cutoff()),
        fingerprint: ResearchHasher::canonical(&(value, boundary))?.to_string(),
    })
}

fn feature_row_evidence(row: &QuantFeatureEventRow) -> QuantResult<FeatureParityEvidence> {
    Ok(FeatureParityEvidence {
        state: Some(match row.cell_state {
            ChFeatureCellState::Observed => FeatureCellState::Observed,
            ChFeatureCellState::Substituted => FeatureCellState::Substituted,
            ChFeatureCellState::Missing => FeatureCellState::Missing,
            ChFeatureCellState::NotApplicable => FeatureCellState::NotApplicable,
        }),
        value: row.raw_value.clone(),
        effective_at: optional_millis(row.evidence_effective_at, "feature effective_at")?,
        available_at: optional_millis(row.evidence_available_at, "feature available_at")?,
        cutoff: Some(required_millis(
            row.knowledge_cutoff,
            "feature knowledge_cutoff",
        )?),
        fingerprint: row.audit_fingerprint.clone(),
    })
}

fn model_input_evidence(row: &QuantModelInputEventRow) -> QuantResult<FeatureParityEvidence> {
    let state = match row.raw_state.as_str() {
        "observed" | "scored" => Some(FeatureCellState::Observed),
        "substituted" => Some(FeatureCellState::Substituted),
        "missing" | "missing_input" | "indeterminate" => Some(FeatureCellState::Missing),
        "not_applicable" => Some(FeatureCellState::NotApplicable),
        other => return Err(determinism(format!("unknown model input state `{other}`"))),
    };
    Ok(FeatureParityEvidence {
        state,
        value: row.encoded_value_bits.map(|bits| format!("{bits:016x}")),
        effective_at: None,
        available_at: None,
        cutoff: Some(required_millis(
            row.knowledge_cutoff,
            "model input knowledge_cutoff",
        )?),
        fingerprint: row.audit_fingerprint.clone(),
    })
}

fn boundary_from_online(
    completion: &QuantServingEvidenceCompletionRow,
    inputs: &[QuantModelInputEventRow],
    features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
) -> QuantResult<DecisionBoundary> {
    let vector_ids = completion_vector_ids(completion)?;
    let first_vector_id = vector_ids
        .first()
        .ok_or_else(|| determinism("serving completion has no feature vectors".to_owned()))?;
    let first = feature_infos.get(first_vector_id).ok_or_else(|| {
        determinism(format!(
            "no persisted feature boundary for vector {first_vector_id}"
        ))
    })?;
    let boundary = &first.decision_boundary;
    boundary.validate()?;

    // The typed PG boundary owns the exact clock. CH scalar columns are
    // millisecond projections; JSON cutoffs retain the original precision.
    let registered = DecisionClock::new(boundary.knowledge_lag_secs()).serving_boundary(
        boundary.decision_at(),
        0,
        0,
    )?;
    if boundary
        .per_source_cutoffs()
        .keys()
        .ne(registered.per_source_cutoffs().keys())
    {
        return Err(determinism(
            "persisted serving boundary has an incomplete source population".to_owned(),
        ));
    }
    let decision_at_ms = boundary.decision_at().timestamp_millis();
    let knowledge_cutoff_ms = boundary.knowledge_cutoff().timestamp_millis();
    if completion.decision_at != decision_at_ms
        || completion.knowledge_cutoff != knowledge_cutoff_ms
    {
        return Err(determinism(
            "serving completion clock differs from its exact Postgres boundary".to_owned(),
        ));
    }
    if features.len() != vector_ids.len() || feature_infos.len() != vector_ids.len() {
        return Err(determinism(
            "serving completion has an inconsistent feature boundary population".to_owned(),
        ));
    }
    let first_row = features
        .get(first_vector_id)
        .and_then(|rows| rows.first())
        .ok_or_else(|| determinism(format!("no feature boundary for vector {first_vector_id}")))?;
    let expected_source_cutoffs = &first_row.per_source_cutoffs_json;
    let cutoffs: BTreeMap<DecisionSource, DateTime<Utc>> =
        serde_json::from_str(expected_source_cutoffs).map_err(|error| {
            ResearchError::Determinism {
                detail: format!("invalid serving per-source cutoffs: {error}"),
            }
        })?;
    if &cutoffs != boundary.per_source_cutoffs() {
        return Err(determinism(format!(
            "feature vector {first_vector_id} source cutoffs differ from its exact Postgres boundary"
        )));
    }

    let mut seen = HashSet::with_capacity(vector_ids.len());
    for vector_id in vector_ids {
        if !seen.insert(vector_id) {
            return Err(determinism(
                "serving completion repeats a feature boundary".to_owned(),
            ));
        }
        let info = feature_infos.get(&vector_id).ok_or_else(|| {
            determinism(format!(
                "no persisted feature boundary for vector {vector_id}"
            ))
        })?;
        info.decision_boundary.validate()?;
        if info.feature_vector_id != vector_id
            || info.decision_at != boundary.decision_at()
            || &info.decision_boundary != boundary
            || &info.decision_capture.snapshot.boundary != boundary
        {
            return Err(determinism(format!(
                "Postgres feature vector {vector_id} contains a different exact decision boundary"
            )));
        }
        let rows = features
            .get(&vector_id)
            .filter(|rows| !rows.is_empty())
            .ok_or_else(|| determinism(format!("no feature boundary for vector {vector_id}")))?;
        for row in rows {
            if row.feature_vector_id != vector_id
                || row.decision_at != decision_at_ms
                || row.knowledge_cutoff != knowledge_cutoff_ms
                || &row.per_source_cutoffs_json != expected_source_cutoffs
            {
                return Err(determinism(format!(
                    "feature vector {vector_id} source cutoffs or projected clock differ from its exact Postgres boundary"
                )));
            }
        }
    }
    for row in inputs {
        if row.decision_at != decision_at_ms || row.knowledge_cutoff != knowledge_cutoff_ms {
            return Err(determinism(format!(
                "model input run {} contains multiple decision boundaries",
                completion.model_run_id
            )));
        }
    }
    Ok(boundary.clone())
}

fn vector_binding(
    inputs: &[QuantModelInputEventRow],
) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
    let mut binding = HashMap::new();
    for row in inputs {
        match binding.insert(row.market_id.clone(), row.feature_vector_id) {
            Some(previous) if previous != row.feature_vector_id => {
                return Err(determinism(format!(
                    "market {} is bound to multiple feature vectors in run {}",
                    row.market_id, row.model_run_id
                )));
            }
            _ => {}
        }
    }
    Ok(binding)
}

fn feature_vector_binding(
    features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
    let mut binding = HashMap::with_capacity(features.len());
    for (vector_id, rows) in features {
        let first = rows
            .first()
            .ok_or_else(|| determinism(format!("feature evidence group {vector_id} is empty")))?;
        if first.feature_vector_id != *vector_id {
            return Err(determinism(format!(
                "feature evidence group key {vector_id} contains vector {}",
                first.feature_vector_id
            )));
        }
        if rows.iter().any(|row| {
            row.feature_vector_id != *vector_id
                || row.market_id != first.market_id
                || row.token_id != first.token_id
        }) {
            return Err(determinism(format!(
                "feature evidence group {vector_id} has inconsistent vector/market/token bindings"
            )));
        }
        if let Some(previous) = binding.insert(first.market_id.clone(), *vector_id) {
            return Err(determinism(format!(
                "market {} is bound to feature vectors {previous} and {vector_id}",
                first.market_id
            )));
        }
    }
    Ok(binding)
}

fn group_model_inputs(
    rows: Vec<QuantModelInputEventRow>,
) -> HashMap<ModelRunId, Vec<QuantModelInputEventRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.model_run_id)
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn group_feature_rows(
    rows: Vec<QuantFeatureEventRow>,
) -> HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>> {
    let mut grouped = HashMap::new();
    for row in rows {
        grouped
            .entry(row.feature_vector_id)
            .or_insert_with(Vec::new)
            .push(row);
    }
    grouped
}

fn dedupe_model_input_rows(
    rows: Vec<QuantModelInputEventRow>,
) -> QuantResult<Vec<QuantModelInputEventRow>> {
    let mut unique: BTreeMap<String, QuantModelInputEventRow> = BTreeMap::new();
    for row in rows {
        let key = model_input_key(&row);
        if let Some(previous) = unique.get(&key) {
            if previous.audit_fingerprint != row.audit_fingerprint {
                return Err(determinism(format!(
                    "conflicting model-input retries for key {key}"
                )));
            }
            continue;
        }
        unique.insert(key, row);
    }
    Ok(unique.into_values().collect())
}

fn dedupe_completions(
    rows: Vec<QuantServingEvidenceCompletionRow>,
) -> QuantResult<Vec<QuantServingEvidenceCompletionRow>> {
    let mut unique: HashMap<ModelRunId, QuantServingEvidenceCompletionRow> = HashMap::new();
    for row in rows {
        if let Some(previous) = unique.get(&row.model_run_id) {
            if previous.completion_hash != row.completion_hash {
                return Err(determinism(format!(
                    "conflicting serving evidence completion markers for run {}",
                    row.model_run_id
                )));
            }
            continue;
        }
        unique.insert(row.model_run_id, row);
    }
    let mut rows = unique.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|row| row.model_run_id.to_string());
    Ok(rows)
}

fn completion_vector_ids(
    row: &QuantServingEvidenceCompletionRow,
) -> QuantResult<Vec<FeatureVectorId>> {
    serde_json::from_str(&row.feature_vector_ids_json).map_err(|error| {
        ResearchError::Serialization {
            detail: format!(
                "deserialize feature vector ids from serving completion {}: {error}",
                row.model_run_id
            ),
        }
        .into()
    })
}

fn unique_run_ids(candidates: &[FeatureParityCandidate]) -> Vec<ModelRunId> {
    candidates
        .iter()
        .filter_map(|candidate| match &candidate.subject {
            FeatureParitySubject::ModelRun(run_id) => Some(*run_id),
            FeatureParitySubject::RecommendationReport(_) => None,
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect()
}

fn group_candidates_by_run(
    candidates: &[FeatureParityCandidate],
) -> Vec<(ModelRunId, Vec<&FeatureParityCandidate>)> {
    let mut by_run: HashMap<ModelRunId, Vec<&FeatureParityCandidate>> = HashMap::new();
    for candidate in candidates {
        if let FeatureParitySubject::ModelRun(run_id) = &candidate.subject {
            by_run.entry(*run_id).or_default().push(candidate);
        }
    }
    let mut grouped = by_run.into_iter().collect::<Vec<_>>();
    grouped.sort_by_key(|(run_id, _)| run_id.to_string());
    for (_, run_candidates) in &mut grouped {
        run_candidates.sort_by(|left, right| left.sampling_key.cmp(&right.sampling_key));
    }
    grouped
}

fn group_candidates_by_report(
    candidates: &[FeatureParityCandidate],
) -> Vec<(RecommendationReportId, Vec<&FeatureParityCandidate>)> {
    let mut by_report: HashMap<RecommendationReportId, Vec<&FeatureParityCandidate>> =
        HashMap::new();
    for candidate in candidates {
        if let FeatureParitySubject::RecommendationReport(report_id) = &candidate.subject {
            by_report.entry(*report_id).or_default().push(candidate);
        }
    }
    let mut grouped = by_report.into_iter().collect::<Vec<_>>();
    grouped.sort_by_key(|(report_id, _)| report_id.to_string());
    for (_, report_candidates) in &mut grouped {
        report_candidates.sort_by(|left, right| left.sampling_key.cmp(&right.sampling_key));
    }
    grouped
}

fn validate_run_completion<'a>(
    run_id: &ModelRunId,
    candidate: &FeatureParityCandidate,
    completion: &QuantServingEvidenceCompletionRow,
    inputs_by_run: &'a HashMap<ModelRunId, Vec<QuantModelInputEventRow>>,
    features_by_vector: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
) -> QuantResult<&'a [QuantModelInputEventRow]> {
    if completion.decision_at != candidate.decision_at.timestamp_millis() {
        return Err(determinism(format!(
            "serving evidence completion for run {run_id} has decision_at {} instead of {}",
            completion.decision_at,
            candidate.decision_at.timestamp_millis()
        )));
    }
    let run_inputs = inputs_by_run.get(run_id).map_or(&[][..], Vec::as_slice);
    let expected_feature_ids = completion_vector_ids(completion)?;
    let run_features = expected_feature_ids
        .iter()
        .map(|id| {
            features_by_vector.get(id).cloned().ok_or_else(|| {
                determinism(format!(
                    "completed serving run {run_id} has no durable feature rows for vector {id}"
                ))
            })
        })
        .collect::<QuantResult<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    verify_completion(completion, &run_features, run_inputs)?;
    Ok(run_inputs)
}

fn validate_candidate_run(
    row: &ModelRunInfo,
    parity_run: &FeatureParityRunInfo,
) -> QuantResult<()> {
    if row.run_kind != ModelRunKind::LiveInference || row.status != ModelRunStatus::Succeeded {
        return Err(determinism(format!(
            "parity candidate {} is {:?}/{:?}, expected live_inference/succeeded",
            row.model_run_id, row.run_kind, row.status
        )));
    }
    if row.model_version_id.is_none() || row.market_selection_id.is_none() {
        return Err(determinism(format!(
            "successful live run {} lacks model/selection binding",
            row.model_run_id
        )));
    }
    let decision_at = row.window_start;
    let parity_window = parity_run.window_start..parity_run.window_end;
    if !parity_window.contains(&decision_at) {
        return Err(determinism(format!(
            "model run {} decision time {} is outside parity window [{}, {})",
            row.model_run_id, row.window_start, parity_run.window_start, parity_run.window_end
        )));
    }
    if parity_run
        .model_version_id
        .as_ref()
        .is_some_and(|expected| row.model_version_id.as_ref() != Some(expected))
    {
        return Err(determinism(format!(
            "model run {} does not match parity model-version scope",
            row.model_run_id
        )));
    }
    Ok(())
}

fn validate_route_run_binding<'a>(
    report: &RecommendationReportInfo,
    route_run: &'a ReportRouteRunInfo,
    run: &ModelRunInfo,
) -> QuantResult<&'a RouteModelLineage> {
    let lineage = route_run.lineage_json.as_ref().ok_or_else(|| {
        determinism(format!(
            "report {} Route {:?} has no frozen model lineage",
            report.recommendation_report_id, route_run.route
        ))
    })?;
    if route_run.report_run_id != report.report_run_id
        || route_run.model_run_id.as_ref() != Some(&run.model_run_id)
        || route_run.model_version_id != run.model_version_id
        || lineage.model_run_id.as_ref() != Some(&run.model_run_id)
        || Some(lineage.model_version_id) != run.model_version_id
        || route_run.model_run_id != lineage.model_run_id
        || route_run.model_version_id != Some(lineage.model_version_id)
        || run.window_start != report.decision_at
        || run.decision_policy_snapshot_id != report.decision_policy_snapshot_id
        || run.market_selection_id.as_ref() != Some(&report.market_selection_id)
    {
        return Err(determinism(format!(
            "report {} Route {:?} serving binding disagrees with model run {}",
            report.recommendation_report_id, route_run.route, run.model_run_id
        )));
    }
    Ok(lineage)
}

fn validate_report_subject(
    report: &RecommendationReportInfo,
    parity_run: &FeatureParityRunInfo,
) -> QuantResult<()> {
    if report.decision_at < parity_run.window_start || report.decision_at >= parity_run.window_end {
        return Err(determinism(format!(
            "global report {} decision time {} is outside parity window [{}, {})",
            report.recommendation_report_id,
            report.decision_at,
            parity_run.window_start,
            parity_run.window_end
        )));
    }
    if parity_run
        .report_id
        .as_ref()
        .is_some_and(|expected| expected != &report.recommendation_report_id)
    {
        return Err(determinism(format!(
            "global report {} does not match parity report scope",
            report.recommendation_report_id
        )));
    }
    Ok(())
}

fn validate_quality_evidence(
    report: &RecommendationReportInfo,
    members: &[MarketSelectionMemberInfo],
    dq: &ReportDataQualitySnapshotInfo,
) -> QuantResult<()> {
    let wrong_snapshot = dq.report_data_quality_snapshot_id != report.data_quality_snapshot_ref;
    let wrong_decision = dq.decision_at != report.decision_at;
    let wrong_config = dq.decision_policy_snapshot_id != report.decision_policy_snapshot_id;
    if wrong_snapshot || wrong_decision || wrong_config {
        return Err(determinism(format!(
            "report {} DQ snapshot is not bound to its decision/config",
            report.recommendation_report_id
        )));
    }
    let member_markets = members
        .iter()
        .map(|member| member.market_id.clone())
        .collect::<BTreeSet<_>>();
    if member_markets.len() != members.len() {
        return Err(determinism(format!(
            "report {} selection contains duplicate markets",
            report.recommendation_report_id
        )));
    }
    let mut vector_ids = HashSet::new();
    let mut token_ids = HashSet::new();
    for record in &dq.tokens_json.0 {
        if !member_markets.contains(&record.market_id)
            || !vector_ids.insert(record.feature_vector_id)
            || !token_ids.insert(record.token_id.clone())
        {
            return Err(determinism(format!(
                "global report {} has duplicate or out-of-selection DQ evidence",
                report.recommendation_report_id
            )));
        }
    }
    if member_markets.is_empty() && !dq.tokens_json.0.is_empty() {
        return Err(determinism(format!(
            "empty global report selection {} carries DQ rows",
            report.recommendation_report_id
        )));
    }
    Ok(())
}

fn validate_report_selection_binding(
    report: &RecommendationReportInfo,
    selection: &MarketSelectionInfo,
) -> QuantResult<()> {
    if selection.decision_at != report.decision_at
        || selection.decision_policy_snapshot_id != report.decision_policy_snapshot_id
    {
        return Err(determinism(format!(
            "selection {} is not bound to global report {} decision/config",
            selection.market_selection_id, report.recommendation_report_id
        )));
    }
    Ok(())
}

fn dedupe_feature_rows(rows: Vec<QuantFeatureEventRow>) -> QuantResult<Vec<QuantFeatureEventRow>> {
    let mut unique: BTreeMap<String, QuantFeatureEventRow> = BTreeMap::new();
    for row in rows {
        let key = format!("{}/{}", row.feature_vector_id, row.feature_name);
        if let Some(previous) = unique.get(&key) {
            if previous.audit_fingerprint != row.audit_fingerprint {
                return Err(determinism(format!(
                    "conflicting feature-event retries for key {key}"
                )));
            }
            continue;
        }
        unique.insert(key, row);
    }
    Ok(unique.into_values().collect())
}

fn model_input_key(row: &QuantModelInputEventRow) -> String {
    format!(
        "{}/{}/{}/{}/{}",
        row.model_version_id,
        row.market_id,
        row.feature_vector_id,
        row.raw_input_name,
        row.encoded_column
    )
}

fn representative_candidate<'a>(
    subject: &str,
    candidates: &[&'a FeatureParityCandidate],
) -> QuantResult<&'a FeatureParityCandidate> {
    candidates
        .first()
        .copied()
        .ok_or_else(|| determinism(format!("{subject} has no parity candidates")))
}

fn verified_input_bindings(
    run: &ModelRunInfo,
    inputs: &[QuantModelInputEventRow],
) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
    let version = run.model_version_id.ok_or_else(|| {
        determinism(format!(
            "model {} has no frozen version for input witnesses",
            run.model_run_id
        ))
    })?;
    for input in inputs {
        if input.model_run_id != run.model_run_id || input.model_version_id != version {
            return Err(determinism(format!(
                "model {} input witness changed its run/version binding",
                run.model_run_id
            )));
        }
        ContentHash::parse(&input.transform_hash).map_err(|error| {
            determinism(format!(
                "model {} input witness has an invalid transform: {error}",
                run.model_run_id
            ))
        })?;
    }
    vector_binding(inputs)
}

fn validate_replay_witnesses(
    run: &ModelRunInfo,
    candidates: &[&FeatureParityCandidate],
    inputs: &[QuantModelInputEventRow],
) -> QuantResult<()> {
    let bindings = verified_input_bindings(run, inputs)?;
    for candidate in candidates {
        let observed = candidate
            .market_id
            .as_ref()
            .and_then(|market| bindings.get(market));
        let valid = match candidate.input_witness {
            FeatureParityInputWitness::VerifiedModelInput { feature_vector_id } => {
                observed == Some(&feature_vector_id)
            }
            FeatureParityInputWitness::SelectionOnly => observed.is_none(),
            FeatureParityInputWitness::PendingServingEvidence => false,
        };
        if candidate.subject != FeatureParitySubject::ModelRun(run.model_run_id) || !valid {
            return Err(determinism(format!(
                "model {} candidate {} input witness differs from its committed binding",
                run.model_run_id, candidate.sampling_key,
            )));
        }
    }
    Ok(())
}

fn validate_witness_states(candidates: &[FeatureParityCandidate]) -> QuantResult<()> {
    let mut states = HashMap::<ModelRunId, bool>::new();
    for candidate in candidates {
        match &candidate.subject {
            FeatureParitySubject::RecommendationReport(_) => {
                if candidate.input_witness != FeatureParityInputWitness::SelectionOnly {
                    return Err(determinism(
                        "report selection cannot claim a model-input witness".to_owned(),
                    ));
                }
            }
            FeatureParitySubject::ModelRun(id) => {
                let pending =
                    candidate.input_witness == FeatureParityInputWitness::PendingServingEvidence;
                if states
                    .insert(*id, pending)
                    .is_some_and(|previous| previous != pending)
                {
                    return Err(determinism(format!(
                        "model {id} mixes pending and committed input qualifications"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn pending_completion(
    run_id: &ModelRunId,
    candidates: &[&FeatureParityCandidate],
) -> Vec<PendingFeatureParityComparison> {
    candidates
        .iter()
        .map(|candidate| {
            pending_writer(
                candidate,
                FeatureParityStage::ModelInput,
                Some(*run_id),
                None,
                "serving_evidence_completion_missing",
            )
        })
        .collect()
}

fn partition_witness_candidates(
    candidates: &[FeatureParityCandidate],
) -> QuantResult<(
    Vec<FeatureParityCandidate>,
    Vec<PendingFeatureParityComparison>,
)> {
    let mut ready = Vec::with_capacity(candidates.len());
    let mut pending = Vec::new();
    for candidate in candidates {
        match (&candidate.subject, candidate.input_witness) {
            (
                FeatureParitySubject::ModelRun(run_id),
                FeatureParityInputWitness::PendingServingEvidence,
            ) => {
                // The discovery snapshot stays pending even if the writer becomes
                // visible before replay starts. The next attempt re-qualifies it.
                pending.extend(pending_completion(run_id, &[candidate]));
            }
            (FeatureParitySubject::RecommendationReport(_), witness)
                if witness != FeatureParityInputWitness::SelectionOnly =>
            {
                return Err(determinism(
                    "report selection cannot claim a model-input witness".to_owned(),
                ));
            }
            _ => ready.push(candidate.clone()),
        }
    }
    Ok((ready, pending))
}

fn select_comparisons(
    subject: &str,
    candidates: &[&FeatureParityCandidate],
    comparisons: &[FeatureParityComparison],
) -> QuantResult<Vec<FeatureParityComparison>> {
    let mut selected_comparisons = Vec::new();
    for (index, selected) in candidates.iter().enumerate() {
        let mut selected_count = 0_usize;
        for comparison in comparisons {
            let selected_market = comparison.market_id.as_ref() == selected.market_id.as_ref();
            let run_global = comparison.market_id.is_none() && index == 0;
            if selected_market || run_global {
                let mut comparison = comparison.clone();
                comparison.sampling_key.clone_from(&selected.sampling_key);
                selected_comparisons.push(comparison);
                selected_count += 1;
            }
        }
        if selected_count == 0 {
            return Err(determinism(format!(
                "replay produced no comparison for sampled market {} in subject {subject}",
                selected
                    .market_id
                    .as_ref()
                    .map_or("<empty-selection>", MarketId::as_str),
            )));
        }
    }
    Ok(selected_comparisons)
}

fn pending_writer(
    candidate: &FeatureParityCandidate,
    stage: FeatureParityStage,
    model_run_id: Option<ModelRunId>,
    observed_watermark: Option<DateTime<Utc>>,
    reason: &str,
) -> PendingFeatureParityComparison {
    PendingFeatureParityComparison {
        sampling_key: candidate.sampling_key.clone(),
        decision_at: candidate.decision_at,
        stage,
        report_id: None,
        model_run_id,
        model_version_id: None,
        training_dataset_id: None,
        market_id: candidate.market_id.clone(),
        feature_name: None,
        reason: reason.to_owned(),
        online: None,
        required_watermark: candidate.decision_at,
        observed_watermark,
    }
}

fn required_millis(value: i64, field: &str) -> QuantResult<DateTime<Utc>> {
    DateTime::from_timestamp_millis(value)
        .ok_or_else(|| determinism(format!("{field} is outside chrono range: {value}")))
}

fn optional_millis(value: Option<i64>, field: &str) -> QuantResult<Option<DateTime<Utc>>> {
    value.map(|value| required_millis(value, field)).transpose()
}

fn verify_replay_contract(
    version: &ModelVersionInfo,
    feature_schema_hash: ContentHash,
    factor_plane: &FactorServingPlane,
    bias_table_hash: Option<ContentHash>,
) -> QuantResult<()> {
    let contract = version.verified_serving_contract().map_err(|error| {
        determinism(format!(
            "model {} has an invalid persisted serving contract: {error}",
            version.model_version_id
        ))
    })?;
    let bindings = contract.bindings();
    let bound_bias_hash = bindings
        .factors
        .bias_table
        .as_ref()
        .map(|binding| binding.content_hash);
    let factor_plane_matches = version.model_family.is_classical()
        || (&bindings.factors.plane == factor_plane && bound_bias_hash == bias_table_hash);
    if bindings.schemas.feature_schema_hash != feature_schema_hash || !factor_plane_matches {
        return Err(determinism(format!(
            "model {} serving contract differs from exact replay: feature schema bound={} replay={}; factor plane bound={} replay={}; bias table bound={bound_bias_hash:?} replay={bias_table_hash:?}",
            version.model_version_id,
            bindings.schemas.feature_schema_hash,
            feature_schema_hash,
            bindings.factors.plane.factor_schema_hash(),
            factor_plane.factor_schema_hash(),
        )));
    }
    Ok(())
}

fn determinism(detail: String) -> QuantError {
    ResearchError::Determinism { detail }.into()
}

#[cfg(test)]
mod report_selector_tests {
    use chrono::{DateTime, Utc};
    use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
    use quant_pivot_models::{
        domain::{data_plane::DecisionClock, quant::MarketSelectionInfo},
        runtime_config::{BuyModelRoute, DataQualityConfig, FeaturesConfig, SelectionConfig},
        types::{DecisionPolicySnapshotId, HistoryServingHeadSealId, RecommendationReportId},
    };
    use quant_pivot_research::{
        hashing::ResearchHasher,
        selection::{
            ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelector,
            ModelFeatureRequirements, RouteAvailabilityContract,
        },
    };

    use super::{
        ComparisonSubject, FeatureParityCandidate, FeatureParityInputWitness, FeatureParitySubject,
        ReportSelectorBinding, selection_comparisons,
    };
    use crate::report::universe::ReportUniverseContract;

    #[test]
    fn rejects_changed_universe() -> QuantResult<()> {
        let hash = ResearchHasher::canonical(&"frozen universe")?;
        let changed = ResearchHasher::canonical(&"changed universe")?;
        let binding = ReportSelectorBinding::Runtime {
            serving_head_seal_id: HistoryServingHeadSealId::from_v7(),
            serving_head_seal_hash: hash,
            universe_plan_hash: hash,
        };
        for candidate_hash in [hash, changed] {
            let contract = ReportUniverseContract {
                availability: RouteAvailabilityContract {
                    primary_route: BuyModelRoute::Pooled,
                    active_routes: vec![BuyModelRoute::Pooled, BuyModelRoute::Weather],
                    universe_plan_hash: candidate_hash,
                },
                requirements: ModelFeatureRequirements::default(),
            };
            let result = binding.verify_universe(contract);
            if candidate_hash == hash {
                assert_eq!(
                    result?.route_availability.map(|row| row.universe_plan_hash),
                    Some(hash)
                );
            } else {
                assert!(matches!(
                    result,
                    Err(QuantError::Research(ResearchError::Determinism { .. }))
                ));
            }
        }
        let contract = ReportUniverseContract {
            availability: RouteAvailabilityContract {
                primary_route: BuyModelRoute::Pooled,
                active_routes: vec![BuyModelRoute::Pooled],
                universe_plan_hash: hash,
            },
            requirements: ModelFeatureRequirements::default(),
        };
        assert!(
            ReportSelectorBinding::Materialized
                .verify_universe(contract)
                .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn typed_selection_comparison() -> QuantResult<()> {
        let decision_at: DateTime<Utc> =
            DateTime::from_timestamp(1_800_000_000, 0).expect("decision");
        let request = MarketSelectionBuildRequest {
            decision_at,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            selection: SelectionConfig::default(),
            data_quality: DataQualityConfig::default(),
            features: FeaturesConfig::default(),
            model_requirements: ModelFeatureRequirements::default(),
            knowledge_lag_secs: 10,
            route_availability: Some(RouteAvailabilityContract {
                primary_route: BuyModelRoute::Pooled,
                active_routes: vec![BuyModelRoute::Pooled, BuyModelRoute::Weather],
                universe_plan_hash: ResearchHasher::canonical(&"all-active universe")?,
            }),
        };
        let selector = ConfiguredMarketSelector::new();
        let online = selector.build_snapshot(request.clone(), Vec::new()).await?;
        let replay = selector.build_snapshot(request.clone(), Vec::new()).await?;
        let mut omitted = request;
        omitted.route_availability = None;
        let broken = selector.build_snapshot(omitted, Vec::new()).await?;
        let persisted = MarketSelectionInfo {
            market_selection_id: online.market_selection_id,
            decision_at,
            decision_policy_snapshot_id: online.decision_policy_snapshot_id,
            selector_hash: online.selector_hash,
            selector_evidence: online.selector_evidence,
            market_count: 0,
            exclusion_summary: online.exclusion_summary,
            created_at: decision_at,
        };
        let report_id = RecommendationReportId::from_v7();
        let candidate = FeatureParityCandidate {
            sampling_key: format!("report/{report_id}"),
            subject: FeatureParitySubject::RecommendationReport(report_id),
            market_id: None,
            decision_at,
            input_witness: FeatureParityInputWitness::SelectionOnly,
        };
        let boundary = DecisionClock::new(10).serving_boundary(decision_at, 0, 0)?;
        for (replayed, matches) in [(&replay, true), (&broken, false)] {
            let comparisons = selection_comparisons(
                &candidate,
                ComparisonSubject {
                    report: Some(&report_id),
                    model_run: None,
                    model_version: None,
                },
                &persisted,
                &[],
                replayed,
                &boundary,
            )?;
            assert_eq!(comparisons.len(), 1);
            assert_eq!(
                comparisons[0].online.fingerprint == comparisons[0].replay.fingerprint,
                matches
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeMap, HashMap},
        hint, slice,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread,
        time::{Duration as StdDuration, Instant as StdInstant},
    };

    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
    use quant_pivot_error::{QuantError, research::ResearchError};
    use quant_pivot_models::{
        clickhouse::{
            QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
        },
        domain::{
            data_plane::{DecisionBoundary, DecisionClock, DecisionSource},
            quant::{FeatureVectorInfo, MarketSelectionMemberInfo, ReportDataQualitySnapshotInfo},
        },
        enums::{
            catalog::CatalogTimestampQuality,
            clickhouse::{ChFeatureCellState, ChFeatureSourceKind, ChFeatureValueKind},
            common::MarketCategory,
            factor::FactorFamily,
            market::MarketStatus,
            quant::{OutcomeSide, RecommendationReportStatus, ReportKind},
        },
        runtime_config::{BuyModelRoute, DomainConfig, FactorsConfig, FeaturesConfig},
        types::{
            CatalogDecisionRef, CatalogEventChangeId, CatalogMarketChangeId, CatalogSyncBatchId,
            ContentHash, DecisionCaptureEvidence, DecisionPolicySnapshotId,
            DecisionSnapshotEvidence, EventId, FeatureParityDetailSource, FeatureSourceRefs,
            FeatureVectorId, FeatureVectorPayload, FinalizedExecutionEvidence, MarketSelectionId,
            ModelVersionId, Probability, RecommendationId, ReportDataQualityTokens,
            ResearchFeatureContract, SchemaVersion, SelectionMemberEvidence,
            TokenDataQualityRecord, TokenId, Usd,
        },
    };
    use quant_pivot_research::hashing::ResearchHasher;
    use rust_decimal_macros::dec;
    use tokio::time::{Instant, sleep, timeout};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::{
        DataQualityStatus, DurableFeatureParitySource, FactorEngine, FeatureParityCandidate,
        FeatureParityComparison, FeatureParityEvidence, FeatureParityInputWitness,
        FeatureParityStage, FeatureParitySubject, MarketId, ModelRunId, ModelRunInfo, ModelRunKind,
        ModelRunStatus, ParityComputeBoundary, RecommendationReportId, ReplayHistoryMode,
        boundary_from_online, feature_vector_binding, online_route_binding,
        partition_witness_candidates, pending_completion, replay_feature_population,
        representative_candidate, select_comparisons, validate_input_population,
        validate_quality_evidence, validate_replay_witnesses, validate_run_completion,
        validate_witness_states, verified_input_bindings,
    };
    use crate::{
        observability::serving_evidence::{
            SERVING_EVIDENCE_FORMAT_VERSION, completion_marker, feature_commitment,
        },
        test_fixtures::report_fixtures,
    };

    fn candidate(
        run_id: &ModelRunId,
        market: &str,
        decision_at: DateTime<Utc>,
    ) -> FeatureParityCandidate {
        let market_id = MarketId::new(market);
        FeatureParityCandidate {
            sampling_key: format!("{run_id}/{market_id}"),
            subject: FeatureParitySubject::ModelRun(*run_id),
            market_id: Some(market_id),
            decision_at,
            input_witness: FeatureParityInputWitness::SelectionOnly,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn boundary_threads_and_cancel() {
        let io_thread = async {
            tokio::task::yield_now().await;
            thread::current().name().unwrap_or_default().to_owned()
        }
        .await;
        assert!(!io_thread.starts_with("quant-offline-"));
        let boundary = ParityComputeBoundary::new(
            Arc::new(ComputeExecutor::new().expect("compute executor")),
            OfflineMemory::try_gib(1).expect("offline memory"),
            1,
        );
        let cancel = CancellationToken::new();
        let kernel = boundary.clone();
        let kernel_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            kernel
                .run(&kernel_cancel, || {
                    let until = StdInstant::now() + StdDuration::from_millis(100);
                    while StdInstant::now() < until {
                        hint::spin_loop();
                    }
                    Ok(thread::current().name().unwrap_or_default().to_owned())
                })
                .await
        });
        let heartbeat = Instant::now();
        sleep(StdDuration::from_millis(10)).await;
        assert!(heartbeat.elapsed() < StdDuration::from_millis(80));
        let kernel_thread = task.await.expect("kernel task").expect("offline kernel");
        assert!(kernel_thread.starts_with("quant-offline-"));

        let release = Arc::new(AtomicBool::new(false));
        let started = Arc::new(AtomicBool::new(false));
        let holder_boundary = boundary.clone();
        let holder_cancel = CancellationToken::new();
        let holder_release = Arc::clone(&release);
        let holder_started = Arc::clone(&started);
        let holder = tokio::spawn(async move {
            holder_boundary
                .run(&holder_cancel, move || {
                    holder_started.store(true, Ordering::Release);
                    while !holder_release.load(Ordering::Acquire) {
                        hint::spin_loop();
                    }
                    Ok(())
                })
                .await
        });
        while !started.load(Ordering::Acquire) {
            sleep(StdDuration::from_millis(1)).await;
        }
        let waiting_cancel = CancellationToken::new();
        let waiting_boundary = boundary.clone();
        let waiting_token = waiting_cancel.clone();
        let waiting =
            tokio::spawn(async move { waiting_boundary.run(&waiting_token, || Ok(())).await });
        waiting_cancel.cancel();
        let error = timeout(StdDuration::from_millis(100), waiting)
            .await
            .expect("waiting cancellation deadline")
            .expect("waiting task")
            .expect_err("waiting kernel must cancel");
        assert!(matches!(
            error,
            QuantError::Research(ResearchError::Cancelled { .. })
        ));
        release.store(true, Ordering::Release);
        holder.await.expect("holder task").expect("holder kernel");

        let running_cancel = CancellationToken::new();
        let running_boundary = boundary.clone();
        let running_token = running_cancel.clone();
        let running_started = Arc::new(AtomicBool::new(false));
        let kernel_started = Arc::clone(&running_started);
        let kernel_token = running_cancel.clone();
        let running = tokio::spawn(async move {
            running_boundary
                .run(&running_token, move || {
                    kernel_started.store(true, Ordering::Release);
                    while !kernel_token.is_cancelled() {
                        hint::spin_loop();
                    }
                    Err::<(), QuantError>(
                        ResearchError::Cancelled {
                            detail: "test kernel observed cancellation".to_owned(),
                        }
                        .into(),
                    )
                })
                .await
        });
        while !running_started.load(Ordering::Acquire) {
            sleep(StdDuration::from_millis(1)).await;
        }
        running_cancel.cancel();
        let error = timeout(StdDuration::from_millis(100), running)
            .await
            .expect("running cancellation deadline")
            .expect("running task")
            .expect_err("running kernel must cancel");
        assert!(matches!(
            error,
            QuantError::Research(ResearchError::Cancelled { .. })
        ));
    }

    #[test]
    fn history_modes_fail_closed() {
        let market = MarketId::new("history-mode-market");
        let mut evidence =
            HashMap::from([(market.clone(), FinalizedExecutionEvidence::NotRequired)]);
        assert_eq!(
            DurableFeatureParitySource::replay_history_mode(false, &evidence)
                .expect("not-required history mode"),
            ReplayHistoryMode::NotRequired
        );
        assert!(DurableFeatureParitySource::replay_history_mode(true, &evidence).is_err());

        evidence.insert(
            market.clone(),
            FinalizedExecutionEvidence::runtime(false, None, None),
        );
        assert_eq!(
            DurableFeatureParitySource::replay_history_mode(true, &evidence)
                .expect("disabled runtime history mode"),
            ReplayHistoryMode::RuntimeDisabled
        );

        let accepted_at =
            DateTime::from_timestamp(1_700_000_000, 0).expect("history-mode acceptance time");
        evidence.insert(
            market.clone(),
            FinalizedExecutionEvidence::runtime(true, Some(42), Some(accepted_at)),
        );
        assert_eq!(
            DurableFeatureParitySource::replay_history_mode(true, &evidence)
                .expect("enabled runtime history mode"),
            ReplayHistoryMode::RuntimeEnabled {
                accepted_through_block: 42,
                accepted_through_at: accepted_at,
            }
        );

        evidence.insert(
            MarketId::new("history-mode-mismatch"),
            FinalizedExecutionEvidence::runtime(true, Some(43), Some(accepted_at)),
        );
        assert!(DurableFeatureParitySource::replay_history_mode(true, &evidence).is_err());

        evidence.clear();
        evidence.insert(
            market,
            FinalizedExecutionEvidence::materialized(accepted_at),
        );
        assert_eq!(
            DurableFeatureParitySource::replay_history_mode(true, &evidence)
                .expect("materialized history mode"),
            ReplayHistoryMode::Materialized {
                available_by: accepted_at,
            }
        );
    }

    fn comparison(
        run_id: &ModelRunId,
        market_id: Option<MarketId>,
        decision_at: DateTime<Utc>,
    ) -> FeatureParityComparison {
        let evidence = FeatureParityEvidence {
            state: None,
            value: Some("value".to_owned()),
            effective_at: None,
            available_at: None,
            cutoff: Some(decision_at),
            fingerprint: "fingerprint".to_owned(),
        };
        FeatureParityComparison {
            sampling_key: run_id.to_string(),
            decision_at,
            stage: FeatureParityStage::FeatureCell,
            report_id: None,
            model_run_id: Some(*run_id),
            model_version_id: None,
            training_dataset_id: None,
            market_id,
            feature_name: None,
            reason: None,
            online: evidence.clone(),
            replay: evidence,
            transform_hash: None,
            detail: FeatureParityDetailSource::FeatureCell {
                feature_vector_id: FeatureVectorId::from_v7(),
            },
        }
    }

    fn feature_row(
        vector_id: &FeatureVectorId,
        market_id: &MarketId,
        decision_at: i64,
    ) -> QuantFeatureEventRow {
        QuantFeatureEventRow {
            event_time: decision_at,
            feature_vector_id: *vector_id,
            decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
            decision_at,
            knowledge_cutoff: decision_at,
            per_source_cutoffs_json: "{}".to_owned(),
            market_id: market_id.clone(),
            token_id: None,
            feature_schema_version: 1,
            feature_schema_hash: "schema".to_owned(),
            feature_hash: "feature".to_owned(),
            decision_capture_hash: "capture".to_owned(),
            feature_name: "book.best_bid".to_owned(),
            cell_state: ChFeatureCellState::Observed,
            raw_value: Some("0.5".to_owned()),
            value_kind: ChFeatureValueKind::Probability,
            source_kind: ChFeatureSourceKind::Book,
            evidence_source_kind: Some(ChFeatureSourceKind::Book),
            evidence_reference: Some("book:token".to_owned()),
            evidence_effective_at: Some(decision_at),
            evidence_available_at: None,
            reason: None,
            staleness_ms: Some(0),
            data_quality: "fresh".to_owned(),
            audit_fingerprint: "feature-fingerprint".to_owned(),
            ingestion_time: decision_at + 1,
        }
    }

    fn input_row(
        run_id: &ModelRunId,
        vector_id: &FeatureVectorId,
        market_id: &MarketId,
        decision_at: i64,
    ) -> QuantModelInputEventRow {
        QuantModelInputEventRow {
            event_time: decision_at,
            format_version: SERVING_EVIDENCE_FORMAT_VERSION,
            decision_at,
            knowledge_cutoff: decision_at,
            model_run_id: *run_id,
            model_version_id: ModelVersionId::from_v7(),
            market_id: market_id.clone(),
            feature_vector_id: *vector_id,
            model_family: "classical_logistic".to_owned(),
            raw_input_name: "book.best_bid".to_owned(),
            raw_state: "observed".to_owned(),
            raw_value: Some("0.5".to_owned()),
            encoded_column: "book.best_bid.value".to_owned(),
            encoded_value_bits: Some(0.5_f64.to_bits()),
            input_contract_hash: "contract".to_owned(),
            transform_hash: "transform".to_owned(),
            training_input_hash: "training".to_owned(),
            audit_fingerprint: "input-fingerprint".to_owned(),
            ingestion_time: decision_at + 2,
        }
    }

    struct OnlineBoundaryFixture {
        boundary: DecisionBoundary,
        completion: QuantServingEvidenceCompletionRow,
        features: HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
        persisted: HashMap<FeatureVectorId, FeatureVectorInfo>,
    }

    impl OnlineBoundaryFixture {
        fn fractional() -> Self {
            let decision_at = "2026-08-31T15:33:43.931123Z"
                .parse::<DateTime<Utc>>()
                .expect("microsecond decision time");
            let watermark = "2026-08-31T15:33:02.000017Z"
                .parse::<DateTime<Utc>>()
                .expect("microsecond finalized watermark");
            let boundary = DecisionClock::new(2)
                .serving_boundary(decision_at, 5, 300)
                .expect("canonical serving boundary")
                .with_source_watermark(DecisionSource::FinalizedExecution, watermark)
                .expect("immutable finalized watermark");
            Self::new(&boundary)
        }

        fn refresh_completion(&mut self) {
            let rows = self
                .features
                .values()
                .flatten()
                .cloned()
                .collect::<Vec<_>>();
            let commitment = feature_commitment(&rows).expect("complete feature commitment");
            self.completion = completion_marker(
                &self.completion.model_run_id,
                &self.boundary,
                &commitment,
                &[],
                self.boundary.decision_at().timestamp_millis() + 1,
            )
            .expect("refreshed complete serving evidence");
        }

        fn add_vector(
            &mut self,
            market: &str,
            token: &str,
            category: MarketCategory,
        ) -> FeatureVectorId {
            let first_id = *self.persisted.keys().next().expect("template vector");
            let vector_id = FeatureVectorId::from_v7();
            let mut info = self.persisted[&first_id].clone();
            info.feature_vector_id = vector_id;
            info.market_id = MarketId::new(market);
            info.token_id = Some(TokenId::new(token));
            let capture = &mut info.decision_capture;
            capture.identity.category = category;
            capture.snapshot.market_id = info.market_id.clone();
            capture.snapshot.token_id = TokenId::new(token);
            capture.snapshot.selection.market_id = info.market_id.clone();
            capture.snapshot.selection.category = category;
            capture.snapshot.selection.primary_token_id = TokenId::new(token);
            capture.snapshot.book_snapshot_ref.token_id = TokenId::new(token);
            info.decision_capture_hash = ResearchHasher::canonical(capture).expect("capture hash");
            let mut row = self.features[&first_id][0].clone();
            row.feature_vector_id = vector_id;
            row.market_id = info.market_id.clone();
            row.token_id = info.token_id.clone();
            row.decision_capture_hash = info.decision_capture_hash.canonical_text().to_string();
            self.features.insert(vector_id, vec![row]);
            self.persisted.insert(vector_id, info);
            self.refresh_completion();
            vector_id
        }

        fn new(boundary: &DecisionBoundary) -> Self {
            let decision_at = boundary.decision_at();
            let recommendation = report_fixtures::recommendation(
                RecommendationReportId::from_v7(),
                RecommendationId::from_v7(),
                1,
                "boundary-precision-market",
                OutcomeSide::Yes,
                Usd::ZERO,
            );
            let digest = ContentHash::from_bytes([1; 32]);
            let mut book_snapshot_ref = recommendation.evidence_refs.book_snapshot_ref;
            book_snapshot_ref.token_id = recommendation.token_id.clone();
            let capture = DecisionCaptureEvidence {
                snapshot: DecisionSnapshotEvidence {
                    boundary: boundary.clone(),
                    market_id: recommendation.market_id.clone(),
                    event_id: recommendation.event_id.clone(),
                    token_id: recommendation.token_id.clone(),
                    catalog: CatalogDecisionRef {
                        catalog_sync_batch_id: CatalogSyncBatchId::from_v7(),
                        market_change_id: CatalogMarketChangeId::from_v7(),
                        event_change_id: CatalogEventChangeId::from_v7(),
                        market_content_hash: digest,
                        event_content_hash: digest,
                        membership_hash: digest,
                        market_effective_at: boundary.cutoff_for(DecisionSource::Catalog),
                        market_available_at: decision_at,
                        event_effective_at: boundary.cutoff_for(DecisionSource::Catalog),
                        event_available_at: decision_at,
                        market_timestamp_quality: CatalogTimestampQuality::Source,
                        event_timestamp_quality: CatalogTimestampQuality::Source,
                    },
                    book_snapshot_ref,
                    book_effective_at: boundary.cutoff_for(DecisionSource::Book),
                    book_available_at: decision_at,
                    selection: SelectionMemberEvidence {
                        market_id: recommendation.market_id.clone(),
                        event_id: recommendation.event_id.clone(),
                        category: recommendation.identity.category,
                        primary_token_id: recommendation.token_id.clone(),
                        secondary_token_id: None,
                        liquidity_usd: None,
                        volume_24h_usd: None,
                        source_refs: Vec::new(),
                    },
                },
                finalized_execution_evidence: FinalizedExecutionEvidence::runtime(
                    true,
                    Some(42),
                    Some(boundary.cutoff_for(DecisionSource::FinalizedExecution)),
                ),
                identity: recommendation.identity,
                market_context: recommendation.market_context,
                data_quality: DataQualityStatus::Fresh,
                liquidity_score: Probability::ZERO,
            };
            let vector_id = FeatureVectorId::from_v7();
            let capture_hash = ResearchHasher::canonical(&capture).expect("capture commitment");
            let persisted = FeatureVectorInfo {
                feature_vector_id: vector_id,
                market_id: recommendation.market_id.clone(),
                token_id: Some(recommendation.token_id.clone()),
                decision_at,
                decision_boundary: boundary.clone(),
                feature_schema_version: SchemaVersion::FIRST,
                feature_hash: digest,
                data_quality: DataQualityStatus::Fresh,
                staleness_ms: 0,
                payload: FeatureVectorPayload {
                    generic: BTreeMap::new(),
                    domain: None,
                },
                source_refs: FeatureSourceRefs::default(),
                decision_capture: capture,
                decision_capture_hash: capture_hash,
                created_at: decision_at,
            };
            let mut row = feature_row(
                &vector_id,
                &recommendation.market_id,
                decision_at.timestamp_millis(),
            );
            row.token_id = Some(recommendation.token_id);
            row.knowledge_cutoff = boundary.knowledge_cutoff().timestamp_millis();
            row.per_source_cutoffs_json = serde_json::to_string(boundary.per_source_cutoffs())
                .expect("exact source cutoff projection");
            row.decision_capture_hash = capture_hash.canonical_text().to_string();
            let rows = vec![row];
            let evidence = feature_commitment(&rows).expect("feature commitment");
            let completion = completion_marker(
                &ModelRunId::from_v7(),
                boundary,
                &evidence,
                &[],
                decision_at.timestamp_millis() + 1,
            )
            .expect("serving completion");
            Self {
                boundary: boundary.clone(),
                completion,
                features: HashMap::from([(vector_id, rows)]),
                persisted: HashMap::from([(vector_id, persisted)]),
            }
        }
    }

    struct RoutePopulationFixture {
        online: OnlineBoundaryFixture,
        selection_id: MarketSelectionId,
        members: Vec<MarketSelectionMemberInfo>,
    }

    impl Default for RoutePopulationFixture {
        fn default() -> Self {
            let mut online = OnlineBoundaryFixture::fractional();
            let first_id = *online.persisted.keys().next().expect("first vector");
            let first = online
                .persisted
                .get_mut(&first_id)
                .expect("first PG vector");
            first.decision_capture.identity.category = MarketCategory::Crypto;
            first.decision_capture.snapshot.selection.category = MarketCategory::Crypto;
            first.decision_capture_hash =
                ResearchHasher::canonical(&first.decision_capture).expect("capture hash");
            online.features.get_mut(&first_id).expect("first CH group")[0].decision_capture_hash =
                first.decision_capture_hash.canonical_text().to_string();
            online.add_vector(
                "route-second-crypto",
                "second-crypto-token",
                MarketCategory::Crypto,
            );
            let selection_id = MarketSelectionId::from_v7();
            let mut members = online
                .persisted
                .values()
                .map(|info| MarketSelectionMemberInfo {
                    market_selection_id: selection_id,
                    market_id: info.market_id.clone(),
                    event_id: info.decision_capture.snapshot.event_id.clone(),
                    category: MarketCategory::Crypto,
                    status: MarketStatus::Active,
                    primary_token_id: info.token_id.clone().expect("bound token"),
                    secondary_token_id: None,
                    liquidity_usd: None,
                    volume_24h_usd: None,
                })
                .collect::<Vec<_>>();
            members.sort_by(|left, right| left.market_id.cmp(&right.market_id));
            members.push(MarketSelectionMemberInfo {
                market_selection_id: selection_id,
                market_id: MarketId::new("weather-dependency"),
                event_id: EventId::new("weather-event"),
                category: MarketCategory::Weather,
                status: MarketStatus::Active,
                primary_token_id: TokenId::new("weather-token"),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
            });
            Self {
                online,
                selection_id,
                members,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum PopulationFault {
        MissingRouteVector,
        ForeignRouteVector,
        SameCountReplacement,
        ForeignMarketVector,
        DuplicateGlobalMember,
        ForeignGlobalMember,
        WrongChToken,
        WrongPgToken,
        DuplicateVectorMarket,
    }

    #[test]
    fn route_population_is_exact() {
        let fixture = RoutePopulationFixture::default();
        let samples = replay_feature_population(
            &fixture.selection_id,
            &fixture.online.boundary,
            BuyModelRoute::Crypto,
            &fixture.members,
            &fixture.online.features,
            &fixture.online.persisted,
        )
        .expect("exact Route vector population within global selection");
        assert_eq!(samples.len(), 2);
        assert_eq!(
            fixture.members.len(),
            3,
            "global dependency population remains complete"
        );
        assert_eq!(
            samples
                .iter()
                .map(|sample| (&sample.market_id, &sample.token_id))
                .collect::<Vec<_>>(),
            fixture
                .members
                .iter()
                .take(2)
                .map(|member| (&member.market_id, &member.primary_token_id))
                .collect::<Vec<_>>(),
        );

        let run_id = fixture.online.completion.model_run_id;
        let at = fixture.online.boundary.decision_at();
        let candidates = fixture
            .members
            .iter()
            .map(|member| candidate(&run_id, member.market_id.as_str(), at))
            .collect::<Vec<_>>();
        let references = candidates.iter().collect::<Vec<_>>();
        let mut comparisons = fixture
            .members
            .iter()
            .map(|member| {
                let mut row = comparison(&run_id, Some(member.market_id.clone()), at);
                row.stage = FeatureParityStage::Selection;
                row
            })
            .collect::<Vec<_>>();
        comparisons.extend(
            samples
                .iter()
                .map(|sample| comparison(&run_id, Some(sample.market_id.clone()), at)),
        );
        let selected = select_comparisons(&run_id.to_string(), &references, &comparisons)
            .expect("off-Route dependency retains real selection evidence");
        let foreign = &candidates[2];
        let foreign_rows = selected
            .iter()
            .filter(|row| row.sampling_key == foreign.sampling_key)
            .collect::<Vec<_>>();
        assert_eq!(foreign_rows.len(), 1);
        assert_eq!(foreign_rows[0].stage, FeatureParityStage::Selection);
        assert_eq!(selected.len(), 5);
    }

    #[test]
    fn route_population_fails_closed() {
        for fault in [
            PopulationFault::MissingRouteVector,
            PopulationFault::ForeignRouteVector,
            PopulationFault::SameCountReplacement,
            PopulationFault::ForeignMarketVector,
            PopulationFault::DuplicateGlobalMember,
            PopulationFault::ForeignGlobalMember,
            PopulationFault::WrongChToken,
            PopulationFault::WrongPgToken,
            PopulationFault::DuplicateVectorMarket,
        ] {
            let mut fixture = RoutePopulationFixture::default();
            let id = *fixture
                .online
                .persisted
                .keys()
                .next()
                .expect("existing vector");
            match fault {
                PopulationFault::MissingRouteVector | PopulationFault::SameCountReplacement => {
                    fixture.online.features.remove(&id);
                    fixture.online.persisted.remove(&id);
                    if matches!(fault, PopulationFault::SameCountReplacement) {
                        fixture.online.add_vector(
                            "weather-dependency",
                            "weather-token",
                            MarketCategory::Weather,
                        );
                        assert_eq!(fixture.online.persisted.len(), 2);
                    }
                }
                PopulationFault::ForeignRouteVector => {
                    fixture.online.add_vector(
                        "weather-dependency",
                        "weather-token",
                        MarketCategory::Weather,
                    );
                }
                PopulationFault::ForeignMarketVector => {
                    fixture.online.add_vector(
                        "outside-selection",
                        "outside-token",
                        MarketCategory::Crypto,
                    );
                }
                PopulationFault::DuplicateGlobalMember => {
                    fixture.members.push(fixture.members[2].clone());
                }
                PopulationFault::ForeignGlobalMember => {
                    fixture.members[2].market_selection_id = MarketSelectionId::from_v7();
                }
                PopulationFault::WrongChToken => {
                    fixture.online.features.get_mut(&id).expect("CH group")[0].token_id =
                        Some(TokenId::new("wrong-token"));
                }
                PopulationFault::WrongPgToken => {
                    fixture
                        .online
                        .persisted
                        .get_mut(&id)
                        .expect("PG row")
                        .token_id = Some(TokenId::new("wrong-token"));
                }
                PopulationFault::DuplicateVectorMarket => {
                    let info = fixture.online.persisted[&id].clone();
                    fixture.online.add_vector(
                        info.market_id.as_str(),
                        info.token_id.as_ref().expect("token").as_str(),
                        MarketCategory::Crypto,
                    );
                }
            }
            let error = replay_feature_population(
                &fixture.selection_id,
                &fixture.online.boundary,
                BuyModelRoute::Crypto,
                &fixture.members,
                &fixture.online.features,
                &fixture.online.persisted,
            )
            .expect_err("Route membership corruption must not be repaired by filtering vectors");
            assert!(
                matches!(
                    &error,
                    QuantError::Research(ResearchError::Determinism { .. })
                ),
                "{fault:?}: {error}"
            );
        }
    }

    #[test]
    fn boundary_precision_watermark() {
        let decision_at = "2026-08-31T15:33:43.931Z"
            .parse::<DateTime<Utc>>()
            .expect("R7 decision time");
        let watermark = "2026-08-31T15:33:02Z"
            .parse::<DateTime<Utc>>()
            .expect("R7 finalized execution watermark");
        let boundary = DecisionClock::new(2)
            .serving_boundary(decision_at, 5, 300)
            .expect("canonical serving boundary")
            .with_source_watermark(DecisionSource::FinalizedExecution, watermark)
            .expect("exact immutable watermark");
        let fixture = OnlineBoundaryFixture::new(&boundary);
        let recovered = boundary_from_online(
            &fixture.completion,
            &[],
            &fixture.features,
            &fixture.persisted,
        )
        .expect("fractional finalized watermark must remain exact");
        assert_eq!(recovered, boundary);
        assert_eq!(
            recovered.cutoff_for(DecisionSource::FinalizedExecution),
            watermark
        );
    }

    #[test]
    fn boundary_precision_submillis() {
        let fixture = OnlineBoundaryFixture::fractional();
        let recovered = boundary_from_online(
            &fixture.completion,
            &[],
            &fixture.features,
            &fixture.persisted,
        )
        .expect("PG and JSON precision must survive the CH projection");
        assert_eq!(recovered, fixture.boundary);
        assert_eq!(
            recovered
                .cutoff_for(DecisionSource::FinalizedExecution)
                .timestamp_subsec_nanos(),
            17_000,
        );
        assert_eq!(
            recovered.decision_at().timestamp_subsec_nanos(),
            931_123_000
        );
    }

    #[test]
    fn boundary_precision_mixed_pg() {
        let mut fixture = OnlineBoundaryFixture::fractional();
        let first_id = *fixture.persisted.keys().next().expect("first vector");
        let mut second = fixture.persisted[&first_id].clone();
        // The maximal ID sorts after the first v7 ID, keeping the unmodified
        // exact boundary as the completion's canonical first vector.
        let second_id = FeatureVectorId::new(Uuid::from_u128(u128::MAX));
        let shifted = DecisionClock::new(2)
            .serving_boundary(
                fixture.boundary.decision_at() + ChronoDuration::microseconds(1),
                5,
                300,
            )
            .expect("shifted exact PG clock")
            .with_source_watermark(
                DecisionSource::FinalizedExecution,
                fixture
                    .boundary
                    .cutoff_for(DecisionSource::FinalizedExecution),
            )
            .expect("unchanged immutable execution watermark");
        assert_ne!(shifted.decision_at(), fixture.boundary.decision_at());
        assert_eq!(
            shifted.decision_at().timestamp_millis(),
            fixture.boundary.decision_at().timestamp_millis(),
        );
        second.feature_vector_id = second_id;
        second.market_id = MarketId::new("second-boundary-market");
        second.token_id = Some(TokenId::new("second-boundary-token"));
        second.decision_at = shifted.decision_at();
        second.created_at = shifted.decision_at();
        second.decision_boundary = shifted.clone();
        let capture = &mut second.decision_capture;
        capture.snapshot.boundary = shifted.clone();
        capture.snapshot.market_id = second.market_id.clone();
        capture.snapshot.selection.market_id = second.market_id.clone();
        let token = second.token_id.clone().expect("second token");
        capture.snapshot.token_id = token.clone();
        capture.snapshot.selection.primary_token_id = token.clone();
        capture.snapshot.book_snapshot_ref.token_id = token;
        capture.finalized_execution_evidence = FinalizedExecutionEvidence::runtime(
            true,
            Some(42),
            Some(shifted.cutoff_for(DecisionSource::FinalizedExecution)),
        );
        second.decision_capture_hash = ResearchHasher::canonical(capture).expect("second capture");
        let mut row = fixture.features[&first_id][0].clone();
        row.feature_vector_id = second_id;
        row.market_id = second.market_id.clone();
        row.token_id = second.token_id.clone();
        row.decision_capture_hash = second.decision_capture_hash.canonical_text().to_string();
        fixture.persisted.insert(second_id, second);
        fixture.features.insert(second_id, vec![row]);
        fixture.refresh_completion();
        let error = boundary_from_online(
            &fixture.completion,
            &[],
            &fixture.features,
            &fixture.persisted,
        )
        .expect_err("same CH millisecond must not hide different PG microseconds");
        assert!(matches!(
            &error,
            QuantError::Research(ResearchError::Determinism { .. })
        ));
        assert!(
            error.to_string().contains("Postgres feature vector"),
            "{error}"
        );
    }

    #[test]
    fn boundary_precision_json_mismatch() {
        let mut fixture = OnlineBoundaryFixture::fractional();
        let mut cutoffs = fixture.boundary.per_source_cutoffs().clone();
        let cutoff = cutoffs
            .get_mut(&DecisionSource::FinalizedExecution)
            .expect("registered source");
        let original_ms = cutoff.timestamp_millis();
        *cutoff += ChronoDuration::microseconds(1);
        assert_eq!(cutoff.timestamp_millis(), original_ms);
        fixture.features.values_mut().next().expect("feature group")[0].per_source_cutoffs_json =
            serde_json::to_string(&cutoffs).expect("changed exact source cutoff");
        let error = boundary_from_online(
            &fixture.completion,
            &[],
            &fixture.features,
            &fixture.persisted,
        )
        .expect_err("JSON must not lose a one-microsecond watermark mismatch");
        assert!(matches!(
            &error,
            QuantError::Research(ResearchError::Determinism { .. })
        ));
        assert!(error.to_string().contains("source cutoffs"), "{error}");
    }

    #[test]
    fn boundary_precision_future_source() {
        for change_persisted in [false, true] {
            let mut fixture = OnlineBoundaryFixture::fractional();
            let future = fixture.boundary.knowledge_cutoff() + ChronoDuration::microseconds(1);
            if change_persisted {
                let mut encoded = serde_json::to_value(&fixture.boundary).expect("typed boundary");
                encoded["per_source_cutoffs"]["finalized_execution"] =
                    serde_json::to_value(future).expect("future cutoff");
                fixture
                    .persisted
                    .values_mut()
                    .next()
                    .expect("PG vector")
                    .decision_boundary =
                    serde_json::from_value(encoded).expect("untrusted persisted boundary");
            } else {
                let mut cutoffs = fixture.boundary.per_source_cutoffs().clone();
                cutoffs.insert(DecisionSource::FinalizedExecution, future);
                fixture.features.values_mut().next().expect("feature group")[0]
                    .per_source_cutoffs_json =
                    serde_json::to_string(&cutoffs).expect("future JSON cutoff");
            }
            let error = boundary_from_online(
                &fixture.completion,
                &[],
                &fixture.features,
                &fixture.persisted,
            )
            .expect_err("a future source cutoff must never be clamped into acceptance");
            if change_persisted {
                assert!(matches!(&error, QuantError::Config(_)));
            } else {
                assert!(matches!(
                    &error,
                    QuantError::Research(ResearchError::Determinism { .. })
                ));
            }
            assert!(error.to_string().contains("source"), "{error}");
        }
    }

    #[test]
    fn boundary_precision_missing_source() {
        for change_persisted in [false, true] {
            let mut fixture = OnlineBoundaryFixture::fractional();
            if change_persisted {
                let incomplete = DecisionClock::new(2)
                    .boundary(fixture.boundary.decision_at())
                    .expect("general clock without serving source registrations");
                let info = fixture.persisted.values_mut().next().expect("PG vector");
                info.decision_boundary = incomplete.clone();
                info.decision_capture.snapshot.boundary = incomplete;
                fixture.features.values_mut().next().expect("feature group")[0]
                    .per_source_cutoffs_json = "{}".to_owned();
            } else {
                let mut cutoffs = fixture.boundary.per_source_cutoffs().clone();
                cutoffs.remove(&DecisionSource::DomainWeather);
                fixture.features.values_mut().next().expect("feature group")[0]
                    .per_source_cutoffs_json =
                    serde_json::to_string(&cutoffs).expect("incomplete JSON source map");
            }
            let error = boundary_from_online(
                &fixture.completion,
                &[],
                &fixture.features,
                &fixture.persisted,
            )
            .expect_err("serving replay requires all seven frozen sources");
            assert!(matches!(
                &error,
                QuantError::Research(ResearchError::Determinism { .. })
            ));
            assert!(error.to_string().contains("source"), "{error}");
        }
    }

    #[test]
    fn boundary_precision_missing_vectors() {
        for missing_persisted in [false, true] {
            let mut fixture = OnlineBoundaryFixture::fractional();
            if missing_persisted {
                fixture.persisted.clear();
            } else {
                fixture.features.clear();
            }
            let error = boundary_from_online(
                &fixture.completion,
                &[],
                &fixture.features,
                &fixture.persisted,
            )
            .expect_err("missing persisted or CH boundary population must fail closed");
            assert!(matches!(
                &error,
                QuantError::Research(ResearchError::Determinism { .. })
            ));
        }
    }

    #[test]
    fn boundary_precision_clock_mismatch() {
        for change_completion in [false, true] {
            let mut fixture = OnlineBoundaryFixture::fractional();
            if change_completion {
                fixture.completion.knowledge_cutoff += 1;
            } else {
                fixture.features.values_mut().next().expect("feature group")[0].decision_at += 1;
            }
            let error = boundary_from_online(
                &fixture.completion,
                &[],
                &fixture.features,
                &fixture.persisted,
            )
            .expect_err("CH scalar projections must match the exact PG boundary");
            assert!(matches!(
                &error,
                QuantError::Research(ResearchError::Determinism { .. })
            ));
            assert!(error.to_string().contains("clock"), "{error}");
        }
    }

    #[test]
    fn zero_inputs_complete() {
        let decision_at = DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
            .expect("millisecond decision time");
        let boundary = DecisionClock::new(0)
            .serving_boundary(decision_at, 0, 0)
            .expect("complete serving boundary");
        let fixture = OnlineBoundaryFixture::new(&boundary);
        let run_id = fixture.completion.model_run_id;
        let market = &fixture
            .persisted
            .values()
            .next()
            .expect("PG vector")
            .market_id;
        let candidate = candidate(&run_id, market.as_str(), decision_at);
        let inputs_by_run = HashMap::new();
        let inputs = validate_run_completion(
            &run_id,
            &candidate,
            &fixture.completion,
            &inputs_by_run,
            &fixture.features,
        )
        .expect("valid zero-input serving completion");
        assert!(inputs.is_empty());
        assert_eq!(
            boundary_from_online(
                &fixture.completion,
                inputs,
                &fixture.features,
                &fixture.persisted,
            )
            .expect("complete boundary with zero inputs"),
            boundary,
        );
    }

    #[test]
    fn missing_inputs_rejected() {
        let decision_at = DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
            .expect("millisecond decision time");
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("decision boundary");
        let run_id = ModelRunId::from_v7();
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("missing-input-market");
        let row = feature_row(&vector_id, &market_id, decision_at.timestamp_millis());
        let feature_rows = vec![row];
        let feature_evidence = feature_commitment(&feature_rows).expect("feature commitment");
        let expected_inputs = vec![input_row(
            &run_id,
            &vector_id,
            &market_id,
            decision_at.timestamp_millis(),
        )];
        let completion =
            completion_marker(&run_id, &boundary, &feature_evidence, &expected_inputs, 1)
                .expect("non-empty completion");
        let inputs_by_run = HashMap::new();
        let features_by_vector = HashMap::from([(vector_id, feature_rows)]);
        let candidate = candidate(&run_id, market_id.as_str(), decision_at);

        let error = validate_run_completion(
            &run_id,
            &candidate,
            &completion,
            &inputs_by_run,
            &features_by_vector,
        )
        .expect_err("missing committed inputs must fail closed");

        assert!(
            error
                .to_string()
                .contains("durable model-input evidence does not match completion marker")
        );
    }

    #[test]
    fn zero_audit_retains_route() {
        let decision_at = Utc::now().timestamp_millis();
        let selection_id = MarketSelectionId::from_v7();
        let pooled_market = MarketId::new("pooled-admitted");
        let weather_market = MarketId::new("weather-admitted");
        let rejected_market = MarketId::new("pooled-rejected");
        let pooled_vector = FeatureVectorId::from_v7();
        let weather_vector = FeatureVectorId::from_v7();
        let rejected_vector = FeatureVectorId::from_v7();
        let pooled_rows = vec![feature_row(&pooled_vector, &pooled_market, decision_at)];
        let weather_rows = vec![feature_row(&weather_vector, &weather_market, decision_at)];
        let mut rejected_rows = vec![feature_row(&rejected_vector, &rejected_market, decision_at)];
        rejected_rows[0].data_quality = DataQualityStatus::Insufficient.to_string();
        let online_features = HashMap::from([
            (pooled_vector, pooled_rows),
            (weather_vector, weather_rows),
            (rejected_vector, rejected_rows),
        ]);
        let all_vectors =
            feature_vector_binding(&online_features).expect("complete feature-vector binding");
        let members = vec![
            MarketSelectionMemberInfo {
                market_selection_id: selection_id,
                market_id: pooled_market.clone(),
                event_id: EventId::new("pooled-event"),
                category: MarketCategory::Politics,
                status: MarketStatus::Active,
                primary_token_id: TokenId::new("pooled-token"),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
            },
            MarketSelectionMemberInfo {
                market_selection_id: selection_id,
                market_id: weather_market.clone(),
                event_id: EventId::new("weather-event"),
                category: MarketCategory::Weather,
                status: MarketStatus::Active,
                primary_token_id: TokenId::new("weather-token"),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
            },
            MarketSelectionMemberInfo {
                market_selection_id: selection_id,
                market_id: rejected_market,
                event_id: EventId::new("rejected-event"),
                category: MarketCategory::Politics,
                status: MarketStatus::Active,
                primary_token_id: TokenId::new("rejected-token"),
                secondary_token_id: None,
                liquidity_usd: None,
                volume_24h_usd: None,
            },
        ];

        let route_vectors = online_route_binding(
            &all_vectors,
            &online_features,
            &members,
            BuyModelRoute::Pooled,
        )
        .expect("pooled admitted route binding");

        assert_eq!(route_vectors.len(), 1);
        assert_eq!(route_vectors.get(&pooled_market), Some(&pooled_vector));
        validate_input_population(&HashMap::new(), &route_vectors)
            .expect("zero audit rows are valid for an admitted route population");
        assert!(
            validate_input_population(
                &HashMap::from([(weather_market, weather_vector)]),
                &route_vectors,
            )
            .is_err()
        );
    }

    #[test]
    fn missing_completion_marks_pending() {
        let run_id = ModelRunId::from_v7();
        let decision_at = Utc::now();
        let first = candidate(&run_id, "0xfirst", decision_at);
        let second = candidate(&run_id, "0xsecond", decision_at);
        let candidates = vec![&first, &second];

        let pending = pending_completion(&run_id, &candidates);

        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].sampling_key, first.sampling_key);
        assert_eq!(pending[0].market_id, first.market_id);
        assert_eq!(pending[1].sampling_key, second.sampling_key);
        assert_eq!(pending[1].market_id, second.market_id);
        assert!(pending.iter().all(|row| {
            row.reason == "serving_evidence_completion_missing"
                && row.model_run_id.as_ref() == Some(&run_id)
        }));
    }

    struct InputWitnessFixture {
        online: OnlineBoundaryFixture,
        run: ModelRunInfo,
        candidate: FeatureParityCandidate,
        input: QuantModelInputEventRow,
    }

    impl InputWitnessFixture {
        fn new() -> Self {
            let online = OnlineBoundaryFixture::fractional();
            let vector = online.persisted.values().next().expect("committed vector");
            let decision_at = online.boundary.decision_at();
            let run_id = online.completion.model_run_id;
            let mut input = input_row(
                &run_id,
                &vector.feature_vector_id,
                &vector.market_id,
                decision_at.timestamp_millis(),
            );
            let digest = ContentHash::from_bytes([7; 32]);
            input.knowledge_cutoff = online.boundary.knowledge_cutoff().timestamp_millis();
            input.transform_hash = digest.canonical_text().to_string();
            let mut candidate = candidate(&run_id, vector.market_id.as_str(), decision_at);
            candidate.input_witness = FeatureParityInputWitness::VerifiedModelInput {
                feature_vector_id: vector.feature_vector_id,
            };
            let run = ModelRunInfo {
                model_run_id: run_id,
                run_kind: ModelRunKind::LiveInference,
                model_version_id: Some(input.model_version_id),
                decision_policy_snapshot_id: online.features[&vector.feature_vector_id][0]
                    .decision_policy_snapshot_id,
                market_selection_id: Some(MarketSelectionId::from_v7()),
                window_start: decision_at,
                window_end: decision_at + ChronoDuration::milliseconds(1),
                status: ModelRunStatus::Succeeded,
                input_hash: digest,
                output_hash: Some(digest),
                error_code: None,
                error_message: None,
                started_at: decision_at,
                finished_at: Some(decision_at + ChronoDuration::milliseconds(1)),
            };
            Self {
                online,
                run,
                candidate,
                input,
            }
        }
    }

    #[test]
    fn pending_requires_rediscovery() {
        let fixture = InputWitnessFixture::new();
        let inputs = vec![fixture.input.clone()];
        let rows = fixture
            .online
            .features
            .values()
            .flatten()
            .cloned()
            .collect::<Vec<_>>();
        let commitment = feature_commitment(&rows).expect("actual feature commitment");
        let completion = completion_marker(
            &fixture.run.model_run_id,
            &fixture.online.boundary,
            &commitment,
            &inputs,
            fixture.input.ingestion_time,
        )
        .expect("writer became complete after discovery");
        let input_groups = HashMap::from([(fixture.run.model_run_id, inputs)]);
        validate_run_completion(
            &fixture.run.model_run_id,
            &fixture.candidate,
            &completion,
            &input_groups,
            &fixture.online.features,
        )
        .expect("fresh writer evidence is already complete");

        let mut pending = fixture.candidate.clone();
        pending.input_witness = FeatureParityInputWitness::PendingServingEvidence;
        validate_witness_states(slice::from_ref(&pending)).expect("all-pending discovery");
        let (ready, deferred) = partition_witness_candidates(slice::from_ref(&pending))
            .expect("pending snapshot partitions before any evidence load");
        assert!(ready.is_empty());
        assert_eq!(deferred.len(), 1);
        assert_eq!(deferred[0].sampling_key, pending.sampling_key);
        assert_eq!(deferred[0].reason, "serving_evidence_completion_missing");
        assert!(
            validate_replay_witnesses(&fixture.run, &[&pending], slice::from_ref(&fixture.input))
                .is_err()
        );

        let (ready, deferred) = partition_witness_candidates(slice::from_ref(&fixture.candidate))
            .expect("next attempt re-discovered the committed input");
        assert_eq!(ready.len(), 1);
        assert!(deferred.is_empty());
        validate_replay_witnesses(&fixture.run, &[&ready[0]], slice::from_ref(&fixture.input))
            .expect("verified retry uses the exact committed binding");
    }

    #[test]
    fn witness_states_are_scoped() {
        let fixture = InputWitnessFixture::new();
        let mut pending = fixture.candidate.clone();
        pending.input_witness = FeatureParityInputWitness::PendingServingEvidence;
        assert!(validate_witness_states(&[pending.clone(), fixture.candidate.clone()]).is_err());
        pending.subject = FeatureParitySubject::ModelRun(ModelRunId::from_v7());
        validate_witness_states(&[pending, fixture.candidate.clone()])
            .expect("different model subjects retain independent qualification states");

        let mut report = fixture.candidate.clone();
        report.subject =
            FeatureParitySubject::RecommendationReport(RecommendationReportId::from_v7());
        for witness in [
            fixture.candidate.input_witness,
            FeatureParityInputWitness::PendingServingEvidence,
        ] {
            report.input_witness = witness;
            assert!(validate_witness_states(slice::from_ref(&report)).is_err());
            assert!(partition_witness_candidates(slice::from_ref(&report)).is_err());
        }
        report.input_witness = FeatureParityInputWitness::SelectionOnly;
        validate_witness_states(slice::from_ref(&report)).expect("report selection remains valid");
    }

    #[test]
    fn input_binding_is_bidirectional() {
        let fixture = InputWitnessFixture::new();
        let inputs = slice::from_ref(&fixture.input);
        validate_replay_witnesses(&fixture.run, &[&fixture.candidate], inputs)
            .expect("exact verified model/market/vector witness");
        for case in 0..6 {
            let mut candidate = fixture.candidate.clone();
            match case {
                0 => {
                    candidate.input_witness = FeatureParityInputWitness::VerifiedModelInput {
                        feature_vector_id: FeatureVectorId::from_v7(),
                    }
                }
                1 => candidate.input_witness = FeatureParityInputWitness::SelectionOnly,
                2 => candidate.input_witness = FeatureParityInputWitness::PendingServingEvidence,
                3 => candidate.subject = FeatureParitySubject::ModelRun(ModelRunId::from_v7()),
                4 => candidate.market_id = Some(MarketId::new("outside-frozen-input")),
                5 => candidate.market_id = None,
                _ => unreachable!("bounded witness mutation matrix"),
            }
            let error = validate_replay_witnesses(&fixture.run, &[&candidate], inputs)
                .expect_err("qualified input binding drift must fail closed");
            assert!(
                error.to_string().contains("input witness differs"),
                "case {case}: {error}"
            );
        }
        assert!(validate_replay_witnesses(&fixture.run, &[&fixture.candidate], &[]).is_err());
        let mut selection_only = fixture.candidate;
        selection_only.input_witness = FeatureParityInputWitness::SelectionOnly;
        validate_replay_witnesses(&fixture.run, &[&selection_only], &[])
            .expect("no-input selection evidence never claims a model input");
    }

    #[test]
    fn input_identity_fails_closed() {
        let fixture = InputWitnessFixture::new();
        for case in 0..3 {
            let mut input = fixture.input.clone();
            match case {
                0 => input.model_run_id = ModelRunId::from_v7(),
                1 => input.model_version_id = ModelVersionId::from_v7(),
                2 => input.transform_hash = "missing-transform".to_owned(),
                _ => unreachable!("bounded input identity matrix"),
            }
            let error = verified_input_bindings(&fixture.run, &[input])
                .expect_err("foreign or unverifiable input cannot qualify");
            assert!(
                error.to_string().contains("input witness"),
                "case {case}: {error}"
            );
        }
        let mut missing_version = fixture.run.clone();
        missing_version.model_version_id = None;
        assert!(
            verified_input_bindings(&missing_version, slice::from_ref(&fixture.input)).is_err()
        );
        let mut conflicting = fixture.input.clone();
        conflicting.feature_vector_id = FeatureVectorId::from_v7();
        let error = verified_input_bindings(&fixture.run, &[fixture.input, conflicting])
            .expect_err("one market cannot qualify two feature vectors");
        assert!(error.to_string().contains("multiple feature vectors"));
    }

    #[test]
    fn empty_returns_error_indexing() {
        let run_id = ModelRunId::from_v7();
        assert!(representative_candidate(&run_id.to_string(), &[]).is_err());
    }

    #[test]
    fn replay_projects_market_once() {
        let run_id = ModelRunId::from_v7();
        let decision_at = Utc::now();
        let first = candidate(&run_id, "0xfirst", decision_at);
        let second = candidate(&run_id, "0xsecond", decision_at);
        let candidates = vec![&first, &second];
        let comparisons = vec![
            comparison(&run_id, first.market_id.clone(), decision_at),
            comparison(&run_id, second.market_id.clone(), decision_at),
            comparison(&run_id, None, decision_at),
        ];

        let selected =
            select_comparisons(&run_id.to_string(), &candidates, &comparisons).expect("selection");

        assert_eq!(selected.len(), 3);
        assert_eq!(
            selected
                .iter()
                .filter(|row| row.sampling_key == first.sampling_key)
                .count(),
            2
        );
        assert_eq!(
            selected
                .iter()
                .filter(|row| row.sampling_key == second.sampling_key)
                .count(),
            1
        );
        assert_eq!(
            selected
                .iter()
                .filter(|row| row.market_id.is_none())
                .count(),
            1
        );
    }

    #[test]
    fn replay_uses_route_scope() {
        let factors = FactorsConfig::default();
        let features = FeaturesConfig::default();
        let domain = DomainConfig::default();
        let scoped_engine = FactorEngine::for_model_scope(
            &factors,
            &features,
            &domain,
            ResearchFeatureContract::FullL2Weather,
            Some(MarketCategory::Weather),
            None,
        );
        let scoped = scoped_engine
            .serving_plane()
            .expect("weather replay factor plane");
        let unscoped_engine = FactorEngine::new(&factors, &features, &domain, None);
        let unscoped = unscoped_engine
            .serving_plane()
            .expect("unscoped factor plane");
        let families = scoped
            .definitions()
            .iter()
            .map(|definition| definition.definition().family)
            .collect::<Vec<_>>();

        assert!(families.contains(&FactorFamily::DomainWeather));
        assert!(!families.contains(&FactorFamily::DomainCrypto));
        assert_ne!(scoped.factor_schema_hash(), unscoped.factor_schema_hash());
    }

    #[test]
    fn global_report_requires_quality() {
        let report_id = RecommendationReportId::from_v7();
        let report = report_fixtures::report(
            report_id,
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        let member = MarketSelectionMemberInfo {
            market_selection_id: report.market_selection_id,
            market_id: MarketId::new("0xreal"),
            event_id: EventId::new("event-real"),
            category: MarketCategory::Politics,
            status: MarketStatus::Active,
            primary_token_id: TokenId::new("token-real"),
            secondary_token_id: None,
            liquidity_usd: Some(Usd::new(dec!(10))),
            volume_24h_usd: None,
        };
        let mut dq = ReportDataQualitySnapshotInfo {
            report_data_quality_snapshot_id: report.data_quality_snapshot_ref,
            decision_at: report.decision_at,
            decision_policy_snapshot_id: report.decision_policy_snapshot_id,
            tokens_json: ReportDataQualityTokens(Vec::new()),
            created_at: report.created_at,
        };
        validate_quality_evidence(&report, slice::from_ref(&member), &dq)
            .expect("selection evidence may precede feature materialization");

        let feature_vector_id = FeatureVectorId::from_v7();
        dq.tokens_json = ReportDataQualityTokens(vec![TokenDataQualityRecord {
            feature_vector_id,
            token_id: member.primary_token_id.clone(),
            market_id: member.market_id.clone(),
            status: DataQualityStatus::Fresh,
            book_age_ms: 0,
            crossed: false,
            empty: true,
            fact_lag_ms: None,
            missing_required: vec!["book.best_ask".to_owned()],
        }]);
        validate_quality_evidence(&report, slice::from_ref(&member), &dq)
            .expect("bound global DQ evidence");

        dq.tokens_json.0[0].market_id = MarketId::new("outside-selection");
        assert!(validate_quality_evidence(&report, &[member], &dq).is_err());
    }

    #[test]
    fn orchestration_excludes_compute_calls() {
        let source = include_str!("durable_feature_parity.rs");
        let replay = source
            .split("async fn replay(")
            .nth(2)
            .and_then(|tail| tail.split("impl DurableFeatureParitySource").next())
            .expect("replay source");
        let pages = source
            .split("async fn replay_pages")
            .nth(1)
            .and_then(|tail| tail.split("async fn frozen_candidates").next())
            .expect("replay pages source");
        for orchestration in [replay, pages] {
            assert!(!orchestration.contains("run_compute("));
            assert!(!orchestration.contains("block_on("));
            assert!(!orchestration.contains("run_offline"));
        }
        let scoped = ["run_offline", "_scoped"].concat();
        assert!(!source.contains(&scoped));
    }
}
