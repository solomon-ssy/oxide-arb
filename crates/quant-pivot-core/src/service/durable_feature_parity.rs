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
        data_plane::{DecisionBoundary, DecisionClock, DecisionSource},
        quant::{
            FactorValueInfo, FeatureParityRunInfo, FeatureVectorInfo, FrozenFeatureParitySubject,
            FrozenFeatureParitySubjectId, MarketSelectionInfo, MarketSelectionMemberInfo,
            ModelRunInfo, ModelRunParityEvidence, ModelVersionInfo, RecommendationReportInfo,
            ReportDataQualitySnapshotInfo, ReportRouteRunInfo, ReportRunInfo, RepresentedRouteSet,
            parity_candidate_membership_hash, parity_selection_hash, report_parity_evidence_hash,
            report_parity_generation_hash,
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
        FeatureVectorId, FinalizedExecutionEvidence, MarketId, MarketSelectionId, ModelRunId,
        ModelVersionId, RecommendationReportId, SelectionExclusionSummary, SelectorHashEvidence,
        SelectorParityEvidence, Usd, factor::FactorServingPlane, stable_name::FeatureName,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    FactorRepository, FeatureParityRepository, FeatureRepository, MarketLinkageRepository,
    MarketSelectionRepository, ModelRunRepository, PolicyRepository, QuantFactReadRepository,
    RecommendationReportRepository, ReportRunRepository, ServingEvidenceRepository,
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
        MarketSelector, ModelFeatureRequirements, SelectedMarket,
    },
};
use serde::Serialize;
use tokio::{runtime::Handle, sync::Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    observability::serving_evidence::{ModelInputEvidenceBatch, verify_completion},
    pit::platform::ch_historical::DurablePitSource,
    prefetch::{
        historical_window::{HistoricalWindowLoader, ReplaySample, WindowSpec},
        market_candidates::{DecisionSnapshotSource, MarketCandidateProvider},
    },
    projection::inference_batch::build_runtime_input,
    service::{
        bias_table_fit::resolve_frozen_bias_table,
        feature_parity_executor::{
            FeatureParityCandidate, FeatureParityComparison, FeatureParityEvidence,
            FeatureParityReplayAttempt, FeatureParityReplaySource, FeatureParitySubject,
            PendingFeatureParityComparison,
        },
        historical_replay::{
            CrossSectionRequest, ReplayCaptureKey, ReplayConfig, ReplayCrossSection,
            ReplayExecutionSource, ReplayFactorMode, materialize_cross_section,
        },
        model_serving_generation::{
            ModelServingGenerationRequest, ModelServingGenerationStore, ModelServingRouteSnapshot,
        },
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
    pub compute: Arc<ComputeExecutor>,
    pub compute_budget: FeatureParityComputeConfig,
}

/// Replays successful serving runs from durable PIT data and frozen artifacts.
#[derive(Clone)]
pub struct DurableFeatureParitySource {
    deps: DurableFeatureParityDeps,
    compute_memory: OfflineMemory,
    compute_slots: Arc<Semaphore>,
}

struct DurableReplayEvidence {
    completion_by_run: HashMap<ModelRunId, QuantServingEvidenceCompletionRow>,
    inputs_by_run: HashMap<ModelRunId, Vec<QuantModelInputEventRow>>,
    features_by_vector: HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    info_by_id: HashMap<FeatureVectorId, FeatureVectorInfo>,
}

struct ReplayRunContext {
    model_version_id: ModelVersionId,
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    config: DecisionPolicySnapshot,
    boundary: DecisionBoundary,
    report_id: Option<RecommendationReportId>,
    represented_routes: Option<RepresentedRouteSet>,
    selection: MarketSelectionInfo,
    members: Vec<MarketSelectionMemberInfo>,
    samples: Vec<ReplaySample>,
    finalized_execution_evidences: HashMap<MarketId, FinalizedExecutionEvidence>,
}

struct ReportReplayContext {
    decision_policy_snapshot_id: DecisionPolicySnapshotId,
    snapshot_hash: ContentHash,
    config: DecisionPolicySnapshot,
    boundary: DecisionBoundary,
    represented_routes: RepresentedRouteSet,
    selection: MarketSelectionInfo,
    members: Vec<MarketSelectionMemberInfo>,
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
    snapshot_source: Arc<DecisionSnapshotSource>,
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
        Ok(Self {
            compute_memory: OfflineMemory::try_bytes(working_set)?,
            compute_slots: Arc::new(Semaphore::new(budget.max_concurrency)),
            deps,
        })
    }
}

#[async_trait]
impl FeatureParityReplaySource for DurableFeatureParitySource {
    async fn list_candidates(
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

    async fn replay(
        &self,
        parity_run: &FeatureParityRunInfo,
        candidates: &[FeatureParityCandidate],
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityReplayAttempt> {
        let bounded_cancel = cancel.child_token();
        let replay = self.replay_governed(parity_run, candidates, &bounded_cancel);
        tokio::time::timeout(
            Duration::from_secs(self.deps.compute_budget.deadline_secs),
            replay,
        )
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
    async fn replay_governed(
        &self,
        parity_run: &FeatureParityRunInfo,
        candidates: &[FeatureParityCandidate],
        cancel: &CancellationToken,
    ) -> QuantResult<FeatureParityReplayAttempt> {
        let permit = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(ResearchError::Cancelled {
                    detail: "cancelled while waiting for feature parity compute capacity"
                        .to_owned(),
                }
                .into());
            }
            permit = Arc::clone(&self.compute_slots).acquire_owned() => {
                permit.map_err(|_| InfraError::ComputeExecution {
                    detail: "feature parity compute semaphore closed".to_owned(),
                })?
            }
        };
        let source = self.clone();
        let parity_run = parity_run.clone();
        let candidates = candidates.to_vec();
        let work_cancel = cancel.clone();
        let runtime = Handle::current();
        self.deps
            .compute
            .run_offline_cancellable(self.compute_memory, cancel, move || {
                let _permit = permit;
                runtime.block_on(async move {
                    tokio::select! {
                        biased;
                        () = work_cancel.cancelled() => Err(ResearchError::Cancelled {
                            detail: format!(
                                "feature parity replay {} cancelled inside governed compute",
                                parity_run.run_id
                            ),
                        }
                        .into()),
                        result = source.replay_pages(&candidates, &work_cancel) => result,
                    }
                })
            })
            .await
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
            let run_ids = unique_run_ids(page);
            let evidence = self.load_replay_evidence(&run_ids).await?;
            let mut attempt = self.replay_candidate_groups(page, &evidence).await?;
            let report_attempt = self.replay_report_candidate_groups(page).await?;
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
                });
                continue;
            }
            for candidate in subject.candidates {
                candidates.push(FeatureParityCandidate {
                    sampling_key: format!("{subject_label}/{}", candidate.market_id),
                    subject: owner.clone(),
                    market_id: Some(candidate.market_id),
                    decision_at,
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
                let route_runs = self.deps.reports.list_route_runs(report_id).await?;
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
    ) -> QuantResult<DurableReplayEvidence> {
        let completions = dedupe_completions(
            self.deps
                .serving_evidence
                .completions_for_runs(run_ids)
                .await?,
        )?;
        let completion_by_run = completions
            .into_iter()
            .map(|row| (row.model_run_id, row))
            .collect::<HashMap<_, _>>();
        let online_inputs = dedupe_model_input_rows(
            self.deps
                .serving_evidence
                .model_inputs_for_runs(run_ids)
                .await?,
        )?;
        let feature_ids = completion_by_run
            .values()
            .map(completion_vector_ids)
            .collect::<QuantResult<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let online_features = dedupe_feature_rows(
            self.deps
                .serving_evidence
                .feature_cells_for_vectors(&feature_ids)
                .await?,
        )?;
        let feature_infos = self.deps.feature_vectors.find_by_ids(&feature_ids).await?;
        Ok(DurableReplayEvidence {
            completion_by_run,
            inputs_by_run: group_model_inputs(online_inputs),
            features_by_vector: group_feature_rows(online_features),
            info_by_id: feature_infos
                .into_iter()
                .map(|info| (info.feature_vector_id, info))
                .collect(),
        })
    }

    async fn replay_candidate_groups(
        &self,
        candidates: &[FeatureParityCandidate],
        evidence: &DurableReplayEvidence,
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
                attempt
                    .pending
                    .extend(pending_completion(&run_id, &run_candidates));
                continue;
            };
            let run_inputs = validate_run_completion(
                &run_id,
                candidate,
                completion,
                &evidence.inputs_by_run,
                &evidence.features_by_vector,
            )?;
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
            let comparisons = self
                .replay_run(
                    candidate,
                    &run,
                    completion,
                    run_inputs,
                    &run_features,
                    &run_infos,
                )
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
        let config = config_info.snapshot;
        let expected_boundary = report_decision_boundary(&report, &report_run, &config)?;
        validate_report_selection_binding(&report, &selection)?;
        Ok(Box::new(PreparedReportReplay {
            report_id: *report_id,
            context: ReportReplayContext {
                decision_policy_snapshot_id: report.decision_policy_snapshot_id,
                snapshot_hash,
                config,
                boundary: expected_boundary,
                represented_routes: report.represented_routes_json,
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
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let context = &prepared.context;
        let subject = ComparisonSubject {
            report: Some(&prepared.report_id),
            model_run: None,
            model_version: None,
        };
        let replay = self.materialize_report_selection(context).await?;
        selection_comparisons(
            candidate,
            subject,
            &context.selection,
            &context.members,
            &replay,
            &context.boundary,
        )
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
        let boundary = boundary_from_online(completion, online_inputs, online_features)?;
        if boundary.decision_at() != candidate.decision_at {
            return Err(determinism(format!(
                "serving evidence boundary {} does not match candidate {}",
                boundary.decision_at(),
                candidate.decision_at
            )));
        }
        let (report_id, represented_routes) = if let Some(route_run) = self
            .deps
            .reports
            .find_model_route_run(&run.model_run_id)
            .await?
        {
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
            validate_route_run_binding(&report, &route_run, run)?;
            (
                Some(report.recommendation_report_id),
                Some(report.represented_routes_json),
            )
        } else {
            (None, None)
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
            represented_routes,
            selection,
            members,
            samples,
            finalized_execution_evidences,
        })
    }

    async fn materialize_run_replay(
        &self,
        subject: &str,
        context: &ReplayRunContext,
    ) -> QuantResult<MaterializedRunReplay> {
        let config = &context.config;
        let boundary = &context.boundary;
        let selection_replay = self.materialize_selection_replay(context).await?;
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
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.catalog),
            Arc::clone(&self.deps.clob_market_info),
            Arc::clone(&self.deps.linkages),
            Arc::clone(&self.deps.calibration_artifacts),
            Duration::from_millis(
                config
                    .profile_artifacts
                    .research_method
                    .training
                    .max_book_staleness_ms,
            ),
        );
        let window_end = boundary
            .decision_at()
            .checked_add_signed(ChronoDuration::milliseconds(1))
            .ok_or_else(|| determinism("parity window end is outside chrono range".to_owned()))?;
        let window = loader
            .load(&WindowSpec {
                window_start: boundary.decision_at(),
                window_end,
                available_by: window_end,
                samples: samples.clone(),
                lookback: prefetch_lookback,
                knowledge_lag: boundary.knowledge_lag(),
                max_horizon_secs: 0,
                domain: config.profile_artifacts.domain.definition.clone(),
                feature_contract: selection_replay.replay_config.feature_contract,
            })
            .await?;
        let cross = materialize_cross_section(
            &selection_replay.builder,
            ReplayFactorMode::FactorNative {
                engine: &selection_replay.factor_engine,
            },
            &selection_replay.replay_config,
            &CrossSectionRequest {
                pit: selection_replay.snapshot_source.as_ref(),
                prefetched: &window.prefetched,
                finalized_execution_evidence: ReplayExecutionSource::FrozenRuntime(
                    &context.finalized_execution_evidences,
                ),
                decision_at: boundary.decision_at(),
                group: &samples,
                required_features: &selection_replay.required_features,
                category_scope: None,
                knowledge_lag: boundary.knowledge_lag(),
            },
        )
        .await?
        .ok_or_else(|| {
            determinism(format!(
                "durable replay resolved no catalog rows for {subject}"
            ))
        })?;
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
    ) -> QuantResult<MaterializedSelectionReplay> {
        let config = &context.config;
        let boundary = &context.boundary;
        let bias_table = resolve_frozen_bias_table(
            self.deps.calibration_artifacts.as_ref(),
            &config.profile_artifacts.scoring.definition,
        )
        .await?;
        let serving = self.replay_serving(context).await?;
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
        let model_requirements = if let Some(represented_routes) = &context.represented_routes {
            self.route_requirements(
                context.decision_policy_snapshot_id,
                context.snapshot_hash,
                config,
                represented_routes,
            )
            .await?
        } else {
            serving.model_requirements()
        };
        let required_features = model_requirements.union_all();
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
        let selection = ConfiguredMarketSelector::new()
            .build_snapshot(
                MarketSelectionBuildRequest {
                    decision_at: boundary.decision_at(),
                    decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                    selection: config.recommendation.selection.clone(),
                    data_quality: config.recommendation.data_quality.clone(),
                    features: config.profile_artifacts.features.definition.clone(),
                    model_requirements,
                    knowledge_lag_secs: boundary.knowledge_lag_secs(),
                },
                candidate_batch.candidates,
            )
            .await?;
        Ok(MaterializedSelectionReplay {
            builder,
            factor_engine,
            bias_table_hash,
            selection,
            snapshot_source: candidate_batch.snapshot_source,
            replay_config,
            required_features,
            serving,
        })
    }

    async fn materialize_report_selection(
        &self,
        context: &ReportReplayContext,
    ) -> QuantResult<MarketSelectionSnapshot> {
        let model_requirements = self
            .route_requirements(
                context.decision_policy_snapshot_id,
                context.snapshot_hash,
                &context.config,
                &context.represented_routes,
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
        ConfiguredMarketSelector::new()
            .build_snapshot(
                MarketSelectionBuildRequest {
                    decision_at: context.boundary.decision_at(),
                    decision_policy_snapshot_id: context.decision_policy_snapshot_id,
                    selection: context.config.recommendation.selection.clone(),
                    data_quality: context.config.recommendation.data_quality.clone(),
                    features: context.config.profile_artifacts.features.definition.clone(),
                    model_requirements,
                    knowledge_lag_secs: context.boundary.knowledge_lag_secs(),
                },
                candidates.candidates,
            )
            .await
    }

    async fn route_requirements(
        &self,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        snapshot_hash: ContentHash,
        config: &DecisionPolicySnapshot,
        represented_routes: &RepresentedRouteSet,
    ) -> QuantResult<ModelFeatureRequirements> {
        let mut model_requirements = ModelFeatureRequirements::default();
        let serving_routes = self
            .deps
            .serving_generations
            .resolve_routes(
                ModelServingGenerationRequest {
                    decision_policy_snapshot_id,
                    snapshot_hash,
                    snapshot: config,
                },
                represented_routes,
            )
            .await?;
        for serving in serving_routes {
            model_requirements.merge(serving.model_requirements());
        }
        Ok(model_requirements)
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
        candidate: &FeatureParityCandidate,
        run: &ModelRunInfo,
        completion: &QuantServingEvidenceCompletionRow,
        online_inputs: &[QuantModelInputEventRow],
        online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
        feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
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
        let replay = self
            .materialize_run_replay(&format!("serving run {}", run.model_run_id), &context)
            .await?;

        let model_vector_binding = vector_binding(online_inputs)?;
        let all_vector_binding = feature_vector_binding(online_features)?;
        let route_vector_binding = online_route_binding(
            &all_vector_binding,
            online_features,
            &context.members,
            replay.serving.route(),
        )?;
        validate_input_population(&model_vector_binding, &route_vector_binding)?;
        let replay_by_market = replay.cross_section.replay_vectors_by_market();
        let comparison_subject = ComparisonSubject {
            report: context.report_id.as_ref(),
            model_run: Some(&run.model_run_id),
            model_version: Some(&context.model_version_id),
        };
        let mut comparisons = Vec::new();
        comparisons.extend(selection_comparisons(
            candidate,
            comparison_subject,
            &context.selection,
            &context.members,
            &replay.selection,
            &context.boundary,
        )?);
        comparisons.extend(snapshot_and_feature_comparisons(
            candidate,
            comparison_subject,
            FeatureComparisonInputs {
                online_features,
                feature_infos,
                replay_by_market: &replay_by_market,
                replay_captures: &replay.cross_section.captures,
                vector_binding: &all_vector_binding,
                boundary: &context.boundary,
                decision_policy_snapshot_id: &run.decision_policy_snapshot_id,
                schema: replay.builder.schema(),
            },
        )?);

        let admission_matches = route_admission_matches(
            &route_vector_binding,
            &replay.cross_section,
            replay.serving.route(),
        );
        comparisons.push(data_quality_comparison(
            candidate,
            comparison_subject,
            online_features,
            &replay_by_market,
            &context.boundary,
        )?);
        if !admission_matches {
            return Ok(comparisons);
        }

        let replay_outputs = self
            .replay_model_routes(ModelRouteReplayRequest {
                run,
                config: &context.config,
                boundary: &context.boundary,
                online_inputs,
                markets: &replay.cross_section.markets,
                vectors: &replay.cross_section.vectors,
                vector_binding: &route_vector_binding,
                factor_engine: &replay.factor_engine,
                bias_table_hash: replay.bias_table_hash,
                serving: &replay.serving,
            })
            .await?;
        comparisons.extend(model_input_comparisons(
            candidate,
            &run.model_run_id,
            context.report_id.as_ref(),
            online_inputs,
            &replay_outputs.input_rows,
        )?);
        comparisons.extend(
            self.factor_comparisons(
                candidate,
                run,
                context.report_id.as_ref(),
                &replay_outputs.factor_outcomes,
                &context.boundary,
            )
            .await?,
        );
        comparisons.push(prediction_comparison(
            candidate,
            run,
            context.report_id,
            &replay_outputs.runtime_output,
            &context.boundary,
        )?);
        Ok(comparisons)
    }

    async fn replay_model_routes(
        &self,
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

    async fn factor_comparisons(
        &self,
        candidate: &FeatureParityCandidate,
        run: &ModelRunInfo,
        report_id: Option<&RecommendationReportId>,
        replay_outcomes: &[MarketFactorOutcome],
        boundary: &DecisionBoundary,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let online = self
            .deps
            .factors
            .list_values_for_run(&run.model_run_id)
            .await?;
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
            .iter()
            .map(FactorProjection::from_online)
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
    if samples_by_market.len() != member_by_market.len() {
        return Err(determinism(format!(
            "selection {selection_id} members do not match its committed feature-vector population"
        )));
    }
    members
        .iter()
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
) -> QuantResult<DecisionBoundary> {
    let decision_at = required_millis(completion.decision_at, "serving completion decision_at")?;
    let knowledge_cutoff = required_millis(
        completion.knowledge_cutoff,
        "serving completion knowledge_cutoff",
    )?;
    let lag_ms = decision_at
        .signed_duration_since(knowledge_cutoff)
        .num_milliseconds();
    if lag_ms < 0 || lag_ms % 1_000 != 0 {
        return Err(determinism(format!(
            "model input boundary has invalid whole-second lag {lag_ms}ms"
        )));
    }
    let lag_secs = u64::try_from(lag_ms / 1_000)
        .map_err(|error| determinism(format!("knowledge lag conversion failed: {error}")))?;
    let mut boundary = DecisionClock::new(lag_secs).boundary(decision_at)?;
    let first_vector_id = completion_vector_ids(completion)?
        .into_iter()
        .next()
        .ok_or_else(|| determinism("serving completion has no feature vectors".to_owned()))?;
    let feature = features
        .get(&first_vector_id)
        .and_then(|rows| rows.first())
        .ok_or_else(|| determinism(format!("no feature boundary for vector {first_vector_id}")))?;
    let cutoffs: BTreeMap<DecisionSource, DateTime<Utc>> =
        serde_json::from_str(&feature.per_source_cutoffs_json).map_err(|error| {
            ResearchError::Determinism {
                detail: format!("invalid serving per-source cutoffs: {error}"),
            }
        })?;
    for (source, cutoff) in cutoffs {
        let source_lag_ms = decision_at.signed_duration_since(cutoff).num_milliseconds();
        if source_lag_ms < 0 || source_lag_ms % 1_000 != 0 {
            return Err(determinism(format!(
                "source {source:?} cutoff has invalid lag {source_lag_ms}ms"
            )));
        }
        let source_lag_secs = u64::try_from(source_lag_ms / 1_000)
            .map_err(|error| determinism(format!("source cutoff conversion failed: {error}")))?;
        boundary = boundary.with_source_cutoff(source, source_lag_secs)?;
    }
    for row in inputs {
        if row.decision_at != completion.decision_at
            || row.knowledge_cutoff != completion.knowledge_cutoff
        {
            return Err(determinism(format!(
                "model input run {} contains multiple decision boundaries",
                completion.model_run_id
            )));
        }
    }
    let expected_source_cutoffs = &feature.per_source_cutoffs_json;
    for rows in features.values() {
        for row in rows {
            if row.decision_at != completion.decision_at
                || row.knowledge_cutoff != completion.knowledge_cutoff
                || &row.per_source_cutoffs_json != expected_source_cutoffs
            {
                return Err(determinism(format!(
                    "feature vector {} contains a boundary inconsistent with model run {}",
                    row.feature_vector_id, completion.model_run_id
                )));
            }
        }
    }
    Ok(boundary)
}

pub(crate) fn report_decision_boundary(
    report: &RecommendationReportInfo,
    run: &ReportRunInfo,
    config: &DecisionPolicySnapshot,
) -> QuantResult<DecisionBoundary> {
    let persisted_lag = run.knowledge_lag_secs.ok_or_else(|| {
        determinism(format!(
            "report run {} has no frozen knowledge lag",
            run.report_run_id
        ))
    })?;
    let knowledge_lag_secs = u64::try_from(persisted_lag).map_err(|error| {
        determinism(format!(
            "report run {} has invalid knowledge lag {}: {error}",
            run.report_run_id, persisted_lag
        ))
    })?;
    DecisionClock::new(knowledge_lag_secs).serving_boundary(
        report.decision_at,
        config
            .profile_artifacts
            .domain
            .definition
            .crypto
            .availability_lag_secs,
        config
            .profile_artifacts
            .domain
            .definition
            .weather
            .availability_lag_secs,
    )
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

fn validate_route_run_binding(
    report: &RecommendationReportInfo,
    route_run: &ReportRouteRunInfo,
    run: &ModelRunInfo,
) -> QuantResult<()> {
    if route_run.report_run_id != report.report_run_id
        || route_run.model_run_id.as_ref() != Some(&run.model_run_id)
        || route_run.model_version_id != run.model_version_id
        || run.window_start != report.decision_at
        || run.decision_policy_snapshot_id != report.decision_policy_snapshot_id
        || run.market_selection_id.as_ref() != Some(&report.market_selection_id)
    {
        return Err(determinism(format!(
            "report {} Route {:?} serving binding disagrees with model run {}",
            report.recommendation_report_id, route_run.route, run.model_run_id
        )));
    }
    Ok(())
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
        let feature_vector_id = record.feature_vector_id.as_ref().ok_or_else(|| {
            determinism(format!(
                "global report {} contains unbound DQ evidence for market {}",
                report.recommendation_report_id, record.market_id
            ))
        })?;
        if !member_markets.contains(&record.market_id)
            || !vector_ids.insert(*feature_vector_id)
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
mod tests {
    use std::{collections::HashMap, slice};

    use chrono::{DateTime, Utc};
    use quant_pivot_models::{
        clickhouse::{QuantFeatureEventRow, QuantModelInputEventRow},
        domain::data_plane::DecisionClock,
        domain::quant::{MarketSelectionMemberInfo, ReportDataQualitySnapshotInfo},
        enums::{
            clickhouse::{ChFeatureCellState, ChFeatureSourceKind, ChFeatureValueKind},
            common::MarketCategory,
            factor::FactorFamily,
            market::MarketStatus,
            quant::{RecommendationReportStatus, ReportKind},
        },
        runtime_config::{BuyModelRoute, DomainConfig, FactorsConfig, FeaturesConfig},
        types::{
            DecisionPolicySnapshotId, EventId, FeatureParityDetailSource, FeatureVectorId,
            MarketSelectionId, ModelVersionId, ReportDataQualityTokens, ResearchFeatureContract,
            TokenDataQualityRecord, TokenId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    use super::{
        DataQualityStatus, FactorEngine, FeatureParityCandidate, FeatureParityComparison,
        FeatureParityEvidence, FeatureParityStage, FeatureParitySubject, MarketId, ModelRunId,
        RecommendationReportId, boundary_from_online, feature_vector_binding, online_route_binding,
        pending_completion, representative_candidate, select_comparisons,
        validate_input_population, validate_quality_evidence, validate_run_completion,
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
        }
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

    #[test]
    fn zero_inputs_complete() {
        let decision_at = DateTime::from_timestamp_millis(Utc::now().timestamp_millis())
            .expect("millisecond decision time");
        let boundary = DecisionClock::new(0)
            .boundary(decision_at)
            .expect("decision boundary");
        let run_id = ModelRunId::from_v7();
        let vector_id = FeatureVectorId::from_v7();
        let market_id = MarketId::new("zero-input-market");
        let row = feature_row(&vector_id, &market_id, decision_at.timestamp_millis());
        let feature_rows = vec![row];
        let feature_evidence = feature_commitment(&feature_rows).expect("feature commitment");
        let completion = completion_marker(&run_id, &boundary, &feature_evidence, &[], 1)
            .expect("zero-input completion");
        let inputs_by_run = HashMap::new();
        let features_by_vector = HashMap::from([(vector_id, feature_rows)]);
        let candidate = candidate(&run_id, market_id.as_str(), decision_at);

        let inputs = validate_run_completion(
            &run_id,
            &candidate,
            &completion,
            &inputs_by_run,
            &features_by_vector,
        )
        .expect("valid zero-input serving completion");

        assert!(inputs.is_empty());
        assert_eq!(
            boundary_from_online(&completion, inputs, &features_by_vector)
                .expect("boundary from feature evidence"),
            boundary
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
            feature_vector_id: Some(feature_vector_id),
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
}
