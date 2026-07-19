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
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
    },
    domain::{
        DecisionBoundary, DecisionClock, DecisionSource, FactorValueInfo, FeatureParityRunInfo,
        FeatureVectorInfo, FrozenFeatureParitySubject, FrozenFeatureParitySubjectId,
        MarketSelectionInfo, MarketSelectionMemberInfo, ModelRunInfo, RecommendationReportInfo,
        ReportDataQualitySnapshotInfo, ReportRunInfo, model_run_parity_evidence_hash,
        parity_candidate_membership_hash, parity_selection_hash, report_parity_evidence_hash,
        report_parity_generation_hash,
    },
    enums::{
        clickhouse::ChFeatureCellState,
        quant::{
            DataQualityStatus, EmptyReportReason, FeatureCellState, FeatureParityRunKind,
            FeatureParityStage, ModelRunKind, ModelRunStatus,
        },
    },
    runtime_config::DecisionPolicySnapshot,
    types::{
        ContentHash, DecisionPolicySnapshotId, FeatureVectorId, MarketId, MarketSelectionId,
        ModelRunId, ModelVersionId, RecommendationReportId, SelectionExclusionSummary,
    },
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CatalogLedgerRepository, ClobMarketInfoRepository,
    FactorRepository, FeatureParityRepository, FeatureRepository, MarketLinkageRepository,
    MarketSelectionRepository, ModelRegistryRepository, ModelRunRepository, PolicyRepository,
    QuantFactReadRepository, RecommendationReportRepository, ReportRunRepository,
    ServingEvidenceRepository,
};
use quant_pivot_research::{
    factors::{FactorEngine, FactorValue, MarketFactorOutcome},
    features::{
        ConfiguredFeatureBuilder, DecisionCaptureEvidence, FeatureSchema, FeatureVector,
        MarketDecisionCapture, feature_events,
    },
    hashing::ResearchHasher,
    model::{
        ActiveSchemaBinding, ModelFamily, ModelInputAuditRow, ModelRuntimeFactoryBuilder,
        ModelRuntimeMetrics, ModelRuntimeOutput, SignalCandidate,
        canonical_business_prediction_hash,
    },
    selection::{
        ConfiguredMarketSelector, MarketSelectionBuildRequest, MarketSelectionSnapshot,
        MarketSelector, ModelFeatureRequirements, SelectedMarket,
    },
};
use serde::Serialize;

use crate::{
    observability::serving_evidence::verify_completion,
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
            CrossSectionRequest, ReplayConfig, ReplayCrossSection, materialize_cross_section,
        },
        model_runner::{AlignedFeatureCrossSection, project_model_input_rows},
    },
};
/// Process-lifetime dependencies of the production parity source.
pub struct DurableFeatureParityDeps {
    pub parity: Arc<dyn FeatureParityRepository>,
    pub model_runs: Arc<dyn ModelRunRepository>,
    pub model_registry: Arc<dyn ModelRegistryRepository>,
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
    pub runtime_factory: Arc<dyn ModelRuntimeFactoryBuilder>,
}

/// Replays successful serving runs from durable PIT data and frozen artifacts.
pub struct DurableFeatureParitySource {
    deps: DurableFeatureParityDeps,
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
    config: DecisionPolicySnapshot,
    boundary: DecisionBoundary,
    report_id: Option<RecommendationReportId>,
    selection: MarketSelectionInfo,
    members: Vec<MarketSelectionMemberInfo>,
}

struct MaterializedRunReplay {
    builder: ConfiguredFeatureBuilder,
    factor_engine: FactorEngine,
    selection: MarketSelectionSnapshot,
    cross_section: ReplayCrossSection,
}

struct MaterializedSelectionReplay {
    builder: ConfiguredFeatureBuilder,
    factor_engine: FactorEngine,
    selection: MarketSelectionSnapshot,
    snapshot_source: Arc<DecisionSnapshotSource>,
    replay_config: ReplayConfig,
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
    replay_captures: &'a HashMap<MarketId, MarketDecisionCapture>,
    vector_binding: &'a HashMap<MarketId, FeatureVectorId>,
    boundary: &'a DecisionBoundary,
    decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    schema: &'a FeatureSchema,
}

struct ReportOnlineEvidence {
    features: HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: HashMap<FeatureVectorId, FeatureVectorInfo>,
    boundary: DecisionBoundary,
}

enum ReportOnlineEvidenceOutcome {
    Ready(ReportOnlineEvidence),
    Pending(Vec<PendingFeatureParityComparison>),
}

struct PreparedReportReplay {
    report_id: RecommendationReportId,
    ceiling: PreInferenceStageCeiling,
    context: ReplayRunContext,
    online: ReportOnlineEvidence,
}

enum PreparedReportReplayOutcome {
    Ready(Box<PreparedReportReplay>),
    Pending(Vec<PendingFeatureParityComparison>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PreInferenceStageCeiling {
    Selection,
    Feature,
}

#[derive(Default)]
struct CandidateSubjects {
    model_runs: Vec<ModelRunInfo>,
    pre_inference_reports: Vec<RecommendationReportInfo>,
}

impl DurableFeatureParitySource {
    #[must_use]
    pub const fn new(deps: DurableFeatureParityDeps) -> Self {
        Self { deps }
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
        candidates.extend(
            self.candidates_for_reports(subjects.pre_inference_reports, run)
                .await?,
        );
        Ok(candidates)
    }

    async fn replay(
        &self,
        _parity_run: &FeatureParityRunInfo,
        candidates: &[FeatureParityCandidate],
    ) -> QuantResult<FeatureParityReplayAttempt> {
        let run_ids = unique_run_ids(candidates);
        let evidence = self.load_replay_evidence(&run_ids).await?;
        let mut attempt = self.replay_candidate_groups(candidates, &evidence).await?;
        let report_attempt = self.replay_report_candidate_groups(candidates).await?;
        attempt.comparisons.extend(report_attempt.comparisons);
        attempt.pending.extend(report_attempt.pending);
        Ok(attempt)
    }
}

impl DurableFeatureParitySource {
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
                    FeatureParitySubject::PreInferenceReport(id)
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
                let evidence_hash = model_run_parity_evidence_hash(
                    &model_run.model_run_id,
                    &model_run.input_hash,
                    output_hash,
                    &model_run.model_version_id,
                    &model_run.decision_policy_snapshot_id,
                )?;
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
                    if report.model_run_id.as_ref() != Some(model_run_id) {
                        return Err(determinism(format!(
                            "parity run {} report does not bind frozen model run {model_run_id}",
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
                    &report.model_version_id,
                    &report.decision_policy_snapshot_id,
                    &report.market_selection_id,
                    &report.data_quality_snapshot_ref,
                    &report.portfolio_plan_id,
                )?;
                if report.model_run_id.is_some()
                    || &report.market_selection_id != selection_id
                    || report.decision_at != decision_at
                    || generation != subject.subject_generation
                    || evidence_hash != subject.evidence_hash
                    || parity_run
                        .report_id
                        .as_ref()
                        .is_some_and(|bound| bound != report_id)
                {
                    return Err(determinism(format!(
                        "frozen report evidence drifted for parity run {} subject {report_id}",
                        parity_run.run_id
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
                if let Some(model_run_id) = report.model_run_id.as_ref() {
                    let model_run = self
                        .deps
                        .model_runs
                        .find_by_id(model_run_id)
                        .await?
                        .ok_or_else(|| StorageError::not_found("quant_model_run", model_run_id))?;
                    validate_report_run_binding(&report, &model_run)?;
                    Ok(CandidateSubjects {
                        model_runs: vec![model_run],
                        pre_inference_reports: Vec::new(),
                    })
                } else {
                    validate_pre_inference_report(&report, run)?;
                    Ok(CandidateSubjects {
                        model_runs: Vec::new(),
                        pre_inference_reports: vec![report],
                    })
                }
            }
            FeatureParityRunKind::Full => {
                let reports = self
                    .deps
                    .reports
                    .list_committed_between(run.window_start, run.window_end)
                    .await?;
                let live_runs = self
                    .deps
                    .model_runs
                    .list_succeeded_live_between(run.window_start, run.window_end)
                    .await?;
                let mut live_by_id = live_runs
                    .into_iter()
                    .map(|row| (row.model_run_id.clone(), row))
                    .collect::<HashMap<_, _>>();
                let mut subjects = CandidateSubjects::default();
                for report in reports {
                    if let Some(model_run_id) = report.model_run_id.as_ref() {
                        let model_run = live_by_id.remove(model_run_id).ok_or_else(|| {
                            determinism(format!(
                                "committed report {} references live run {} absent from its full-parity window",
                                report.recommendation_report_id, model_run_id
                            ))
                        })?;
                        validate_report_run_binding(&report, &model_run)?;
                        subjects.model_runs.push(model_run);
                    } else {
                        validate_pre_inference_report(&report, run)?;
                        subjects.pre_inference_reports.push(report);
                    }
                }
                // Successful live runs without a committed report (for example
                // an intentionally suppressed empty artifact) remain serving
                // evidence and are audited by the daily full replay.
                subjects.model_runs.extend(live_by_id.into_values());
                subjects
                    .model_runs
                    .sort_by_key(|row| (row.window_start, row.model_run_id.to_string()));
                subjects.pre_inference_reports.sort_by_key(|report| {
                    (
                        report.decision_at,
                        report.recommendation_report_id.to_string(),
                    )
                });
                Ok(subjects)
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
                subject: FeatureParitySubject::ModelRun(row.model_run_id.clone()),
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
            validate_pre_inference_report(&report, parity_run)?;
            let members = self
                .deps
                .selections
                .list_members(&report.market_selection_id)
                .await?;
            let dq = self
                .deps
                .reports
                .find_data_quality_snapshot(&report.recommendation_report_id)
                .await?
                .ok_or_else(|| {
                    StorageError::not_found(
                        "quant_report_data_quality_snapshot",
                        &report.data_quality_snapshot_ref,
                    )
                })?;
            let ceiling = validate_pre_inference_stage_evidence(&report, &members, &dq)?;
            let subject =
                FeatureParitySubject::PreInferenceReport(report.recommendation_report_id.clone());
            if ceiling == PreInferenceStageCeiling::Selection {
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
            .map(|row| (row.model_run_id.clone(), row))
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
                .map(|info| (info.feature_vector_id.clone(), info))
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
                .ok_or_else(|| StorageError::not_found("quant_model_run", &run_id))?;
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
                        .map(|rows| (vector_id.clone(), rows))
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
                        .map(|info| (vector_id.clone(), info))
                        .ok_or_else(|| {
                            determinism(format!(
                                "serving completion for {run_id} has no Postgres vector {vector_id}"
                            ))
                        })
                })
                .collect::<QuantResult<HashMap<_, _>>>()?;
            let comparisons = self
                .replay_run(candidate, &run, run_inputs, &run_features, &run_infos)
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
            match self
                .prepare_report_replay(&report_id, &report_candidates)
                .await?
            {
                PreparedReportReplayOutcome::Ready(prepared) => {
                    let comparisons = self
                        .compare_pre_inference_report(
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
                PreparedReportReplayOutcome::Pending(rows) => attempt.pending.extend(rows),
            }
        }
        Ok(attempt)
    }

    async fn prepare_report_replay(
        &self,
        report_id: &RecommendationReportId,
        candidates: &[&FeatureParityCandidate],
    ) -> QuantResult<PreparedReportReplayOutcome> {
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
                StorageError::not_found("quant_market_selection", &report.market_selection_id)
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
                    &report.data_quality_snapshot_ref,
                )
            })?;
        let ceiling = validate_pre_inference_stage_evidence(&report, &members, &dq)?;
        let config_info = self
            .deps
            .runtime_configs
            .load_snapshot(&report.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    &report.decision_policy_snapshot_id,
                )
            })?;
        let config = config_info.snapshot;
        let expected_boundary = report_decision_boundary(&report, &report_run, &config)?;
        validate_report_selection_binding(&report, &selection)?;
        let online = match self
            .load_report_online_evidence(&report, &dq, ceiling, expected_boundary, candidates)
            .await?
        {
            ReportOnlineEvidenceOutcome::Ready(online) => online,
            ReportOnlineEvidenceOutcome::Pending(pending) => {
                return Ok(PreparedReportReplayOutcome::Pending(pending));
            }
        };
        Ok(PreparedReportReplayOutcome::Ready(Box::new(
            PreparedReportReplay {
                report_id: report_id.clone(),
                ceiling,
                context: ReplayRunContext {
                    model_version_id: report.model_version_id,
                    decision_policy_snapshot_id: report.decision_policy_snapshot_id,
                    config,
                    boundary: online.boundary.clone(),
                    report_id: Some(report_id.clone()),
                    selection,
                    members,
                },
                online,
            },
        )))
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
                    &report.recommendation_report_id,
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

    async fn load_report_online_evidence(
        &self,
        report: &RecommendationReportInfo,
        dq: &ReportDataQualitySnapshotInfo,
        ceiling: PreInferenceStageCeiling,
        expected_boundary: DecisionBoundary,
        candidates: &[&FeatureParityCandidate],
    ) -> QuantResult<ReportOnlineEvidenceOutcome> {
        if ceiling == PreInferenceStageCeiling::Selection {
            return Ok(ReportOnlineEvidenceOutcome::Ready(ReportOnlineEvidence {
                features: HashMap::new(),
                feature_infos: HashMap::new(),
                boundary: expected_boundary,
            }));
        }
        let vector_ids = dq
            .tokens_json
            .0
            .iter()
            .map(|record| {
                record.feature_vector_id.clone().ok_or_else(|| {
                    determinism(format!(
                        "report {} contains legacy-unbound DQ evidence for market {}",
                        report.recommendation_report_id, record.market_id
                    ))
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        let rows = dedupe_feature_rows(
            self.deps
                .serving_evidence
                .feature_cells_for_vectors(&vector_ids)
                .await?,
        )?;
        let features = group_feature_rows(rows);
        let missing = vector_ids
            .iter()
            .filter(|vector_id| !features.contains_key(*vector_id))
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            return Ok(ReportOnlineEvidenceOutcome::Pending(
                pending_report_features(
                    &report.recommendation_report_id,
                    &report.model_version_id,
                    candidates,
                    &format!("serving_feature_rows_missing:{}", missing.join(",")),
                ),
            ));
        }
        let infos = self.deps.feature_vectors.find_by_ids(&vector_ids).await?;
        if infos.len() != vector_ids.len() {
            return Err(determinism(format!(
                "report {} exact feature-vector binding resolves {} of {} rows",
                report.recommendation_report_id,
                infos.len(),
                vector_ids.len()
            )));
        }
        let feature_infos = infos
            .into_iter()
            .map(|info| (info.feature_vector_id.clone(), info))
            .collect::<HashMap<_, _>>();
        validate_report_dq_vector_bindings(report, dq, &feature_infos)?;
        let boundary =
            boundary_from_report_features(report, &expected_boundary, &features, &feature_infos)?;
        Ok(ReportOnlineEvidenceOutcome::Ready(ReportOnlineEvidence {
            features,
            feature_infos,
            boundary,
        }))
    }

    async fn compare_pre_inference_report(
        &self,
        candidate: &FeatureParityCandidate,
        prepared: &PreparedReportReplay,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let context = &prepared.context;
        let subject = ComparisonSubject {
            report: Some(&prepared.report_id),
            model_run: None,
            model_version: Some(&context.model_version_id),
        };
        if prepared.ceiling == PreInferenceStageCeiling::Selection {
            let replay = self.materialize_selection_replay(context).await?;
            return selection_comparisons(
                candidate,
                subject,
                &context.selection,
                &context.members,
                &replay.selection,
                &context.boundary,
            );
        }
        let replay = self
            .materialize_run_replay(
                &format!("pre-inference report {}", prepared.report_id),
                context,
            )
            .await?;
        let vector_binding = feature_vector_binding(&prepared.online.features)?;
        let replay_by_market = replay_vectors_by_market(&replay.cross_section);
        let mut comparisons = selection_comparisons(
            candidate,
            subject,
            &context.selection,
            &context.members,
            &replay.selection,
            &context.boundary,
        )?;
        comparisons.extend(snapshot_and_feature_comparisons(
            candidate,
            subject,
            FeatureComparisonInputs {
                online_features: &prepared.online.features,
                feature_infos: &prepared.online.feature_infos,
                replay_by_market: &replay_by_market,
                replay_captures: &replay.cross_section.captures,
                vector_binding: &vector_binding,
                boundary: &context.boundary,
                decision_policy_snapshot_id: &context.decision_policy_snapshot_id,
                schema: replay.builder.schema(),
            },
        )?);
        comparisons.push(data_quality_comparison(
            candidate,
            subject,
            &prepared.online.features,
            &HashMap::new(),
            &replay_by_market,
            &context.boundary,
        )?);
        Ok(comparisons)
    }

    async fn prepare_replay_run(
        &self,
        candidate: &FeatureParityCandidate,
        run: &ModelRunInfo,
        online_inputs: &[QuantModelInputEventRow],
        online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
        feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
    ) -> QuantResult<ReplayRunContext> {
        let model_version_id = run.model_version_id.clone().ok_or_else(|| {
            determinism(format!(
                "live run {} has no model version",
                run.model_run_id
            ))
        })?;
        let selection_id = run.market_selection_id.clone().ok_or_else(|| {
            determinism(format!("live run {} has no selection", run.model_run_id))
        })?;
        let config_info = self
            .deps
            .runtime_configs
            .load_snapshot(&run.decision_policy_snapshot_id)
            .await?
            .ok_or_else(|| {
                StorageError::not_found(
                    "decision_policy_snapshot",
                    &run.decision_policy_snapshot_id,
                )
            })?;
        let config = config_info.snapshot;
        let boundary = boundary_from_online(online_inputs, online_features)?;
        if boundary.decision_at() != candidate.decision_at {
            return Err(determinism(format!(
                "serving evidence boundary {} does not match candidate {}",
                boundary.decision_at(),
                candidate.decision_at
            )));
        }
        let report_id = self
            .deps
            .reports
            .find_by_model_run_id(&run.model_run_id)
            .await?
            .map(|report| report.recommendation_report_id);

        let selection = self
            .deps
            .selections
            .find_by_id(&selection_id)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_market_selection", &selection_id))?;
        if selection.decision_at != boundary.decision_at()
            || selection.decision_policy_snapshot_id != run.decision_policy_snapshot_id
        {
            return Err(determinism(format!(
                "selection {selection_id} is not bound to model run {} decision/config",
                run.model_run_id
            )));
        }
        let members = self.deps.selections.list_members(&selection_id).await?;
        validate_replay_feature_population(
            &selection_id,
            &boundary,
            &members,
            online_features,
            feature_infos,
        )?;
        Ok(ReplayRunContext {
            model_version_id,
            decision_policy_snapshot_id: run.decision_policy_snapshot_id.clone(),
            config,
            boundary,
            report_id,
            selection,
            members,
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
        let samples = selection_replay
            .selection
            .included
            .iter()
            .map(|market| ReplaySample {
                market_id: market.market_id.clone(),
                token_id: market.primary_token_id.clone(),
            })
            .collect::<Vec<_>>();
        if samples.is_empty() {
            return Err(determinism(format!(
                "durable selector replay produced no markets for {subject}"
            )));
        }
        let lookback = Duration::from_secs(
            config
                .profile_artifacts
                .features
                .definition
                .max_lookback_secs(),
        );
        let loader = HistoricalWindowLoader::new(
            Arc::clone(&self.deps.fact_read),
            Arc::clone(&self.deps.catalog),
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
                lookback,
                knowledge_lag: boundary.knowledge_lag(),
                max_horizon_secs: 0,
                domain: config.profile_artifacts.domain.definition.clone(),
            })
            .await?;
        let cross = materialize_cross_section(
            &selection_replay.builder,
            &selection_replay.factor_engine,
            &selection_replay.replay_config,
            &CrossSectionRequest {
                pit: selection_replay.snapshot_source.as_ref(),
                prefetched: &window.prefetched,
                decision_at: boundary.decision_at(),
                group: &samples,
                knowledge_lag: boundary.knowledge_lag(),
                lookback,
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
            selection: selection_replay.selection,
            cross_section: cross,
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
        let replay_config = ReplayConfig {
            features: config.profile_artifacts.features.definition.clone(),
            factors: config.profile_artifacts.scoring.definition.clone(),
            domain: config.profile_artifacts.domain.definition.clone(),
            data_quality: config.recommendation.data_quality.clone(),
            bias_table: bias_table.as_ref().map(Arc::clone),
        };
        let builder = ConfiguredFeatureBuilder::new(
            &config.profile_artifacts.features.definition,
            &config.profile_artifacts.domain.definition,
        )?;
        let factor_engine = FactorEngine::new(
            &config.profile_artifacts.scoring.definition,
            &config.profile_artifacts.features.definition,
            &config.profile_artifacts.domain.definition,
            bias_table.clone(),
        );
        let model_requirements = self
            .replay_model_requirements(
                context,
                &builder,
                &factor_engine,
                bias_table.as_ref().map(|table| table.content_hash.clone()),
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
        let candidate_batch = candidate_provider
            .candidates(boundary, &config.profile_artifacts.domain.definition)
            .await?;
        let selection = ConfiguredMarketSelector::new()
            .build_snapshot(
                MarketSelectionBuildRequest {
                    decision_at: boundary.decision_at(),
                    decision_policy_snapshot_id: context.decision_policy_snapshot_id.clone(),
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
            selection,
            snapshot_source: candidate_batch.snapshot_source,
            replay_config,
        })
    }

    async fn replay_model_requirements(
        &self,
        context: &ReplayRunContext,
        builder: &ConfiguredFeatureBuilder,
        factor_engine: &FactorEngine,
        bias_table_hash: Option<ContentHash>,
    ) -> QuantResult<ModelFeatureRequirements> {
        let configured_generic = context
            .config
            .model_routing
            .model
            .active_model_version_id
            .as_ref()
            .ok_or_else(|| determinism("frozen config has no active model pointer".to_owned()))
            .map(|reference| reference.id.clone())?;
        if configured_generic != context.model_version_id {
            return Err(determinism(format!(
                "frozen config generic model {configured_generic} differs from serving run model {}",
                context.model_version_id
            )));
        }
        let factory = self.deps.runtime_factory.build(ActiveSchemaBinding {
            feature_schema_hash: ResearchHasher::feature_schema(builder.schema())?,
            factor_schema_hash: factor_engine.factor_schema_hash()?,
            bias_table_hash,
        });
        let generic_version = self
            .deps
            .model_registry
            .find_model_version_by_id(&configured_generic)
            .await?
            .ok_or_else(|| StorageError::not_found("quant_model_version", &configured_generic))?;
        let generic = factory.load(&generic_version, None).await?;
        if generic.category_scope().is_some() {
            return Err(determinism(format!(
                "generic serving model {configured_generic} declares category scope {:?}",
                generic.category_scope()
            )));
        }
        let mut by_category = BTreeMap::new();
        for (category, reference) in &context.config.model_routing.model.category_model_pointers {
            let version_id = reference.id.clone();
            let version = self
                .deps
                .model_registry
                .find_model_version_by_id(&version_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_model_version", &version_id))?;
            let runtime = factory.load(&version, None).await?;
            if runtime.category_scope() != Some(*category) {
                return Err(determinism(format!(
                    "category route {category} model {version_id} declares scope {:?}",
                    runtime.category_scope()
                )));
            }
            by_category.insert(*category, runtime.required_features());
        }
        Ok(ModelFeatureRequirements {
            generic: generic.required_features(),
            by_category,
        })
    }

    async fn replay_run(
        &self,
        candidate: &FeatureParityCandidate,
        run: &ModelRunInfo,
        online_inputs: &[QuantModelInputEventRow],
        online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
        feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
    ) -> QuantResult<Vec<FeatureParityComparison>> {
        let context = self
            .prepare_replay_run(
                candidate,
                run,
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
        let replay_by_market = replay_vectors_by_market(&replay.cross_section);
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

        let admission_matches = replay_admission_matches(&model_vector_binding, &replay);
        comparisons.push(data_quality_comparison(
            candidate,
            comparison_subject,
            online_features,
            &model_vector_binding,
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
                vector_binding: &model_vector_binding,
                factor_engine: &replay.factor_engine,
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
        let feature_schema_hash = ResearchHasher::feature_schema(&FeatureSchema::build(
            &request.config.profile_artifacts.features.definition,
        )?)?;
        let factor_schema_hash = request.factor_engine.factor_schema_hash()?;
        let factory = self.deps.runtime_factory.build(ActiveSchemaBinding {
            feature_schema_hash,
            factor_schema_hash,
            bias_table_hash: None,
        });
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
        let mut markets_by_version: HashMap<ModelVersionId, BTreeSet<MarketId>> = HashMap::new();
        for row in request.online_inputs {
            markets_by_version
                .entry(row.model_version_id.clone())
                .or_default()
                .insert(row.market_id.clone());
        }

        let mut all_candidates = Vec::new();
        let mut all_audit = Vec::new();
        let mut all_factor_outcomes = Vec::new();
        let mut ordered_routes = markets_by_version.into_iter().collect::<Vec<_>>();
        ordered_routes.sort_by_key(|(version_id, _)| version_id.to_string());
        for (version_id, market_ids) in ordered_routes {
            let version = self
                .deps
                .model_registry
                .find_model_version_by_id(&version_id)
                .await?
                .ok_or_else(|| StorageError::not_found("quant_model_version", &version_id))?;
            let runtime = factory.load(&version, None).await?;
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
                request.factor_engine.compute_all_batch_with_references(
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
            all_candidates.extend(output.candidates.clone());
            all_audit.extend(output.input_audit.clone());
            all_factor_outcomes.extend(outcomes);

            if output.input_audit.is_empty() {
                return Err(determinism(format!(
                    "runtime {version_id} emitted no model-input audit rows"
                )));
            }
        }

        finish_replayed_model_output(&request, all_candidates, all_audit, all_factor_outcomes)
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
                report_id: report_id.cloned(),
                model_run_id: Some(run.model_run_id.clone()),
                model_version_id: run.model_version_id.clone(),
                market_id: None,
                feature_name: Some("classical_factor_bypass".to_owned()),
                online: canonical_evidence(&"classical_factor_bypass", None, boundary)?,
                replay: canonical_evidence(&"classical_factor_bypass", None, boundary)?,
                transform_hash: None,
                detail: serde_json::json!({"family_dispatch": "classical"}),
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
            report_id: report_id.cloned(),
            model_run_id: Some(run.model_run_id.clone()),
            model_version_id: run.model_version_id.clone(),
            market_id: None,
            feature_name: None,
            online: canonical_evidence(&online_projection, None, boundary)?,
            replay: canonical_evidence(&replay_projection, None, boundary)?,
            transform_hash: None,
            detail: serde_json::json!({"online_count": online_projection.len(), "replay_count": replay_projection.len()}),
        })])
    }
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
    candidates: Vec<SignalCandidate>,
    input_audit: Vec<ModelInputAuditRow>,
    factor_outcomes: Vec<MarketFactorOutcome>,
) -> QuantResult<ReplayedModelOutput> {
    let runtime_output = ModelRuntimeOutput {
        candidates,
        runtime_metrics: ModelRuntimeMetrics {
            markets_scored: 0,
            candidates_emitted: 0,
            inference_duration_ms: 0,
        },
        input_audit,
    };
    let input_rows = project_replay_rows_from_audit(
        &request.run.model_run_id,
        request.boundary,
        request.markets,
        request.vectors,
        request.vector_binding,
        &runtime_output.input_audit,
    )?;
    Ok(ReplayedModelOutput {
        runtime_output,
        input_rows,
        factor_outcomes,
    })
}

fn project_replay_rows_from_audit(
    model_run_id: &ModelRunId,
    boundary: &DecisionBoundary,
    markets: &[SelectedMarket],
    vectors: &[FeatureVector],
    vector_binding: &HashMap<MarketId, FeatureVectorId>,
    audit: &[ModelInputAuditRow],
) -> QuantResult<Vec<QuantModelInputEventRow>> {
    let vector_ids = vectors
        .iter()
        .map(|vector| {
            vector_binding
                .get(&vector.market_id)
                .cloned()
                .ok_or_else(|| {
                    determinism(format!("no serving vector id for {}", vector.market_id))
                })
        })
        .collect::<QuantResult<Vec<_>>>()?;
    let aligned = AlignedFeatureCrossSection {
        markets: markets.to_vec(),
        vectors: vectors.to_vec(),
        vector_ids,
    };
    project_model_input_rows(
        model_run_id,
        boundary,
        &aligned,
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
            state: value.value_state.as_str().to_owned(),
            raw_value: value.raw_value.map(|value| value.to_string()),
            normalized_score: value.normalized_score.map(|value| value.to_string()),
            normalization_source: value
                .normalization_source
                .map(|value| value.as_str().to_owned()),
            confidence: value.confidence.to_string(),
        }
    }

    fn from_replay(market_id: &MarketId, value: &FactorValue) -> Self {
        Self {
            market_id: market_id.to_string(),
            factor_definition_id: value.definition_id.to_string(),
            state: value.value_state().as_str().to_owned(),
            raw_value: value.raw_value.map(|raw| raw.to_string()),
            normalized_score: value.normalized_score().map(|score| score.to_string()),
            normalization_source: value
                .normalization_source()
                .map(|source| source.as_str().to_owned()),
            confidence: value.confidence.to_string(),
        }
    }
}

fn selection_comparisons(
    candidate: &FeatureParityCandidate,
    subject: ComparisonSubject<'_>,
    online_selection: &MarketSelectionInfo,
    online_members: &[MarketSelectionMemberInfo],
    replay: &MarketSelectionSnapshot,
    boundary: &DecisionBoundary,
) -> QuantResult<Vec<FeatureParityComparison>> {
    #[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
    struct Member {
        market_id: String,
        event_id: String,
        category: String,
        primary_token_id: String,
        secondary_token_id: Option<String>,
        liquidity_usd: Option<String>,
        volume_24h_usd: Option<String>,
    }

    #[derive(Serialize)]
    struct SelectionProjection<'a> {
        decision_at: DateTime<Utc>,
        decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
        selector_hash: &'a ContentHash,
        market_count: usize,
        exclusion_summary: SelectionExclusionSummary,
        members: &'a [Member],
    }

    let online_market_count = usize::try_from(online_selection.market_count).map_err(|error| {
        determinism(format!(
            "selection {} has invalid market_count {}: {error}",
            online_selection.market_selection_id, online_selection.market_count
        ))
    })?;
    let mut online_projection_members = online_members
        .iter()
        .map(|row| Member {
            market_id: row.market_id.to_string(),
            event_id: row.event_id.to_string(),
            category: row.category.as_str().to_owned(),
            primary_token_id: row.primary_token_id.to_string(),
            secondary_token_id: row.secondary_token_id.as_ref().map(ToString::to_string),
            liquidity_usd: row.liquidity_usd.map(|value| value.to_string()),
            volume_24h_usd: row.volume_24h_usd.map(|value| value.to_string()),
        })
        .collect::<Vec<_>>();
    online_projection_members.sort();
    let mut replay_members = replay
        .included
        .iter()
        .map(|row| Member {
            market_id: row.market_id.to_string(),
            event_id: row.event_id.to_string(),
            category: row.category.as_str().to_owned(),
            primary_token_id: row.primary_token_id.to_string(),
            secondary_token_id: row.secondary_token_id.as_ref().map(ToString::to_string),
            liquidity_usd: row.liquidity_usd.map(|value| value.to_string()),
            volume_24h_usd: row.volume_24h_usd.map(|value| value.to_string()),
        })
        .collect::<Vec<_>>();
    replay_members.sort();
    let online_projection = SelectionProjection {
        decision_at: online_selection.decision_at,
        decision_policy_snapshot_id: &online_selection.decision_policy_snapshot_id,
        selector_hash: &online_selection.selector_hash,
        market_count: online_market_count,
        exclusion_summary: online_selection.exclusion_summary,
        members: &online_projection_members,
    };
    let replay_projection = SelectionProjection {
        decision_at: replay.decision_at,
        decision_policy_snapshot_id: &replay.decision_policy_snapshot_id,
        selector_hash: &replay.selector_hash,
        market_count: replay.included.len(),
        exclusion_summary: replay.exclusion_summary,
        members: &replay_members,
    };
    Ok(vec![comparison(ComparisonInput {
        candidate,
        stage: FeatureParityStage::Selection,
        report_id: subject.report.cloned(),
        model_run_id: subject.model_run.cloned(),
        model_version_id: subject.model_version.cloned(),
        market_id: None,
        feature_name: None,
        online: canonical_evidence(&online_projection, None, boundary)?,
        replay: canonical_evidence(&replay_projection, None, boundary)?,
        transform_hash: None,
        detail: serde_json::json!({
            "online_count": online_projection_members.len(),
            "replay_count": replay_members.len(),
            "online_selector_hash": online_selection.selector_hash.to_string(),
            "replay_selector_hash": replay.selector_hash.to_string(),
            "replay_excluded_count": replay.excluded.len(),
        }),
    })])
}

fn data_quality_comparison(
    candidate: &FeatureParityCandidate,
    subject: ComparisonSubject<'_>,
    online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    admitted_binding: &HashMap<MarketId, FeatureVectorId>,
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
        if rows
            .iter()
            .any(|row| row.market_id != first.market_id || row.data_quality != first.data_quality)
        {
            return Err(determinism(format!(
                "feature evidence group {vector_id} has inconsistent market or data-quality state"
            )));
        }
        online.push(DataQualityProjection {
            market_id: first.market_id.to_string(),
            data_quality: first.data_quality.clone(),
            admitted: admitted_binding.get(&first.market_id) == Some(vector_id),
        });
    }
    online.sort();

    let mut replay = replay_by_market
        .values()
        .map(|vector| DataQualityProjection {
            market_id: vector.market_id.to_string(),
            data_quality: vector.data_quality.as_str().to_owned(),
            admitted: vector.data_quality != DataQualityStatus::Insufficient,
        })
        .collect::<Vec<_>>();
    replay.sort();

    Ok(comparison(ComparisonInput {
        candidate,
        stage: FeatureParityStage::DataQuality,
        report_id: subject.report.cloned(),
        model_run_id: subject.model_run.cloned(),
        model_version_id: subject.model_version.cloned(),
        market_id: None,
        feature_name: None,
        online: canonical_evidence(&online, None, boundary)?,
        replay: canonical_evidence(&replay, None, boundary)?,
        transform_hash: None,
        detail: serde_json::json!({
            "online_count": online.len(),
            "replay_count": replay.len(),
            "online_admitted_count": online.iter().filter(|row| row.admitted).count(),
            "replay_admitted_count": replay.iter().filter(|row| row.admitted).count(),
        }),
    }))
}

fn replay_vectors_by_market(
    cross_section: &ReplayCrossSection,
) -> HashMap<MarketId, FeatureVector> {
    cross_section
        .vectors
        .iter()
        .chain(&cross_section.rejected_vectors)
        .cloned()
        .map(|vector| (vector.market_id.clone(), vector))
        .collect()
}

fn replay_admission_matches(
    model_vector_binding: &HashMap<MarketId, FeatureVectorId>,
    replay: &MaterializedRunReplay,
) -> bool {
    model_vector_binding.keys().collect::<BTreeSet<_>>()
        == replay
            .cross_section
            .vectors
            .iter()
            .map(|vector| &vector.market_id)
            .collect::<BTreeSet<_>>()
}

fn validate_replay_feature_population(
    selection_id: &MarketSelectionId,
    boundary: &DecisionBoundary,
    members: &[MarketSelectionMemberInfo],
    online_features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    feature_infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
) -> QuantResult<()> {
    let member_markets = members
        .iter()
        .map(|member| member.market_id.clone())
        .collect::<BTreeSet<_>>();
    let mut vector_markets = BTreeSet::new();
    for (vector_id, persisted) in feature_infos {
        let persisted_boundary = persisted.decision_boundary.as_ref().ok_or_else(|| {
            determinism(format!(
                "Postgres feature vector {vector_id} is a pre-v10 row without a decision boundary"
            ))
        })?;
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
        let valid_binding = member_markets.contains(&first.market_id)
            && persisted.market_id == first.market_id
            && persisted.token_id.as_ref() == Some(token_id)
            && vector_markets.insert(first.market_id.clone());
        if !valid_binding {
            return Err(determinism(format!(
                "feature vector {vector_id} has a duplicate or inconsistent selection/market/token binding"
            )));
        }
    }
    if vector_markets != member_markets {
        return Err(determinism(format!(
            "selection {selection_id} members do not match its committed feature-vector population"
        )));
    }
    Ok(())
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
        let online_capture = persisted_capture(online_info, online_rows, inputs.boundary)?;
        let replay_capture = inputs.replay_captures.get(market_id).ok_or_else(|| {
            determinism(format!(
                "replay dropped decision capture for market {market_id}"
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
            report_id: subject.report.cloned(),
            model_run_id: subject.model_run.cloned(),
            model_version_id: subject.model_version.cloned(),
            market_id: Some(market_id.clone()),
            feature_name: None,
            online: canonical_evidence(&online_capture.snapshot, None, inputs.boundary)?,
            replay: canonical_evidence(
                &replay_capture_evidence.snapshot,
                None,
                inputs.boundary,
            )?,
            transform_hash: None,
            detail: serde_json::json!({
                "feature_vector_id": vector_id.to_string(),
                "online_catalog_change_id": online_capture.snapshot.catalog.market_change_id.to_string(),
                "replay_catalog_change_id": replay_capture_evidence.snapshot.catalog.market_change_id.to_string(),
                "online_book_ref": online_capture.snapshot.book_snapshot_ref.to_string(),
                "replay_book_ref": replay_capture_evidence.snapshot.book_snapshot_ref.to_string(),
            }),
        }));
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::Capture,
            report_id: subject.report.cloned(),
            model_run_id: subject.model_run.cloned(),
            model_version_id: subject.model_version.cloned(),
            market_id: Some(market_id.clone()),
            feature_name: None,
            online: canonical_evidence(&online_capture, None, inputs.boundary)?,
            replay: canonical_evidence(&replay_capture_evidence, None, inputs.boundary)?,
            transform_hash: None,
            detail: serde_json::json!({
                "feature_vector_id": vector_id.to_string(),
                "online_capture_hash": online_info.decision_capture_hash.as_ref().map(ToString::to_string),
                "replay_capture_hash": replay_capture_hash.to_string(),
            }),
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
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::FeatureCell,
            report_id: subject.report.cloned(),
            model_run_id: subject.model_run.cloned(),
            model_version_id: subject.model_version.cloned(),
            market_id: Some(market_id.clone()),
            feature_name: Some(online.feature_name.clone()),
            online: feature_row_evidence(online)?,
            replay: feature_row_evidence(replay)?,
            transform_hash: None,
            detail: serde_json::json!({"feature_vector_id": vector_id.to_string()}),
        }));
    }
    Ok(comparisons)
}

fn replay_feature_info(
    vector_id: &FeatureVectorId,
    replay_vector: &FeatureVector,
    boundary: &DecisionBoundary,
    capture: &DecisionCaptureEvidence,
    capture_hash: &ContentHash,
    created_at: DateTime<Utc>,
) -> QuantResult<FeatureVectorInfo> {
    let replay_new = replay_vector.try_to_new(boundary)?;
    Ok(FeatureVectorInfo {
        feature_vector_id: vector_id.clone(),
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
        decision_capture: Some(serde_json::to_value(capture).map_err(|error| {
            ResearchError::Serialization {
                detail: format!(
                    "serialize replay decision capture for market {}: {error}",
                    replay_vector.market_id
                ),
            }
        })?),
        decision_capture_hash: Some(capture_hash.clone()),
        created_at,
    })
}

pub(crate) fn persisted_capture(
    info: &FeatureVectorInfo,
    rows: &[QuantFeatureEventRow],
    boundary: &DecisionBoundary,
) -> QuantResult<DecisionCaptureEvidence> {
    let payload = info.decision_capture.clone().ok_or_else(|| {
        determinism(format!(
            "feature vector {} is a pre-v10 row without decision capture",
            info.feature_vector_id
        ))
    })?;
    let expected_hash = info.decision_capture_hash.as_ref().ok_or_else(|| {
        determinism(format!(
            "feature vector {} has decision capture without hash",
            info.feature_vector_id
        ))
    })?;
    let capture: DecisionCaptureEvidence =
        serde_json::from_value(payload).map_err(|error| ResearchError::Serialization {
            detail: format!(
                "decode decision capture for vector {}: {error}",
                info.feature_vector_id
            ),
        })?;
    let actual_hash = ResearchHasher::canonical(&capture)?;
    if &actual_hash != expected_hash
        || capture.snapshot.boundary != *boundary
        || capture.snapshot.market_id != info.market_id
        || Some(&capture.snapshot.token_id) != info.token_id.as_ref()
        || rows
            .iter()
            .any(|row| row.decision_capture_hash != expected_hash.as_str())
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
        let transform_hash = Some(ContentHash::parse(row.transform_hash.clone())?);
        comparisons.push(comparison(ComparisonInput {
            candidate,
            stage: FeatureParityStage::ModelInput,
            report_id: report_id.cloned(),
            model_run_id: Some(model_run_id.clone()),
            model_version_id: Some(row.model_version_id.clone()),
            market_id: Some(row.market_id.clone()),
            feature_name: Some(row.encoded_column.clone()),
            online: model_input_evidence(row)?,
            replay: model_input_evidence(replay_row)?,
            transform_hash,
            detail: serde_json::json!({
                "raw_input_name": row.raw_input_name,
                "feature_vector_id": row.feature_vector_id.to_string(),
            }),
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
    let online_hash = run.output_hash.clone().ok_or_else(|| {
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
        model_run_id: Some(run.model_run_id.clone()),
        model_version_id: run.model_version_id.clone(),
        market_id: None,
        feature_name: None,
        online: canonical_evidence(&online_hash, None, boundary)?,
        replay: canonical_evidence(&replay_hash, None, boundary)?,
        transform_hash: None,
        detail: serde_json::json!({"candidate_count": replay.candidates.len()}),
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
    detail: serde_json::Value,
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
    inputs: &[QuantModelInputEventRow],
    features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
) -> QuantResult<DecisionBoundary> {
    let first = inputs
        .first()
        .ok_or_else(|| determinism("model input group is empty".to_owned()))?;
    let decision_at = required_millis(first.decision_at, "model input decision_at")?;
    let knowledge_cutoff = required_millis(first.knowledge_cutoff, "model input cutoff")?;
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
    let feature = features
        .get(&first.feature_vector_id)
        .and_then(|rows| rows.first())
        .ok_or_else(|| {
            determinism(format!(
                "no feature boundary for vector {}",
                first.feature_vector_id
            ))
        })?;
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
        if row.decision_at != first.decision_at || row.knowledge_cutoff != first.knowledge_cutoff {
            return Err(determinism(format!(
                "model input run {} contains multiple decision boundaries",
                first.model_run_id
            )));
        }
    }
    let expected_source_cutoffs = &feature.per_source_cutoffs_json;
    for rows in features.values() {
        for row in rows {
            if row.decision_at != first.decision_at
                || row.knowledge_cutoff != first.knowledge_cutoff
                || &row.per_source_cutoffs_json != expected_source_cutoffs
            {
                return Err(determinism(format!(
                    "feature vector {} contains a boundary inconsistent with model run {}",
                    row.feature_vector_id, first.model_run_id
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

fn boundary_from_report_features(
    report: &RecommendationReportInfo,
    expected: &DecisionBoundary,
    features: &HashMap<FeatureVectorId, Vec<QuantFeatureEventRow>>,
    infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
) -> QuantResult<DecisionBoundary> {
    if features.len() != infos.len() || infos.is_empty() {
        return Err(determinism(format!(
            "report {} feature evidence is empty or incomplete",
            report.recommendation_report_id
        )));
    }
    for (vector_id, info) in infos {
        let boundary = info.decision_boundary.as_ref().ok_or_else(|| {
            determinism(format!(
                "report {} vector {} has no v10 decision boundary",
                report.recommendation_report_id, vector_id
            ))
        })?;
        boundary.validate()?;
        if boundary != expected || info.decision_at != report.decision_at {
            return Err(determinism(format!(
                "report {} vector {} boundary differs from its frozen report config",
                report.recommendation_report_id, vector_id
            )));
        }
        let rows = features.get(vector_id).ok_or_else(|| {
            determinism(format!(
                "report {} vector {} has no durable feature rows",
                report.recommendation_report_id, vector_id
            ))
        })?;
        if rows.is_empty() {
            return Err(determinism(format!(
                "report {} vector {} durable feature group is empty",
                report.recommendation_report_id, vector_id
            )));
        }
        for row in rows {
            let cutoffs: BTreeMap<DecisionSource, DateTime<Utc>> =
                serde_json::from_str(&row.per_source_cutoffs_json).map_err(|error| {
                    ResearchError::Determinism {
                        detail: format!(
                            "report {} vector {} has invalid source cutoffs: {error}",
                            report.recommendation_report_id, vector_id
                        ),
                    }
                })?;
            if row.feature_vector_id != *vector_id
                || row.market_id != info.market_id
                || row.token_id.as_ref() != info.token_id.as_ref()
                || row.decision_at != expected.decision_at().timestamp_millis()
                || row.knowledge_cutoff != expected.knowledge_cutoff().timestamp_millis()
                || &cutoffs != expected.per_source_cutoffs()
            {
                return Err(determinism(format!(
                    "report {} vector {} feature rows disagree with its PG boundary/identity",
                    report.recommendation_report_id, vector_id
                )));
            }
        }
    }
    Ok(expected.clone())
}

fn vector_binding(
    inputs: &[QuantModelInputEventRow],
) -> QuantResult<HashMap<MarketId, FeatureVectorId>> {
    let mut binding = HashMap::new();
    for row in inputs {
        match binding.insert(row.market_id.clone(), row.feature_vector_id.clone()) {
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
        if let Some(previous) = binding.insert(first.market_id.clone(), vector_id.clone()) {
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
            .entry(row.model_run_id.clone())
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
            .entry(row.feature_vector_id.clone())
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
        unique.insert(row.model_run_id.clone(), row);
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
            FeatureParitySubject::ModelRun(run_id) => Some(run_id.clone()),
            FeatureParitySubject::PreInferenceReport(_) => None,
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
            by_run.entry(run_id.clone()).or_default().push(candidate);
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
        if let FeatureParitySubject::PreInferenceReport(report_id) = &candidate.subject {
            by_report
                .entry(report_id.clone())
                .or_default()
                .push(candidate);
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
    let run_inputs = inputs_by_run.get(run_id).ok_or_else(|| {
        determinism(format!(
            "completed serving run {run_id} has no durable model-input rows"
        ))
    })?;
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

fn validate_report_run_binding(
    report: &RecommendationReportInfo,
    run: &ModelRunInfo,
) -> QuantResult<()> {
    if report.model_run_id.as_ref() != Some(&run.model_run_id)
        || run.window_start != report.decision_at
        || run.decision_policy_snapshot_id != report.decision_policy_snapshot_id
        || run.model_version_id.as_ref() != Some(&report.model_version_id)
        || run.market_selection_id.as_ref() != Some(&report.market_selection_id)
    {
        return Err(determinism(format!(
            "report {} serving binding disagrees with model run {}",
            report.recommendation_report_id, run.model_run_id
        )));
    }
    Ok(())
}

fn pre_inference_stage_ceiling(
    report: &RecommendationReportInfo,
) -> QuantResult<PreInferenceStageCeiling> {
    if report.model_run_id.is_some() {
        return Err(determinism(format!(
            "report {} has a real model run and cannot be replayed as pre-inference",
            report.recommendation_report_id
        )));
    }
    match report.summary_json.empty_reason {
        Some(EmptyReportReason::EmptySelection | EmptyReportReason::SystemDegraded) => {
            Ok(PreInferenceStageCeiling::Selection)
        }
        Some(EmptyReportReason::InsufficientDataQuality) => Ok(PreInferenceStageCeiling::Feature),
        Some(reason) => Err(determinism(format!(
            "report {} stopped before inference with impossible empty reason {}",
            report.recommendation_report_id,
            reason.as_str()
        ))),
        None => Err(determinism(format!(
            "report {} stopped before inference without a frozen empty reason",
            report.recommendation_report_id
        ))),
    }
}

fn validate_pre_inference_report(
    report: &RecommendationReportInfo,
    parity_run: &FeatureParityRunInfo,
) -> QuantResult<()> {
    pre_inference_stage_ceiling(report)?;
    if report.decision_at < parity_run.window_start || report.decision_at >= parity_run.window_end {
        return Err(determinism(format!(
            "pre-inference report {} decision time {} is outside parity window [{}, {})",
            report.recommendation_report_id,
            report.decision_at,
            parity_run.window_start,
            parity_run.window_end
        )));
    }
    if parity_run
        .model_version_id
        .as_ref()
        .is_some_and(|expected| expected != &report.model_version_id)
    {
        return Err(determinism(format!(
            "pre-inference report {} does not match parity model-version scope",
            report.recommendation_report_id
        )));
    }
    Ok(())
}

fn validate_pre_inference_stage_evidence(
    report: &RecommendationReportInfo,
    members: &[MarketSelectionMemberInfo],
    dq: &ReportDataQualitySnapshotInfo,
) -> QuantResult<PreInferenceStageCeiling> {
    let wrong_snapshot = dq.report_data_quality_snapshot_id != report.data_quality_snapshot_ref;
    let wrong_decision = dq.decision_at != report.decision_at;
    let wrong_config = dq.decision_policy_snapshot_id != report.decision_policy_snapshot_id;
    if wrong_snapshot || wrong_decision || wrong_config {
        return Err(determinism(format!(
            "report {} DQ snapshot is not bound to its decision/config",
            report.recommendation_report_id
        )));
    }
    let ceiling = pre_inference_stage_ceiling(report)?;
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
    match ceiling {
        PreInferenceStageCeiling::Selection => {
            if !dq.tokens_json.0.is_empty() {
                return Err(determinism(format!(
                    "selection-only report {} carries feature/DQ vector bindings",
                    report.recommendation_report_id
                )));
            }
            if report.summary_json.empty_reason == Some(EmptyReportReason::EmptySelection)
                && !member_markets.is_empty()
            {
                return Err(determinism(format!(
                    "empty-selection report {} has persisted selection members",
                    report.recommendation_report_id
                )));
            }
        }
        PreInferenceStageCeiling::Feature => {
            let mut vector_ids = HashSet::new();
            let mut dq_markets = BTreeSet::new();
            for record in &dq.tokens_json.0 {
                let feature_vector_id = record.feature_vector_id.as_ref().ok_or_else(|| {
                    determinism(format!(
                        "feature-stage report {} contains legacy-unbound DQ evidence",
                        report.recommendation_report_id
                    ))
                })?;
                if !vector_ids.insert(feature_vector_id.clone())
                    || !dq_markets.insert(record.market_id.clone())
                    || record.status != DataQualityStatus::Insufficient
                {
                    return Err(determinism(format!(
                        "feature-stage report {} has duplicate or non-rejected DQ evidence",
                        report.recommendation_report_id
                    )));
                }
            }
            if dq_markets != member_markets || dq_markets.is_empty() {
                return Err(determinism(format!(
                    "feature-stage report {} DQ vectors do not exactly cover its selection",
                    report.recommendation_report_id
                )));
            }
        }
    }
    Ok(ceiling)
}

fn validate_report_selection_binding(
    report: &RecommendationReportInfo,
    selection: &MarketSelectionInfo,
) -> QuantResult<()> {
    if selection.decision_at != report.decision_at
        || selection.decision_policy_snapshot_id != report.decision_policy_snapshot_id
    {
        return Err(determinism(format!(
            "selection {} is not bound to pre-inference report {} decision/config",
            selection.market_selection_id, report.recommendation_report_id
        )));
    }
    Ok(())
}

fn validate_report_dq_vector_bindings(
    report: &RecommendationReportInfo,
    dq: &ReportDataQualitySnapshotInfo,
    infos: &HashMap<FeatureVectorId, FeatureVectorInfo>,
) -> QuantResult<()> {
    for record in &dq.tokens_json.0 {
        let feature_vector_id = record.feature_vector_id.as_ref().ok_or_else(|| {
            determinism(format!(
                "report {} contains legacy-unbound DQ evidence for market {}",
                report.recommendation_report_id, record.market_id
            ))
        })?;
        let info = infos.get(feature_vector_id).ok_or_else(|| {
            determinism(format!(
                "report {} DQ binding references missing vector {}",
                report.recommendation_report_id, feature_vector_id
            ))
        })?;
        if info.market_id != record.market_id
            || info.token_id.as_ref() != Some(&record.token_id)
            || info.data_quality != record.status
        {
            return Err(determinism(format!(
                "report {} DQ binding disagrees with vector {} identity/state",
                report.recommendation_report_id, feature_vector_id
            )));
        }
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
                Some(run_id.clone()),
                None,
                "serving_evidence_completion_missing",
            )
        })
        .collect()
}

fn pending_report_features(
    report_id: &RecommendationReportId,
    model_version_id: &ModelVersionId,
    candidates: &[&FeatureParityCandidate],
    reason: &str,
) -> Vec<PendingFeatureParityComparison> {
    candidates
        .iter()
        .map(|candidate| PendingFeatureParityComparison {
            sampling_key: candidate.sampling_key.clone(),
            decision_at: candidate.decision_at,
            stage: FeatureParityStage::FeatureCell,
            report_id: Some(report_id.clone()),
            model_run_id: None,
            model_version_id: Some(model_version_id.clone()),
            training_dataset_id: None,
            market_id: candidate.market_id.clone(),
            feature_name: None,
            reason: reason.to_owned(),
            online: None,
            required_watermark: candidate.decision_at,
            observed_watermark: None,
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
                "replay produced no comparison for sampled market {} in run {run_id}",
                selected
                    .market_id
                    .as_ref()
                    .map_or("<empty-selection>", MarketId::as_str),
                run_id = subject
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

fn determinism(detail: String) -> QuantError {
    ResearchError::Determinism { detail }.into()
}

#[cfg(test)]
mod tests {
    use super::{
        DataQualityStatus, EmptyReportReason, FeatureParityCandidate, FeatureParityComparison,
        FeatureParityEvidence, FeatureParityStage, FeatureParitySubject, MarketId, ModelRunId,
        PreInferenceStageCeiling, RecommendationReportId, pending_completion,
        representative_candidate, select_comparisons, validate_pre_inference_stage_evidence,
    };
    use chrono::Utc;
    use quant_pivot_models::{
        domain::{MarketSelectionMemberInfo, ReportDataQualitySnapshotInfo},
        enums::{
            common::MarketCategory,
            market::MarketStatus,
            quant::{RecommendationReportStatus, ReportKind},
        },
        types::{
            EventId, FeatureVectorId, ReportDataQualityTokens, TokenDataQualityRecord, TokenId, Usd,
        },
    };
    use quant_pivot_test_support::report_fixtures;
    use rust_decimal_macros::dec;
    use std::slice;

    fn candidate(
        run_id: &ModelRunId,
        market: &str,
        decision_at: chrono::DateTime<Utc>,
    ) -> FeatureParityCandidate {
        let market_id = MarketId::new(market);
        FeatureParityCandidate {
            sampling_key: format!("{run_id}/{market_id}"),
            subject: FeatureParitySubject::ModelRun(run_id.clone()),
            market_id: Some(market_id),
            decision_at,
        }
    }

    fn comparison(
        run_id: &ModelRunId,
        market_id: Option<MarketId>,
        decision_at: chrono::DateTime<Utc>,
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
            model_run_id: Some(run_id.clone()),
            model_version_id: None,
            training_dataset_id: None,
            market_id,
            feature_name: None,
            reason: None,
            online: evidence.clone(),
            replay: evidence,
            transform_hash: None,
            detail: serde_json::Value::Null,
        }
    }

    #[test]
    fn missing_completion_marks_every_sampled_market_pending() {
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
    fn empty_run_group_returns_typed_error_instead_of_indexing() {
        let run_id = ModelRunId::from_v7();
        assert!(representative_candidate(&run_id.to_string(), &[]).is_err());
    }

    #[test]
    fn replay_projects_market_rows_per_candidate_and_global_rows_once() {
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
    fn pre_inference_stage_ceiling_requires_exact_real_evidence() {
        let report_id = RecommendationReportId::from_v7();
        let mut report = report_fixtures::report(
            report_id,
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        report.model_run_id = None;
        report.summary_json.empty_reason = Some(EmptyReportReason::SystemDegraded);
        let member = MarketSelectionMemberInfo {
            market_selection_id: report.market_selection_id.clone(),
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
            report_data_quality_snapshot_id: report.data_quality_snapshot_ref.clone(),
            decision_at: report.decision_at,
            decision_policy_snapshot_id: report.decision_policy_snapshot_id.clone(),
            tokens_json: ReportDataQualityTokens(Vec::new()),
            created_at: report.created_at,
        };
        assert_eq!(
            validate_pre_inference_stage_evidence(&report, slice::from_ref(&member), &dq)
                .expect("selection-only evidence"),
            PreInferenceStageCeiling::Selection
        );

        report.summary_json.empty_reason = Some(EmptyReportReason::InsufficientDataQuality);
        let feature_vector_id = FeatureVectorId::from_v7();
        dq.tokens_json = ReportDataQualityTokens(vec![TokenDataQualityRecord {
            feature_vector_id: Some(feature_vector_id),
            token_id: member.primary_token_id.clone(),
            market_id: member.market_id.clone(),
            status: DataQualityStatus::Insufficient,
            book_age_ms: 0,
            crossed: false,
            empty: true,
            fact_lag_ms: None,
            missing_required: vec!["book.best_ask".to_owned()],
        }]);
        assert_eq!(
            validate_pre_inference_stage_evidence(&report, &[member], &dq)
                .expect("feature-stage evidence"),
            PreInferenceStageCeiling::Feature
        );

        dq.tokens_json.0[0].status = DataQualityStatus::Fresh;
        assert!(validate_pre_inference_stage_evidence(&report, &[], &dq).is_err());
    }
}
