//! Frozen feedback cohort to immutable training Dataset orchestration.

use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    mem,
    str::FromStr,
    sync::Arc,
};

use chrono::{DateTime, Utc};
use quant_pivot_compute::{ComputeExecutor, OfflineMemory};
use quant_pivot_error::{QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        MarketResolutionRow, QuantFeatureEventRow, QuantModelInputEventRow,
        QuantServingEvidenceCompletionRow, ReportMarketFunnelRow,
    },
    domain::{
        ports::FeedbackDatasetBuildRequest,
        quant::{
            FEEDBACK_COHORT_PAGE_LIMIT, FactorDefinitionInfo, FactorValueInfo, FeatureVectorInfo,
            FeedbackCohortDecision, FeedbackCohortEvidence, FeedbackCohortPageQuery,
            FeedbackCohortSnapshot, FeedbackCohortWindow, FeedbackRecommendationContext,
            FeedbackResolutionEvidence, JobProgressSink, RecommendationReportInfo,
            ReportRouteRunInfo,
        },
    },
    enums::{
        model::ModelFamily,
        quant::{CohortCensorReason, CohortExclusionReason, FeedbackCohort, OutcomeSide},
    },
    hashing::CanonicalDigest,
    types::{
        CapabilityRegistryHashes, CohortCensorCount, CohortExclusionCount, ContentHash,
        DATASET_COHORT_MANIFEST_FORMAT_VERSION, DatasetCohortArtifactRef, DatasetCohortCounts,
        DatasetCohortManifest, DatasetCoverage, FactorDefinitionId, FeatureSourceRefs,
        FeatureVectorId, MODEL_SCORE_COHORT_FORMAT_VERSION, ModelLearningCohortRow, ModelRunId,
        ModelScoreCohortArtifact, ModelScoreCohortRow, ModelVersionId, NewModelLearningCohortRow,
        NewModelScoreCohortRow, RecommendationReportId, ReportFunnelStage, ReportRouteRunId,
        ReportRunId, ResearchJobProgress, SchemaVersion, TokenId, TrainingSampleSource,
        TrainingSampleSources,
        factor::{FactorDefinitionRef, FactorServingPlane},
    },
};
use quant_pivot_repository::traits::{
    FactorRepository, FeatureRepository, FeedbackCohortRepository, QuantFactReadRepository,
    RecommendationReportRepository, ServingEvidenceRepository,
};
use quant_pivot_research::{
    artifact::{ArtifactKey, ArtifactNamespace, ArtifactStore},
    factors::FactorValue,
    features::FeatureVector,
    feedback::{FeedbackCoverageCohorts, FeedbackMatureLabel},
    selection::SelectedMarket,
    training::{
        DatasetPlanRequest, ModelScoreCohortCodec, TOKEN_PAYOUT_RATIO, TrainingDatasetArtifact,
        TrainingExample, TrainingLabel,
    },
};
use tokio_util::sync::CancellationToken;

use crate::{
    observability::serving_evidence::verify_completion,
    service::{
        feedback_cohort::evaluate_feedback_cohort, training_dataset::TrainingDatasetService,
    },
};

/// Dependencies for [`FeedbackDatasetService`].
pub struct FeedbackDatasetServiceDeps {
    pub report_repository: Arc<dyn RecommendationReportRepository>,
    pub fact_repository: Arc<dyn QuantFactReadRepository>,
    pub serving_evidence_repository: Arc<dyn ServingEvidenceRepository>,
    pub feature_repository: Arc<dyn FeatureRepository>,
    pub factor_repository: Arc<dyn FactorRepository>,
    pub artifact_store: Arc<dyn ArtifactStore>,
    pub compute: Arc<ComputeExecutor>,
    pub compute_memory: OfflineMemory,
    pub dataset_service: Arc<TrainingDatasetService>,
}

/// Repository dependencies for the canonical feedback-cohort materializer.
pub(crate) struct FeedbackCohortMaterializerDeps {
    pub cohorts: Arc<dyn FeedbackCohortRepository>,
    pub features: Arc<dyn FeatureRepository>,
    pub factors: Arc<dyn FactorRepository>,
}

/// Single owner of feedback keyset scans and exact serving-row reconstruction.
#[derive(Clone)]
pub(crate) struct FeedbackCohortMaterializer {
    cohorts: Arc<dyn FeedbackCohortRepository>,
    features: Arc<dyn FeatureRepository>,
    factors: Arc<dyn FactorRepository>,
}

/// Frozen all-cohort counts and champion-compatible evaluation rows.
pub(crate) struct FeedbackCoverageMaterialization {
    pub cohorts: FeedbackCoverageCohorts,
    pub mature_labels: Vec<FeedbackMatureLabel>,
    pub new_mature_label_count: u64,
    pub champion_rows: Vec<ModelLearningCohortRow>,
    pub champion_examples: Vec<TrainingExample>,
}

impl FeedbackCohortMaterializer {
    #[must_use]
    pub(crate) fn new(deps: FeedbackCohortMaterializerDeps) -> Self {
        Self {
            cohorts: deps.cohorts,
            features: deps.features,
            factors: deps.factors,
        }
    }
}

/// Seals `ModelLearning` truth and materializes only exact serving rows.
pub struct FeedbackDatasetService {
    score_materializer: ModelScoreCohortMaterializer,
    artifact_store: Arc<dyn ArtifactStore>,
    compute: Arc<ComputeExecutor>,
    compute_memory: OfflineMemory,
    dataset_service: Arc<TrainingDatasetService>,
}

struct EncodedScoreCohort {
    artifact: ModelScoreCohortArtifact,
    source_hash: ContentHash,
    bytes: Vec<u8>,
    bytes_hash: ContentHash,
    schema_hash: ContentHash,
}

/// Complete scored-serving population reader. Recommendation publication is
/// intentionally absent from this boundary.
struct ModelScoreCohortMaterializer {
    reports: Arc<dyn RecommendationReportRepository>,
    facts: Arc<dyn QuantFactReadRepository>,
    evidence: Arc<dyn ServingEvidenceRepository>,
    features: Arc<dyn FeatureRepository>,
    factors: Arc<dyn FactorRepository>,
}

#[derive(Clone)]
struct ScoreFunnelBinding {
    report: RecommendationReportInfo,
    route_run: ReportRouteRunInfo,
    funnel: ReportMarketFunnelRow,
    feature_vector_id: FeatureVectorId,
    model_run_id: ModelRunId,
}

struct ScoreReportBinding {
    report: RecommendationReportInfo,
    route_runs: HashMap<ReportRouteRunId, ReportRouteRunInfo>,
}

struct ScoreInputBinding {
    model_family: ModelFamily,
    input_contract_hash: ContentHash,
    transform_hash: ContentHash,
    training_input_hash: ContentHash,
}

struct ScoreSeed {
    binding: ScoreFunnelBinding,
    serving_evidence_available_at: DateTime<Utc>,
    serving_completion_hash: ContentHash,
    model_input_rows_hash: ContentHash,
    input: ScoreInputBinding,
    resolution: FeedbackResolutionEvidence,
}

struct UnresolvedScoreSeed {
    binding: ScoreFunnelBinding,
    serving_evidence_available_at: DateTime<Utc>,
    serving_completion_hash: ContentHash,
    model_input_rows_hash: ContentHash,
    input: ScoreInputBinding,
}

struct MaterializedModelScores {
    rows: Vec<ModelScoreCohortRow>,
    examples: Vec<TrainingExample>,
    factor_serving_plane: FactorServingPlane,
    coverage: DatasetCoverage,
    counts: DatasetCohortCounts,
    feature_schema_version: SchemaVersion,
    knowledge_lag_secs: u64,
    model_family: ModelFamily,
}

struct ModelScoreRowsAssembly {
    rows: Vec<ModelScoreCohortRow>,
    examples: Vec<TrainingExample>,
    factor_serving_plane: FactorServingPlane,
    counts: DatasetCohortCounts,
    market_count: u64,
    feature_schema_version: Option<SchemaVersion>,
    knowledge_lag_secs: Option<u64>,
    model_family: Option<ModelFamily>,
}

impl FeedbackDatasetService {
    #[must_use]
    pub fn new(deps: FeedbackDatasetServiceDeps) -> Self {
        Self {
            score_materializer: ModelScoreCohortMaterializer {
                reports: deps.report_repository,
                facts: deps.fact_repository,
                evidence: deps.serving_evidence_repository,
                features: deps.feature_repository,
                factors: deps.factor_repository,
            },
            artifact_store: deps.artifact_store,
            compute: deps.compute,
            compute_memory: deps.compute_memory,
            dataset_service: deps.dataset_service,
        }
    }

    /// Seal the cohort, verify serving evidence, and persist the Dataset.
    pub async fn build(
        &self,
        request: FeedbackDatasetBuildRequest,
        progress: Arc<dyn JobProgressSink>,
        cancel: CancellationToken,
    ) -> QuantResult<TrainingDatasetArtifact> {
        request.validate()?;
        let expected_model_spec_hash = request.model_spec_definition_hash;
        let mut materialized = self
            .score_materializer
            .materialize(
                &request.window,
                request.source_lineage.pit_cutoff,
                &progress,
                &cancel,
            )
            .await?;
        let cohort_manifest = self
            .persist_score_cohort(
                request.window.clone(),
                materialized.counts.clone(),
                request.source_lineage.capability_registry_hashes.clone(),
                mem::take(&mut materialized.rows),
                &cancel,
            )
            .await?;
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "feedback Dataset cancelled after cohort seal".to_owned(),
            }
            .into());
        }
        let plan = self
            .dataset_service
            .plan_feedback(
                DatasetPlanRequest {
                    model_spec_id: request.model_spec_id,
                    source_lineage: request.source_lineage,
                    cohort_manifest: Some(cohort_manifest),
                    window_start: request.window.window_start(),
                    window_end: request.window.cutoff(),
                    sample_interval_secs: 0,
                    horizons_secs: vec![0],
                    knowledge_lag_secs: materialized.knowledge_lag_secs,
                    feature_schema_version: materialized.feature_schema_version,
                    sample_sources: TrainingSampleSources::from(
                        TrainingSampleSource::ModelScoreFeedback,
                    ),
                    training_dataset_id: Some(request.training_dataset_id),
                    purpose: request.purpose,
                },
                materialized.factor_serving_plane,
            )
            .await?;
        if plan.model_spec_definition_hash != expected_model_spec_hash {
            return Err(ResearchError::DatasetPlan {
                detail: "feedback Dataset ModelSpec differs from the frozen candidate recipe"
                    .to_owned(),
            }
            .into());
        }
        if plan.model_family != materialized.model_family {
            return Err(ResearchError::DatasetPlan {
                detail: format!(
                    "feedback Dataset recipe family {} differs from completed serving family {}",
                    plan.model_family, materialized.model_family
                ),
            }
            .into());
        }
        Box::pin(self.dataset_service.build_feedback(
            plan,
            materialized.examples,
            materialized.coverage,
            self.compute_memory,
            &cancel,
        ))
        .await
    }
}

impl ModelScoreCohortMaterializer {
    async fn materialize(
        &self,
        window: &FeedbackCohortWindow,
        truth_cutoff: DateTime<Utc>,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<MaterializedModelScores> {
        let reports = self.load_reports(window, truth_cutoff).await?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-model-score-reports",
            u64::try_from(reports.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("serving report count conversion failed: {error}"),
            })?,
        ));
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "model-score cohort cancelled after report freeze".to_owned(),
            }
            .into());
        }
        let bindings = self.load_funnels(window, truth_cutoff, &reports).await?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-model-score-funnel",
            u64::try_from(bindings.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("model-score funnel count conversion failed: {error}"),
            })?,
        ));
        if bindings.is_empty() {
            return Err(ResearchError::NotEligible {
                code: "model_score_cohort_empty",
                detail: "frozen window contains no completed model-scoring population".to_owned(),
            }
            .into());
        }
        let unresolved = self.load_evidence(bindings, truth_cutoff).await?;
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: "model-score cohort cancelled after serving-evidence verification"
                    .to_owned(),
            }
            .into());
        }
        let (seeds, counts) = self.bind_truth(unresolved, window, truth_cutoff).await?;
        progress.report(ResearchJobProgress::indeterminate(
            "feedback-model-score-truth",
            counts.included_count(),
        ));
        self.assemble(seeds, counts).await
    }

    async fn load_reports(
        &self,
        window: &FeedbackCohortWindow,
        truth_cutoff: DateTime<Utc>,
    ) -> QuantResult<HashMap<RecommendationReportId, ScoreReportBinding>> {
        let reports = self
            .reports
            .list_committed_between(window.window_start(), window.cutoff())
            .await?
            .into_iter()
            .filter(|report| report.created_at <= truth_cutoff)
            .collect::<Vec<_>>();
        let mut report_by_run = HashMap::<ReportRunId, RecommendationReportId>::new();
        for report in &reports {
            if report_by_run
                .insert(report.report_run_id, report.recommendation_report_id)
                .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "committed report window contains duplicate run identity {}",
                        report.report_run_id
                    ),
                }
                .into());
            }
        }
        let report_run_ids = report_by_run.keys().copied().collect::<Vec<_>>();
        let mut routes_by_run =
            HashMap::<ReportRunId, HashMap<ReportRouteRunId, ReportRouteRunInfo>>::new();
        for route_run in self.reports.find_route_runs(&report_run_ids).await? {
            let report_run_id = route_run.report_run_id;
            let report_route_run_id = route_run.report_route_run_id;
            if !report_by_run.contains_key(&report_run_id) {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "Route run {report_route_run_id} belongs to unrequested report run {report_run_id}"
                    ),
                }
                .into());
            }
            if routes_by_run
                .entry(report_run_id)
                .or_default()
                .insert(report_route_run_id, route_run)
                .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "report run {report_run_id} returned duplicate Route-run identity {report_route_run_id}"
                    ),
                }
                .into());
            }
        }
        let mut by_id = HashMap::new();
        for report in reports {
            let route_runs = routes_by_run
                .remove(&report.report_run_id)
                .unwrap_or_default()
                .into_iter()
                .filter(|(_, route_run)| {
                    route_run.lineage_json.as_ref().is_some_and(|lineage| {
                        lineage.research_profile_ref == *window.profile_ref()
                    })
                })
                .collect::<HashMap<_, _>>();
            if route_runs.is_empty() {
                continue;
            }
            let report_id = report.recommendation_report_id;
            if by_id
                .insert(report_id, ScoreReportBinding { report, route_runs })
                .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: "committed report query returned a duplicate report identity"
                        .to_owned(),
                }
                .into());
            }
        }
        if !routes_by_run.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: "batched Route-run query returned orphan report-run groups".to_owned(),
            }
            .into());
        }
        Ok(by_id)
    }

    async fn load_funnels(
        &self,
        window: &FeedbackCohortWindow,
        truth_cutoff: DateTime<Utc>,
        reports: &HashMap<RecommendationReportId, ScoreReportBinding>,
    ) -> QuantResult<Vec<ScoreFunnelBinding>> {
        let mut bindings = Vec::new();
        let mut identities = HashSet::new();
        for funnel in self
            .facts
            .report_funnel_between(
                window.window_start().timestamp_millis(),
                window.cutoff().timestamp_millis(),
            )
            .await?
        {
            let Some(report_binding) = reports.get(&funnel.recommendation_report_id) else {
                continue;
            };
            if funnel.ingestion_time > truth_cutoff.timestamp_millis() {
                continue;
            }
            let stage = ReportFunnelStage::from_str(&funnel.terminal_stage).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("decode conserved report-funnel stage: {error}"),
                }
            })?;
            let route_run_id =
                funnel
                    .report_route_run_id
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "scored funnel row {}/{} has no Route-run identity",
                            funnel.recommendation_report_id, funnel.market_id
                        ),
                    })?;
            let route_run = report_binding
                .route_runs
                .get(&route_run_id)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "funnel Route run {route_run_id} is outside the requested profile cohort"
                    ),
                })?;
            Self::validate_funnel(&report_binding.report, route_run, &funnel)?;
            if stage < ReportFunnelStage::ModelScored {
                continue;
            }
            let feature_vector_id =
                funnel
                    .feature_vector_id
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "scored funnel row {}/{} has no feature-vector identity",
                            funnel.recommendation_report_id, funnel.market_id
                        ),
                    })?;
            let model_run_id = funnel
                .model_run_id
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!(
                        "scored funnel row {}/{} has no model-run identity",
                        funnel.recommendation_report_id, funnel.market_id
                    ),
                })?;
            if !identities.insert((model_run_id, feature_vector_id)) {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "serving run {model_run_id} conserves feature vector {feature_vector_id} more than once"
                    ),
                }
                .into());
            }
            bindings.push(ScoreFunnelBinding {
                report: report_binding.report.clone(),
                route_run: route_run.clone(),
                funnel,
                feature_vector_id,
                model_run_id,
            });
        }
        bindings.sort_by_key(|binding| {
            (
                binding.report.recommendation_report_id.as_uuid(),
                binding.funnel.market_id.clone(),
            )
        });
        Ok(bindings)
    }

    fn validate_funnel(
        report: &RecommendationReportInfo,
        route_run: &ReportRouteRunInfo,
        funnel: &ReportMarketFunnelRow,
    ) -> QuantResult<()> {
        let valid = funnel.decision_policy_snapshot_id == report.decision_policy_snapshot_id
            && funnel.report_route_run_id == Some(route_run.report_route_run_id)
            && funnel.route.as_deref() == Some(route_run.route.as_str())
            && funnel.model_version_id == route_run.model_version_id
            && funnel.model_run_id == route_run.model_run_id
            && funnel.market_selection_id == report.market_selection_id
            && funnel.event_time == report.decision_at.timestamp_millis();
        if !valid {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "report-funnel row {}/{} disagrees with its committed report header",
                    funnel.recommendation_report_id, funnel.market_id
                ),
            }
            .into());
        }
        funnel
            .verify_hash()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!(
                    "report-funnel row {}/{} failed semantic hash verification: {error}",
                    funnel.recommendation_report_id, funnel.market_id
                ),
            })?;
        Ok(())
    }

    async fn load_evidence(
        &self,
        bindings: Vec<ScoreFunnelBinding>,
        truth_cutoff: DateTime<Utc>,
    ) -> QuantResult<Vec<UnresolvedScoreSeed>> {
        let mut run_ids = bindings
            .iter()
            .map(|binding| binding.model_run_id)
            .collect::<Vec<_>>();
        run_ids.sort_by_key(|id| id.as_uuid());
        run_ids.dedup();
        let completions =
            Self::canonical_completions(self.evidence.completions_for_runs(&run_ids).await?)?;
        let inputs = Self::canonical_inputs(self.evidence.model_inputs_for_runs(&run_ids).await?)?;
        let mut marker_vectors = Vec::new();
        for run_id in &run_ids {
            let marker = completions
                .get(run_id)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!("serving run {run_id} has no durable completion marker"),
                })?;
            marker_vectors.extend(Self::completion_vectors(marker)?);
        }
        marker_vectors.sort_by_key(|id| id.as_uuid());
        marker_vectors.dedup();
        let feature_rows = Self::canonical_features(
            self.evidence
                .feature_cells_for_vectors(&marker_vectors)
                .await?,
        )?;
        let mut inputs_by_run = HashMap::<ModelRunId, Vec<QuantModelInputEventRow>>::new();
        for row in inputs {
            inputs_by_run.entry(row.model_run_id).or_default().push(row);
        }
        let mut features_by_vector = HashMap::<FeatureVectorId, Vec<QuantFeatureEventRow>>::new();
        for row in feature_rows {
            features_by_vector
                .entry(row.feature_vector_id)
                .or_default()
                .push(row);
        }
        let mut bindings_by_run = HashMap::<ModelRunId, Vec<ScoreFunnelBinding>>::new();
        for binding in bindings {
            bindings_by_run
                .entry(binding.model_run_id)
                .or_default()
                .push(binding);
        }

        let mut seeds = Vec::new();
        for run_id in run_ids {
            let marker = completions
                .get(&run_id)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: format!("serving completion {run_id} disappeared during assembly"),
                })?;
            let available_at = Self::utc_millis(marker.ingestion_time, "completion ingestion")?;
            if available_at > truth_cutoff {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "serving completion {run_id} was not available at the frozen truth cutoff"
                    ),
                }
                .into());
            }
            let vector_ids = Self::completion_vectors(marker)?;
            let mut run_features = Vec::new();
            for vector_id in &vector_ids {
                let rows = features_by_vector.get(vector_id).ok_or_else(|| {
                    ResearchError::DatasetBuild {
                        detail: format!(
                            "serving completion {run_id} references missing feature evidence {vector_id}"
                        ),
                    }
                })?;
                run_features.extend(rows.iter().cloned());
            }
            let run_inputs = inputs_by_run.remove(&run_id).unwrap_or_default();
            verify_completion(marker, &run_features, &run_inputs)?;
            let run_bindings =
                bindings_by_run
                    .remove(&run_id)
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!("serving run {run_id} lost its report-funnel bindings"),
                    })?;
            let scored_vectors = run_bindings
                .iter()
                .map(|binding| binding.feature_vector_id)
                .collect::<HashSet<_>>();
            let input_vectors = run_inputs
                .iter()
                .map(|row| row.feature_vector_id)
                .collect::<HashSet<_>>();
            if scored_vectors != input_vectors {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "serving run {run_id} model-input population disagrees with its conserved scored funnel"
                    ),
                }
                .into());
            }
            let completion_hash =
                marker
                    .completion_hash
                    .parse::<ContentHash>()
                    .map_err(|error| ResearchError::DatasetBuild {
                        detail: format!("invalid completion hash for run {run_id}: {error}"),
                    })?;
            let input_rows_hash = marker
                .model_input_rows_hash
                .parse::<ContentHash>()
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("invalid model-input hash for run {run_id}: {error}"),
                })?;
            for binding in run_bindings {
                let vector_inputs = run_inputs
                    .iter()
                    .filter(|row| row.feature_vector_id == binding.feature_vector_id)
                    .collect::<Vec<_>>();
                let input = Self::bind_input(&binding, &vector_inputs)?;
                seeds.push(UnresolvedScoreSeed {
                    binding,
                    serving_evidence_available_at: available_at,
                    serving_completion_hash: completion_hash,
                    model_input_rows_hash: input_rows_hash,
                    input,
                });
            }
        }
        seeds.sort_by_key(|seed| {
            (
                seed.serving_evidence_available_at,
                seed.binding.report.recommendation_report_id.as_uuid(),
                seed.binding.feature_vector_id.as_uuid(),
            )
        });
        Ok(seeds)
    }

    fn bind_input(
        binding: &ScoreFunnelBinding,
        rows: &[&QuantModelInputEventRow],
    ) -> QuantResult<ScoreInputBinding> {
        let first = rows.first().ok_or_else(|| ResearchError::DatasetBuild {
            detail: format!(
                "scored feature vector {} has no model-input evidence",
                binding.feature_vector_id
            ),
        })?;
        let valid = rows.iter().all(|row| {
            row.model_run_id == binding.model_run_id
                && Some(row.model_version_id) == binding.route_run.model_version_id
                && row.market_id == binding.funnel.market_id
                && row.feature_vector_id == binding.feature_vector_id
                && row.decision_at == binding.report.decision_at.timestamp_millis()
                && row.model_family == first.model_family
                && row.input_contract_hash == first.input_contract_hash
                && row.transform_hash == first.transform_hash
                && row.training_input_hash == first.training_input_hash
        });
        if !valid {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "model-input evidence for feature vector {} has contradictory serving lineage",
                    binding.feature_vector_id
                ),
            }
            .into());
        }
        Ok(ScoreInputBinding {
            model_family: ModelFamily::from_str(&first.model_family).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "feature vector {} has unknown serving model family: {error}",
                        binding.feature_vector_id
                    ),
                }
            })?,
            input_contract_hash: Self::parse_hash(
                &first.input_contract_hash,
                "model-input contract",
            )?,
            transform_hash: Self::parse_hash(&first.transform_hash, "model-input transform")?,
            training_input_hash: Self::parse_hash(&first.training_input_hash, "training input")?,
        })
    }

    async fn bind_truth(
        &self,
        unresolved: Vec<UnresolvedScoreSeed>,
        window: &FeedbackCohortWindow,
        truth_cutoff: DateTime<Utc>,
    ) -> QuantResult<(Vec<ScoreSeed>, DatasetCohortCounts)> {
        let candidate_count =
            u64::try_from(unresolved.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("model-score candidate count conversion failed: {error}"),
            })?;
        let mut market_ids = unresolved
            .iter()
            .map(|seed| seed.binding.funnel.market_id.clone())
            .collect::<Vec<_>>();
        market_ids.sort();
        market_ids.dedup();
        let resolutions = self
            .facts
            .resolutions_between(
                market_ids,
                window.window_start().timestamp_millis(),
                truth_cutoff.timestamp_millis(),
                truth_cutoff.timestamp_millis(),
            )
            .await?;
        let resolutions = resolutions
            .into_iter()
            .map(|resolution| (resolution.market_id.clone(), resolution))
            .collect::<HashMap<_, _>>();
        let mut seeds = Vec::new();
        let mut censored = 0_u64;
        for seed in unresolved {
            let Some(resolution) = resolutions.get(&seed.binding.funnel.market_id) else {
                censored = censored
                    .checked_add(1)
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: "model-score resolution censor count overflowed u64".to_owned(),
                    })?;
                continue;
            };
            seeds.push(ScoreSeed {
                resolution: Self::resolution_evidence(
                    resolution,
                    &seed.binding.funnel.primary_token_id,
                )?,
                binding: seed.binding,
                serving_evidence_available_at: seed.serving_evidence_available_at,
                serving_completion_hash: seed.serving_completion_hash,
                model_input_rows_hash: seed.model_input_rows_hash,
                input: seed.input,
            });
        }
        if seeds.is_empty() {
            return Err(ResearchError::NotEligible {
                code: "model_score_labels_unavailable",
                detail: "complete scored-serving population has no mature resolution labels"
                    .to_owned(),
            }
            .into());
        }
        let included = u64::try_from(seeds.len()).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("model-score included count conversion failed: {error}"),
        })?;
        let censors = if censored == 0 {
            Vec::new()
        } else {
            vec![CohortCensorCount {
                reason: CohortCensorReason::ResolutionUnavailableAtCutoff,
                count: censored,
            }]
        };
        let counts =
            DatasetCohortCounts::try_new(candidate_count, included, included, Vec::new(), censors)
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("model-score cohort counts do not reconcile: {error}"),
                })?;
        Ok((seeds, counts))
    }

    fn resolution_evidence(
        resolution: &MarketResolutionRow,
        token_id: &TokenId,
    ) -> QuantResult<FeedbackResolutionEvidence> {
        let token_payout_ratio =
            resolution
                .payout_for(token_id)
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!(
                        "resolution {} cannot label token {token_id}: {error}",
                        resolution.market_id
                    ),
                })?;
        let resolution_kind =
            resolution
                .resolution_kind()
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!(
                        "resolution {} has invalid payout semantics: {error}",
                        resolution.market_id
                    ),
                })?;
        Ok(FeedbackResolutionEvidence {
            resolution_kind,
            token_payout_ratio,
            resolved_at: Self::utc_millis(resolution.resolved_at, "resolution time")?,
            available_at: Self::utc_millis(resolution.observed_at, "resolution availability")?,
            outcome_hash: resolution.resolution_fact_hash,
        })
    }

    async fn assemble(
        &self,
        seeds: Vec<ScoreSeed>,
        counts: DatasetCohortCounts,
    ) -> QuantResult<MaterializedModelScores> {
        let feature_ids = seeds
            .iter()
            .map(|seed| seed.binding.feature_vector_id)
            .collect::<Vec<_>>();
        let features = self.features.find_by_ids(&feature_ids).await?;
        let features = features
            .into_iter()
            .map(|feature| (feature.feature_vector_id, feature))
            .collect::<HashMap<_, _>>();
        if features.len() != feature_ids.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_feature_vector"),
                "model-score cohort references a missing feature vector",
            )
            .into());
        }
        let factor_rows = self.factors.find_values_by_vectors(&feature_ids).await?;
        let mut factors_by_vector = HashMap::<FeatureVectorId, Vec<FactorValueInfo>>::new();
        for row in factor_rows {
            factors_by_vector
                .entry(row.feature_vector_id)
                .or_default()
                .push(row);
        }
        let expected_factor_ids = Self::factor_contract(&seeds, &factors_by_vector)?;
        let definitions = self
            .factors
            .find_definitions_by_ids(&expected_factor_ids)
            .await?
            .into_iter()
            .map(|definition| (definition.factor_definition_id, definition))
            .collect::<HashMap<_, _>>();
        if definitions.len() != expected_factor_ids.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_factor_definition"),
                "model-score cohort references a missing factor definition",
            )
            .into());
        }
        let factor_serving_plane = if expected_factor_ids.is_empty() {
            FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
                detail: format!("seal model-score factor-free plane: {error}"),
            })?
        } else {
            let revisions = expected_factor_ids
                .iter()
                .map(|id| {
                    let definition = definitions.get(id).ok_or_else(|| {
                        StorageError::invariant_violation(
                            Some("quant_factor_definition"),
                            format!("factor definition {id} disappeared during assembly"),
                        )
                    })?;
                    FactorDefinitionRef::try_from(definition).map_err(|error| {
                        ResearchError::DatasetBuild {
                            detail: format!("reconstruct factor revision {id}: {error}"),
                        }
                        .into()
                    })
                })
                .collect::<QuantResult<Vec<_>>>()?;
            FactorServingPlane::try_seal(revisions).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("seal model-score factor serving plane: {error}"),
                }
            })?
        };
        Self::assemble_rows(
            seeds,
            counts,
            &features,
            factors_by_vector,
            &definitions,
            factor_serving_plane,
        )
    }

    fn factor_contract(
        seeds: &[ScoreSeed],
        factors_by_vector: &HashMap<FeatureVectorId, Vec<FactorValueInfo>>,
    ) -> QuantResult<Vec<FactorDefinitionId>> {
        let mut expected = None;
        for seed in seeds {
            let mut ids = factors_by_vector
                .get(&seed.binding.feature_vector_id)
                .into_iter()
                .flatten()
                .filter(|row| row.model_run_id == seed.binding.model_run_id)
                .map(|row| row.factor_definition_id)
                .collect::<Vec<_>>();
            ids.sort_by_key(|id| id.as_uuid());
            if ids.windows(2).any(|pair| pair[0] == pair[1]) {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "feature vector {} has duplicate factor rows for serving run {}",
                        seed.binding.feature_vector_id, seed.binding.model_run_id
                    ),
                }
                .into());
            }
            match &expected {
                Some(expected) if expected != &ids => {
                    return Err(ResearchError::DatasetBuild {
                        detail: "one model-score Dataset cannot mix factor contracts".to_owned(),
                    }
                    .into());
                }
                None => expected = Some(ids),
                Some(_) => {}
            }
        }
        Ok(expected.unwrap_or_default())
    }

    fn assemble_rows(
        seeds: Vec<ScoreSeed>,
        counts: DatasetCohortCounts,
        features: &HashMap<FeatureVectorId, FeatureVectorInfo>,
        mut factors_by_vector: HashMap<FeatureVectorId, Vec<FactorValueInfo>>,
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
        factor_serving_plane: FactorServingPlane,
    ) -> QuantResult<MaterializedModelScores> {
        let mut rows = Vec::with_capacity(seeds.len());
        let mut examples = Vec::with_capacity(seeds.len());
        let mut feature_schema_version = None;
        let mut knowledge_lag_secs = None;
        let mut model_family = None;
        let mut markets = HashSet::new();
        for seed in seeds {
            let feature = features
                .get(&seed.binding.feature_vector_id)
                .ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_feature_vector"),
                        format!(
                            "feature vector {} disappeared during assembly",
                            seed.binding.feature_vector_id
                        ),
                    )
                })?;
            Self::validate_score_feature(&seed, feature)?;
            let vector = FeatureVector::try_from(feature)?;
            let factor_values = Self::score_factors(
                &seed,
                factors_by_vector
                    .remove(&seed.binding.feature_vector_id)
                    .unwrap_or_default(),
                definitions,
                &factor_serving_plane,
            )?;
            let snapshot = &feature.decision_capture.snapshot;
            let model_token_id =
                vector
                    .token_id
                    .clone()
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!(
                            "scored feature vector {} has no primary token",
                            seed.binding.feature_vector_id
                        ),
                    })?;
            let payout = seed.resolution.token_payout_ratio;
            let row = ModelScoreCohortRow::try_seal(NewModelScoreCohortRow {
                recommendation_report_id: seed.binding.report.recommendation_report_id,
                category: snapshot.selection.category,
                market_id: seed.binding.funnel.market_id.clone(),
                event_id: seed.binding.funnel.event_id.clone(),
                model_token_id: model_token_id.clone(),
                decision_at: seed.binding.report.decision_at,
                serving_evidence_available_at: seed.serving_evidence_available_at,
                decision_policy_snapshot_id: seed.binding.report.decision_policy_snapshot_id,
                market_selection_id: seed.binding.report.market_selection_id,
                feature_vector_id: seed.binding.feature_vector_id,
                model_run_id: seed.binding.model_run_id,
                model_version_id: seed.binding.route_run.model_version_id.ok_or_else(|| {
                    ResearchError::DatasetBuild {
                        detail: "scored Route run has no model version".to_owned(),
                    }
                })?,
                model_family: seed.input.model_family,
                factor_definition_versions: {
                    let mut ids = factor_serving_plane
                        .definitions()
                        .iter()
                        .map(FactorDefinitionRef::factor_definition_id)
                        .collect::<Vec<_>>();
                    ids.sort_by_key(|id| id.as_uuid());
                    ids
                },
                book_snapshot_ref: snapshot.book_snapshot_ref.clone(),
                data_quality_snapshot_id: seed.binding.report.data_quality_snapshot_ref,
                resolution: seed.resolution,
                model_token_payout_ratio: payout,
                serving_completion_hash: seed.serving_completion_hash,
                model_input_rows_hash: seed.model_input_rows_hash,
                input_contract_hash: seed.input.input_contract_hash,
                transform_hash: seed.input.transform_hash,
                training_input_hash: seed.input.training_input_hash,
                funnel_row_hash: Self::parse_hash(
                    &seed.binding.funnel.row_hash,
                    "report-funnel row",
                )?,
            })
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("seal model-score cohort row: {error}"),
            })?;
            examples.push(TrainingExample {
                example_id: row.example_id,
                market_id: row.market_id.clone(),
                token_id: model_token_id,
                selected_market: SelectedMarket::from(&snapshot.selection),
                decision_boundary: feature.decision_boundary.clone(),
                sample_source: TrainingSampleSource::ModelScoreFeedback,
                feature_vector: vector,
                factor_values,
                labels: vec![TrainingLabel {
                    label_name: TOKEN_PAYOUT_RATIO,
                    horizon_secs: 0,
                    value: row.model_token_payout_ratio.inner(),
                    is_resolved: true,
                    matured_at: row.resolution.resolved_at,
                }],
                source_refs: feature.source_refs.0.clone(),
                decision_capture: Some(feature.decision_capture.clone()),
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            });
            feature_schema_version = FeedbackCohortMaterializer::one_schema(
                feature_schema_version,
                feature.feature_schema_version,
            )?;
            knowledge_lag_secs = FeedbackCohortMaterializer::one_lag(
                knowledge_lag_secs,
                feature.decision_boundary.knowledge_lag_secs(),
            )?;
            if model_family.is_some_and(|family| family != row.model_family) {
                return Err(ResearchError::DatasetBuild {
                    detail: "one model-score Dataset cannot mix model families".to_owned(),
                }
                .into());
            }
            model_family = Some(row.model_family);
            markets.insert(row.market_id.clone());
            rows.push(row);
        }
        let market_count =
            u64::try_from(markets.len()).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("model-score market count conversion failed: {error}"),
            })?;
        Self::finalize_rows(ModelScoreRowsAssembly {
            rows,
            examples,
            factor_serving_plane,
            counts,
            market_count,
            feature_schema_version,
            knowledge_lag_secs,
            model_family,
        })
    }

    fn finalize_rows(mut assembly: ModelScoreRowsAssembly) -> QuantResult<MaterializedModelScores> {
        assembly.rows.sort_by_key(|row| {
            (
                row.serving_evidence_available_at,
                row.recommendation_report_id.as_uuid(),
                row.feature_vector_id.as_uuid(),
            )
        });
        assembly.examples.sort_by(|left, right| {
            (
                left.market_id.as_str(),
                left.token_id.as_str(),
                left.decision_at(),
                left.example_id.as_uuid(),
            )
                .cmp(&(
                    right.market_id.as_str(),
                    right.token_id.as_str(),
                    right.decision_at(),
                    right.example_id.as_uuid(),
                ))
        });
        let included = u64::try_from(assembly.examples.len()).map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("model-score example count conversion failed: {error}"),
            }
        })?;
        Ok(MaterializedModelScores {
            rows: assembly.rows,
            examples: assembly.examples,
            factor_serving_plane: assembly.factor_serving_plane,
            coverage: DatasetCoverage {
                planned_samples: assembly.counts.candidate_count(),
                built_examples: included,
                markets: assembly.market_count,
                labels_available: included,
                labels_not_mature: assembly.counts.candidate_count() - included,
                ..DatasetCoverage::default()
            },
            counts: assembly.counts,
            feature_schema_version: assembly.feature_schema_version.ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: "model-score materialization produced no feature schema".to_owned(),
                }
            })?,
            knowledge_lag_secs: assembly.knowledge_lag_secs.ok_or_else(|| {
                ResearchError::DatasetBuild {
                    detail: "model-score materialization produced no knowledge-lag contract"
                        .to_owned(),
                }
            })?,
            model_family: assembly
                .model_family
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "model-score materialization produced no model family".to_owned(),
                })?,
        })
    }

    fn validate_score_feature(seed: &ScoreSeed, feature: &FeatureVectorInfo) -> QuantResult<()> {
        let capture_hash = CanonicalDigest::content_hash_json(&feature.decision_capture)?;
        let snapshot = &feature.decision_capture.snapshot;
        let selection = &snapshot.selection;
        let mut mismatches = Vec::new();
        if feature.feature_vector_id != seed.binding.feature_vector_id {
            mismatches.push("feature_vector_id");
        }
        if feature.market_id != seed.binding.funnel.market_id {
            mismatches.push("feature_market_id");
        }
        if feature.token_id.as_ref() != Some(&seed.binding.funnel.primary_token_id) {
            mismatches.push("feature_token_id");
        }
        if feature.decision_at != seed.binding.report.decision_at {
            mismatches.push("decision_at");
        }
        if feature.decision_boundary != snapshot.boundary {
            mismatches.push("decision_boundary");
        }
        if feature.created_at > seed.serving_evidence_available_at {
            mismatches.push("created_after_serving_completion");
        }
        if feature.decision_capture_hash != capture_hash {
            mismatches.push("decision_capture_hash");
        }
        if snapshot.market_id != seed.binding.funnel.market_id {
            mismatches.push("snapshot_market_id");
        }
        if snapshot.event_id != seed.binding.funnel.event_id {
            mismatches.push("snapshot_event_id");
        }
        if snapshot.token_id != selection.primary_token_id {
            mismatches.push("snapshot_primary_token_id");
        }
        if selection.primary_token_id != seed.binding.funnel.primary_token_id {
            mismatches.push("selection_primary_token_id");
        }
        if selection.market_id != seed.binding.funnel.market_id {
            mismatches.push("selection_market_id");
        }
        if selection.event_id != seed.binding.funnel.event_id {
            mismatches.push("selection_event_id");
        }
        if feature.decision_capture.data_quality != feature.data_quality {
            mismatches.push("data_quality");
        }
        if !mismatches.is_empty() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature vector {} disagrees with its completed serving/funnel evidence: mismatches={mismatches:?}, feature_created_at={}, serving_available_at={}, feature_decision_at={}, report_decision_at={}, feature_token_id={:?}, funnel_primary_token_id={}",
                    seed.binding.feature_vector_id,
                    feature.created_at,
                    seed.serving_evidence_available_at,
                    feature.decision_at,
                    seed.binding.report.decision_at,
                    feature.token_id,
                    seed.binding.funnel.primary_token_id,
                ),
            }
            .into());
        }
        let vector = FeatureVector::try_from(feature)?;
        if FeatureSourceRefs(vector.evidence_refs()) != feature.source_refs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature vector {} source references do not reproduce",
                    seed.binding.feature_vector_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn score_factors(
        seed: &ScoreSeed,
        rows: Vec<FactorValueInfo>,
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
        serving_plane: &FactorServingPlane,
    ) -> QuantResult<Vec<FactorValue>> {
        let mut by_definition = HashMap::new();
        for row in rows
            .into_iter()
            .filter(|row| row.model_run_id == seed.binding.model_run_id)
        {
            if row.market_id != seed.binding.funnel.market_id
                || row.decision_at != seed.binding.report.decision_at
                || by_definition
                    .insert(row.factor_definition_id, row)
                    .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "feature vector {} has contradictory factor serving rows",
                        seed.binding.feature_vector_id
                    ),
                }
                .into());
            }
        }
        serving_plane
            .definitions()
            .iter()
            .map(|revision| {
                let id = revision.factor_definition_id();
                let value = by_definition.get(&id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_value"),
                        format!("factor value {id} disappeared during assembly"),
                    )
                })?;
                let definition = definitions.get(&id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_definition"),
                        format!("factor definition {id} disappeared during assembly"),
                    )
                })?;
                let factor = FactorValue::try_from_persistence(value, definition)?;
                factor.validate_against(revision)?;
                Ok(factor)
            })
            .collect()
    }

    fn canonical_completions(
        rows: Vec<QuantServingEvidenceCompletionRow>,
    ) -> QuantResult<HashMap<ModelRunId, QuantServingEvidenceCompletionRow>> {
        let mut canonical = HashMap::<ModelRunId, QuantServingEvidenceCompletionRow>::new();
        for row in rows {
            if let Some(existing) = canonical.get_mut(&row.model_run_id) {
                let mut left = existing.clone();
                let mut right = row.clone();
                left.ingestion_time = 0;
                right.ingestion_time = 0;
                if left != right {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "serving run {} has conflicting completion rows",
                            row.model_run_id
                        ),
                    }
                    .into());
                }
                existing.ingestion_time = existing.ingestion_time.min(row.ingestion_time);
            } else {
                canonical.insert(row.model_run_id, row);
            }
        }
        Ok(canonical)
    }

    fn canonical_inputs(
        rows: Vec<QuantModelInputEventRow>,
    ) -> QuantResult<Vec<QuantModelInputEventRow>> {
        let mut canonical =
            HashMap::<(ModelRunId, FeatureVectorId, String, String), QuantModelInputEventRow>::new(
            );
        for row in rows {
            let key = (
                row.model_run_id,
                row.feature_vector_id,
                row.raw_input_name.clone(),
                row.encoded_column.clone(),
            );
            if let Some(existing) = canonical.get_mut(&key) {
                let mut left = existing.clone();
                let mut right = row.clone();
                left.ingestion_time = 0;
                right.ingestion_time = 0;
                if left != right {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "serving run {} has conflicting model-input retries for feature vector {}",
                            row.model_run_id, row.feature_vector_id
                        ),
                    }
                    .into());
                }
                existing.ingestion_time = existing.ingestion_time.min(row.ingestion_time);
            } else {
                canonical.insert(key, row);
            }
        }
        let mut rows = canonical.into_values().collect::<Vec<_>>();
        rows.sort_by_key(|row| {
            (
                row.model_run_id.as_uuid(),
                row.feature_vector_id.as_uuid(),
                row.raw_input_name.clone(),
                row.encoded_column.clone(),
            )
        });
        Ok(rows)
    }

    fn canonical_features(
        rows: Vec<QuantFeatureEventRow>,
    ) -> QuantResult<Vec<QuantFeatureEventRow>> {
        let mut canonical = HashMap::<(FeatureVectorId, String), QuantFeatureEventRow>::new();
        for row in rows {
            let key = (row.feature_vector_id, row.feature_name.clone());
            if let Some(existing) = canonical.get_mut(&key) {
                let mut left = existing.clone();
                let mut right = row.clone();
                left.ingestion_time = 0;
                right.ingestion_time = 0;
                if left != right {
                    return Err(ResearchError::DatasetBuild {
                        detail: format!(
                            "feature vector {} has conflicting durable feature retries for {}",
                            row.feature_vector_id, row.feature_name
                        ),
                    }
                    .into());
                }
                existing.ingestion_time = existing.ingestion_time.min(row.ingestion_time);
            } else {
                canonical.insert(key, row);
            }
        }
        let mut rows = canonical.into_values().collect::<Vec<_>>();
        rows.sort_by_key(|row| (row.feature_vector_id.as_uuid(), row.feature_name.clone()));
        Ok(rows)
    }

    fn completion_vectors(
        marker: &QuantServingEvidenceCompletionRow,
    ) -> QuantResult<Vec<FeatureVectorId>> {
        serde_json::from_str::<Vec<FeatureVectorId>>(&marker.feature_vector_ids_json).map_err(
            |error| {
                ResearchError::DatasetBuild {
                    detail: format!(
                        "decode serving completion vectors for {}: {error}",
                        marker.model_run_id
                    ),
                }
                .into()
            },
        )
    }

    fn parse_hash(value: &str, subject: &str) -> QuantResult<ContentHash> {
        value.parse::<ContentHash>().map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("invalid {subject} hash: {error}"),
            }
            .into()
        })
    }

    fn utc_millis(value: i64, subject: &str) -> QuantResult<DateTime<Utc>> {
        DateTime::from_timestamp_millis(value).ok_or_else(|| {
            ResearchError::DatasetBuild {
                detail: format!("{subject} is outside the supported UTC range"),
            }
            .into()
        })
    }
}

#[derive(Debug)]
struct EligibleFeedback {
    context: FeedbackRecommendationContext,
    resolution: FeedbackResolutionEvidence,
}

#[derive(Default)]
struct CohortScan {
    candidate_count: u64,
    eligible_count: u64,
    eligible: Vec<EligibleFeedback>,
    exclusions: HashMap<CohortExclusionReason, u64>,
    censors: HashMap<CohortCensorReason, u64>,
}

impl CohortScan {
    fn record(
        &mut self,
        cohort: FeedbackCohort,
        context: FeedbackRecommendationContext,
        decision: FeedbackCohortDecision,
    ) -> QuantResult<()> {
        self.candidate_count =
            self.candidate_count
                .checked_add(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "feedback cohort candidate count overflowed u64".to_owned(),
                })?;
        match (cohort, decision) {
            (
                FeedbackCohort::ModelLearning,
                FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ModelLearning(resolution)),
            ) => {
                self.increment_eligible()?;
                self.eligible.push(EligibleFeedback {
                    context,
                    resolution,
                });
            }
            (
                FeedbackCohort::ExecutionLearning,
                FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::ExecutionLearning(_)),
            )
            | (
                FeedbackCohort::PolicyEvaluation,
                FeedbackCohortDecision::Eligible(FeedbackCohortEvidence::PolicyEvaluation {
                    ..
                }),
            ) => {
                self.increment_eligible()?;
            }
            (_, FeedbackCohortDecision::Eligible(_)) => {
                return Err(ResearchError::DatasetBuild {
                    detail: format!("{cohort} scan produced evidence from another cohort"),
                }
                .into());
            }
            (_, FeedbackCohortDecision::Excluded(reason)) => {
                Self::increment(&mut self.exclusions, reason)?;
            }
            (_, FeedbackCohortDecision::Censored(reason)) => {
                Self::increment(&mut self.censors, reason)?;
            }
        }
        Ok(())
    }

    fn increment_eligible(&mut self) -> QuantResult<()> {
        self.eligible_count =
            self.eligible_count
                .checked_add(1)
                .ok_or_else(|| ResearchError::DatasetBuild {
                    detail: "feedback cohort eligible count overflowed u64".to_owned(),
                })?;
        Ok(())
    }

    fn increment<K>(counts: &mut HashMap<K, u64>, reason: K) -> QuantResult<()>
    where
        K: Eq + Hash,
    {
        let count = counts.entry(reason).or_default();
        *count = count
            .checked_add(1)
            .ok_or_else(|| ResearchError::DatasetBuild {
                detail: "feedback cohort reason count overflowed u64".to_owned(),
            })?;
        Ok(())
    }

    fn counts(&self, included: usize) -> QuantResult<DatasetCohortCounts> {
        let included = u64::try_from(included).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("feedback included count conversion failed: {error}"),
        })?;
        let mut exclusions = self
            .exclusions
            .iter()
            .map(|(reason, count)| CohortExclusionCount {
                reason: *reason,
                count: *count,
            })
            .collect::<Vec<_>>();
        exclusions.sort_by_key(|entry| entry.reason.as_str());
        let mut censors = self
            .censors
            .iter()
            .map(|(reason, count)| CohortCensorCount {
                reason: *reason,
                count: *count,
            })
            .collect::<Vec<_>>();
        censors.sort_by_key(|entry| entry.reason.as_str());
        DatasetCohortCounts::try_new(
            self.candidate_count,
            self.eligible_count,
            included,
            exclusions,
            censors,
        )
        .map_err(|error| {
            ResearchError::DatasetBuild {
                detail: format!("feedback cohort counts do not reconcile: {error}"),
            }
            .into()
        })
    }

    fn count_all(&self) -> QuantResult<DatasetCohortCounts> {
        let included =
            usize::try_from(self.eligible_count).map_err(|error| ResearchError::DatasetBuild {
                detail: format!("feedback eligible count exceeds usize: {error}"),
            })?;
        self.counts(included)
    }
}

impl FeedbackCohortMaterializer {
    async fn scan_cohort(
        &self,
        cohort: FeedbackCohort,
        window: &FeedbackCohortWindow,
        truth_cutoff: DateTime<Utc>,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<CohortScan> {
        let snapshot =
            FeedbackCohortSnapshot::try_new(window.clone(), truth_cutoff).map_err(|error| {
                ResearchError::DatasetBuild {
                    detail: format!("construct feedback cohort snapshot: {error}"),
                }
            })?;
        let mut scan = CohortScan::default();
        let mut after = None;
        loop {
            if cancel.is_cancelled() {
                return Err(ResearchError::Cancelled {
                    detail: "feedback cohort scan cancelled between keyset pages".to_owned(),
                }
                .into());
            }
            let query = FeedbackCohortPageQuery::try_new(
                cohort,
                snapshot.clone(),
                after,
                FEEDBACK_COHORT_PAGE_LIMIT,
            )
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("construct feedback keyset page: {error}"),
            })?;
            let page = self.cohorts.list_page(query).await?;
            for candidate in page.candidates() {
                let decision = evaluate_feedback_cohort(
                    cohort,
                    &snapshot,
                    candidate.context(),
                    candidate.resolution_outcome(),
                    candidate.execution_rollup(),
                    candidate.economic_outcome(),
                )
                .map_err(|error| ResearchError::DatasetBuild {
                    detail: format!("classify {cohort} candidate: {error}"),
                })?;
                scan.record(cohort, candidate.context().clone(), decision)?;
            }
            progress.report(ResearchJobProgress::indeterminate(
                format!("feedback-{cohort}-scan"),
                scan.candidate_count,
            ));
            let Some(cursor) = page.next_cursor() else {
                break;
            };
            after = Some(cursor);
        }
        Ok(scan)
    }

    /// Freeze all three orthogonal cohorts and reconstruct only rows bound to
    /// the exact champion serving version.
    pub(crate) async fn freeze_coverage(
        &self,
        window: &FeedbackCohortWindow,
        champion_model_version_id: ModelVersionId,
        truth_cutoff: DateTime<Utc>,
        champion_pit_cutoff: DateTime<Utc>,
        progress: &Arc<dyn JobProgressSink>,
        cancel: &CancellationToken,
    ) -> QuantResult<FeedbackCoverageMaterialization> {
        let mut model = self
            .scan_cohort(
                FeedbackCohort::ModelLearning,
                window,
                truth_cutoff,
                progress,
                cancel,
            )
            .await?;
        let execution = self
            .scan_cohort(
                FeedbackCohort::ExecutionLearning,
                window,
                truth_cutoff,
                progress,
                cancel,
            )
            .await?;
        let policy = self
            .scan_cohort(
                FeedbackCohort::PolicyEvaluation,
                window,
                truth_cutoff,
                progress,
                cancel,
            )
            .await?;

        let mut mature_labels = model
            .eligible
            .iter()
            .map(|eligible| FeedbackMatureLabel {
                recommendation_id: eligible.context.recommendation_id(),
                model_version_id: eligible.context.model_version_id(),
                decision_at: eligible.context.decision_at(),
                candidate_available_at: eligible.context.available_at(),
                label_available_at: eligible.resolution.available_at,
                outcome_hash: eligible.resolution.outcome_hash,
            })
            .collect::<Vec<_>>();
        mature_labels.sort_by_key(|label| {
            (
                label.recommendation_id.as_uuid(),
                label.candidate_available_at,
                label.label_available_at,
            )
        });
        let new_mature_label_count = u64::try_from(
            mature_labels
                .iter()
                .filter(|label| label.label_available_at > champion_pit_cutoff)
                .count(),
        )
        .map_err(|error| ResearchError::DatasetBuild {
            detail: format!("new mature-label count conversion failed: {error}"),
        })?;
        let champion = mem::take(&mut model.eligible)
            .into_iter()
            .filter(|eligible| eligible.context.model_version_id() == champion_model_version_id)
            .collect::<Vec<_>>();
        let (mut champion_rows, mut champion_examples) = if champion.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            let materialized = self.materialize(champion).await?;
            (materialized.rows, materialized.examples)
        };
        champion_rows.sort_by_key(|row| row.example_id.as_uuid());
        champion_examples.sort_by_key(|example| example.example_id.as_uuid());
        let cohorts = FeedbackCoverageCohorts {
            model_learning: model.counts(champion_rows.len())?,
            execution_learning: execution.count_all()?,
            policy_evaluation: policy.count_all()?,
        };
        Ok(FeedbackCoverageMaterialization {
            cohorts,
            mature_labels,
            new_mature_label_count,
            champion_rows,
            champion_examples,
        })
    }
}

struct MaterializedFeedback {
    rows: Vec<ModelLearningCohortRow>,
    examples: Vec<TrainingExample>,
}

impl FeedbackCohortMaterializer {
    async fn materialize(
        &self,
        eligible: Vec<EligibleFeedback>,
    ) -> QuantResult<MaterializedFeedback> {
        let feature_ids = eligible
            .iter()
            .map(|seed| seed.context.feature_vector_id())
            .collect::<Vec<_>>();
        let unique_features = feature_ids.iter().copied().collect::<HashSet<_>>();
        if unique_features.len() != feature_ids.len() {
            return Err(ResearchError::DatasetBuild {
                detail: "ModelLearning cohort contains duplicate serving feature vectors"
                    .to_owned(),
            }
            .into());
        }
        let features = self.features.find_by_ids(&feature_ids).await?;
        let features = features
            .into_iter()
            .map(|feature| (feature.feature_vector_id, feature))
            .collect::<HashMap<_, _>>();
        if features.len() != feature_ids.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_feature_vector"),
                "ModelLearning cohort references a missing feature vector",
            )
            .into());
        }

        let expected_factor_ids = eligible
            .first()
            .map(|seed| seed.context.factor_definition_versions().to_vec())
            .unwrap_or_default();
        if eligible
            .iter()
            .any(|seed| seed.context.factor_definition_versions() != expected_factor_ids.as_slice())
        {
            return Err(ResearchError::DatasetBuild {
                detail: "one ModelLearning Dataset cannot mix serving factor contracts".to_owned(),
            }
            .into());
        }
        if expected_factor_ids.is_empty() {
            FactorServingPlane::try_empty().map_err(|error| ResearchError::DatasetBuild {
                detail: format!("seal feedback factor-free plane: {error}"),
            })?;
            return Self::materialize_rows(eligible, &features, HashMap::new(), &HashMap::new());
        }
        let definitions = self
            .factors
            .find_definitions_by_ids(&expected_factor_ids)
            .await?;
        let definitions = definitions
            .into_iter()
            .map(|definition| (definition.factor_definition_id, definition))
            .collect::<HashMap<_, _>>();
        if definitions.len() != expected_factor_ids.len() {
            return Err(StorageError::invariant_violation(
                Some("quant_factor_definition"),
                "ModelLearning cohort references a missing factor definition",
            )
            .into());
        }
        let factor_rows = self.factors.find_values_by_vectors(&feature_ids).await?;
        let mut factors_by_vector = HashMap::<FeatureVectorId, Vec<FactorValueInfo>>::new();
        for factor in factor_rows {
            factors_by_vector
                .entry(factor.feature_vector_id)
                .or_default()
                .push(factor);
        }

        let revisions = expected_factor_ids
            .iter()
            .map(|id| -> QuantResult<FactorDefinitionRef> {
                let definition = definitions.get(id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_definition"),
                        format!("factor definition {id} disappeared during assembly"),
                    )
                })?;
                FactorDefinitionRef::try_from(definition).map_err(|error| {
                    ResearchError::DatasetBuild {
                        detail: format!(
                            "reconstruct persisted factor revision {}: {error}",
                            definition.factor_definition_id
                        ),
                    }
                    .into()
                })
            })
            .collect::<QuantResult<Vec<_>>>()?;
        FactorServingPlane::try_seal(revisions).map_err(|error| ResearchError::DatasetBuild {
            detail: format!("seal feedback factor serving plane: {error}"),
        })?;
        Self::materialize_rows(eligible, &features, factors_by_vector, &definitions)
    }

    fn materialize_rows(
        eligible: Vec<EligibleFeedback>,
        features: &HashMap<FeatureVectorId, FeatureVectorInfo>,
        mut factors_by_vector: HashMap<FeatureVectorId, Vec<FactorValueInfo>>,
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    ) -> QuantResult<MaterializedFeedback> {
        let mut rows = Vec::with_capacity(eligible.len());
        let mut examples = Vec::with_capacity(eligible.len());
        let mut feature_schema_version = None;
        let mut knowledge_lag_secs = None;

        for seed in eligible {
            let feature_id = seed.context.feature_vector_id();
            let feature = features.get(&feature_id).ok_or_else(|| {
                StorageError::invariant_violation(
                    Some("quant_feature_vector"),
                    format!("feature vector {feature_id} disappeared during assembly"),
                )
            })?;
            Self::validate_feature(&seed.context, feature)?;
            let vector = FeatureVector::try_from(feature)?;
            let factors = Self::materialize_factors(
                &seed.context,
                factors_by_vector.remove(&feature_id).unwrap_or_default(),
                definitions,
            )?;
            Self::validate_breakdown(&seed.context, &factors)?;
            let model_token_id =
                vector
                    .token_id
                    .clone()
                    .ok_or_else(|| ResearchError::DatasetBuild {
                        detail: format!("serving feature vector {feature_id} has no primary token"),
                    })?;
            Self::validate_token_projection(&seed.context, feature, &model_token_id)?;
            let model_token_payout_ratio = match seed.context.outcome_side() {
                OutcomeSide::Yes => seed.resolution.token_payout_ratio,
                OutcomeSide::No => seed.resolution.token_payout_ratio.complement(),
            };
            let row = ModelLearningCohortRow::try_seal(NewModelLearningCohortRow {
                recommendation_id: seed.context.recommendation_id(),
                recommendation_report_id: seed.context.recommendation_report_id(),
                category: seed.context.category(),
                market_id: seed.context.market_id().clone(),
                event_id: seed.context.event_id().clone(),
                recommendation_token_id: seed.context.token_id().clone(),
                model_token_id: model_token_id.clone(),
                outcome_side: seed.context.outcome_side(),
                decision_at: seed.context.decision_at(),
                candidate_available_at: seed.context.available_at(),
                decision_policy_snapshot_id: seed.context.decision_policy_snapshot_id(),
                market_selection_id: seed.context.market_selection_id(),
                feature_vector_id: feature_id,
                model_run_id: seed.context.model_run_id(),
                model_version_id: seed.context.model_version_id(),
                factor_definition_versions: seed.context.factor_definition_versions().to_vec(),
                book_snapshot_ref: seed.context.book_snapshot_ref().clone(),
                data_quality_snapshot_id: seed.context.data_quality_snapshot_id(),
                resolution: seed.resolution,
                model_token_payout_ratio,
            })
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("seal ModelLearning cohort row: {error}"),
            })?;
            let example = TrainingExample {
                example_id: row.example_id,
                market_id: row.market_id.clone(),
                token_id: row.model_token_id.clone(),
                selected_market: SelectedMarket::from(&feature.decision_capture.snapshot.selection),
                decision_boundary: feature.decision_boundary.clone(),
                sample_source: TrainingSampleSource::PublishedDecisionDiagnostic,
                feature_vector: vector,
                factor_values: factors,
                labels: vec![TrainingLabel {
                    label_name: TOKEN_PAYOUT_RATIO,
                    horizon_secs: 0,
                    value: row.model_token_payout_ratio.inner(),
                    is_resolved: true,
                    matured_at: row.resolution.resolved_at,
                }],
                source_refs: feature.source_refs.0.clone(),
                decision_capture: Some(feature.decision_capture.clone()),
                lot_context: None,
                position_state: None,
                book_fidelity: None,
            };
            feature_schema_version =
                Self::one_schema(feature_schema_version, feature.feature_schema_version)?;
            knowledge_lag_secs = Self::one_lag(
                knowledge_lag_secs,
                feature.decision_boundary.knowledge_lag_secs(),
            )?;
            rows.push(row);
            examples.push(example);
        }
        examples.sort_by(|left, right| {
            (
                left.market_id.as_str(),
                left.token_id.as_str(),
                left.decision_at(),
                left.example_id.as_uuid(),
            )
                .cmp(&(
                    right.market_id.as_str(),
                    right.token_id.as_str(),
                    right.decision_at(),
                    right.example_id.as_uuid(),
                ))
        });
        feature_schema_version.ok_or_else(|| ResearchError::DatasetBuild {
            detail: "feedback materialization produced no feature schema".to_owned(),
        })?;
        knowledge_lag_secs.ok_or_else(|| ResearchError::DatasetBuild {
            detail: "feedback materialization produced no knowledge-lag contract".to_owned(),
        })?;
        Ok(MaterializedFeedback { rows, examples })
    }
}

impl FeedbackCohortMaterializer {
    fn validate_feature(
        context: &FeedbackRecommendationContext,
        feature: &FeatureVectorInfo,
    ) -> QuantResult<()> {
        let capture_hash = CanonicalDigest::content_hash_json(&feature.decision_capture)?;
        let snapshot = &feature.decision_capture.snapshot;
        let selection = &snapshot.selection;
        let valid = feature.feature_vector_id == context.feature_vector_id()
            && &feature.market_id == context.market_id()
            && feature.decision_at == context.decision_at()
            && feature.decision_boundary == snapshot.boundary
            && feature.created_at <= context.available_at()
            && feature.decision_capture_hash == capture_hash
            && &snapshot.market_id == context.market_id()
            && snapshot.event_id == *context.event_id()
            && snapshot.book_snapshot_ref == *context.book_snapshot_ref()
            && snapshot.token_id == selection.primary_token_id
            && selection.market_id == *context.market_id()
            && selection.event_id == *context.event_id()
            && selection.category == context.category()
            && feature.decision_capture.identity == *context.identity()
            && feature.decision_capture.market_context == *context.market_context()
            && feature.decision_capture.data_quality == feature.data_quality;
        if !valid {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature vector {} does not match recommendation {} serving evidence",
                    feature.feature_vector_id,
                    context.recommendation_id()
                ),
            }
            .into());
        }
        let vector = FeatureVector::try_from(feature)?;
        if FeatureSourceRefs(vector.evidence_refs()) != feature.source_refs {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "feature vector {} source references do not reproduce",
                    feature.feature_vector_id
                ),
            }
            .into());
        }
        Ok(())
    }

    fn validate_token_projection(
        context: &FeedbackRecommendationContext,
        feature: &FeatureVectorInfo,
        model_token_id: &TokenId,
    ) -> QuantResult<()> {
        let selection = &feature.decision_capture.snapshot.selection;
        let valid = selection.primary_token_id == *model_token_id
            && match context.outcome_side() {
                OutcomeSide::Yes => context.token_id() == model_token_id,
                OutcomeSide::No => {
                    selection.secondary_token_id.as_ref() == Some(context.token_id())
                        && context.token_id() != model_token_id
                }
            };
        if !valid {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "recommendation {} token side does not match its frozen selection member",
                    context.recommendation_id()
                ),
            }
            .into());
        }
        Ok(())
    }

    fn materialize_factors(
        context: &FeedbackRecommendationContext,
        rows: Vec<FactorValueInfo>,
        definitions: &HashMap<FactorDefinitionId, FactorDefinitionInfo>,
    ) -> QuantResult<Vec<FactorValue>> {
        let mut by_definition = HashMap::new();
        for row in rows
            .into_iter()
            .filter(|row| row.model_run_id == context.model_run_id())
        {
            if row.market_id != *context.market_id()
                || row.decision_at != context.decision_at()
                || by_definition
                    .insert(row.factor_definition_id, row)
                    .is_some()
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "recommendation {} has contradictory persisted factor rows",
                        context.recommendation_id()
                    ),
                }
                .into());
            }
        }
        if by_definition.len() != context.factor_definition_versions().len()
            || !context
                .factor_definition_versions()
                .iter()
                .all(|id| by_definition.contains_key(id))
        {
            return Err(StorageError::invariant_violation(
                Some("quant_factor_value"),
                format!(
                    "recommendation {} does not have one exact factor row per governed definition",
                    context.recommendation_id()
                ),
            )
            .into());
        }
        context
            .factor_definition_versions()
            .iter()
            .map(|id| {
                let value = by_definition.get(id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_value"),
                        format!("factor value {id} disappeared during assembly"),
                    )
                })?;
                let definition = definitions.get(id).ok_or_else(|| {
                    StorageError::invariant_violation(
                        Some("quant_factor_definition"),
                        format!("factor definition {id} disappeared during assembly"),
                    )
                })?;
                FactorValue::try_from_persistence(value, definition)
            })
            .collect()
    }

    fn validate_breakdown(
        context: &FeedbackRecommendationContext,
        factors: &[FactorValue],
    ) -> QuantResult<()> {
        let mut breakdown = context
            .factor_breakdown()
            .0
            .iter()
            .map(|entry| (entry.factor_name.as_str(), entry))
            .collect::<HashMap<_, _>>();
        if breakdown.len() != factors.len() {
            return Err(ResearchError::DatasetBuild {
                detail: format!(
                    "recommendation {} factor breakdown cardinality does not match its ledger rows",
                    context.recommendation_id()
                ),
            }
            .into());
        }
        for factor in factors {
            let Some(entry) = breakdown.remove(factor.name.as_str()) else {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "recommendation {} factor breakdown omits {}",
                        context.recommendation_id(),
                        factor.name
                    ),
                }
                .into());
            };
            if entry.family != factor.family
                || entry.value_state != factor.value_state()
                || entry.raw_value != factor.raw_value
                || entry.normalized_score != factor.normalized_score()
                || entry.normalization_source != factor.normalization_source()
                || entry.indeterminate_reason != factor.indeterminate_reason()
                || entry.direction != factor.direction
                || entry.confidence != factor.confidence
                || entry.explanation != factor.explanation.headline
            {
                return Err(ResearchError::DatasetBuild {
                    detail: format!(
                        "recommendation {} factor breakdown disagrees with persisted {}",
                        context.recommendation_id(),
                        factor.name
                    ),
                }
                .into());
            }
        }
        Ok(())
    }

    fn one_schema(
        current: Option<SchemaVersion>,
        next: SchemaVersion,
    ) -> QuantResult<Option<SchemaVersion>> {
        if current.is_some_and(|version| version != next) {
            return Err(ResearchError::DatasetBuild {
                detail: "one feedback Dataset cannot mix feature schema versions".to_owned(),
            }
            .into());
        }
        Ok(Some(next))
    }

    fn one_lag(current: Option<u64>, next: u64) -> QuantResult<Option<u64>> {
        if current.is_some_and(|lag| lag != next) {
            return Err(ResearchError::DatasetBuild {
                detail: "one feedback Dataset cannot mix knowledge-lag contracts".to_owned(),
            }
            .into());
        }
        Ok(Some(next))
    }
}

impl FeedbackDatasetService {
    async fn persist_score_cohort(
        &self,
        window: FeedbackCohortWindow,
        counts: DatasetCohortCounts,
        capability_registry_hashes: CapabilityRegistryHashes,
        rows: Vec<ModelScoreCohortRow>,
        cancel: &CancellationToken,
    ) -> QuantResult<DatasetCohortManifest> {
        let artifact = ModelScoreCohortArtifact {
            format_version: MODEL_SCORE_COHORT_FORMAT_VERSION,
            window: window.clone(),
            counts: counts.clone(),
            rows,
        };
        Self::require_active(cancel, "before model-score cohort encoding")?;
        let encoded = self
            .compute
            .run_offline_cancellable(self.compute_memory, cancel, move || {
                let source_hash =
                    artifact
                        .source_hash()
                        .map_err(|error| ResearchError::DatasetBuild {
                            detail: format!("hash model-score cohort artifact: {error}"),
                        })?;
                let bytes = ModelScoreCohortCodec::encode(&artifact)?;
                let bytes_hash = ModelScoreCohortCodec::bytes_hash(&bytes);
                let schema_hash = ModelScoreCohortCodec::schema_hash()?;
                Ok(EncodedScoreCohort {
                    artifact,
                    source_hash,
                    bytes,
                    bytes_hash,
                    schema_hash,
                })
            })
            .await?;
        Self::require_active(cancel, "after model-score cohort encoding")?;
        let key = ArtifactKey::new(
            ArtifactNamespace::FeedbackCohort,
            encoded.source_hash.hex(),
            "json",
        )?;
        let uri = self.artifact_store.put(key, &encoded.bytes).await?;
        let persisted = self.artifact_store.get(&uri).await?;
        Self::require_active(cancel, "before model-score cohort verification")?;
        let EncodedScoreCohort {
            artifact,
            source_hash,
            bytes: _,
            bytes_hash,
            schema_hash,
        } = encoded;
        self.compute
            .run_offline_cancellable(self.compute_memory, cancel, move || {
                let actual_hash = ModelScoreCohortCodec::bytes_hash(&persisted);
                if actual_hash != bytes_hash
                    || ModelScoreCohortCodec::decode(&persisted)? != artifact
                {
                    return Err(ResearchError::ArtifactHashMismatch {
                        expected: bytes_hash.to_string(),
                        actual: actual_hash.to_string(),
                    }
                    .into());
                }
                Ok(())
            })
            .await?;
        Self::require_active(cancel, "after model-score cohort verification")?;
        let manifest = DatasetCohortManifest {
            format_version: DATASET_COHORT_MANIFEST_FORMAT_VERSION,
            cohort: FeedbackCohort::ModelScoreLearning,
            window,
            artifact: DatasetCohortArtifactRef {
                uri,
                bytes_hash,
                schema_hash,
                source_hash,
                row_count: counts.included_count(),
            },
            counts,
            capability_registry_hashes,
        };
        manifest
            .validate()
            .map_err(|error| ResearchError::DatasetBuild {
                detail: format!("validate model-score cohort manifest: {error}"),
            })?;
        Ok(manifest)
    }

    fn require_active(cancel: &CancellationToken, boundary: &'static str) -> QuantResult<()> {
        if cancel.is_cancelled() {
            return Err(ResearchError::Cancelled {
                detail: format!("feedback Dataset cancelled {boundary}"),
            }
            .into());
        }
        Ok(())
    }
}
