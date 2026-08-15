//! Durable fresh-boot orchestration from accepted exchange history to the first report.

use std::{
    collections::BTreeSet,
    fmt::{Display, Formatter, Result as FmtResult},
    sync::Arc,
    time::Duration,
};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use futures_util::future::join_all;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{
            BuildTrainingDatasetRequest, CpcvBacktestJobParams, FitModelCalibratorRequest,
            ModelTrainJobParams, RunCpcvBacktestRequest, TrainModelRequest,
        },
        data_plane::{ExchangeHistoryChunkInfo, ExchangeHistoryFrontier},
        ports::{GovernanceActor, ModelCalibrationFitJobParams},
        quant::{
            AdvanceFreshBootRun, BlockFreshBootRun, BootstrapModelRoute, DelayFreshBootRun,
            FRESH_BOOT_MAX_RETRY_COUNT, FRESH_BOOT_REASON_CODE, FreshBootAdvancePatch,
            FreshBootRunContract, FreshBootRunInfo, FreshBootSourceCoverage,
            FreshBootSourceCoverageManifest, ModelRouteBootstrapActor, ResearchJobInfo,
        },
    },
    enums::quant::{
        CalibrationMethod, DatasetPurpose, DownsideSource, FreshBootBlockedReason,
        FreshBootEventKind, FreshBootRetryReason, FreshBootStage, FreshBootStatus,
        RecommendationReportStatus, ReportRunStatus, ResearchJobResultKind, ResearchJobStatus,
        TrainingDatasetStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        BacktestPathSetId, CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID, EvmBlockHash, FreshBootRunId,
        ModelRunId, ModelSpecId, ModelVersionId, POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID,
        ResearchJobParams, ResearchProfileArtifact, ResearchProfileArtifactId,
        ResearchProfileDataSource, ResearchReadinessEvidencePayload, ResearchReadinessSource,
        RetentionRunwayEvidenceV1, SchemaVersion, ServingAuthority, TrainingDatasetId,
        TrainingSampleSources, WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID, WorkerId,
        builtin_research_profiles, model_lineage::ModelVersionDerivation,
    },
};
use quant_pivot_repository::{
    postgres::PgModelRegistryRepository,
    traits::{
        BacktestPathSetRepository, ExchangeHistoryRepository, FreshBootRepository,
        ModelRegistryRepository, PolicyRepository, RecommendationReportRepository,
        ReportRunRepository, ResearchJobRepository, TrainingDatasetRepository,
    },
};
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use super::{
    AppContext, ports::research_job::CoreResearchJobPort, task_id::TaskId, task_registry::AppRunner,
};
use crate::{
    report::{AdHocReportRequest, ReportLifecycleService},
    service::{
        frozen_model_parity::FrozenModelParityService,
        model_route_bootstrap::ModelRouteBootstrapService,
        model_route_governance::ModelRouteGovernanceService,
        research_readiness::ResearchReadinessEvidenceService,
    },
};

const SOURCE_COVERAGE_VERSION: u32 = 1;
const SOURCE_COVERAGE_DOMAIN: &str = "quant-pivot/fresh-boot-source-coverage";
const FRESH_BOOT_INTERVAL: Duration = Duration::from_secs(1);
const CLAIM_LEASE: ChronoDuration = ChronoDuration::seconds(10);
const SOURCE_RETRY: ChronoDuration = ChronoDuration::seconds(30);
const CALIBRATION_WINDOW_HOURS: i64 = 23;
const JOB_ACTOR: &str = "system:fresh_boot_orchestrator";

struct HistoryWindow {
    from_block: i64,
    through_block: i64,
    effective_through: DateTime<Utc>,
}

enum HistoryCoverageError {
    Incomplete(&'static str),
    Storage(StorageError),
}

impl Display for HistoryCoverageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        match self {
            Self::Incomplete(detail) => formatter.write_str(detail),
            Self::Storage(error) => Display::fmt(error, formatter),
        }
    }
}

impl From<StorageError> for HistoryCoverageError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

struct DatasetWindows {
    training_start: DateTime<Utc>,
    training_end: DateTime<Utc>,
    calibration_start: DateTime<Utc>,
    calibration_end: DateTime<Utc>,
}

#[derive(Clone)]
struct FreshBootDeps {
    runs: Arc<dyn FreshBootRepository>,
    history: Arc<dyn ExchangeHistoryRepository>,
    readiness: Arc<ResearchReadinessEvidenceService>,
    jobs: Arc<dyn ResearchJobRepository>,
    job_port: Arc<CoreResearchJobPort>,
    model_registry: Arc<PgModelRegistryRepository>,
    datasets: Arc<dyn TrainingDatasetRepository>,
    path_sets: Arc<dyn BacktestPathSetRepository>,
    policies: Arc<dyn PolicyRepository>,
    parity: Arc<FrozenModelParityService>,
    bootstrap: Arc<ModelRouteBootstrapService>,
    route_governance: Arc<ModelRouteGovernanceService>,
    report_lifecycle: Arc<ReportLifecycleService>,
    report_runs: Arc<dyn ReportRunRepository>,
    reports: Arc<dyn RecommendationReportRepository>,
}

/// Crash-safe coordinator. Every call to `tick` writes at most one FSM edge per
/// independently governed bootstrap profile.
#[derive(Clone)]
pub struct FreshBootOrchestrator {
    deps: FreshBootDeps,
    worker_id: WorkerId,
}

impl FreshBootOrchestrator {
    #[must_use]
    fn new(deps: FreshBootDeps) -> Self {
        Self {
            deps,
            worker_id: WorkerId::new(Uuid::new_v4()),
        }
    }

    pub async fn run(&self, token: CancellationToken) {
        let mut ticker = interval(FRESH_BOOT_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut seeded = false;
        let mut seed_failures = 0_i32;
        let mut seed_next_attempt = Utc::now();
        loop {
            tokio::select! {
                () = token.cancelled() => return,
                _ = ticker.tick() => {
                    let now = Utc::now();
                    if !seeded && now >= seed_next_attempt {
                        match self.seed_contracts().await {
                            Ok(()) => {
                                seeded = true;
                                seed_failures = 0;
                                info!("fresh-boot immutable contracts are ready");
                            }
                            Err(error) => {
                                seed_failures = seed_failures.saturating_add(1);
                                seed_next_attempt = now + Self::retry_delay(seed_failures);
                                warn!(
                                    %error,
                                    seed_failures,
                                    next_attempt_at = %seed_next_attempt,
                                    capability = "fresh_boot_orchestration",
                                    "fresh-boot contract seeding is degraded and will retry"
                                );
                                continue;
                            }
                        }
                    }
                    if !seeded {
                        continue;
                    }
                    if let Err(error) = Box::pin(self.tick()).await {
                        warn!(%error, "fresh-boot coordinator tick did not advance");
                    }
                }
            }
        }
    }

    async fn seed_contracts(&self) -> QuantResult<()> {
        self.deps
            .model_registry
            .ensure_builtin_research_profiles()
            .await?;
        for profile in builtin_research_profiles()
            .map_err(Self::invalid)?
            .into_iter()
            .filter(|profile| {
                matches!(
                    profile.spec.serving_authority,
                    ServingAuthority::ReportOnlyWithLiveL2
                )
            })
        {
            self.deps
                .model_registry
                .ensure_bootstrap_spec(&profile)
                .await?;
        }
        Ok(())
    }

    async fn tick(&self) -> QuantResult<()> {
        let existing = self
            .deps
            .runs
            .list_latest()
            .await?
            .into_iter()
            .map(|run| run.research_profile_artifact_id)
            .collect::<Vec<_>>();
        for (profile, route) in Self::bootstrap_profiles()? {
            let artifact_id = ResearchProfileArtifactId::from_profile_ref(&profile.profile_ref);
            if existing.contains(&artifact_id) {
                continue;
            }
            if let Err(error) = self.create_run(profile, route, None).await {
                warn!(
                    %error,
                    route = route.as_str(),
                    "fresh-boot run seeding failed without blocking other routes"
                );
            }
        }
        let now = Utc::now();
        let claimed = self
            .deps
            .runs
            .claim_due(self.worker_id, now, now + CLAIM_LEASE, 3)
            .await?;
        let mut bootstrap_barrier_taken = false;
        let mut parallel = Vec::with_capacity(claimed.len());
        for run in claimed {
            if run.stage == FreshBootStage::BootstrapPreflight {
                if bootstrap_barrier_taken {
                    self.schedule_retry(
                        run,
                        FreshBootRetryReason::PreflightStale,
                        "another fresh-boot route owns the bounded bootstrap commit barrier",
                        false,
                    )
                    .await?;
                    continue;
                }
                bootstrap_barrier_taken = true;
            }
            parallel.push(run);
        }
        let outcomes = join_all(parallel.into_iter().map(|run| async move {
            let snapshot = run.clone();
            (snapshot, Box::pin(self.advance_run(run)).await)
        }))
        .await;
        for (run, outcome) in outcomes {
            if let Err(error) = outcome
                && let Err(recovery_error) = self.recover_error(run.clone(), &error).await
            {
                warn!(
                    run_id = %run.run_id,
                    error = %error,
                    recovery_error = %recovery_error,
                    "fresh-boot run failed and its retry transition also failed"
                );
            }
        }
        Ok(())
    }

    async fn create_run(
        &self,
        profile: ResearchProfileArtifact,
        route: BuyModelRoute,
        supersedes_run_id: Option<FreshBootRunId>,
    ) -> QuantResult<Option<FreshBootRunInfo>> {
        let Some(plan) = self.deps.history.load_plan(137).await? else {
            return Ok(None);
        };
        let from_block = plan.required_from_block(route);
        let through_block = plan.activation_through_block;
        let bundle = self
            .deps
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::invalid("fresh boot requires an active policy bundle"))?;
        let contract = FreshBootRunContract {
            profile_ref: profile.profile_ref.clone(),
            route,
            history_plan_id: plan.plan_id,
            history_policy_hash: plan.policy_hash,
            history_from_block: from_block,
            history_through_block: through_block,
            decision_policy_snapshot_id: bundle.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: bundle.snapshot_hash,
            supersedes_run_id,
        };
        let now = Utc::now();
        let new_run = contract.seal(plan.created_at, now)?;
        let created = self.deps.runs.create_or_load(new_run).await?;
        info!(
            run_id = %created.run_id,
            route = created.route.as_str(),
            "durable fresh-boot run loaded"
        );
        Ok(Some(created))
    }

    async fn advance_run(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        match run.stage {
            FreshBootStage::AwaitingSourceCoverage => Box::pin(self.accept_coverage(run)).await,
            FreshBootStage::DatasetQueued => {
                Box::pin(self.observe_start(run, FreshBootEventKind::DatasetStarted)).await
            }
            FreshBootStage::DatasetRunning => Box::pin(self.finish_dataset(run, false)).await,
            FreshBootStage::DatasetReady => Box::pin(self.enqueue_training(run)).await,
            FreshBootStage::TrainingQueued => {
                Box::pin(self.observe_start(run, FreshBootEventKind::TrainingStarted)).await
            }
            FreshBootStage::TrainingRunning => Box::pin(self.finish_training(run)).await,
            FreshBootStage::TrainingReady => Box::pin(self.enqueue_calibration_dataset(run)).await,
            FreshBootStage::CalibrationDatasetQueued => {
                Box::pin(self.observe_start(run, FreshBootEventKind::CalibrationDatasetStarted))
                    .await
            }
            FreshBootStage::CalibrationDatasetRunning => {
                Box::pin(self.finish_dataset(run, true)).await
            }
            FreshBootStage::CalibrationDatasetReady => {
                Box::pin(self.enqueue_calibration(run)).await
            }
            FreshBootStage::CalibrationQueued => {
                Box::pin(self.observe_start(run, FreshBootEventKind::CalibrationStarted)).await
            }
            FreshBootStage::CalibrationRunning => Box::pin(self.finish_calibration(run)).await,
            FreshBootStage::CalibrationReady => Box::pin(self.enqueue_cpcv(run)).await,
            FreshBootStage::CpcvQueued => {
                Box::pin(self.observe_start(run, FreshBootEventKind::CpcvStarted)).await
            }
            FreshBootStage::CpcvRunning => Box::pin(self.finish_cpcv(run)).await,
            FreshBootStage::CpcvReady => Box::pin(self.verify_parity(run)).await,
            FreshBootStage::ParityReady => Box::pin(self.bind_scenario(run)).await,
            FreshBootStage::ScenarioReady => Box::pin(self.persist_preflight(run)).await,
            FreshBootStage::BootstrapPreflight => Box::pin(self.commit_bootstrap(run)).await,
            FreshBootStage::BootstrapCommitted => Box::pin(self.enable_report(run)).await,
            FreshBootStage::ReportEligible => Box::pin(self.observe_report(run)).await,
            FreshBootStage::FirstReportPublished => Ok(()),
        }
    }

    async fn accept_coverage(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let plan = self
            .deps
            .history
            .load_plan(137)
            .await?
            .ok_or_else(|| Self::invalid("fresh-boot history plan does not exist"))?;
        let from = plan.required_from_block(run.route);
        let history = match self
            .verify_history(from, plan.activation_through_block)
            .await
        {
            Ok(history) => history,
            Err(HistoryCoverageError::Incomplete(detail)) => {
                return self
                    .schedule_wait(
                        run,
                        FreshBootRetryReason::SourceCoverageIncomplete,
                        &format!("exchange history is not ready: {detail}"),
                    )
                    .await;
            }
            Err(HistoryCoverageError::Storage(error)) => return Err(error.into()),
        };
        let profile = Self::profile_for_run(&run)?;
        let windows = Self::dataset_windows(&profile, history.effective_through);
        let verified = self.deps.readiness.latest_verified(Utc::now()).await?;
        let Some(evidence) = verified.retention else {
            return self
                .schedule_wait(
                    run,
                    FreshBootRetryReason::SourceCoverageIncomplete,
                    &verified.diagnostics.join("; "),
                )
                .await;
        };
        let ResearchReadinessEvidencePayload::RetentionRunway(retention) = evidence.payload_json
        else {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::SourceCoverageInvalid,
                    "verified retention evidence has the wrong payload kind",
                )
                .await;
        };
        let requirements = match Self::source_requirements(&profile, &windows, &retention) {
            Ok(requirements) => requirements,
            Err(detail) => {
                return self
                    .schedule_wait(run, FreshBootRetryReason::SourceCoverageIncomplete, &detail)
                    .await;
            }
        };
        let manifest = FreshBootSourceCoverageManifest {
            history_plan_id: plan.plan_id,
            history_policy_hash: plan.policy_hash,
            availability_policy_hash: profile.spec.availability_policy.content_hash()?,
            readiness_evidence_id: evidence.evidence_id,
            source_registry_hash: retention.registry_hash,
            window_start: windows.training_start,
            window_end: windows.calibration_end,
            pit_cutoff: history.effective_through,
            history_from_block: history.from_block,
            history_through_block: history.through_block,
            requirements,
            sealed_at: Utc::now(),
        };
        if !manifest.is_complete() {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::SourceCoverageInvalid,
                    "source coverage manifest failed its complete-window invariant",
                )
                .await;
        }
        let manifest_hash = CanonicalDigest::content_hash_typed(
            SOURCE_COVERAGE_DOMAIN,
            SOURCE_COVERAGE_VERSION,
            &manifest,
        )?;
        let spec = self
            .deps
            .model_registry
            .ensure_bootstrap_spec(&profile)
            .await?;
        let request = Self::dataset_request(&run, &profile, &manifest, spec.model_spec_id, false)?;
        let dataset_id = request
            .training_dataset_id
            .ok_or_else(|| Self::invalid("fresh-boot dataset request lost its deterministic id"))?;
        let job = self
            .deps
            .job_port
            .enqueue_fresh_boot(
                run.run_id,
                "training_dataset",
                ResearchJobParams::DatasetBuild(request),
                Some(spec.model_spec_id),
                run.decision_policy_snapshot_id,
                run.last_job_id,
            )
            .await?;
        self.advance(
            run,
            FreshBootEventKind::SourceCoverageSatisfied,
            FreshBootAdvancePatch {
                source_coverage_manifest: Some(manifest),
                source_coverage_hash: Some(manifest_hash),
                model_spec_id: Some(spec.model_spec_id),
                training_dataset_id: Some(dataset_id),
                active_job_id: Some(Some(job.job_id)),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn verify_history(
        &self,
        from: i64,
        through: i64,
    ) -> Result<HistoryWindow, HistoryCoverageError> {
        let mut chunks = self
            .deps
            .history
            .accepted_from(ExchangeHistoryFrontier::Retention, from)
            .await?;
        chunks.extend(
            self.deps
                .history
                .accepted_from(ExchangeHistoryFrontier::Activation, from)
                .await?,
        );
        chunks.sort_unstable_by_key(|chunk| chunk.from_block);
        if chunks.is_empty() {
            return Err(HistoryCoverageError::Incomplete(
                "fresh-boot history window has no accepted chunks",
            ));
        }
        let (activation_quarantine, retention_quarantine) = tokio::try_join!(
            self.deps.history.active_quarantine(
                ExchangeHistoryFrontier::Activation,
                from,
                through,
                1
            ),
            self.deps.history.active_quarantine(
                ExchangeHistoryFrontier::Retention,
                from,
                through,
                1
            ),
        )?;
        if !activation_quarantine.is_empty() || !retention_quarantine.is_empty() {
            return Err(HistoryCoverageError::Incomplete(
                "fresh-boot history contains quarantined evidence",
            ));
        }
        let mut cursor = from;
        let mut effective_through = None;
        let mut previous_hash = None;
        for chunk in &chunks {
            Self::verify_chunk(chunk, cursor, through, previous_hash.as_ref())?;
            cursor = chunk
                .to_block
                .checked_add(1)
                .ok_or(HistoryCoverageError::Incomplete(
                    "history chunk cursor overflowed",
                ))?;
            effective_through = chunk.effective_through_at;
            previous_hash.clone_from(&chunk.last_block_hash);
            if chunk.to_block == through {
                break;
            }
        }
        if cursor != through.saturating_add(1) {
            return Err(HistoryCoverageError::Incomplete(
                "accepted chunks do not cover the fresh-boot target contiguously",
            ));
        }
        Ok(HistoryWindow {
            from_block: from,
            through_block: through,
            effective_through: effective_through.ok_or(HistoryCoverageError::Incomplete(
                "accepted history window has no PIT availability timestamp",
            ))?,
        })
    }

    fn verify_chunk(
        chunk: &ExchangeHistoryChunkInfo,
        expected_from: i64,
        target: i64,
        previous_hash: Option<&EvmBlockHash>,
    ) -> Result<(), HistoryCoverageError> {
        let expected_parent = chunk.from_block.checked_sub(1);
        let complete = chunk.from_block == expected_from
            && chunk.to_block >= chunk.from_block
            && chunk.to_block <= target
            && chunk.hypersync_count == chunk.attestor_count
            && chunk.hypersync_digest.is_some()
            && chunk.hypersync_digest == chunk.attestor_digest
            && chunk.first_block_hash.is_some()
            && chunk.last_block_hash.is_some()
            && chunk.archive_height.is_some()
            && chunk.continuity_basis.is_some()
            && chunk.continuity_block == expected_parent
            && chunk.continuity_hash.is_some()
            && chunk.effective_through_at.is_some()
            && chunk.accepted_at.is_some()
            && previous_hash.is_none_or(|hash| chunk.continuity_hash.as_ref() == Some(hash));
        if !complete {
            return Err(HistoryCoverageError::Incomplete(
                "history chunk is not a complete, contiguous dual-provider acceptance proof",
            ));
        }
        Ok(())
    }

    async fn enqueue_calibration_dataset(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let profile = Self::profile_for_run(&run)?;
        let spec = self
            .deps
            .model_registry
            .ensure_bootstrap_spec(&profile)
            .await?;
        let manifest = run
            .source_coverage_manifest
            .as_ref()
            .ok_or_else(|| Self::invalid("fresh boot has no source coverage manifest"))?;
        let request = Self::dataset_request(&run, &profile, manifest, spec.model_spec_id, true)?;
        let stage = "calibration_dataset";
        let dataset_id = request
            .training_dataset_id
            .ok_or_else(|| Self::invalid("fresh-boot dataset request lost its deterministic id"))?;
        let job = self
            .deps
            .job_port
            .enqueue_fresh_boot(
                run.run_id,
                stage,
                ResearchJobParams::DatasetBuild(request),
                Some(spec.model_spec_id),
                run.decision_policy_snapshot_id,
                run.last_job_id,
            )
            .await?;
        self.advance(
            run,
            FreshBootEventKind::CalibrationDatasetEnqueued,
            FreshBootAdvancePatch {
                model_spec_id: Some(spec.model_spec_id),
                calibration_dataset_id: Some(dataset_id),
                active_job_id: Some(Some(job.job_id)),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    fn dataset_request(
        run: &FreshBootRunInfo,
        profile: &ResearchProfileArtifact,
        manifest: &FreshBootSourceCoverageManifest,
        model_spec_id: ModelSpecId,
        calibration: bool,
    ) -> QuantResult<BuildTrainingDatasetRequest> {
        let profile_ref = run.research_profile_artifact_id.profile_ref();
        let windows = Self::dataset_windows(profile, manifest.pit_cutoff);
        if manifest.window_start != windows.training_start
            || manifest.window_end != windows.calibration_end
        {
            return Err(Self::invalid(
                "fresh-boot source coverage window differs from the profile dataset window",
            ));
        }
        let (purpose, window_start, window_end, stage) = if calibration {
            (
                DatasetPurpose::Calibration,
                windows.calibration_start,
                windows.calibration_end,
                "calibration_dataset",
            )
        } else {
            (
                DatasetPurpose::Training,
                windows.training_start,
                windows.training_end,
                "training_dataset",
            )
        };
        Ok(BuildTrainingDatasetRequest {
            model_spec_id,
            profile_ref,
            purpose,
            decision_policy_snapshot_id: run.decision_policy_snapshot_id,
            window_start,
            window_end,
            pit_cutoff: manifest.pit_cutoff,
            sample_interval_secs: profile.spec.decision_cadence_secs,
            horizons_secs: vec![profile.spec.target_horizon_secs],
            knowledge_lag_secs: 1,
            feature_schema_version: SchemaVersion::FIRST,
            sample_sources: TrainingSampleSources::default(),
            reason: format!("fresh-boot {stage} for run {}", run.run_id),
            training_dataset_id: Some(TrainingDatasetId::from_fresh_boot_stage(run.run_id, stage)),
        })
    }

    async fn observe_start(
        &self,
        run: FreshBootRunInfo,
        event: FreshBootEventKind,
    ) -> QuantResult<()> {
        let job = self.load_active_job(&run).await?;
        match job.status {
            ResearchJobStatus::Queued => Ok(()),
            ResearchJobStatus::AwaitingEvidence => {
                self.schedule_wait(
                    run,
                    FreshBootRetryReason::JobRetryScheduled,
                    "research job is waiting for external evidence",
                )
                .await
            }
            ResearchJobStatus::RetryScheduled => {
                self.schedule_retry(
                    run,
                    FreshBootRetryReason::JobRetryScheduled,
                    "research job has a durable retry scheduled",
                    false,
                )
                .await
            }
            ResearchJobStatus::Running
            | ResearchJobStatus::Succeeded
            | ResearchJobStatus::Failed
            | ResearchJobStatus::Cancelled => {
                self.advance(run, event, FreshBootAdvancePatch::default())
                    .await
            }
        }
    }

    async fn finish_dataset(&self, run: FreshBootRunInfo, calibration: bool) -> QuantResult<()> {
        let job = self.load_active_job(&run).await?;
        if job.status.is_active() {
            return self.wait_for_job(run, &job).await;
        }
        if job.status != ResearchJobStatus::Succeeded {
            return self
                .block_job(run, &job, FreshBootBlockedReason::DatasetBuildFailed)
                .await;
        }
        let expected = if calibration {
            run.calibration_dataset_id
        } else {
            run.training_dataset_id
        }
        .ok_or_else(|| Self::invalid("fresh-boot dataset stage has no expected dataset id"))?;
        Self::require_result(
            &job,
            ResearchJobResultKind::TrainingDataset,
            expected.as_uuid_ref(),
        )?;
        let dataset = self
            .deps
            .datasets
            .find_by_id(&expected)
            .await?
            .ok_or_else(|| Self::invalid("successful dataset job has no dataset row"))?;
        if dataset.status == TrainingDatasetStatus::InsufficientLabels {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::InsufficientMatureLabels,
                    "fresh-boot dataset did not meet the immutable mature-label threshold",
                )
                .await;
        }
        let expected_purpose = if calibration {
            DatasetPurpose::Calibration
        } else {
            DatasetPurpose::Training
        };
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != expected_purpose
            || dataset.decision_policy_snapshot_id != run.decision_policy_snapshot_id
            || dataset.research_profile_artifact_id != run.research_profile_artifact_id
        {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::SourceSliceMismatch,
                    "successful dataset job did not produce the exact ready fresh-boot dataset",
                )
                .await;
        }
        let event = if calibration {
            FreshBootEventKind::CalibrationDatasetCompleted
        } else {
            FreshBootEventKind::DatasetCompleted
        };
        let patch = if calibration {
            FreshBootAdvancePatch {
                active_job_id: Some(None),
                last_job_id: Some(job.job_id),
                ..FreshBootAdvancePatch::default()
            }
        } else {
            FreshBootAdvancePatch {
                source_slice_id: Some(dataset.source_slice_id),
                source_slice_hash: Some(dataset.source_lineage.source_slice_identity_hash),
                active_job_id: Some(None),
                last_job_id: Some(job.job_id),
                ..FreshBootAdvancePatch::default()
            }
        };
        self.advance(run, event, patch).await
    }

    async fn enqueue_training(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let model_spec_id = run
            .model_spec_id
            .ok_or_else(|| Self::invalid("fresh boot has no model spec"))?;
        let dataset_id = run
            .training_dataset_id
            .ok_or_else(|| Self::invalid("fresh boot has no training dataset"))?;
        let model_version_id = ModelVersionId::from_fresh_boot(run.run_id);
        let params = ModelTrainJobParams {
            model_version_id,
            model_run_id: ModelRunId::from_fresh_boot_stage(run.run_id, "training"),
            request: TrainModelRequest {
                training_dataset_id: dataset_id,
                reason: format!("fresh-boot model training for run {}", run.run_id),
            },
        };
        let job = self
            .deps
            .job_port
            .enqueue_fresh_boot(
                run.run_id,
                "training",
                ResearchJobParams::ModelTrain(params),
                Some(model_spec_id),
                run.decision_policy_snapshot_id,
                run.last_job_id,
            )
            .await?;
        self.advance(
            run,
            FreshBootEventKind::TrainingEnqueued,
            FreshBootAdvancePatch {
                source_model_version_id: Some(model_version_id),
                active_job_id: Some(Some(job.job_id)),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn finish_training(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let job = self.load_active_job(&run).await?;
        if job.status.is_active() {
            return self.wait_for_job(run, &job).await;
        }
        if job.status != ResearchJobStatus::Succeeded {
            return self
                .block_job(run, &job, FreshBootBlockedReason::ModelTrainingFailed)
                .await;
        }
        let model_id = run
            .source_model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no expected source model"))?;
        Self::require_result(
            &job,
            ResearchJobResultKind::ModelVersion,
            model_id.as_uuid_ref(),
        )?;
        let model = self
            .deps
            .model_registry
            .find_model_version(&model_id)
            .await?
            .ok_or_else(|| Self::invalid("successful training job has no model version"))?;
        if !matches!(
            model.verified_derivation(),
            Ok(ModelVersionDerivation::Training)
        ) || model.training_dataset_id != run.training_dataset_id
        {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::ModelTrainingFailed,
                    "trained fresh-boot model has invalid root derivation or dataset binding",
                )
                .await;
        }
        self.advance(
            run,
            FreshBootEventKind::TrainingCompleted,
            FreshBootAdvancePatch {
                active_job_id: Some(None),
                last_job_id: Some(job.job_id),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn enqueue_calibration(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let model_spec_id = run
            .model_spec_id
            .ok_or_else(|| Self::invalid("fresh boot has no model spec"))?;
        let source_model = run
            .source_model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no source model"))?;
        let calibration_dataset = run
            .calibration_dataset_id
            .ok_or_else(|| Self::invalid("fresh boot has no calibration dataset"))?;
        let params = ModelCalibrationFitJobParams {
            model_run_id: ModelRunId::from_fresh_boot_stage(run.run_id, "calibration"),
            request: FitModelCalibratorRequest {
                model_version_id: source_model,
                calibration_dataset_id: calibration_dataset,
                method: CalibrationMethod::Platt,
                reason: format!("fresh-boot Platt calibration for run {}", run.run_id),
            },
            decision_policy_snapshot_id: run.decision_policy_snapshot_id,
            downside_source: DownsideSource::MfeMae,
            actor: GovernanceActor::system(),
        };
        let job = self
            .deps
            .job_port
            .enqueue_fresh_boot(
                run.run_id,
                "calibration",
                ResearchJobParams::ModelCalibrationFit(params),
                Some(model_spec_id),
                run.decision_policy_snapshot_id,
                run.last_job_id,
            )
            .await?;
        self.advance(
            run,
            FreshBootEventKind::CalibrationEnqueued,
            FreshBootAdvancePatch {
                active_job_id: Some(Some(job.job_id)),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn finish_calibration(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let job = self.load_active_job(&run).await?;
        if job.status.is_active() {
            return self.wait_for_job(run, &job).await;
        }
        if job.status != ResearchJobStatus::Succeeded {
            return self
                .block_job(run, &job, FreshBootBlockedReason::CalibrationFailed)
                .await;
        }
        let model_uuid = job.result_ref.ok_or_else(|| {
            Self::invalid("calibration completed without a calibrated model result")
        })?;
        if job.result_kind != Some(ResearchJobResultKind::ModelVersion) {
            return Err(Self::invalid(
                "calibration returned the wrong result namespace",
            ));
        }
        let model_id = ModelVersionId::new(model_uuid);
        let model = self
            .deps
            .model_registry
            .find_model_version(&model_id)
            .await?
            .ok_or_else(|| Self::invalid("calibration result model does not exist"))?;
        let derivation = model
            .verified_derivation()
            .map_err(|error| Self::invalid(error.to_string()))?;
        let ModelVersionDerivation::ReturnCalibration {
            parent_model_version_id,
            calibration_artifact_id,
        } = derivation
        else {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::CalibrationFailed,
                    "calibration job result is not a calibrated model derivation",
                )
                .await;
        };
        if Some(parent_model_version_id) != run.source_model_version_id {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::CalibrationFailed,
                    "calibrated model is not derived from the fresh-boot source model",
                )
                .await;
        }
        self.advance(
            run,
            FreshBootEventKind::CalibrationCompleted,
            FreshBootAdvancePatch {
                model_version_id: Some(model_id),
                calibration_id: Some(calibration_artifact_id),
                active_job_id: Some(None),
                last_job_id: Some(job.job_id),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn enqueue_cpcv(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let model_spec_id = run
            .model_spec_id
            .ok_or_else(|| Self::invalid("fresh boot has no model spec"))?;
        let model_version_id = run
            .model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no calibrated model"))?;
        let training_dataset_id = run
            .training_dataset_id
            .ok_or_else(|| Self::invalid("fresh boot has no training dataset"))?;
        let path_set_id = BacktestPathSetId::from_fresh_boot(run.run_id);
        let params = CpcvBacktestJobParams {
            model_version_id,
            model_run_id: ModelRunId::from_fresh_boot_stage(run.run_id, "cpcv"),
            request: RunCpcvBacktestRequest {
                training_dataset_id,
                decision_policy_snapshot_id: run.decision_policy_snapshot_id,
                reason: format!("fresh-boot CPCV validation for run {}", run.run_id),
                path_set_id: Some(path_set_id),
            },
        };
        let job = self
            .deps
            .job_port
            .enqueue_fresh_boot(
                run.run_id,
                "cpcv",
                ResearchJobParams::CpcvBacktest(params),
                Some(model_spec_id),
                run.decision_policy_snapshot_id,
                run.last_job_id,
            )
            .await?;
        self.advance(
            run,
            FreshBootEventKind::CpcvEnqueued,
            FreshBootAdvancePatch {
                path_set_id: Some(path_set_id),
                active_job_id: Some(Some(job.job_id)),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn finish_cpcv(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let job = self.load_active_job(&run).await?;
        if job.status.is_active() {
            return self.wait_for_job(run, &job).await;
        }
        if job.status != ResearchJobStatus::Succeeded {
            return self
                .block_job(run, &job, FreshBootBlockedReason::CpcvFailed)
                .await;
        }
        let path_set_id = run
            .path_set_id
            .ok_or_else(|| Self::invalid("fresh boot has no expected CPCV path set"))?;
        Self::require_result(
            &job,
            ResearchJobResultKind::BacktestPathSet,
            path_set_id.as_uuid_ref(),
        )?;
        let path_set = self
            .deps
            .path_sets
            .find_by_id(&path_set_id)
            .await?
            .ok_or_else(|| Self::invalid("successful CPCV job has no path-set row"))?;
        if Some(path_set.model_version_id) != run.model_version_id
            || Some(path_set.training_dataset_id) != run.training_dataset_id
            || path_set.decision_policy_snapshot_id != run.decision_policy_snapshot_id
        {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::CpcvFailed,
                    "CPCV path set differs from the fresh-boot candidate preimage",
                )
                .await;
        }
        self.advance(
            run,
            FreshBootEventKind::CpcvCompleted,
            FreshBootAdvancePatch {
                active_job_id: Some(None),
                last_job_id: Some(job.job_id),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn verify_parity(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let model_id = run
            .model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no calibrated model"))?;
        let model = self
            .deps
            .model_registry
            .find_model_version(&model_id)
            .await?
            .ok_or_else(|| Self::invalid("fresh-boot calibrated model does not exist"))?;
        let parity = match self
            .deps
            .parity
            .verify_and_record(
                &model,
                JOB_ACTOR,
                "fresh-boot full frozen dataset/model parity",
            )
            .await
        {
            Ok(parity) => parity,
            Err(error) if Self::retry_reason(&error).is_some() => return Err(error),
            Err(error) => {
                return self
                    .block(
                        run,
                        FreshBootBlockedReason::ParityFailed,
                        &format!("fresh-boot parity failed: {error}"),
                    )
                    .await;
            }
        };
        self.advance(
            run,
            FreshBootEventKind::ParityVerified,
            FreshBootAdvancePatch {
                parity_run_id: Some(parity.run_id),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn bind_scenario(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let model_id = run
            .model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no calibrated model"))?;
        let plan = match Box::pin(self.deps.bootstrap.prepare(model_id)).await {
            Ok(plan) => plan,
            Err(error) if Self::retry_reason(&error).is_some() => return Err(error),
            Err(error) => {
                return self
                    .block(
                        run,
                        FreshBootBlockedReason::ScenarioBindingFailed,
                        &format!("fresh-boot scenario preparation failed: {error}"),
                    )
                    .await;
            }
        };
        let binding = plan.preflight().manifest().scenario_model_binding();
        self.advance(
            run,
            FreshBootEventKind::ScenarioBound,
            FreshBootAdvancePatch {
                scenario_artifact_id: Some(binding.portfolio_scenario_model_artifact_id),
                scenario_artifact_hash: Some(binding.model_content_hash),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn persist_preflight(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let model_id = run
            .model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no calibrated model"))?;
        let plan = match Box::pin(self.deps.bootstrap.prepare(model_id)).await {
            Ok(plan) => plan,
            Err(error) if Self::retry_reason(&error).is_some() => return Err(error),
            Err(error) => {
                return self
                    .block(
                        run,
                        FreshBootBlockedReason::QualityGateFailed,
                        &format!("fresh-boot bootstrap preflight failed: {error}"),
                    )
                    .await;
            }
        };
        let preflight = plan.preflight().clone();
        let binding = preflight.manifest().scenario_model_binding();
        self.advance(
            run,
            FreshBootEventKind::BootstrapPrepared,
            FreshBootAdvancePatch {
                scenario_artifact_id: Some(binding.portfolio_scenario_model_artifact_id),
                scenario_artifact_hash: Some(binding.model_content_hash),
                bootstrap_preflight_hash: Some(preflight.preflight_hash()),
                bootstrap_preflight: Some(preflight),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn commit_bootstrap(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let preflight = run
            .bootstrap_preflight
            .clone()
            .ok_or_else(|| Self::invalid("fresh boot lost its exact bootstrap preflight"))?;
        if Some(preflight.preflight_hash()) != run.bootstrap_preflight_hash {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::BootstrapConflict,
                    "fresh-boot persisted preflight hash does not match its typed document",
                )
                .await;
        }
        let model_version_id = run
            .model_version_id
            .ok_or_else(|| Self::invalid("fresh boot has no calibrated model"))?;
        let request = BootstrapModelRoute {
            model_version_id,
            expected_policy_generation: preflight.expected_policy_generation(),
            expected_runtime_control_revision: preflight.expected_runtime_revision(),
            idempotency_key: run.idempotency_key.clone(),
            actor: ModelRouteBootstrapActor::FreshBootOrchestrator,
            reason_code: FRESH_BOOT_REASON_CODE.to_owned(),
            note: format!("system fresh-boot activation for run {}", run.run_id),
        };
        let preflight_hash = preflight.preflight_hash();
        let commit = match self
            .deps
            .route_governance
            .bootstrap_prepared(request, preflight)
            .await
        {
            Ok(commit) => commit,
            Err(error) => {
                let plan = match Box::pin(self.deps.bootstrap.prepare(model_version_id)).await {
                    Ok(plan) => plan,
                    Err(refresh_error) if Self::retry_reason(&refresh_error).is_some() => {
                        return Err(refresh_error);
                    }
                    Err(refresh_error) => {
                        return self
                            .block(
                                run,
                                FreshBootBlockedReason::BootstrapConflict,
                                &format!(
                                    "bootstrap commit and preflight refresh failed: commit={error}; refresh={refresh_error}"
                                ),
                            )
                            .await;
                    }
                };
                let refreshed = plan.preflight().clone();
                if refreshed.preflight_hash() != preflight_hash {
                    let binding = refreshed.manifest().scenario_model_binding();
                    return self
                        .advance(
                            run,
                            FreshBootEventKind::PreflightRefreshed,
                            FreshBootAdvancePatch {
                                scenario_artifact_id: Some(
                                    binding.portfolio_scenario_model_artifact_id,
                                ),
                                scenario_artifact_hash: Some(binding.model_content_hash),
                                bootstrap_preflight_hash: Some(refreshed.preflight_hash()),
                                bootstrap_preflight: Some(refreshed),
                                ..FreshBootAdvancePatch::default()
                            },
                        )
                        .await;
                }
                if let Some(reason) = Self::retry_reason(&error) {
                    return self
                        .schedule_retry(
                            run,
                            reason,
                            &format!("bootstrap dependency failed: {error}"),
                            true,
                        )
                        .await;
                }
                return self
                    .block(
                        run,
                        FreshBootBlockedReason::BootstrapConflict,
                        &format!("bootstrap rejected an unchanged governed preflight: {error}"),
                    )
                    .await;
            }
        };
        self.advance(
            run,
            FreshBootEventKind::BootstrapCommitted,
            FreshBootAdvancePatch {
                bootstrap_policy_activation_id: Some(commit.activation.policy_activation_id),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn enable_report(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let now = Utc::now();
        let request_id = format!("fresh-boot-{}", run.run_id);
        let outcome = match self
            .deps
            .report_lifecycle
            .run_ad_hoc(AdHocReportRequest {
                request_id,
                trigger_time: now,
                top_n: Some(20),
                knowledge_lag_secs: None,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return self
                    .schedule_retry(
                        run,
                        FreshBootRetryReason::DependencyUnavailable,
                        &format!("fresh-boot report enqueue failed: {error}"),
                        true,
                    )
                    .await;
            }
        };
        let bundle = self
            .deps
            .policies
            .load_current_bundle()
            .await?
            .ok_or_else(|| Self::invalid("bootstrap commit has no current policy bundle"))?;
        let next_scheduled_report_at = self
            .deps
            .report_runs
            .list_schedule_states()
            .await?
            .into_iter()
            .filter(|state| {
                state.enabled
                    && state.decision_policy_snapshot_id == bundle.decision_policy_snapshot_id
            })
            .map(|state| state.next_scheduled_for)
            .min();
        self.advance(
            run,
            FreshBootEventKind::ReportEnabled,
            FreshBootAdvancePatch {
                manual_report_ready_at: Some(now),
                first_report_run_id: Some(outcome.run().report_run_id),
                next_scheduled_report_at,
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn observe_report(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        let report_run_id = run
            .first_report_run_id
            .ok_or_else(|| Self::invalid("report-eligible fresh boot has no report run"))?;
        let report_run = self
            .deps
            .report_runs
            .find_by_id(&report_run_id)
            .await?
            .ok_or_else(|| Self::invalid("fresh-boot report run does not exist"))?;
        match report_run.status {
            ReportRunStatus::Queued | ReportRunStatus::Running => Ok(()),
            ReportRunStatus::Failed | ReportRunStatus::Skipped | ReportRunStatus::Abandoned => {
                self.retry_report(run).await
            }
            ReportRunStatus::Succeeded => {
                let report_id = report_run.output_report_id.ok_or_else(|| {
                    Self::invalid("successful fresh-boot report run has no output report")
                })?;
                let report = self
                    .deps
                    .reports
                    .find_by_id(&report_id)
                    .await?
                    .ok_or_else(|| Self::invalid("fresh-boot output report does not exist"))?;
                if report.status == RecommendationReportStatus::Prepared {
                    return Ok(());
                }
                if report.status != RecommendationReportStatus::Published {
                    return self.retry_report(run).await;
                }
                self.advance(
                    run,
                    FreshBootEventKind::ReportPublished,
                    FreshBootAdvancePatch {
                        first_report_id: Some(report_id),
                        ..FreshBootAdvancePatch::default()
                    },
                )
                .await
            }
        }
    }

    async fn retry_report(&self, run: FreshBootRunInfo) -> QuantResult<()> {
        if run.retry_count >= FRESH_BOOT_MAX_RETRY_COUNT {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::RetryBudgetExhausted,
                    "fresh-boot report retry budget was exhausted",
                )
                .await;
        }
        let next_retry_count = run.retry_count.saturating_add(1);
        let now = Utc::now();
        let request_id = format!("fresh-boot-{}-report-{}", run.run_id, run.revision + 1);
        let outcome = match self
            .deps
            .report_lifecycle
            .run_ad_hoc(AdHocReportRequest {
                request_id,
                trigger_time: now,
                top_n: Some(20),
                knowledge_lag_secs: None,
            })
            .await
        {
            Ok(outcome) => outcome,
            Err(error) => {
                return self
                    .schedule_retry(
                        run,
                        FreshBootRetryReason::ReportPending,
                        &format!("fresh-boot report retry enqueue failed: {error}"),
                        true,
                    )
                    .await;
            }
        };
        self.advance(
            run,
            FreshBootEventKind::ReportRetried,
            FreshBootAdvancePatch {
                first_report_run_id: Some(outcome.run().report_run_id),
                retry_count: Some(next_retry_count),
                ..FreshBootAdvancePatch::default()
            },
        )
        .await
    }

    async fn load_active_job(&self, run: &FreshBootRunInfo) -> QuantResult<ResearchJobInfo> {
        let job_id = run
            .active_job_id
            .ok_or_else(|| Self::invalid("fresh-boot job stage has no active job id"))?;
        self.deps
            .jobs
            .find_by_id(&job_id)
            .await?
            .ok_or_else(|| Self::invalid("fresh-boot active job does not exist"))
    }

    fn require_result(
        job: &ResearchJobInfo,
        expected_kind: ResearchJobResultKind,
        expected_id: &Uuid,
    ) -> QuantResult<()> {
        if job.result_kind != Some(expected_kind) || job.result_ref.as_ref() != Some(expected_id) {
            return Err(Self::invalid(
                "successful fresh-boot job returned a different result identity",
            ));
        }
        Ok(())
    }

    async fn advance(
        &self,
        run: FreshBootRunInfo,
        event: FreshBootEventKind,
        patch: FreshBootAdvancePatch,
    ) -> QuantResult<()> {
        let evidence_hash = patch
            .source_coverage_hash
            .or(patch.source_slice_hash)
            .or(patch.scenario_artifact_hash)
            .or(patch.bootstrap_preflight_hash);
        let updated = self
            .deps
            .runs
            .advance(AdvanceFreshBootRun {
                run_id: run.run_id,
                expected_revision: run.revision,
                event,
                patch,
                evidence_hash,
                actor: JOB_ACTOR.to_owned(),
                detail: None,
                occurred_at: Utc::now(),
            })
            .await?;
        info!(
            run_id = %updated.run_id,
            stage = %updated.stage,
            revision = updated.revision,
            "fresh-boot run advanced"
        );
        Ok(())
    }

    async fn block_job(
        &self,
        run: FreshBootRunInfo,
        job: &ResearchJobInfo,
        reason: FreshBootBlockedReason,
    ) -> QuantResult<()> {
        let detail = job.error_json.as_ref().map_or_else(
            || format!("fresh-boot job {} terminated as {}", job.job_id, job.status),
            |error| format!("fresh-boot job {} failed: {}", job.job_id, error.message),
        );
        self.block(run, reason, &detail).await
    }

    async fn block(
        &self,
        run: FreshBootRunInfo,
        reason: FreshBootBlockedReason,
        detail: &str,
    ) -> QuantResult<()> {
        let detail = Self::bounded_detail(detail);
        self.deps
            .runs
            .block_terminal(BlockFreshBootRun {
                run_id: run.run_id,
                expected_revision: run.revision,
                reason,
                detail,
                actor: JOB_ACTOR.to_owned(),
                occurred_at: Utc::now(),
            })
            .await?;
        Ok(())
    }

    async fn schedule_wait(
        &self,
        run: FreshBootRunInfo,
        reason: FreshBootRetryReason,
        detail: &str,
    ) -> QuantResult<()> {
        let now = Utc::now();
        self.deps
            .runs
            .delay(DelayFreshBootRun {
                run_id: run.run_id,
                expected_revision: run.revision,
                status: FreshBootStatus::WaitingEvidence,
                reason,
                detail: Self::bounded_detail(detail),
                next_attempt_at: now + SOURCE_RETRY,
                consume_retry: false,
                actor: JOB_ACTOR.to_owned(),
                occurred_at: now,
            })
            .await?;
        Ok(())
    }

    async fn schedule_retry(
        &self,
        run: FreshBootRunInfo,
        reason: FreshBootRetryReason,
        detail: &str,
        consume_retry: bool,
    ) -> QuantResult<()> {
        if consume_retry && run.retry_count >= FRESH_BOOT_MAX_RETRY_COUNT {
            return self
                .block(
                    run,
                    FreshBootBlockedReason::RetryBudgetExhausted,
                    &format!("fresh-boot retry budget exhausted after: {detail}"),
                )
                .await;
        }
        let now = Utc::now();
        let retry_ordinal = run.retry_count.saturating_add(i32::from(consume_retry));
        self.deps
            .runs
            .delay(DelayFreshBootRun {
                run_id: run.run_id,
                expected_revision: run.revision,
                status: FreshBootStatus::RetryScheduled,
                reason,
                detail: Self::bounded_detail(detail),
                next_attempt_at: now + Self::retry_delay(retry_ordinal),
                consume_retry,
                actor: JOB_ACTOR.to_owned(),
                occurred_at: now,
            })
            .await?;
        Ok(())
    }

    async fn wait_for_job(&self, run: FreshBootRunInfo, job: &ResearchJobInfo) -> QuantResult<()> {
        match job.status {
            ResearchJobStatus::AwaitingEvidence => {
                self.schedule_wait(
                    run,
                    FreshBootRetryReason::JobRetryScheduled,
                    "research job is waiting for durable source evidence",
                )
                .await
            }
            ResearchJobStatus::RetryScheduled => {
                self.schedule_retry(
                    run,
                    FreshBootRetryReason::JobRetryScheduled,
                    "research job has a typed retry scheduled",
                    false,
                )
                .await
            }
            ResearchJobStatus::Queued | ResearchJobStatus::Running => Ok(()),
            ResearchJobStatus::Succeeded
            | ResearchJobStatus::Failed
            | ResearchJobStatus::Cancelled => Err(Self::invalid(
                "terminal research job was classified as active",
            )),
        }
    }

    async fn recover_error(
        &self,
        attempted: FreshBootRunInfo,
        error: &QuantError,
    ) -> QuantResult<()> {
        let Some(current) = self.deps.runs.find(&attempted.run_id).await? else {
            return Ok(());
        };
        if current.status != FreshBootStatus::Running || current.revision != attempted.revision {
            return Ok(());
        }
        let reason = Self::retry_reason(error);
        if let Some(reason) = reason {
            self.schedule_retry(current, reason, &error.to_string(), true)
                .await
        } else {
            self.block(
                current,
                FreshBootBlockedReason::QualityGateFailed,
                &format!("non-retryable fresh-boot failure: {error}"),
            )
            .await
        }
    }

    const fn retry_reason(error: &QuantError) -> Option<FreshBootRetryReason> {
        match error {
            QuantError::Storage(
                StorageError::Database(_)
                | StorageError::Connection(_)
                | StorageError::Timeout { .. }
                | StorageError::ClickHouse(_),
            ) => Some(FreshBootRetryReason::StorageTransient),
            QuantError::Api(_)
            | QuantError::Rpc(_)
            | QuantError::WebSocket(_)
            | QuantError::Infra(_)
            | QuantError::Account(_) => Some(FreshBootRetryReason::ProviderUnavailable),
            _ => None,
        }
    }

    fn bounded_detail(detail: &str) -> String {
        let detail = detail.trim();
        if detail.is_empty() {
            return "fresh-boot failure did not provide detail".to_owned();
        }
        if detail.len() <= 2_048 {
            return detail.to_owned();
        }
        let mut end = 2_048;
        while !detail.is_char_boundary(end) {
            end = end.saturating_sub(1);
        }
        detail[..end].to_owned()
    }

    fn retry_delay(retry_ordinal: i32) -> ChronoDuration {
        let exponent = retry_ordinal
            .clamp(0, FRESH_BOOT_MAX_RETRY_COUNT)
            .cast_unsigned();
        ChronoDuration::seconds((2_i64.pow(exponent) * 2).min(300))
    }

    fn source_requirements(
        profile: &ResearchProfileArtifact,
        windows: &DatasetWindows,
        retention: &RetentionRunwayEvidenceV1,
    ) -> Result<Vec<FreshBootSourceCoverage>, String> {
        let mut requirements = Vec::new();
        let mut gaps = Vec::new();
        for source in Self::required_sources(profile) {
            let observations = retention
                .observations
                .iter()
                .filter(|observation| observation.source == source)
                .collect::<Vec<_>>();
            if observations.is_empty() {
                gaps.push(format!("{source}: no durable observation"));
                continue;
            }
            for observation in observations {
                let (Some(earliest), Some(latest)) = (
                    observation.earliest_event_time,
                    observation.latest_event_time,
                ) else {
                    gaps.push(format!("{}: timestamps unavailable", observation.object));
                    continue;
                };
                if observation.row_count == 0
                    || earliest > windows.training_start
                    || latest < windows.calibration_end
                    || observation.table_ttl_expression.is_some()
                {
                    gaps.push(format!(
                        "{}: required {}..={}, observed {}..={} rows={}",
                        observation.object,
                        windows.training_start,
                        windows.calibration_end,
                        earliest,
                        latest,
                        observation.row_count
                    ));
                }
                requirements.push(FreshBootSourceCoverage {
                    source,
                    object: observation.object.clone(),
                    earliest_event_time: earliest,
                    latest_event_time: latest,
                    row_count: observation.row_count,
                });
            }
        }
        if !gaps.is_empty() {
            return Err(format!(
                "profile source coverage incomplete: {}",
                gaps.join("; ")
            ));
        }
        requirements.sort_unstable_by(|left, right| {
            (left.source, left.object.as_str()).cmp(&(right.source, right.object.as_str()))
        });
        Ok(requirements)
    }

    fn dataset_windows(
        profile: &ResearchProfileArtifact,
        pit_cutoff: DateTime<Utc>,
    ) -> DatasetWindows {
        let calibration_end =
            pit_cutoff - ChronoDuration::seconds(profile.spec.target_horizon_secs.cast_signed());
        let calibration_start = calibration_end - ChronoDuration::hours(CALIBRATION_WINDOW_HOURS);
        let training_end = calibration_start
            - ChronoDuration::seconds(profile.spec.purge_embargo_secs.cast_signed());
        let training_start =
            training_end - ChronoDuration::days(i64::from(profile.spec.fit_span_days));
        DatasetWindows {
            training_start,
            training_end,
            calibration_start,
            calibration_end,
        }
    }

    fn required_sources(profile: &ResearchProfileArtifact) -> BTreeSet<ResearchReadinessSource> {
        let profile_sources = profile.spec.required_sources();
        let needs_domain_observation = profile_sources.iter().any(|source| {
            matches!(
                source,
                ResearchProfileDataSource::AviationWeather
                    | ResearchProfileDataSource::GhcnhCalibration
                    | ResearchProfileDataSource::GefsEnsemble
            )
        });
        let mut required = profile_sources
            .into_iter()
            .map(ResearchReadinessSource::from)
            .collect::<BTreeSet<_>>();
        if needs_domain_observation {
            required.insert(ResearchReadinessSource::DomainObservation);
        }
        required
    }

    fn bootstrap_profiles() -> QuantResult<Vec<(ResearchProfileArtifact, BuyModelRoute)>> {
        let profiles = builtin_research_profiles().map_err(Self::invalid)?;
        [
            (POOLED_BINARY_1H_BOOTSTRAP_PROFILE_ID, BuyModelRoute::Pooled),
            (CRYPTO_PRICE_15M_BOOTSTRAP_PROFILE_ID, BuyModelRoute::Crypto),
            (
                WEATHER_FORECAST_24H_BOOTSTRAP_PROFILE_ID,
                BuyModelRoute::Weather,
            ),
        ]
        .into_iter()
        .map(|(profile_id, route)| {
            profiles
                .iter()
                .find(|profile| profile.profile_ref.id.as_str() == profile_id)
                .cloned()
                .map(|profile| (profile, route))
                .ok_or_else(|| {
                    Self::invalid(format!(
                        "built-in {profile_id} fresh-boot profile is missing"
                    ))
                })
        })
        .collect()
    }

    fn profile_for_run(run: &FreshBootRunInfo) -> QuantResult<ResearchProfileArtifact> {
        let profile = run
            .research_profile_artifact_id
            .profile_ref()
            .resolve_builtin_research_profile()
            .map_err(Self::invalid)?;
        if profile.profile_ref.content_hash != run.profile_hash {
            return Err(Self::invalid(
                "fresh-boot run profile hash differs from the built-in artifact",
            ));
        }
        Ok(profile)
    }

    fn invalid(detail: impl Into<String>) -> QuantError {
        StorageError::invariant_violation(Some("fresh_boot_orchestrator"), detail.into()).into()
    }
}

impl AppContext {
    pub fn register_fresh_boot(&self, runner: &mut AppRunner, job_port: Arc<CoreResearchJobPort>) {
        let orchestrator = FreshBootOrchestrator::new(FreshBootDeps {
            runs: Arc::clone(&self.infra.repos.fresh_boot) as Arc<dyn FreshBootRepository>,
            history: Arc::clone(&self.infra.repos.exchange_history)
                as Arc<dyn ExchangeHistoryRepository>,
            readiness: Arc::clone(&self.research.research_readiness),
            jobs: Arc::clone(&self.infra.repos.research_job) as Arc<dyn ResearchJobRepository>,
            job_port,
            model_registry: Arc::clone(&self.infra.repos.model_registry),
            datasets: Arc::clone(&self.infra.repos.training_dataset)
                as Arc<dyn TrainingDatasetRepository>,
            path_sets: Arc::clone(&self.infra.repos.backtest_path_set)
                as Arc<dyn BacktestPathSetRepository>,
            policies: Arc::clone(&self.infra.repos.runtime_config) as Arc<dyn PolicyRepository>,
            parity: Arc::clone(&self.research.frozen_model_parity),
            bootstrap: Arc::clone(&self.research.model_route_bootstrap),
            route_governance: Arc::clone(&self.research.model_route_governance),
            report_lifecycle: Arc::clone(&self.report.lifecycle),
            report_runs: Arc::clone(&self.infra.repos.report_run) as Arc<dyn ReportRunRepository>,
            reports: Arc::clone(&self.infra.repos.recommendation_report)
                as Arc<dyn RecommendationReportRepository>,
        });
        runner.spawn(TaskId::FreshBootOrchestrator, move |token| async move {
            Box::pin(orchestrator.run(token)).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use chrono::Duration as ChronoDuration;
    use quant_pivot_error::QuantResult;
    use quant_pivot_models::runtime_config::BuyModelRoute;

    use super::FreshBootOrchestrator;

    #[test]
    fn profiles_preserve_cadence() -> QuantResult<()> {
        let profiles = FreshBootOrchestrator::bootstrap_profiles()?;
        assert_eq!(profiles.len(), 3);
        for (profile, route) in profiles {
            let expected = match route {
                BuyModelRoute::Pooled | BuyModelRoute::Weather => 3_600,
                BuyModelRoute::Crypto => 300,
            };
            assert_eq!(profile.spec.decision_cadence_secs, expected);
        }
        Ok(())
    }

    #[test]
    fn retry_backoff_is_bounded() {
        let expected = [2, 4, 8, 16, 32, 64, 128, 256, 300, 300];
        for (ordinal, seconds) in (0_i32..).zip(expected) {
            assert_eq!(
                FreshBootOrchestrator::retry_delay(ordinal),
                ChronoDuration::seconds(seconds)
            );
        }
    }
}
