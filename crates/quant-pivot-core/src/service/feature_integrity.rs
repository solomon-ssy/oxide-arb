//! Feature-integrity application service: exact-replay ledgers and latch governance.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
use quant_pivot_models::{
    domain::{
        api::{
            AcknowledgeFeatureParityLatchRequest, FeatureIntegrityLatchView,
            FeatureIntegritySummaryView, FeatureParityEventListQuery, FeatureParityEventView,
            FeatureParityJobParams, FeatureParityRunListQuery, FeatureParityRunView,
            ResearchJobView, RunFullFeatureParityRequest,
        },
        governance::DecisionPolicySnapshotInfo,
        pagination::Paginated,
        ports::{FeatureIntegrityActionContext, FeatureIntegrityPort},
        quant::{
            FeatureParityRunInfo, NewFeatureParityRun, NewRecommendationReport,
            NewReportFeatureParity, NewResearchJob, RecommendationReportInfo, ReportRunInfo,
        },
    },
    enums::{
        quant::{
            FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus,
            ReportTriggerKind, ResearchJobKind, ResearchJobStatus,
        },
        system::CapabilityReason,
    },
    runtime_config::{DecisionPolicySnapshot, ScheduleCadence, preview_fire_times},
    types::{
        ContentHash, DecisionPolicySnapshotId, FeatureParityRunId, FeatureParityStateId,
        RecommendationReportId, ReportScheduleId, ResearchJobId, ResearchJobParams, RoleCode,
    },
};
use quant_pivot_repository::traits::{
    CatalogLedgerRepository, EnqueueFrozenFeatureParityOutcome, FeatureParityEventRepository,
    FeatureParityLatchActor, FeatureParityRepository, PolicyRepository,
};
use quant_pivot_research::{features::FeatureSchema, hashing::ResearchHasher};

use crate::observability::metrics_hub::MetricsHub;
const DEFAULT_FULL_WINDOW_HOURS: i64 = 24;
const MAX_FULL_WINDOW_DAYS: i64 = 31;
const AUTOMATIC_FULL_INTERVAL_HOURS: i64 = 24;
const AUTOMATIC_FULL_BUCKET_SECS: i64 = 60 * 60;
const SAMPLED_REPORT_WINDOW_MILLIS: i64 = 1;
const MIN_MATERIALIZATION_TIMEOUT_SECS: u64 = 10 * 60;
const SYSTEM_ACTOR: &str = "system:feature_parity";
const SYSTEM_ACTING_ROLE: &str = "system";

/// Result of one idempotent automatic full-parity scheduling tick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AutomaticFullParityOutcome {
    NotEligible {
        reason: CapabilityReason,
    },
    NotDue,
    Existing(FeatureParityRunId),
    Enqueued {
        run_id: FeatureParityRunId,
        job_id: ResearchJobId,
    },
}

/// Single run factory for manual full, scheduled full, and report-bound sampled
/// parity. It freezes the exact runtime/feature contract in every durable job.
pub struct FeatureParityRunCoordinator {
    parity: Arc<dyn FeatureParityRepository>,
    runtime_config: Arc<dyn PolicyRepository>,
    max_recovery_attempts: i32,
}

impl FeatureParityRunCoordinator {
    #[must_use]
    pub const fn new(
        parity: Arc<dyn FeatureParityRepository>,
        runtime_config: Arc<dyn PolicyRepository>,
        max_recovery_attempts: i32,
    ) -> Self {
        Self {
            parity,
            runtime_config,
            max_recovery_attempts,
        }
    }

    /// Build the sampled run/job that must be committed atomically with one
    /// serving report. Reports that stop before inference still require
    /// selection and, when produced, feature/capture parity; their run carries
    /// no invented model-run binding and stops at the last stage actually
    /// executed online.
    pub async fn build_report_sample(
        &self,
        report: &NewRecommendationReport,
        report_run: &ReportRunInfo,
    ) -> QuantResult<NewReportFeatureParity> {
        let (feature_contract_hash, materialization_timeout_secs) = self
            .feature_contract_for_report(&report.decision_policy_snapshot_id, report, report_run)
            .await?;
        let window_end = report
            .decision_at
            .checked_add_signed(Duration::milliseconds(SAMPLED_REPORT_WINDOW_MILLIS))
            .ok_or_else(|| {
                QuantError::from(ResearchError::Determinism {
                    detail: format!(
                        "sampled parity window overflows for report {}",
                        report.recommendation_report_id
                    ),
                })
            })?;
        let reason = format!(
            "automatic sampled replay for committed report {}",
            report.recommendation_report_id
        );
        let run_id = FeatureParityRunId::from_v7();
        let request = RunFullFeatureParityRequest {
            window_start: Some(report.decision_at),
            window_end: Some(window_end),
            reason: reason.clone(),
        };
        let run = NewFeatureParityRun {
            run_id: run_id.clone(),
            kind: FeatureParityRunKind::Sampled,
            status: FeatureParityRunStatus::Queued,
            window_start: report.decision_at,
            window_end,
            report_id: Some(report.recommendation_report_id.clone()),
            model_version_id: Some(report.model_version_id.clone()),
            training_dataset_id: None,
            triggered_by: SYSTEM_ACTOR.to_owned(),
            requested_by: None,
            acting_role: RoleCode::new(SYSTEM_ACTING_ROLE),
            reason,
            total_count: 0,
            compared_count: 0,
            matched_count: 0,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(feature_contract_hash),
            transform_hash: None,
            failure_code: None,
            failure_detail: None,
            started_at: None,
            pending_since: None,
            containment_completed_at: None,
            finished_at: None,
        };
        let job = self.build_job(
            run_id,
            request,
            report.decision_policy_snapshot_id.clone(),
            None,
            SYSTEM_ACTING_ROLE.to_owned(),
            materialization_timeout_secs,
        );
        Ok(NewReportFeatureParity { run, job })
    }

    /// Verify the invariant on an idempotent report lookup. This rejects legacy
    /// serving reports that were committed without a sampled replay instead of
    /// silently treating them as parity-compliant.
    pub async fn ensure_report_sample_committed(
        &self,
        report: &RecommendationReportInfo,
    ) -> QuantResult<()> {
        let sampled = self
            .parity
            .find_sampled_report(&report.recommendation_report_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(ResearchError::Determinism {
                    detail: format!(
                        "serving report {} has no atomically committed sampled parity run",
                        report.recommendation_report_id
                    ),
                })
            })?;
        if sampled.model_version_id.as_ref() != Some(&report.model_version_id)
            || sampled.window_start != report.decision_at
            || sampled.report_id.as_ref() != Some(&report.recommendation_report_id)
        {
            return Err(ResearchError::Determinism {
                detail: format!(
                    "sampled parity run {} is not bound to exact report {} evidence",
                    sampled.run_id, report.recommendation_report_id
                ),
            }
            .into());
        }
        Ok(())
    }

    /// Enqueue a governed full replay using the current runtime contract.
    pub async fn request_full_run(
        &self,
        request: RunFullFeatureParityRequest,
        ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<ResearchJobView> {
        let (window_start, window_end) = resolve_full_window(&request)?;
        let current = self.current_config().await?;
        let (feature_contract_hash, materialization_timeout_secs) =
            contract_and_full_timeout(&current.snapshot)?;
        let parity_run_id = FeatureParityRunId::from_v7();
        let triggered_by = ctx.actor.clone().unwrap_or_else(|| ctx.acting_role.clone());
        let resolved_request = RunFullFeatureParityRequest {
            window_start: Some(window_start),
            window_end: Some(window_end),
            reason: request.reason.clone(),
        };
        let run = queued_full_run(QueuedFullRunArgs {
            run_id: parity_run_id.clone(),
            window_start,
            window_end,
            triggered_by,
            requested_by: ctx.actor.clone(),
            acting_role: ctx.acting_role.clone(),
            reason: request.reason,
            feature_contract_hash,
        });
        let job = self.build_job(
            parity_run_id,
            resolved_request,
            current.decision_policy_snapshot_id,
            ctx.actor,
            ctx.acting_role,
            materialization_timeout_secs,
        );
        let outcome = self
            .parity
            .enqueue_frozen_full(run, job)
            .await
            .map_err(QuantError::from)?;
        match outcome {
            EnqueueFrozenFeatureParityOutcome::NotEligible => Err(ResearchError::NotEligible {
                code: "no_serving_evidence",
                detail: "the requested parity window contains no durable serving subject"
                    .to_owned(),
            }
            .into()),
            EnqueueFrozenFeatureParityOutcome::Enqueued { job, .. } => {
                Ok(ResearchJobView::from(*job))
            }
        }
    }

    /// Ensure one full replay of the latest complete 24-hour UTC window. The
    /// exact hour bucket and DB uniqueness make this safe under replicas and
    /// restart races; a recent complete full replay suppresses unnecessary work.
    pub async fn ensure_automatic_full(
        &self,
        now: DateTime<Utc>,
    ) -> QuantResult<AutomaticFullParityOutcome> {
        let window_end = truncate_to_hour(now)?;
        let window_start = window_end - Duration::hours(AUTOMATIC_FULL_INTERVAL_HOURS);
        if let Some(latest) = self
            .parity
            .latest_unbound_full()
            .await
            .map_err(QuantError::from)?
            && latest.created_at > now - Duration::hours(AUTOMATIC_FULL_INTERVAL_HOURS)
            && latest.window_end >= now - Duration::hours(AUTOMATIC_FULL_INTERVAL_HOURS)
            && latest.window_end - latest.window_start
                >= Duration::hours(AUTOMATIC_FULL_INTERVAL_HOURS)
        {
            return Ok(AutomaticFullParityOutcome::NotDue);
        }
        if let Some(existing) = self
            .parity
            .find_full_window(window_start, window_end)
            .await
            .map_err(QuantError::from)?
        {
            return Ok(AutomaticFullParityOutcome::Existing(existing.run_id));
        }

        let current = self.current_config().await?;
        let (feature_contract_hash, materialization_timeout_secs) =
            contract_and_full_timeout(&current.snapshot)?;
        let run_id = FeatureParityRunId::from_v7();
        let reason = format!(
            "automatic 24-hour full replay ending {}",
            window_end.to_rfc3339()
        );
        let request = RunFullFeatureParityRequest {
            window_start: Some(window_start),
            window_end: Some(window_end),
            reason: reason.clone(),
        };
        let run = queued_full_run(QueuedFullRunArgs {
            run_id: run_id.clone(),
            window_start,
            window_end,
            triggered_by: SYSTEM_ACTOR.to_owned(),
            requested_by: None,
            acting_role: SYSTEM_ACTING_ROLE.to_owned(),
            reason,
            feature_contract_hash,
        });
        let job = self.build_job(
            run_id.clone(),
            request,
            current.decision_policy_snapshot_id,
            None,
            SYSTEM_ACTING_ROLE.to_owned(),
            materialization_timeout_secs,
        );
        let job_id = job.job_id.clone();
        match self.parity.enqueue_frozen_full(run, job).await {
            Ok(EnqueueFrozenFeatureParityOutcome::NotEligible) => {
                Ok(AutomaticFullParityOutcome::NotEligible {
                    reason: CapabilityReason::NoServingEvidence,
                })
            }
            Ok(EnqueueFrozenFeatureParityOutcome::Enqueued { .. }) => {
                Ok(AutomaticFullParityOutcome::Enqueued { run_id, job_id })
            }
            Err(StorageError::Duplicate { .. }) => self
                .parity
                .find_full_window(window_start, window_end)
                .await
                .map_err(QuantError::from)?
                .map(|existing| AutomaticFullParityOutcome::Existing(existing.run_id))
                .ok_or_else(|| {
                    QuantError::from(ResearchError::Determinism {
                        detail: format!(
                            "full parity window [{window_start}, {window_end}) reported duplicate but no ledger row exists"
                        ),
                    })
                }),
            Err(error) => Err(error.into()),
        }
    }

    async fn current_config(&self) -> QuantResult<DecisionPolicySnapshotInfo> {
        self.runtime_config
            .load_current()
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found(
                    "decision_policy_snapshot",
                    "current",
                ))
            })
    }

    async fn feature_contract_for_report(
        &self,
        version_id: &DecisionPolicySnapshotId,
        report: &NewRecommendationReport,
        report_run: &ReportRunInfo,
    ) -> QuantResult<(ContentHash, u64)> {
        let version = self
            .runtime_config
            .load_snapshot(version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::not_found("decision_policy_snapshot", version_id))?;
        contract_and_report_timeout(&version.snapshot, report, report_run)
    }

    fn build_job(
        &self,
        parity_run_id: FeatureParityRunId,
        request: RunFullFeatureParityRequest,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        requested_by: Option<String>,
        acting_role: String,
        materialization_timeout_secs: u64,
    ) -> NewResearchJob {
        let params = ResearchJobParams::FeatureParity(FeatureParityJobParams {
            parity_run_id,
            materialization_timeout_secs,
            request,
        });
        NewResearchJob {
            job_id: ResearchJobId::from_v7(),
            kind: ResearchJobKind::FeatureParity,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            decision_policy_snapshot_id: Some(decision_policy_snapshot_id),
            params_json: params,
            requested_by,
            acting_role: RoleCode::new(acting_role),
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: self.max_recovery_attempts,
        }
    }
}

/// Fail-closed admission boundary shared by report generation, model publish,
/// and entry admission.
#[async_trait]
pub trait FeatureParityGatePort: Send + Sync {
    async fn ensure_clear(&self, action: &'static str) -> QuantResult<()>;

    /// Capture the exact clear-latch generation for a commit-time transactional
    /// compare. A gate without a durable generation cannot authorize a
    /// risk-increasing commit.
    async fn commit_state_id(&self, action: &'static str) -> QuantResult<FeatureParityStateId> {
        self.ensure_clear(action).await?;
        Err(ResearchError::Determinism {
            detail: format!(
                "feature parity gate cannot provide a durable clear generation for {action}"
            ),
        }
        .into())
    }

    /// Capture an existing clear generation, or explicitly report that the
    /// append-only latch has never been initialized. Only first model publish
    /// may consume the uninitialized outcome with a subject-bound full proof.
    async fn publish_state_id(
        &self,
        action: &'static str,
    ) -> QuantResult<Option<FeatureParityStateId>> {
        self.commit_state_id(action).await.map(Some)
    }

    /// Record a governance integrity incident and open the global safety latch.
    /// Implementations must make the incident+latch transition durable before
    /// returning success.
    async fn trip_integrity_failure(
        &self,
        _source_run_id: &FeatureParityRunId,
        action: &'static str,
        _reason: String,
    ) -> QuantResult<FeatureParityRunId> {
        Err(ResearchError::Determinism {
            detail: format!(
                "feature parity gate cannot persist a governance integrity latch for {action}"
            ),
        }
        .into())
    }
}

/// Repository-backed feature-parity gate. An uninitialized ledger is blocked;
/// only an explicit governed `Clear` state admits new risk.
pub struct RepositoryFeatureParityGate {
    parity: Arc<dyn FeatureParityRepository>,
}

impl RepositoryFeatureParityGate {
    #[must_use]
    pub const fn new(parity: Arc<dyn FeatureParityRepository>) -> Self {
        Self { parity }
    }
}

#[async_trait]
impl FeatureParityGatePort for RepositoryFeatureParityGate {
    async fn ensure_clear(&self, action: &'static str) -> QuantResult<()> {
        let state = self.parity.current_state().await?;
        match state {
            Some(state)
                if state.state
                    == FeatureParityLatchState::Clear =>
            {
                Ok(())
            }
            Some(state) => Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity latch blocks {action}: cause_run_id={}, reason={}",
                    state
                        .cause_run_id
                        .as_ref()
                        .map_or_else(|| "unknown".to_owned(), ToString::to_string),
                    state.reason
                ),
            }
            .into()),
            None => Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity latch is uninitialized; {action} is blocked until a full run passes and is governed-acknowledged"
                ),
            }
            .into()),
        }
    }

    async fn commit_state_id(&self, action: &'static str) -> QuantResult<FeatureParityStateId> {
        let state = self.parity.current_state().await?;
        match state {
            Some(state)
                if state.state
                    == FeatureParityLatchState::Clear =>
            {
                Ok(state.state_id)
            }
            Some(state) => Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity latch blocks {action}: cause_run_id={}, reason={}",
                    state
                        .cause_run_id
                        .as_ref()
                        .map_or_else(|| "unknown".to_owned(), ToString::to_string),
                    state.reason
                ),
            }
            .into()),
            None => Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity latch is uninitialized; {action} cannot acquire a commit generation"
                ),
            }
            .into()),
        }
    }

    async fn publish_state_id(
        &self,
        action: &'static str,
    ) -> QuantResult<Option<FeatureParityStateId>> {
        let state = self.parity.current_state().await?;
        match state {
            Some(state) if state.state == FeatureParityLatchState::Clear => {
                Ok(Some(state.state_id))
            }
            Some(state) => Err(ResearchError::Determinism {
                detail: format!(
                    "feature parity latch blocks {action}: cause_run_id={}, reason={}",
                    state
                        .cause_run_id
                        .as_ref()
                        .map_or_else(|| "unknown".to_owned(), ToString::to_string),
                    state.reason
                ),
            }
            .into()),
            None => Ok(None),
        }
    }

    async fn trip_integrity_failure(
        &self,
        source_run_id: &FeatureParityRunId,
        _action: &'static str,
        reason: String,
    ) -> QuantResult<FeatureParityRunId> {
        self.parity
            .record_integrity_failure_and_open_latch(source_run_id, reason)
            .await
            .map(|(run, _state)| run.run_id)
            .map_err(QuantError::from)
    }
}

/// Optional catalog-boundary provider. Absence is serialized as `null`, never a
/// fabricated timestamp; the catalog plane wires the concrete provider once its
/// bitemporal watermark port is available.
#[async_trait]
pub trait FeatureIntegrityCoveragePort: Send + Sync {
    async fn bounds(&self) -> QuantResult<FeatureIntegrityCoverage>;
}

/// Real catalog coverage bounds exposed by the summary API.
#[derive(Debug, Clone, Copy)]
pub struct FeatureIntegrityCoverage {
    pub start: Option<DateTime<Utc>>,
    pub watermark: Option<DateTime<Utc>>,
}

/// Catalog-ledger coverage adapter used by the Feature Integrity summary.
pub struct CatalogFeatureIntegrityCoverage {
    catalog: Arc<dyn CatalogLedgerRepository>,
}

impl CatalogFeatureIntegrityCoverage {
    #[must_use]
    pub const fn new(catalog: Arc<dyn CatalogLedgerRepository>) -> Self {
        Self { catalog }
    }
}

#[async_trait]
impl FeatureIntegrityCoveragePort for CatalogFeatureIntegrityCoverage {
    async fn bounds(&self) -> QuantResult<FeatureIntegrityCoverage> {
        let (start, watermark) =
            tokio::try_join!(self.catalog.coverage_start(), self.catalog.watermark(),)?;
        Ok(FeatureIntegrityCoverage { start, watermark })
    }
}

/// Production feature-integrity application service.
pub struct FeatureIntegrityService {
    parity: Arc<dyn FeatureParityRepository>,
    coordinator: Arc<FeatureParityRunCoordinator>,
    events: Arc<dyn FeatureParityEventRepository>,
    coverage: Option<Arc<dyn FeatureIntegrityCoveragePort>>,
    metrics: Arc<MetricsHub>,
}

impl FeatureIntegrityService {
    #[must_use]
    pub fn new(
        coordinator: Arc<FeatureParityRunCoordinator>,
        events: Arc<dyn FeatureParityEventRepository>,
        coverage: Option<Arc<dyn FeatureIntegrityCoveragePort>>,
        metrics: Arc<MetricsHub>,
    ) -> Self {
        let parity = Arc::clone(&coordinator.parity);
        Self {
            parity,
            coordinator,
            events,
            coverage,
            metrics,
        }
    }

    fn run_view(info: FeatureParityRunInfo) -> QuantResult<FeatureParityRunView> {
        FeatureParityRunView::try_from_info(info).map_err(|detail| {
            QuantError::from(ResearchError::Determinism {
                detail: detail.to_owned(),
            })
        })
    }
}

#[async_trait]
impl FeatureIntegrityPort for FeatureIntegrityService {
    async fn summary(&self) -> QuantResult<FeatureIntegritySummaryView> {
        let state = self
            .parity
            .current_state()
            .await
            .map_err(QuantError::from)?;
        self.metrics.set_feature_parity_latch_open(
            state
                .as_ref()
                .is_none_or(|state| state.state == FeatureParityLatchState::Open),
        );
        let latest_sampled = self
            .parity
            .latest_run(FeatureParityRunKind::Sampled)
            .await
            .map_err(QuantError::from)?;
        let latest_full = self
            .parity
            .latest_unbound_full()
            .await
            .map_err(QuantError::from)?;
        let counts = self
            .events
            .summary_counts()
            .await
            .map_err(QuantError::from)?;
        let coverage = match self.coverage.as_ref() {
            Some(port) => port.bounds().await?,
            None => FeatureIntegrityCoverage {
                start: None,
                watermark: None,
            },
        };
        let last_sampled_run = latest_sampled.map(Self::run_view).transpose()?;
        let last_full_run = latest_full.map(Self::run_view).transpose()?;
        let latest_finished = [last_sampled_run.as_ref(), last_full_run.as_ref()]
            .into_iter()
            .flatten()
            .filter_map(|run| run.finished_at)
            .max();
        let parity_age_secs = latest_finished
            .map(|finished| {
                (Utc::now() - finished)
                    .to_std()
                    .map(|age| age.as_secs())
                    .map_err(|_| {
                        QuantError::from(ResearchError::Determinism {
                            detail: format!("latest parity completion {finished} is in the future"),
                        })
                    })
            })
            .transpose()?;
        Ok(FeatureIntegritySummaryView {
            catalog_coverage_start: coverage.start,
            catalog_watermark: coverage.watermark,
            feature_state_counts: counts.feature_state_counts,
            rejection_reason_counts: counts.rejection_reason_counts,
            last_full_run,
            last_sampled_run,
            latch: state.map_or_else(
                FeatureIntegrityLatchView::uninitialized,
                FeatureIntegrityLatchView::from,
            ),
            parity_age_secs,
        })
    }

    async fn list_runs(
        &self,
        query: FeatureParityRunListQuery,
    ) -> QuantResult<Paginated<FeatureParityRunView>> {
        let page = self
            .parity
            .page_runs(query)
            .await
            .map_err(QuantError::from)?;
        let items = page
            .items
            .into_iter()
            .map(Self::run_view)
            .collect::<QuantResult<Vec<_>>>()?;
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }

    async fn list_events(
        &self,
        query: FeatureParityEventListQuery,
    ) -> QuantResult<Paginated<FeatureParityEventView>> {
        self.events
            .page_events(query)
            .await
            .map_err(QuantError::from)
    }

    async fn request_full_run(
        &self,
        request: RunFullFeatureParityRequest,
        ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<ResearchJobView> {
        self.coordinator.request_full_run(request, ctx).await
    }

    async fn acknowledge_latch(
        &self,
        request: AcknowledgeFeatureParityLatchRequest,
        ctx: FeatureIntegrityActionContext,
    ) -> QuantResult<FeatureIntegrityLatchView> {
        let state = self
            .parity
            .acknowledge_latch(
                &request.parity_run_id,
                FeatureParityLatchActor {
                    actor: ctx.actor,
                    acting_role: ctx.acting_role,
                    reason: request.reason,
                },
            )
            .await
            .map_err(QuantError::from)?;
        self.metrics.set_feature_parity_latch_open(false);
        Ok(FeatureIntegrityLatchView::from(state))
    }
}

fn feature_contract(config: &DecisionPolicySnapshot) -> QuantResult<ContentHash> {
    ResearchHasher::feature_schema(&FeatureSchema::build(
        &config.profile_artifacts.features.definition,
    )?)
}

/// A full replay inspects evidence that should already be durable. It has no
/// single report cadence, so it receives only the fixed writer-lag grace rather
/// than borrowing an unrelated schedule's (possibly day-long) interval.
fn contract_and_full_timeout(config: &DecisionPolicySnapshot) -> QuantResult<(ContentHash, u64)> {
    let feature_contract_hash = feature_contract(config)?;
    Ok((feature_contract_hash, MIN_MATERIALIZATION_TIMEOUT_SECS))
}

/// Freeze the materialization deadline for one atomically committed report.
/// Scheduled reports use exactly their own frozen schedule; ad-hoc reports have
/// no cadence and therefore use the fixed ten-minute writer-lag grace.
fn contract_and_report_timeout(
    config: &DecisionPolicySnapshot,
    report: &NewRecommendationReport,
    report_run: &ReportRunInfo,
) -> QuantResult<(ContentHash, u64)> {
    let feature_contract_hash = feature_contract(config)?;
    let timeout = report_materialization_timeout(
        config,
        report_run.trigger_kind,
        report_run.schedule_id.as_ref(),
        report_run.scheduled_for.unwrap_or(report.decision_at),
        &report.recommendation_report_id,
    )?;
    Ok((feature_contract_hash, timeout))
}

fn report_materialization_timeout(
    config: &DecisionPolicySnapshot,
    trigger_kind: ReportTriggerKind,
    schedule_id: Option<&ReportScheduleId>,
    trigger_time: DateTime<Utc>,
    report_id: &RecommendationReportId,
) -> QuantResult<u64> {
    let cadence = match trigger_kind {
        ReportTriggerKind::AdHoc => None,
        ReportTriggerKind::Scheduled => {
            let schedule_id = schedule_id.ok_or_else(|| ResearchError::Determinism {
                detail: format!("scheduled report {report_id} has no schedule id"),
            })?;
            let schedule = config.report_schedule.schedules
                .iter()
                .find(|schedule| schedule.schedule_id == *schedule_id)
                .ok_or_else(|| {
                    QuantError::from(ResearchError::Determinism {
                        detail: format!(
                            "scheduled report {report_id} references unknown frozen schedule `{schedule_id}`"
                        ),
                    })
                })?;
            Some(cadence_secs(&schedule.cadence, trigger_time)?)
        }
    };
    let Some(cadence_secs) = cadence else {
        return Ok(MIN_MATERIALIZATION_TIMEOUT_SECS);
    };
    cadence_secs
        .checked_mul(2)
        .ok_or_else(|| {
            QuantError::from(ResearchError::Determinism {
                detail: format!("report cadence {cadence_secs}s overflows parity timeout"),
            })
        })
        .map(|twice_cadence| twice_cadence.max(MIN_MATERIALIZATION_TIMEOUT_SECS))
}

fn cadence_secs(cadence: &ScheduleCadence, reference_at: DateTime<Utc>) -> QuantResult<u64> {
    match cadence {
        ScheduleCadence::Interval { interval_secs } => Ok(*interval_secs),
        ScheduleCadence::Cron { .. } => {
            let occurrences = preview_fire_times(cadence, reference_at, 2)?;
            let [first, second, ..] = occurrences.as_slice() else {
                return Err(ResearchError::Determinism {
                    detail: "report cron did not yield two occurrences for parity timeout"
                        .to_owned(),
                }
                .into());
            };
            (*second - *first)
                .to_std()
                .map(|duration| duration.as_secs())
                .map_err(|_| {
                    ResearchError::Determinism {
                        detail: format!(
                            "report cron occurrences are not monotonic: {first} then {second}"
                        ),
                    }
                    .into()
                })
        }
    }
}

struct QueuedFullRunArgs {
    run_id: FeatureParityRunId,
    window_start: DateTime<Utc>,
    window_end: DateTime<Utc>,
    triggered_by: String,
    requested_by: Option<String>,
    acting_role: String,
    reason: String,
    feature_contract_hash: ContentHash,
}

fn queued_full_run(args: QueuedFullRunArgs) -> NewFeatureParityRun {
    NewFeatureParityRun {
        run_id: args.run_id,
        kind: FeatureParityRunKind::Full,
        status: FeatureParityRunStatus::Queued,
        window_start: args.window_start,
        window_end: args.window_end,
        report_id: None,
        model_version_id: None,
        training_dataset_id: None,
        triggered_by: args.triggered_by,
        requested_by: args.requested_by,
        acting_role: RoleCode::new(args.acting_role),
        reason: args.reason,
        total_count: 0,
        compared_count: 0,
        matched_count: 0,
        mismatched_count: 0,
        pending_materialization_count: 0,
        feature_contract_hash: Some(args.feature_contract_hash),
        transform_hash: None,
        failure_code: None,
        failure_detail: None,
        started_at: None,
        pending_since: None,
        containment_completed_at: None,
        finished_at: None,
    }
}

fn truncate_to_hour(value: DateTime<Utc>) -> QuantResult<DateTime<Utc>> {
    let timestamp = value.timestamp();
    let bucket = timestamp - timestamp.rem_euclid(AUTOMATIC_FULL_BUCKET_SECS);
    DateTime::from_timestamp(bucket, 0).ok_or_else(|| {
        ResearchError::Determinism {
            detail: format!("automatic parity hour bucket is outside chrono range: {bucket}"),
        }
        .into()
    })
}

fn resolve_full_window(
    request: &RunFullFeatureParityRequest,
) -> QuantResult<(DateTime<Utc>, DateTime<Utc>)> {
    let window_end = request.window_end.unwrap_or_else(Utc::now);
    let window_start = request
        .window_start
        .unwrap_or(window_end - Duration::hours(DEFAULT_FULL_WINDOW_HOURS));
    if window_end <= window_start {
        return Err(QuantError::from(StorageError::invariant_violation(
            Some("quant_feature_parity_run"),
            "window_end must be later than window_start",
        )));
    }
    if window_end - window_start > Duration::days(MAX_FULL_WINDOW_DAYS) {
        return Err(QuantError::from(StorageError::invariant_violation(
            Some("quant_feature_parity_run"),
            format!("full parity window cannot exceed {MAX_FULL_WINDOW_DAYS} days"),
        )));
    }
    Ok((window_start, window_end))
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard};

    use quant_pivot_models::{
        domain::{
            governance::{
                ConfigActivityInfo, ConfigResourceInventoryInfo, DecisionPolicySnapshotInfo,
                DecisionPolicySnapshotOptionInfo, NewDecisionPolicySnapshot, NewPolicyActivation,
                NewPolicyRevision, NewProductionBaseline, NewProductionEvidence,
                PolicyActivationCommit, PolicyActivationInfo, PolicyApprovalInfo,
                PolicyRevisionInfo, ProductionBaselineInfo, ProductionEvidenceInfo,
                RecordPolicyApproval,
            },
            ports::{LifecycleSchemaVerificationPort, ProductionEvidenceArtifactVerificationPort},
            quant::{
                CompleteFeatureParityRun, FeatureParityRunInfo, FeatureParityStateInfo,
                FrozenFeatureParitySubject, NewFrozenModelParitySubject, ResearchJobInfo,
            },
        },
        enums::{
            quant::{
                EmptyReportReason, FeatureParityStateTransition, RecommendationReportStatus,
                ReportKind, ReportRunStatus,
            },
            runtime_config::{
                ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActorKind,
                ProductionEvidenceKind,
            },
        },
        runtime_config::PolicyValidationEvidence,
        types::{
            ModelVersionId, PolicyApprovalId, PolicyBundleGeneration, PolicyRevisionId,
            RecommendationReportId, ReportRunId, ReportTriggerKey, TrainingDatasetId,
        },
    };
    use quant_pivot_repository::traits::FeatureParityLatchActor;

    use super::*;
    use crate::test_fixtures::report_fixtures;

    fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().expect("test mutex")
    }

    fn unexpected(operation: &str) -> StorageError {
        StorageError::invariant_violation(
            Some("feature_integrity_test"),
            format!("unexpected repository call: {operation}"),
        )
    }

    fn hash() -> ContentHash {
        ContentHash::parse(format!("blake3:{}", "b".repeat(64))).expect("content hash")
    }

    fn run_info(run: NewFeatureParityRun, now: DateTime<Utc>) -> FeatureParityRunInfo {
        FeatureParityRunInfo {
            run_id: run.run_id,
            kind: run.kind,
            status: run.status,
            window_start: run.window_start,
            window_end: run.window_end,
            report_id: run.report_id,
            model_version_id: run.model_version_id,
            training_dataset_id: run.training_dataset_id,
            triggered_by: run.triggered_by,
            requested_by: run.requested_by,
            acting_role: run.acting_role,
            reason: run.reason,
            total_count: run.total_count,
            compared_count: run.compared_count,
            matched_count: run.matched_count,
            mismatched_count: run.mismatched_count,
            pending_materialization_count: run.pending_materialization_count,
            feature_contract_hash: run.feature_contract_hash,
            transform_hash: run.transform_hash,
            failure_code: run.failure_code,
            failure_detail: run.failure_detail,
            started_at: run.started_at,
            pending_since: run.pending_since,
            containment_completed_at: run.containment_completed_at,
            finished_at: run.finished_at,
            created_at: now,
            updated_at: now,
        }
    }

    fn job_info(job: NewResearchJob, now: DateTime<Utc>) -> ResearchJobInfo {
        ResearchJobInfo {
            job_id: job.job_id,
            kind: job.kind,
            status: job.status,
            model_spec_id: job.model_spec_id,
            decision_policy_snapshot_id: job.decision_policy_snapshot_id,
            params_json: job.params_json,
            progress_json: None,
            result_kind: None,
            result_ref: None,
            error_json: None,
            coverage_json: None,
            requested_by: job.requested_by,
            acting_role: job.acting_role,
            parent_job_id: job.parent_job_id,
            recovery_attempt: job.recovery_attempt,
            max_recovery_attempts: job.max_recovery_attempts,
            lease_owner: None,
            lease_expires_at: None,
            started_at: None,
            finished_at: None,
            heartbeat_at: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[derive(Default)]
    struct CoordinatorParityRepository {
        latest: Mutex<Option<FeatureParityRunInfo>>,
        exact_window: Mutex<Option<FeatureParityRunInfo>>,
        enqueued: Mutex<Vec<(NewFeatureParityRun, NewResearchJob)>>,
    }

    #[async_trait]
    impl FeatureParityRepository for CoordinatorParityRepository {
        async fn create_run(
            &self,
            _run: NewFeatureParityRun,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected("create_run"))
        }

        async fn create_frozen_model_run(
            &self,
            _run: NewFeatureParityRun,
            _subject: NewFrozenModelParitySubject,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected("create_frozen_model_run"))
        }

        async fn enqueue_run(
            &self,
            run: NewFeatureParityRun,
            job: NewResearchJob,
        ) -> Result<(FeatureParityRunInfo, ResearchJobInfo), StorageError> {
            let now = Utc::now();
            let info = run_info(run.clone(), now);
            let job_info = job_info(job.clone(), now);
            lock(&self.enqueued).push((run, job));
            *lock(&self.latest) = Some(info.clone());
            *lock(&self.exact_window) = Some(info.clone());
            Ok((info, job_info))
        }

        async fn enqueue_frozen_full(
            &self,
            run: NewFeatureParityRun,
            job: NewResearchJob,
        ) -> Result<EnqueueFrozenFeatureParityOutcome, StorageError> {
            self.enqueue_run(run, job).await.map(|(run, job)| {
                EnqueueFrozenFeatureParityOutcome::Enqueued {
                    run: Box::new(run),
                    job: Box::new(job),
                }
            })
        }

        async fn load_frozen_subjects(
            &self,
            _run_id: &FeatureParityRunId,
        ) -> Result<Vec<FrozenFeatureParitySubject>, StorageError> {
            Ok(Vec::new())
        }

        async fn find_run(
            &self,
            _run_id: &FeatureParityRunId,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected("find_run"))
        }

        async fn page_runs(
            &self,
            _query: FeatureParityRunListQuery,
        ) -> Result<Paginated<FeatureParityRunInfo>, StorageError> {
            Err(unexpected("page_runs"))
        }

        async fn latest_run(
            &self,
            _kind: FeatureParityRunKind,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Ok(lock(&self.latest).clone())
        }

        async fn latest_unbound_full(&self) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Ok(lock(&self.latest).clone())
        }

        async fn find_full_window(
            &self,
            window_start: DateTime<Utc>,
            window_end: DateTime<Utc>,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Ok(lock(&self.exact_window)
                .clone()
                .filter(|run| run.window_start == window_start && run.window_end == window_end))
        }

        async fn find_sampled_report(
            &self,
            _report_id: &RecommendationReportId,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected("find_sampled_report"))
        }

        async fn latest_full_for_model(
            &self,
            _model_version_id: &ModelVersionId,
            _training_dataset_id: &TrainingDatasetId,
        ) -> Result<Option<FeatureParityRunInfo>, StorageError> {
            Err(unexpected("latest_full_for_model"))
        }

        async fn mark_running(
            &self,
            _run_id: &FeatureParityRunId,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected("mark_running"))
        }

        async fn complete_run(
            &self,
            _run_id: &FeatureParityRunId,
            _result: CompleteFeatureParityRun,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected("complete_run"))
        }

        async fn mark_containment_complete(
            &self,
            _run_id: &FeatureParityRunId,
        ) -> Result<FeatureParityRunInfo, StorageError> {
            Err(unexpected("mark_containment_complete"))
        }

        async fn current_state(&self) -> Result<Option<FeatureParityStateInfo>, StorageError> {
            Ok(None)
        }

        async fn open_latch(
            &self,
            _cause_run_id: &FeatureParityRunId,
            _transition: FeatureParityStateTransition,
            _reason: String,
        ) -> Result<FeatureParityStateInfo, StorageError> {
            Err(unexpected("open_latch"))
        }

        async fn acknowledge_latch(
            &self,
            _recovery_run_id: &FeatureParityRunId,
            _actor: FeatureParityLatchActor,
        ) -> Result<FeatureParityStateInfo, StorageError> {
            Err(unexpected("acknowledge_latch"))
        }
    }

    struct FixedRuntimeConfigRepository {
        current: DecisionPolicySnapshotInfo,
    }

    #[async_trait]
    impl PolicyRepository for FixedRuntimeConfigRepository {
        async fn create_revision(
            &self,
            _revision: NewPolicyRevision,
        ) -> Result<PolicyRevisionInfo, StorageError> {
            Err(unexpected("create_revision"))
        }

        async fn mark_revision_validated(
            &self,
            _revision_id: &PolicyRevisionId,
            _validation_evidence: PolicyValidationEvidence,
            _preflight_token_hash: ContentHash,
            _preflight_expires_at: DateTime<Utc>,
        ) -> Result<PolicyRevisionInfo, StorageError> {
            Err(unexpected("mark_revision_validated"))
        }

        async fn load_revision(
            &self,
            _revision_id: &PolicyRevisionId,
        ) -> Result<Option<PolicyRevisionInfo>, StorageError> {
            Err(unexpected("load_revision"))
        }

        async fn list_revisions(
            &self,
            _kind: ConfigResourceKind,
            _limit: u64,
        ) -> Result<Vec<PolicyRevisionInfo>, StorageError> {
            Err(unexpected("list_revisions"))
        }

        async fn list_all_revisions(
            &self,
            _limit: u64,
        ) -> Result<Vec<PolicyRevisionInfo>, StorageError> {
            Err(unexpected("list_all_revisions"))
        }

        async fn list_activity(
            &self,
            _limit: u64,
        ) -> Result<Vec<ConfigActivityInfo>, StorageError> {
            Err(unexpected("list_activity"))
        }

        async fn load_resource_inventory(
            &self,
        ) -> Result<ConfigResourceInventoryInfo, StorageError> {
            Err(unexpected("load_resource_inventory"))
        }

        async fn record_approval(
            &self,
            _approval: RecordPolicyApproval,
        ) -> Result<PolicyApprovalInfo, StorageError> {
            Err(unexpected("record_approval"))
        }

        async fn load_approval(
            &self,
            _approval_id: &PolicyApprovalId,
        ) -> Result<Option<PolicyApprovalInfo>, StorageError> {
            Err(unexpected("load_approval"))
        }

        async fn list_valid_approvals(
            &self,
            _kind: Option<ConfigResourceKind>,
            _limit: u64,
        ) -> Result<Vec<PolicyApprovalInfo>, StorageError> {
            Err(unexpected("list_valid_approvals"))
        }

        async fn activate_resource(
            &self,
            _activation: NewPolicyActivation,
            _snapshot: NewDecisionPolicySnapshot,
        ) -> Result<PolicyActivationCommit, StorageError> {
            Err(unexpected("activate_resource"))
        }

        async fn load_current_activation(
            &self,
            _kind: Option<ConfigResourceKind>,
        ) -> Result<Option<PolicyActivationInfo>, StorageError> {
            Err(unexpected("load_current_activation"))
        }

        async fn load_current_revision(
            &self,
            _kind: ConfigResourceKind,
        ) -> Result<Option<PolicyRevisionInfo>, StorageError> {
            Err(unexpected("load_current_revision"))
        }

        async fn load_snapshot(
            &self,
            version_id: &DecisionPolicySnapshotId,
        ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
            Ok((&self.current.decision_policy_snapshot_id == version_id)
                .then(|| self.current.clone()))
        }

        async fn load_current(&self) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
            Ok(Some(self.current.clone()))
        }

        async fn load_active_at(
            &self,
            _at: DateTime<Utc>,
        ) -> Result<Option<DecisionPolicySnapshotInfo>, StorageError> {
            Err(unexpected("load_active_at"))
        }

        async fn list_snapshots(
            &self,
            _limit: u64,
        ) -> Result<Vec<DecisionPolicySnapshotInfo>, StorageError> {
            Err(unexpected("list_snapshots"))
        }

        async fn list_snapshot_options(
            &self,
            _limit: u64,
        ) -> Result<Vec<DecisionPolicySnapshotOptionInfo>, StorageError> {
            Err(unexpected("list_snapshot_options"))
        }

        async fn list_activations(
            &self,
            _kind: Option<ConfigResourceKind>,
            _limit: u64,
        ) -> Result<Vec<PolicyActivationInfo>, StorageError> {
            Err(unexpected("list_activations"))
        }

        async fn load_production_baseline(
            &self,
        ) -> Result<Option<ProductionBaselineInfo>, StorageError> {
            Err(unexpected("load_production_baseline"))
        }

        async fn record_production_evidence(
            &self,
            _evidence: NewProductionEvidence,
            _schema_verification: &dyn LifecycleSchemaVerificationPort,
            _artifact_verification: &dyn ProductionEvidenceArtifactVerificationPort,
        ) -> Result<ProductionEvidenceInfo, StorageError> {
            Err(unexpected("record_production_evidence"))
        }

        async fn load_latest_production_evidence(
            &self,
            _kind: ProductionEvidenceKind,
        ) -> Result<Option<ProductionEvidenceInfo>, StorageError> {
            Err(unexpected("load_latest_production_evidence"))
        }

        async fn seal_production_baseline(
            &self,
            _baseline: NewProductionBaseline,
            _schema_verification: &dyn LifecycleSchemaVerificationPort,
            _artifact_verification: &dyn ProductionEvidenceArtifactVerificationPort,
        ) -> Result<ProductionBaselineInfo, StorageError> {
            Err(unexpected("seal_production_baseline"))
        }
    }

    fn runtime_repo(now: DateTime<Utc>) -> FixedRuntimeConfigRepository {
        let config = DecisionPolicySnapshot::default();
        FixedRuntimeConfigRepository {
            current: DecisionPolicySnapshotInfo {
                bundle_generation: PolicyBundleGeneration::FIRST,
                decision_policy_snapshot_id: DecisionPolicySnapshotId::from_v7(),
                snapshot_hash: hash(),
                snapshot: config,
                recommendation_policy_revision_id: PolicyRevisionId::from_v7(),
                execution_risk_policy_revision_id: PolicyRevisionId::from_v7(),
                model_routing_revision_id: PolicyRevisionId::from_v7(),
                report_schedule_revision_id: PolicyRevisionId::from_v7(),
                operational_control_revision_id: PolicyRevisionId::from_v7(),
                execution_authorization_revision_id: PolicyRevisionId::from_v7(),
                source: DecisionPolicySnapshotSource::Bootstrap,
                created_by_kind: PolicyActorKind::System,
                created_by_user_id: None,
                created_by_label: "test".to_owned(),
                reason: "test".to_owned(),
                created_at: now,
            },
        }
    }

    fn exact_full_run(
        now: DateTime<Utc>,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> FeatureParityRunInfo {
        run_info(
            queued_full_run(QueuedFullRunArgs {
                run_id: FeatureParityRunId::from_v7(),
                window_start,
                window_end,
                triggered_by: SYSTEM_ACTOR.to_owned(),
                requested_by: None,
                acting_role: SYSTEM_ACTING_ROLE.to_owned(),
                reason: "automatic".to_owned(),
                feature_contract_hash: hash(),
            }),
            now,
        )
    }

    fn new_report(info: RecommendationReportInfo) -> NewRecommendationReport {
        NewRecommendationReport {
            recommendation_report_id: info.recommendation_report_id,
            research_profile_artifact_id: info.profile_ref.artifact_id(),
            report_kind: info.report_kind,
            decision_at: info.decision_at,
            horizon_secs: info.horizon_secs,
            runtime_mode: info.runtime_mode,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            model_run_id: info.model_run_id,
            model_version_id: info.model_version_id,
            market_selection_id: info.market_selection_id,
            portfolio_plan_id: info.portfolio_plan_id,
            top_n: info.top_n,
            status: info.status,
            account_source: info.account_source,
            capital_base_usd: info.capital_base_usd,
            account_snapshot_ref: info.account_snapshot_ref,
            equity_snapshot_ref: info.equity_snapshot_ref,
            data_quality_snapshot_ref: info.data_quality_snapshot_ref,
            summary_json: info.summary_json,
            published_at: info.published_at,
            successor_report_id: info.successor_report_id,
            superseded_at: info.superseded_at,
            obsoleted_at: info.obsoleted_at,
            valid_until: info.valid_until,
            revoked_at: info.revoked_at,
            expired_at: info.expired_at,
            status_reason: info.status_reason,
        }
    }

    fn report_run(
        report: &NewRecommendationReport,
        trigger_kind: ReportTriggerKind,
        schedule_id: Option<&str>,
        scheduled_for: Option<DateTime<Utc>>,
    ) -> ReportRunInfo {
        ReportRunInfo {
            report_run_id: ReportRunId::from_v7(),
            trigger_kind,
            trigger_key: ReportTriggerKey::parse(format!(
                "test:{}",
                report.recommendation_report_id
            ))
            .expect("report trigger key"),
            schedule_id: schedule_id.map(Into::into),
            request_id: (trigger_kind == ReportTriggerKind::AdHoc).then(|| "test-request".into()),
            retry_of_run_id: None,
            scheduled_for,
            requested_at: report.decision_at,
            status: ReportRunStatus::Succeeded,
            started_at: Some(report.decision_at),
            decision_at: Some(report.decision_at),
            heartbeat_at: Some(report.decision_at),
            lease_expires_at: None,
            finished_at: Some(report.decision_at),
            lease_owner: None,
            decision_policy_snapshot_id: Some(report.decision_policy_snapshot_id.clone()),
            top_n: Some(report.top_n),
            knowledge_lag_secs: Some(10),
            output_report_id: Some(report.recommendation_report_id.clone()),
            terminal_reason: None,
            error_code: None,
            error_summary: None,
        }
    }

    #[tokio::test]
    async fn pre_inference_report_still_gets_atomic_sampled_parity() {
        let now = Utc::now();
        let runtime = runtime_repo(now);
        let decision_policy_snapshot_id = runtime.current.decision_policy_snapshot_id.clone();
        let coordinator = FeatureParityRunCoordinator::new(
            Arc::new(CoordinatorParityRepository::default()),
            Arc::new(runtime),
            3,
        );
        let mut info = report_fixtures::report(
            RecommendationReportId::from_v7(),
            ReportKind::TopN,
            RecommendationReportStatus::Published,
        );
        info.decision_policy_snapshot_id = decision_policy_snapshot_id.clone();
        info.model_run_id = None;
        info.summary_json.empty_reason = Some(EmptyReportReason::EmptySelection);
        let report = new_report(info);
        let run = report_run(
            &report,
            ReportTriggerKind::Scheduled,
            Some("default_interval"),
            Some(report.decision_at),
        );

        let parity = coordinator
            .build_report_sample(&report, &run)
            .await
            .expect("pre-inference sampled parity");

        assert_eq!(parity.run.kind, FeatureParityRunKind::Sampled);
        assert_eq!(
            parity.run.report_id.as_ref(),
            Some(&report.recommendation_report_id)
        );
        assert_eq!(
            parity.run.model_version_id.as_ref(),
            Some(&report.model_version_id)
        );
        assert_eq!(parity.job.kind, ResearchJobKind::FeatureParity);
        assert_eq!(
            parity.job.decision_policy_snapshot_id.as_ref(),
            Some(&decision_policy_snapshot_id)
        );
    }

    #[tokio::test]
    async fn automatic_full_freezes_exact_window_timeout_and_is_idempotent() {
        let now = DateTime::parse_from_rfc3339("2026-07-11T12:34:56Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let parity = Arc::new(CoordinatorParityRepository::default());
        let coordinator = FeatureParityRunCoordinator::new(
            Arc::clone(&parity) as Arc<dyn FeatureParityRepository>,
            Arc::new(runtime_repo(now)),
            3,
        );

        let first = coordinator
            .ensure_automatic_full(now)
            .await
            .expect("automatic enqueue");
        assert!(matches!(first, AutomaticFullParityOutcome::Enqueued { .. }));
        let (run, job) = {
            let enqueued = lock(&parity.enqueued);
            assert_eq!(enqueued.len(), 1);
            enqueued[0].clone()
        };
        let expected_end = DateTime::parse_from_rfc3339("2026-07-11T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        assert_eq!(run.window_end, expected_end);
        assert_eq!(run.window_start, expected_end - Duration::hours(24));
        let ResearchJobParams::FeatureParity(params) = job.params_json else {
            panic!("feature parity job params");
        };
        assert_eq!(params.request.window_start, Some(run.window_start));
        assert_eq!(params.request.window_end, Some(run.window_end));
        assert!(params.materialization_timeout_secs >= MIN_MATERIALIZATION_TIMEOUT_SECS);

        let second = coordinator
            .ensure_automatic_full(now + Duration::minutes(1))
            .await
            .expect("idempotent scheduler tick");
        assert_eq!(second, AutomaticFullParityOutcome::NotDue);
        assert_eq!(lock(&parity.enqueued).len(), 1);
    }

    #[tokio::test]
    async fn automatic_full_reuses_exact_window_before_loading_runtime_config() {
        let now = DateTime::parse_from_rfc3339("2026-07-11T12:34:56Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let window_end = truncate_to_hour(now).expect("hour bucket");
        let window_start = window_end - Duration::hours(24);
        let existing = exact_full_run(now - Duration::days(2), window_start, window_end);
        let existing_id = existing.run_id.clone();
        let parity = Arc::new(CoordinatorParityRepository::default());
        *lock(&parity.exact_window) = Some(existing);
        let coordinator = FeatureParityRunCoordinator::new(
            Arc::clone(&parity) as Arc<dyn FeatureParityRepository>,
            Arc::new(runtime_repo(now)),
            3,
        );

        let outcome = coordinator
            .ensure_automatic_full(now)
            .await
            .expect("existing exact window");
        assert_eq!(outcome, AutomaticFullParityOutcome::Existing(existing_id));
        assert!(lock(&parity.enqueued).is_empty());
    }

    #[test]
    fn sampled_timeout_uses_exact_report_schedule_and_ad_hoc_uses_fixed_grace() {
        let trigger_time = DateTime::parse_from_rfc3339("2026-07-11T12:34:56Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let report_id = RecommendationReportId::from_v7();
        let mut config = DecisionPolicySnapshot::default();
        let mut exact = config.report_schedule.schedules[0].clone();
        exact.schedule_id = "desk:fast".into();
        exact.cadence = ScheduleCadence::Interval { interval_secs: 900 };
        let mut unrelated = exact.clone();
        unrelated.schedule_id = "unrelated_daily".into();
        unrelated.cadence = ScheduleCadence::Interval {
            interval_secs: 86_400,
        };
        config.report_schedule.schedules = vec![exact, unrelated];

        let exact_schedule_id = ReportScheduleId::new("desk:fast");
        let scheduled = report_materialization_timeout(
            &config,
            ReportTriggerKind::Scheduled,
            Some(&exact_schedule_id),
            trigger_time,
            &report_id,
        )
        .expect("exact scheduled timeout");
        assert_eq!(scheduled, 1_800);

        let ad_hoc = report_materialization_timeout(
            &config,
            ReportTriggerKind::AdHoc,
            None,
            trigger_time,
            &report_id,
        )
        .expect("ad-hoc timeout");
        assert_eq!(ad_hoc, MIN_MATERIALIZATION_TIMEOUT_SECS);
    }

    #[test]
    fn sampled_timeout_rejects_missing_or_unknown_schedule_binding() {
        let trigger_time = Utc::now();
        let report_id = RecommendationReportId::from_v7();
        let config = DecisionPolicySnapshot::default();

        let missing = report_materialization_timeout(
            &config,
            ReportTriggerKind::Scheduled,
            None,
            trigger_time,
            &report_id,
        )
        .expect_err("missing schedule id must fail closed");
        assert!(missing.to_string().contains("has no schedule id"));

        let unknown_schedule_id = ReportScheduleId::new("unknown");
        let unknown = report_materialization_timeout(
            &config,
            ReportTriggerKind::Scheduled,
            Some(&unknown_schedule_id),
            trigger_time,
            &report_id,
        )
        .expect_err("unknown frozen schedule must fail closed");
        assert!(unknown.to_string().contains("unknown frozen schedule"));
    }

    #[test]
    fn full_replay_timeout_never_borrows_report_schedule_cadence() {
        let mut config = DecisionPolicySnapshot::default();
        config.report_schedule.schedules[0].cadence = ScheduleCadence::Interval {
            interval_secs: 86_400,
        };
        let (_, timeout) = contract_and_full_timeout(&config).expect("full timeout");
        assert_eq!(timeout, MIN_MATERIALIZATION_TIMEOUT_SECS);
    }
}
