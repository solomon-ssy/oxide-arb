//! Feature-integrity application service: exact-replay ledgers and latch governance.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use quant_pivot_error::{QuantError, QuantResult, research::ResearchError, storage::StorageError};
#[cfg(test)]
use quant_pivot_models::runtime_config::RUNTIME_CONFIG_SCHEMA_VERSION;
use quant_pivot_models::{
    domain::{
        AcknowledgeFeatureParityLatchRequest, FeatureIntegrityActionContext,
        FeatureIntegrityLatchView, FeatureIntegrityPort, FeatureIntegritySummaryView,
        FeatureParityEventListQuery, FeatureParityEventView, FeatureParityJobParams,
        FeatureParityRunInfo, FeatureParityRunListQuery, FeatureParityRunView, NewFeatureParityRun,
        NewRecommendationReport, NewReportFeatureParity, NewResearchJob, Paginated,
        RecommendationReportInfo, ResearchJobView, RunFullFeatureParityRequest,
        RuntimeConfigVersionInfo,
    },
    enums::quant::{
        FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus, ReportTriggerKind,
        ResearchJobKind, ResearchJobStatus,
    },
    runtime_config::{RuntimeConfig, ScheduleCadence, preview_fire_times},
    types::{
        ContentHash, FeatureParityRunId, FeatureParityStateId, RecommendationReportId,
        ResearchJobId, RuntimeConfigVersionId,
    },
};
use quant_pivot_repository::traits::{
    CatalogVersionRepository, FeatureParityEventRepository, FeatureParityLatchActor,
    FeatureParityRepository, RuntimeConfigVersionRepository,
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
    runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
    max_recovery_attempts: i32,
}

impl FeatureParityRunCoordinator {
    #[must_use]
    pub const fn new(
        parity: Arc<dyn FeatureParityRepository>,
        runtime_config: Arc<dyn RuntimeConfigVersionRepository>,
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
    ) -> QuantResult<NewReportFeatureParity> {
        let (feature_contract_hash, materialization_timeout_secs) = self
            .feature_contract_for_report(&report.runtime_config_version_id, report)
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
            acting_role: SYSTEM_ACTING_ROLE.to_owned(),
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
            report.runtime_config_version_id.clone(),
            None,
            SYSTEM_ACTING_ROLE.to_owned(),
            materialization_timeout_secs,
        )?;
        Ok(NewReportFeatureParity { run, job })
    }

    /// Verify the invariant on an idempotent report lookup. This rejects legacy
    /// serving reports that were committed without a sampled replay instead of
    /// silently treating them as Phase 11.6-compliant.
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
            contract_and_full_timeout(&current.config_json)?;
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
            current.runtime_config_version_id,
            ctx.actor,
            ctx.acting_role,
            materialization_timeout_secs,
        )?;
        let (_, job) = self
            .parity
            .enqueue_run(run, job)
            .await
            .map_err(QuantError::from)?;
        Ok(ResearchJobView::from(job))
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
            contract_and_full_timeout(&current.config_json)?;
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
            current.runtime_config_version_id,
            None,
            SYSTEM_ACTING_ROLE.to_owned(),
            materialization_timeout_secs,
        )?;
        let job_id = job.job_id.clone();
        match self.parity.enqueue_run(run, job).await {
            Ok(_) => Ok(AutomaticFullParityOutcome::Enqueued { run_id, job_id }),
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

    async fn current_config(&self) -> QuantResult<RuntimeConfigVersionInfo> {
        self.runtime_config
            .load_current()
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| {
                QuantError::from(StorageError::not_found("runtime_config_version", "current"))
            })
    }

    async fn feature_contract_for_report(
        &self,
        version_id: &RuntimeConfigVersionId,
        report: &NewRecommendationReport,
    ) -> QuantResult<(ContentHash, u64)> {
        let version = self
            .runtime_config
            .load_version(version_id)
            .await
            .map_err(QuantError::from)?
            .ok_or_else(|| StorageError::not_found("runtime_config_version", version_id))?;
        contract_and_report_timeout(&version.config_json, report)
    }

    fn build_job(
        &self,
        parity_run_id: FeatureParityRunId,
        request: RunFullFeatureParityRequest,
        runtime_config_version_id: RuntimeConfigVersionId,
        requested_by: Option<String>,
        acting_role: String,
        materialization_timeout_secs: u64,
    ) -> QuantResult<NewResearchJob> {
        let params = serde_json::to_value(FeatureParityJobParams {
            parity_run_id,
            materialization_timeout_secs,
            request,
        })
        .map_err(|error| {
            QuantError::from(ResearchError::Serialization {
                detail: format!("feature parity job params serialization failed: {error}"),
            })
        })?;
        Ok(NewResearchJob {
            job_id: ResearchJobId::from_v7(),
            kind: ResearchJobKind::FeatureParity,
            status: ResearchJobStatus::Queued,
            model_spec_id: None,
            runtime_config_version_id: Some(runtime_config_version_id),
            params_json: params,
            requested_by,
            acting_role,
            parent_job_id: None,
            recovery_attempt: 0,
            max_recovery_attempts: self.max_recovery_attempts,
        })
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
    catalog: Arc<dyn CatalogVersionRepository>,
}

impl CatalogFeatureIntegrityCoverage {
    #[must_use]
    pub const fn new(catalog: Arc<dyn CatalogVersionRepository>) -> Self {
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

fn feature_contract(config_json: &serde_json::Value) -> QuantResult<(RuntimeConfig, ContentHash)> {
    let config = RuntimeConfig::from_json(config_json)?;
    let feature_contract_hash =
        ResearchHasher::feature_schema(&FeatureSchema::build(&config.features)?)?;
    Ok((config, feature_contract_hash))
}

/// A full replay inspects evidence that should already be durable. It has no
/// single report cadence, so it receives only the fixed writer-lag grace rather
/// than borrowing an unrelated schedule's (possibly day-long) interval.
fn contract_and_full_timeout(config_json: &serde_json::Value) -> QuantResult<(ContentHash, u64)> {
    let (_, feature_contract_hash) = feature_contract(config_json)?;
    Ok((feature_contract_hash, MIN_MATERIALIZATION_TIMEOUT_SECS))
}

/// Freeze the materialization deadline for one atomically committed report.
/// Scheduled reports use exactly their own frozen schedule; ad-hoc reports have
/// no cadence and therefore use the fixed ten-minute writer-lag grace.
fn contract_and_report_timeout(
    config_json: &serde_json::Value,
    report: &NewRecommendationReport,
) -> QuantResult<(ContentHash, u64)> {
    let (config, feature_contract_hash) = feature_contract(config_json)?;
    let timeout = report_materialization_timeout(
        &config,
        report.trigger_kind,
        &report.trigger_key,
        report.trigger_time,
        &report.recommendation_report_id,
    )?;
    Ok((feature_contract_hash, timeout))
}

fn report_materialization_timeout(
    config: &RuntimeConfig,
    trigger_kind: ReportTriggerKind,
    trigger_key: &str,
    trigger_time: DateTime<Utc>,
    report_id: &RecommendationReportId,
) -> QuantResult<u64> {
    let cadence = match trigger_kind {
        ReportTriggerKind::AdHoc => None,
        ReportTriggerKind::Scheduled => {
            let schedule_id = scheduled_report_id(trigger_key, trigger_time, report_id)?;
            let schedule = config
                .reports
                .schedules
                .iter()
                .find(|schedule| schedule.schedule_id == schedule_id)
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

fn scheduled_report_id<'a>(
    trigger_key: &'a str,
    trigger_time: DateTime<Utc>,
    report_id: &RecommendationReportId,
) -> QuantResult<&'a str> {
    let suffix = format!(":{}", trigger_time.to_rfc3339());
    trigger_key
        .strip_prefix("scheduled:")
        .and_then(|key| key.strip_suffix(&suffix))
        .filter(|schedule_id| !schedule_id.is_empty())
        .ok_or_else(|| {
            ResearchError::Determinism {
                detail: format!(
                    "scheduled report {report_id} has malformed trigger key `{trigger_key}`"
                ),
            }
            .into()
        })
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
        acting_role: args.acting_role,
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
            CompleteFeatureParityRun, FeatureParityRunInfo, FeatureParityStateInfo,
            NewRuntimeConfigActivation, NewRuntimeConfigVersion, ResearchJobInfo,
            RuntimeConfigActivationInfo, RuntimeConfigVersionInfo,
        },
        enums::{
            quant::{
                EmptyReportReason, FeatureParityStateTransition, RecommendationReportStatus,
                ReportKind,
            },
            runtime_config::RuntimeConfigVersionSource,
        },
        types::{
            ModelVersionId, RecommendationReportId, RuntimeConfigActivationId, TrainingDatasetId,
        },
    };
    use quant_pivot_repository::traits::FeatureParityLatchActor;
    use quant_pivot_test_support::report_fixtures;

    use super::*;

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
            runtime_config_version_id: job.runtime_config_version_id,
            params_json: job.params_json,
            progress_json: None,
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
        current: RuntimeConfigVersionInfo,
    }

    #[async_trait]
    impl RuntimeConfigVersionRepository for FixedRuntimeConfigRepository {
        async fn create_version(
            &self,
            _version: NewRuntimeConfigVersion,
        ) -> Result<RuntimeConfigVersionInfo, StorageError> {
            Err(unexpected("create_version"))
        }

        async fn activate_version(
            &self,
            _activation: NewRuntimeConfigActivation,
        ) -> Result<RuntimeConfigActivationInfo, StorageError> {
            Err(unexpected("activate_version"))
        }

        async fn activate_version_if_current(
            &self,
            _expected_current_activation_id: Option<&RuntimeConfigActivationId>,
            _activation: NewRuntimeConfigActivation,
        ) -> Result<RuntimeConfigActivationInfo, StorageError> {
            Err(unexpected("activate_version_if_current"))
        }

        async fn load_current_activation(
            &self,
        ) -> Result<Option<RuntimeConfigActivationInfo>, StorageError> {
            Err(unexpected("load_current_activation"))
        }

        async fn load_version(
            &self,
            version_id: &RuntimeConfigVersionId,
        ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
            Ok((&self.current.runtime_config_version_id == version_id)
                .then(|| self.current.clone()))
        }

        async fn load_by_hash(
            &self,
            _config_hash: &ContentHash,
        ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
            Err(unexpected("load_by_hash"))
        }

        async fn load_current(&self) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
            Ok(Some(self.current.clone()))
        }

        async fn load_active_at(
            &self,
            _at: DateTime<Utc>,
        ) -> Result<Option<RuntimeConfigVersionInfo>, StorageError> {
            Err(unexpected("load_active_at"))
        }

        async fn list_versions(
            &self,
            _limit: u64,
        ) -> Result<Vec<RuntimeConfigVersionInfo>, StorageError> {
            Err(unexpected("list_versions"))
        }

        async fn list_activations(
            &self,
            _limit: u64,
        ) -> Result<Vec<RuntimeConfigActivationInfo>, StorageError> {
            Err(unexpected("list_activations"))
        }
    }

    fn runtime_repo(now: DateTime<Utc>) -> FixedRuntimeConfigRepository {
        let config = RuntimeConfig::default();
        FixedRuntimeConfigRepository {
            current: RuntimeConfigVersionInfo {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash: hash(),
                schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                config_json: serde_json::to_value(config).expect("runtime config json"),
                source: RuntimeConfigVersionSource::Bootstrap,
                created_by: "test".to_owned(),
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
            report_kind: info.report_kind,
            trigger_kind: info.trigger_kind,
            trigger_key: info.trigger_key,
            trigger_time: info.trigger_time,
            knowledge_lag_secs: info.knowledge_lag_secs,
            decision_at: info.decision_at,
            horizon_secs: info.horizon_secs,
            runtime_mode: info.runtime_mode,
            runtime_config_version_id: info.runtime_config_version_id,
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
            valid_until: info.valid_until,
            revoked_at: info.revoked_at,
            expired_at: info.expired_at,
            status_reason: info.status_reason,
        }
    }

    #[tokio::test]
    async fn pre_inference_report_still_gets_atomic_sampled_parity() {
        let now = Utc::now();
        let runtime = runtime_repo(now);
        let runtime_config_version_id = runtime.current.runtime_config_version_id.clone();
        let coordinator = FeatureParityRunCoordinator::new(
            Arc::new(CoordinatorParityRepository::default()),
            Arc::new(runtime),
            3,
        );
        let mut info = report_fixtures::report(
            RecommendationReportId::from_v7(),
            ReportKind::TopN,
            RecommendationReportStatus::PublishedEmpty,
        );
        info.runtime_config_version_id = runtime_config_version_id.clone();
        info.model_run_id = None;
        info.trigger_key = format!(
            "scheduled:default_interval:{}",
            info.trigger_time.to_rfc3339()
        );
        info.summary_json.empty_reason = Some(EmptyReportReason::EmptySelection);
        let report = new_report(info);

        let parity = coordinator
            .build_report_sample(&report)
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
            parity.job.runtime_config_version_id.as_ref(),
            Some(&runtime_config_version_id)
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
        let params: FeatureParityJobParams =
            serde_json::from_value(job.params_json).expect("job params");
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
        let mut config = RuntimeConfig::default();
        let mut exact = config.reports.schedules[0].clone();
        exact.schedule_id = "desk:fast".to_owned();
        exact.cadence = ScheduleCadence::Interval { interval_secs: 900 };
        let mut unrelated = exact.clone();
        unrelated.schedule_id = "unrelated_daily".to_owned();
        unrelated.cadence = ScheduleCadence::Interval {
            interval_secs: 86_400,
        };
        config.reports.schedules = vec![exact, unrelated];

        let trigger_key = format!("scheduled:desk:fast:{}", trigger_time.to_rfc3339());
        let scheduled = report_materialization_timeout(
            &config,
            ReportTriggerKind::Scheduled,
            &trigger_key,
            trigger_time,
            &report_id,
        )
        .expect("exact scheduled timeout");
        assert_eq!(scheduled, 1_800);

        let ad_hoc = report_materialization_timeout(
            &config,
            ReportTriggerKind::AdHoc,
            "ad_hoc:request-1",
            trigger_time,
            &report_id,
        )
        .expect("ad-hoc timeout");
        assert_eq!(ad_hoc, MIN_MATERIALIZATION_TIMEOUT_SECS);
    }

    #[test]
    fn sampled_timeout_rejects_malformed_or_unknown_schedule_binding() {
        let trigger_time = Utc::now();
        let report_id = RecommendationReportId::from_v7();
        let config = RuntimeConfig::default();

        let malformed = report_materialization_timeout(
            &config,
            ReportTriggerKind::Scheduled,
            "scheduled:default_interval:wrong-time",
            trigger_time,
            &report_id,
        )
        .expect_err("malformed trigger key must fail closed");
        assert!(malformed.to_string().contains("malformed trigger key"));

        let unknown_key = format!("scheduled:unknown:{}", trigger_time.to_rfc3339());
        let unknown = report_materialization_timeout(
            &config,
            ReportTriggerKind::Scheduled,
            &unknown_key,
            trigger_time,
            &report_id,
        )
        .expect_err("unknown frozen schedule must fail closed");
        assert!(unknown.to_string().contains("unknown frozen schedule"));
    }

    #[test]
    fn full_replay_timeout_never_borrows_report_schedule_cadence() {
        let mut config = RuntimeConfig::default();
        config.reports.schedules[0].cadence = ScheduleCadence::Interval {
            interval_secs: 86_400,
        };
        let config_json = serde_json::to_value(config).expect("runtime config json");
        let (_, timeout) = contract_and_full_timeout(&config_json).expect("full timeout");
        assert_eq!(timeout, MIN_MATERIALIZATION_TIMEOUT_SECS);
    }
}
