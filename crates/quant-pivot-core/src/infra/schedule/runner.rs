//! Report schedule runner: the `tokio-cron-scheduler` facade.
//!
//! This is the **only** module allowed to depend on `tokio-cron-scheduler`
//! (enforced by `scripts/lint-quant-pivot-boundary.sh`). It owns "when to
//! fire"; it never owns "what to build". The report pipeline is reached through
//! the [`ScheduledReportExecutor`] port so the runner stays testable with fakes
//! and the lifecycle stays free of any scheduler dependency.
//!
//! Invariants (parent doc §23.5/§23.6):
//! - `trigger_time` is supplied here as `Utc::now()` at fire; `as_of` and the
//!   runtime-config version freeze happen downstream in the builder.
//! - Skip-if-running: a fire for an already in-flight `schedule_id` is dropped,
//!   never queued (no stale `as_of`, no duplicate `TopN`).
//! - A failed report records metrics + an operator alert and the scheduler
//!   keeps running; it never panics or tears down ingest.
//! - Graceful drain: shutdown stops new fires, then awaits in-flight builds via
//!   an owned [`TaskTracker`] within the Execution stage budget.

use std::{
    collections::HashSet,
    fmt::Display,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::Utc;
use dashmap::DashMap;
use quant_pivot_error::{QuantError, QuantResult, report::ReportError, scheduler::SchedulerError};
use quant_pivot_models::{
    domain::RecommendationReportInfo,
    enums::{
        common::{AlertCategory, AlertLevel, AlertSource},
        quant::RecommendationReportStatus,
    },
    runtime_config::{ReportScheduleConfig, ReportsConfig},
};
use tokio_cron_scheduler::{Job, JobScheduler};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use uuid::Uuid;

use crate::{
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
    report::{AdHocReportRequest, ScheduledReportRequest},
};

use super::{job_factory::job_for_cadence, overlap::ScheduleOverlapGuard};

fn scheduler_backend(error: impl Display) -> QuantError {
    SchedulerError::Backend {
        detail: error.to_string(),
    }
    .into()
}

/// Synthetic `schedule_id` label used for ad-hoc fires in metrics/alerts.
const AD_HOC_LABEL: &str = "ad_hoc";

/// The report pipeline as seen by the scheduler: it only needs to fire
/// scheduled / ad-hoc runs. Implemented by `ReportLifecycleService`; fakeable
/// in tests.
#[async_trait]
pub trait ScheduledReportExecutor: Send + Sync {
    /// Run a scheduled report (idempotency keyed by `schedule_id` + trigger).
    async fn run_scheduled(
        &self,
        request: ScheduledReportRequest,
    ) -> QuantResult<RecommendationReportInfo>;

    /// Run an ad-hoc report (idempotency keyed by `request_id`).
    async fn run_ad_hoc(
        &self,
        request: AdHocReportRequest,
    ) -> QuantResult<RecommendationReportInfo>;
}

/// When and how to fire report generation; not what to build (parent doc §23.3).
#[async_trait]
pub trait ReportScheduleRunner: Send + Sync {
    /// Idempotently upsert one schedule (interval / cron; disabled → remove).
    async fn upsert(&self, schedule: &ReportScheduleConfig) -> QuantResult<()>;

    /// Remove a schedule by id.
    async fn remove(&self, schedule_id: &str) -> QuantResult<()>;

    /// Rebuild all jobs from the active runtime-config snapshot.
    async fn sync_from_config(&self, reports: &ReportsConfig) -> QuantResult<()>;

    /// Enqueue a one-shot ad-hoc run (`POST /api/quant/reports/run`).
    async fn enqueue_ad_hoc(&self, request: AdHocReportRequest) -> QuantResult<()>;

    /// Run until cancellation; integrates with the `AppRunner` shutdown token.
    async fn run(&self, shutdown: CancellationToken) -> QuantResult<()>;
}

/// Shared dependencies for [`TokioCronScheduleRunner`] and its job closures.
pub struct ReportSchedulerDeps {
    /// Report pipeline port (lifecycle in production, fake in tests).
    pub executor: Arc<dyn ScheduledReportExecutor>,
    /// Per-`schedule_id` skip-if-running guard.
    pub overlap: ScheduleOverlapGuard,
    /// Tracks in-flight report builds for graceful drain.
    pub inflight: TaskTracker,
    /// Scheduler observability.
    pub metrics: Arc<MetricsHub>,
    /// Operator alerting for failed generations.
    pub alerts: Arc<AlertDispatcher>,
}

/// `tokio-cron-scheduler`-backed [`ReportScheduleRunner`].
pub struct TokioCronScheduleRunner {
    scheduler: JobScheduler,
    deps: Arc<ReportSchedulerDeps>,
    /// `schedule_id` → scheduler job id, for idempotent upsert/remove.
    jobs: DashMap<String, Uuid>,
}

impl TokioCronScheduleRunner {
    /// Build a runner with an in-memory scheduler (no postgres/nats storage).
    pub async fn new(deps: Arc<ReportSchedulerDeps>) -> QuantResult<Self> {
        let scheduler = JobScheduler::new().await.map_err(scheduler_backend)?;
        Ok(Self {
            scheduler,
            deps,
            jobs: DashMap::new(),
        })
    }

    fn publish_active_jobs(&self) {
        self.deps
            .metrics
            .set_report_schedule_active_jobs(self.jobs.len());
    }
}

#[async_trait]
impl ReportScheduleRunner for TokioCronScheduleRunner {
    async fn upsert(&self, schedule: &ReportScheduleConfig) -> QuantResult<()> {
        if !schedule.enabled {
            return self.remove(&schedule.schedule_id).await;
        }

        let schedule_id: Arc<str> = Arc::from(schedule.schedule_id.as_str());
        let deps = Arc::clone(&self.deps);
        let job = job_for_cadence(&schedule.cadence, move || {
            let schedule_id = Arc::clone(&schedule_id);
            let deps = Arc::clone(&deps);
            Box::pin(async move { dispatch_scheduled_fire(&schedule_id, &deps) })
        })?;

        // Add the new job before removing the old one so there is no fire gap.
        let new_id = self.scheduler.add(job).await.map_err(scheduler_backend)?;
        if let Some((_, old_id)) = self.jobs.remove(&schedule.schedule_id) {
            let _ = self.scheduler.remove(&old_id).await;
        }
        self.jobs.insert(schedule.schedule_id.clone(), new_id);
        self.publish_active_jobs();
        Ok(())
    }

    async fn remove(&self, schedule_id: &str) -> QuantResult<()> {
        if let Some((_, job_id)) = self.jobs.remove(schedule_id) {
            self.scheduler
                .remove(&job_id)
                .await
                .map_err(scheduler_backend)?;
            self.publish_active_jobs();
        }
        Ok(())
    }

    async fn sync_from_config(&self, reports: &ReportsConfig) -> QuantResult<()> {
        // Drop jobs whose schedule was deleted or disabled in the new snapshot.
        let desired = reports
            .schedules
            .iter()
            .filter(|schedule| schedule.enabled)
            .map(|schedule| schedule.schedule_id.as_str())
            .collect::<HashSet<_>>();
        let stale = self
            .jobs
            .iter()
            .map(|entry| entry.key().clone())
            .filter(|id| !desired.contains(id.as_str()))
            .collect::<Vec<_>>();
        for id in stale {
            self.remove(&id).await?;
        }

        for schedule in &reports.schedules {
            self.upsert(schedule).await?;
        }
        Ok(())
    }

    async fn enqueue_ad_hoc(&self, request: AdHocReportRequest) -> QuantResult<()> {
        let deps = Arc::clone(&self.deps);
        let job = Job::new_one_shot_async(Duration::ZERO, move |_uuid, _sched| {
            let deps = Arc::clone(&deps);
            let request = request.clone();
            Box::pin(async move { dispatch_ad_hoc_fire(request, &deps) })
        })
        .map_err(|error| {
            QuantError::from(SchedulerError::InvalidJobSpec {
                detail: error.to_string(),
            })
        })?;
        self.scheduler.add(job).await.map_err(scheduler_backend)?;
        Ok(())
    }

    async fn run(&self, shutdown: CancellationToken) -> QuantResult<()> {
        self.scheduler.start().await.map_err(scheduler_backend)?;
        shutdown.cancelled().await;

        // Stop firing new jobs, then drain in-flight builds (Execution stage).
        let mut scheduler = self.scheduler.clone();
        if let Err(error) = scheduler.shutdown().await {
            tracing::warn!(%error, "report scheduler shutdown returned an error");
        }
        self.deps.inflight.close();
        self.deps.inflight.wait().await;
        Ok(())
    }
}

/// Per-fire entry point: claim the skip-if-running slot, then run the build on
/// the in-flight tracker so shutdown can drain it. Runs synchronously inside the
/// scheduler's job future; the report build itself is spawned and tracked.
fn dispatch_scheduled_fire(schedule_id: &Arc<str>, deps: &Arc<ReportSchedulerDeps>) {
    let Some(guard) = deps.overlap.try_acquire(schedule_id) else {
        deps.metrics
            .inc_report_schedule_skipped_overlap(schedule_id);
        tracing::debug!(schedule_id = %schedule_id, "report fire skipped — prior run still in flight");
        return;
    };

    let task_schedule_id = Arc::clone(schedule_id);
    let task_deps = Arc::clone(deps);
    deps.inflight.spawn(async move {
        let _guard = guard; // held until the build finishes
        execute_scheduled_fire(&task_schedule_id, &task_deps).await;
    });
}

async fn execute_scheduled_fire(schedule_id: &str, deps: &ReportSchedulerDeps) {
    let request = ScheduledReportRequest {
        schedule_id: schedule_id.to_owned(),
        trigger_time: Utc::now(),
    };
    let started = Instant::now();
    match deps.executor.run_scheduled(request).await {
        Ok(report) => {
            deps.metrics.record_report_schedule_fire(
                schedule_id,
                fire_outcome(&report),
                started.elapsed(),
            );
        }
        Err(error) if empty_report_suppressed(&error) => {
            deps.metrics.record_report_schedule_fire(
                schedule_id,
                "skipped_empty",
                started.elapsed(),
            );
        }
        Err(error) => report_fire_failed(schedule_id, deps, &error, started.elapsed()),
    }
}

/// Ad-hoc fire dispatch: not overlap-guarded (idempotency is keyed by
/// `request_id`); tracked on the in-flight tracker for graceful drain.
fn dispatch_ad_hoc_fire(request: AdHocReportRequest, deps: &Arc<ReportSchedulerDeps>) {
    let task_deps = Arc::clone(deps);
    deps.inflight.spawn(async move {
        execute_ad_hoc_fire(request, &task_deps).await;
    });
}

async fn execute_ad_hoc_fire(request: AdHocReportRequest, deps: &ReportSchedulerDeps) {
    let started = Instant::now();
    match deps.executor.run_ad_hoc(request).await {
        Ok(report) => {
            deps.metrics.record_report_schedule_fire(
                AD_HOC_LABEL,
                fire_outcome(&report),
                started.elapsed(),
            );
        }
        Err(error) if empty_report_suppressed(&error) => {
            deps.metrics.record_report_schedule_fire(
                AD_HOC_LABEL,
                "skipped_empty",
                started.elapsed(),
            );
        }
        Err(error) => report_fire_failed(AD_HOC_LABEL, deps, &error, started.elapsed()),
    }
}

/// Failure isolation: record metrics + raise an operator alert; never panic.
fn report_fire_failed(
    schedule_id: &str,
    deps: &ReportSchedulerDeps,
    error: &QuantError,
    elapsed: Duration,
) {
    deps.metrics
        .record_report_schedule_fire(schedule_id, "error", elapsed);
    deps.alerts
        .dispatch_background(report_generation_failed_alert(schedule_id, error));
    tracing::error!(schedule_id, %error, "report generation failed");
}

const fn fire_outcome(report: &RecommendationReportInfo) -> &'static str {
    match report.status {
        RecommendationReportStatus::PublishedEmpty => "empty",
        RecommendationReportStatus::Published => "published",
        other => other.as_str(),
    }
}

const fn empty_report_suppressed(error: &QuantError) -> bool {
    matches!(
        error,
        QuantError::Report(ReportError::EmptyReportSuppressed { .. })
    )
}

fn report_generation_failed_alert(schedule_id: &str, error: &QuantError) -> Alert {
    Alert::new(
        format!("report-schedule-fail:{schedule_id}"),
        AlertLevel::Critical,
        AlertCategory::SchedulerHealth,
        AlertSource::ReportGenerator,
        format!("Report generation failed for schedule {schedule_id}"),
        error.to_string(),
        Utc::now(),
    )
    .with_affects_trading(false)
    .with_dedupe_secs(60)
}
