//! Report scheduler wiring.
//!
//! Bridges the concrete report lifecycle into the `infra/schedule` cron runner
//! (the report side of the §23.2 layering): it implements the
//! [`ScheduledReportExecutor`] port for [`ReportLifecycleService`] and assembles
//! a [`TokioCronScheduleRunner`] with its dependencies.

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_error::QuantResult;
use quant_pivot_models::domain::RecommendationReportInfo;
use tokio_util::task::TaskTracker;

use crate::{
    infra::schedule::{
        ReportScheduleRunner, ReportSchedulerDeps, ScheduleOverlapGuard, ScheduledReportExecutor,
        TokioCronScheduleRunner,
    },
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    report::{AdHocReportRequest, ReportLifecycleService, ScheduledReportRequest},
};

#[async_trait]
impl ScheduledReportExecutor for ReportLifecycleService {
    async fn run_scheduled(
        &self,
        request: ScheduledReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        Self::run_scheduled(self, request).await
    }

    async fn run_ad_hoc(
        &self,
        request: AdHocReportRequest,
    ) -> QuantResult<RecommendationReportInfo> {
        Self::run_ad_hoc(self, request).await
    }
}

/// Assemble the report schedule runner from wired report-plane dependencies.
pub async fn build_report_scheduler(
    lifecycle: Arc<ReportLifecycleService>,
    metrics: Arc<MetricsHub>,
    alerts: Arc<AlertDispatcher>,
) -> QuantResult<Arc<dyn ReportScheduleRunner>> {
    let executor: Arc<dyn ScheduledReportExecutor> = lifecycle;
    let deps = Arc::new(ReportSchedulerDeps {
        executor,
        overlap: ScheduleOverlapGuard::new(),
        inflight: TaskTracker::new(),
        metrics,
        alerts,
    });
    let runner: Arc<dyn ReportScheduleRunner> = Arc::new(TokioCronScheduleRunner::new(deps).await?);
    Ok(runner)
}
