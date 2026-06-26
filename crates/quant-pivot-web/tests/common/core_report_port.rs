//! Real [`CoreQuantReportPort`] wiring for web integration tests (Phase 04.5).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quant_pivot_core::{
    app::quant_report::CoreQuantReportPort, infra::schedule::ReportScheduleRunner,
    report::AdHocReportRequest,
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    domain::CoreEventPublisher,
    runtime_config::{ReportScheduleConfig, ReportsConfig},
};
use quant_pivot_repository::traits::{RecommendationReportRepository, RecommendationRepository};
use quant_pivot_test_support::report_pipeline_harness::FixtureReportSeedContext;
use quant_pivot_test_support::report_pipeline_harness::{HarnessOptions, ReportPipelineHarness};
use sea_orm::DatabaseConnection;
use tokio_util::sync::CancellationToken;

/// Captures ad-hoc enqueue calls; all other scheduler hooks are no-ops.
pub struct FakeReportScheduleRunner {
    pub enqueued: Arc<Mutex<Vec<AdHocReportRequest>>>,
}

impl FakeReportScheduleRunner {
    #[must_use]
    pub const fn new(enqueued: Arc<Mutex<Vec<AdHocReportRequest>>>) -> Self {
        Self { enqueued }
    }
}

#[async_trait]
impl ReportScheduleRunner for FakeReportScheduleRunner {
    async fn upsert(&self, _schedule: &ReportScheduleConfig) -> QuantResult<()> {
        Ok(())
    }

    async fn remove(&self, _schedule_id: &str) -> QuantResult<()> {
        Ok(())
    }

    async fn sync_from_config(&self, _reports: &ReportsConfig) -> QuantResult<()> {
        Ok(())
    }

    async fn enqueue_ad_hoc(&self, request: AdHocReportRequest) -> QuantResult<()> {
        self.enqueued.lock().expect("enqueued lock").push(request);
        Ok(())
    }

    async fn run(&self, _shutdown: CancellationToken) -> QuantResult<()> {
        Ok(())
    }
}

/// Wired core report port plus handles tests inspect (ad-hoc queue).
pub struct CoreReportTestHandle {
    pub port: Arc<CoreQuantReportPort>,
    pub enqueued: Arc<Mutex<Vec<AdHocReportRequest>>>,
    pub fixture_ctx: FixtureReportSeedContext,
}

/// Bootstrap the report plane against Postgres and assemble [`CoreQuantReportPort`].
pub async fn build_core_report_stack(
    db: &DatabaseConnection,
    _events: CoreEventPublisher,
) -> CoreReportTestHandle {
    let harness = ReportPipelineHarness::bootstrap(db, HarnessOptions::default()).await;

    let enqueued = Arc::new(Mutex::new(Vec::new()));
    let scheduler = Arc::new(FakeReportScheduleRunner::new(Arc::clone(&enqueued)));

    let report_repo = Arc::clone(&harness.report_repo) as Arc<dyn RecommendationReportRepository>;
    let recommendation_repo =
        Arc::clone(&harness.recommendation_repo) as Arc<dyn RecommendationRepository>;

    let port = Arc::new(CoreQuantReportPort::new(
        report_repo,
        recommendation_repo,
        Arc::new(harness.lifecycle),
        scheduler,
    ));

    CoreReportTestHandle {
        port,
        enqueued,
        fixture_ctx: FixtureReportSeedContext {
            runtime_config_version_id: harness.runtime_config_version_id,
            model_version_id: harness.model_version_id,
        },
    }
}
