//! Real [`CoreQuantReportPort`] wiring for web integration tests (Phase 04.5).

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quant_pivot_core::{
    app::ports::quant_report::{CoreQuantReportPort, CoreQuantReportPortDeps},
    infra::schedule::ReportScheduleRunner,
    report::AdHocReportRequest,
};
use quant_pivot_error::{QuantResult, storage::StorageError};
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
    },
    domain::CoreEventPublisher,
    runtime_config::{ReportScheduleConfig, ReportsConfig},
    types::{FeatureVectorId, ModelRunId},
};
use quant_pivot_repository::postgres::{
    PgFeatureRepository, PgOrderIntentRepository, PgRuntimeConfigVersionRepository,
};
use quant_pivot_repository::traits::{
    FeatureRepository, OrderIntentRepository, QuantFactReadRepository,
    RecommendationReportRepository, RecommendationRepository, RuntimeConfigVersionRepository,
    ServingEvidenceRepository,
};
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

struct EmptyServingEvidenceRepository;

#[async_trait]
impl ServingEvidenceRepository for EmptyServingEvidenceRepository {
    async fn completions_for_runs(
        &self,
        _model_run_ids: &[ModelRunId],
    ) -> Result<Vec<QuantServingEvidenceCompletionRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn model_inputs_for_runs(
        &self,
        _model_run_ids: &[ModelRunId],
    ) -> Result<Vec<QuantModelInputEventRow>, StorageError> {
        Ok(Vec::new())
    }

    async fn feature_cells_for_vectors(
        &self,
        _feature_vector_ids: &[FeatureVectorId],
    ) -> Result<Vec<QuantFeatureEventRow>, StorageError> {
        Ok(Vec::new())
    }
}

/// Wired core report port plus handles tests inspect (ad-hoc queue).
pub struct CoreReportTestHandle {
    pub port: Arc<CoreQuantReportPort>,
    pub enqueued: Arc<Mutex<Vec<AdHocReportRequest>>>,
    pub fixture_ctx: FixtureReportSeedContext,
    pub quant_facts: Arc<crate::harness::MockQuantFactRead>,
}

/// Bootstrap the report plane against Postgres and assemble [`CoreQuantReportPort`].
pub async fn build_core_report_stack(
    db: &DatabaseConnection,
    _events: CoreEventPublisher,
) -> CoreReportTestHandle {
    let harness = ReportPipelineHarness::bootstrap(db, HarnessOptions::default()).await;

    let enqueued = Arc::new(Mutex::new(Vec::new()));
    let scheduler = Arc::new(FakeReportScheduleRunner::new(Arc::clone(&enqueued)));
    let quant_facts = Arc::new(crate::harness::MockQuantFactRead::default());

    let report_repo = Arc::clone(&harness.report_repo) as Arc<dyn RecommendationReportRepository>;
    let recommendation_repo =
        Arc::clone(&harness.recommendation_repo) as Arc<dyn RecommendationRepository>;
    let order_intent_repo =
        Arc::new(PgOrderIntentRepository::new(db.clone())) as Arc<dyn OrderIntentRepository>;

    let port = Arc::new(CoreQuantReportPort::new(CoreQuantReportPortDeps {
        report_repo,
        recommendation_repo,
        order_intent_repo,
        lifecycle: Arc::new(harness.lifecycle),
        scheduler,
        serving_evidence: Arc::new(EmptyServingEvidenceRepository),
        feature_repo: Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>,
        runtime_config_repo: Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()))
            as Arc<dyn RuntimeConfigVersionRepository>,
        quant_fact_read: Arc::clone(&quant_facts) as Arc<dyn QuantFactReadRepository>,
    }));

    CoreReportTestHandle {
        port,
        enqueued,
        fixture_ctx: FixtureReportSeedContext {
            runtime_config_version_id: harness.runtime_config_version_id,
            model_version_id: harness.model_version_id,
        },
        quant_facts,
    }
}
