//! Real [`CoreQuantReportPort`] wiring for web integration tests (Phase 04.5).

use std::sync::Arc;

use async_trait::async_trait;
use quant_pivot_core::app::ports::quant_report::{CoreQuantReportPort, CoreQuantReportPortDeps};
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    clickhouse::{
        QuantFeatureEventRow, QuantModelInputEventRow, QuantServingEvidenceCompletionRow,
    },
    domain::CoreEventPublisher,
    types::{FeatureVectorId, ModelRunId},
};
use quant_pivot_repository::postgres::{
    PgFeatureRepository, PgOperationLogRepository, PgOrderIntentRepository, PgPolicyRepository,
    PgReportRunRepository,
};
use quant_pivot_repository::traits::{
    FeatureRepository, OperationLogRepository, OrderIntentRepository, PolicyRepository,
    QuantFactReadRepository, RecommendationReportRepository, RecommendationRepository,
    ReportRunRepository, ServingEvidenceRepository,
};
use quant_pivot_test_support::report_pipeline_harness::FixtureReportSeedContext;
use quant_pivot_test_support::report_pipeline_harness::{HarnessOptions, ReportPipelineHarness};
use sea_orm::DatabaseConnection;

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

/// Wired core report port plus durable repositories tests inspect.
pub struct CoreReportTestHandle {
    pub port: Arc<CoreQuantReportPort>,
    pub report_runs: Arc<PgReportRunRepository>,
    pub fixture_ctx: FixtureReportSeedContext,
    pub quant_facts: Arc<crate::harness::MockQuantFactRead>,
}

/// Bootstrap the report plane against Postgres and assemble [`CoreQuantReportPort`].
pub async fn build_core_report_stack(
    db: &DatabaseConnection,
    _events: CoreEventPublisher,
) -> CoreReportTestHandle {
    let harness = ReportPipelineHarness::bootstrap(db, HarnessOptions::default()).await;

    let quant_facts = Arc::new(crate::harness::MockQuantFactRead::default());
    let report_runs = Arc::clone(&harness.report_run_repo);

    let report_repo = Arc::clone(&harness.report_repo) as Arc<dyn RecommendationReportRepository>;
    let recommendation_repo =
        Arc::clone(&harness.recommendation_repo) as Arc<dyn RecommendationRepository>;
    let order_intent_repo =
        Arc::new(PgOrderIntentRepository::new(db.clone())) as Arc<dyn OrderIntentRepository>;

    let port = Arc::new(CoreQuantReportPort::new(CoreQuantReportPortDeps {
        report_repo,
        report_run_repo: Arc::clone(&report_runs) as Arc<dyn ReportRunRepository>,
        recommendation_repo,
        order_intent_repo,
        lifecycle: Arc::new(harness.lifecycle),
        serving_evidence: Arc::new(EmptyServingEvidenceRepository),
        feature_repo: Arc::new(PgFeatureRepository::new(db.clone())) as Arc<dyn FeatureRepository>,
        runtime_config_repo: Arc::new(PgPolicyRepository::new(db.clone()))
            as Arc<dyn PolicyRepository>,
        quant_fact_read: Arc::clone(&quant_facts) as Arc<dyn QuantFactReadRepository>,
        operation_logs: Arc::new(PgOperationLogRepository::new(db.clone()))
            as Arc<dyn OperationLogRepository>,
    }));

    CoreReportTestHandle {
        port,
        report_runs,
        fixture_ctx: FixtureReportSeedContext {
            decision_policy_snapshot_id: harness.decision_policy_snapshot_id,
            model_version_id: harness.model_version_id,
        },
        quant_facts,
    }
}
