//! Later-phase bundles now active for Phase 04 report generation.

use std::sync::Arc;

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle, ResearchBundle};
use crate::{
    infra::schedule::ReportScheduleRunner,
    report::{
        DefaultRecommendationComposer, DefaultReportBuilder, DefaultReportReadinessGate,
        ReportBuilderDeps, ReportLifecycleDeps, ReportLifecycleService, ReportPublisher,
        ReportPublisherDeps, build_report_scheduler,
    },
    service::equity::EquitySnapshotService,
    service::feature_integrity::{FeatureParityRunCoordinator, RepositoryFeatureParityGate},
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::domain::CoreEventPublisher;
use quant_pivot_repository::traits::{
    EquitySnapshotRepository, FeatureParityRepository, PositionRepository,
    RecommendationReportRepository, RecommendationRepository, RuntimeConfigVersionRepository,
};
use quant_pivot_research::portfolio::HistoricalCorrelationEstimator;

/// Dependencies for the recommendation report bundle.
pub struct ReportBundleDeps<'a> {
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub governance: &'a GovernanceBundle,
    pub research: &'a ResearchBundle,
    pub account: &'a AccountBundle,
    pub events: CoreEventPublisher,
    pub max_recovery_attempts: i32,
}

/// Recommendation report bundle (Phase 4+).
pub struct ReportBundle {
    pub lifecycle: Arc<ReportLifecycleService>,
    pub scheduler: Arc<dyn ReportScheduleRunner>,
    pub feature_parity: Arc<FeatureParityRunCoordinator>,
}

impl ReportBundle {
    /// Assemble report builder/composer/publisher/lifecycle + schedule runner.
    pub async fn assemble(deps: ReportBundleDeps<'_>) -> QuantResult<Self> {
        let repos = &deps.infra.repos;
        let report_repo: Arc<dyn RecommendationReportRepository> =
            Arc::clone(&repos.recommendation_report) as Arc<dyn RecommendationReportRepository>;
        let recommendation_repo: Arc<dyn RecommendationRepository> =
            Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>;
        let equity_repo: Arc<dyn EquitySnapshotRepository> =
            Arc::clone(&repos.equity_snapshot) as Arc<dyn EquitySnapshotRepository>;
        let position_repo: Arc<dyn PositionRepository> =
            Arc::clone(&repos.position) as Arc<dyn PositionRepository>;
        let runtime_config_repo: Arc<dyn RuntimeConfigVersionRepository> =
            Arc::clone(&repos.runtime_config) as Arc<dyn RuntimeConfigVersionRepository>;
        let feature_parity = Arc::new(FeatureParityRunCoordinator::new(
            Arc::clone(&repos.feature_parity) as Arc<dyn FeatureParityRepository>,
            Arc::clone(&runtime_config_repo),
            deps.max_recovery_attempts,
        ));
        let composer = Arc::new(DefaultRecommendationComposer::new());
        let builder = Arc::new(DefaultReportBuilder::new(ReportBuilderDeps {
            runtime_config_repo: Arc::clone(&runtime_config_repo),
            artifact_store: Arc::clone(&deps.research.artifact_store),
            calibration_loader: Arc::clone(&deps.research.calibration_loader),
            trade_policy_repo: Arc::clone(&deps.research.trade_policy_repo),
            market_selector: Arc::clone(&deps.research.market_selector),
            market_selection_repo: Arc::clone(&deps.research.market_selection_repo),
            candidate_provider: Arc::clone(&deps.research.candidate_provider),
            feature_pipeline: Arc::clone(&deps.research.feature_pipeline),
            model_runner: Arc::clone(&deps.research.model_runner),
            account_provider_factory: Arc::clone(&deps.account.provider_factory),
            drawdown_provider: Arc::new(EquitySnapshotService::new(
                Arc::clone(&equity_repo),
                Arc::clone(&position_repo),
            )),
            composer,
            quant_fact_read_repo: Arc::clone(&deps.infra.quant_fact_read),
            correlation_estimator: Arc::new(HistoricalCorrelationEstimator::new()),
            runtime_mode: deps.governance.runtime_mode.clone(),
            readiness_gate: Arc::new(DefaultReportReadinessGate::new(
                Arc::clone(&deps.data.catalog),
                Arc::clone(&deps.data.ws_manager),
            )),
        }));
        let publisher = Arc::new(ReportPublisher::new(ReportPublisherDeps {
            events: deps.events,
            recommendation_writer: Arc::clone(&deps.infra.recommendation_event_writer),
            alerts: Arc::clone(&deps.governance.alerts),
            metrics: Arc::clone(&deps.infra.metrics),
        }));
        let lifecycle = Arc::new(ReportLifecycleService::new(ReportLifecycleDeps {
            report_repo,
            recommendation_repo,
            runtime_config_repo,
            builder,
            publisher,
            runtime_mode: deps.governance.runtime_mode.clone(),
            metrics: Arc::clone(&deps.infra.metrics),
            feature_parity_gate: Arc::new(RepositoryFeatureParityGate::new(Arc::clone(
                &repos.feature_parity,
            )
                as Arc<dyn FeatureParityRepository>)),
            feature_parity_runs: Arc::clone(&feature_parity),
        }));
        let scheduler = build_report_scheduler(
            Arc::clone(&lifecycle),
            Arc::clone(&deps.infra.metrics),
            Arc::clone(&deps.governance.alerts),
        )
        .await?;
        Ok(Self {
            lifecycle,
            scheduler,
            feature_parity,
        })
    }
}

/// Portfolio planning bundle (Phase 4+).
pub struct PortfolioBundle;

/// Cross-bundle runtime channels (Phase 2+).
pub struct RuntimeChannels;
