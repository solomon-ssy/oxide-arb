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
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::domain::CoreEventPublisher;
use quant_pivot_repository::{
    postgres::{
        PgEquitySnapshotRepository, PgPositionRepository, PgRecommendationReportRepository,
        PgRecommendationRepository, PgRuntimeConfigVersionRepository,
    },
    traits::{
        EquitySnapshotRepository, PositionRepository, RecommendationReportRepository,
        RecommendationRepository,
    },
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
}

/// Recommendation report bundle (Phase 4+).
pub struct ReportBundle {
    pub lifecycle: Arc<ReportLifecycleService>,
    pub scheduler: Arc<dyn ReportScheduleRunner>,
}

impl ReportBundle {
    /// Assemble report builder/composer/publisher/lifecycle + schedule runner.
    pub async fn assemble(deps: ReportBundleDeps<'_>) -> QuantResult<Self> {
        let report_repo: Arc<dyn RecommendationReportRepository> = Arc::new(
            PgRecommendationReportRepository::new(deps.infra.pg.connection().clone()),
        );
        let recommendation_repo: Arc<dyn RecommendationRepository> = Arc::new(
            PgRecommendationRepository::new(deps.infra.pg.connection().clone()),
        );
        let equity_repo: Arc<dyn EquitySnapshotRepository> = Arc::new(
            PgEquitySnapshotRepository::new(deps.infra.pg.connection().clone()),
        );
        let position_repo: Arc<dyn PositionRepository> = Arc::new(PgPositionRepository::new(
            deps.infra.pg.connection().clone(),
        ));
        let composer = Arc::new(DefaultRecommendationComposer::new());
        let builder = Arc::new(DefaultReportBuilder::new(ReportBuilderDeps {
            runtime_config_repo: Arc::new(PgRuntimeConfigVersionRepository::new(
                deps.infra.pg.connection().clone(),
            )),
            market_selector: Arc::clone(&deps.research.market_selector),
            market_selection_repo: Arc::clone(&deps.research.market_selection_repo),
            candidate_provider: Arc::clone(&deps.research.candidate_provider),
            feature_pipeline: Arc::clone(&deps.research.feature_pipeline),
            model_runner: Arc::clone(&deps.research.model_runner),
            account_provider_factory: Arc::clone(&deps.account.provider_factory),
            drawdown_provider: Arc::new(EquitySnapshotService::new(equity_repo, position_repo)),
            composer,
            pit_source: Arc::clone(&deps.data.pit_source),
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
            runtime_config_repo: Arc::new(PgRuntimeConfigVersionRepository::new(
                deps.infra.pg.connection().clone(),
            )),
            builder,
            publisher,
            runtime_mode: deps.governance.runtime_mode.clone(),
            metrics: Arc::clone(&deps.infra.metrics),
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
        })
    }
}

/// Portfolio planning bundle (Phase 4+).
pub struct PortfolioBundle;

/// Cross-bundle runtime channels (Phase 2+).
pub struct RuntimeChannels;
