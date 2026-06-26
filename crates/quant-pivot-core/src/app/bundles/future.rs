//! Later-phase bundles now active for Phase 04 report generation.

use std::sync::Arc;

use quant_pivot_models::domain::CoreEventPublisher;
use quant_pivot_repository::{
    postgres::{PgRecommendationReportRepository, PgRuntimeConfigVersionRepository},
    traits::RecommendationReportRepository,
};

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle, ResearchBundle};
use crate::report::{
    DefaultRecommendationComposer, DefaultReportBuilder, ReportBuilderDeps, ReportLifecycleDeps,
    ReportLifecycleService, ReportPublisher, ReportPublisherDeps,
};

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
}

impl ReportBundle {
    /// Assemble report builder/composer/publisher/lifecycle.
    #[must_use]
    pub fn assemble(deps: ReportBundleDeps<'_>) -> Self {
        let report_repo: Arc<dyn RecommendationReportRepository> = Arc::new(
            PgRecommendationReportRepository::new(deps.infra.pg.connection().clone()),
        );
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
            portfolio_planner: Arc::new(
                quant_pivot_research::portfolio::DefaultPortfolioPlanner::new(),
            ),
            composer,
            pit_source: Arc::clone(&deps.data.pit_source),
            runtime_mode: deps.governance.runtime_mode.clone(),
        }));
        let publisher = Arc::new(ReportPublisher::new(ReportPublisherDeps {
            events: deps.events,
            recommendation_writer: Arc::clone(&deps.infra.recommendation_event_writer),
            alerts: Arc::clone(&deps.governance.alerts),
            metrics: Arc::clone(&deps.infra.metrics),
        }));
        Self {
            lifecycle: Arc::new(ReportLifecycleService::new(ReportLifecycleDeps {
                report_repo,
                builder,
                publisher,
            })),
        }
    }
}

/// Portfolio planning bundle (Phase 4+).
pub struct PortfolioBundle;

/// Execution intent bundle (Phase 5+).
pub struct ExecutionIntentBundle;

/// Cross-bundle runtime channels (Phase 2+).
pub struct RuntimeChannels;
