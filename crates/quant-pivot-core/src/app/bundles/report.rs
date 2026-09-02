//! Recommendation report runtime composition.

use std::sync::Arc;

use super::{AccountBundle, DataBundle, GovernanceBundle, InfraBundle, ResearchBundle};
use crate::{
    ingest::data_pipeline::MicrostructureCommitBarrier,
    report::{
        DefaultRecommendationComposer, DefaultReportBuilder, DefaultReportReadinessGate,
        ReportBuilderDeps, ReportCoordinator, ReportCoordinatorConfig, ReportFactDeliveryDeps,
        ReportFactDeliveryWorker, ReportLifecycleDeps, ReportLifecycleService, ReportPublisher,
        ReportPublisherDeps,
    },
    service::{
        economic_feedback::{EconomicFeedbackService, EconomicFeedbackServiceDeps},
        equity::EquitySnapshotService,
        feature_integrity::{FeatureParityRunCoordinator, RepositoryFeatureParityGate},
    },
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{config::QuantWorkersConfig, domain::runtime::CoreEventPublisher};
use quant_pivot_repository::{
    clickhouse::ChNativeReadRepository,
    traits::{
        AttributionArtifactRepository, EquitySnapshotRepository, FeatureParityRepository,
        PolicyRepository, RecommendationEconomicOutcomeRepository, RecommendationReportRepository,
        RecommendationRepository, ReportRunRepository, RouteEconomicHealthRepository,
        StrategyPositionLotRepository, VenueIncentiveRepository,
    },
};

/// Dependencies for the recommendation report bundle.
pub struct ReportBundleDeps<'a> {
    pub workers: &'a QuantWorkersConfig,
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub governance: &'a GovernanceBundle,
    pub research: &'a ResearchBundle,
    pub account: &'a AccountBundle,
    pub events: CoreEventPublisher,
    pub max_recovery_attempts: i32,
}

/// Recommendation report services and workers.
pub struct ReportBundle {
    pub lifecycle: Arc<ReportLifecycleService>,
    pub fact_delivery: Arc<ReportFactDeliveryWorker>,
    pub coordinator: Arc<ReportCoordinator>,
    pub feature_parity: Arc<FeatureParityRunCoordinator>,
    pub economic_feedback: Arc<EconomicFeedbackService>,
}

impl ReportBundle {
    /// Assemble report builder/composer/publisher/lifecycle + schedule runner.
    pub fn assemble(deps: ReportBundleDeps<'_>) -> QuantResult<Self> {
        let repos = &deps.infra.repos;
        let report_repo: Arc<dyn RecommendationReportRepository> =
            Arc::clone(&repos.recommendation_report) as Arc<dyn RecommendationReportRepository>;
        let recommendation_repo: Arc<dyn RecommendationRepository> =
            Arc::clone(&repos.recommendation) as Arc<dyn RecommendationRepository>;
        let equity_repo: Arc<dyn EquitySnapshotRepository> =
            Arc::clone(&repos.equity_snapshot) as Arc<dyn EquitySnapshotRepository>;
        let position_repo: Arc<dyn StrategyPositionLotRepository> =
            Arc::clone(&repos.position) as Arc<dyn StrategyPositionLotRepository>;
        let runtime_config_repo: Arc<dyn PolicyRepository> =
            Arc::clone(&repos.runtime_config) as Arc<dyn PolicyRepository>;
        let run_repo: Arc<dyn ReportRunRepository> =
            Arc::clone(&repos.report_run) as Arc<dyn ReportRunRepository>;
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
                Arc::clone(&deps.infra.repos.venue_incentive) as Arc<dyn VenueIncentiveRepository>,
                deps.account.execution_account.execution_account_id,
            )),
            composer,
            portfolio_solver: deps.research.portfolio_solver,
            runtime_controls: deps.governance.runtime_controls.clone(),
            readiness_gate: Arc::new(DefaultReportReadinessGate::new(
                Arc::clone(&deps.data.catalog),
                Arc::clone(&deps.data.ws_manager),
            )),
            microstructure_commit: Arc::clone(&deps.data.data_pipeline)
                as Arc<dyn MicrostructureCommitBarrier>,
            exchange_history_repo: Arc::clone(&deps.research.exchange_history_repo),
            venue_incentive_repo: Arc::clone(&deps.infra.repos.venue_incentive)
                as Arc<dyn VenueIncentiveRepository>,
            execution_account_id: deps.account.execution_account.execution_account_id,
            venue_incentive_stale_secs: deps
                .workers
                .venue_incentive_reconciliation_secs
                .saturating_mul(2),
            metrics: Arc::clone(&deps.infra.metrics),
        }));
        let publisher = Arc::new(ReportPublisher::new(ReportPublisherDeps {
            events: deps.events,
            alerts: Arc::clone(&deps.governance.alerts),
            metrics: Arc::clone(&deps.infra.metrics),
        }));
        let lifecycle = Arc::new(ReportLifecycleService::new(ReportLifecycleDeps {
            report_repo,
            run_repo: Arc::clone(&run_repo),
            recommendation_repo,
            builder,
            publisher: Arc::clone(&publisher),
            feature_parity_gate: Arc::new(RepositoryFeatureParityGate::new(Arc::clone(
                &repos.feature_parity,
            )
                as Arc<dyn FeatureParityRepository>)),
            feature_parity_runs: Arc::clone(&feature_parity),
            artifact_store: Arc::clone(&deps.research.artifact_store),
            ad_hoc_queue_capacity: deps.workers.report_ad_hoc_queue_capacity,
            ad_hoc_queue_ttl_secs: deps.workers.report_ad_hoc_queue_ttl_secs,
        }));
        let fact_delivery = Arc::new(ReportFactDeliveryWorker::new(ReportFactDeliveryDeps {
            reports: Arc::clone(&repos.recommendation_report)
                as Arc<dyn RecommendationReportRepository>,
            artifacts: Arc::clone(&deps.research.artifact_store),
            clickhouse: Arc::clone(&deps.infra.ch),
            native_reads: Arc::new(ChNativeReadRepository::new(Arc::clone(&deps.infra.ch))),
            write_manager: Arc::clone(&deps.infra.ch_write_manager),
            publisher: Arc::clone(&publisher),
            metrics: Arc::clone(&deps.infra.metrics),
        }));
        let workers = deps.workers;
        let coordinator = Arc::new(ReportCoordinator::new(
            run_repo,
            runtime_config_repo,
            Arc::clone(&lifecycle),
            publisher,
            ReportCoordinatorConfig {
                poll_secs: workers.report_schedule_poll_secs,
                lease_secs: workers.report_run_lease_secs,
                heartbeat_secs: workers.report_run_heartbeat_secs,
                ad_hoc_ttl_secs: workers.report_ad_hoc_queue_ttl_secs,
            },
        ));
        let economic_feedback =
            Arc::new(EconomicFeedbackService::new(EconomicFeedbackServiceDeps {
                outcomes: Arc::clone(&repos.recommendation_economic_outcome)
                    as Arc<dyn RecommendationEconomicOutcomeRepository>,
                route_health: Arc::clone(&repos.route_economic_health)
                    as Arc<dyn RouteEconomicHealthRepository>,
                attribution_index: Arc::clone(&repos.attribution_artifact)
                    as Arc<dyn AttributionArtifactRepository>,
                artifacts: Arc::clone(&deps.research.artifact_store),
                recommendations: Arc::clone(&repos.recommendation)
                    as Arc<dyn RecommendationRepository>,
                reports: Arc::clone(&repos.recommendation_report)
                    as Arc<dyn RecommendationReportRepository>,
            }));
        Ok(Self {
            lifecycle,
            fact_delivery,
            coordinator,
            feature_parity,
            economic_feedback,
        })
    }
}
