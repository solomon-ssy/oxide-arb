//! Detection stack wiring (books, scanner, coalescer, calibration).

use super::types::{
    BuildClients, BuildInfra, COALESCER_TOKEN_CHANNEL_CAPACITY, DetectionStack,
    DetectionStackParts, SCANNER_MARKET_CHANNEL_CAPACITY, WiringConfig,
};
use crate::{
    app::periodic_services::run_calibration_startup_tick,
    bridge::{
        CoreOpportunityPipeline, calibration_source::CoreCalibrationDataSource,
        fee_estimator::CoreFeeEstimator,
    },
    detection::{
        coalescer::Coalescer,
        scanner::{Scanner, ScannerDeps},
    },
    infra::oracle_health_tracker::OracleHealthTracker,
    pipeline::{
        book_store::BookStore, market_cache::MarketCache, market_registry::MarketRegistry,
        staleness_classifier::StalenessClassifier, universe_filter::MarketUniverseFilter,
    },
    service::{
        detection_readiness::DetectionReadiness,
        gamma::{GammaService, GammaServiceDeps},
        ws_subscription::WsSubscriptionCoordinator,
    },
};
use oxide_arb_algorithm::{
    calibration::{CalibrationEntry, CalibrationUpdater, ResolutionCalibrator},
    cooldown::InMemoryEmissionCooldown,
    endgame::EndgameDetector,
    pipeline::OpportunityPipeline,
    scorer::EndgameScorer,
};
use oxide_arb_error::{OxideError, OxideResult};
use oxide_arb_models::{
    domain::{CoreEventPublisher, control_factor::ControlFactorProvider},
    runtime_config::{CalibrationConfig, RuntimeConfig},
};
use oxide_arb_repository::{postgres::PgCalibrationRepository, traits::CalibrationRepository};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::control::factor_snapshot::FactorSnapshotStore;

impl DetectionStack {
    pub(super) async fn wire(
        wiring: WiringConfig<'_>,
        infra: &BuildInfra,
        clients: &BuildClients,
        events: &CoreEventPublisher,
        shutdown: CancellationToken,
        detection_readiness: Arc<DetectionReadiness>,
    ) -> OxideResult<Self> {
        let deploy = wiring.deploy();
        let runtime = wiring.runtime();
        let book_store = Arc::new(BookStore::new(Arc::clone(infra.metrics())));
        let market_registry = Arc::new(MarketRegistry::new());
        let universe = Arc::new(MarketUniverseFilter::new(
            &runtime.market_data.enabled_categories,
        ));
        let market_cache = Arc::new(MarketCache::new(
            Arc::clone(&market_registry),
            Arc::clone(&universe),
        ));

        let (calibrator, calibration_updater) =
            Self::wire_calibration_stack(runtime, infra, clients).await?;

        let detector = EndgameDetector::new(
            &runtime.detection.endgame,
            &runtime.detection.calibration,
            Arc::clone(&calibrator),
            CoreFeeEstimator(Arc::clone(clients.fee_calculator())),
        );
        let scorer = EndgameScorer::new(
            &runtime.detection.endgame.scorer,
            &runtime.detection.endgame.fill_probability,
            runtime.detection.endgame.settlement_window_hours,
        );
        let cooldown = InMemoryEmissionCooldown::new(&runtime.detection.endgame.emission_cooldown);
        let factor_store: Arc<FactorSnapshotStore> = Arc::clone(infra.factor_store());
        let factor_provider: Arc<dyn ControlFactorProvider> = factor_store;
        let opportunity_pipeline: Arc<CoreOpportunityPipeline> =
            Arc::new(OpportunityPipeline::new(
                detector,
                scorer,
                cooldown,
                factor_provider,
                &runtime.detection,
            ));

        let staleness = StalenessClassifier::new(&runtime.market_data);
        let scanner = Arc::new(Scanner::new(ScannerDeps {
            pipeline: Arc::clone(&opportunity_pipeline),
            book_store: Arc::clone(&book_store),
            market_cache: Arc::clone(&market_cache),
            staleness_classifier: staleness.clone(),
            metrics: Arc::clone(infra.metrics()),
            detection_writer: Some(Arc::clone(&infra.persistence().detection_writer)),
            events: events.clone(),
            catalog: Arc::clone(infra.catalog()),
            detection_readiness,
        }));

        let (token_tx, token_rx) = flume::bounded(COALESCER_TOKEN_CHANNEL_CAPACITY);
        let (market_tx, market_rx) = flume::bounded(SCANNER_MARKET_CHANNEL_CAPACITY);

        let coalescer = Arc::new(Coalescer::new(
            Arc::clone(&market_registry),
            Duration::from_millis(runtime.execution.coalescer.coalesce_window_ms),
            market_tx,
            Arc::clone(infra.metrics()),
            shutdown,
        ));

        let ws_subscription = Arc::new(WsSubscriptionCoordinator::new(Arc::clone(
            clients.ws_manager(),
        )));
        let gamma_service = Arc::new(GammaService::new(GammaServiceDeps {
            gamma_client: Arc::clone(clients.gamma_client()),
            market_registry: Arc::clone(&market_registry),
            market_cache: Arc::clone(&market_cache),
            universe: Arc::clone(&universe),
            fee_calculator: Arc::clone(clients.fee_calculator()),
            market_repo: Arc::clone(infra.repos().market()),
            event_repo: Arc::clone(infra.repos().event()),
            cache: Arc::clone(infra.cache()),
            metrics: Arc::clone(infra.metrics()),
            ws_subscription: Some(Arc::clone(&ws_subscription)),
            full_sync_interval_secs: deploy.market_data.gamma.full_sync_interval_secs,
        }));

        Ok(Self::assembled(DetectionStackParts {
            book_store,
            market_registry,
            market_cache,
            universe,
            ws_subscription,
            gamma_service,
            opportunity_pipeline,
            calibrator,
            calibration_updater,
            scanner,
            coalescer,
            staleness,
            token_tx,
            token_rx,
            market_rx,
        }))
    }

    async fn wire_calibration_stack(
        runtime: &RuntimeConfig,
        infra: &BuildInfra,
        clients: &BuildClients,
    ) -> OxideResult<(Arc<ResolutionCalibrator>, Arc<CalibrationUpdater>)> {
        let calibrator = Self::load_resolution_calibrator(
            Arc::clone(infra.repos().calibration()),
            runtime.detection.calibration.clone(),
        )
        .await?;
        infra
            .metrics()
            .calibration_bucket_count
            .set(i64::try_from(calibrator.bucket_count()).unwrap_or(i64::MAX));
        let calibration_source = Arc::new(CoreCalibrationDataSource::new(
            Arc::clone(infra.repos().calibration()),
            Arc::clone(clients.gamma_client()),
            Arc::clone(clients.voting_oracle()),
            Arc::new(OracleHealthTracker::new()),
            Arc::clone(&infra.persistence().timeseries),
        ));
        let calibration_updater = Arc::new(CalibrationUpdater::new(
            Arc::clone(&calibrator),
            calibration_source,
            runtime.detection.calibration.clone(),
        ));
        run_calibration_startup_tick(
            calibration_updater.as_ref(),
            infra.metrics(),
            calibrator.as_ref(),
        )
        .await;
        Ok((calibrator, calibration_updater))
    }

    async fn load_resolution_calibrator(
        calibration_repo: Arc<PgCalibrationRepository>,
        config: CalibrationConfig,
    ) -> OxideResult<Arc<ResolutionCalibrator>> {
        let buckets = calibration_repo
            .get_all_buckets()
            .await
            .map_err(OxideError::from)?;
        let bucket_count = buckets.len();
        let entries: Vec<CalibrationEntry> =
            buckets.into_iter().map(CalibrationEntry::from).collect();
        let calibrator = Arc::new(if entries.is_empty() {
            ResolutionCalibrator::empty(config)
        } else {
            ResolutionCalibrator::from_entries(entries, config)
        });
        tracing::info!(bucket_count, "loaded calibration buckets from database");
        Ok(calibrator)
    }
}
