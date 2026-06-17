//! Final assembly: applicator, settlement, health checker, and `AppContext` projection.
//!
//! **Owns:** [`BuildInfra::wire_risk_and_trading`], [`BuildInfra::wire_applicator`],
//! [`BuildInfra::wire_settlement_bundle`], [`BuildInfra::finalize`], and
//! [`AppContextAssembly::assemble`].
//!
//! **Does not own:** pool connection, runtime-config seed, or mode restore — see
//! [`super::infra::BuildInfra::connect`].

use super::super::{
    AppContext, ControlFactorBundle, DataBundle, InfraBundle, RiskBundle, RuntimeChannels,
    SettlementBundle, TradingBundle, task_registry::PendingTaskQueue,
};
use super::types::{
    AppContextAssembly, BuildClients, BuildInfra, BuildInfraCore, BuildInfraCoreParts,
    BuildPersistence, BuildPersistenceParts, BuildRisk, BuildTrading, DetectionStack,
    HealthCheckerBundle, TradingBuildInput, TradingLifecycleWiring, WiringConfig,
};
use crate::{
    bridge::execution_mode::ExecutionModeHandle,
    infra::{
        health_alert_state::HealthAlertState,
        health_checker::{HealthChecker, HealthCheckerDeps},
        persistence_writers::PersistenceBackgroundWorkers,
    },
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
    service::runtime_lifecycle::LatestUnhealthySubsystems,
};
use oxide_arb_error::{OxideResult, config::ConfigError};
use oxide_arb_models::{
    domain::CoreEventPublisher,
    enums::common::ExecutionMode,
    runtime_config::{RuntimeConfig, validation::validate_runtime_for_mode},
};
use oxide_arb_repository::traits::PositionRepository;
use parking_lot::Mutex;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;

use crate::execution::settlement::{dedup::SettlementDedup, service::MarketSettlementService};

/// Inputs for [`BuildInfra::wire_applicator`].
pub(super) struct ApplicatorWiring<'a> {
    runtime_store: Arc<RuntimeConfigStore>,
    execution_mode: ExecutionModeHandle,
    clients: &'a BuildClients,
    risk: &'a BuildRisk,
    trading: &'a BuildTrading,
    settlement_service: Arc<MarketSettlementService>,
    settlement_dedup: Arc<SettlementDedup>,
}

impl<'a> ApplicatorWiring<'a> {
    pub(super) const fn new(
        runtime_store: Arc<RuntimeConfigStore>,
        execution_mode: ExecutionModeHandle,
        clients: &'a BuildClients,
        risk: &'a BuildRisk,
        trading: &'a BuildTrading,
        settlement_service: Arc<MarketSettlementService>,
        settlement_dedup: Arc<SettlementDedup>,
    ) -> Self {
        Self {
            runtime_store,
            execution_mode,
            clients,
            risk,
            trading,
            settlement_service,
            settlement_dedup,
        }
    }
}

impl WiringConfig<'_> {
    /// Fail-closed gate: the persisted operational mode must be valid for the active runtime config.
    pub(super) fn ensure_valid_for_mode(self, mode: ExecutionMode) -> OxideResult<()> {
        let report = validate_runtime_for_mode(self.runtime(), mode);
        for w in &report.warnings {
            tracing::warn!(mode = ?mode, "Runtime config warning: {w}");
        }
        if report.has_errors() {
            return Err(ConfigError::from(report).into());
        }
        Ok(())
    }
}

impl BuildInfra {
    /// Wire detection, risk, and execution stacks in dependency order.
    pub(super) async fn wire_risk_and_trading(
        &self,
        wiring: WiringConfig<'_>,
        execution_mode: &ExecutionModeHandle,
        clients: &BuildClients,
        events: &CoreEventPublisher,
        shutdown: CancellationToken,
        lifecycle: &TradingLifecycleWiring,
    ) -> OxideResult<(BuildRisk, BuildTrading)> {
        let detection = DetectionStack::wire(
            wiring,
            self,
            clients,
            events,
            shutdown.clone(),
            Arc::clone(lifecycle.detection_readiness()),
        )
        .await?;
        let risk =
            BuildRisk::wire(wiring, execution_mode, self, clients, events, &detection).await?;
        let trading = BuildTrading::wire(
            &TradingBuildInput::new(wiring, execution_mode, self, clients, lifecycle),
            &risk,
            detection,
            shutdown,
        );
        Ok((risk, trading))
    }

    /// Assemble the activation applicator over every live subscriber handle.
    pub(super) fn wire_applicator(
        &self,
        wiring: ApplicatorWiring<'_>,
    ) -> Arc<RuntimeConfigApplicator> {
        let detection = wiring.trading.detection();
        let execution = wiring.trading.execution();
        let position_repo = Arc::clone(self.repos().position()) as Arc<dyn PositionRepository>;
        Arc::new(RuntimeConfigApplicator::new(
            wiring.runtime_store,
            wiring.execution_mode,
            position_repo,
            RuntimeConfigSubscribers {
                risk_engine: Arc::clone(wiring.risk.engine()),
                metrics_state: Arc::clone(wiring.risk.metrics_state()),
                exposure: Arc::clone(wiring.risk.exposure()),
                capital: Arc::clone(&execution.execution().capital_manager),
                opportunity_pipeline: Arc::clone(detection.opportunity_pipeline()),
                calibration_updater: Arc::clone(detection.calibration_updater()),
                staleness: detection.staleness().clone(),
                universe: Arc::clone(detection.universe()),
                market_registry: Arc::clone(detection.market_registry()),
                market_cache: Arc::clone(detection.market_cache()),
                ws_subscription: Some(Arc::clone(detection.ws_subscription())),
                validator: Arc::clone(execution.validator()),
                order_strategy: Arc::clone(execution.order_strategy()),
                coalescer: Arc::clone(detection.coalescer()),
                funnel: Arc::clone(execution.funnel()),
                settlement_service: wiring.settlement_service,
                settlement_dedup: wiring.settlement_dedup,
                voting_oracle: Arc::clone(wiring.clients.voting_oracle()),
                ctf_redeem: wiring.clients.ctf_redeem().map(Arc::clone),
                alerts: Arc::clone(self.alerts()),
            },
        ))
    }

    pub(super) fn wire_settlement_bundle(
        &self,
        runtime: &RuntimeConfig,
        clients: &BuildClients,
        risk: &BuildRisk,
        trading: &BuildTrading,
        events: &CoreEventPublisher,
    ) -> (Arc<MarketSettlementService>, Arc<SettlementDedup>) {
        let detection = trading.detection();
        let trade_repo = Arc::clone(&self.persistence().trade_repo);
        let audit_writer = Arc::clone(&self.persistence().audit_writer);
        let settlement_service = Arc::new(MarketSettlementService::new(
            crate::execution::settlement::service::MarketSettlementServiceDeps {
                position_repo: Arc::clone(self.repos().position()),
                resolution_event_repo: Arc::clone(self.repos().resolution_event()),
                trade_repo,
                risk_engine: Arc::clone(risk.engine()),
                risk_metrics: Arc::clone(risk.metrics()),
                fsm: Arc::clone(risk.fsm()),
                ctf_redeem: clients.ctf_redeem().map(Arc::clone),
                market_registry: Arc::clone(detection.market_registry()),
                voting_oracle: Arc::clone(clients.voting_oracle()),
                metrics: Arc::clone(self.metrics()),
                alerts: Arc::clone(self.alerts()),
                audit_writer,
                metrics_refresh: Arc::clone(risk.metrics_refresh()),
                events: events.clone(),
                config: runtime.settlement.clone(),
            },
        ));
        let settlement_dedup = Arc::new(SettlementDedup::new(Duration::from_secs(
            runtime.settlement.lifecycle.dedup_window_secs,
        )));
        (settlement_service, settlement_dedup)
    }

    /// Strip bootstrap-only fields and queue persistence background workers.
    pub(super) fn finalize(
        self,
        persistence_workers: PersistenceBackgroundWorkers,
    ) -> (BuildInfraCore, BuildPersistence, PendingTaskQueue) {
        let (
            pg_pool,
            ch_pool,
            redis_pool,
            cache,
            jwt_blacklist,
            catalog,
            metrics,
            alerts,
            risk_decision_audit,
            risk_decision_audit_rx,
            repos,
            balance_fact_writer,
            factor_store,
            factor_refresher,
            factor_registry,
            shadow_writer_task,
            persistence,
        ) = self.into_post_bootstrap();

        let persistence_handles = BuildPersistence::assembled(BuildPersistenceParts {
            trade_repo: Arc::clone(&persistence.trade_repo),
            timeseries: Arc::clone(&persistence.timeseries),
            audit_writer: Arc::clone(&persistence.audit_writer),
            book_fact_writer: Arc::clone(&persistence.book_fact_writer),
        });
        let mut pending_tasks = PendingTaskQueue::default();
        persistence.queue_background_tasks(persistence_workers, &mut pending_tasks);

        let core = BuildInfraCore::assembled(BuildInfraCoreParts {
            pg_pool,
            ch_pool,
            redis_pool,
            cache,
            jwt_blacklist,
            catalog,
            metrics,
            alerts,
            risk_decision_audit,
            risk_decision_audit_rx,
            repos,
            balance_fact_writer,
            factor_store,
            factor_refresher,
            factor_registry,
            shadow_writer_task,
        });
        (core, persistence_handles, pending_tasks)
    }
}

impl BuildInfraCore {
    fn health_checker_bundle(
        &self,
        clients: &BuildClients,
        risk: &BuildRisk,
        execution_mode: &ExecutionModeHandle,
    ) -> HealthCheckerBundle {
        let unhealthy_subsystems = Arc::new(LatestUnhealthySubsystems::default());
        let health_alert_state = Arc::new(HealthAlertState::default());
        let checker = Arc::new(HealthChecker::new(HealthCheckerDeps {
            pg_pool: Arc::clone(self.pg_pool()),
            ch_pool: Arc::clone(self.ch_pool()),
            ws_manager: Arc::clone(clients.ws_manager()),
            catalog: Arc::clone(self.catalog()),
            risk_engine: Arc::clone(risk.engine()),
            metrics: Arc::clone(risk.metrics()),
            factor_store: Arc::clone(self.factor_store()),
            clob_client: clients.clob_client().map(Arc::clone),
            mode: execution_mode.clone(),
            unhealthy_subsystems: Arc::clone(&unhealthy_subsystems),
            alert_state: health_alert_state,
        }));
        HealthCheckerBundle::assembled(checker, unhealthy_subsystems)
    }

    fn pack_with_control(
        self,
        clients: &BuildClients,
        persistence: &BuildPersistence,
    ) -> (InfraBundle, ControlFactorBundle) {
        let (
            pg_pool,
            ch_pool,
            redis_pool,
            cache,
            jwt_blacklist,
            metrics,
            alerts,
            risk_decision_audit,
            risk_decision_audit_rx,
            repos,
            balance_fact_writer,
            factor_store,
            factor_refresher,
            factor_registry,
            shadow_writer_task,
        ) = self.into_pack_parts();

        let control = ControlFactorBundle {
            store: factor_store,
            refresher: factor_refresher,
            registry: factor_registry,
            shadow_writer_task,
        };
        let bundle = InfraBundle {
            pg: pg_pool,
            ch: ch_pool,
            redis: redis_pool,
            cache,
            jwt_blacklist,
            metrics,
            alerts,
            risk_decision_audit,
            risk_decision_audit_rx,
            trade_repo: Arc::clone(persistence.trade_repo()),
            position_repo: Arc::clone(repos.position()),
            report_repo: Arc::clone(repos.report()),
            fact_data_repo: Arc::clone(repos.fact_data()),
            calibration_repo: Arc::clone(repos.calibration()),
            risk_state_repo: Arc::clone(repos.risk_state()),
            timeseries: Arc::clone(persistence.timeseries()),
            audit_writer: Arc::clone(persistence.audit_writer()),
            balance_fact_writer,
            book_fact_writer: Arc::clone(persistence.book_fact_writer()),
            holder_address: clients.holder_address().to_owned(),
            fee_calculator: Arc::clone(clients.fee_calculator()),
        };
        (bundle, control)
    }
}

impl AppContextAssembly {
    pub(super) fn assemble(self) -> AppContext {
        let (
            config,
            runtime_store,
            applicator,
            execution_mode,
            events,
            event_rx,
            infra,
            clients,
            risk,
            trading,
            trade_integrity,
            persistence,
            settlement_service,
            settlement_dedup,
            shutdown,
            pending_tasks,
            lifecycle,
        ) = self.into_parts();

        let health = infra.health_checker_bundle(&clients, &risk, &execution_mode);
        let (detection, execution) = trading.into_parts();
        let (
            book_store,
            market_registry,
            market_cache,
            gamma_service,
            token_rx,
            market_rx,
            opportunity_pipeline,
            calibrator,
            calibration_updater,
            scanner,
            coalescer,
        ) = detection.into_app_data();
        let (funnel, data_pipeline, execution_bundle, settlement_rx, execution_runners) =
            execution.into_app_execution();
        let (engine, metrics, metrics_state, exposure, potential_loss_store, metrics_refresh, fsm) =
            risk.into_risk_bundle();
        let (system_status_nudge, detection_readiness) = lifecycle.into_lifecycle_handles();
        let catalog = Arc::clone(infra.catalog());
        let (infra_bundle, control) = infra.pack_with_control(&clients, &persistence);
        let (clob_client, ctf_redeem, ws_manager) = clients.into_trading_clients();
        AppContext {
            config,
            runtime_config: runtime_store,
            applicator,
            execution_mode,
            events,
            event_rx: Mutex::new(Some(event_rx)),
            trade_integrity,
            infra: infra_bundle,
            data: DataBundle {
                book_store,
                market_registry,
                market_cache,
                data_pipeline,
                gamma_service,
                catalog,
            },
            risk: RiskBundle {
                engine,
                metrics,
                metrics_state,
                exposure,
                potential_loss_store,
                metrics_refresh,
            },
            trading: TradingBundle {
                opportunity_pipeline,
                calibrator,
                calibration_updater,
                scanner,
                coalescer,
                funnel,
                fsm,
                execution: Some(execution_bundle),
                clob_client,
                ctf_redeem,
                ws_manager,
            },
            control,
            settlement: SettlementBundle {
                service: settlement_service,
                dedup: settlement_dedup,
                settlement_rx: Mutex::new(Some(settlement_rx)),
            },
            runtime: RuntimeChannels {
                coalescer_token_rx: Mutex::new(Some(token_rx)),
                scanner_market_rx: Mutex::new(Some(market_rx)),
                execution_runners: Mutex::new(Some(execution_runners)),
            },
            shutdown,
            pending_tasks,
            started_at: Instant::now(),
            system_status_nudge,
            detection_readiness,
            health_checker: Arc::clone(health.checker()),
            unhealthy_subsystems: Arc::clone(health.unhealthy_subsystems()),
        }
    }
}
