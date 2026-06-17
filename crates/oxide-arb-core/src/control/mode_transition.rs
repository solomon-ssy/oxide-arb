//! Core implementation of the web-facing [`RuntimeControlPort`].
//!
//! This module owns the money-critical execution-mode hot-swap protocol and the
//! halt/resume, circuit-breaker reset, and blacklist control surface. Every
//! operation drives the live engine (never a stale persisted copy) and fails
//! closed.
//!
//! # Execution-mode transition protocol
//!
//! Switching `DryRun`/`Paper`/`Live` at runtime is dangerous because a flip
//! mid-flight could submit a real order for a simulated intent (or vice versa).
//! The protocol is therefore strictly ordered and never partially commits:
//!
//! 1. **Preflight** — validate settings for the target mode; entering `Live`
//!    additionally requires a connected CLOB client and a metrics refresher.
//! 2. **Quiesce** — halt trading, then wait until the risk engine blocks new
//!    trades *and* all capital reservations have drained (no execution holds
//!    money in flight). A timeout aborts without committing.
//! 3. **Commit** — a single atomic [`ExecutionModeHandle::store`]; every hot-path
//!    reader observes the new mode on its next access.
//! 4. **Activate** — entering `Live` refreshes CLOB metrics and verifies they are
//!    authoritative; entering `DryRun`/`Paper` recomputes the derived simulated
//!    ledger snapshot from Postgres and verifies the simulated source. The
//!    heartbeat probe and fail-closed factor TTLs self-gate on the live mode, so
//!    no task respawn is needed.
//! 5. **Resume** — only after activation succeeds.
//!
//! On any post-commit activation failure the system stays halted (trades blocked
//! by the kill switch and `MetricsFreshnessCheck`), so the failure mode is safe.

use crate::{
    bridge::{
        execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics,
        trading_gate::resume_trading,
    },
    control::factor_snapshot::FactorSnapshotStore,
    control::status::{build_system_balance, build_system_status},
    execution::{
        capital_manager::CapitalManager,
        fsm::{EmergencyAckError, EmergencyClass, ExecutionFSM},
        settlement::redeem_preflight::ensure_live_pending_redeem_portfolio,
        venue_guard::halt_trading_and_cancel_open_orders,
    },
    exposure::in_memory::InMemoryExposureReservation,
    infra::health_checker::HealthChecker,
    observability::alert_dispatcher::AlertDispatcher,
    pipeline::market_registry::MarketRegistry,
    post_trade::reconciliation::CloseUnresolvableService,
    runtime_config::RuntimeConfigStore,
    service::{
        catalog_readiness::CatalogReadiness,
        detection_readiness::DetectionReadiness,
        risk_metrics::{RiskMetricsRefreshService, RiskMetricsSource, RiskMetricsState},
        runtime_lifecycle::LatestUnhealthySubsystems,
        system_status_publisher::SharedSystemStatusPublisher,
    },
    trade_integrity::TradeIntegrityStore,
};
use async_trait::async_trait;
use oxide_arb_api::{clob::ClobClient, ctf::client::CtfRedeemClient, ws::ClobWsManager};
use oxide_arb_models::{
    config::DeployConfig,
    domain::{
        BlacklistInfo, HealthReport, ModeTransitionReport, RiskEngineState, RuntimeControlError,
        RuntimeControlPort, SubsystemHealth, SystemBalanceView, SystemStatus,
    },
    enums::{common::ExecutionMode, risk::BlacklistReason},
    runtime_config::validation::validate_runtime_for_mode,
    types::{MarketId, TradeId},
};
use oxide_arb_repository::traits::{
    PositionRepository, SystemRuntimeStateRepository, TradeRepository,
};
use oxide_arb_risk::{engine::RiskEngine, traits::RiskMetrics};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Maximum time to wait for the trading loop to quiesce before aborting a mode
/// transition (fail-closed: stays halted on timeout).
#[cfg(not(test))]
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);
#[cfg(test)]
const QUIESCE_TIMEOUT: Duration = Duration::from_millis(100);
/// Poll cadence while waiting for capital reservations to drain.
const QUIESCE_POLL: Duration = Duration::from_millis(100);

/// Construction dependencies for [`CoreRuntimeControl`].
#[derive(Clone)]
pub struct CoreRuntimeControlDeps {
    pub execution_mode: ExecutionModeHandle,
    pub risk_engine: Arc<RiskEngine>,
    /// Catalog warmup gate surfaced through `GET /system`.
    pub catalog: Arc<CatalogReadiness>,
    pub fsm: Arc<ExecutionFSM>,
    pub exposure: Arc<InMemoryExposureReservation>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub metrics_refresh: Arc<RiskMetricsRefreshService>,
    pub clob_client: Option<Arc<ClobClient>>,
    pub ctf_redeem: Option<Arc<CtfRedeemClient>>,
    pub holder_address: String,
    pub market_registry: Arc<MarketRegistry>,
    pub ws_manager: Arc<ClobWsManager>,
    pub unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
    pub health_checker: Option<Arc<HealthChecker>>,
    pub deploy: Arc<DeployConfig>,
    /// Live runtime config: mode preflight reads the active snapshot, never a
    /// startup copy.
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Open positions for pending-redeem portfolio preflight when entering Live.
    pub position_repo: Arc<dyn PositionRepository>,
    /// Persists the active mode so a transition survives a restart.
    pub system_runtime_state: Arc<dyn SystemRuntimeStateRepository>,
    pub trade_repo: Arc<dyn TradeRepository>,
    pub capital_manager: Arc<CapitalManager>,
    pub trade_integrity: Arc<TradeIntegrityStore>,
    pub factor_store: Arc<FactorSnapshotStore>,
    pub alerts: Arc<AlertDispatcher>,
    /// Hot-path detection gate mirrored from operational phase on status publish.
    pub detection_readiness: Arc<DetectionReadiness>,
    /// Optional publisher for immediate WebSocket status pushes after control ops.
    pub status_publisher: Option<SharedSystemStatusPublisher>,
}

/// Live runtime control surface backing the web `/system` and `/risk` routes.
pub struct CoreRuntimeControl {
    execution_mode: ExecutionModeHandle,
    risk_engine: Arc<RiskEngine>,
    catalog: Arc<CatalogReadiness>,
    fsm: Arc<ExecutionFSM>,
    exposure: Arc<InMemoryExposureReservation>,
    metrics: Arc<CoreRiskMetrics>,
    metrics_state: Arc<RiskMetricsState>,
    metrics_refresh: Arc<RiskMetricsRefreshService>,
    clob_client: Option<Arc<ClobClient>>,
    ctf_redeem: Option<Arc<CtfRedeemClient>>,
    holder_address: String,
    market_registry: Arc<MarketRegistry>,
    ws_manager: Arc<ClobWsManager>,
    unhealthy_subsystems: Arc<LatestUnhealthySubsystems>,
    health_checker: Option<Arc<HealthChecker>>,
    deploy: Arc<DeployConfig>,
    runtime_config: Arc<RuntimeConfigStore>,
    position_repo: Arc<dyn PositionRepository>,
    system_runtime_state: Arc<dyn SystemRuntimeStateRepository>,
    trade_repo: Arc<dyn TradeRepository>,
    capital_manager: Arc<CapitalManager>,
    trade_integrity: Arc<TradeIntegrityStore>,
    factor_store: Arc<FactorSnapshotStore>,
    alerts: Arc<AlertDispatcher>,
    detection_readiness: Arc<DetectionReadiness>,
    close_unresolvable: CloseUnresolvableService,
    status_publisher: Option<SharedSystemStatusPublisher>,
    started_at: Instant,
}

impl CoreRuntimeControl {
    #[must_use]
    pub fn new(deps: CoreRuntimeControlDeps, started_at: Instant) -> Self {
        let close_unresolvable = CloseUnresolvableService::new(
            Arc::clone(&deps.trade_repo),
            Arc::clone(&deps.capital_manager),
            Arc::clone(&deps.fsm),
            Arc::clone(&deps.alerts),
            Arc::clone(&deps.metrics_state),
        );
        Self {
            execution_mode: deps.execution_mode,
            risk_engine: deps.risk_engine,
            catalog: deps.catalog,
            fsm: deps.fsm,
            exposure: deps.exposure,
            metrics: deps.metrics,
            metrics_state: deps.metrics_state,
            metrics_refresh: deps.metrics_refresh,
            clob_client: deps.clob_client,
            ctf_redeem: deps.ctf_redeem,
            holder_address: deps.holder_address,
            market_registry: deps.market_registry,
            ws_manager: deps.ws_manager,
            unhealthy_subsystems: deps.unhealthy_subsystems,
            health_checker: deps.health_checker,
            deploy: deps.deploy,
            runtime_config: deps.runtime_config,
            position_repo: deps.position_repo,
            system_runtime_state: deps.system_runtime_state,
            trade_repo: deps.trade_repo,
            capital_manager: deps.capital_manager,
            trade_integrity: deps.trade_integrity,
            factor_store: deps.factor_store,
            alerts: deps.alerts,
            detection_readiness: deps.detection_readiness,
            close_unresolvable,
            status_publisher: deps.status_publisher,
            started_at,
        }
    }

    fn control_deps(&self) -> CoreRuntimeControlDeps {
        CoreRuntimeControlDeps {
            execution_mode: self.execution_mode.clone(),
            risk_engine: Arc::clone(&self.risk_engine),
            catalog: Arc::clone(&self.catalog),
            fsm: Arc::clone(&self.fsm),
            exposure: Arc::clone(&self.exposure),
            metrics: Arc::clone(&self.metrics),
            metrics_state: Arc::clone(&self.metrics_state),
            metrics_refresh: Arc::clone(&self.metrics_refresh),
            clob_client: self.clob_client.clone(),
            ctf_redeem: self.ctf_redeem.clone(),
            holder_address: self.holder_address.clone(),
            market_registry: Arc::clone(&self.market_registry),
            ws_manager: Arc::clone(&self.ws_manager),
            unhealthy_subsystems: Arc::clone(&self.unhealthy_subsystems),
            health_checker: self.health_checker.clone(),
            deploy: Arc::clone(&self.deploy),
            runtime_config: Arc::clone(&self.runtime_config),
            position_repo: Arc::clone(&self.position_repo),
            system_runtime_state: Arc::clone(&self.system_runtime_state),
            trade_repo: Arc::clone(&self.trade_repo),
            capital_manager: Arc::clone(&self.capital_manager),
            trade_integrity: Arc::clone(&self.trade_integrity),
            factor_store: Arc::clone(&self.factor_store),
            alerts: Arc::clone(&self.alerts),
            detection_readiness: Arc::clone(&self.detection_readiness),
            status_publisher: self.status_publisher.clone(),
        }
    }

    fn publish_status_now(&self) {
        if let Some(publisher) = &self.status_publisher {
            publisher.publish_now();
        }
    }

    fn risk_metrics(&self) -> &dyn RiskMetrics {
        self.metrics.as_ref()
    }

    /// Validate that the target mode can be entered before any state change:
    /// deploy credential/JWT policy **and** the active runtime config must
    /// both be valid for the target mode (fail-closed).
    fn preflight(&self, target: ExecutionMode) -> Result<(), RuntimeControlError> {
        self.deploy
            .ensure_valid_for_mode(target)
            .map_err(|error| RuntimeControlError::Precondition(error.to_string()))?;
        let runtime_report = validate_runtime_for_mode(&self.runtime_config.current(), target);
        if runtime_report.has_errors() {
            return Err(RuntimeControlError::Precondition(
                runtime_report.to_string(),
            ));
        }
        if target == ExecutionMode::Live {
            if self.clob_client.is_none() {
                return Err(RuntimeControlError::Precondition(
                    "entering Live requires a CLOB client, but none was loaded at boot".to_owned(),
                ));
            }
            if self.ctf_redeem.is_none() {
                return Err(RuntimeControlError::Precondition(
                    "entering Live requires a CTF redeem client, but none was loaded at boot"
                        .to_owned(),
                ));
            }
            if self.holder_address == "unavailable" {
                return Err(RuntimeControlError::Precondition(
                    "entering Live requires a configured holder address".to_owned(),
                ));
            }
            let status = build_system_status(&self.control_deps(), self.started_at);
            if !status.operational_phase.allows_live_trading() {
                return Err(RuntimeControlError::Precondition(format!(
                    "cannot enter Live while operational_phase is {:?}; market_data_ready={}",
                    status.operational_phase, status.market_data.ready
                )));
            }
        }
        Ok(())
    }

    /// Wait until trading is blocked and all capital reservations have drained.
    ///
    /// Every execution reserves capital before submitting and releases it on the
    /// outcome, so `active reservations == 0` after a halt means no order is in
    /// flight. Times out fail-closed (system stays halted).
    async fn quiesce(&self) -> Result<(), RuntimeControlError> {
        let deadline = Instant::now() + QUIESCE_TIMEOUT;
        loop {
            let reservations = self.exposure.active_count_sync();
            if !self.risk_engine.allows_trading() && reservations == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(RuntimeControlError::QuiesceTimeout {
                    waited_secs: QUIESCE_TIMEOUT.as_secs(),
                    detail: format!("active_reservations={reservations}"),
                });
            }
            tokio::time::sleep(QUIESCE_POLL).await;
        }
    }

    /// Mode-specific activation after the atomic commit.
    ///
    /// The mode-aware refresher reads the freshly committed mode, so a single
    /// `refresh()` hydrates the correct snapshot: CLOB-authoritative for
    /// `Live`, the Postgres-derived simulated ledger for `DryRun`/`Paper`.
    /// The source assertion afterwards is the fail-closed guard against a
    /// refresh racing a concurrent transition.
    async fn activate(&self, target: ExecutionMode) -> Result<(), RuntimeControlError> {
        self.metrics_refresh
            .refresh()
            .await
            .map_err(|error| RuntimeControlError::Activation(error.to_string()))?;
        let expected = match target {
            ExecutionMode::Live => RiskMetricsSource::AuthoritativeClob,
            ExecutionMode::DryRun => RiskMetricsSource::SimulatedDryRun,
            ExecutionMode::Paper => RiskMetricsSource::SimulatedPaper,
        };
        if self.metrics_state.source() != expected {
            return Err(RuntimeControlError::Activation(format!(
                "metrics source mismatch after refresh for {target}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl RuntimeControlPort for CoreRuntimeControl {
    fn execution_mode(&self) -> ExecutionMode {
        self.execution_mode.current()
    }

    async fn switch_execution_mode(
        &self,
        target: ExecutionMode,
        operator_ack: &str,
    ) -> Result<ModeTransitionReport, RuntimeControlError> {
        let from = self.execution_mode.current();
        if from == target {
            return Ok(ModeTransitionReport { from, to: target });
        }

        // 1. Preflight (no state change yet).
        self.preflight(target)?;
        if target == ExecutionMode::Live {
            let policy = self.runtime_config.current().settlement.redeem.clone();
            ensure_live_pending_redeem_portfolio(
                self.position_repo.as_ref(),
                &self.market_registry,
                &policy,
                target,
            )
            .await?;
        }

        // 2. Quiesce: halt then wait for the loop to drain.
        halt_trading_and_cancel_open_orders(
            self.execution_mode.current(),
            self.clob_client.as_deref(),
            &self.risk_engine,
            &self.fsm,
            format!("execution mode transition {from} -> {target}"),
            EmergencyClass::VenueFault,
        )
        .await;
        self.quiesce().await?;

        // 3. Atomic commit — single store observed by every hot-path reader,
        //    then durably persist so the transition survives a restart.
        self.execution_mode.store(target);
        self.system_runtime_state
            .upsert_execution_mode(target, operator_ack, "runtime mode transition")
            .await
            .map_err(|error| {
                RuntimeControlError::Activation(format!("persist execution mode: {error}"))
            })?;

        // 4. Activate; on failure stay halted (fail-closed) and surface the error.
        self.activate(target).await?;

        // 5. Resume only after a fully successful activation.
        resume_trading(
            &self.risk_engine,
            &self.fsm,
            &self.trade_integrity,
            operator_ack,
        )
        .await?;

        tracing::warn!(%from, %target, "execution mode transition committed");
        self.publish_status_now();
        Ok(ModeTransitionReport { from, to: target })
    }

    async fn halt(&self, reason: String) {
        halt_trading_and_cancel_open_orders(
            self.execution_mode.current(),
            self.clob_client.as_deref(),
            &self.risk_engine,
            &self.fsm,
            reason,
            EmergencyClass::VenueFault,
        )
        .await;
        self.publish_status_now();
    }

    async fn resume(&self, operator_ack: &str) -> Result<(), RuntimeControlError> {
        resume_trading(
            &self.risk_engine,
            &self.fsm,
            &self.trade_integrity,
            operator_ack,
        )
        .await?;
        self.publish_status_now();
        Ok(())
    }

    async fn ack_execution_emergency(&self, operator_ack: &str) -> Result<(), RuntimeControlError> {
        let class = self
            .fsm
            .ack_operator_emergency(&self.trade_integrity, &self.risk_engine)
            .await?;
        tracing::info!(?class, operator_ack, "execution emergency acknowledged");
        self.publish_status_now();
        Ok(())
    }

    async fn reset_circuit_breaker(&self, reason: &str) -> Result<(), RuntimeControlError> {
        self.risk_engine
            .reset_circuit_breaker(reason, self.risk_metrics())
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))?;
        self.publish_status_now();
        Ok(())
    }

    fn risk_snapshot(&self) -> RiskEngineState {
        self.risk_engine.snapshot(self.risk_metrics())
    }

    fn open_position_count(&self) -> u32 {
        u32::try_from(self.metrics.open_position_count()).unwrap_or(u32::MAX)
    }

    fn blacklist(&self) -> Vec<BlacklistInfo> {
        self.risk_engine.blacklist().active_entries()
    }

    async fn add_blacklist(
        &self,
        market_id: MarketId,
        blacklist_reason: BlacklistReason,
        operator_reason: &str,
    ) -> Result<(), RuntimeControlError> {
        self.risk_engine
            .add_blacklist(
                market_id,
                blacklist_reason,
                operator_reason,
                self.risk_metrics(),
            )
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))
    }

    async fn remove_blacklist(
        &self,
        market_id: &MarketId,
        reason: &str,
    ) -> Result<(), RuntimeControlError> {
        self.risk_engine
            .remove_blacklist(market_id, reason, self.risk_metrics())
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))
    }

    async fn system_status(&self) -> SystemStatus {
        build_system_status(&self.control_deps(), self.started_at)
    }

    async fn system_balance(&self) -> SystemBalanceView {
        build_system_balance(&self.control_deps())
    }

    async fn health(&self) -> HealthReport {
        match &self.health_checker {
            Some(checker) => checker.check_all().await,
            None => HealthReport {
                overall_healthy: false,
                checks: vec![SubsystemHealth::unhealthy(
                    "health_checker",
                    None,
                    "health checker unavailable",
                )],
                checked_at: chrono::Utc::now(),
            },
        }
    }

    async fn close_unresolvable_trade(
        &self,
        trade_id: &TradeId,
        note: &str,
        operator: &str,
    ) -> Result<bool, RuntimeControlError> {
        self.close_unresolvable
            .close(trade_id, note, operator)
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))
    }
}

impl From<EmergencyAckError> for RuntimeControlError {
    fn from(error: EmergencyAckError) -> Self {
        match error {
            EmergencyAckError::Gate(gate) => Self::Engine(gate.to_string()),
            EmergencyAckError::BlockingTrades { count } => Self::BlockingTrades { count },
            other => Self::Precondition(other.to_string()),
        }
    }
}
