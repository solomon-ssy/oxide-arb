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
//!    authoritative; leaving `Live` re-seeds the simulated metrics snapshot. The
//!    heartbeat probe and fail-closed factor TTLs self-gate on the live mode, so
//!    no task respawn is needed.
//! 5. **Resume** — only after activation succeeds.
//!
//! On any post-commit activation failure the system stays halted (trades blocked
//! by the kill switch and `MetricsFreshnessCheck`), so the failure mode is safe.

use crate::{
    bridge::{
        execution_mode::ExecutionModeHandle,
        risk_metrics::CoreRiskMetrics,
        trading_gate::{halt_trading, resume_trading},
    },
    execution::fsm::ExecutionFSM,
    exposure::in_memory::InMemoryExposureReservation,
    infra::health_checker::HealthChecker,
    pipeline::market_registry::MarketRegistry,
    runtime_config::RuntimeConfigStore,
    service::{
        catalog_readiness::CatalogReadiness,
        risk_metrics::{RiskMetricsRefreshService, RiskMetricsSource, RiskMetricsState},
    },
};
use async_trait::async_trait;
use chrono::Utc;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_models::{
    config::DeployConfig,
    domain::{
        BlacklistInfo, CatalogStatusPort, HealthReport, ModeTransitionReport, RiskEngineState,
        RuntimeControlError, RuntimeControlPort, SystemStatus,
    },
    enums::{common::ExecutionMode, risk::BlacklistReason},
    runtime_config::validation::validate_runtime_for_mode,
    types::{MarketId, Usd},
};
use oxide_arb_repository::traits::SystemRuntimeStateRepository;
use oxide_arb_risk::{engine::RiskEngine, traits::RiskMetrics};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Maximum time to wait for the trading loop to quiesce before aborting a mode
/// transition (fail-closed: stays halted on timeout).
const QUIESCE_TIMEOUT: Duration = Duration::from_secs(30);
/// Poll cadence while waiting for capital reservations to drain.
const QUIESCE_POLL: Duration = Duration::from_millis(100);

/// Construction dependencies for [`CoreRuntimeControl`].
pub struct CoreRuntimeControlDeps {
    pub execution_mode: ExecutionModeHandle,
    pub risk_engine: Arc<RiskEngine>,
    /// Catalog warmup gate surfaced through `GET /system`.
    pub catalog: Arc<CatalogReadiness>,
    pub fsm: Arc<ExecutionFSM>,
    pub exposure: Arc<InMemoryExposureReservation>,
    pub metrics: Arc<CoreRiskMetrics>,
    pub metrics_state: Arc<RiskMetricsState>,
    pub metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    pub clob_client: Option<Arc<ClobClient>>,
    pub market_registry: Arc<MarketRegistry>,
    pub health_checker: Arc<HealthChecker>,
    pub deploy: Arc<DeployConfig>,
    /// Live runtime config: mode preflight + simulated bankroll reseeding read
    /// the active snapshot, never a startup copy.
    pub runtime_config: Arc<RuntimeConfigStore>,
    /// Persists the active mode so a transition survives a restart.
    pub system_runtime_state: Arc<dyn SystemRuntimeStateRepository>,
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
    metrics_refresh: Option<Arc<RiskMetricsRefreshService>>,
    clob_client: Option<Arc<ClobClient>>,
    market_registry: Arc<MarketRegistry>,
    health_checker: Arc<HealthChecker>,
    deploy: Arc<DeployConfig>,
    runtime_config: Arc<RuntimeConfigStore>,
    system_runtime_state: Arc<dyn SystemRuntimeStateRepository>,
    started_at: Instant,
}

impl CoreRuntimeControl {
    #[must_use]
    pub fn new(deps: CoreRuntimeControlDeps) -> Self {
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
            market_registry: deps.market_registry,
            health_checker: deps.health_checker,
            deploy: deps.deploy,
            runtime_config: deps.runtime_config,
            system_runtime_state: deps.system_runtime_state,
            started_at: Instant::now(),
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
            if self.metrics_refresh.is_none() {
                return Err(RuntimeControlError::Precondition(
                    "entering Live requires a CLOB metrics refresher".to_owned(),
                ));
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
    async fn activate(&self, target: ExecutionMode) -> Result<(), RuntimeControlError> {
        match target {
            ExecutionMode::Live => {
                let refresher = self.metrics_refresh.as_ref().ok_or_else(|| {
                    RuntimeControlError::Activation("metrics refresher unavailable".to_owned())
                })?;
                refresher
                    .refresh()
                    .await
                    .map_err(|error| RuntimeControlError::Activation(error.to_string()))?;
                if self.metrics_state.source() != RiskMetricsSource::AuthoritativeClob {
                    return Err(RuntimeControlError::Activation(
                        "CLOB metrics not authoritative after refresh".to_owned(),
                    ));
                }
                Ok(())
            }
            ExecutionMode::DryRun | ExecutionMode::Paper => {
                self.metrics_state.seed_simulated_snapshot(
                    target,
                    Usd::new(self.runtime_config.load().risk.bankroll_usd),
                );
                Ok(())
            }
        }
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

        // 2. Quiesce: halt then wait for the loop to drain.
        halt_trading(
            &self.risk_engine,
            &self.fsm,
            format!("execution mode transition {from} -> {target}"),
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
        resume_trading(&self.risk_engine, &self.fsm, operator_ack)
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))?;

        tracing::warn!(%from, %target, "execution mode transition committed");
        Ok(ModeTransitionReport { from, to: target })
    }

    async fn halt(&self, reason: String) {
        halt_trading(&self.risk_engine, &self.fsm, reason).await;
    }

    async fn resume(&self, operator_ack: &str) -> Result<(), RuntimeControlError> {
        resume_trading(&self.risk_engine, &self.fsm, operator_ack)
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))
    }

    async fn reset_circuit_breaker(&self, reason: &str) -> Result<(), RuntimeControlError> {
        self.risk_engine
            .reset_circuit_breaker(reason, self.risk_metrics())
            .await
            .map_err(|error| RuntimeControlError::Engine(error.to_string()))
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
        reason: BlacklistReason,
    ) -> Result<(), RuntimeControlError> {
        self.risk_engine
            .add_blacklist(market_id, reason, self.risk_metrics())
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
        let snapshot = self.risk_snapshot();
        SystemStatus {
            execution_mode: self.execution_mode.current(),
            breaker_state: snapshot.breaker_state,
            uptime_secs: self.started_at.elapsed().as_secs(),
            active_markets: u32::try_from(self.market_registry.active_markets().len())
                .unwrap_or(u32::MAX),
            open_positions: self.open_position_count(),
            pending_reservations: u32::try_from(self.exposure.active_count_sync())
                .unwrap_or(u32::MAX),
            total_exposure: snapshot.total_exposure,
            daily_pnl: snapshot.daily_pnl,
            catalog: self.catalog.catalog_state(),
            checked_at: Utc::now(),
        }
    }

    async fn health(&self) -> HealthReport {
        self.health_checker.check_all().await
    }
}
