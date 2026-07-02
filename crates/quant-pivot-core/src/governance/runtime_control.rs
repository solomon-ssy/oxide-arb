//! Quant runtime control port implementation for the web layer.

use crate::{
    governance::{
        ModePreflight, ModeTransitionGate, RuntimeModeHandle,
        execution_recovery::ExecutionRecoveryHandle,
        operational_phase::operational_phase_from_readiness, system_status::SystemStatusPublisher,
    },
    infra::health_checker::HealthChecker,
};
use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::{
        CatalogState, CatalogStatusPort, HealthReport, KillSwitchPort, MarketDataConnectivity,
        QuantModeTransitionReport, RuntimeControlPort, SystemStatus, WsShardConnectivity,
    },
    enums::quant::QuantRuntimeMode,
};
use quant_pivot_repository::traits::SystemRuntimeStateRepository;
use std::{sync::Arc, time::Instant};

/// Governed quant runtime control: mode reads/transitions, kill-switch projection,
/// and live system-status assembly.
pub struct QuantRuntimeControl {
    runtime_mode: RuntimeModeHandle,
    health_checker: Arc<HealthChecker>,
    runtime_state_repo: Arc<dyn SystemRuntimeStateRepository>,
    transition_gate: Arc<dyn ModeTransitionGate>,
    preflight: Arc<dyn ModePreflight>,
    kill_switch: Arc<dyn KillSwitchPort>,
    status_publisher: Arc<SystemStatusPublisher>,
    execution_recovery: ExecutionRecoveryHandle,
    started_at: Instant,
}

/// Construction dependencies for [`QuantRuntimeControl`].
pub struct QuantRuntimeControlDeps {
    pub runtime_mode: RuntimeModeHandle,
    pub health_checker: Arc<HealthChecker>,
    pub runtime_state_repo: Arc<dyn SystemRuntimeStateRepository>,
    pub transition_gate: Arc<dyn ModeTransitionGate>,
    pub preflight: Arc<dyn ModePreflight>,
    pub kill_switch: Arc<dyn KillSwitchPort>,
    pub status_publisher: Arc<SystemStatusPublisher>,
    pub execution_recovery: ExecutionRecoveryHandle,
}

impl QuantRuntimeControl {
    #[must_use]
    pub fn new(deps: QuantRuntimeControlDeps) -> Self {
        Self {
            runtime_mode: deps.runtime_mode,
            health_checker: deps.health_checker,
            runtime_state_repo: deps.runtime_state_repo,
            transition_gate: deps.transition_gate,
            preflight: deps.preflight,
            kill_switch: deps.kill_switch,
            status_publisher: deps.status_publisher,
            execution_recovery: deps.execution_recovery,
            started_at: Instant::now(),
        }
    }
}

#[async_trait]
impl RuntimeControlPort for QuantRuntimeControl {
    fn quant_runtime_mode(&self) -> QuantRuntimeMode {
        self.runtime_mode.current()
    }

    async fn switch_quant_mode(
        &self,
        target: QuantRuntimeMode,
        actor: &str,
        reason: &str,
    ) -> QuantResult<QuantModeTransitionReport> {
        let from = self.runtime_mode.current();
        // No-op: same mode never runs preflight and never re-persists.
        if from == target {
            return Ok(QuantModeTransitionReport {
                from,
                to: target,
                preflight: None,
            });
        }

        // Gate 1: transition matrix (forbidden edges fail closed, no persist).
        self.transition_gate.check(from, target)?;

        // Gate 2: business preflight on upgrades only (downgrades always allowed).
        let preflight = if from.is_upgrade_to(target) {
            let report = self.preflight.run(target).await?;
            if !report.passed {
                return Err(ExecutionError::ModePreflightDenied {
                    reason: report.summary(),
                }
                .into());
            }
            Some(report)
        } else {
            None
        };

        // Persist operational truth, then hot-swap the in-process handle.
        self.runtime_state_repo
            .upsert_quant_runtime_mode(target, actor, reason)
            .await?;
        self.runtime_mode.store(target);
        self.status_publisher.publish();
        Ok(QuantModeTransitionReport {
            from,
            to: target,
            preflight,
        })
    }

    fn system_status(&self) -> SystemStatus {
        let catalog = self.health_checker.catalog().catalog_state();
        let shards = self.health_checker.ws_shard_health();
        let ws_shards = WsShardConnectivity {
            total: u32::try_from(shards.total).unwrap_or(u32::MAX),
            disconnected: u32::try_from(shards.disconnected).unwrap_or(u32::MAX),
            oldest_disconnected_secs: shards.oldest_disconnected_secs,
            connected_ratio_bps: shards.connected_ratio_bps,
        };
        let last_message_age_ms = self.health_checker.ws_last_message_age_ms();
        let market_data = MarketDataConnectivity::from_parts(last_message_age_ms, ws_shards);
        let active_markets = match &catalog {
            CatalogState::Ready { markets, .. } => u32::try_from(*markets).unwrap_or(u32::MAX),
            CatalogState::Warming => 0,
        };
        let kill_switch = self.kill_switch.view();
        let operational_phase = operational_phase_from_readiness(
            kill_switch.state,
            catalog.is_ready(),
            market_data.ready,
        );

        SystemStatus {
            quant_runtime_mode: self.runtime_mode.current(),
            uptime_secs: self.started_at.elapsed().as_secs(),
            active_markets,
            catalog,
            operational_phase,
            market_data,
            kill_switch,
            execution_recovery: self.execution_recovery.current(),
            checked_at: Utc::now(),
        }
    }

    async fn health(&self) -> HealthReport {
        self.health_checker.check_all().await
    }
}
