//! Atomic runtime-control service for entry authorization, settlement, and kill switch.

use std::{sync::Arc, time::Instant};

use async_trait::async_trait;
use chrono::Utc;
use quant_pivot_error::{QuantResult, execution::ExecutionError};
use quant_pivot_models::{
    domain::{
        governance::{
            HealthReport, KillSwitchView, MarketDataConnectivity, RuntimeControlSnapshot,
            RuntimeControlUpdate, SystemStatus, WsShardConnectivity,
        },
        ports::{
            CatalogState, CatalogStatusPort, EntryAuthorizationTransitionReport, KillSwitchPort,
            RuntimeControlPort, SetKillSwitchCommand,
        },
    },
    enums::{
        execution::KillSwitchState, quant::EntryAuthorizationPolicy,
        settlement::SettlementWritePolicy,
    },
};
use quant_pivot_repository::traits::RuntimeControlRepository;

use crate::{
    governance::{
        AuthorizationPreflight, RuntimeControlsHandle, execution_recovery::ExecutionRecoveryHandle,
        operational_phase::operational_phase_from_readiness, system_status::SystemStatusPublisher,
    },
    infra::health_checker::HealthChecker,
    observability::metrics_hub::MetricsHub,
};

pub struct QuantRuntimeControl {
    controls: RuntimeControlsHandle,
    health_checker: Arc<HealthChecker>,
    repository: Arc<dyn RuntimeControlRepository>,
    preflight: Arc<dyn AuthorizationPreflight>,
    metrics: Arc<MetricsHub>,
    status_publisher: Arc<SystemStatusPublisher>,
    execution_recovery: ExecutionRecoveryHandle,
    started_at: Instant,
}

pub struct QuantRuntimeControlDeps {
    pub controls: RuntimeControlsHandle,
    pub health_checker: Arc<HealthChecker>,
    pub repository: Arc<dyn RuntimeControlRepository>,
    pub preflight: Arc<dyn AuthorizationPreflight>,
    pub metrics: Arc<MetricsHub>,
    pub status_publisher: Arc<SystemStatusPublisher>,
    pub execution_recovery: ExecutionRecoveryHandle,
}

impl QuantRuntimeControl {
    #[must_use]
    pub fn new(deps: QuantRuntimeControlDeps) -> Self {
        deps.metrics
            .set_policy_automatic_halted(!deps.controls.kill_switch_state().allows_new_entry());
        Self {
            controls: deps.controls,
            health_checker: deps.health_checker,
            repository: deps.repository,
            preflight: deps.preflight,
            metrics: deps.metrics,
            status_publisher: deps.status_publisher,
            execution_recovery: deps.execution_recovery,
            started_at: Instant::now(),
        }
    }

    async fn persist(&self, update: RuntimeControlUpdate) -> QuantResult<RuntimeControlSnapshot> {
        let snapshot = RuntimeControlSnapshot::from(self.repository.compare_and_set(update).await?);
        self.controls.publish_local(snapshot.clone());
        self.metrics
            .set_policy_automatic_halted(!snapshot.kill_switch_state.allows_new_entry());
        self.status_publisher.publish();
        Ok(snapshot)
    }

    fn kill_switch_view(snapshot: &RuntimeControlSnapshot) -> KillSwitchView {
        KillSwitchView {
            state: snapshot.kill_switch_state,
            requires_operator_ack: snapshot.kill_switch_requires_ack,
            revision: snapshot.revision,
            last_reason: snapshot.reason.clone(),
            changed_by: snapshot.changed_by.clone(),
            changed_at: snapshot.changed_at,
        }
    }
}

#[async_trait]
impl RuntimeControlPort for QuantRuntimeControl {
    fn snapshot(&self) -> RuntimeControlSnapshot {
        self.controls.snapshot()
    }

    async fn switch_entry_authorization_policy(
        &self,
        target: EntryAuthorizationPolicy,
        expected_revision: i64,
        actor: &str,
        reason: &str,
    ) -> QuantResult<EntryAuthorizationTransitionReport> {
        let current = self.controls.snapshot();
        let from = current.entry_authorization_policy;
        let preflight = if from == target {
            None
        } else {
            if from.is_upgrade_to(target) {
                let report = self.preflight.run(target).await?;
                if !report.passed {
                    return Err(ExecutionError::AuthorizationPreflightDenied {
                        reason: report.summary(),
                    }
                    .into());
                }
                Some(report)
            } else {
                None
            }
        };

        self.persist(RuntimeControlUpdate {
            expected_revision,
            entry_authorization_policy: Some(target),
            settlement_write_policy: None,
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: actor.to_owned(),
            reason: reason.to_owned(),
        })
        .await?;
        Ok(EntryAuthorizationTransitionReport {
            from,
            to: target,
            preflight,
        })
    }

    async fn switch_settlement_write_policy(
        &self,
        target: SettlementWritePolicy,
        expected_revision: i64,
        actor: &str,
        reason: &str,
    ) -> QuantResult<RuntimeControlSnapshot> {
        let current = self.controls.snapshot();
        if settlement_policy_rank(target) > settlement_policy_rank(current.settlement_write_policy)
        {
            let report = self
                .preflight
                .run(current.entry_authorization_policy)
                .await?;
            if !report.passed {
                return Err(ExecutionError::AuthorizationPreflightDenied {
                    reason: report.summary(),
                }
                .into());
            }
        }
        self.persist(RuntimeControlUpdate {
            expected_revision,
            entry_authorization_policy: None,
            settlement_write_policy: Some(target),
            kill_switch_state: None,
            kill_switch_requires_ack: None,
            actor: actor.to_owned(),
            reason: reason.to_owned(),
        })
        .await
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
        let last_message_age_ms = self.health_checker.ws_message_age_ms();
        let market_data = MarketDataConnectivity::from_parts(last_message_age_ms, ws_shards);
        let active_markets = match &catalog {
            CatalogState::Ready { markets, .. } => u32::try_from(*markets).unwrap_or(u32::MAX),
            CatalogState::Warming => 0,
        };
        let controls = self.controls.snapshot();
        let kill_switch = Self::kill_switch_view(&controls);
        let operational_phase = operational_phase_from_readiness(
            controls.kill_switch_state,
            catalog.is_ready(),
            market_data.ready,
        );

        SystemStatus {
            entry_authorization_policy: controls.entry_authorization_policy,
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

#[async_trait]
impl KillSwitchPort for QuantRuntimeControl {
    fn current(&self) -> KillSwitchState {
        self.controls.kill_switch_state()
    }

    fn view(&self) -> KillSwitchView {
        Self::kill_switch_view(&self.controls.snapshot())
    }

    async fn set(&self, command: SetKillSwitchCommand) -> QuantResult<KillSwitchView> {
        let current = self.controls.snapshot();
        let current_latched =
            current.kill_switch_state.is_emergency() || current.kill_switch_requires_ack;
        let loosening =
            command.target.restriction_rank() < current.kill_switch_state.restriction_rank();
        if current_latched && loosening && !command.ack {
            return Err(ExecutionError::KillSwitchBlocks {
                state: current.kill_switch_state.to_string(),
                operation: "loosen_latched_requires_ack".to_owned(),
            }
            .into());
        }
        let snapshot = self
            .persist(RuntimeControlUpdate {
                expected_revision: command.expected_revision,
                entry_authorization_policy: None,
                settlement_write_policy: None,
                kill_switch_state: Some(command.target),
                kill_switch_requires_ack: Some(command.target.is_emergency() || command.latch),
                actor: command.actor,
                reason: command.reason,
            })
            .await?;
        Ok(Self::kill_switch_view(&snapshot))
    }
}

const fn settlement_policy_rank(policy: SettlementWritePolicy) -> u8 {
    match policy {
        SettlementWritePolicy::Disabled => 0,
        SettlementWritePolicy::GovernedCanary => 1,
        SettlementWritePolicy::OperatorApproval => 2,
        SettlementWritePolicy::PolicyAutomatic => 3,
    }
}
