//! Governance bundle: runtime config, mode control, health, notifications.

use super::{DataBundle, InfraBundle, PgRepositories};
use crate::{
    execution::ExitMonitorHealthHandle,
    governance::{
        DefaultModePreflight, DefaultModeTransitionGate, KillSwitchControl, KillSwitchHandle,
        ModePreflightDeps, RuntimeModeHandle, SystemStatusPublisher, WeightOverlayApplicator,
        execution_recovery::{ExecutionRecoveryCoordinator, ExecutionRecoveryHandle},
        runtime_control::{QuantRuntimeControl, QuantRuntimeControlDeps},
    },
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
};
use chrono::Utc;
use quant_pivot_api::ws::WsShardHealthPort;
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        CoreEventPublisher, DataQualityPort, KillSwitchPort, KillSwitchStateInfo, KillSwitchView,
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeControlPort, SystemStatus,
        UpsertKillSwitchState,
    },
    enums::{
        execution::KillSwitchState,
        quant::QuantRuntimeMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use quant_pivot_repository::{
    postgres::{
        PgKillSwitchStateRepository, PgSystemRuntimeStateRepository, SYSTEM_KILL_SWITCH_ID,
    },
    traits::{
        CapitalAllocationRepository, KillSwitchStateRepository, ModelRegistryRepository,
        ReconciliationRepository, RuntimeConfigVersionRepository, ShadowComparisonRepository,
        SystemRuntimeStateRepository,
    },
};
use std::sync::Arc;

/// Active runtime config, mode, kill-switch, and notification wiring loaded from Postgres.
pub struct RuntimeSnapshot {
    pub config: RuntimeConfig,
    pub store: Arc<RuntimeConfigStore>,
    pub mode: RuntimeModeHandle,
    pub kill_switch_handle: KillSwitchHandle,
    pub kill_switch_view: KillSwitchView,
    pub alerts: Arc<AlertDispatcher>,
}

impl RuntimeSnapshot {
    /// Bootstrap or restore the active runtime config, quant runtime mode, and
    /// operational kill-switch (both operational singletons fail closed if missing).
    pub async fn bootstrap(repos: &PgRepositories) -> QuantResult<Self> {
        let config = ensure_runtime_config_activation(repos.runtime_config.as_ref()).await?;
        let alerts = Arc::new(AlertDispatcher::new(&config.notification));
        let store = Arc::new(RuntimeConfigStore::new(config.clone()));
        let mode = RuntimeModeHandle::new(
            restore_quant_runtime_mode(repos.system_runtime_state.as_ref()).await?,
        );
        let kill_switch_info = restore_kill_switch(repos.kill_switch_state.as_ref()).await?;
        let kill_switch_handle = KillSwitchHandle::new(kill_switch_info.state);
        let kill_switch_view = KillSwitchView::from(kill_switch_info);
        Ok(Self {
            config,
            store,
            mode,
            kill_switch_handle,
            kill_switch_view,
            alerts,
        })
    }
}

/// Dependencies required after the data plane is wired.
pub struct GovernanceBundleDeps<'a> {
    pub deploy: &'a Arc<DeployConfig>,
    pub metrics: &'a Arc<MetricsHub>,
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub runtime: RuntimeSnapshot,
    pub events: CoreEventPublisher,
}

/// Governance: runtime config propagation, mode control, kill switch, health, alerts.
pub struct GovernanceBundle {
    pub runtime_config: Arc<RuntimeConfigStore>,
    pub applicator: Arc<RuntimeConfigApplicator>,
    pub runtime_mode: RuntimeModeHandle,
    /// Lock-free kill-switch hot read (admission / exit paths; mirrors `runtime_mode`).
    pub kill_switch_handle: KillSwitchHandle,
    pub alerts: Arc<AlertDispatcher>,
    pub health_checker: Arc<HealthChecker>,
    pub runtime_control: Arc<dyn RuntimeControlPort>,
    /// Operational kill-switch control surface (governed read/write).
    pub kill_switch: Arc<dyn KillSwitchPort>,
    /// Candidate / shadow factor-weight overlay snapshot, reloaded on activation.
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Exit-monitor health (05.6): shared with the execution bundle's worker and
    /// read by admission `#20` + the auto-execution mode preflight.
    pub exit_monitor_health: ExitMonitorHealthHandle,
    /// Lock-free execution recovery summary embedded in [`SystemStatus`].
    pub execution_recovery: Arc<ExecutionRecoveryCoordinator>,
    /// Shared WS fan-out helper for mode / kill-switch and lifecycle broadcasts.
    pub status_publisher: Arc<SystemStatusPublisher>,
}

impl GovernanceBundle {
    /// Finish governance wiring once infra and data bundles are assembled.
    pub fn assemble(deps: GovernanceBundleDeps<'_>) -> Self {
        let GovernanceBundleDeps {
            deploy,
            metrics,
            infra,
            data,
            runtime,
            events,
        } = deps;
        let RuntimeSnapshot {
            config: _,
            store: runtime_config,
            mode: runtime_mode,
            kill_switch_handle,
            kill_switch_view,
            alerts,
        } = runtime;

        let status_publisher = SystemStatusPublisher::new(events);
        let weight_overlay = seed_weight_overlay(&runtime_config);
        let applicator = build_runtime_config_applicator(
            deploy,
            metrics,
            &runtime_config,
            &alerts,
            &weight_overlay,
            data,
        );
        let health_checker = build_health_checker(infra, data, &runtime_mode);
        let reconciliation_repo: Arc<dyn ReconciliationRepository> =
            Arc::clone(&infra.repos.reconciliation) as Arc<dyn ReconciliationRepository>;

        let OperationalControls {
            kill_switch,
            execution_recovery,
            exit_monitor_health,
            runtime_control,
        } = wire_operational_controls(OperationalControlsDeps {
            deploy,
            metrics,
            data,
            infra,
            runtime_config: &runtime_config,
            runtime_mode: &runtime_mode,
            kill_switch_handle: kill_switch_handle.clone(),
            kill_switch_view,
            status_publisher: &status_publisher,
            health_checker: &health_checker,
            reconciliation_repo: &reconciliation_repo,
        });

        Self {
            runtime_config,
            applicator,
            runtime_mode,
            kill_switch_handle,
            alerts,
            health_checker,
            runtime_control,
            kill_switch,
            weight_overlay,
            exit_monitor_health,
            execution_recovery,
            status_publisher,
        }
    }

    /// Bootstrap the execution recovery summary from live reconciliation state.
    pub async fn bootstrap_execution_recovery(&self) -> QuantResult<()> {
        self.execution_recovery.refresh().await
    }
}

fn seed_weight_overlay(runtime_config: &Arc<RuntimeConfigStore>) -> Arc<WeightOverlayApplicator> {
    let weight_overlay = Arc::new(WeightOverlayApplicator::new());
    // Seed the overlay from the active config so a non-published candidate /
    // shadow runs under its configured weights before the first re-activation.
    weight_overlay.reload(
        &runtime_config.current().factors,
        &runtime_config.current().model,
    );
    weight_overlay
}

fn build_runtime_config_applicator(
    deploy: &Arc<DeployConfig>,
    metrics: &Arc<MetricsHub>,
    runtime_config: &Arc<RuntimeConfigStore>,
    alerts: &Arc<AlertDispatcher>,
    weight_overlay: &Arc<WeightOverlayApplicator>,
    data: &DataBundle,
) -> Arc<RuntimeConfigApplicator> {
    Arc::new(RuntimeConfigApplicator::new(
        Arc::clone(runtime_config),
        RuntimeConfigSubscribers {
            market_filter: Arc::clone(&data.market_filter),
            market_registry: Arc::clone(&data.market_registry),
            market_cache: Arc::clone(&data.market_cache),
            ws_subscription: Some(Arc::clone(&data.ws_subscription)),
            data_quality: Arc::clone(&data.data_quality),
            metrics: Arc::clone(metrics),
            alerts: Arc::clone(alerts),
            weight_overlay: Arc::clone(weight_overlay),
            subscription_window_hours: deploy
                .market_data
                .websocket
                .engine_subscription_window_hours,
        },
    ))
}

fn build_health_checker(
    infra: &InfraBundle,
    data: &DataBundle,
    runtime_mode: &RuntimeModeHandle,
) -> Arc<HealthChecker> {
    Arc::new(HealthChecker::new(HealthCheckerDeps {
        pg_pool: Arc::clone(&infra.pg),
        ch_pool: Arc::clone(&infra.ch),
        ws_health: Arc::clone(&data.ws_manager) as Arc<dyn WsShardHealthPort>,
        catalog: Arc::clone(&data.catalog),
        runtime_mode: runtime_mode.clone(),
    }))
}

struct OperationalControls {
    kill_switch: Arc<dyn KillSwitchPort>,
    execution_recovery: Arc<ExecutionRecoveryCoordinator>,
    exit_monitor_health: ExitMonitorHealthHandle,
    runtime_control: Arc<dyn RuntimeControlPort>,
}

struct OperationalControlsDeps<'a> {
    deploy: &'a Arc<DeployConfig>,
    metrics: &'a Arc<MetricsHub>,
    data: &'a DataBundle,
    infra: &'a InfraBundle,
    runtime_config: &'a Arc<RuntimeConfigStore>,
    runtime_mode: &'a RuntimeModeHandle,
    kill_switch_handle: KillSwitchHandle,
    kill_switch_view: KillSwitchView,
    status_publisher: &'a Arc<SystemStatusPublisher>,
    health_checker: &'a Arc<HealthChecker>,
    reconciliation_repo: &'a Arc<dyn ReconciliationRepository>,
}

fn wire_operational_controls(deps: OperationalControlsDeps<'_>) -> OperationalControls {
    let OperationalControlsDeps {
        deploy,
        metrics,
        data,
        infra,
        runtime_config,
        runtime_mode,
        kill_switch_handle,
        kill_switch_view,
        status_publisher,
        health_checker,
        reconciliation_repo,
    } = deps;
    let repos = &infra.repos;
    let recovery_slot = Arc::new(std::sync::OnceLock::new());
    let kill_switch: Arc<dyn KillSwitchPort> = Arc::new(KillSwitchControl::new(
        kill_switch_handle.clone(),
        kill_switch_view,
        Arc::clone(&repos.kill_switch_state) as Arc<dyn KillSwitchStateRepository>,
        Arc::clone(metrics),
        Arc::clone(status_publisher),
        Arc::clone(&recovery_slot),
    ));
    let execution_recovery = Arc::new(ExecutionRecoveryCoordinator::new(
        ExecutionRecoveryHandle::new(
            SystemStatus::bootstrap(runtime_mode.current()).execution_recovery,
        ),
        Arc::clone(reconciliation_repo),
        Arc::clone(&kill_switch),
        runtime_mode.clone(),
    ));
    let _ = recovery_slot.set(Arc::clone(&execution_recovery));
    let exit_monitor_health = ExitMonitorHealthHandle::new();
    let transition_gate = Arc::new(DefaultModeTransitionGate::new());
    let preflight = Arc::new(DefaultModePreflight::new(ModePreflightDeps {
        deploy: Arc::clone(deploy),
        config_store: Arc::clone(runtime_config),
        data_quality: Arc::clone(&data.data_quality) as Arc<dyn DataQualityPort>,
        model_registry: Arc::clone(&repos.model_registry) as Arc<dyn ModelRegistryRepository>,
        shadow_comparison: Arc::clone(&repos.shadow_comparison)
            as Arc<dyn ShadowComparisonRepository>,
        reconciliation: Arc::clone(reconciliation_repo),
        capital: Arc::clone(&repos.capital_allocation) as Arc<dyn CapitalAllocationRepository>,
        kill_switch: kill_switch_handle,
        exit_monitor_health: exit_monitor_health.clone(),
    }));
    let runtime_control = Arc::new(QuantRuntimeControl::new(QuantRuntimeControlDeps {
        runtime_mode: runtime_mode.clone(),
        health_checker: Arc::clone(health_checker),
        runtime_state_repo: Arc::clone(&repos.system_runtime_state)
            as Arc<dyn SystemRuntimeStateRepository>,
        transition_gate,
        preflight,
        kill_switch: Arc::clone(&kill_switch),
        status_publisher: Arc::clone(status_publisher),
        execution_recovery: execution_recovery.handle(),
    }));
    status_publisher.register(Arc::clone(&runtime_control) as Arc<dyn RuntimeControlPort>);

    OperationalControls {
        kill_switch,
        execution_recovery,
        exit_monitor_health,
        runtime_control: runtime_control as Arc<dyn RuntimeControlPort>,
    }
}

async fn restore_quant_runtime_mode(
    repo: &PgSystemRuntimeStateRepository,
) -> QuantResult<QuantRuntimeMode> {
    if let Some(state) = repo.load().await? {
        return Ok(state.quant_runtime_mode);
    }
    tracing::warn!("system_runtime_state singleton missing; re-seeding ReportOnly");
    let mode = QuantRuntimeMode::ReportOnly;
    repo.upsert_quant_runtime_mode(mode, "bootstrap", "fail-closed re-seed (row missing)")
        .await?;
    Ok(mode)
}

async fn restore_kill_switch(
    repo: &PgKillSwitchStateRepository,
) -> QuantResult<KillSwitchStateInfo> {
    if let Some(info) = repo.load().await? {
        return Ok(info);
    }
    tracing::warn!("system_kill_switch singleton missing; re-seeding Closed (fail-closed)");
    let info = repo
        .upsert(UpsertKillSwitchState {
            id: SYSTEM_KILL_SWITCH_ID,
            state: KillSwitchState::Closed,
            changed_by: "bootstrap".to_owned(),
            reason: "fail-closed re-seed (row missing)".to_owned(),
            requires_operator_ack: false,
            changed_at: Utc::now(),
        })
        .await?;
    Ok(info)
}

async fn ensure_runtime_config_activation(
    repo: &dyn RuntimeConfigVersionRepository,
) -> QuantResult<RuntimeConfig> {
    let current = repo.load_current().await?;
    // Fast path: the active config already parses under the current schema.
    if let Some(version) = &current
        && let Ok(config) = RuntimeConfig::from_json(&version.config_json)
    {
        return Ok(config);
    }

    // Otherwise fail closed to defaults (no schema migration — project is pre-production).
    if current.is_some() {
        tracing::warn!("active runtime config invalid — reseeding defaults");
    }
    let config = RuntimeConfig::default();
    let reason = format!(
        "bootstrap default runtime config (schema_version={RUNTIME_CONFIG_SCHEMA_VERSION})"
    );

    let config_json = config.to_json();
    let config_hash = CanonicalDigest::content_hash_json(&config_json)?;
    let version = match repo.load_by_hash(&config_hash).await? {
        Some(version) => version,
        None => {
            repo.create_version(NewRuntimeConfigVersion {
                runtime_config_version_id: RuntimeConfigVersionId::from_v7(),
                config_hash,
                schema_version: RUNTIME_CONFIG_SCHEMA_VERSION,
                config_json,
                source: RuntimeConfigVersionSource::Bootstrap,
                created_by: "system".to_owned(),
                reason,
            })
            .await?
        }
    };

    repo.activate_version(NewRuntimeConfigActivation {
        runtime_config_activation_id: RuntimeConfigActivationId::from_v7(),
        runtime_config_version_id: version.runtime_config_version_id.clone(),
        activated_at: chrono::Utc::now(),
        activated_by: "system".to_owned(),
        reason: "bootstrap runtime config activation".to_owned(),
        activation_kind: if current.is_some() {
            RuntimeConfigActivationKind::Promote
        } else {
            RuntimeConfigActivationKind::Initial
        },
        previous_runtime_config_version_id: current.map(|v| v.runtime_config_version_id),
        rollback_target_version_id: None,
        audit_event_id: None,
    })
    .await?;
    Ok(config)
}
