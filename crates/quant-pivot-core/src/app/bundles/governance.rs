//! Governance bundle: runtime config, mode control, health, notifications.

use super::{DataBundle, InfraBundle};
use crate::{
    governance::{
        DefaultModePreflight, DefaultModeTransitionGate, KillSwitchControl, KillSwitchHandle,
        ModePreflightDeps, RuntimeModeHandle, SystemStatusPublisher, WeightOverlayApplicator,
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
        NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeControlPort,
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
        PgCapitalAllocationRepository, PgKillSwitchStateRepository, PgModelRegistryRepository,
        PgReconciliationRepository, PgRuntimeConfigVersionRepository, PgShadowComparisonRepository,
        PgSystemRuntimeStateRepository, SYSTEM_KILL_SWITCH_ID,
    },
    traits::{
        KillSwitchStateRepository, RuntimeConfigVersionRepository, SystemRuntimeStateRepository,
    },
};
use quant_pivot_storage::postgres::PostgresPool;
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
    pub async fn bootstrap(pg: &PostgresPool) -> QuantResult<Self> {
        let runtime_config_repo = Arc::new(PgRuntimeConfigVersionRepository::new(
            pg.connection().clone(),
        ));
        let config = ensure_runtime_config_activation(runtime_config_repo.as_ref()).await?;
        let alerts = Arc::new(AlertDispatcher::new(&config.notification));
        let store = Arc::new(RuntimeConfigStore::new(config.clone()));
        let mode = RuntimeModeHandle::new(
            restore_quant_runtime_mode(&PgSystemRuntimeStateRepository::new(
                pg.connection().clone(),
            ))
            .await?,
        );
        let kill_switch_info =
            restore_kill_switch(&PgKillSwitchStateRepository::new(pg.connection().clone())).await?;
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

        let weight_overlay = Arc::new(WeightOverlayApplicator::new());
        // Seed the overlay from the active config so a non-published candidate /
        // shadow runs under its configured weights before the first re-activation.
        weight_overlay.reload(
            &runtime_config.current().factors,
            &runtime_config.current().model,
        );

        let applicator = Arc::new(RuntimeConfigApplicator::new(
            Arc::clone(&runtime_config),
            RuntimeConfigSubscribers {
                market_filter: Arc::clone(&data.market_filter),
                market_registry: Arc::clone(&data.market_registry),
                market_cache: Arc::clone(&data.market_cache),
                ws_subscription: Some(Arc::clone(&data.ws_subscription)),
                data_quality: Arc::clone(&data.data_quality),
                metrics: Arc::clone(metrics),
                alerts: Arc::clone(&alerts),
                weight_overlay: Arc::clone(&weight_overlay),
                subscription_window_hours: deploy
                    .market_data
                    .websocket
                    .engine_subscription_window_hours,
            },
        ));

        let health_checker = Arc::new(HealthChecker::new(HealthCheckerDeps {
            pg_pool: Arc::clone(&infra.pg),
            ch_pool: Arc::clone(&infra.ch),
            ws_health: Arc::clone(&data.ws_manager) as Arc<dyn WsShardHealthPort>,
            catalog: Arc::clone(&data.catalog),
            runtime_mode: runtime_mode.clone(),
        }));

        let conn = infra.pg.connection().clone();

        // Operational kill-switch control (persist + hot-swap + metric + WS fan-out).
        let kill_switch_control = Arc::new(KillSwitchControl::new(
            kill_switch_handle.clone(),
            kill_switch_view,
            Arc::new(PgKillSwitchStateRepository::new(conn.clone())),
            Arc::clone(metrics),
            Arc::clone(&status_publisher),
        ));
        let kill_switch: Arc<dyn KillSwitchPort> = kill_switch_control;

        // Mode-transition matrix + read-only upgrade preflight.
        let transition_gate = Arc::new(DefaultModeTransitionGate::new());
        let preflight = Arc::new(DefaultModePreflight::new(ModePreflightDeps {
            deploy: Arc::clone(deploy),
            config_store: Arc::clone(&runtime_config),
            data_quality: Arc::clone(&data.data_quality) as Arc<dyn DataQualityPort>,
            model_registry: Arc::new(PgModelRegistryRepository::new(conn.clone())),
            shadow_comparison: Arc::new(PgShadowComparisonRepository::new(conn.clone())),
            reconciliation: Arc::new(PgReconciliationRepository::new(conn.clone())),
            capital: Arc::new(PgCapitalAllocationRepository::new(conn.clone())),
            kill_switch: kill_switch_handle.clone(),
        }));

        let runtime_control = Arc::new(QuantRuntimeControl::new(QuantRuntimeControlDeps {
            runtime_mode: runtime_mode.clone(),
            health_checker: Arc::clone(&health_checker),
            runtime_state_repo: PgSystemRuntimeStateRepository::new(conn),
            transition_gate,
            preflight,
            kill_switch: Arc::clone(&kill_switch),
            status_publisher: Arc::clone(&status_publisher),
        }));
        status_publisher.register(Arc::clone(&runtime_control));
        let runtime_control: Arc<dyn RuntimeControlPort> = runtime_control;

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
        }
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
    if let Some(version) = &current {
        if let Ok(config) = RuntimeConfig::from_json(&version.config_json) {
            return Ok(config);
        }
        tracing::warn!("active runtime config invalid — reseeding defaults");
    }

    let config = RuntimeConfig::default();
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
                reason: format!(
                    "bootstrap default runtime config (schema_version={RUNTIME_CONFIG_SCHEMA_VERSION})"
                ),
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
