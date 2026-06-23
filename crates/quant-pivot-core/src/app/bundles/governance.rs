//! Governance bundle: runtime config, mode control, health, notifications.

use super::{DataBundle, InfraBundle};
use crate::{
    governance::{RuntimeModeHandle, runtime_control::QuantRuntimeControl},
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    runtime_config::{RuntimeConfigApplicator, RuntimeConfigStore, RuntimeConfigSubscribers},
};
use quant_pivot_error::QuantResult;
use quant_pivot_models::{
    config::DeployConfig,
    domain::{NewRuntimeConfigActivation, NewRuntimeConfigVersion, RuntimeControlPort},
    enums::{
        quant::QuantRuntimeMode,
        runtime_config::{RuntimeConfigActivationKind, RuntimeConfigVersionSource},
    },
    hashing::CanonicalDigest,
    runtime_config::{RUNTIME_CONFIG_SCHEMA_VERSION, RuntimeConfig},
    types::{RuntimeConfigActivationId, RuntimeConfigVersionId},
};
use quant_pivot_repository::{
    postgres::{PgRuntimeConfigVersionRepository, PgSystemRuntimeStateRepository},
    traits::{RuntimeConfigVersionRepository, SystemRuntimeStateRepository},
};
use quant_pivot_storage::postgres::PostgresPool;
use std::sync::Arc;

/// Active runtime config, mode, and notification wiring loaded from Postgres.
pub struct RuntimeSnapshot {
    pub config: RuntimeConfig,
    pub store: Arc<RuntimeConfigStore>,
    pub mode: RuntimeModeHandle,
    pub alerts: Arc<AlertDispatcher>,
}

impl RuntimeSnapshot {
    /// Bootstrap or restore the active runtime config and quant runtime mode.
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
        Ok(Self {
            config,
            store,
            mode,
            alerts,
        })
    }
}

/// Dependencies required after the data plane is wired.
pub struct GovernanceBundleDeps<'a> {
    pub deploy: &'a DeployConfig,
    pub metrics: &'a Arc<MetricsHub>,
    pub infra: &'a InfraBundle,
    pub data: &'a DataBundle,
    pub runtime: RuntimeSnapshot,
}

/// Governance: runtime config propagation, mode control, health, alerts.
pub struct GovernanceBundle {
    pub runtime_config: Arc<RuntimeConfigStore>,
    pub applicator: Arc<RuntimeConfigApplicator>,
    pub runtime_mode: RuntimeModeHandle,
    pub alerts: Arc<AlertDispatcher>,
    pub health_checker: Arc<HealthChecker>,
    pub runtime_control: Arc<dyn RuntimeControlPort>,
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
        } = deps;
        let RuntimeSnapshot {
            config: _,
            store: runtime_config,
            mode: runtime_mode,
            alerts,
        } = runtime;

        let applicator = Arc::new(RuntimeConfigApplicator::new(
            Arc::clone(&runtime_config),
            RuntimeConfigSubscribers {
                market_filter: Arc::clone(&data.market_filter),
                market_registry: Arc::clone(&data.market_registry),
                market_cache: Arc::clone(&data.market_cache),
                ws_subscription: Some(Arc::clone(&data.ws_subscription)),
                data_quality: Arc::clone(&data.data_quality),
                metrics: Arc::clone(metrics),
                subscription_window_hours: deploy
                    .market_data
                    .websocket
                    .engine_subscription_window_hours,
            },
        ));

        let health_checker = Arc::new(HealthChecker::new(HealthCheckerDeps {
            pg_pool: Arc::clone(&infra.pg),
            ch_pool: Arc::clone(&infra.ch),
            ws_manager: Arc::clone(&data.ws_manager),
            catalog: Arc::clone(&data.catalog),
            runtime_mode: runtime_mode.clone(),
        }));

        let runtime_control: Arc<dyn RuntimeControlPort> = Arc::new(QuantRuntimeControl::new(
            runtime_mode.clone(),
            Arc::clone(&health_checker),
            PgSystemRuntimeStateRepository::new(infra.pg.connection().clone()),
        ));

        Self {
            runtime_config,
            applicator,
            runtime_mode,
            alerts,
            health_checker,
            runtime_control,
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
