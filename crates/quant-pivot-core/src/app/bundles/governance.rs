//! Governance bundle: runtime config, mode control, health, notifications.

use std::{sync::Arc, time::Duration};

use quant_pivot_api::ws::WsShardHealthPort;
use quant_pivot_error::{QuantError, QuantResult, storage::StorageError};
use quant_pivot_models::{
    config::DeployConfig,
    domain::{
        governance::{RuntimeControlSnapshot, SystemStatus},
        ports::{
            DataQualityPort, KillSwitchPort, PolicySnapshotPort, RuntimeControlPort,
            SystemCapabilityPort,
        },
        runtime::CoreEventPublisher,
    },
    runtime_config::{ActivePolicyBundle, DecisionPolicySnapshot},
};
use quant_pivot_repository::traits::{
    CalibrationArtifactRepository, CapitalAllocationRepository, ModelRegistryRepository,
    ModelRunRepository, PolicyRepository, RecommendationReportRepository, ReconciliationRepository,
    RuntimeControlRepository, ShadowComparisonRepository,
};
use quant_pivot_research::artifact::{ArtifactStore, build_artifact_store};
use tokio::{task::JoinHandle, time::MissedTickBehavior};

use super::{DataBundle, InfraBundle, PgRepositories};
use crate::{
    execution::ExitMonitorHealthHandle,
    governance::{
        BiasTableApplicator, CategoryPointerGuard, DefaultModePreflight, DefaultModeTransitionGate,
        ModePreflightDeps, RuntimeControlsHandle, SystemCapabilityService, SystemStatusPublisher,
        WeightOverlayApplicator,
        execution_recovery::{ExecutionRecoveryCoordinator, ExecutionRecoveryHandle},
        runtime_control::{QuantRuntimeControl, QuantRuntimeControlDeps},
    },
    infra::health_checker::{HealthChecker, HealthCheckerDeps},
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    runtime_config::{DecisionPolicyStore, PolicySnapshotApplicator, PolicySnapshotSubscribers},
};

/// Active runtime config, mode, kill-switch, and notification wiring loaded from Postgres.
pub struct RuntimeSnapshot {
    pub config: DecisionPolicySnapshot,
    pub store: Arc<DecisionPolicyStore>,
    pub controls: RuntimeControlsHandle,
    pub alerts: Arc<AlertDispatcher>,
}

impl RuntimeSnapshot {
    /// Bootstrap or restore the active runtime config, quant runtime mode, and
    /// operational kill-switch (both operational singletons fail closed if missing).
    pub async fn bootstrap(repos: &PgRepositories, deploy: &DeployConfig) -> QuantResult<Self> {
        let control = repos.runtime_control.load().await?;
        let active_bundle = ensure_policy_activation(repos.runtime_config.as_ref()).await?;
        let config = active_bundle.snapshot.clone();
        let alerts = Arc::new(AlertDispatcher::new(&deploy.notifications)?);
        let store = Arc::new(DecisionPolicyStore::new_active(active_bundle));
        let controls = RuntimeControlsHandle::new(RuntimeControlSnapshot::from(control));
        Ok(Self {
            config,
            store,
            controls,
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
    pub runtime_config: Arc<DecisionPolicyStore>,
    pub applicator: Arc<PolicySnapshotApplicator>,
    pub runtime_controls: RuntimeControlsHandle,
    pub alerts: Arc<AlertDispatcher>,
    pub health_checker: Arc<HealthChecker>,
    pub runtime_control: Arc<dyn RuntimeControlPort>,
    /// Derived live capability surface.
    pub capabilities: Arc<dyn SystemCapabilityPort>,
    /// Operational kill-switch control surface (governed read/write).
    pub kill_switch: Arc<dyn KillSwitchPort>,
    /// Candidate / shadow factor-weight overlay snapshot, reloaded on activation.
    pub weight_overlay: Arc<WeightOverlayApplicator>,
    /// Favorite-longshot bias-table snapshot bound to the factor plane,
    /// reloaded + content-hash verified on activation.
    pub bias_table: Arc<BiasTableApplicator>,
    /// Exit-monitor health: shared with the execution bundle's worker and
    /// read by admission `#20` + the auto-execution mode preflight.
    pub exit_monitor_health: ExitMonitorHealthHandle,
    /// Lock-free execution recovery summary embedded in [`SystemStatus`].
    pub execution_recovery: Arc<ExecutionRecoveryCoordinator>,
    /// Shared WS fan-out helper for mode / kill-switch and lifecycle broadcasts.
    pub status_publisher: Arc<SystemStatusPublisher>,
    /// Durable DB → `ArcSwap` convergence loop for activations committed by any instance.
    pub policy_bundle_reconciler: JoinHandle<()>,
}

impl GovernanceBundle {
    /// Finish governance wiring once infra and data bundles are assembled.
    pub fn assemble(deps: GovernanceBundleDeps<'_>) -> QuantResult<Self> {
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
            controls: runtime_controls,
            alerts,
        } = runtime;

        let status_publisher = SystemStatusPublisher::new(events);
        let weight_overlay = seed_weight_overlay(&runtime_config);
        let bias_table = Arc::new(BiasTableApplicator::new(Arc::clone(
            &infra.repos.calibration_artifact,
        )
            as Arc<dyn CalibrationArtifactRepository>));
        let artifact_store: Arc<dyn ArtifactStore> =
            build_artifact_store(&deploy.research.artifact_store)?;
        let category_pointer_guard = Arc::new(CategoryPointerGuard::new(
            Arc::clone(&infra.repos.model_registry) as Arc<dyn ModelRegistryRepository>,
            artifact_store,
        ));
        let applicator = build_runtime_config_applicator(PolicySnapshotApplicatorDeps {
            runtime_config: &runtime_config,
            weight_overlay: &weight_overlay,
            bias_table: &bias_table,
            category_pointer_guard: &category_pointer_guard,
            data,
        });
        let policy_bundle_reconciler = spawn_policy_bundle_reconciler(
            Arc::clone(&infra.repos.runtime_config) as Arc<dyn PolicyRepository>,
            Arc::clone(&applicator),
        );
        let capabilities: Arc<dyn SystemCapabilityPort> = Arc::new(SystemCapabilityService::new(
            Arc::clone(&applicator) as Arc<dyn PolicySnapshotPort>,
            Arc::clone(&infra.repos.model_run) as Arc<dyn ModelRunRepository>,
            Arc::clone(&infra.repos.recommendation_report)
                as Arc<dyn RecommendationReportRepository>,
        ));
        let health_checker = build_health_checker(infra, data, &runtime_controls);
        let reconciliation_repo: Arc<dyn ReconciliationRepository> =
            Arc::clone(&infra.repos.reconciliation) as Arc<dyn ReconciliationRepository>;

        let OperationalControls {
            kill_switch,
            execution_recovery,
            exit_monitor_health,
            runtime_control,
        } = wire_operational_controls(&OperationalControlsDeps {
            deploy,
            metrics,
            data,
            infra,
            runtime_config: &runtime_config,
            runtime_controls: &runtime_controls,
            status_publisher: &status_publisher,
            health_checker: &health_checker,
            reconciliation_repo: &reconciliation_repo,
        });
        status_publisher.register_capabilities(Arc::clone(&capabilities));
        status_publisher.publish();

        Ok(Self {
            runtime_config,
            applicator,
            runtime_controls,
            alerts,
            health_checker,
            runtime_control,
            capabilities,
            kill_switch,
            weight_overlay,
            bias_table,
            exit_monitor_health,
            execution_recovery,
            status_publisher,
            policy_bundle_reconciler,
        })
    }

    /// Bootstrap the execution recovery summary from live reconciliation state.
    pub async fn bootstrap_execution_recovery(&self) -> QuantResult<()> {
        self.execution_recovery.refresh().await
    }

    /// Seed the favorite-longshot bias table from the active config on boot, so
    /// the factor plane binds the pinned table before the first re-activation. A
    /// pinned-but-unloadable table fails boot closed (never silently inert).
    pub async fn bootstrap_bias_table(&self) -> QuantResult<()> {
        let table = self
            .bias_table
            .prepare(
                &self
                    .runtime_config
                    .current()
                    .profile_artifacts
                    .scoring
                    .definition
                    .structural
                    .favorite_longshot,
            )
            .await
            .map_err(QuantError::from)?;
        self.bias_table.publish(table);
        Ok(())
    }
}

fn spawn_policy_bundle_reconciler(
    repository: Arc<dyn PolicyRepository>,
    applicator: Arc<PolicySnapshotApplicator>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(2));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let bundle = match repository.load_current_bundle().await {
                Ok(Some(bundle)) => bundle,
                Ok(None) => continue,
                Err(error) => {
                    tracing::error!(error = %error, "policy bundle reconciliation read failed");
                    continue;
                }
            };
            let local = applicator.current_bundle();
            if local.as_ref().is_some_and(|published| {
                published.generation == bundle.generation
                    && published.decision_policy_snapshot_id == bundle.decision_policy_snapshot_id
                    && published.snapshot_hash == bundle.snapshot_hash
            }) {
                continue;
            }
            if local
                .as_ref()
                .is_some_and(|published| published.generation > bundle.generation)
            {
                tracing::error!(
                    local_generation = %local.as_ref().map_or(bundle.generation, |value| value.generation),
                    durable_generation = %bundle.generation,
                    "local policy bundle is newer than the database guard; refusing rollback"
                );
                continue;
            }
            match applicator.prepare(bundle.snapshot.clone()).await {
                Ok(prepared) => {
                    if let Err(error) = prepared.publish_bundle(bundle) {
                        tracing::error!(error = %error, "policy bundle reconciliation publish failed");
                    }
                }
                Err(error) => {
                    tracing::error!(error = %error, "durable policy bundle failed consumer preparation");
                }
            }
        }
    })
}

fn seed_weight_overlay(runtime_config: &Arc<DecisionPolicyStore>) -> Arc<WeightOverlayApplicator> {
    let weight_overlay = Arc::new(WeightOverlayApplicator::new());
    // Seed the overlay from the active config so a non-published candidate /
    // shadow runs under its configured weights before the first re-activation.
    weight_overlay.reload(
        &runtime_config
            .current()
            .profile_artifacts
            .scoring
            .definition,
        &runtime_config.current().model_routing.model,
    );
    weight_overlay
}

/// Dependencies for [`build_runtime_config_applicator`].
#[derive(Clone, Copy)]
struct PolicySnapshotApplicatorDeps<'a> {
    runtime_config: &'a Arc<DecisionPolicyStore>,
    weight_overlay: &'a Arc<WeightOverlayApplicator>,
    bias_table: &'a Arc<BiasTableApplicator>,
    category_pointer_guard: &'a Arc<CategoryPointerGuard>,
    data: &'a DataBundle,
}

fn build_runtime_config_applicator(
    deps: PolicySnapshotApplicatorDeps<'_>,
) -> Arc<PolicySnapshotApplicator> {
    let PolicySnapshotApplicatorDeps {
        runtime_config,
        weight_overlay,
        bias_table,
        category_pointer_guard,
        data,
    } = deps;
    Arc::new(PolicySnapshotApplicator::new(
        Arc::clone(runtime_config),
        PolicySnapshotSubscribers {
            market_filter: Arc::clone(&data.market_filter),
            market_cache: Arc::clone(&data.market_cache),
            data_quality: Arc::clone(&data.data_quality),
            weight_overlay: Arc::clone(weight_overlay),
            bias_table: Arc::clone(bias_table),
            category_pointer_guard: Arc::clone(category_pointer_guard),
        },
    ))
}

fn build_health_checker(
    infra: &InfraBundle,
    data: &DataBundle,
    runtime_controls: &RuntimeControlsHandle,
) -> Arc<HealthChecker> {
    Arc::new(HealthChecker::new(HealthCheckerDeps {
        pg_pool: Arc::clone(&infra.pg),
        ch_pool: Arc::clone(&infra.ch),
        ws_health: Arc::clone(&data.ws_manager) as Arc<dyn WsShardHealthPort>,
        catalog: Arc::clone(&data.catalog),
        runtime_controls: runtime_controls.clone(),
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
    runtime_config: &'a Arc<DecisionPolicyStore>,
    runtime_controls: &'a RuntimeControlsHandle,
    status_publisher: &'a Arc<SystemStatusPublisher>,
    health_checker: &'a Arc<HealthChecker>,
    reconciliation_repo: &'a Arc<dyn ReconciliationRepository>,
}

fn wire_operational_controls(deps: &OperationalControlsDeps<'_>) -> OperationalControls {
    let deploy = deps.deploy;
    let metrics = deps.metrics;
    let infra = deps.infra;
    let runtime_config = deps.runtime_config;
    let runtime_controls = deps.runtime_controls;
    let status_publisher = deps.status_publisher;
    let health_checker = deps.health_checker;
    let reconciliation_repo = deps.reconciliation_repo;
    let repos = &infra.repos;
    let execution_recovery_handle = ExecutionRecoveryHandle::new(
        SystemStatus::bootstrap(runtime_controls.quant_runtime_mode()).execution_recovery,
    );
    let exit_monitor_health = ExitMonitorHealthHandle::new();
    let transition_gate = Arc::new(DefaultModeTransitionGate::new());
    let preflight = Arc::new(DefaultModePreflight::new(ModePreflightDeps {
        deploy: Arc::clone(deploy),
        config_store: Arc::clone(runtime_config),
        data_quality: Arc::clone(&deps.data.data_quality) as Arc<dyn DataQualityPort>,
        model_registry: Arc::clone(&repos.model_registry) as Arc<dyn ModelRegistryRepository>,
        shadow_comparison: Arc::clone(&repos.shadow_comparison)
            as Arc<dyn ShadowComparisonRepository>,
        reconciliation: Arc::clone(reconciliation_repo),
        capital: Arc::clone(&repos.capital_allocation) as Arc<dyn CapitalAllocationRepository>,
        runtime_controls: runtime_controls.clone(),
        exit_monitor_health: exit_monitor_health.clone(),
    }));
    let runtime_control = Arc::new(QuantRuntimeControl::new(QuantRuntimeControlDeps {
        controls: runtime_controls.clone(),
        health_checker: Arc::clone(health_checker),
        repository: Arc::clone(&repos.runtime_control) as Arc<dyn RuntimeControlRepository>,
        transition_gate,
        preflight,
        metrics: Arc::clone(metrics),
        status_publisher: Arc::clone(status_publisher),
        execution_recovery: execution_recovery_handle.clone(),
    }));
    let kill_switch = Arc::clone(&runtime_control) as Arc<dyn KillSwitchPort>;
    let execution_recovery = Arc::new(ExecutionRecoveryCoordinator::new(
        execution_recovery_handle,
        Arc::clone(reconciliation_repo),
        Arc::clone(&kill_switch),
        runtime_controls.clone(),
    ));
    status_publisher.register(Arc::clone(&runtime_control) as Arc<dyn RuntimeControlPort>);

    OperationalControls {
        kill_switch,
        execution_recovery,
        exit_monitor_health,
        runtime_control: runtime_control as Arc<dyn RuntimeControlPort>,
    }
}

async fn ensure_policy_activation(repo: &dyn PolicyRepository) -> QuantResult<ActivePolicyBundle> {
    repo.load_current_bundle().await?.ok_or_else(|| {
        StorageError::InvariantViolation {
            entity: Some("decision_policy_snapshot"),
            detail: "fresh boot requires one canonical active decision-policy bundle".to_owned(),
        }
        .into()
    })
}
