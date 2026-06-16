mod common;

use common::disconnected_metrics_refresh;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::{
    bridge::{execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics},
    control::mode_transition::{CoreRuntimeControl, CoreRuntimeControlDeps},
    execution::fsm::ExecutionFSM,
    exposure::in_memory::InMemoryExposureReservation,
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    pipeline::market_registry::MarketRegistry,
    runtime_config::RuntimeConfigStore,
    service::{catalog_readiness::CatalogReadiness, risk_metrics::RiskMetricsState},
};
use oxide_arb_error::storage::StorageError;
use oxide_arb_models::{
    config::{DeployConfig, PolymarketConfig, WebSocketConfig},
    domain::{RuntimeControlError, RuntimeControlPort, SystemRuntimeStateInfo},
    enums::common::ExecutionMode,
    runtime_config::{NotificationConfig, RuntimeConfig},
    types::{MarketId, Usd},
};
use oxide_arb_repository::traits::SystemRuntimeStateRepository;
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_test_support::{mocks::MockPositionRepository, risk::TestRiskMetrics};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

struct MockSystemRuntimeState {
    mode: Mutex<Option<ExecutionMode>>,
}

impl MockSystemRuntimeState {
    const fn new() -> Self {
        Self {
            mode: Mutex::new(None),
        }
    }

    fn stored_mode(&self) -> Option<ExecutionMode> {
        *self.mode.lock().expect("mode lock")
    }
}

#[async_trait::async_trait]
impl SystemRuntimeStateRepository for MockSystemRuntimeState {
    async fn load(&self) -> Result<Option<SystemRuntimeStateInfo>, StorageError> {
        Ok(None)
    }

    async fn upsert_execution_mode(
        &self,
        mode: ExecutionMode,
        _changed_by: &str,
        _reason: &str,
    ) -> Result<(), StorageError> {
        *self.mode.lock().expect("mode lock") = Some(mode);
        Ok(())
    }
}

fn runtime_control(
    mode: ExecutionMode,
) -> (
    CoreRuntimeControl,
    ExecutionModeHandle,
    Arc<InMemoryExposureReservation>,
    Arc<MockSystemRuntimeState>,
) {
    let metrics_hub = Arc::new(MetricsHub::new());
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(
        oxide_arb_core::service::risk_metrics::ApiHealthTracker::new(
            std::time::Duration::from_secs(60),
        ),
    )));
    metrics_state.seed_simulated_snapshot(mode, Usd::new(rust_decimal_macros::dec!(1000)));
    let runtime_config = Arc::new(RuntimeConfigStore::new(RuntimeConfig::default()));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let execution_mode = ExecutionModeHandle::new(mode);
    let ws = Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ));
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        ws,
        execution_mode.clone(),
    ));
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .build(&TestRiskMetrics)
            .expect("risk engine"),
    );
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(Arc::clone(&metrics_hub), alerts));
    let system_runtime_state = Arc::new(MockSystemRuntimeState::new());
    let control = CoreRuntimeControl::new(
        CoreRuntimeControlDeps {
            execution_mode: execution_mode.clone(),
            risk_engine,
            catalog: Arc::new(CatalogReadiness::new()),
            fsm,
            exposure: Arc::clone(&exposure),
            metrics: risk_metrics,
            metrics_state: Arc::clone(&metrics_state),
            metrics_refresh: disconnected_metrics_refresh(
                metrics_state,
                mode,
                Arc::clone(&metrics_hub),
            ),
            clob_client: None,
            market_registry: Arc::new(MarketRegistry::new()),
            health_checker: None,
            deploy: Arc::new(DeployConfig::default()),
            runtime_config,
            position_repo: Arc::new(MockPositionRepository::default()),
            system_runtime_state: system_runtime_state.clone(),
            status_publisher: None,
        },
        std::time::Instant::now(),
    );
    (control, execution_mode, exposure, system_runtime_state)
}

#[tokio::test]
async fn live_without_clob_client_fails_before_commit() {
    let (control, mode, _, state) = runtime_control(ExecutionMode::Paper);

    let err = control
        .switch_execution_mode(ExecutionMode::Live, "operator")
        .await
        .expect_err("live transition should fail");

    assert!(matches!(err, RuntimeControlError::Precondition(_)));
    assert_eq!(mode.current(), ExecutionMode::Paper);
    assert_eq!(state.stored_mode(), None);
}

#[tokio::test]
async fn quiesce_timeout_fails_without_commit() {
    let (control, mode, exposure, state) = runtime_control(ExecutionMode::DryRun);
    let market = MarketId::new("m1");
    let _reservation = exposure
        .try_reserve_sync(
            &market,
            Usd::new(rust_decimal_macros::dec!(10)),
            std::time::Duration::from_secs(60),
        )
        .expect("reserve");

    let err = control
        .switch_execution_mode(ExecutionMode::Paper, "operator")
        .await
        .expect_err("quiesce should time out");

    assert!(matches!(err, RuntimeControlError::QuiesceTimeout { .. }));
    assert_eq!(mode.current(), ExecutionMode::DryRun);
    assert_eq!(state.stored_mode(), None);
}
