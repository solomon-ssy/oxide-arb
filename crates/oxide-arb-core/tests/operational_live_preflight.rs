//! Live preflight operational-phase gate — validates `build_system_status` inputs
//! used by `CoreRuntimeControl::preflight` before mode commit.

mod common;

use chrono::Utc;
use common::disconnected_metrics_refresh;
use oxide_arb_api::ws::ClobWsManager;
use oxide_arb_core::{
    bridge::{execution_mode::ExecutionModeHandle, risk_metrics::CoreRiskMetrics},
    control::{
        factor_snapshot::FactorSnapshotStore, mode_transition::CoreRuntimeControlDeps,
        status::build_system_status,
    },
    execution::{capital_manager::CapitalManager, fsm::ExecutionFSM},
    exposure::in_memory::InMemoryExposureReservation,
    observability::{alert_dispatcher::AlertDispatcher, metrics_hub::MetricsHub},
    pipeline::market_registry::MarketRegistry,
    runtime_config::RuntimeConfigStore,
    service::{
        catalog_readiness::CatalogReadiness,
        detection_readiness::DetectionReadiness,
        risk_metrics::{ApiHealthTracker, RiskMetricsState},
        runtime_lifecycle::LatestUnhealthySubsystems,
    },
    trade_integrity::TradeIntegrityStore,
};
use oxide_arb_models::{
    config::{DeployConfig, PolymarketConfig, WebSocketConfig},
    domain::OperationalPhase,
    enums::common::ExecutionMode,
    runtime_config::{NotificationConfig, RuntimeConfig},
    types::Usd,
};
use oxide_arb_repository::traits::TradeRepository;
use oxide_arb_risk::builder::RiskEngineBuilder;
use oxide_arb_test_support::{
    mocks::{MockPositionRepository, MockTradeRepository},
    risk::TestRiskMetrics,
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

fn control_deps(catalog: Arc<CatalogReadiness>, ws: Arc<ClobWsManager>) -> CoreRuntimeControlDeps {
    let mode = ExecutionMode::Paper;
    let metrics_hub = Arc::new(MetricsHub::new());
    let metrics_state = Arc::new(RiskMetricsState::new(Arc::new(ApiHealthTracker::new(
        Duration::from_secs(60),
    ))));
    metrics_state.seed_simulated_snapshot(mode, Usd::new(rust_decimal_macros::dec!(1000)));
    let runtime_config = Arc::new(RuntimeConfigStore::new(RuntimeConfig::default()));
    let exposure = Arc::new(InMemoryExposureReservation::new(
        RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let execution_mode = ExecutionModeHandle::new(mode);
    let market_registry = Arc::new(MarketRegistry::new());
    let risk_metrics = Arc::new(CoreRiskMetrics::new(
        Arc::clone(&metrics_state),
        Arc::clone(&exposure),
        market_registry,
        Arc::clone(&ws),
        execution_mode.clone(),
    ));
    let risk_engine = Arc::new(
        RiskEngineBuilder::new()
            .build(&TestRiskMetrics)
            .expect("risk engine"),
    );
    let alerts = Arc::new(AlertDispatcher::new(&NotificationConfig::default()));
    let fsm = Arc::new(ExecutionFSM::new(
        Arc::clone(&metrics_hub),
        Arc::clone(&alerts),
    ));
    let capital_manager = Arc::new(CapitalManager::new(
        Arc::clone(&exposure),
        &RuntimeConfig::default().risk.exposure_reservation_config(),
    ));
    let factor_store = Arc::new(FactorSnapshotStore::new(Utc::now()));
    let trade_integrity = Arc::new(TradeIntegrityStore::new(
        Arc::new(MockTradeRepository::default()) as Arc<dyn TradeRepository>,
        Arc::clone(&exposure),
        Arc::clone(&fsm),
        Arc::clone(&runtime_config),
        Arc::clone(&alerts),
    ));
    CoreRuntimeControlDeps {
        execution_mode,
        risk_engine,
        catalog,
        fsm,
        exposure,
        metrics: risk_metrics,
        metrics_state: Arc::clone(&metrics_state),
        metrics_refresh: disconnected_metrics_refresh(
            metrics_state,
            mode,
            Arc::clone(&metrics_hub),
        ),
        clob_client: None,
        ctf_redeem: None,
        holder_address: "unavailable".into(),
        market_registry: Arc::new(MarketRegistry::new()),
        ws_manager: ws,
        unhealthy_subsystems: Arc::new(LatestUnhealthySubsystems::default()),
        health_checker: None,
        deploy: Arc::new(DeployConfig::default()),
        runtime_config,
        position_repo: Arc::new(MockPositionRepository::default()),
        system_runtime_state: Arc::new(MockSystemRuntimeState::new()),
        trade_repo: Arc::new(MockTradeRepository::default()),
        capital_manager,
        trade_integrity,
        factor_store,
        alerts,
        detection_readiness: Arc::new(DetectionReadiness::default()),
        status_publisher: None,
    }
}

struct MockSystemRuntimeState;

impl MockSystemRuntimeState {
    const fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl oxide_arb_repository::traits::SystemRuntimeStateRepository for MockSystemRuntimeState {
    async fn load(
        &self,
    ) -> Result<
        Option<oxide_arb_models::domain::SystemRuntimeStateInfo>,
        oxide_arb_error::storage::StorageError,
    > {
        Ok(None)
    }

    async fn upsert_execution_mode(
        &self,
        _mode: ExecutionMode,
        _changed_by: &str,
        _reason: &str,
    ) -> Result<(), oxide_arb_error::storage::StorageError> {
        Ok(())
    }
}

fn ws_manager() -> Arc<ClobWsManager> {
    Arc::new(ClobWsManager::new(
        &PolymarketConfig::default(),
        &WebSocketConfig::default(),
        CancellationToken::new(),
        None,
        None,
    ))
}

#[test]
fn live_gate_rejects_catalog_warming() {
    let catalog = Arc::new(CatalogReadiness::new());
    let ws = ws_manager();
    let deps = control_deps(catalog, ws);
    let status = build_system_status(&deps, Instant::now());

    assert_eq!(status.operational_phase, OperationalPhase::CatalogWarming);
    assert!(!status.operational_phase.allows_live_trading());
}

#[test]
fn live_gate_rejects_market_data_connecting() {
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(10, Utc::now());
    let ws = ws_manager();
    let deps = control_deps(catalog, ws);
    let status = build_system_status(&deps, Instant::now());

    assert_eq!(
        status.operational_phase,
        OperationalPhase::MarketDataConnecting
    );
    assert!(!status.market_data.ready);
    assert!(!status.operational_phase.allows_live_trading());
}

#[test]
fn live_gate_accepts_operational() {
    let catalog = Arc::new(CatalogReadiness::new());
    catalog.mark_ready(10, Utc::now());
    let ws = ws_manager();
    ws.seed_test_connectivity();
    let deps = control_deps(catalog, ws);
    let status = build_system_status(&deps, Instant::now());

    assert_eq!(status.operational_phase, OperationalPhase::Operational);
    assert!(status.market_data.ready);
    assert!(status.operational_phase.allows_live_trading());
}
