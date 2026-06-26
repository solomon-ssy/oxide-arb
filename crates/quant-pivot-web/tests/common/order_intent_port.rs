//! Real [`CoreOrderIntentService`] wiring for web integration tests (Phase 05.2).

use std::sync::Arc;

use quant_pivot_core::{
    execution::{
        CoreOrderIntentService, DefaultRuntimeModeGate, OrderIntentServiceDeps, RuntimeModeGate,
    },
    governance::{KillSwitchHandle, RuntimeModeHandle},
    runtime_config::RuntimeConfigStore,
};
use quant_pivot_models::{
    domain::CoreEventPublisher,
    enums::{execution::KillSwitchState, quant::QuantRuntimeMode},
    runtime_config::RuntimeConfig,
};
use quant_pivot_repository::{
    postgres::{
        PgOrderIntentRepository, PgRecommendationReportRepository, PgRecommendationRepository,
        PgRuntimeConfigVersionRepository,
    },
    traits::{
        OrderIntentRepository, RecommendationReportRepository, RecommendationRepository,
        RuntimeConfigVersionRepository,
    },
};
use sea_orm::DatabaseConnection;

/// Assemble a real [`CoreOrderIntentService`] over the test Postgres connection.
///
/// The mode gate runs against a default `SemiAuto` runtime mode / `Closed` kill
/// switch — the RBAC and routing tests do not depend on the hot governance
/// state (the money flow is covered by the repository + unit tests).
pub fn build_order_intent_service(
    db: &DatabaseConnection,
    events: CoreEventPublisher,
) -> Arc<CoreOrderIntentService> {
    let runtime_mode = RuntimeModeHandle::new(QuantRuntimeMode::SemiAuto);
    let kill_switch = KillSwitchHandle::new(KillSwitchState::Closed);
    let config = Arc::new(RuntimeConfigStore::new(RuntimeConfig::default()));
    let mode_gate: Arc<dyn RuntimeModeGate> =
        Arc::new(DefaultRuntimeModeGate::new(Arc::clone(&config)));
    Arc::new(CoreOrderIntentService::new(OrderIntentServiceDeps {
        mode_gate,
        runtime_mode,
        kill_switch,
        recommendations: Arc::new(PgRecommendationRepository::new(db.clone()))
            as Arc<dyn RecommendationRepository>,
        reports: Arc::new(PgRecommendationReportRepository::new(db.clone()))
            as Arc<dyn RecommendationReportRepository>,
        intents: Arc::new(PgOrderIntentRepository::new(db.clone()))
            as Arc<dyn OrderIntentRepository>,
        config,
        config_versions: Arc::new(PgRuntimeConfigVersionRepository::new(db.clone()))
            as Arc<dyn RuntimeConfigVersionRepository>,
        events,
    }))
}
