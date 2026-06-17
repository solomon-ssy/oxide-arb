//! Risk engine and metrics wiring phase.

use super::types::{
    BuildClients, BuildInfra, BuildRisk, BuildRiskParts, DetectionStack, WiringConfig,
};
use crate::{
    bridge::{
        potential_loss_store::CorePotentialLossStore, risk_metrics::CoreRiskMetrics,
        risk_persistence::CoreRiskPersistence,
    },
    exposure::in_memory::InMemoryExposureReservation,
    observability::backpressure::BackpressurePolicy,
    service::{
        equity_valuator::EquityValuator,
        risk_metrics::{
            ApiHealthTracker, RiskMetricsRefreshDeps, RiskMetricsRefreshService, RiskMetricsState,
        },
    },
};
use oxide_arb_error::OxideResult;
use oxide_arb_models::{
    domain::{CoreEventPublisher, risk::RiskEngineState},
    enums::common::ExecutionMode,
};
use oxide_arb_repository::traits::{
    BlacklistPersistenceRepository, PotentialLossRepository, RiskStateRepository,
};
use oxide_arb_risk::{
    audit_sink::AuditSink, builder::RiskEngineBuilder, clock::utc_clock, traits::PotentialLossStore,
};
use std::{sync::Arc, time::Duration};

use crate::bridge::execution_mode::ExecutionModeHandle;
use crate::execution::fsm::ExecutionFSM;

impl BuildRisk {
    pub(super) async fn wire(
        wiring: WiringConfig<'_>,
        execution_mode: &ExecutionModeHandle,
        infra: &BuildInfra,
        clients: &BuildClients,
        events: &CoreEventPublisher,
        detection: &DetectionStack,
    ) -> OxideResult<Self> {
        let deploy = wiring.deploy();
        let runtime = wiring.runtime();
        let exposure = Arc::new(InMemoryExposureReservation::new(
            runtime.risk.exposure_reservation_config(),
        ));
        let api_tracker = Arc::new(ApiHealthTracker::new(Duration::from_secs(60)));
        let metrics_state = Arc::new(RiskMetricsState::new(api_tracker));
        let equity_valuator = Arc::new(EquityValuator::new(
            Arc::clone(detection.market_registry()),
            Arc::clone(detection.book_store()),
            Arc::clone(detection.calibrator()),
        ));
        let metrics_refresh = Arc::new(RiskMetricsRefreshService::new(RiskMetricsRefreshDeps {
            state: Arc::clone(&metrics_state),
            execution_mode: execution_mode.clone(),
            runtime_config: Arc::clone(infra.runtime_store()),
            clob_client: clients.clob_client().map(Arc::clone),
            trade_repo: Arc::clone(infra.repos().trade()),
            position_repo: Arc::clone(infra.repos().position()),
            equity_valuator,
            metrics: Arc::clone(infra.metrics()),
        }));
        if execution_mode.current() != ExecutionMode::Live {
            metrics_refresh.refresh().await?;
        }
        let risk_metrics = Arc::new(CoreRiskMetrics::new(
            Arc::clone(&metrics_state),
            Arc::clone(&exposure),
            Arc::clone(clients.ws_manager()),
            execution_mode.clone(),
        ));

        let risk_persistence = Arc::new(CoreRiskPersistence::new(
            Arc::clone(infra.repos().risk_state()),
            Arc::clone(infra.repos().blacklist()),
            Arc::clone(infra.repos().audit()),
            Arc::clone(infra.repos().risk_fill()),
            Arc::clone(infra.repos().emergency()),
            Arc::clone(infra.repos().reconciliation()),
        ));
        let risk_state_info = infra.repos().risk_state().load().await?;
        let blacklist = infra.repos().blacklist().load_active().await?;
        let potential_loss = infra.repos().potential_loss().find_active().await?;
        let audit_sink = Arc::clone(infra.risk_decision_audit());
        let audit_sink: Arc<dyn AuditSink> = audit_sink;
        let potential_loss_store = Arc::new(CorePotentialLossStore::new(Arc::clone(
            infra.repos().potential_loss(),
        )));

        let engine = Arc::new(
            RiskEngineBuilder::new()
                .config(runtime.risk.clone())
                .persistence(risk_persistence)
                .snapshot(RiskEngineState::from(&risk_state_info))
                .blacklist_entries(blacklist)
                .potential_loss_entries(potential_loss)
                .potential_loss_store(
                    Arc::clone(&potential_loss_store) as Arc<dyn PotentialLossStore>
                )
                .audit_sink(audit_sink)
                .event_publisher(events.clone())
                .clock(utc_clock())
                .build(risk_metrics.as_ref())?,
        );

        let fsm = Arc::new(ExecutionFSM::new(
            Arc::clone(infra.metrics()),
            Arc::clone(infra.alerts()),
        ));
        let backpressure = Arc::new(BackpressurePolicy::new(
            Arc::clone(infra.metrics()),
            deploy.execution.book_apply.shard_count.max(1),
        ));

        Ok(Self::assembled(BuildRiskParts {
            exposure,
            metrics: risk_metrics,
            metrics_state,
            engine,
            potential_loss_store,
            metrics_refresh,
            fsm,
            backpressure,
        }))
    }
}
