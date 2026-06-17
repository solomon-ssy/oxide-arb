//! Periodic venue connectivity probe — feeds execution health into the risk engine.

use crate::{
    bridge::execution_mode::ExecutionModeHandle,
    control::factor_snapshot::FactorSnapshotStore,
    execution::fsm::ExecutionFSM,
    observability::{
        alert_dispatcher::{Alert, AlertDispatcher},
        metrics_hub::MetricsHub,
    },
    trade_integrity::TradeIntegrityStore,
};
use chrono::Utc;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideError;
use oxide_arb_models::enums::common::{AlertCategory, AlertLevel, AlertSource, ExecutionMode};
use oxide_arb_risk::{engine::RiskEngine, types::ExecutionRiskEvent};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct HeartbeatTaskConfig {
    pub clob_client: Arc<ClobClient>,
    pub risk_engine: Arc<RiskEngine>,
    pub fsm: Arc<ExecutionFSM>,
    pub integrity: Arc<TradeIntegrityStore>,
    pub factor_store: Arc<FactorSnapshotStore>,
    pub alerts: Arc<AlertDispatcher>,
    pub metrics: Arc<MetricsHub>,
    pub interval_secs: u64,
    pub shutdown: CancellationToken,
    pub mode: ExecutionModeHandle,
}

pub struct HeartbeatTask {
    clob_client: Arc<ClobClient>,
    risk_engine: Arc<RiskEngine>,
    fsm: Arc<ExecutionFSM>,
    integrity: Arc<TradeIntegrityStore>,
    factor_store: Arc<FactorSnapshotStore>,
    alerts: Arc<AlertDispatcher>,
    metrics: Arc<MetricsHub>,
    interval_secs: u64,
    shutdown: CancellationToken,
    mode: ExecutionModeHandle,
}

impl HeartbeatTask {
    pub fn new(config: HeartbeatTaskConfig) -> Self {
        Self {
            clob_client: config.clob_client,
            risk_engine: config.risk_engine,
            fsm: config.fsm,
            integrity: config.integrity,
            factor_store: config.factor_store,
            alerts: config.alerts,
            metrics: config.metrics,
            interval_secs: config.interval_secs.max(1),
            shutdown: config.shutdown,
            mode: config.mode,
        }
    }

    pub async fn run(self) -> Result<(), OxideError> {
        let mut ticker = tokio::time::interval(Duration::from_secs(self.interval_secs));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await;

        loop {
            tokio::select! {
                () = self.shutdown.cancelled() => {
                    tracing::info!("heartbeat task shutting down");
                    return Ok(());
                }
                _ = ticker.tick() => {
                    if self.mode.current() != ExecutionMode::Live {
                        continue;
                    }
                    warn_live_without_publication(
                        &self.factor_store,
                        &self.alerts,
                        &self.metrics,
                    );
                    if let Err(error) = self.integrity.refresh_async().await {
                        tracing::warn!(%error, "integrity snapshot refresh failed during heartbeat");
                    }
                    match self.clob_client.collateral_balance().await {
                        Ok(_) => {
                            tracing::debug!("heartbeat OK");
                            self.risk_engine
                                .on_execution_event(ExecutionRiskEvent::HeartbeatSuccess);
                            let _ = self
                                .fsm
                                .try_auto_recover(&self.integrity, &self.risk_engine);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "heartbeat failed");
                            self.risk_engine
                                .on_execution_event(ExecutionRiskEvent::HeartbeatFailure);
                            if !self.risk_engine.allows_trading() && !self.fsm.is_emergency() {
                                self.fsm.enter_emergency(
                                    crate::execution::fsm::EmergencyClass::VenueFault,
                                    "risk circuit breaker blocking trades",
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}

fn warn_live_without_publication(
    factor_store: &FactorSnapshotStore,
    alerts: &Arc<AlertDispatcher>,
    metrics: &MetricsHub,
) {
    let published = factor_store.published();
    let active = published.publication_id.is_some();
    metrics
        .control_factor_publication_active
        .set(i64::from(active));
    if active {
        return;
    }
    let alert = Alert::new(
        "control_factor.no_publication_live",
        AlertLevel::Warning,
        AlertCategory::OperatorNotice,
        AlertSource::System,
        "Live mode without an active control-factor publication",
        "Trading continues under neutral-pass factor checks — publish a control-factor snapshot for governed Live tuning",
        Utc::now(),
    )
    .with_affects_trading(false)
    .with_dedupe_secs(3600);
    alerts.dispatch_background(alert);
}
