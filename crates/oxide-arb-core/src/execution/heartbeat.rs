//! Periodic venue connectivity probe — feeds execution health into the risk engine.

use crate::{
    bridge::execution_mode::ExecutionModeHandle,
    execution::{fsm::ExecutionFSM, trade_safety_gate::TradeSafetyGate},
};
use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideError;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_risk::{engine::RiskEngine, types::ExecutionRiskEvent};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

pub struct HeartbeatTask {
    clob_client: Arc<ClobClient>,
    risk_engine: Arc<RiskEngine>,
    fsm: Arc<ExecutionFSM>,
    trade_safety_gate: Arc<TradeSafetyGate>,
    interval_secs: u64,
    shutdown: CancellationToken,
    mode: ExecutionModeHandle,
}

impl HeartbeatTask {
    pub fn new(
        clob_client: Arc<ClobClient>,
        risk_engine: Arc<RiskEngine>,
        fsm: Arc<ExecutionFSM>,
        trade_safety_gate: Arc<TradeSafetyGate>,
        interval_secs: u64,
        shutdown: CancellationToken,
        mode: ExecutionModeHandle,
    ) -> Self {
        Self {
            clob_client,
            risk_engine,
            fsm,
            trade_safety_gate,
            interval_secs: interval_secs.max(1),
            shutdown,
            mode,
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
                    match self.clob_client.collateral_balance().await {
                        Ok(_) => {
                            tracing::debug!("heartbeat OK");
                            self.risk_engine
                                .on_execution_event(ExecutionRiskEvent::HeartbeatSuccess);
                            let _ = self
                                .fsm
                                .try_auto_recover(&self.trade_safety_gate, &self.risk_engine)
                                .await;
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
