//! Periodic venue connectivity probe — feeds execution health into the risk engine.

use std::sync::Arc;
use std::time::Duration;

use crate::execution::fsm::ExecutionFSM;
use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideError;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_risk::engine::RiskEngine;
use oxide_arb_risk::types::ExecutionRiskEvent;
use tokio_util::sync::CancellationToken;

pub struct HeartbeatTask {
    clob_client: Arc<ClobClient>,
    risk_engine: Arc<RiskEngine>,
    fsm: Arc<ExecutionFSM>,
    interval_secs: u64,
    shutdown: CancellationToken,
    execution_mode: ExecutionMode,
}

impl HeartbeatTask {
    pub fn new(
        clob_client: Arc<ClobClient>,
        risk_engine: Arc<RiskEngine>,
        fsm: Arc<ExecutionFSM>,
        interval_secs: u64,
        shutdown: CancellationToken,
        execution_mode: ExecutionMode,
    ) -> Self {
        Self {
            clob_client,
            risk_engine,
            fsm,
            interval_secs: interval_secs.max(1),
            shutdown,
            execution_mode,
        }
    }

    pub async fn run(self) -> Result<(), OxideError> {
        if self.execution_mode != ExecutionMode::Live {
            tracing::debug!("heartbeat task disabled outside Live mode");
            self.shutdown.cancelled().await;
            return Ok(());
        }

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
                    match self.clob_client.collateral_balance().await {
                        Ok(_) => {
                            tracing::debug!("heartbeat OK");
                            self.risk_engine
                                .on_execution_event(ExecutionRiskEvent::HeartbeatSuccess);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "heartbeat failed");
                            self.risk_engine
                                .on_execution_event(ExecutionRiskEvent::HeartbeatFailure);
                            if !self.risk_engine.allows_trading() && !self.fsm.is_emergency() {
                                self.fsm
                                    .enter_emergency("risk circuit breaker blocking trades");
                            }
                        }
                    }
                }
            }
        }
    }
}
