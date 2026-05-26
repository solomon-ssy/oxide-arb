//! Coordinates risk-engine halt/resume with the execution kill switch.

use std::sync::Arc;

use oxide_arb_error::OxideResult;
use oxide_arb_risk::engine::RiskEngine;

use crate::execution::fsm::ExecutionFSM;

/// Halt risk + engage execution kill switch atomically from the core layer.
pub async fn halt_trading(risk: &RiskEngine, fsm: &ExecutionFSM, reason: String) {
    risk.halt(reason.clone()).await;
    fsm.enter_emergency(&reason);
}

/// Resume risk after operator ack; clears kill switch when trading is allowed again.
pub async fn resume_trading(
    risk: &RiskEngine,
    fsm: &ExecutionFSM,
    operator_ack: &str,
) -> OxideResult<()> {
    risk.acknowledge_and_resume(operator_ack).await?;
    if risk.allows_trading() {
        fsm.clear_emergency();
    }
    Ok(())
}

/// Align kill switch with current breaker state (e.g. after L2 trip without halt).
pub fn sync_kill_switch(risk: &RiskEngine, fsm: &ExecutionFSM) {
    if risk.allows_trading() {
        if fsm.is_emergency() {
            fsm.clear_emergency();
        }
    } else if !fsm.is_emergency() {
        fsm.enter_emergency("risk engine blocking new trades");
    }
}

/// Shared handles for halt/resume from HTTP/admin hooks.
pub struct TradingGate {
    risk: Arc<RiskEngine>,
    fsm: Arc<ExecutionFSM>,
}

impl TradingGate {
    pub const fn new(risk: Arc<RiskEngine>, fsm: Arc<ExecutionFSM>) -> Self {
        Self { risk, fsm }
    }

    pub async fn halt(&self, reason: String) {
        halt_trading(&self.risk, &self.fsm, reason).await;
    }

    pub async fn resume(&self, operator_ack: &str) -> OxideResult<()> {
        resume_trading(&self.risk, &self.fsm, operator_ack).await
    }

    pub fn sync(&self) {
        sync_kill_switch(&self.risk, &self.fsm);
    }
}
