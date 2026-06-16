use crate::{
    bridge::execution_mode::ExecutionModeHandle,
    execution::{
        fsm::{EmergencyClass, ExecutionFSM},
        venue_guard::halt_trading_and_cancel_open_orders,
    },
};
use oxide_arb_api::clob::ClobClient;
use oxide_arb_error::OxideResult;
use oxide_arb_risk::engine::RiskEngine;
use std::sync::Arc;

/// Resume risk after operator ack; clears kill switch when trading is allowed again.
pub async fn resume_trading(
    risk: &RiskEngine,
    fsm: &ExecutionFSM,
    operator_ack: &str,
) -> OxideResult<()> {
    risk.acknowledge_and_resume(operator_ack).await?;
    if risk.allows_trading() && fsm.emergency_class().allows_auto_recover() {
        fsm.clear_emergency();
    }
    Ok(())
}

/// Align kill switch with current breaker state (e.g. after L2 trip without halt).
pub fn sync_kill_switch(risk: &RiskEngine, fsm: &ExecutionFSM) {
    if risk.allows_trading() {
        if fsm.is_emergency() && fsm.emergency_class().allows_auto_recover() {
            fsm.clear_emergency();
        }
    } else if !fsm.is_emergency() {
        fsm.enter_emergency(
            EmergencyClass::VenueFault,
            "risk engine blocking new trades",
        );
    }
}

/// Shared handles for halt/resume from HTTP/admin hooks.
pub struct TradingGate {
    risk: Arc<RiskEngine>,
    fsm: Arc<ExecutionFSM>,
    clob: Option<Arc<ClobClient>>,
    mode: ExecutionModeHandle,
}

impl TradingGate {
    pub fn new(
        risk: Arc<RiskEngine>,
        fsm: Arc<ExecutionFSM>,
        clob: Option<Arc<ClobClient>>,
        mode: ExecutionModeHandle,
    ) -> Self {
        Self {
            risk,
            fsm,
            clob,
            mode,
        }
    }

    pub async fn halt(&self, reason: String) {
        halt_trading_and_cancel_open_orders(
            self.mode.current(),
            self.clob.as_deref(),
            &self.risk,
            &self.fsm,
            reason,
            EmergencyClass::VenueFault,
        )
        .await;
    }

    pub async fn resume(&self, operator_ack: &str) -> OxideResult<()> {
        resume_trading(&self.risk, &self.fsm, operator_ack).await
    }

    pub fn sync(&self) {
        sync_kill_switch(&self.risk, &self.fsm);
    }
}
