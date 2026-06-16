//! Halt trading and best-effort cancel all open CLOB orders (Live safety net).

use crate::execution::fsm::{EmergencyClass, ExecutionFSM};
use oxide_arb_api::clob::ClobClient;
use oxide_arb_models::enums::common::ExecutionMode;
use oxide_arb_risk::engine::RiskEngine;

/// Engage risk halt + execution kill switch and cancel open venue orders in Live.
pub async fn halt_trading_and_cancel_open_orders(
    mode: ExecutionMode,
    clob: Option<&ClobClient>,
    risk: &RiskEngine,
    fsm: &ExecutionFSM,
    reason: String,
    class: EmergencyClass,
) {
    risk.halt(reason.clone()).await;
    fsm.enter_emergency(class, &reason);
    if mode != ExecutionMode::Live {
        return;
    }
    let Some(clob) = clob else {
        return;
    };
    fsm.record_venue_cancel_all();
    if let Err(error) = clob.cancel_all().await {
        tracing::warn!(%error, "cancel_all failed during emergency halt");
    }
}
