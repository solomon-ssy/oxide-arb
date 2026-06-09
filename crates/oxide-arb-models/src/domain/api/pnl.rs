//! `PnL` API contract (outbound live snapshot view).

use crate::{domain::RiskEngineState, types::Usd};
use serde::Serialize;

/// Live in-memory `PnL` snapshot (current trading day), projected from the
/// risk-engine state so the wire contract is decoupled from the engine internals.
#[derive(Debug, Clone, Serialize)]
pub struct LivePnlView {
    pub daily_pnl: Usd,
    pub daily_loss_usd: Usd,
    pub total_exposure: Usd,
}

impl From<&RiskEngineState> for LivePnlView {
    fn from(state: &RiskEngineState) -> Self {
        Self {
            daily_pnl: state.daily_pnl,
            daily_loss_usd: state.daily_loss_usd,
            total_exposure: state.total_exposure,
        }
    }
}
