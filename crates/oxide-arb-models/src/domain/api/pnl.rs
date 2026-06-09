//! `PnL` API contract (outbound live snapshot view).

use crate::{domain::RiskEngineState, types::Usd};
use serde::Serialize;

/// Live in-memory `PnL` snapshot, projected from the risk-engine state so the
/// wire contract is decoupled from the engine internals.
///
/// `daily_pnl` is the current trading day's realized `PnL`; `total_realized_pnl`
/// is the lifetime cumulative realized `PnL` on the same accounting basis, so a
/// `sync` snapshot agrees with the `pnl.update` push.
#[derive(Debug, Clone, Serialize)]
pub struct LivePnlView {
    pub daily_pnl: Usd,
    pub daily_loss_usd: Usd,
    pub total_realized_pnl: Usd,
    pub total_exposure: Usd,
}

impl From<&RiskEngineState> for LivePnlView {
    fn from(state: &RiskEngineState) -> Self {
        Self {
            daily_pnl: state.daily_pnl,
            daily_loss_usd: state.daily_loss_usd,
            total_realized_pnl: state.total_realized_pnl,
            total_exposure: state.total_exposure,
        }
    }
}
