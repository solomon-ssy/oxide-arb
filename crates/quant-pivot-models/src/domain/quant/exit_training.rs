//! Closed-lot projections for `ExitDecision` training.

use chrono::{DateTime, Utc};

use crate::{
    enums::quant::OutcomeSide,
    types::{MarketId, OrderIntentId, Price, Shares, StrategyPositionLotId, TokenId, Usd},
};

/// One realized exit fill on a lot timeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LotExitEventRow {
    /// When the exit fill settled.
    pub at: DateTime<Utc>,
    /// Shares sold.
    pub shares: Shares,
    /// Net proceeds from the exit (after fee, when known).
    pub net_proceeds: Usd,
}

/// Closed/settled lot eligible for hold-vs-exit training.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExitTrainingLotRow {
    pub order_intent_id: OrderIntentId,
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub market_id: MarketId,
    pub token_id: TokenId,
    pub outcome_side: OutcomeSide,
    pub opened_at: DateTime<Utc>,
    pub closed_at: DateTime<Utc>,
    pub entry_shares: Shares,
    pub avg_price: Price,
    pub peak_mark_price: Option<Price>,
    pub max_hold_secs: u64,
    pub total_net_proceeds: Usd,
    pub exit_events: Vec<LotExitEventRow>,
}
