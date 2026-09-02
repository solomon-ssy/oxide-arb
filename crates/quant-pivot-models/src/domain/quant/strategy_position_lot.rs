//! Strategy position-lot persistence contracts.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    enums::{
        common::MarketCategory,
        execution::{ExitReason, PositionLedgerState, StrategyPositionOriginKind},
        quant::{AccountSource, OutcomeSide},
    },
    types::{
        AccountRecoveryIncidentId, EventId, ExecutionAccountId, MarketId, OrderIntentId, Price,
        Shares, StrategyPositionLotId, TokenId, Usd,
    },
};

/// Persisted current-position ledger row (one lot per filled entry intent).
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_strategy_position_lot::Entity")]
pub struct StrategyPositionLot {
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub origin_kind: StrategyPositionOriginKind,
    pub order_intent_id: Option<OrderIntentId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub execution_account_id: ExecutionAccountId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub state: PositionLedgerState,
    pub shares: Shares,
    pub avg_price: Price,
    pub cost_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub source: AccountSource,
    pub opened_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

info_from_model!(StrategyPositionLot, crate::entities::quant_strategy_position_lot::Model, {
    strategy_position_lot_id, origin_kind, order_intent_id, recovery_incident_id,
    execution_account_id, token_id, market_id, event_id, category, side,
    state, shares, avg_price, cost_usd, realized_pnl_usd, source, opened_at,
    updated_at, closed_at,
});

/// Insert payload for `quant_strategy_position_lot`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_strategy_position_lot::ActiveModel")]
pub struct NewStrategyPositionLot {
    pub strategy_position_lot_id: StrategyPositionLotId,
    pub origin_kind: StrategyPositionOriginKind,
    pub order_intent_id: Option<OrderIntentId>,
    pub recovery_incident_id: Option<AccountRecoveryIncidentId>,
    pub execution_account_id: ExecutionAccountId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub state: PositionLedgerState,
    pub shares: Shares,
    pub avg_price: Price,
    pub cost_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub source: AccountSource,
    pub opened_at: DateTime<Utc>,
    pub closed_at: Option<DateTime<Utc>>,
}

/// Fill fact used to upsert/open a per-intent position lot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionFill {
    pub order_intent_id: OrderIntentId,
    pub execution_account_id: ExecutionAccountId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub shares: Shares,
    pub price: Price,
    pub cost_usd: Usd,
    pub filled_at: DateTime<Utc>,
    pub source: AccountSource,
}

/// Venue-authoritative cumulative entry fill used by reconciliation.
///
/// Unlike [`PositionFill`], this is not an increment. The repository compares
/// it with the locked per-intent lot and applies only the missing cumulative
/// state, making repeated sweeps and crash recovery idempotent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CumulativePositionFill {
    pub order_intent_id: OrderIntentId,
    pub execution_account_id: ExecutionAccountId,
    pub token_id: TokenId,
    pub market_id: MarketId,
    pub event_id: Option<EventId>,
    pub category: MarketCategory,
    pub side: OutcomeSide,
    pub cumulative_shares: Shares,
    pub cumulative_cost_usd: Usd,
    pub observed_at: DateTime<Utc>,
    pub source: AccountSource,
}

/// Increment between the previously committed and newly observed cumulative
/// entry fill. Applying this delta preserves any exits already reflected in the
/// current remaining lot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionFillReconciliation {
    pub cumulative: CumulativePositionFill,
    pub shares_delta: Shares,
    pub cost_delta_usd: Usd,
}

/// Exit fact used to reduce or close an existing position.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionExit {
    pub shares: Shares,
    pub avg_price: Price,
    pub proceeds_usd: Usd,
    pub realized_pnl_usd: Usd,
    pub exited_at: DateTime<Utc>,
    pub reason: ExitReason,
}

/// Venue-authoritative cumulative exit state for one execution order.
///
/// Reconciliation compares this state with the previously committed summary
/// and applies only the delta. A later exact fee measurement may change
/// cumulative proceeds and realized `PnL` without changing cumulative shares.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CumulativePositionExit {
    pub cumulative_shares: Shares,
    pub avg_price: Price,
    pub cumulative_proceeds_usd: Usd,
    pub cumulative_realized_pnl_usd: Usd,
    pub observed_at: DateTime<Utc>,
    pub reason: ExitReason,
}

/// Delta derived from two cumulative exit states and applied to one lot.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionExitReconciliation {
    pub shares_delta: Shares,
    pub realized_pnl_delta_usd: Usd,
    pub observed_at: DateTime<Utc>,
    pub reason: ExitReason,
}
