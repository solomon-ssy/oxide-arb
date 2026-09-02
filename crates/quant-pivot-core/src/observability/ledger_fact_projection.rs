//! Shared projections from Postgres ledger rows to `ClickHouse` fact rows.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    clickhouse::{
        ChPrice, ChShares, ChUsd, QuantCapitalAllocationEventRow, QuantExecutionEventRow,
        QuantPositionEventRow,
    },
    domain::quant::{CapitalAllocationInfo, ExecutionOrderInfo, StrategyPositionLot},
    enums::clickhouse::ChQuantLedgerEventKind,
    types::RecommendationId,
};

/// Project one execution-order lifecycle event.
#[must_use]
pub fn project_execution_event(
    order: &ExecutionOrderInfo,
    recommendation_id: RecommendationId,
    event_kind: ChQuantLedgerEventKind,
    event_time: DateTime<Utc>,
) -> QuantExecutionEventRow {
    QuantExecutionEventRow {
        event_time: event_time.timestamp_millis(),
        order_intent_id: order.order_intent_id,
        execution_order_id: order.execution_order_id,
        recommendation_id,
        event_kind,
        market_id: order.market_id.clone(),
        token_id: order.token_id.clone(),
        side: order.side.into(),
        price: ChPrice::from(order.price),
        shares: ChShares::from(order.shares),
        cost_usd: ChUsd::from(order.cost_usd),
        venue_order_id: order.venue_order_id.clone(),
        ingestion_time: Utc::now().timestamp_millis(),
    }
}

/// Project one capital-allocation ledger event.
#[must_use]
pub fn project_capital_event(
    capital: &CapitalAllocationInfo,
    event_kind: ChQuantLedgerEventKind,
    event_time: DateTime<Utc>,
) -> QuantCapitalAllocationEventRow {
    QuantCapitalAllocationEventRow {
        event_time: event_time.timestamp_millis(),
        capital_allocation_id: capital.capital_allocation_id,
        order_intent_id: capital.order_intent_id,
        recommendation_id: capital.recommendation_id,
        event_kind,
        state: capital.state.into(),
        allocated_usd: ChUsd::from(capital.allocated_usd),
        locked_usd: ChUsd::from(capital.locked_usd),
        spent_usd: ChUsd::from(capital.spent_usd),
        released_usd: ChUsd::from(capital.released_usd),
        ingestion_time: Utc::now().timestamp_millis(),
    }
}

/// Project one position-lot ledger event.
#[must_use]
pub fn project_position_event(
    position: &StrategyPositionLot,
    event_kind: ChQuantLedgerEventKind,
    event_time: DateTime<Utc>,
) -> QuantPositionEventRow {
    QuantPositionEventRow {
        event_time: event_time.timestamp_millis(),
        strategy_position_lot_id: position.strategy_position_lot_id,
        origin_kind: position.origin_kind.into(),
        order_intent_id: position.order_intent_id,
        recovery_incident_id: position.recovery_incident_id,
        market_id: position.market_id.clone(),
        token_id: position.token_id.clone(),
        event_kind,
        state: position.state.into(),
        side: position.side.into(),
        shares: ChShares::from(position.shares),
        avg_price: ChPrice::from(position.avg_price),
        cost_usd: ChUsd::from(position.cost_usd),
        realized_pnl_usd: ChUsd::from(position.realized_pnl_usd),
        ingestion_time: Utc::now().timestamp_millis(),
    }
}

#[cfg(test)]
mod tests {
    use quant_pivot_models::{
        enums::{
            clickhouse::{ChExecutionSide, ChPositionLedgerState, ChQuantLedgerEventKind},
            common::{MarketCategory, OrderType, Side},
            execution::{
                ExecutionOrderPhase, OrderTypeKind, PositionLedgerState, StrategyPositionOriginKind,
            },
            quant::{AccountSource, ExecutionOrderState, OutcomeSide},
        },
        types::{
            ExecutionAccountId, ExecutionOrderId, MarketId, OrderIntentId, Price, Shares,
            StrategyPositionLotId, TokenId, Usd, VenueOrderAmount,
        },
    };
    use rust_decimal_macros::dec;

    use super::*;
    use crate::test_fixtures::execution_pg_seed::PreparedOrderFixture;

    #[test]
    fn execution_projection_maps_fields() {
        let now = Utc::now();
        let order = ExecutionOrderInfo {
            execution_order_id: ExecutionOrderId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
            order_phase: ExecutionOrderPhase::Entry,
            market_id: MarketId::from("m1"),
            token_id: TokenId::from("t1"),
            side: Side::Buy,
            order_type: OrderTypeKind::Fok,
            price: Price::new(dec!(0.55)),
            shares: Shares::new(dec!(10)),
            cost_usd: Usd::new(dec!(5.5)),
            prepared_order_json: PreparedOrderFixture {
                market_id: MarketId::from("m1"),
                token_id: TokenId::from("t1"),
                side: Side::Buy,
                order_type: OrderType::Fok,
                venue_amount: VenueOrderAmount::PrincipalUsd(Usd::new(dec!(5.4))),
                expected_fee: Usd::new(dec!(0.1)),
                expected_filled_shares: Shares::new(dec!(10)),
                limit_price: Price::new(dec!(0.55)),
            }
            .build(),
            venue_order_id: None,
            venue_status: None,
            state: ExecutionOrderState::Submitted,
            submitted_at: Some(now),
            filled_at: None,
            cancelled_at: None,
            gtd_expiration_at: None,
            error_message: None,
            created_at: now,
            updated_at: now,
        };
        let row = project_execution_event(
            &order,
            RecommendationId::from_v7(),
            ChQuantLedgerEventKind::Submitted,
            now,
        );
        assert_eq!(row.event_kind, ChQuantLedgerEventKind::Submitted);
        assert_eq!(row.side, ChExecutionSide::Buy);
    }

    #[test]
    fn position_projection_maps_side() {
        let now = Utc::now();
        let position = StrategyPositionLot {
            strategy_position_lot_id: StrategyPositionLotId::from_v7(),
            origin_kind: StrategyPositionOriginKind::SystemIntent,
            order_intent_id: Some(OrderIntentId::from_v7()),
            recovery_incident_id: None,
            execution_account_id: ExecutionAccountId::from_v7(),
            token_id: TokenId::from("t1"),
            market_id: MarketId::from("m1"),
            event_id: None,
            category: MarketCategory::Politics,
            side: OutcomeSide::Yes,
            state: PositionLedgerState::Open,
            shares: Shares::new(dec!(10)),
            avg_price: Price::new(dec!(0.5)),
            cost_usd: Usd::new(dec!(5)),
            realized_pnl_usd: Usd::ZERO,
            source: AccountSource::Polymarket,
            opened_at: now,
            updated_at: now,
            closed_at: None,
        };
        let row = project_position_event(&position, ChQuantLedgerEventKind::Opened, now);
        assert_eq!(row.event_kind, ChQuantLedgerEventKind::Opened);
        assert_eq!(row.state, ChPositionLedgerState::Open);
    }
}
