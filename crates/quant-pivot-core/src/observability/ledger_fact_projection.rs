//! Shared projections from Postgres ledger rows to `ClickHouse` fact rows.

use chrono::{DateTime, Utc};
use quant_pivot_models::{
    clickhouse::{
        ChPrice, ChShares, ChUsd, QuantCapitalAllocationEventRow, QuantExecutionEventRow,
        QuantPositionEventRow,
    },
    domain::{CapitalAllocationInfo, ExecutionOrderInfo, PositionInfo},
    types::RecommendationId,
};

/// Project one execution-order lifecycle event.
#[must_use]
pub fn project_execution_event(
    order: &ExecutionOrderInfo,
    recommendation_id: RecommendationId,
    event_kind: &str,
    event_time: DateTime<Utc>,
) -> QuantExecutionEventRow {
    QuantExecutionEventRow {
        event_time: event_time.timestamp_millis(),
        order_intent_id: order.order_intent_id.clone(),
        execution_order_id: order.execution_order_id.to_string(),
        recommendation_id,
        event_kind: event_kind.to_owned(),
        market_id: order.market_id.clone(),
        token_id: order.token_id.clone(),
        side: order.side.as_i8(),
        price: ChPrice::from(order.price),
        shares: ChShares::from(order.shares),
        cost_usd: ChUsd::from(order.cost_usd),
        venue_order_id: order
            .venue_order_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_default(),
        ingestion_time: Utc::now().timestamp_millis(),
    }
}

/// Project one capital-allocation ledger event.
#[must_use]
pub fn project_capital_event(
    capital: &CapitalAllocationInfo,
    event_kind: &str,
    event_time: DateTime<Utc>,
) -> QuantCapitalAllocationEventRow {
    QuantCapitalAllocationEventRow {
        event_time: event_time.timestamp_millis(),
        capital_allocation_id: capital.capital_allocation_id.clone(),
        order_intent_id: capital.order_intent_id.clone(),
        recommendation_id: capital.recommendation_id.clone(),
        event_kind: event_kind.to_owned(),
        state: capital.state.as_str().to_owned(),
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
    position: &PositionInfo,
    event_kind: &str,
    event_time: DateTime<Utc>,
) -> QuantPositionEventRow {
    QuantPositionEventRow {
        event_time: event_time.timestamp_millis(),
        position_id: position.position_id.clone(),
        order_intent_id: position.order_intent_id.clone(),
        market_id: position.market_id.clone(),
        token_id: position.token_id.clone(),
        event_kind: event_kind.to_owned(),
        state: position.state.as_str().to_owned(),
        side: position.side.as_str().to_owned(),
        shares: ChShares::from(position.shares),
        avg_price: ChPrice::from(position.avg_price),
        cost_usd: ChUsd::from(position.cost_usd),
        realized_pnl_usd: ChUsd::from(position.realized_pnl_usd),
        ingestion_time: Utc::now().timestamp_millis(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quant_pivot_models::{
        enums::{
            common::{MarketCategory, Side},
            execution::{ExecutionOrderPhase, OrderTypeKind, PositionLedgerState},
            quant::{AccountSource, ExecutionOrderState, OutcomeSide},
        },
        types::{
            ExecutionOrderId, MarketId, OrderIntentId, PositionId, Price, Shares, TokenId, Usd,
        },
    };
    use rust_decimal_macros::dec;

    #[test]
    fn execution_projection_maps_core_fields() {
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
        let row = project_execution_event(&order, RecommendationId::from_v7(), "submitted", now);
        assert_eq!(row.event_kind, "submitted");
        assert_eq!(row.side, Side::Buy.as_i8());
    }

    #[test]
    fn position_projection_maps_state_and_side() {
        let now = Utc::now();
        let position = PositionInfo {
            position_id: PositionId::from_v7(),
            order_intent_id: OrderIntentId::from_v7(),
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
        let row = project_position_event(&position, "opened", now);
        assert_eq!(row.event_kind, "opened");
        assert_eq!(row.state, PositionLedgerState::Open.as_str());
    }
}
