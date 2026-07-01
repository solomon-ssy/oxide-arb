//! Postgres-backed position ledger repository.
//!
//! Each row is **one lot per filled entry intent** (`order_intent_id` is the
//! lot's natural unique key). `apply_fill` weighted-averages only the fills of
//! the *same* intent, so a lot's `avg_price` is its exact realized cost — never
//! blended across intents. The per-token aggregate is a query view
//! (`find_lots_by_token`), not a single row.

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::PositionRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ExitTrainingLotRow, LotExitEventRow, NewPosition, Paginated, PositionExit, PositionFill,
        PositionInfo, PositionListQuery,
    },
    entities::{quant_execution_order, quant_order_intent, quant_position},
    enums::{
        execution::{ExecutionOrderPhase, PositionLedgerState},
        quant::ExecutionOrderState,
    },
    types::{MarketId, OrderIntentId, PositionId, Price, Shares, TokenId, Usd},
};
use rust_decimal::Decimal;
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, FromQueryResult, IntoActiveModel, QueryFilter, QueryOrder, QuerySelect,
    sea_query::Expr,
};

/// Position-lot states still considered "open" (subject to exit monitoring).
const OPEN_STATES: [PositionLedgerState; 2] =
    [PositionLedgerState::Open, PositionLedgerState::Closing];

const CLOSED_STATES: [PositionLedgerState; 2] =
    [PositionLedgerState::Closed, PositionLedgerState::Settled];

const DEFAULT_HOLD_HORIZON_SECS: u64 = 86_400;

/// Postgres-backed current-position ledger repository.
pub struct PgPositionRepository {
    db: DatabaseConnection,
}

impl PgPositionRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[derive(Debug, FromQueryResult)]
struct RealizedPnlSum {
    total: Option<Decimal>,
}

#[async_trait::async_trait]
impl PositionRepository for PgPositionRepository {
    async fn apply_fill(&self, fill: PositionFill) -> Result<PositionInfo, StorageError> {
        apply_fill(&self.db, fill).await
    }

    async fn apply_exit(
        &self,
        order_intent_id: &OrderIntentId,
        exit: PositionExit,
    ) -> Result<PositionInfo, StorageError> {
        apply_exit(&self.db, order_intent_id, exit).await
    }

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        quant_position::Entity::find()
            .filter(quant_position::Column::OrderIntentId.eq(order_intent_id.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_id(
        &self,
        position_id: &PositionId,
    ) -> Result<Option<PositionInfo>, StorageError> {
        quant_position::Entity::find_by_id(position_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: PositionListQuery,
    ) -> Result<Paginated<PositionInfo>, StorageError> {
        let query = query.normalized();
        paginate_mapped(
            quant_position::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_position::Column::OpenedAt),
            &self.db,
            &query.page,
            Into::into,
        )
        .await
    }

    async fn find_open_lots(&self) -> Result<Vec<PositionInfo>, StorageError> {
        quant_position::Entity::find()
            .filter(quant_position::Column::State.is_in(OPEN_STATES))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_lots_by_token(
        &self,
        token_id: &TokenId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        quant_position::Entity::find()
            .filter(quant_position::Column::TokenId.eq(token_id.clone()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_open_by_market(
        &self,
        market_id: &MarketId,
    ) -> Result<Vec<PositionInfo>, StorageError> {
        quant_position::Entity::find()
            .filter(quant_position::Column::MarketId.eq(market_id.clone()))
            .filter(quant_position::Column::State.is_in(OPEN_STATES))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn realized_pnl_cumulative_usd(&self) -> Result<Usd, StorageError> {
        let row = quant_position::Entity::find()
            .select_only()
            .column_as(Expr::cust("SUM(realized_pnl_usd)"), "total")
            .into_model::<RealizedPnlSum>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        Ok(Usd::new(
            row.and_then(|row| row.total).unwrap_or(Decimal::ZERO),
        ))
    }

    async fn find_exit_training_lots(
        &self,
        closed_from: chrono::DateTime<chrono::Utc>,
        closed_to: chrono::DateTime<chrono::Utc>,
        limit: u64,
    ) -> Result<Vec<ExitTrainingLotRow>, StorageError> {
        let rows = quant_position::Entity::find()
            .filter(quant_position::Column::State.is_in(CLOSED_STATES))
            .filter(quant_position::Column::ClosedAt.gte(closed_from))
            .filter(quant_position::Column::ClosedAt.lt(closed_to))
            .order_by_desc(quant_position::Column::ClosedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;

        let mut lots = Vec::with_capacity(rows.len());
        for row in rows {
            let position = PositionInfo::from(row);
            let Some(closed_at) = position.closed_at else {
                continue;
            };
            let Some(intent) =
                quant_order_intent::Entity::find_by_id(position.order_intent_id.clone())
                    .one(&self.db)
                    .await
                    .map_err(StorageError::from)?
            else {
                continue;
            };
            let orders = quant_execution_order::Entity::find()
                .filter(
                    quant_execution_order::Column::OrderIntentId
                        .eq(position.order_intent_id.clone()),
                )
                .all(&self.db)
                .await
                .map_err(StorageError::from)?;
            let Some(entry_shares) = resolve_entry_shares(&intent, &orders) else {
                continue;
            };
            let avg_price = resolve_entry_avg_price(&position, &orders);
            if avg_price.is_zero() {
                continue;
            }
            let exit_events = exit_events_from_orders(&orders);
            let entry_cost = avg_price.inner() * entry_shares.inner();
            let total_net_proceeds =
                Usd::new((entry_cost + position.realized_pnl_usd.inner()).max(Decimal::ZERO));
            lots.push(ExitTrainingLotRow {
                order_intent_id: position.order_intent_id,
                position_id: position.position_id,
                market_id: position.market_id,
                token_id: position.token_id,
                opened_at: position.opened_at,
                closed_at,
                entry_shares,
                avg_price,
                peak_mark_price: intent.peak_mark_price,
                max_hold_secs: intent
                    .exit_policy_json
                    .max_hold_secs
                    .unwrap_or(DEFAULT_HOLD_HORIZON_SECS),
                total_net_proceeds,
                exit_events,
            });
        }
        Ok(lots)
    }
}

/// Upsert the per-intent position lot from a fill (weighted-average cost over
/// the *same* intent's fills), on the caller's connection — usable inside the
/// execution-submission transaction so the position ledger and capital
/// settlement commit atomically.
pub async fn apply_fill(
    db: &impl ConnectionTrait,
    fill: PositionFill,
) -> Result<PositionInfo, StorageError> {
    if !fill.shares.is_positive() || fill.cost_usd.is_negative() {
        return Err(error::invariant_violation(
            Some(entity::QUANT_POSITION),
            "position fill must have positive shares and non-negative cost",
        ));
    }

    let existing = quant_position::Entity::find()
        .filter(quant_position::Column::OrderIntentId.eq(fill.order_intent_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?;

    let Some(row) = existing else {
        return quant_position::Entity::insert(
            NewPosition {
                position_id: PositionId::from_v7(),
                order_intent_id: fill.order_intent_id,
                token_id: fill.token_id,
                market_id: fill.market_id,
                event_id: fill.event_id,
                category: fill.category,
                side: fill.side,
                state: PositionLedgerState::Open,
                shares: fill.shares,
                avg_price: fill.price,
                cost_usd: fill.cost_usd,
                realized_pnl_usd: Usd::ZERO,
                source: fill.source,
                opened_at: fill.filled_at,
                closed_at: None,
            }
            .into_active_model(),
        )
        .exec_with_returning(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into);
    };

    if row.state == PositionLedgerState::Closed || row.state == PositionLedgerState::Settled {
        return Err(error::state_conflict(
            entity::QUANT_POSITION,
            Some(&row.order_intent_id),
            "cannot apply fill to closed position lot",
        ));
    }

    let shares = row.shares + fill.shares;
    let cost_usd = row.cost_usd + fill.cost_usd;
    let avg_price = price_from_cost_and_shares(cost_usd, shares)?;
    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(PositionLedgerState::Open);
    active.shares = ActiveValue::Set(shares);
    active.avg_price = ActiveValue::Set(avg_price);
    active.cost_usd = ActiveValue::Set(cost_usd);
    active.source = ActiveValue::Set(fill.source);
    active.updated_at = ActiveValue::Set(fill.filled_at);
    active.closed_at = ActiveValue::Set(None);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

/// Reduce or close the per-intent position lot from an exit fill, on the
/// caller's connection. Average-cost: a partial exit leaves `avg_price`
/// unchanged (the realized cost basis of the remaining shares is preserved);
/// the caller-supplied `realized_pnl_usd` is accumulated.
pub async fn apply_exit(
    db: &impl ConnectionTrait,
    order_intent_id: &OrderIntentId,
    exit: PositionExit,
) -> Result<PositionInfo, StorageError> {
    if !exit.shares.is_positive() {
        return Err(error::invariant_violation(
            Some(entity::QUANT_POSITION),
            "position exit must have positive shares",
        ));
    }

    let Some(row) = quant_position::Entity::find()
        .filter(quant_position::Column::OrderIntentId.eq(order_intent_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(entity::QUANT_POSITION, order_intent_id));
    };

    if exit.shares > row.shares {
        return Err(error::invariant_violation(
            Some(entity::QUANT_POSITION),
            format!(
                "exit shares exceed position shares for intent {order_intent_id}: {} > {}",
                exit.shares, row.shares
            ),
        ));
    }

    let cost_reduction = row.avg_price * exit.shares;
    let shares = row.shares - exit.shares;
    let cost_usd = row.cost_usd - cost_reduction;
    let realized_pnl_usd = row.realized_pnl_usd + exit.realized_pnl_usd;
    let (state, closed_at, normalized_cost) = if shares.is_zero() {
        (PositionLedgerState::Closed, Some(exit.exited_at), Usd::ZERO)
    } else {
        (PositionLedgerState::Closing, row.closed_at, cost_usd)
    };
    let avg_price = if shares.is_zero() {
        Price::ZERO
    } else {
        price_from_cost_and_shares(normalized_cost, shares)?
    };

    let mut active = row.into_active_model();
    active.state = ActiveValue::Set(state);
    active.shares = ActiveValue::Set(shares);
    active.avg_price = ActiveValue::Set(avg_price);
    active.cost_usd = ActiveValue::Set(normalized_cost);
    active.realized_pnl_usd = ActiveValue::Set(realized_pnl_usd);
    active.updated_at = ActiveValue::Set(exit.exited_at);
    active.closed_at = ActiveValue::Set(closed_at);
    active
        .update(db)
        .await
        .map_err(StorageError::from)
        .map(Into::into)
}

/// Mark a lot `Open -> Closing` as an exit order is written ahead (idempotent:
/// an already-`Closing` lot is left untouched; a terminal lot is a conflict).
pub async fn mark_closing(
    db: &impl ConnectionTrait,
    order_intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let Some(row) = quant_position::Entity::find()
        .filter(quant_position::Column::OrderIntentId.eq(order_intent_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(entity::QUANT_POSITION, order_intent_id));
    };
    match row.state {
        PositionLedgerState::Closing => Ok(()),
        PositionLedgerState::Open => {
            let mut active = row.into_active_model();
            active.state = ActiveValue::Set(PositionLedgerState::Closing);
            active.update(db).await.map_err(StorageError::from)?;
            Ok(())
        }
        other => Err(error::state_conflict(
            entity::QUANT_POSITION,
            Some(order_intent_id),
            format!("cannot mark exit-closing from {}", other.as_str()),
        )),
    }
}

/// Revert a lot `Closing -> Open` after a failed / cancelled exit attempt so it
/// is re-monitored (idempotent: a non-`Closing` lot is left untouched).
pub async fn revert_lot_to_open(
    db: &impl ConnectionTrait,
    order_intent_id: &OrderIntentId,
) -> Result<(), StorageError> {
    let Some(row) = quant_position::Entity::find()
        .filter(quant_position::Column::OrderIntentId.eq(order_intent_id.clone()))
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Ok(());
    };
    if row.state == PositionLedgerState::Closing {
        let mut active = row.into_active_model();
        active.state = ActiveValue::Set(PositionLedgerState::Open);
        active.update(db).await.map_err(StorageError::from)?;
    }
    Ok(())
}

fn price_from_cost_and_shares(cost_usd: Usd, shares: Shares) -> Result<Price, StorageError> {
    if shares.is_zero() {
        return Err(error::invariant_violation(
            Some(entity::QUANT_POSITION),
            "cannot compute average price from zero shares",
        ));
    }
    let price = cost_usd.inner() / shares.inner();
    if price < Decimal::ZERO {
        return Err(error::invariant_violation(
            Some(entity::QUANT_POSITION),
            "computed average price is negative",
        ));
    }
    Ok(Price::new(price))
}

fn filled_entry_order(
    orders: &[quant_execution_order::Model],
) -> Option<&quant_execution_order::Model> {
    orders.iter().find(|order| {
        order.order_phase == ExecutionOrderPhase::Entry
            && matches!(
                order.state,
                ExecutionOrderState::Filled | ExecutionOrderState::PartiallyFilled
            )
    })
}

fn resolve_entry_shares(
    intent: &quant_order_intent::Model,
    orders: &[quant_execution_order::Model],
) -> Option<Shares> {
    if let Some(shares) = intent.opportunistic_exit_state.denominator_shares {
        return Some(shares);
    }
    filled_entry_order(orders).map(|order| order.shares)
}

fn resolve_entry_avg_price(
    position: &PositionInfo,
    orders: &[quant_execution_order::Model],
) -> Price {
    if !position.avg_price.is_zero() {
        return position.avg_price;
    }
    filled_entry_order(orders).map_or(Price::ZERO, |order| order.price)
}

fn exit_events_from_orders(orders: &[quant_execution_order::Model]) -> Vec<LotExitEventRow> {
    let mut events: Vec<LotExitEventRow> = orders
        .iter()
        .filter(|order| {
            order.order_phase == ExecutionOrderPhase::Exit
                && matches!(
                    order.state,
                    ExecutionOrderState::Filled | ExecutionOrderState::PartiallyFilled
                )
        })
        .filter_map(|order| {
            let at = order.filled_at.or(Some(order.updated_at))?;
            Some(LotExitEventRow {
                at,
                shares: order.shares,
                net_proceeds: Usd::new(order.shares.inner() * order.price.inner()),
            })
        })
        .collect();
    events.sort_by_key(|event| event.at);
    events
}

fn page_condition(query: &PositionListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .state
                .map(|state| quant_position::Column::State.eq(state)),
        )
        .add_option(
            query
                .order_intent_id
                .clone()
                .map(|order_intent_id| quant_position::Column::OrderIntentId.eq(order_intent_id)),
        )
        .add_option(
            query
                .market_id
                .clone()
                .map(|market_id| quant_position::Column::MarketId.eq(market_id)),
        )
        .add_option(
            query
                .token_id
                .clone()
                .map(|token_id| quant_position::Column::TokenId.eq(token_id)),
        )
        .add_option(
            query
                .from
                .map(|from| quant_position::Column::OpenedAt.gte(from)),
        )
        .add_option(query.to.map(|to| quant_position::Column::OpenedAt.lt(to)))
}
