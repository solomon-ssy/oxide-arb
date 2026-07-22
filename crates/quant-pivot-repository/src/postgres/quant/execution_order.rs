//! Postgres-backed execution-order repository.

use quant_pivot_error::storage::{StorageError, entity::QUANT_EXECUTION_ORDER};
use quant_pivot_models::{
    domain::{
        api::ExecutionOrderListQuery,
        pagination::{PageWindow, Paginated},
        quant::{ExecutionOrderInfo, ExecutionOrderPatch, NewExecutionOrder},
    },
    entities::quant_execution_order::{Column, Entity},
    enums::quant::ExecutionOrderState,
    types::{ExecutionOrderId, OrderIntentId},
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel,
    IntoActiveValue, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
};

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::ExecutionOrderRepository,
};

/// Postgres-backed execution-order repository.
pub struct PgExecutionOrderRepository {
    db: DatabaseConnection,
}

impl PgExecutionOrderRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ExecutionOrderRepository for PgExecutionOrderRepository {
    async fn create(&self, order: NewExecutionOrder) -> Result<ExecutionOrderInfo, StorageError> {
        Entity::insert(order.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        Entity::find()
            .filter(Column::OrderIntentId.eq(*order_intent_id))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ExecutionOrderInfo>, StorageError> {
        Entity::find_by_id(*execution_order_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: ExecutionOrderListQuery,
    ) -> Result<Paginated<ExecutionOrderInfo>, StorageError> {
        paginate_mapped(
            Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn has_ambiguous_inflight(&self) -> Result<bool, StorageError> {
        Entity::find()
            .filter(Column::State.eq(ExecutionOrderState::Ambiguous))
            .count(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|count| count > 0)
    }

    async fn find_reconcilable(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        Entity::find()
            .filter(Column::State.is_in([
                ExecutionOrderState::Submitted,
                ExecutionOrderState::Ambiguous,
            ]))
            .order_by_asc(Column::SubmittedAt)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn transition(
        &self,
        execution_order_id: &ExecutionOrderId,
        patch: ExecutionOrderPatch,
    ) -> Result<ExecutionOrderInfo, StorageError> {
        let Some(row) = Entity::find_by_id(*execution_order_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(QUANT_EXECUTION_ORDER, execution_order_id));
        };

        if let Some(next) = patch.state.into_option() {
            validate_execution_order_transition(row.state, next, execution_order_id)?;
        }

        let mut active = row.into_active_model();
        active.state = patch.state.into_active_value();
        active.venue_order_id = patch.venue_order_id.into_active_value();
        active.venue_status = patch.venue_status.into_active_value();
        active.submitted_at = patch.submitted_at.into_active_value();
        active.filled_at = patch.filled_at.into_active_value();
        active.cancelled_at = patch.cancelled_at.into_active_value();
        active.gtd_expiration_at = patch.gtd_expiration_at.into_active_value();
        active.error_message = patch.error_message.into_active_value();
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }
}

pub fn validate_execution_order_transition(
    current: ExecutionOrderState,
    next: ExecutionOrderState,
    execution_order_id: &ExecutionOrderId,
) -> Result<(), StorageError> {
    if current == next {
        return Ok(());
    }
    let valid = matches!(
        (current, next),
        (
            ExecutionOrderState::Planned,
            ExecutionOrderState::Accepted
                | ExecutionOrderState::Submitted
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed,
        ) | (
            ExecutionOrderState::Accepted,
            ExecutionOrderState::Submitted
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed,
        ) | (
            ExecutionOrderState::Submitted,
            ExecutionOrderState::PartiallyFilled
                | ExecutionOrderState::Filled
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed
                | ExecutionOrderState::Ambiguous,
        ) | (
            ExecutionOrderState::PartiallyFilled,
            ExecutionOrderState::Filled
                | ExecutionOrderState::CancelRequested
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed
                | ExecutionOrderState::Ambiguous,
        ) | (
            ExecutionOrderState::CancelRequested,
            ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed
                | ExecutionOrderState::Ambiguous,
        ) | (
            ExecutionOrderState::Ambiguous,
            ExecutionOrderState::PartiallyFilled
                | ExecutionOrderState::Filled
                | ExecutionOrderState::Cancelled
                | ExecutionOrderState::Failed,
        )
    );
    if valid {
        return Ok(());
    }
    Err(error::illegal_transition(
        QUANT_EXECUTION_ORDER,
        Some(execution_order_id),
        current,
        next,
    ))
}

fn page_condition(query: &ExecutionOrderListQuery) -> Condition {
    Condition::all()
        .add_option(query.state.map(|state| Column::State.eq(state)))
        .add_option(
            query
                .order_phase
                .map(|order_phase| Column::OrderPhase.eq(order_phase)),
        )
        .add_option(
            query
                .order_intent_id
                .map(|order_intent_id| Column::OrderIntentId.eq(order_intent_id)),
        )
        .add_option(
            query
                .market_id
                .clone()
                .map(|market_id| Column::MarketId.eq(market_id)),
        )
        .add_option(
            query
                .token_id
                .clone()
                .map(|token_id| Column::TokenId.eq(token_id)),
        )
        .add_option(query.from.map(|from| Column::CreatedAt.gte(from)))
        .add_option(query.to.map(|to| Column::CreatedAt.lt(to)))
}
