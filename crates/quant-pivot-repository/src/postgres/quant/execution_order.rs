//! Postgres-backed execution-order repository.

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::ExecutionOrderRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ExecutionOrderInfo, ExecutionOrderListQuery, ExecutionOrderPatch, NewExecutionOrder,
        Paginated,
    },
    entities::quant_execution_order,
    enums::quant::ExecutionOrderState,
    types::{ExecutionOrderId, OrderIntentId},
};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel,
    IntoActiveValue, PaginatorTrait, QueryFilter, QueryOrder, QuerySelect,
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
        quant_execution_order::Entity::insert(order.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_intent(
        &self,
        order_intent_id: &OrderIntentId,
    ) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        quant_execution_order::Entity::find()
            .filter(quant_execution_order::Column::OrderIntentId.eq(order_intent_id.clone()))
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ExecutionOrderInfo>, StorageError> {
        quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: ExecutionOrderListQuery,
    ) -> Result<Paginated<ExecutionOrderInfo>, StorageError> {
        let query = query.normalized();
        paginate_mapped(
            quant_execution_order::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_execution_order::Column::CreatedAt),
            &self.db,
            &query.page,
            Into::into,
        )
        .await
    }

    async fn has_ambiguous_inflight(&self) -> Result<bool, StorageError> {
        quant_execution_order::Entity::find()
            .filter(quant_execution_order::Column::State.eq(ExecutionOrderState::Ambiguous))
            .count(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|count| count > 0)
    }

    async fn find_reconcilable(&self, limit: u64) -> Result<Vec<ExecutionOrderInfo>, StorageError> {
        quant_execution_order::Entity::find()
            .filter(quant_execution_order::Column::State.is_in([
                ExecutionOrderState::Submitted,
                ExecutionOrderState::Ambiguous,
            ]))
            .order_by_asc(quant_execution_order::Column::SubmittedAt)
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
        let Some(row) = quant_execution_order::Entity::find_by_id(execution_order_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_EXECUTION_ORDER,
                execution_order_id,
            ));
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
        entity::QUANT_EXECUTION_ORDER,
        Some(execution_order_id),
        current,
        next,
    ))
}

fn page_condition(query: &ExecutionOrderListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .state
                .map(|state| quant_execution_order::Column::State.eq(state)),
        )
        .add_option(
            query
                .order_phase
                .map(|order_phase| quant_execution_order::Column::OrderPhase.eq(order_phase)),
        )
        .add_option(query.order_intent_id.clone().map(|order_intent_id| {
            quant_execution_order::Column::OrderIntentId.eq(order_intent_id)
        }))
        .add_option(
            query
                .market_id
                .clone()
                .map(|market_id| quant_execution_order::Column::MarketId.eq(market_id)),
        )
        .add_option(
            query
                .token_id
                .clone()
                .map(|token_id| quant_execution_order::Column::TokenId.eq(token_id)),
        )
        .add_option(
            query
                .from
                .map(|from| quant_execution_order::Column::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_execution_order::Column::CreatedAt.lt(to)),
        )
}
