//! Postgres-backed execution-order reconciliation repository.

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::ReconciliationRepository,
};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        AppendReconciliationEvidence, NewReconciliation, PageWindow, Paginated, ReconciliationInfo,
        ReconciliationListQuery, ReconciliationPatch,
    },
    entities::quant_reconciliation,
    enums::execution::ReconciliationResult,
    types::{ExecutionOrderId, ReconciliationId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, IntoActiveValue, PaginatorTrait, QueryFilter, QueryOrder,
};

/// Postgres-backed reconciliation repository.
pub struct PgReconciliationRepository {
    db: DatabaseConnection,
}

impl PgReconciliationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ReconciliationRepository for PgReconciliationRepository {
    async fn create(
        &self,
        reconciliation: NewReconciliation,
    ) -> Result<ReconciliationInfo, StorageError> {
        quant_reconciliation::Entity::insert(reconciliation.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn append_evidence(
        &self,
        reconciliation_id: &ReconciliationId,
        evidence: AppendReconciliationEvidence,
    ) -> Result<ReconciliationInfo, StorageError> {
        let Some(row) = quant_reconciliation::Entity::find_by_id(reconciliation_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_RECONCILIATION,
                reconciliation_id,
            ));
        };

        let mut chain = row.evidence_json.clone();
        chain.push(evidence.evidence);
        let mut active = row.into_active_model();
        active.evidence_json = ActiveValue::Set(chain);
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn patch(
        &self,
        reconciliation_id: &ReconciliationId,
        patch: ReconciliationPatch,
    ) -> Result<ReconciliationInfo, StorageError> {
        let Some(row) = quant_reconciliation::Entity::find_by_id(reconciliation_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_RECONCILIATION,
                reconciliation_id,
            ));
        };

        let mut active = row.into_active_model();
        active.result = patch.result.into_active_value();
        active.venue_filled_shares = patch.venue_filled_shares.into_active_value();
        active.venue_avg_price = patch.venue_avg_price.into_active_value();
        active.expected_cash_delta_usd = patch.expected_cash_delta_usd.into_active_value();
        active.venue_cash_delta_usd = patch.venue_cash_delta_usd.into_active_value();
        active.realized_pnl_usd = patch.realized_pnl_usd.into_active_value();
        active.expected_fee_usd = patch.expected_fee_usd.into_active_value();
        active.observed_fee_usd = patch.observed_fee_usd.into_active_value();
        active.fee_delta_usd = patch.fee_delta_usd.into_active_value();
        active.resolved_by = patch.resolved_by.into_active_value();
        active.resolved_at = patch.resolved_at.into_active_value();
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        reconciliation_id: &ReconciliationId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        quant_reconciliation::Entity::find_by_id(reconciliation_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_execution_order(
        &self,
        execution_order_id: &ExecutionOrderId,
    ) -> Result<Option<ReconciliationInfo>, StorageError> {
        quant_reconciliation::Entity::find()
            .filter(quant_reconciliation::Column::ExecutionOrderId.eq(execution_order_id.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: ReconciliationListQuery,
    ) -> Result<Paginated<ReconciliationInfo>, StorageError> {
        paginate_mapped(
            quant_reconciliation::Entity::find()
                .filter(page_condition(&query))
                .order_by_desc(quant_reconciliation::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn find_unresolved(&self) -> Result<Vec<ReconciliationInfo>, StorageError> {
        quant_reconciliation::Entity::find()
            .filter(quant_reconciliation::Column::ResolvedAt.is_null())
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn has_unresolvable(&self) -> Result<bool, StorageError> {
        Ok(self.count_blocking_unresolvable().await? > 0)
    }

    async fn count_blocking_unresolvable(&self) -> Result<u64, StorageError> {
        quant_reconciliation::Entity::find()
            .filter(quant_reconciliation::Column::Result.eq(ReconciliationResult::Unresolvable))
            .filter(quant_reconciliation::Column::ResolvedAt.is_null())
            .count(&self.db)
            .await
            .map_err(StorageError::from)
    }
}

fn page_condition(query: &ReconciliationListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .result
                .map(|result| quant_reconciliation::Column::Result.eq(result)),
        )
        .add_option(query.resolved.map(|resolved| {
            if resolved {
                quant_reconciliation::Column::ResolvedAt.is_not_null()
            } else {
                quant_reconciliation::Column::ResolvedAt.is_null()
            }
        }))
        .add_option(query.execution_order_id.clone().map(|execution_order_id| {
            quant_reconciliation::Column::ExecutionOrderId.eq(execution_order_id)
        }))
        .add_option(
            query.order_intent_id.clone().map(|order_intent_id| {
                quant_reconciliation::Column::OrderIntentId.eq(order_intent_id)
            }),
        )
        .add_option(
            query
                .from
                .map(|from| quant_reconciliation::Column::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| quant_reconciliation::Column::CreatedAt.lte(to)),
        )
}
