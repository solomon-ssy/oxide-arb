//! Postgres-backed execution-order reconciliation repository.

use crate::{postgres::error, traits::ReconciliationRepository};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        AppendReconciliationEvidence, NewReconciliation, ReconciliationInfo, ReconciliationPatch,
    },
    entities::quant_reconciliation,
    enums::execution::ReconciliationResult,
    types::{ExecutionOrderId, ReconciliationId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    IntoActiveValue, PaginatorTrait, QueryFilter,
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

    async fn resolve(
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
        active.discrepancy_usd = patch.discrepancy_usd.into_active_value();
        active.resolved_by = patch.resolved_by.into_active_value();
        active.resolved_at = patch.resolved_at.into_active_value();
        active
            .update(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
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

    async fn find_unresolved(&self) -> Result<Vec<ReconciliationInfo>, StorageError> {
        quant_reconciliation::Entity::find()
            .filter(quant_reconciliation::Column::ResolvedAt.is_null())
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn has_unresolvable(&self) -> Result<bool, StorageError> {
        quant_reconciliation::Entity::find()
            .filter(quant_reconciliation::Column::Result.eq(ReconciliationResult::Unresolvable))
            .filter(quant_reconciliation::Column::ResolvedAt.is_null())
            .count(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|count| count > 0)
    }
}
