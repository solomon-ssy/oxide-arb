//! Postgres-backed model-governance audit ledger repository (append-only WORM).

use crate::traits::ModelGovernanceAuditRepository;
use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::{ModelGovernanceAuditInfo, NewModelGovernanceAudit},
    entities::quant_model_governance_audit,
    types::ModelVersionId,
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
};

/// Postgres-backed model-governance audit ledger repository.
pub struct PgModelGovernanceAuditRepository {
    db: DatabaseConnection,
}

impl PgModelGovernanceAuditRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl ModelGovernanceAuditRepository for PgModelGovernanceAuditRepository {
    async fn create(
        &self,
        audit: NewModelGovernanceAudit,
    ) -> Result<ModelGovernanceAuditInfo, StorageError> {
        quant_model_governance_audit::Entity::insert(audit.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn list_by_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelGovernanceAuditInfo>, StorageError> {
        quant_model_governance_audit::Entity::find()
            .filter(
                quant_model_governance_audit::Column::ModelVersionId.eq(model_version_id.clone()),
            )
            .order_by_desc(quant_model_governance_audit::Column::CreatedAt)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }
}
