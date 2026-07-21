//! Postgres-backed model-governance audit ledger repository (append-only WORM).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{ModelGovernanceAuditInfo, NewModelGovernanceAudit},
    entities::quant_model_governance_audit::{Column, Entity},
    types::ModelVersionId,
};
use sea_orm::{DatabaseConnection, EntityTrait, IntoActiveModel};

use crate::{postgres::query::list_by_fk_ordered_desc, traits::ModelGovernanceAuditRepository};

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
        Entity::insert(audit.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(StorageError::from)
            .map(Into::into)
    }

    async fn list_by_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelGovernanceAuditInfo>, StorageError> {
        list_by_fk_ordered_desc::<Entity, _, _, _>(
            &self.db,
            Column::ModelVersionId,
            model_version_id.clone(),
            Column::CreatedAt,
            Into::into,
        )
        .await
    }
}
