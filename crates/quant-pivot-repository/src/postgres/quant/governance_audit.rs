//! Postgres-backed model-governance audit ledger repository (append-only WORM).

use quant_pivot_error::storage::StorageError;
use quant_pivot_models::{
    domain::quant::{
        ModelGovernanceAuditDetail, ModelGovernanceAuditInfo, NewModelGovernanceAudit,
    },
    entities::quant_model_governance_audit::{Column, Entity},
    enums::quant::ModelGovernanceAction,
    types::ModelVersionId,
};
use sea_orm::{
    ColumnTrait, Condition, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter,
    QueryOrder,
    sea_query::{Expr, ExprTrait, extension::postgres::PgBinOper},
};

use crate::{postgres::query::list_fk_desc, traits::ModelGovernanceAuditRepository};

/// Postgres-backed model-governance audit ledger repository.
pub struct PgModelGovernanceAuditRepository {
    db: DatabaseConnection,
}

impl PgModelGovernanceAuditRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn find_by_id(
        &self,
        audit: &NewModelGovernanceAudit,
    ) -> Result<Option<ModelGovernanceAuditInfo>, StorageError> {
        Entity::find_by_id(audit.audit_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Into::into)
            .map(|stored: ModelGovernanceAuditInfo| {
                if stored.matches_new(audit) {
                    Ok(stored)
                } else {
                    Err(StorageError::state_conflict(
                        "quant_model_governance_audit",
                        Some(&audit.audit_id),
                        "governance audit identity was reused with semantic drift",
                    ))
                }
            })
            .transpose()
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

    async fn append_exact(
        &self,
        audit: NewModelGovernanceAudit,
    ) -> Result<ModelGovernanceAuditInfo, StorageError> {
        if let Some(stored) = self.find_by_id(&audit).await? {
            return Ok(stored);
        }
        let insert = Entity::insert(audit.clone().into_active_model())
            .exec_with_returning(&self.db)
            .await;
        match insert {
            Ok(stored) => Ok(stored.into()),
            Err(error) => self
                .find_by_id(&audit)
                .await?
                .ok_or_else(|| StorageError::from(error)),
        }
    }

    async fn list_by_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelGovernanceAuditInfo>, StorageError> {
        list_fk_desc::<Entity, _, _, _>(
            &self.db,
            Column::ModelVersionId,
            *model_version_id,
            Column::CreatedAt,
            Into::into,
        )
        .await
    }

    async fn list_promotions_by_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Vec<ModelGovernanceAuditInfo>, StorageError> {
        let candidate = serde_json::json!({
            "action": "promote_route",
            "record": {
                "route": {
                    "candidate_model_version_id": model_version_id,
                },
            },
        });
        let champion = serde_json::json!({
            "action": "promote_route",
            "record": {
                "route": {
                    "champion_model_version_id": model_version_id,
                },
            },
        });
        let rows = Entity::find()
            .filter(Column::Action.eq(ModelGovernanceAction::PromoteRoute))
            .filter(
                Condition::any()
                    .add(Expr::col((Entity, Column::Detail)).binary(PgBinOper::Contains, candidate))
                    .add(Expr::col((Entity, Column::Detail)).binary(PgBinOper::Contains, champion)),
            )
            .order_by_desc(Column::CreatedAt)
            .order_by_desc(Column::AuditId)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter()
            .map(ModelGovernanceAuditInfo::from)
            .map(|audit| {
                let ModelGovernanceAuditDetail::PromoteRoute { record } = &audit.detail else {
                    return Err(StorageError::invariant_violation(
                        Some("quant_model_governance_audit"),
                        "promotion query returned a non-promotion audit detail",
                    ));
                };
                record.validate().map_err(|error| {
                    StorageError::invariant_violation(
                        Some("quant_model_governance_audit"),
                        format!("stored route-promotion record is invalid: {error}"),
                    )
                })?;
                let route = record.route();
                if route.champion_model_version_id != *model_version_id
                    && route.candidate_model_version_id != *model_version_id
                {
                    return Err(StorageError::invariant_violation(
                        Some("quant_model_governance_audit"),
                        "promotion query returned an unrelated model version",
                    ));
                }
                Ok(audit)
            })
            .collect()
    }
}
