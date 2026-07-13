//! Postgres trade-policy artifact catalog and WORM governance transitions.

use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        NewTradePolicyArtifact, NewTradePolicyGovernanceAudit, PageWindow, Paginated,
        TradePolicyArtifactInfo, TradePolicyAuditListQuery, TradePolicyGovernanceAuditInfo,
        TradePolicyListQuery,
    },
    entities::{quant_trade_policy_artifact, quant_trade_policy_governance_audit},
    enums::quant::TradePolicyStatus,
    types::TradePolicyArtifactId,
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::TradePolicyRepository,
};

pub struct PgTradePolicyRepository {
    db: DatabaseConnection,
}

impl PgTradePolicyRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl TradePolicyRepository for PgTradePolicyRepository {
    async fn insert(
        &self,
        artifact: NewTradePolicyArtifact,
    ) -> Result<TradePolicyArtifactInfo, StorageError> {
        quant_trade_policy_artifact::Entity::insert(artifact.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|error| {
                error::map_unique(error, entity::QUANT_TRADE_POLICY_ARTIFACT, "content_hash")
            })
            .map(Into::into)
    }

    async fn find(
        &self,
        artifact_id: &TradePolicyArtifactId,
    ) -> Result<Option<TradePolicyArtifactInfo>, StorageError> {
        quant_trade_policy_artifact::Entity::find_by_id(artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: TradePolicyListQuery,
    ) -> Result<Paginated<TradePolicyArtifactInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .status
                    .map(|status| quant_trade_policy_artifact::Column::Status.eq(status)),
            )
            .add_option(query.source_dataset_id.as_ref().map(|dataset_id| {
                quant_trade_policy_artifact::Column::SourceDatasetId.eq(dataset_id.clone())
            }))
            .add_option(
                query
                    .from
                    .map(|from| quant_trade_policy_artifact::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_trade_policy_artifact::Column::CreatedAt.lt(to)),
            );
        paginate_mapped(
            quant_trade_policy_artifact::Entity::find()
                .filter(condition)
                .order_by_desc(quant_trade_policy_artifact::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn transition(
        &self,
        artifact_id: &TradePolicyArtifactId,
        expected: TradePolicyStatus,
        target: TradePolicyStatus,
        audit: NewTradePolicyGovernanceAudit,
    ) -> Result<TradePolicyArtifactInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_trade_policy_artifact::Entity::find_by_id(artifact_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(entity::QUANT_TRADE_POLICY_ARTIFACT, artifact_id))?;
        if row.status != expected || !row.status.allows_transition_to(target) {
            return Err(error::illegal_transition(
                entity::QUANT_TRADE_POLICY_ARTIFACT,
                Some(artifact_id),
                row.status.as_str(),
                target.as_str(),
            ));
        }
        if audit.artifact_id != *artifact_id
            || audit.from_status != row.status
            || audit.to_status != target
            || audit.content_hash != row.content_hash
        {
            return Err(error::invariant_violation(
                Some(entity::QUANT_TRADE_POLICY_ARTIFACT),
                "trade-policy governance audit does not match the locked artifact transition",
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(target);
        active.updated_at = ActiveValue::Set(Utc::now());
        let updated = active.update(&txn).await.map_err(StorageError::from)?;
        quant_trade_policy_governance_audit::Entity::insert(audit.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn page_audits(
        &self,
        artifact_id: &TradePolicyArtifactId,
        query: TradePolicyAuditListQuery,
    ) -> Result<Paginated<TradePolicyGovernanceAuditInfo>, StorageError> {
        paginate_mapped(
            quant_trade_policy_governance_audit::Entity::find()
                .filter(
                    quant_trade_policy_governance_audit::Column::ArtifactId.eq(artifact_id.clone()),
                )
                .order_by_desc(quant_trade_policy_governance_audit::Column::CreatedAt)
                .order_by_desc(quant_trade_policy_governance_audit::Column::AuditId),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }
}
