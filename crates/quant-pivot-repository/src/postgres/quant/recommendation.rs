//! Postgres-backed recommendation repository (read + per-recommendation expiry).

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_RECOMMENDATION};
use quant_pivot_models::{
    domain::{
        governance::NewOperationLog,
        quant::{OrderIntentInfo, RecommendationInfo},
    },
    entities::{
        operation_log::Entity as OperationLogEntity,
        quant_recommendation::{Column, Entity},
    },
    enums::{execution::ApprovalInvalidation, quant::RecommendationStatus},
    types::{RecommendationId, RecommendationReportId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{quant::order_intent::PgOrderIntentRepository, query::find_id_chunks, state_hash},
    traits::RecommendationRepository,
};

/// Postgres-backed recommendation repository.
pub struct PgRecommendationRepository {
    db: DatabaseConnection,
}

impl PgRecommendationRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl RecommendationRepository for PgRecommendationRepository {
    async fn find_by_report(
        &self,
        report_id: &RecommendationReportId,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        Entity::find()
            .filter(Column::RecommendationReportId.eq(*report_id))
            .order_by_asc(Column::Rank)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError> {
        Entity::find_by_id(*recommendation_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_ids(
        &self,
        recommendation_ids: &[RecommendationId],
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        find_id_chunks::<Entity, _, _>(&self.db, recommendation_ids, Column::RecommendationId)
            .await
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_expirable(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationId>, StorageError> {
        Entity::find()
            .filter(Column::Status.is_in(RecommendationStatus::NEW_INTENT_AUTHORITY))
            .filter(Column::ValidUntil.lte(now))
            .order_by_asc(Column::ValidUntil)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(|row| row.recommendation_id).collect())
    }

    async fn upcoming_expirations(
        &self,
        before: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<(RecommendationId, DateTime<Utc>)>, StorageError> {
        Entity::find()
            .filter(Column::Status.is_in(RecommendationStatus::NEW_INTENT_AUTHORITY))
            .filter(Column::ValidUntil.lte(before))
            .order_by_asc(Column::ValidUntil)
            .limit(limit)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| (row.recommendation_id, row.valid_until))
                    .collect()
            })
    }

    async fn expire(
        &self,
        recommendation_id: &RecommendationId,
        expired_at: DateTime<Utc>,
        operation_log: NewOperationLog,
    ) -> Result<(RecommendationInfo, Vec<OrderIntentInfo>), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = Entity::find_by_id(*recommendation_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_RECOMMENDATION, recommendation_id))?;
        if !row.status.allows_new_intent() {
            return Err(StorageError::state_conflict(
                QUANT_RECOMMENDATION,
                Some(recommendation_id),
                format!(
                    "recommendation is {} (only actionable recommendations expire)",
                    row.status.as_str()
                ),
            ));
        }
        let before_info: RecommendationInfo = row.clone().into();
        let invalidated = PgOrderIntentRepository::invalidate_pre_submission(
            &txn,
            recommendation_id,
            ApprovalInvalidation::RecommendationExpired,
            expired_at,
            &operation_log,
        )
        .await?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(RecommendationStatus::Expired);
        let model = active.update(&txn).await.map_err(StorageError::from)?;
        let after_info: RecommendationInfo = model.clone().into();
        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        OperationLogEntity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((model.into(), invalidated))
    }
}
