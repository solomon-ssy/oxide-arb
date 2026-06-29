//! Postgres-backed recommendation repository (read + per-recommendation expiry).

use crate::{
    postgres::{error, quant::attribution, state_hash},
    traits::RecommendationRepository,
};
use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{NewOperationLog, RecommendationInfo},
    entities::{operation_log, quant_recommendation, quant_recommendation_attribution},
    enums::quant::RecommendationStatus,
    types::{RecommendationId, RecommendationReportId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait, TransactionTrait,
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
        quant_recommendation::Entity::find()
            .filter(quant_recommendation::Column::RecommendationReportId.eq(report_id.clone()))
            .order_by_asc(quant_recommendation::Column::Rank)
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn find_by_id(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<Option<RecommendationInfo>, StorageError> {
        quant_recommendation::Entity::find_by_id(recommendation_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_expirable(
        &self,
        now: DateTime<Utc>,
        limit: u64,
    ) -> Result<Vec<RecommendationId>, StorageError> {
        quant_recommendation::Entity::find()
            .filter(
                quant_recommendation::Column::Status
                    .is_in(RecommendationStatus::ACTIONABLE_FOR_INTENT),
            )
            .filter(quant_recommendation::Column::ValidUntil.lte(now))
            .order_by_asc(quant_recommendation::Column::ValidUntil)
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
        quant_recommendation::Entity::find()
            .filter(
                quant_recommendation::Column::Status
                    .is_in(RecommendationStatus::ACTIONABLE_FOR_INTENT),
            )
            .filter(quant_recommendation::Column::ValidUntil.lte(before))
            .order_by_asc(quant_recommendation::Column::ValidUntil)
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
        operation_log: NewOperationLog,
    ) -> Result<RecommendationInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let row = quant_recommendation::Entity::find_by_id(recommendation_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| error::not_found(entity::QUANT_RECOMMENDATION, recommendation_id))?;
        if !row.status.is_actionable_for_intent() {
            return Err(error::state_conflict(
                entity::QUANT_RECOMMENDATION,
                Some(recommendation_id),
                format!(
                    "recommendation is {} (only actionable recommendations expire)",
                    row.status.as_str()
                ),
            ));
        }
        let before_info: RecommendationInfo = row.clone().into();
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(RecommendationStatus::Expired);
        let model = active.update(&txn).await.map_err(StorageError::from)?;
        let after_info: RecommendationInfo = model.clone().into();
        let operation_log =
            state_hash::apply_transition_hashes(operation_log, &before_info, &after_info)?;
        operation_log::Entity::insert(operation_log.into_active_model())
            .exec(&txn)
            .await
            .map_err(StorageError::from)?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(model.into())
    }

    async fn find_expired_attribution_candidates(
        &self,
        limit: u64,
    ) -> Result<Vec<RecommendationInfo>, StorageError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut query = quant_recommendation::Entity::find()
            .join(
                JoinType::LeftJoin,
                quant_recommendation::Relation::Attribution.def(),
            )
            .filter(quant_recommendation::Column::Status.eq(RecommendationStatus::Expired))
            .filter(quant_recommendation_attribution::Column::RecommendationId.is_null())
            .order_by_asc(quant_recommendation::Column::ValidUntil)
            .limit(limit);
        for guard in attribution::expired_ledger_filters() {
            query = query.filter(guard);
        }
        query
            .all(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|rows| rows.into_iter().map(Into::into).collect())
    }

    async fn recommendation_blocks_final_attribution(
        &self,
        recommendation_id: &RecommendationId,
    ) -> Result<bool, StorageError> {
        attribution::blocks_attribution(&self.db, recommendation_id)
            .await
            .map_err(StorageError::from)
    }
}
