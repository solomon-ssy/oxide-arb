//! `PostgreSQL` WORM attribution artifact index.

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::{StorageError, entity::QUANT_ATTRIBUTION_ARTIFACT};
use quant_pivot_models::{
    domain::quant::{AttributionArtifactInfo, NewAttributionArtifact},
    entities::quant_attribution_artifact::{Column, Entity, Model},
    types::{AttributionArtifactId, FeedbackCycleId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, QueryOrder,
    TryInsertResult, sea_query::OnConflict,
};

use crate::traits::{AttributionArtifactRepository, AttributionArtifactWriteOutcome};

pub struct PgAttributionArtifactRepository {
    db: DatabaseConnection,
}

impl PgAttributionArtifactRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn info(row: Model) -> Result<AttributionArtifactInfo, StorageError> {
        let info: AttributionArtifactInfo = row.into();
        info.validate().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_ATTRIBUTION_ARTIFACT), error.to_string())
        })?;
        Ok(info)
    }
}

#[async_trait::async_trait]
impl AttributionArtifactRepository for PgAttributionArtifactRepository {
    async fn insert(
        &self,
        artifact: NewAttributionArtifact,
    ) -> Result<AttributionArtifactWriteOutcome, StorageError> {
        artifact.validate().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_ATTRIBUTION_ARTIFACT), error.to_string())
        })?;
        let artifact_id = artifact.attribution_artifact_id;
        let result = Entity::insert(artifact.clone().into_active_model())
            .on_conflict(
                OnConflict::column(Column::AttributionArtifactId)
                    .do_nothing()
                    .to_owned(),
            )
            .try_insert()
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let inserted = match result {
            TryInsertResult::Inserted(1) => true,
            TryInsertResult::Conflicted | TryInsertResult::Inserted(0) => false,
            TryInsertResult::Inserted(rows) => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_ATTRIBUTION_ARTIFACT),
                    format!("single attribution insert affected {rows} rows"),
                ));
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_ATTRIBUTION_ARTIFACT),
                    "non-empty attribution insert produced no statement",
                ));
            }
        };
        let stored = self
            .find_by_id(&artifact_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_ATTRIBUTION_ARTIFACT, artifact_id))?;
        if !stored.matches(&artifact) {
            return Err(StorageError::state_conflict(
                QUANT_ATTRIBUTION_ARTIFACT,
                Some(artifact_id),
                "content-addressed attribution replay differs from the stored artifact",
            ));
        }
        Ok(if inserted {
            AttributionArtifactWriteOutcome::Inserted(stored)
        } else {
            AttributionArtifactWriteOutcome::AlreadyPresent(stored)
        })
    }

    async fn find_by_id(
        &self,
        artifact_id: &AttributionArtifactId,
    ) -> Result<Option<AttributionArtifactInfo>, StorageError> {
        Entity::find_by_id(*artifact_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::info)
            .transpose()
    }

    async fn list_for_cycle(
        &self,
        feedback_cycle_id: FeedbackCycleId,
    ) -> Result<Vec<AttributionArtifactInfo>, StorageError> {
        Entity::find()
            .filter(Column::SourceFeedbackCycleId.eq(feedback_cycle_id))
            .order_by_asc(Column::ArtifactHash)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::info)
            .collect()
    }

    async fn list_available(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        cutoff: DateTime<Utc>,
    ) -> Result<Vec<AttributionArtifactInfo>, StorageError> {
        Entity::find()
            .filter(Column::SourceFeedbackCycleId.ne(feedback_cycle_id))
            .filter(Column::SourceCutoff.lt(cutoff))
            .filter(Column::AvailableAt.lte(cutoff))
            .order_by_asc(Column::ArtifactHash)
            .all(&self.db)
            .await
            .map_err(StorageError::from)?
            .into_iter()
            .map(Self::info)
            .collect()
    }
}
