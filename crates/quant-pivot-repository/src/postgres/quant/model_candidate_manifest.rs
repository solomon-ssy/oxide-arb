//! `PostgreSQL` WORM candidate-manifest repository.

use quant_pivot_error::storage::{StorageError, entity::QUANT_MODEL_CANDIDATE_MANIFEST};
use quant_pivot_models::{
    domain::quant::{ModelCandidateManifestInfo, NewModelCandidateManifest},
    entities::quant_model_candidate_manifest::{Column, Entity, Model},
    types::{ContentHash, FeedbackCycleId, ModelCandidateManifestId},
};
use sea_orm::{
    ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel, QueryFilter, TryInsertResult,
    sea_query::OnConflict,
};

use crate::traits::{ModelCandidateManifestRepository, ModelCandidateManifestWriteOutcome};

pub struct PgModelCandidateManifestRepository {
    db: DatabaseConnection,
}

impl PgModelCandidateManifestRepository {
    #[must_use]
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    fn info(row: Model) -> Result<ModelCandidateManifestInfo, StorageError> {
        let info = ModelCandidateManifestInfo::from(row);
        info.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_MODEL_CANDIDATE_MANIFEST),
                error.to_string(),
            )
        })?;
        Ok(info)
    }
}

#[async_trait::async_trait]
impl ModelCandidateManifestRepository for PgModelCandidateManifestRepository {
    async fn insert(
        &self,
        manifest: NewModelCandidateManifest,
    ) -> Result<ModelCandidateManifestWriteOutcome, StorageError> {
        manifest.validate().map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_MODEL_CANDIDATE_MANIFEST),
                error.to_string(),
            )
        })?;
        let manifest_id = manifest.manifest_id;
        let result = Entity::insert(manifest.clone().into_active_model())
            .on_conflict(
                OnConflict::column(Column::ManifestId)
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
                    Some(QUANT_MODEL_CANDIDATE_MANIFEST),
                    format!("single candidate-manifest insert affected {rows} rows"),
                ));
            }
            TryInsertResult::Empty => {
                return Err(StorageError::invariant_violation(
                    Some(QUANT_MODEL_CANDIDATE_MANIFEST),
                    "non-empty candidate-manifest insert produced no statement",
                ));
            }
        };
        let stored = self
            .find_by_id(&manifest_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_CANDIDATE_MANIFEST, manifest_id))?;
        if !stored.matches(&manifest) {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_CANDIDATE_MANIFEST,
                Some(manifest_id),
                "content-addressed candidate-manifest replay has semantic drift",
            ));
        }
        Ok(if inserted {
            ModelCandidateManifestWriteOutcome::Inserted(stored)
        } else {
            ModelCandidateManifestWriteOutcome::AlreadyPresent(stored)
        })
    }

    async fn find_by_id(
        &self,
        manifest_id: &ModelCandidateManifestId,
    ) -> Result<Option<ModelCandidateManifestInfo>, StorageError> {
        Entity::find_by_id(*manifest_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::info)
            .transpose()
    }

    async fn find_candidate(
        &self,
        feedback_cycle_id: FeedbackCycleId,
        candidate_recipe_hash: ContentHash,
    ) -> Result<Option<ModelCandidateManifestInfo>, StorageError> {
        Entity::find()
            .filter(Column::FeedbackCycleId.eq(feedback_cycle_id))
            .filter(Column::CandidateRecipeHash.eq(candidate_recipe_hash))
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .map(Self::info)
            .transpose()
    }
}
