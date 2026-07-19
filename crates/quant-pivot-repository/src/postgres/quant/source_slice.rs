//! Postgres source-slice materialization ledger.

use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{BeginSourceSliceOutcome, CompleteSourceSlice, NewSourceSlice, SourceSliceInfo},
    entities::quant_source_slice,
    enums::quant::SourceSliceStatus,
    types::{ContentHash, ResearchEvaluationTrack, SourceSliceId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, DatabaseConnection, EntityTrait, IntoActiveModel,
    QueryFilter, QuerySelect, TransactionTrait, sea_query::OnConflict,
};

use crate::{postgres::error, traits::SourceSliceRepository};

pub struct PgSourceSliceRepository {
    db: DatabaseConnection,
}

impl PgSourceSliceRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait::async_trait]
impl SourceSliceRepository for PgSourceSliceRepository {
    async fn begin_or_get(
        &self,
        source_slice: NewSourceSlice,
    ) -> Result<BeginSourceSliceOutcome, StorageError> {
        validate_new(&source_slice)?;
        let identity_hash = source_slice.identity_hash.clone();
        let mut active = source_slice.into_active_model();
        active.status = ActiveValue::Set(SourceSliceStatus::Materializing);
        let insert = quant_source_slice::Entity::insert(active)
            .on_conflict(
                OnConflict::column(quant_source_slice::Column::IdentityHash)
                    .do_nothing()
                    .to_owned(),
            )
            .exec_without_returning(&self.db)
            .await
            .map_err(StorageError::from)?;
        let source_slice = self
            .find_by_identity(&identity_hash)
            .await?
            .ok_or_else(|| {
                error::invariant_violation(
                    Some(entity::QUANT_SOURCE_SLICE),
                    "source-slice claim was not observable after insert".to_owned(),
                )
            })?;
        Ok(BeginSourceSliceOutcome {
            source_slice,
            acquired: insert == 1,
        })
    }

    async fn find_by_id(
        &self,
        source_slice_id: &SourceSliceId,
    ) -> Result<Option<SourceSliceInfo>, StorageError> {
        quant_source_slice::Entity::find_by_id(source_slice_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn find_by_identity(
        &self,
        identity_hash: &ContentHash,
    ) -> Result<Option<SourceSliceInfo>, StorageError> {
        quant_source_slice::Entity::find()
            .filter(quant_source_slice::Column::IdentityHash.eq(identity_hash.clone()))
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn complete(
        &self,
        source_slice_id: &SourceSliceId,
        completion: CompleteSourceSlice,
    ) -> Result<SourceSliceInfo, StorageError> {
        completion.manifest.validate().map_err(|detail| {
            error::invariant_violation(Some(entity::QUANT_SOURCE_SLICE), detail)
        })?;
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_for_update(&transaction, source_slice_id).await?;
        if row.status == SourceSliceStatus::Ready {
            ensure_idempotent_completion(&row, &completion)?;
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        if row.status != SourceSliceStatus::Materializing {
            return Err(error::illegal_transition(
                entity::QUANT_SOURCE_SLICE,
                Some(source_slice_id),
                row.status,
                SourceSliceStatus::Ready,
            ));
        }
        ensure_manifest_binding(&row, &completion)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(SourceSliceStatus::Ready);
        active.manifest_uri = ActiveValue::Set(Some(completion.manifest_ref.manifest_uri));
        active.manifest_hash = ActiveValue::Set(Some(completion.manifest_ref.manifest_hash));
        active.manifest_json = ActiveValue::Set(Some(completion.manifest));
        active.completed_at = ActiveValue::Set(Some(chrono::Utc::now()));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn fail(
        &self,
        source_slice_id: &SourceSliceId,
        detail: String,
    ) -> Result<SourceSliceInfo, StorageError> {
        if detail.trim().is_empty() {
            return Err(error::invariant_violation(
                Some(entity::QUANT_SOURCE_SLICE),
                "source-slice failure detail must not be empty".to_owned(),
            ));
        }
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let row = load_for_update(&transaction, source_slice_id).await?;
        if row.status == SourceSliceStatus::Failed {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        if row.status != SourceSliceStatus::Materializing {
            return Err(error::illegal_transition(
                entity::QUANT_SOURCE_SLICE,
                Some(source_slice_id),
                row.status,
                SourceSliceStatus::Failed,
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(SourceSliceStatus::Failed);
        active.failure_detail = ActiveValue::Set(Some(detail));
        active.completed_at = ActiveValue::Set(Some(chrono::Utc::now()));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }
}

async fn load_for_update<C>(
    db: &C,
    source_slice_id: &SourceSliceId,
) -> Result<quant_source_slice::Model, StorageError>
where
    C: sea_orm::ConnectionTrait,
{
    quant_source_slice::Entity::find_by_id(source_slice_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_SOURCE_SLICE, source_slice_id))
}

fn validate_new(source_slice: &NewSourceSlice) -> Result<(), StorageError> {
    if source_slice.window_start >= source_slice.window_end
        || source_slice.window_end > source_slice.pit_cutoff
        || source_slice.reader_contract_version.trim().is_empty()
        || source_slice.schema_contract_version.trim().is_empty()
        || !matches!(
            source_slice.evaluation_track.as_str(),
            "research_only" | "semi_auto_candidate"
        )
    {
        return Err(error::invariant_violation(
            Some(entity::QUANT_SOURCE_SLICE),
            "source-slice identity boundaries or contracts are invalid".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_manifest_binding(
    row: &quant_source_slice::Model,
    completion: &CompleteSourceSlice,
) -> Result<(), StorageError> {
    let manifest = &completion.manifest;
    let bound = manifest.profile_ref == row.profile_ref
        && track_name(manifest.evaluation_track) == row.evaluation_track
        && manifest.research_program_hash == row.research_program_hash
        && manifest.decision_policy_snapshot_id == row.decision_policy_snapshot_id
        && manifest.runtime_config_hash == row.runtime_config_hash
        && manifest.window_start == row.window_start
        && manifest.window_end == row.window_end
        && manifest.pit_cutoff == row.pit_cutoff
        && manifest.reader_contract_version == row.reader_contract_version
        && manifest.schema_contract_version == row.schema_contract_version;
    if !bound {
        return Err(error::invariant_violation(
            Some(entity::QUANT_SOURCE_SLICE),
            "source-slice manifest does not match the claimed canonical identity".to_owned(),
        ));
    }
    Ok(())
}

fn ensure_idempotent_completion(
    row: &quant_source_slice::Model,
    completion: &CompleteSourceSlice,
) -> Result<(), StorageError> {
    if row.manifest_uri.as_ref() != Some(&completion.manifest_ref.manifest_uri)
        || row.manifest_hash.as_ref() != Some(&completion.manifest_ref.manifest_hash)
        || row.manifest_json.as_ref() != Some(&completion.manifest)
    {
        return Err(error::state_conflict(
            entity::QUANT_SOURCE_SLICE,
            Some(&row.source_slice_id),
            "ready source slice cannot be rebound to different manifest evidence".to_owned(),
        ));
    }
    Ok(())
}

const fn track_name(track: ResearchEvaluationTrack) -> &'static str {
    match track {
        ResearchEvaluationTrack::ResearchOnly => "research_only",
        ResearchEvaluationTrack::SemiAutoCandidate => "semi_auto_candidate",
    }
}
