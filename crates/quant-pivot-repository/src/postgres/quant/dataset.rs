//! Postgres-backed training-dataset ledger repository.

use chrono::Utc;
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_MODEL_SPEC, QUANT_TRAINING_DATASET},
};
use quant_pivot_models::{
    domain::{
        api::TrainingDatasetListQuery,
        pagination::{PageWindow, Paginated},
        quant::{CompleteTrainingDatasetBuild, NewTrainingDatasetPlan, TrainingDatasetInfo},
    },
    entities::{
        quant_model_spec::{Column, Entity},
        quant_source_slice::Entity as QuantSourceSliceEntity,
        quant_training_dataset::{
            Column as QuantTrainingDatasetColumn, Entity as QuantTrainingDatasetEntity, Model,
        },
        research_profile_artifact::Entity as ResearchProfileArtifactEntity,
    },
    enums::quant::{SourceSliceStatus, TrainingDatasetStatus},
    types::{ContentHash, TrainingDatasetId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, DatabaseConnection, EntityTrait,
    IntoActiveModel, QueryFilter, QueryOrder, QuerySelect, TransactionTrait,
};

use crate::{
    postgres::{error, query::paginate_mapped},
    traits::TrainingDatasetRepository,
};

/// Postgres-backed training-dataset ledger repository.
pub struct PgTrainingDatasetRepository {
    db: DatabaseConnection,
}

impl PgTrainingDatasetRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }

    async fn validate_plan_sources(
        &self,
        plan: &NewTrainingDatasetPlan,
    ) -> Result<(), StorageError> {
        plan.validate().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_TRAINING_DATASET), error.to_string())
        })?;
        if ResearchProfileArtifactEntity::find_by_id(plan.research_profile_artifact_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .is_none()
        {
            return Err(StorageError::not_found(
                "research_profile_artifact",
                &plan.research_profile_artifact_id,
            ));
        }
        let source = QuantSourceSliceEntity::find_by_id(plan.source_slice_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found("quant_source_slice", plan.source_slice_id))?;
        let manifest = source.manifest.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                "dataset source slice has no immutable manifest",
            )
        })?;
        plan.source_lineage
            .verify_manifest(manifest)
            .map_err(|error| {
                StorageError::invariant_violation(Some(QUANT_TRAINING_DATASET), error.to_string())
            })?;
        if source.status != SourceSliceStatus::Ready
            || source.identity_hash != plan.source_lineage.source_slice_identity_hash
            || source.profile_ref.artifact_id() != plan.research_profile_artifact_id
            || source.research_program_hash != plan.source_lineage.research_program_hash
            || source.decision_policy_snapshot_id != plan.decision_policy_snapshot_id
            || source.runtime_config_hash != plan.source_lineage.runtime_config_hash
            || source.window_start != plan.source_lineage.source_window_start
            || source.window_end != plan.source_lineage.source_window_end
            || source.pit_cutoff != plan.pit_cutoff
            || source.reader_contract_version != plan.source_lineage.reader_contract_version
            || source.schema_contract_version != plan.source_lineage.schema_contract_version
            || source.manifest_uri.as_ref() != Some(&plan.source_lineage.source_slice.manifest_uri)
            || source.manifest_hash.as_ref()
                != Some(&plan.source_lineage.source_slice.manifest_hash)
        {
            return Err(StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                "dataset source lineage does not match its Ready Source Slice ledger",
            ));
        }
        Ok(())
    }

    fn validate_completion(
        row: &Model,
        completion: &CompleteTrainingDatasetBuild,
    ) -> Result<(), StorageError> {
        let manifest = completion.manifest();
        manifest.validate().map_err(|error| {
            StorageError::invariant_violation(Some(QUANT_TRAINING_DATASET), error.to_string())
        })?;
        let manifest_hash = manifest.content_hash().map_err(|detail| {
            StorageError::invariant_violation(Some(QUANT_TRAINING_DATASET), detail)
        })?;
        let knowledge_lag_secs = u64::try_from(row.knowledge_lag_secs).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                format!("persisted knowledge_lag_secs is invalid: {error}"),
            )
        })?;
        let sample_interval_secs = u64::try_from(row.sample_interval_secs).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                format!("persisted sample_interval_secs is invalid: {error}"),
            )
        })?;
        let sample_count = u64::try_from(completion.sample_count()).map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                format!("completed sample_count is invalid: {error}"),
            )
        })?;
        let bound = manifest.training_dataset_id == row.training_dataset_id
            && manifest.model_spec_id == row.model_spec_id
            && manifest.model_spec_definition_hash == row.model_spec_definition_hash
            && manifest.source_lineage == row.source_lineage
            && manifest.cohort_manifest == row.cohort_manifest
            && manifest.source_lineage.research_profile_artifact_id
                == row.research_profile_artifact_id
            && manifest.source_lineage.source_slice_id == row.source_slice_id
            && manifest.source_lineage.pit_cutoff == row.pit_cutoff
            && manifest.source_lineage.decision_policy_snapshot_id
                == row.decision_policy_snapshot_id
            && manifest.window_start == row.window_start
            && manifest.window_end == row.window_end
            && manifest.purpose == row.purpose
            && manifest.knowledge_lag_secs == knowledge_lag_secs
            && manifest.sample_interval_secs == sample_interval_secs
            && manifest.horizons_secs == row.horizons_secs.0
            && manifest.feature_schema_hash == *completion.feature_schema_hash()
            && manifest.factor_schema_hash == *completion.factor_schema_hash()
            && manifest.label_schema_hash == *completion.label_schema_hash()
            && manifest.semantic_dataset_hash == *completion.dataset_hash()
            && manifest.sample_count == sample_count
            && manifest_hash == completion.manifest_hash()
            && completion.coverage().built_examples == manifest.sample_count;
        if !bound {
            return Err(StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                format!(
                    "manifest does not match frozen dataset plan and artifact bindings for {}",
                    row.training_dataset_id
                ),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl TrainingDatasetRepository for PgTrainingDatasetRepository {
    async fn create_plan(
        &self,
        plan: NewTrainingDatasetPlan,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        self.validate_plan_sources(&plan).await?;
        let stored_definition_hash = Entity::find_by_id(plan.model_spec_id)
            .select_only()
            .column(Column::DefinitionHash)
            .into_tuple::<ContentHash>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_SPEC, plan.model_spec_id))?;
        if plan.model_spec_definition_hash != stored_definition_hash {
            return Err(StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                format!(
                    "model_spec_definition_hash mismatch for {}: expected {stored_definition_hash}, got {}",
                    plan.model_spec_id, plan.model_spec_definition_hash
                ),
            ));
        }
        let key = plan.training_dataset_id.to_string();
        let mut active = plan.into_active_model();
        active.status = ActiveValue::Set(TrainingDatasetStatus::Planned);
        QuantTrainingDatasetEntity::insert(active)
            .exec_with_returning(&self.db)
            .await
            .map_err(|err| error::map_unique(err, QUANT_TRAINING_DATASET, key.as_str()))
            .map(Into::into)
    }

    async fn find_by_id(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<Option<TrainingDatasetInfo>, StorageError> {
        QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn page(
        &self,
        query: TrainingDatasetListQuery,
    ) -> Result<Paginated<TrainingDatasetInfo>, StorageError> {
        paginate_mapped(
            QuantTrainingDatasetEntity::find()
                .filter(page_condition(&query))
                .order_by_desc(QuantTrainingDatasetColumn::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn start_build(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_TRAINING_DATASET,
                training_dataset_id,
            ));
        };
        if row.status == TrainingDatasetStatus::Building {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        if row.status != TrainingDatasetStatus::Planned {
            return Err(StorageError::illegal_transition(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                row.status,
                TrainingDatasetStatus::Building,
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(TrainingDatasetStatus::Building);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn complete_build(
        &self,
        training_dataset_id: &TrainingDatasetId,
        completion: CompleteTrainingDatasetBuild,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        if !matches!(
            completion.status(),
            TrainingDatasetStatus::Ready
                | TrainingDatasetStatus::InsufficientLabels
                | TrainingDatasetStatus::Failed
        ) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_TRAINING_DATASET),
                format!(
                    "complete_build requires ready, insufficient_labels, or failed; got {}",
                    completion.status()
                ),
            ));
        }
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_TRAINING_DATASET,
                training_dataset_id,
            ));
        };
        if row.status != TrainingDatasetStatus::Building {
            return Err(StorageError::illegal_transition(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                row.status,
                completion.status(),
            ));
        }
        Self::validate_completion(&row, &completion)?;
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(completion.status());
        active.feature_schema_hash = ActiveValue::Set(Some(*completion.feature_schema_hash()));
        active.factor_schema_hash = ActiveValue::Set(Some(*completion.factor_schema_hash()));
        active.label_schema_hash = ActiveValue::Set(Some(*completion.label_schema_hash()));
        active.dataset_hash = ActiveValue::Set(Some(*completion.dataset_hash()));
        active.manifest_hash = ActiveValue::Set(Some(completion.manifest_hash()));
        active.manifest = ActiveValue::Set(Some(completion.manifest().clone()));
        active.artifact_bytes_hash = ActiveValue::Set(Some(completion.artifact_bytes_hash()));
        active.parquet_uri = ActiveValue::Set(Some(completion.parquet_uri().clone()));
        active.sample_count = ActiveValue::Set(Some(completion.sample_count()));
        active.coverage = ActiveValue::Set(Some(completion.coverage().clone()));
        active.failure_detail = ActiveValue::Set(completion.failure_detail().map(str::to_owned));
        active.completed_at = ActiveValue::Set(Some(Utc::now()));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn fail_build(
        &self,
        training_dataset_id: &TrainingDatasetId,
        detail: String,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_TRAINING_DATASET,
                training_dataset_id,
            ));
        };
        if row.status == TrainingDatasetStatus::Failed {
            transaction.commit().await.map_err(StorageError::from)?;
            return Ok(row.into());
        }
        if !matches!(
            row.status,
            TrainingDatasetStatus::Planned | TrainingDatasetStatus::Building
        ) {
            return Err(StorageError::illegal_transition(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                row.status,
                TrainingDatasetStatus::Failed,
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(TrainingDatasetStatus::Failed);
        active.failure_detail = ActiveValue::Set(Some(detail));
        active.completed_at = ActiveValue::Set(Some(Utc::now()));
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }

    async fn expire(
        &self,
        training_dataset_id: &TrainingDatasetId,
    ) -> Result<TrainingDatasetInfo, StorageError> {
        let transaction = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .lock_exclusive()
            .one(&transaction)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_TRAINING_DATASET,
                training_dataset_id,
            ));
        };
        if row.status != TrainingDatasetStatus::Ready {
            return Err(StorageError::illegal_transition(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                row.status,
                TrainingDatasetStatus::Expired,
            ));
        }
        let mut active = row.into_active_model();
        active.status = ActiveValue::Set(TrainingDatasetStatus::Expired);
        let updated = active
            .update(&transaction)
            .await
            .map_err(StorageError::from)?;
        transaction.commit().await.map_err(StorageError::from)?;
        Ok(updated.into())
    }
}

fn page_condition(query: &TrainingDatasetListQuery) -> Condition {
    Condition::all()
        .add_option(
            query
                .model_spec_id
                .map(|id| QuantTrainingDatasetColumn::ModelSpecId.eq(id)),
        )
        .add_option(
            query
                .status
                .map(|status| QuantTrainingDatasetColumn::Status.eq(status)),
        )
        .add_option(
            query
                .purpose
                .map(|purpose| QuantTrainingDatasetColumn::Purpose.eq(purpose)),
        )
        .add_option(
            query
                .from
                .map(|from| QuantTrainingDatasetColumn::CreatedAt.gte(from)),
        )
        .add_option(
            query
                .to
                .map(|to| QuantTrainingDatasetColumn::CreatedAt.lt(to)),
        )
}
