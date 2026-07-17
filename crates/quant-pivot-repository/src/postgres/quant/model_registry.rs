//! Postgres-backed model registry repository.

use crate::{
    postgres::{
        error,
        governance::runtime_config,
        quant::feature_parity,
        query::{paginate_into_model, paginate_mapped},
    },
    traits::{
        ModelRegistryRepository, PublishModelVersionCommit, PublishModelVersionOutcome,
        RollbackModelVersionCommit,
    },
};
use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        ModelSpecInfo, ModelSpecListQuery, ModelVersionInfo, ModelVersionListQuery,
        ModelVersionParityEvidence, NewModelSpec, NewModelVersion, PageWindow, Paginated,
        model_version_parity_evidence_hash,
    },
    entities::{
        quant_feature_parity_run, quant_feature_parity_subject, quant_model_spec,
        quant_model_version, quant_training_dataset,
    },
    enums::quant::{
        FeatureParityRunKind, FeatureParityRunStatus, ParitySubjectKind, PublicationStatus,
    },
    hashing::CanonicalDigest,
    types::{BacktestPathSetId, ContentHash, FeatureParityRunId, ModelSpecId, ModelVersionId},
};
use sea_orm::{
    ActiveModelTrait, ActiveValue, ColumnTrait, Condition, ConnectionTrait, DatabaseConnection,
    EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait,
    Select, TransactionTrait,
};
/// Postgres-backed model registry repository.
pub struct PgModelRegistryRepository {
    db: DatabaseConnection,
}

impl PgModelRegistryRepository {
    pub const fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

/// Version row + owning-spec `model_family` (N:1 INNER JOIN).
fn select_version_with_family() -> Select<quant_model_version::Entity> {
    quant_model_version::Entity::find()
        .join(
            JoinType::InnerJoin,
            quant_model_version::Relation::ModelSpec.def(),
        )
        .column_as(quant_model_spec::Column::ModelFamily, "model_family")
}

async fn find_version_info(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
) -> Result<Option<ModelVersionInfo>, StorageError> {
    select_version_with_family()
        .filter(quant_model_version::Column::ModelVersionId.eq(model_version_id.clone()))
        .into_model::<ModelVersionInfo>()
        .one(db)
        .await
        .map_err(StorageError::from)
}

async fn require_version_info(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
) -> Result<ModelVersionInfo, StorageError> {
    find_version_info(db, model_version_id)
        .await?
        .ok_or_else(|| error::not_found(entity::QUANT_MODEL_VERSION, model_version_id))
}

#[async_trait::async_trait]
impl ModelRegistryRepository for PgModelRegistryRepository {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        spec.input_contract
            .validate()
            .map_err(|detail| StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_SPEC),
                detail: format!("invalid input_contract: {detail}"),
            })?;
        if spec.input_contract.inputs.is_empty() {
            return Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_SPEC),
                detail: "input_contract must contain at least one raw feature".to_owned(),
            });
        }
        spec.training_contract
            .validate()
            .map_err(|detail| StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_SPEC),
                detail: format!("invalid training_contract: {detail}"),
            })?;
        let name = spec.name.clone();
        quant_model_spec::Entity::insert(spec.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|err| error::map_unique(err, entity::QUANT_MODEL_SPEC, &name))
            .map(Into::into)
    }

    async fn find_model_spec_by_id(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError> {
        quant_model_spec::Entity::find_by_id(model_spec_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)
            .map(|row| row.map(Into::into))
    }

    async fn create_model_version(
        &self,
        mut version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        // Allocate `version` under an exclusive lock on the owning spec so
        // concurrent trainers cannot mint the same `(model_spec_id, version)`.
        // Callers may pass a preview from `next_version_for_spec`; the locked
        // MAX+1 here is authoritative.
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(_spec) = quant_model_spec::Entity::find_by_id(version.model_spec_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_SPEC,
                &version.model_spec_id,
            ));
        };
        let max_version = quant_model_version::Entity::find()
            .filter(quant_model_version::Column::ModelSpecId.eq(version.model_spec_id.clone()))
            .select_only()
            .column_as(quant_model_version::Column::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        version.version = next_model_version(max_version.flatten())?;
        let duplicate_key = format!("{}:v{}", version.model_spec_id, version.version);
        let model_version_id = version.model_version_id.clone();
        quant_model_version::Entity::insert(version.into_active_model())
            .exec_with_returning(&txn)
            .await
            .map_err(|err| error::map_unique(err, entity::QUANT_MODEL_VERSION, &duplicate_key))?;
        let info = require_version_info(&txn, &model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn next_version_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<i32, StorageError> {
        // Preview only — `create_model_version` re-allocates under lock.
        let max_version = quant_model_version::Entity::find()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .select_only()
            .column_as(quant_model_version::Column::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        next_model_version(max_version.flatten())
    }

    async fn find_model_version_by_id(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        find_version_info(&self.db, model_version_id).await
    }

    async fn page_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> Result<Paginated<ModelSpecInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_family
                    .map(|family| quant_model_spec::Column::ModelFamily.eq(family)),
            )
            .add_option(
                query
                    .status
                    .map(|status| quant_model_spec::Column::Status.eq(status)),
            );
        paginate_mapped(
            quant_model_spec::Entity::find()
                .filter(condition)
                .order_by_desc(quant_model_spec::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await
    }

    async fn page_versions(
        &self,
        query: ModelVersionListQuery,
    ) -> Result<Paginated<ModelVersionInfo>, StorageError> {
        let condition = Condition::all()
            .add_option(
                query
                    .model_spec_id
                    .clone()
                    .map(|id| quant_model_version::Column::ModelSpecId.eq(id)),
            )
            .add_option(
                query
                    .publication_status
                    .map(|status| quant_model_version::Column::PublicationStatus.eq(status)),
            )
            .add_option(
                query
                    .from
                    .map(|from| quant_model_version::Column::CreatedAt.gte(from)),
            )
            .add_option(
                query
                    .to
                    .map(|to| quant_model_version::Column::CreatedAt.lt(to)),
            );
        paginate_into_model(
            select_version_with_family()
                .filter(condition)
                .order_by_desc(quant_model_version::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
        )
        .await
    }

    async fn list_published_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError> {
        select_version_with_family()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
            .order_by_desc(quant_model_version::Column::PublishedAt)
            .into_model::<ModelVersionInfo>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn retire_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Retired).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn publish_replacing_predecessors(
        &self,
        commit: PublishModelVersionCommit<'_>,
    ) -> Result<PublishModelVersionOutcome, StorageError> {
        let PublishModelVersionCommit {
            model_spec_id,
            model_version_id,
            feature_parity_permit,
            feature_parity_run_id,
            expected_runtime_config_activation_id,
            runtime_config_activation,
        } = commit;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let feature_parity_state_id = feature_parity::resolve_publish_latch_generation(
            &txn,
            feature_parity_permit,
            feature_parity_run_id,
        )
        .await?;
        // Serialize concurrent publishes for the same spec.
        let Some(_spec) = quant_model_spec::Entity::find_by_id(model_spec_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(entity::QUANT_MODEL_SPEC, model_spec_id));
        };
        let Some(target) = quant_model_version::Entity::find_by_id(model_version_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        if target.model_spec_id != *model_spec_id {
            return Err(StorageError::state_conflict(
                entity::QUANT_MODEL_VERSION,
                Some(model_version_id),
                "model version does not belong to the locked model spec",
            ));
        }
        verify_frozen_model_parity_permit(&txn, feature_parity_run_id, &target).await?;

        let predecessors = select_version_with_family()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
            .order_by_desc(quant_model_version::Column::PublishedAt)
            .into_model::<ModelVersionInfo>()
            .all(&txn)
            .await
            .map_err(StorageError::from)?;
        let rollback_target = predecessors.first().cloned();

        let mut retired = Vec::new();
        for predecessor in &predecessors {
            if predecessor.model_version_id == *model_version_id {
                continue;
            }
            update_model_version_status(
                &txn,
                &predecessor.model_version_id,
                PublicationStatus::Retired,
            )
            .await?;
            retired.push(predecessor.model_version_id.clone());
        }

        let published =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Published)
                .await?;
        runtime_config::append_activation_if_current(
            &txn,
            expected_runtime_config_activation_id,
            Some(runtime_config_activation),
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(PublishModelVersionOutcome {
            published,
            retired_predecessors: retired,
            rollback_target,
            feature_parity_state_id,
        })
    }

    async fn promote_model_to_shadow(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Shadow).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn rollback_to_retired_predecessor(
        &self,
        commit: RollbackModelVersionCommit<'_>,
    ) -> Result<(ModelVersionInfo, ModelVersionInfo), StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        feature_parity::verify_clear_latch_generation(&txn, commit.feature_parity_state_id).await?;
        let (current, target) = lock_model_switch_subject(
            &txn,
            ModelSwitchSubject {
                spec: commit.model_spec_id,
                current: commit.expected_current_model_version_id,
                target: commit.target_model_version_id,
            },
        )
        .await?;
        verify_locked_rollback_subject(&txn, &commit, &current, &target).await?;

        let retired = update_model_version_status(
            &txn,
            commit.expected_current_model_version_id,
            PublicationStatus::Retired,
        )
        .await?;
        let restored = update_model_version_status(
            &txn,
            commit.target_model_version_id,
            PublicationStatus::Published,
        )
        .await?;
        runtime_config::append_activation_if_current(
            &txn,
            commit.expected_runtime_config_activation_id,
            Some(commit.runtime_config_activation),
        )
        .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok((retired, restored))
    }

    async fn set_quality_gate_report(
        &self,
        model_version_id: &ModelVersionId,
        quality_gate_report: serde_json::Value,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let mut active = row.into_active_model();
        active.quality_gate_report = ActiveValue::Set(quality_gate_report);
        active.update(&txn).await.map_err(StorageError::from)?;
        let info = require_version_info(&txn, model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn set_publish_path_set_id(
        &self,
        model_version_id: &ModelVersionId,
        publish_path_set_id: Option<BacktestPathSetId>,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(error::not_found(
                entity::QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let mut active = row.into_active_model();
        active.publish_path_set_id = ActiveValue::Set(publish_path_set_id);
        active.update(&txn).await.map_err(StorageError::from)?;
        let info = require_version_info(&txn, model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }
}

fn next_model_version(current: Option<i32>) -> Result<i32, StorageError> {
    current.unwrap_or(0).checked_add(1).ok_or_else(|| {
        error::invariant_violation(
            Some(entity::QUANT_MODEL_VERSION),
            "model version sequence exhausted i32 capacity",
        )
    })
}

#[derive(Clone, Copy)]
struct ModelSwitchSubject<'a> {
    spec: &'a ModelSpecId,
    current: &'a ModelVersionId,
    target: &'a ModelVersionId,
}

async fn lock_model_switch_subject(
    txn: &sea_orm::DatabaseTransaction,
    subject: ModelSwitchSubject<'_>,
) -> Result<(quant_model_version::Model, quant_model_version::Model), StorageError> {
    let Some(_spec) = quant_model_spec::Entity::find_by_id(subject.spec.clone())
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(entity::QUANT_MODEL_SPEC, subject.spec));
    };

    // The spec lock serializes every publish/rollback for this scope. Lock both
    // concrete versions before validating either status so a stale request can
    // never expose one half of the switch.
    let Some(current) = quant_model_version::Entity::find_by_id(subject.current.clone())
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(
            entity::QUANT_MODEL_VERSION,
            subject.current,
        ));
    };
    let Some(target) = quant_model_version::Entity::find_by_id(subject.target.clone())
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(
            entity::QUANT_MODEL_VERSION,
            subject.target,
        ));
    };
    Ok((current, target))
}

async fn verify_locked_rollback_subject(
    txn: &sea_orm::DatabaseTransaction,
    commit: &RollbackModelVersionCommit<'_>,
    current: &quant_model_version::Model,
    target: &quant_model_version::Model,
) -> Result<(), StorageError> {
    let published_ids = quant_model_version::Entity::find()
        .filter(quant_model_version::Column::ModelSpecId.eq(commit.model_spec_id.clone()))
        .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
        .select_only()
        .column(quant_model_version::Column::ModelVersionId)
        .into_tuple::<ModelVersionId>()
        .all(txn)
        .await
        .map_err(StorageError::from)?;
    validate_rollback_transition(RollbackTransitionState {
        expected_spec_id: commit.model_spec_id,
        expected_current_id: commit.expected_current_model_version_id,
        current_spec_id: &current.model_spec_id,
        current_status: current.publication_status,
        target_id: commit.target_model_version_id,
        target_spec_id: &target.model_spec_id,
        target_status: target.publication_status,
        published_ids: &published_ids,
    })?;
    verify_locked_rollback_artifact(commit, target)?;
    verify_frozen_model_parity_permit(txn, commit.feature_parity_run_id, target).await
}

fn verify_locked_rollback_artifact(
    commit: &RollbackModelVersionCommit<'_>,
    target: &quant_model_version::Model,
) -> Result<(), StorageError> {
    if &target.artifact_hash != commit.expected_target_artifact_hash {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(commit.target_model_version_id),
            format!(
                "rollback artifact changed after validation: expected {}, found {}",
                commit.expected_target_artifact_hash, target.artifact_hash
            ),
        ));
    }
    if target.publish_path_set_id.as_ref() != commit.expected_target_publish_path_set_id {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(commit.target_model_version_id),
            "rollback publish path-set binding changed after quality-gate evaluation",
        ));
    }
    verify_persisted_rollback_gate_report(
        commit.target_model_version_id,
        &target.quality_gate_report,
        commit.quality_gate_payload_hash,
    )
}

fn verify_persisted_rollback_gate_report(
    target_id: &ModelVersionId,
    report: &serde_json::Value,
    expected_payload_hash: &ContentHash,
) -> Result<(), StorageError> {
    let passed = report.get("passed").and_then(serde_json::Value::as_bool);
    let intent = report.get("intent").and_then(serde_json::Value::as_str);
    let subject_kind = report
        .pointer("/subject/kind")
        .and_then(serde_json::Value::as_str);
    let subject_id = report
        .pointer("/subject/id")
        .and_then(serde_json::Value::as_str);
    let persisted_payload_hash = CanonicalDigest::content_hash_json(report).map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_MODEL_VERSION),
            format!("persisted rollback gate report is not canonical-hashable: {error}"),
        )
    })?;
    let expected_subject_id = target_id.to_string();
    if passed != Some(true)
        || intent != Some("publish")
        || subject_kind != Some("model_version")
        || subject_id != Some(expected_subject_id.as_str())
        || &persisted_payload_hash != expected_payload_hash
    {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(target_id),
            format!(
                "rollback target has no exact canonical passed publish gate payload {expected_payload_hash}; persisted hash={persisted_payload_hash}, passed={passed:?}, intent={intent:?}, subject_kind={subject_kind:?}, subject_id={subject_id:?}"
            ),
        ));
    }
    Ok(())
}

async fn verify_frozen_model_parity_permit(
    txn: &sea_orm::DatabaseTransaction,
    run_id: &FeatureParityRunId,
    model: &quant_model_version::Model,
) -> Result<(), StorageError> {
    let model_version_id = &model.model_version_id;
    let training_dataset_id = model.training_dataset_id.as_ref().ok_or_else(|| {
        StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(model_version_id),
            "model parity permit requires an exact training dataset binding",
        )
    })?;
    let run = quant_feature_parity_run::Entity::find_by_id(run_id.clone())
        .lock_exclusive()
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_FEATURE_PARITY_RUN, run_id))?;
    let valid = run.kind == FeatureParityRunKind::Full
        && run.status == FeatureParityRunStatus::Passed
        && run.report_id.is_none()
        && run.model_version_id.as_ref() == Some(model_version_id)
        && run.training_dataset_id.as_ref() == Some(training_dataset_id)
        && run.total_count > 0
        && run.compared_count == run.total_count
        && run.matched_count == run.total_count
        && run.mismatched_count == 0
        && run.pending_materialization_count == 0
        && run.feature_contract_hash.is_some()
        && run.transform_hash.is_some()
        && run
            .finished_at
            .is_some_and(|finished_at| finished_at >= model.created_at);
    if !valid {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(run_id),
            format!(
                "full parity run is not a complete permit for model {model_version_id} and dataset {training_dataset_id}"
            ),
        ));
    }
    let dataset = quant_training_dataset::Entity::find_by_id(training_dataset_id.clone())
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found("quant_training_dataset", training_dataset_id))?;
    let (Some(dataset_hash), Some(manifest_hash), Some(artifact_bytes_hash)) = (
        dataset.dataset_hash.as_ref(),
        dataset.manifest_hash.as_ref(),
        dataset.artifact_bytes_hash.as_ref(),
    ) else {
        return Err(StorageError::state_conflict(
            "quant_training_dataset",
            Some(training_dataset_id),
            "model parity permit dataset has no complete immutable artifact binding",
        ));
    };
    let subject = quant_feature_parity_subject::Entity::find()
        .filter(quant_feature_parity_subject::Column::RunId.eq(run_id.clone()))
        .filter(
            quant_feature_parity_subject::Column::SubjectKind.eq(ParitySubjectKind::ModelVersion),
        )
        .filter(quant_feature_parity_subject::Column::ModelVersionId.eq(model_version_id.clone()))
        .filter(
            quant_feature_parity_subject::Column::TrainingDatasetId.eq(training_dataset_id.clone()),
        )
        .one(txn)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| {
            StorageError::state_conflict(
                entity::QUANT_FEATURE_PARITY_RUN,
                Some(run_id),
                "full model parity run has no exact WORM model-version subject",
            )
        })?;
    let evidence_hash = model_version_parity_evidence_hash(&ModelVersionParityEvidence {
        model_version_id,
        model_spec_id: &model.model_spec_id,
        artifact_hash: &model.artifact_hash,
        training_dataset_id,
        dataset_hash,
        manifest_hash,
        artifact_bytes_hash,
    })
    .map_err(|error| {
        StorageError::invariant_violation(
            Some(entity::QUANT_FEATURE_PARITY_RUN),
            format!("model-version parity subject hash failed: {error}"),
        )
    })?;
    if subject.subject_generation != model.artifact_hash || subject.evidence_hash != evidence_hash {
        return Err(StorageError::state_conflict(
            entity::QUANT_FEATURE_PARITY_RUN,
            Some(run_id),
            "full model parity WORM subject no longer matches the exact model/dataset artifacts",
        ));
    }
    Ok(())
}

/// Validate the rollback compare-and-swap state before either status is
/// mutated. Kept independent of `SeaORM` so stale-current, cross-spec and
/// half-switched states have fast deterministic unit coverage.
#[derive(Clone, Copy)]
struct RollbackTransitionState<'a> {
    expected_spec_id: &'a ModelSpecId,
    expected_current_id: &'a ModelVersionId,
    current_spec_id: &'a ModelSpecId,
    current_status: PublicationStatus,
    target_id: &'a ModelVersionId,
    target_spec_id: &'a ModelSpecId,
    target_status: PublicationStatus,
    published_ids: &'a [ModelVersionId],
}

fn validate_rollback_transition(state: RollbackTransitionState<'_>) -> Result<(), StorageError> {
    let RollbackTransitionState {
        expected_spec_id,
        expected_current_id,
        current_spec_id,
        current_status,
        target_id,
        target_spec_id,
        target_status,
        published_ids,
    } = state;
    if expected_current_id == target_id {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(target_id),
            "rollback current and target model versions must differ",
        ));
    }
    if current_spec_id != expected_spec_id || target_spec_id != expected_spec_id {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(target_id),
            "rollback current and target must both belong to the locked model spec",
        ));
    }
    if current_status != PublicationStatus::Published {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(expected_current_id),
            format!(
                "expected rollback current to be published, found {}",
                current_status.as_str()
            ),
        ));
    }
    if target_status != PublicationStatus::Retired {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_VERSION,
            Some(target_id),
            format!(
                "expected rollback target to be retired, found {}",
                target_status.as_str()
            ),
        ));
    }
    if published_ids != [expected_current_id.clone()] {
        return Err(StorageError::state_conflict(
            entity::QUANT_MODEL_SPEC,
            Some(expected_spec_id),
            format!(
                "rollback compare-and-swap expected sole published version {expected_current_id}, found [{}]",
                published_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        ));
    }
    Ok(())
}

/// Transition a version's publication status under an existing connection/txn.
/// Always `SELECT … FOR UPDATE`s the version row before mutating.
async fn update_model_version_status(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
    next: PublicationStatus,
) -> Result<ModelVersionInfo, StorageError> {
    let Some(row) = quant_model_version::Entity::find_by_id(model_version_id.clone())
        .lock_exclusive()
        .one(db)
        .await
        .map_err(StorageError::from)?
    else {
        return Err(error::not_found(
            entity::QUANT_MODEL_VERSION,
            model_version_id,
        ));
    };
    let from = row.publication_status;
    if !from.allows_transition_to(next) {
        return Err(error::illegal_transition(
            entity::QUANT_MODEL_VERSION,
            Some(model_version_id),
            from,
            next,
        ));
    }
    if from == next {
        return require_version_info(db, model_version_id).await;
    }
    let mut active = row.into_active_model();
    active.publication_status = ActiveValue::Set(next);
    match next {
        PublicationStatus::Published => {
            active.published_at = ActiveValue::Set(Some(Utc::now()));
            active.retired_at = ActiveValue::Set(None);
        }
        PublicationStatus::Retired => {
            active.retired_at = ActiveValue::Set(Some(Utc::now()));
        }
        _ => {}
    }
    active.update(db).await.map_err(StorageError::from)?;
    require_version_info(db, model_version_id).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::slice;

    #[test]
    fn model_version_sequence_is_checked() {
        assert_eq!(next_model_version(None).expect("initial version"), 1);
        assert_eq!(next_model_version(Some(41)).expect("next version"), 42);
        let error = next_model_version(Some(i32::MAX)).expect_err("overflow must fail closed");
        assert!(error.to_string().contains("sequence exhausted"));
    }

    struct RollbackFixture {
        spec: ModelSpecId,
        current: ModelVersionId,
        target: ModelVersionId,
    }

    impl RollbackFixture {
        fn new() -> Self {
            Self {
                spec: ModelSpecId::from_v7(),
                current: ModelVersionId::from_v7(),
                target: ModelVersionId::from_v7(),
            }
        }

        fn state<'a>(
            &'a self,
            current_spec_id: &'a ModelSpecId,
            current_status: PublicationStatus,
            target_spec_id: &'a ModelSpecId,
            target_status: PublicationStatus,
            published_ids: &'a [ModelVersionId],
        ) -> RollbackTransitionState<'a> {
            RollbackTransitionState {
                expected_spec_id: &self.spec,
                expected_current_id: &self.current,
                current_spec_id,
                current_status,
                target_id: &self.target,
                target_spec_id,
                target_status,
                published_ids,
            }
        }
    }

    #[test]
    fn rollback_compare_and_swap_accepts_exact_published_current_and_retired_target() {
        let fixture = RollbackFixture::new();
        validate_rollback_transition(fixture.state(
            &fixture.spec,
            PublicationStatus::Published,
            &fixture.spec,
            PublicationStatus::Retired,
            slice::from_ref(&fixture.current),
        ))
        .expect("exact rollback state is valid");
    }

    #[test]
    fn rollback_compare_and_swap_rejects_stale_cross_spec_and_half_switched_states() {
        let fixture = RollbackFixture::new();
        let other_spec = ModelSpecId::from_v7();

        let stale_current = validate_rollback_transition(fixture.state(
            &fixture.spec,
            PublicationStatus::Published,
            &fixture.spec,
            PublicationStatus::Retired,
            slice::from_ref(&fixture.target),
        ))
        .expect_err("stale current must fail closed");
        assert!(stale_current.to_string().contains("sole published version"));

        let cross_spec = validate_rollback_transition(fixture.state(
            &fixture.spec,
            PublicationStatus::Published,
            &other_spec,
            PublicationStatus::Retired,
            slice::from_ref(&fixture.current),
        ))
        .expect_err("cross-spec target must fail closed");
        assert!(cross_spec.to_string().contains("locked model spec"));

        let already_restored = validate_rollback_transition(fixture.state(
            &fixture.spec,
            PublicationStatus::Retired,
            &fixture.spec,
            PublicationStatus::Published,
            slice::from_ref(&fixture.target),
        ))
        .expect_err("half-switched state must fail closed");
        assert!(
            already_restored
                .to_string()
                .contains("current to be published")
        );
    }

    #[test]
    fn rollback_commit_requires_exact_persisted_passed_gate_report() {
        let target_id = ModelVersionId::from_v7();
        let report = serde_json::json!({
            "subject": {
                "kind": "model_version",
                "id": target_id.to_string(),
            },
            "intent": "publish",
            "passed": true,
            // This embedded decision hash is deliberately unrelated to the
            // commit permit, which hashes the exact persisted JSON payload.
            "report_hash": format!("blake3:{}", "a".repeat(64)),
        });
        let expected = CanonicalDigest::content_hash_json(&report).expect("payload hash");
        verify_persisted_rollback_gate_report(&target_id, &report, &expected)
            .expect("exact passed report is a valid commit permit");

        let mut failed_report = report.clone();
        failed_report["passed"] = serde_json::Value::Bool(false);
        let failed_hash =
            CanonicalDigest::content_hash_json(&failed_report).expect("failed payload hash");
        let failed =
            verify_persisted_rollback_gate_report(&target_id, &failed_report, &failed_hash)
                .expect_err("failed quality report must block rollback commit");
        assert!(failed.to_string().contains("canonical passed publish gate"));

        let mut stale_report = report;
        stale_report["report_hash"] =
            serde_json::Value::String(format!("blake3:{}", "b".repeat(64)));
        let stale = verify_persisted_rollback_gate_report(&target_id, &stale_report, &expected)
            .expect_err("stale gate report must block rollback commit");
        assert!(stale.to_string().contains("canonical passed publish gate"));
    }
}
