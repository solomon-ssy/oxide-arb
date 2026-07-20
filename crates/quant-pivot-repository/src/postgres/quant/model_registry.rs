//! Postgres-backed model registry repository.

use crate::{
    postgres::{
        error,
        quant::{feature_parity, research_profile},
        query::{paginate_into_model, paginate_mapped},
    },
    traits::{ModelRegistryRepository, PublishModelVersionCommit, PublishModelVersionResult},
};
use chrono::Utc;
use quant_pivot_error::storage::{StorageError, entity};
use quant_pivot_models::{
    domain::{
        CalibrationArtifactPayload, ModelPickerSide, ModelSpecInfo, ModelSpecListQuery,
        ModelVersionInfo, ModelVersionListQuery, ModelVersionParityEvidence, NewModelSpec,
        NewModelVersion, PageWindow, Paginated, PublishedModelCatalogInfo,
        model_version_parity_evidence_hash,
    },
    entities::{
        quant_backtest_report, quant_calibration_artifact, quant_feature_parity_run,
        quant_feature_parity_subject, quant_model_spec, quant_model_version,
        quant_training_dataset, research_profile_artifact,
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            CalibrationKind, FeatureParityRunKind, FeatureParityRunStatus, ParitySubjectKind,
            PublicationStatus,
        },
    },
    types::{
        BacktestPathSetId, FeatureParityRunId, ModelSpecId, ModelVersionId,
        model_lineage::ModelVersionDerivation, model_quality::QualityGateReport,
        model_spec::ModelSpecDefinition,
    },
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

    /// Seed or verify the complete immutable built-in research-profile registry.
    pub async fn ensure_builtin_research_profiles(&self) -> Result<(), StorageError> {
        research_profile::ensure_builtins(&self.db).await.map(drop)
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
        .column_as(quant_model_spec::Column::Name, "model_spec_name")
        .column_as(quant_model_spec::Column::Thesis, "model_spec_thesis")
        .column_as(
            quant_model_spec::Column::DefinitionHash,
            "model_spec_definition_hash",
        )
        .join(
            JoinType::InnerJoin,
            quant_model_version::Relation::ResearchProfileArtifact.def(),
        )
        .column_as(
            research_profile_artifact::Column::ResearchProfileId,
            "profile_ref_id",
        )
        .column_as(
            research_profile_artifact::Column::Version,
            "profile_ref_version",
        )
        .column_as(
            research_profile_artifact::Column::ContentHash,
            "profile_ref_content_hash",
        )
}

async fn find_version_info(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
) -> Result<Option<ModelVersionInfo>, StorageError> {
    let info = select_version_with_family()
        .filter(quant_model_version::Column::ModelVersionId.eq(model_version_id.clone()))
        .into_model::<ModelVersionInfo>()
        .one(db)
        .await
        .map_err(StorageError::from)?;
    info.map(verify_model_version_info).transpose()
}

async fn require_version_info(
    db: &impl ConnectionTrait,
    model_version_id: &ModelVersionId,
) -> Result<ModelVersionInfo, StorageError> {
    find_version_info(db, model_version_id)
        .await?
        .ok_or_else(|| error::not_found(entity::QUANT_MODEL_VERSION, model_version_id))
}

fn verify_model_spec_info(info: ModelSpecInfo) -> Result<ModelSpecInfo, StorageError> {
    let definition = info.definition();
    definition
        .validate()
        .map_err(|detail| StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_SPEC),
            detail: format!("invalid stored model spec definition: {detail}"),
        })?;
    let expected_hash =
        definition
            .content_hash()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_SPEC),
                detail: format!("stored model spec hash failed: {error}"),
            })?;
    if expected_hash != info.definition_hash {
        return Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_SPEC),
            detail: format!(
                "stored model spec {} definition hash mismatch: expected {expected_hash}, got {}",
                info.model_spec_id, info.definition_hash
            ),
        });
    }
    Ok(info)
}

fn verify_model_version_info(info: ModelVersionInfo) -> Result<ModelVersionInfo, StorageError> {
    info.verified_derivation()
        .map_err(|error| StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_VERSION),
            detail: format!(
                "stored model version {} has invalid derivation lineage: {error}",
                info.model_version_id
            ),
        })?;
    Ok(info)
}

async fn validate_version_derivation(
    db: &impl ConnectionTrait,
    version: &NewModelVersion,
) -> Result<(), StorageError> {
    version
        .derivation
        .validate()
        .map_err(|error| StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_VERSION),
            detail: error.to_string(),
        })?;

    let Some(parent_id) = version.derivation.parent_model_version_id() else {
        return Ok(());
    };
    if parent_id == &version.model_version_id {
        return Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_VERSION),
            detail: "a model version cannot derive from itself".to_owned(),
        });
    }
    let parent = quant_model_version::Entity::find_by_id(parent_id.clone())
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| error::not_found(entity::QUANT_MODEL_VERSION, parent_id))?;
    let metadata_matches = parent.model_spec_id == version.model_spec_id
        && parent.category_scope == version.category_scope
        && parent.research_profile_artifact_id == version.profile_ref.artifact_id()
        && parent.training_dataset_id == version.training_dataset_id
        && parent.trade_policy_artifact_id == version.trade_policy_artifact_id
        && parent.trade_policy_hash == version.trade_policy_hash;
    if !metadata_matches {
        return Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_VERSION),
            detail: format!("derived version metadata must match parent model version {parent_id}"),
        });
    }
    if version.publication_status != PublicationStatus::Candidate
        || version.publish_path_set_id.is_some()
        || version.quality_gate_report.is_some()
        || version.published_at.is_some()
        || version.retired_at.is_some()
    {
        return Err(StorageError::InvariantViolation {
            entity: Some(entity::QUANT_MODEL_VERSION),
            detail: "a derived model version must start as an ungated Candidate".to_owned(),
        });
    }

    match &version.derivation {
        ModelVersionDerivation::Training => {}
        ModelVersionDerivation::ScoreMultiplierCalibration {
            source_backtest_report_id,
            ..
        } => {
            let source =
                quant_backtest_report::Entity::find_by_id(source_backtest_report_id.clone())
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| {
                        error::not_found("quant_backtest_report", source_backtest_report_id)
                    })?;
            if source.model_version_id != *parent_id {
                return Err(StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_MODEL_VERSION),
                    detail: format!(
                        "source backtest report {source_backtest_report_id} belongs to model version {}, not parent {parent_id}",
                        source.model_version_id
                    ),
                });
            }
        }
        ModelVersionDerivation::ReturnCalibration {
            calibration_artifact_id,
            ..
        } => {
            let artifact =
                quant_calibration_artifact::Entity::find_by_id(calibration_artifact_id.clone())
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| {
                        error::not_found("quant_calibration_artifact", calibration_artifact_id)
                    })?;
            let CalibrationArtifactPayload::ModelScore(payload) = artifact.payload else {
                return Err(StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_MODEL_VERSION),
                    detail: format!(
                        "calibration artifact {calibration_artifact_id} is not a model_score artifact"
                    ),
                });
            };
            if artifact.kind != CalibrationKind::ModelScore
                || payload.model_version_id != *parent_id
            {
                return Err(StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_MODEL_VERSION),
                    detail: format!(
                        "calibration artifact {calibration_artifact_id} was not fitted for parent model version {parent_id}"
                    ),
                });
            }
        }
    }
    Ok(())
}

#[async_trait::async_trait]
impl ModelRegistryRepository for PgModelRegistryRepository {
    async fn create_model_spec(&self, spec: NewModelSpec) -> Result<ModelSpecInfo, StorageError> {
        let definition = ModelSpecDefinition {
            name: &spec.name,
            model_family: spec.model_family,
            prediction_horizon_secs: spec.prediction_horizon_secs,
            feature_schema_version: spec.feature_schema_version,
            label_schema_version: spec.label_schema_version,
            thesis: &spec.thesis,
            input_contract: &spec.input_contract,
            training_contract: &spec.training_contract,
        };
        definition
            .validate()
            .map_err(|detail| StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_SPEC),
                detail: format!("invalid model spec definition: {detail}"),
            })?;
        let expected_hash =
            definition
                .content_hash()
                .map_err(|error| StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_MODEL_SPEC),
                    detail: format!("model spec definition hash failed: {error}"),
                })?;
        if spec.definition_hash != expected_hash {
            return Err(StorageError::InvariantViolation {
                entity: Some(entity::QUANT_MODEL_SPEC),
                detail: format!(
                    "definition_hash mismatch: expected {expected_hash}, got {}",
                    spec.definition_hash
                ),
            });
        }
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
        let row = quant_model_spec::Entity::find_by_id(model_spec_id.clone())
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        row.map(Into::into).map(verify_model_spec_info).transpose()
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
        research_profile::ensure(&txn, &version.profile_ref).await?;
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
        validate_version_derivation(&txn, &version).await?;
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
        let active =
            version
                .try_into_active_model()
                .map_err(|error| StorageError::InvariantViolation {
                    entity: Some(entity::QUANT_MODEL_VERSION),
                    detail: error.to_string(),
                })?;
        quant_model_version::Entity::insert(active)
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
        let condition = Condition::all().add_option(
            query
                .model_family
                .map(|family| quant_model_spec::Column::ModelFamily.eq(family)),
        );
        let page = paginate_mapped(
            quant_model_spec::Entity::find()
                .filter(condition)
                .order_by_desc(quant_model_spec::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
            Into::into,
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(verify_model_spec_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::new(items, page.total, page.page, page.size))
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
        let page = paginate_into_model(
            select_version_with_family()
                .filter(condition)
                .order_by_desc(quant_model_version::Column::CreatedAt),
            &self.db,
            PageWindow::from_query(&query),
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(verify_model_version_info)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }

    async fn list_published_catalog(
        &self,
        side: ModelPickerSide,
        category: Option<MarketCategory>,
    ) -> Result<Vec<PublishedModelCatalogInfo>, StorageError> {
        let family = match side {
            ModelPickerSide::Buy => {
                quant_model_spec::Column::ModelFamily.ne(ModelFamily::HoldVsExitWeighted)
            }
            ModelPickerSide::Sell => {
                quant_model_spec::Column::ModelFamily.eq(ModelFamily::HoldVsExitWeighted)
            }
        };
        let category = category.map(|category| {
            Condition::any()
                .add(quant_model_version::Column::CategoryScope.is_null())
                .add(quant_model_version::Column::CategoryScope.eq(category))
        });

        quant_model_version::Entity::find()
            .select_only()
            .column(quant_model_version::Column::ModelVersionId)
            .column(quant_model_version::Column::ModelSpecId)
            .column_as(quant_model_spec::Column::Name, "spec_name")
            .column(quant_model_version::Column::Version)
            .column(quant_model_version::Column::ArtifactHash)
            .column_as(quant_model_spec::Column::ModelFamily, "model_family")
            .column(quant_model_version::Column::CategoryScope)
            .column(quant_model_version::Column::PublishedAt)
            .join(
                JoinType::InnerJoin,
                quant_model_version::Relation::ModelSpec.def(),
            )
            .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
            .filter(family)
            .filter(Condition::all().add_option(category))
            .order_by_asc(quant_model_spec::Column::Name)
            .order_by_desc(quant_model_version::Column::Version)
            .into_model::<PublishedModelCatalogInfo>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)
    }

    async fn list_published_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Vec<ModelVersionInfo>, StorageError> {
        let rows = select_version_with_family()
            .filter(quant_model_version::Column::ModelSpecId.eq(model_spec_id.clone()))
            .filter(quant_model_version::Column::PublicationStatus.eq(PublicationStatus::Published))
            .order_by_desc(quant_model_version::Column::PublishedAt)
            .into_model::<ModelVersionInfo>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)?;
        rows.into_iter().map(verify_model_version_info).collect()
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

    async fn publish_model_version(
        &self,
        commit: PublishModelVersionCommit<'_>,
    ) -> Result<PublishModelVersionResult, StorageError> {
        let PublishModelVersionCommit {
            model_spec_id,
            model_version_id,
            feature_parity_permit,
            feature_parity_run_id,
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

        let published =
            update_model_version_status(&txn, model_version_id, PublicationStatus::Published)
                .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(PublishModelVersionResult {
            published,
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

    async fn set_quality_gate_report(
        &self,
        model_version_id: &ModelVersionId,
        quality_gate_report: QualityGateReport,
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
        active.quality_gate_report = ActiveValue::Set(Some(quality_gate_report));
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

    #[test]
    fn model_version_sequence_is_checked() {
        assert_eq!(next_model_version(None).expect("initial version"), 1);
        assert_eq!(next_model_version(Some(41)).expect("next version"), 42);
        let error = next_model_version(Some(i32::MAX)).expect_err("overflow must fail closed");
        assert!(error.to_string().contains("sequence exhausted"));
    }
}
