//! Postgres-backed model registry repository.

use chrono::Utc;
use quant_pivot_error::storage::{
    StorageError,
    entity::{QUANT_FEATURE_PARITY_RUN, QUANT_MODEL_SPEC, QUANT_MODEL_VERSION},
};
use quant_pivot_models::{
    domain::{
        api::{ModelPickerSide, ModelSpecListQuery, ModelVersionListQuery},
        pagination::{PageWindow, Paginated},
        quant::{
            CalibrationArtifactPayload, ModelSpecInfo, ModelVersionInfo,
            ModelVersionParityEvidence, NewModelSpec, NewModelVersion, PublishedModelCatalogInfo,
        },
    },
    entities::{
        quant_backtest_report::Entity as QuantBacktestReportEntity,
        quant_calibration_artifact::Entity as QuantCalibrationArtifactEntity,
        quant_feature_parity_run::Entity as QuantFeatureParityRunEntity,
        quant_feature_parity_subject::{
            Column as QuantFeatureParitySubjectColumn, Entity as QuantFeatureParitySubjectEntity,
        },
        quant_model_spec::{Column, Entity as QuantModelSpecEntity},
        quant_model_version::{Column as QuantModelVersionColumn, Entity, Model, Relation},
        quant_training_dataset::Entity as QuantTrainingDatasetEntity,
        research_profile_artifact::Column as ResearchProfileArtifactColumn,
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
    DatabaseTransaction, EntityTrait, IntoActiveModel, JoinType, QueryFilter, QueryOrder,
    QuerySelect, RelationTrait, Select, TransactionTrait,
};

use crate::{
    postgres::{
        error,
        quant::feature_parity::PgFeatureParityRepository,
        query::{paginate_into_model, paginate_mapped},
    },
    traits::{ModelRegistryRepository, PublishModelVersionCommit, PublishModelVersionResult},
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
        Self::ensure_builtins(&self.db).await.map(drop)
    }
}

/// Version row + owning-spec `model_family` (N:1 INNER JOIN).
fn select_version_with_family() -> Select<Entity> {
    Entity::find()
        .join(JoinType::InnerJoin, Relation::ModelSpec.def())
        .column_as(Column::ModelFamily, "model_family")
        .column_as(Column::Name, "model_spec_name")
        .column_as(Column::Thesis, "model_spec_thesis")
        .column_as(Column::DefinitionHash, "model_spec_definition_hash")
        .join(JoinType::InnerJoin, Relation::ResearchProfileArtifact.def())
        .column_as(
            ResearchProfileArtifactColumn::ResearchProfileId,
            "profile_ref_id",
        )
        .column_as(
            ResearchProfileArtifactColumn::Version,
            "profile_ref_version",
        )
        .column_as(
            ResearchProfileArtifactColumn::ContentHash,
            "profile_ref_content_hash",
        )
}

impl PgModelRegistryRepository {
    async fn find_version_info(
        db: &impl ConnectionTrait,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        let info = select_version_with_family()
            .filter(QuantModelVersionColumn::ModelVersionId.eq(*model_version_id))
            .into_model::<ModelVersionInfo>()
            .one(db)
            .await
            .map_err(StorageError::from)?;
        info.map(verify_model_version_info).transpose()
    }
}

impl PgModelRegistryRepository {
    async fn require_version_info(
        db: &impl ConnectionTrait,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        Self::find_version_info(db, model_version_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_VERSION, model_version_id))
    }
}

fn verify_model_spec_info(info: ModelSpecInfo) -> Result<ModelSpecInfo, StorageError> {
    let definition = info.definition();
    definition
        .validate()
        .map_err(|detail| StorageError::InvariantViolation {
            entity: Some(QUANT_MODEL_SPEC),
            detail: format!("invalid stored model spec definition: {detail}"),
        })?;
    let expected_hash =
        definition
            .content_hash()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_SPEC),
                detail: format!("stored model spec hash failed: {error}"),
            })?;
    if expected_hash != info.definition_hash {
        return Err(StorageError::InvariantViolation {
            entity: Some(QUANT_MODEL_SPEC),
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
            entity: Some(QUANT_MODEL_VERSION),
            detail: format!(
                "stored model version {} has invalid derivation lineage: {error}",
                info.model_version_id
            ),
        })?;
    Ok(info)
}

impl PgModelRegistryRepository {
    async fn validate_version_derivation(
        db: &impl ConnectionTrait,
        version: &NewModelVersion,
    ) -> Result<(), StorageError> {
        version
            .derivation
            .validate()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_VERSION),
                detail: error.to_string(),
            })?;

        let Some(parent_id) = version.derivation.parent_model_version_id() else {
            return Ok(());
        };
        if parent_id == &version.model_version_id {
            return Err(StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_VERSION),
                detail: "a model version cannot derive from itself".to_owned(),
            });
        }
        let parent = Entity::find_by_id(*parent_id)
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_VERSION, parent_id))?;
        let metadata_matches = parent.model_spec_id == version.model_spec_id
            && parent.category_scope == version.category_scope
            && parent.research_profile_artifact_id == version.profile_ref.artifact_id()
            && parent.training_dataset_id == version.training_dataset_id
            && parent.trade_policy_artifact_id == version.trade_policy_artifact_id
            && parent.trade_policy_hash == version.trade_policy_hash;
        if !metadata_matches {
            return Err(StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_VERSION),
                detail: format!(
                    "derived version metadata must match parent model version {parent_id}"
                ),
            });
        }
        if version.publication_status != PublicationStatus::Candidate
            || version.publish_path_set_id.is_some()
            || version.quality_gate_report.is_some()
            || version.published_at.is_some()
            || version.retired_at.is_some()
        {
            return Err(StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_VERSION),
                detail: "a derived model version must start as an ungated Candidate".to_owned(),
            });
        }

        match &version.derivation {
            ModelVersionDerivation::Training => {}
            ModelVersionDerivation::ScoreMultiplierCalibration {
                source_backtest_report_id,
                ..
            } => {
                let source = QuantBacktestReportEntity::find_by_id(*source_backtest_report_id)
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| {
                        StorageError::not_found("quant_backtest_report", source_backtest_report_id)
                    })?;
                if source.model_version_id != *parent_id {
                    return Err(StorageError::InvariantViolation {
                        entity: Some(QUANT_MODEL_VERSION),
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
                let artifact = QuantCalibrationArtifactEntity::find_by_id(*calibration_artifact_id)
                    .one(db)
                    .await
                    .map_err(StorageError::from)?
                    .ok_or_else(|| {
                        StorageError::not_found(
                            "quant_calibration_artifact",
                            calibration_artifact_id,
                        )
                    })?;
                let CalibrationArtifactPayload::ModelScore(payload) = artifact.payload else {
                    return Err(StorageError::InvariantViolation {
                        entity: Some(QUANT_MODEL_VERSION),
                        detail: format!(
                            "calibration artifact {calibration_artifact_id} is not a model_score artifact"
                        ),
                    });
                };
                if artifact.kind != CalibrationKind::ModelScore
                    || payload.model_version_id != *parent_id
                {
                    return Err(StorageError::InvariantViolation {
                        entity: Some(QUANT_MODEL_VERSION),
                        detail: format!(
                            "calibration artifact {calibration_artifact_id} was not fitted for parent model version {parent_id}"
                        ),
                    });
                }
            }
        }
        Ok(())
    }
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
                entity: Some(QUANT_MODEL_SPEC),
                detail: format!("invalid model spec definition: {detail}"),
            })?;
        let expected_hash =
            definition
                .content_hash()
                .map_err(|error| StorageError::InvariantViolation {
                    entity: Some(QUANT_MODEL_SPEC),
                    detail: format!("model spec definition hash failed: {error}"),
                })?;
        if spec.definition_hash != expected_hash {
            return Err(StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_SPEC),
                detail: format!(
                    "definition_hash mismatch: expected {expected_hash}, got {}",
                    spec.definition_hash
                ),
            });
        }
        let name = spec.name.clone();
        QuantModelSpecEntity::insert(spec.into_active_model())
            .exec_with_returning(&self.db)
            .await
            .map_err(|err| error::map_unique(err, QUANT_MODEL_SPEC, &name))
            .map(Into::into)
    }

    async fn find_model_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<Option<ModelSpecInfo>, StorageError> {
        let row = QuantModelSpecEntity::find_by_id(*model_spec_id)
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
        Self::ensure_profile(&txn, &version.profile_ref).await?;
        let Some(_spec) = QuantModelSpecEntity::find_by_id(version.model_spec_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_MODEL_SPEC,
                version.model_spec_id,
            ));
        };
        Self::validate_version_derivation(&txn, &version).await?;
        let max_version = Entity::find()
            .filter(QuantModelVersionColumn::ModelSpecId.eq(version.model_spec_id))
            .select_only()
            .column_as(QuantModelVersionColumn::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(&txn)
            .await
            .map_err(StorageError::from)?;
        version.version = next_model_version(max_version.flatten())?;
        let duplicate_key = format!("{}:v{}", version.model_spec_id, version.version);
        let model_version_id = version.model_version_id;
        let active =
            version
                .try_into_active_model()
                .map_err(|error| StorageError::InvariantViolation {
                    entity: Some(QUANT_MODEL_VERSION),
                    detail: error.to_string(),
                })?;
        Entity::insert(active)
            .exec_with_returning(&txn)
            .await
            .map_err(|err| error::map_unique(err, QUANT_MODEL_VERSION, &duplicate_key))?;
        let info = Self::require_version_info(&txn, &model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn next_version_for_spec(
        &self,
        model_spec_id: &ModelSpecId,
    ) -> Result<i32, StorageError> {
        // Preview only — `create_model_version` re-allocates under lock.
        let max_version = Entity::find()
            .filter(QuantModelVersionColumn::ModelSpecId.eq(*model_spec_id))
            .select_only()
            .column_as(QuantModelVersionColumn::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(&self.db)
            .await
            .map_err(StorageError::from)?;
        next_model_version(max_version.flatten())
    }

    async fn find_model_version(
        &self,
        model_version_id: &ModelVersionId,
    ) -> Result<Option<ModelVersionInfo>, StorageError> {
        Self::find_version_info(&self.db, model_version_id).await
    }

    async fn page_specs(
        &self,
        query: ModelSpecListQuery,
    ) -> Result<Paginated<ModelSpecInfo>, StorageError> {
        let condition = Condition::all().add_option(
            query
                .model_family
                .map(|family| Column::ModelFamily.eq(family)),
        );
        let page = paginate_mapped(
            QuantModelSpecEntity::find()
                .filter(condition)
                .order_by_desc(Column::CreatedAt),
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
                    .map(|id| QuantModelVersionColumn::ModelSpecId.eq(id)),
            )
            .add_option(
                query
                    .publication_status
                    .map(|status| QuantModelVersionColumn::PublicationStatus.eq(status)),
            )
            .add_option(
                query
                    .from
                    .map(|from| QuantModelVersionColumn::CreatedAt.gte(from)),
            )
            .add_option(query.to.map(|to| QuantModelVersionColumn::CreatedAt.lt(to)));
        let page = paginate_into_model(
            select_version_with_family()
                .filter(condition)
                .order_by_desc(QuantModelVersionColumn::CreatedAt),
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
            ModelPickerSide::Buy => Column::ModelFamily.ne(ModelFamily::HoldVsExitWeighted),
            ModelPickerSide::Sell => Column::ModelFamily.eq(ModelFamily::HoldVsExitWeighted),
        };
        let category = category.map(|category| {
            Condition::any()
                .add(QuantModelVersionColumn::CategoryScope.is_null())
                .add(QuantModelVersionColumn::CategoryScope.eq(category))
        });

        Entity::find()
            .select_only()
            .column(QuantModelVersionColumn::ModelVersionId)
            .column(QuantModelVersionColumn::ModelSpecId)
            .column_as(Column::Name, "spec_name")
            .column(QuantModelVersionColumn::Version)
            .column(QuantModelVersionColumn::ArtifactHash)
            .column_as(Column::ModelFamily, "model_family")
            .column(QuantModelVersionColumn::CategoryScope)
            .column(QuantModelVersionColumn::PublishedAt)
            .join(JoinType::InnerJoin, Relation::ModelSpec.def())
            .filter(QuantModelVersionColumn::PublicationStatus.eq(PublicationStatus::Published))
            .filter(family)
            .filter(Condition::all().add_option(category))
            .order_by_asc(Column::Name)
            .order_by_desc(QuantModelVersionColumn::Version)
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
            .filter(QuantModelVersionColumn::ModelSpecId.eq(*model_spec_id))
            .filter(QuantModelVersionColumn::PublicationStatus.eq(PublicationStatus::Published))
            .order_by_desc(QuantModelVersionColumn::PublishedAt)
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
            Self::update_model_version_status(&txn, model_version_id, PublicationStatus::Retired)
                .await?;
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
        let feature_parity_state_id = PgFeatureParityRepository::resolve_publish_latch_generation(
            &txn,
            feature_parity_permit,
            feature_parity_run_id,
        )
        .await?;
        // Serialize concurrent publishes for the same spec.
        let Some(_spec) = QuantModelSpecEntity::find_by_id(*model_spec_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(QUANT_MODEL_SPEC, model_spec_id));
        };
        let Some(target) = Entity::find_by_id(*model_version_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        if target.model_spec_id != *model_spec_id {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_VERSION,
                Some(model_version_id),
                "model version does not belong to the locked model spec",
            ));
        }
        Self::verify_parity_permit(&txn, feature_parity_run_id, &target).await?;

        let published =
            Self::update_model_version_status(&txn, model_version_id, PublicationStatus::Published)
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
            Self::update_model_version_status(&txn, model_version_id, PublicationStatus::Shadow)
                .await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn set_quality_gate_report(
        &self,
        model_version_id: &ModelVersionId,
        quality_gate_report: QualityGateReport,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = Entity::find_by_id(*model_version_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let mut active = row.into_active_model();
        active.quality_gate_report = ActiveValue::Set(Some(quality_gate_report));
        active.update(&txn).await.map_err(StorageError::from)?;
        let info = Self::require_version_info(&txn, model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn set_publish_path(
        &self,
        model_version_id: &ModelVersionId,
        publish_path_set_id: Option<BacktestPathSetId>,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(row) = Entity::find_by_id(*model_version_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let mut active = row.into_active_model();
        active.publish_path_set_id = ActiveValue::Set(publish_path_set_id);
        active.update(&txn).await.map_err(StorageError::from)?;
        let info = Self::require_version_info(&txn, model_version_id).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }
}

fn next_model_version(current: Option<i32>) -> Result<i32, StorageError> {
    current.unwrap_or(0).checked_add(1).ok_or_else(|| {
        StorageError::invariant_violation(
            Some(QUANT_MODEL_VERSION),
            "model version sequence exhausted i32 capacity",
        )
    })
}

impl PgModelRegistryRepository {
    async fn verify_parity_permit(
        txn: &DatabaseTransaction,
        run_id: &FeatureParityRunId,
        model: &Model,
    ) -> Result<(), StorageError> {
        let model_version_id = &model.model_version_id;
        let training_dataset_id = model.training_dataset_id.as_ref().ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_MODEL_VERSION,
                Some(model_version_id),
                "model parity permit requires an exact training dataset binding",
            )
        })?;
        let run = QuantFeatureParityRunEntity::find_by_id(*run_id)
            .lock_exclusive()
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_FEATURE_PARITY_RUN, run_id))?;
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
                QUANT_FEATURE_PARITY_RUN,
                Some(run_id),
                format!(
                    "full parity run is not a complete permit for model {model_version_id} and dataset {training_dataset_id}"
                ),
            ));
        }
        let dataset = QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::not_found("quant_training_dataset", training_dataset_id)
            })?;
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
        let subject = QuantFeatureParitySubjectEntity::find()
            .filter(QuantFeatureParitySubjectColumn::RunId.eq(*run_id))
            .filter(
                QuantFeatureParitySubjectColumn::SubjectKind.eq(ParitySubjectKind::ModelVersion),
            )
            .filter(QuantFeatureParitySubjectColumn::ModelVersionId.eq(*model_version_id))
            .filter(QuantFeatureParitySubjectColumn::TrainingDatasetId.eq(*training_dataset_id))
            .one(txn)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| {
                StorageError::state_conflict(
                    QUANT_FEATURE_PARITY_RUN,
                    Some(run_id),
                    "full model parity run has no exact WORM model-version subject",
                )
            })?;
        let evidence_hash = ModelVersionParityEvidence {
            model_version_id,
            model_spec_id: &model.model_spec_id,
            artifact_hash: &model.artifact_hash,
            training_dataset_id,
            dataset_hash,
            manifest_hash,
            artifact_bytes_hash,
        }
        .content_hash()
        .map_err(|error| {
            StorageError::invariant_violation(
                Some(QUANT_FEATURE_PARITY_RUN),
                format!("model-version parity subject hash failed: {error}"),
            )
        })?;
        if subject.subject_generation != model.artifact_hash
            || subject.evidence_hash != evidence_hash
        {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(run_id),
                "full model parity WORM subject no longer matches the exact model/dataset artifacts",
            ));
        }
        Ok(())
    }
}

impl PgModelRegistryRepository {
    /// Transition a version's publication status under an existing connection/txn.
    /// Always `SELECT … FOR UPDATE`s the version row before mutating.
    async fn update_model_version_status(
        db: &impl ConnectionTrait,
        model_version_id: &ModelVersionId,
        next: PublicationStatus,
    ) -> Result<ModelVersionInfo, StorageError> {
        let Some(row) = Entity::find_by_id(*model_version_id)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_MODEL_VERSION,
                model_version_id,
            ));
        };
        let from = row.publication_status;
        if !from.allows_transition_to(next) {
            return Err(StorageError::illegal_transition(
                QUANT_MODEL_VERSION,
                Some(model_version_id),
                from,
                next,
            ));
        }
        if from == next {
            return Self::require_version_info(db, model_version_id).await;
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
        Self::require_version_info(db, model_version_id).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use sea_orm::{DbBackend, MockDatabase, Value};

    use super::*;

    #[test]
    fn model_version_sequence_checked() {
        assert_eq!(next_model_version(None).expect("initial version"), 1);
        assert_eq!(next_model_version(Some(41)).expect("next version"), 42);
        let error = next_model_version(Some(i32::MAX)).expect_err("overflow must fail closed");
        assert!(error.to_string().contains("sequence exhausted"));
    }

    #[tokio::test]
    async fn published_model_catalog_projection() -> Result<(), StorageError> {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<BTreeMap<String, Value>>::new()])
            .into_connection();
        let repository = PgModelRegistryRepository::new(db.clone());

        let catalog = repository
            .list_published_catalog(ModelPickerSide::Buy, Some(MarketCategory::Crypto))
            .await?;

        assert!(catalog.is_empty());
        assert_eq!(db.into_transaction_log().len(), 1);
        Ok(())
    }
}
