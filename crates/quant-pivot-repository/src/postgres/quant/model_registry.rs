//! Postgres-backed model registry repository.

use quant_pivot_error::storage::{
    StorageError,
    entity::{
        QUANT_FEATURE_PARITY_RUN, QUANT_MODEL_RUN, QUANT_MODEL_SPEC, QUANT_MODEL_VERSION,
        QUANT_TRAINING_DATASET,
    },
};
use quant_pivot_models::{
    domain::{
        api::{ModelPickerSide, ModelSpecListQuery, ModelVersionListQuery},
        pagination::{PageRequest, PageWindow, Paginated},
        quant::{
            CalibrationArtifactPayload, ModelCatalogInfo, ModelSpecInfo, ModelVersionInfo,
            ModelVersionParityEvidence, NewModelSpec, NewModelVersion,
        },
    },
    entities::{
        quant_calibration_artifact::Entity as QuantCalibrationArtifactEntity,
        quant_feature_parity_run::Entity as QuantFeatureParityRunEntity,
        quant_feature_parity_subject::{
            Column as QuantFeatureParitySubjectColumn, Entity as QuantFeatureParitySubjectEntity,
        },
        quant_model_run::{
            Column as QuantModelRunColumn, Entity as QuantModelRunEntity,
            Model as QuantModelRunModel,
        },
        quant_model_spec::{Column, Entity as QuantModelSpecEntity, Model as QuantModelSpecModel},
        quant_model_version::{
            ActiveModel as QuantModelVersionActiveModel, Column as QuantModelVersionColumn, Entity,
            Model, Relation,
        },
        quant_training_dataset::Entity as QuantTrainingDatasetEntity,
        research_profile_artifact::Column as ResearchProfileArtifactColumn,
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            CalibrationKind, DatasetPurpose, FeatureParityRunKind, FeatureParityRunStatus,
            ModelRunErrorCode, ModelRunKind, ModelRunStatus, ParitySubjectKind,
            TrainingDatasetStatus,
        },
    },
    types::{
        ContentHash, FactorDefinitionId, FeatureParityRunId, ModelRunId, ModelSpecId,
        ModelVersionId, model_lineage::ModelVersionDerivation, model_spec::ModelSpecDefinition,
    },
};
use sea_orm::{
    ColumnTrait, Condition, ConnectionTrait, DatabaseConnection, DatabaseTransaction, EntityTrait,
    IntoActiveModel, JoinType, QueryFilter, QueryOrder, QuerySelect, RelationTrait, Select,
    TransactionTrait,
    sea_query::{Expr, ExprTrait, extension::postgres::PgBinOper},
};

use crate::{
    postgres::{
        error, primitives,
        query::{paginate_into_model, paginate_mapped},
    },
    traits::ModelRegistryRepository,
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
        .select_only()
        .columns([
            QuantModelVersionColumn::ModelVersionId,
            QuantModelVersionColumn::ModelSpecId,
            QuantModelVersionColumn::Version,
            QuantModelVersionColumn::ArtifactHash,
            QuantModelVersionColumn::ServingContract,
            QuantModelVersionColumn::CategoryScope,
            QuantModelVersionColumn::TrainingDatasetId,
            QuantModelVersionColumn::TradePolicyArtifactId,
            QuantModelVersionColumn::TradePolicyHash,
            QuantModelVersionColumn::DerivationKind,
            QuantModelVersionColumn::ParentModelVersionId,
            QuantModelVersionColumn::CalibrationArtifactId,
            QuantModelVersionColumn::DerivationEvidenceHash,
            QuantModelVersionColumn::Metrics,
            QuantModelVersionColumn::TrainingObjective,
            QuantModelVersionColumn::CreatedAt,
        ])
        .expr_as(
            Expr::cust(
                "('blake3:'::text || encode(\"quant_model_version\".\"serving_contract_hash\", 'hex'::text))",
            ),
            "serving_contract_hash",
        )
        .join(JoinType::InnerJoin, Relation::ModelSpec.def())
        .column_as(Column::ModelFamily, "model_family")
        .column_as(Column::Name, "model_spec_name")
        .column_as(Column::Thesis, "model_spec_thesis")
        .column_as(Column::DefinitionHash, "model_spec_definition_hash")
        .column_as(
            Column::PredictionHorizonSecs,
            "model_spec_prediction_horizon_secs",
        )
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
    pub(crate) async fn require_version_info(
        db: &impl ConnectionTrait,
        model_version_id: &ModelVersionId,
    ) -> Result<ModelVersionInfo, StorageError> {
        Self::find_version_info(db, model_version_id)
            .await?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_VERSION, model_version_id))
    }

    /// Insert a model version through the one authoritative allocation path.
    ///
    /// The caller owns the surrounding transaction. Locking the model spec
    /// serializes `MAX(version) + 1` allocation across every insertion API.
    async fn insert_model_version(
        db: &impl ConnectionTrait,
        mut version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        Self::ensure_profile(db, &version.profile_ref).await?;
        let Some(spec) = QuantModelSpecEntity::find_by_id(version.model_spec_id)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(
                QUANT_MODEL_SPEC,
                version.model_spec_id,
            ));
        };
        Self::validate_serving_contract(&version, &spec)?;
        Self::validate_version_derivation(db, &version).await?;
        let max_version = Entity::find()
            .filter(QuantModelVersionColumn::ModelSpecId.eq(version.model_spec_id))
            .select_only()
            .column_as(QuantModelVersionColumn::Version.max(), "max_version")
            .into_tuple::<Option<i32>>()
            .one(db)
            .await
            .map_err(StorageError::from)?;
        version.version = next_model_version(max_version.flatten())?;
        let duplicate_key = format!("{}:v{}", version.model_spec_id, version.version);
        let model_version_id = version.model_version_id;
        let active = QuantModelVersionActiveModel::try_from(version).map_err(|error| {
            StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_VERSION),
                detail: error.to_string(),
            }
        })?;
        Entity::insert(active)
            .exec_with_returning(db)
            .await
            .map_err(|error| error::map_unique(error, QUANT_MODEL_VERSION, &duplicate_key))?;
        Self::require_version_info(db, &model_version_id).await
    }

    fn validate_training_version(version: &NewModelVersion) -> Result<(), StorageError> {
        if version.derivation != ModelVersionDerivation::Training {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_VERSION),
                "training completion requires a root Training derivation",
            ));
        }
        Ok(())
    }

    fn validate_serving_contract(
        version: &NewModelVersion,
        spec: &QuantModelSpecModel,
    ) -> Result<(), StorageError> {
        version
            .serving_contract_hash()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some(QUANT_MODEL_VERSION),
                detail: format!(
                    "model version {} has an invalid serving contract: {error}",
                    version.model_version_id
                ),
            })?;
        let model = &version.serving_contract.bindings().model;
        let definition_matches = model.model_spec_definition_hash == spec.definition_hash;
        let exact_spec = model.model_family == spec.model_family
            && definition_matches
            && i64::try_from(model.prediction_horizon_secs)
                .is_ok_and(|horizon| horizon == spec.prediction_horizon_secs);
        if !exact_spec {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_VERSION),
                format!(
                    "serving contract does not exactly bind model spec {}",
                    spec.model_spec_id
                ),
            ));
        }
        Ok(())
    }

    fn request_matches_version(request: &NewModelVersion, stored: &ModelVersionInfo) -> bool {
        stored.model_version_id == request.model_version_id
            && stored.model_spec_id == request.model_spec_id
            && stored.artifact_hash == request.artifact_hash
            && stored.serving_contract == request.serving_contract
            && request
                .serving_contract_hash()
                .is_ok_and(|hash| stored.serving_contract_hash == hash)
            && stored.category_scope == request.category_scope
            && stored.profile_ref == request.profile_ref
            && stored.training_dataset_id == request.training_dataset_id
            && stored.trade_policy_artifact_id == request.trade_policy_artifact_id
            && stored.trade_policy_hash == request.trade_policy_hash
            && stored
                .verified_derivation()
                .is_ok_and(|derivation| derivation == request.derivation)
            && stored.metrics == request.metrics
            && stored.training_objective == request.training_objective
    }

    async fn validate_training_dataset(
        db: &impl ConnectionTrait,
        run: &QuantModelRunModel,
        version: &NewModelVersion,
    ) -> Result<(), StorageError> {
        let spec = QuantModelSpecEntity::find_by_id(version.model_spec_id)
            .lock_exclusive()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_MODEL_SPEC, version.model_spec_id))?;
        let training_dataset_id = version.training_dataset_id.as_ref().ok_or_else(|| {
            StorageError::invariant_violation(
                Some(QUANT_MODEL_VERSION),
                "a root training Candidate requires an exact training dataset binding",
            )
        })?;
        let dataset = QuantTrainingDatasetEntity::find_by_id(*training_dataset_id)
            .lock_shared()
            .one(db)
            .await
            .map_err(StorageError::from)?
            .ok_or_else(|| StorageError::not_found(QUANT_TRAINING_DATASET, training_dataset_id))?;
        if dataset.status != TrainingDatasetStatus::Ready
            || dataset.purpose != DatasetPurpose::Training
        {
            return Err(StorageError::state_conflict(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                format!(
                    "model training requires a Ready Training dataset, found {} {}",
                    dataset.status.as_str(),
                    dataset.purpose.as_str()
                ),
            ));
        }
        let dataset_hash = dataset.dataset_hash.as_ref().ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                "Ready training dataset has no immutable dataset hash",
            )
        })?;
        let manifest = dataset.manifest.as_ref().ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                "Ready training dataset has no immutable manifest",
            )
        })?;
        manifest
            .validate()
            .map_err(|error| StorageError::InvariantViolation {
                entity: Some(QUANT_TRAINING_DATASET),
                detail: format!(
                    "training dataset {training_dataset_id} has an invalid manifest: {error}"
                ),
            })?;
        let expected_manifest_hash =
            manifest
                .content_hash()
                .map_err(|error| StorageError::InvariantViolation {
                    entity: Some(QUANT_TRAINING_DATASET),
                    detail: format!(
                        "training dataset {training_dataset_id} manifest hash failed: {error}"
                    ),
                })?;
        let stored_manifest_hash = dataset.manifest_hash.as_ref().ok_or_else(|| {
            StorageError::state_conflict(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                "Ready training dataset has no immutable manifest hash",
            )
        })?;

        let dataset_definition_matches = dataset.model_spec_definition_hash == spec.definition_hash;
        let manifest_definition_matches =
            manifest.model_spec_definition_hash == spec.definition_hash;
        let exact_binding = dataset.model_spec_id == version.model_spec_id
            && dataset.model_family == spec.model_family
            && dataset_definition_matches
            && dataset.research_profile_artifact_id == version.profile_ref.artifact_id()
            && dataset.decision_policy_snapshot_id == run.decision_policy_snapshot_id
            && dataset.window_start == run.window_start
            && dataset.window_end == run.window_end
            && dataset_hash == &run.input_hash
            && manifest.training_dataset_id == *training_dataset_id
            && manifest.model_spec_id == version.model_spec_id
            && manifest.model_family == spec.model_family
            && manifest_definition_matches
            && manifest.source_lineage.research_profile_artifact_id
                == dataset.research_profile_artifact_id
            && manifest.source_lineage.decision_policy_snapshot_id
                == run.decision_policy_snapshot_id
            && manifest.window_start == run.window_start
            && manifest.window_end == run.window_end
            && manifest.purpose == DatasetPurpose::Training
            && manifest.semantic_dataset_hash == *dataset_hash
            && &expected_manifest_hash == stored_manifest_hash
            && manifest.trade_policy_artifact_id == version.trade_policy_artifact_id
            && manifest.trade_policy_hash == version.trade_policy_hash;
        if !exact_binding {
            return Err(StorageError::state_conflict(
                QUANT_TRAINING_DATASET,
                Some(training_dataset_id),
                format!(
                    "dataset lineage does not exactly bind training run {} and model version {}",
                    run.model_run_id, version.model_version_id
                ),
            ));
        }
        Ok(())
    }

    fn version_readback_matches(actual: &ModelVersionInfo, expected: &ModelVersionInfo) -> bool {
        actual.model_version_id == expected.model_version_id
            && actual.model_spec_id == expected.model_spec_id
            && actual.model_spec_name == expected.model_spec_name
            && actual.model_family == expected.model_family
            && actual.model_spec_thesis == expected.model_spec_thesis
            && actual.model_spec_definition_hash == expected.model_spec_definition_hash
            && actual.model_spec_prediction_horizon_secs
                == expected.model_spec_prediction_horizon_secs
            && actual.version == expected.version
            && actual.artifact_hash == expected.artifact_hash
            && actual.serving_contract == expected.serving_contract
            && actual.serving_contract_hash == expected.serving_contract_hash
            && actual.category_scope == expected.category_scope
            && actual.profile_ref == expected.profile_ref
            && actual.training_dataset_id == expected.training_dataset_id
            && actual.trade_policy_artifact_id == expected.trade_policy_artifact_id
            && actual.trade_policy_hash == expected.trade_policy_hash
            && actual.derivation_kind == expected.derivation_kind
            && actual.parent_model_version_id == expected.parent_model_version_id
            && actual.calibration_artifact_id == expected.calibration_artifact_id
            && actual.derivation_evidence_hash == expected.derivation_evidence_hash
            && actual.metrics == expected.metrics
            && actual.training_objective == expected.training_objective
            && actual.created_at == expected.created_at
    }

    async fn training_commit_matches(
        &self,
        model_run_id: &ModelRunId,
        expected_version: &ModelVersionInfo,
    ) -> bool {
        let Ok(Some(actual_version)) =
            Self::find_version_info(&self.db, &expected_version.model_version_id).await
        else {
            return false;
        };
        if !Self::version_readback_matches(&actual_version, expected_version) {
            return false;
        }
        let Ok(Some(run)) = QuantModelRunEntity::find_by_id(*model_run_id)
            .one(&self.db)
            .await
        else {
            return false;
        };
        run.run_kind == ModelRunKind::Training
            && run.status == ModelRunStatus::Succeeded
            && run.model_version_id == Some(expected_version.model_version_id)
            && run.output_hash == Some(expected_version.artifact_hash)
            && run.error_code.is_none()
            && run.error_message.is_none()
            && run.finished_at.is_some()
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
    info.verified_serving_contract()
        .map_err(|error| StorageError::InvariantViolation {
            entity: Some(QUANT_MODEL_VERSION),
            detail: format!(
                "stored model version {} has an invalid serving contract: {error}",
                info.model_version_id
            ),
        })?;
    Ok(info)
}

fn serving_hash_from_bytes(bytes: &[u8]) -> Result<ContentHash, StorageError> {
    let digest: [u8; 32] = bytes.try_into().map_err(|_| {
        StorageError::invariant_violation(
            Some(QUANT_MODEL_VERSION),
            format!(
                "stored serving-contract hash must contain exactly 32 bytes, found {}",
                bytes.len()
            ),
        )
    })?;
    Ok(ContentHash::from_bytes(digest))
}

fn verify_model_version_row(model: &Model) -> Result<(), StorageError> {
    let persisted_hash = serving_hash_from_bytes(&model.serving_contract_hash)?;
    model
        .serving_contract
        .verify_persisted_hash(persisted_hash)
        .map_err(|error| StorageError::InvariantViolation {
            entity: Some(QUANT_MODEL_VERSION),
            detail: format!(
                "stored model version {} has an invalid serving contract: {error}",
                model.model_version_id
            ),
        })?;
    let bindings = model.serving_contract.bindings();
    let contract_model = &bindings.model;
    let exact_projection = contract_model.model_version_id == model.model_version_id
        && contract_model.model_spec_id == model.model_spec_id
        && contract_model.category_scope == model.category_scope
        && contract_model.profile_ref.artifact_id() == model.research_profile_artifact_id
        && Some(bindings.dataset.manifest.training_dataset_id) == model.training_dataset_id
        && bindings
            .trade_policy
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash))
            == model.trade_policy_artifact_id.zip(model.trade_policy_hash);
    if !exact_projection {
        return Err(StorageError::invariant_violation(
            Some(QUANT_MODEL_VERSION),
            format!(
                "stored model version {} does not match its serving-contract projection",
                model.model_version_id
            ),
        ));
    }
    Ok(())
}

impl PgModelRegistryRepository {
    async fn validate_version_derivation(
        db: &impl ConnectionTrait,
        version: &NewModelVersion,
    ) -> Result<(), StorageError> {
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
        verify_model_version_row(&parent)?;
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
        match &version.derivation {
            ModelVersionDerivation::Training => {}
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
                    || payload.fit_contract.model.model_version_id != *parent_id
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
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let info = Self::insert_model_version(&txn, version).await?;
        txn.commit().await.map_err(StorageError::from)?;
        Ok(info)
    }

    async fn commit_training_model_version(
        &self,
        model_run_id: &ModelRunId,
        version: NewModelVersion,
    ) -> Result<ModelVersionInfo, StorageError> {
        Self::validate_training_version(&version)?;
        let txn = self.db.begin().await.map_err(StorageError::from)?;
        let Some(run) = QuantModelRunEntity::find_by_id(*model_run_id)
            .lock_exclusive()
            .one(&txn)
            .await
            .map_err(StorageError::from)?
        else {
            return Err(StorageError::not_found(QUANT_MODEL_RUN, model_run_id));
        };
        if run.run_kind != ModelRunKind::Training {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(model_run_id),
                format!(
                    "model version completion requires a training run, found {}",
                    run.run_kind.as_str()
                ),
            ));
        }
        if run.status == ModelRunStatus::Succeeded {
            let exact_terminal = run.model_version_id == Some(version.model_version_id)
                && run.output_hash == Some(version.artifact_hash)
                && run.error_code.is_none()
                && run.error_message.is_none()
                && run.finished_at.is_some();
            let stored = Self::find_version_info(&txn, &version.model_version_id).await?;
            if exact_terminal
                && let Some(stored) =
                    stored.filter(|stored| Self::request_matches_version(&version, stored))
            {
                txn.commit().await.map_err(StorageError::from)?;
                return Ok(stored);
            }
        }
        if run.status != ModelRunStatus::Running {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(model_run_id),
                format!(
                    "model version completion requires a running run, found {}",
                    run.status.as_str()
                ),
            ));
        }
        if run.model_version_id.is_some() {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(model_run_id),
                "training run is already bound to a model version",
            ));
        }
        if run.output_hash.is_some()
            || run.error_code.is_some()
            || run.error_message.is_some()
            || run.finished_at.is_some()
        {
            return Err(StorageError::state_conflict(
                QUANT_MODEL_RUN,
                Some(model_run_id),
                "running training run contains terminal output or error fields",
            ));
        }
        Self::validate_training_dataset(&txn, &run, &version).await?;
        let info = Self::insert_model_version(&txn, version).await?;
        let terminal_runs = QuantModelRunEntity::update_many()
            .col_expr(
                QuantModelRunColumn::Status,
                primitives::enum_value(&ModelRunStatus::Succeeded),
            )
            .col_expr(
                QuantModelRunColumn::ModelVersionId,
                Expr::value(Some(info.model_version_id)),
            )
            .col_expr(
                QuantModelRunColumn::OutputHash,
                Expr::value(Some(info.artifact_hash)),
            )
            .col_expr(
                QuantModelRunColumn::ErrorCode,
                Expr::value(Option::<ModelRunErrorCode>::None),
            )
            .col_expr(
                QuantModelRunColumn::ErrorMessage,
                Expr::value(Option::<String>::None),
            )
            .col_expr(
                QuantModelRunColumn::FinishedAt,
                Expr::cust("statement_timestamp()"),
            )
            .filter(QuantModelRunColumn::ModelRunId.eq(*model_run_id))
            .filter(QuantModelRunColumn::Status.eq(ModelRunStatus::Running))
            .exec_with_returning(&txn)
            .await
            .map_err(StorageError::from)?;
        if terminal_runs.len() != 1 {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_RUN),
                format!(
                    "training completion finalized {} runs; expected one",
                    terminal_runs.len()
                ),
            ));
        }

        match txn.commit().await {
            Ok(()) => Ok(info),
            Err(error) => {
                let commit_error = StorageError::from(error);
                if self.training_commit_matches(model_run_id, &info).await {
                    Ok(info)
                } else {
                    Err(commit_error)
                }
            }
        }
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

    async fn page_factor_usages(
        &self,
        factor_definition_id: &FactorDefinitionId,
        page: PageRequest,
    ) -> Result<Paginated<ModelVersionInfo>, StorageError> {
        let factor_binding = serde_json::json!({
            "bindings": {
                "factors": {
                    "plane": {
                        "definitions": [{
                            "factor_definition_id": factor_definition_id,
                        }],
                    },
                },
            },
        });
        let page = paginate_into_model(
            select_version_with_family()
                .filter(
                    Expr::col((Entity, QuantModelVersionColumn::ServingContract))
                        .binary(PgBinOper::Contains, factor_binding),
                )
                .order_by_desc(QuantModelVersionColumn::CreatedAt)
                .order_by_desc(QuantModelVersionColumn::ModelVersionId),
            &self.db,
            PageWindow::harden(page),
        )
        .await?;
        let items = page
            .items
            .into_iter()
            .map(verify_model_version_info)
            .collect::<Result<Vec<_>, _>>()?;
        if items.iter().any(|model| {
            !model
                .serving_contract
                .bindings()
                .factors
                .plane
                .definitions()
                .iter()
                .any(|factor| factor.factor_definition_id() == *factor_definition_id)
        }) {
            return Err(StorageError::invariant_violation(
                Some(QUANT_MODEL_VERSION),
                "factor-serving usage query returned a contract without the requested revision",
            ));
        }
        Ok(Paginated::new(items, page.total, page.page, page.size))
    }

    async fn list_model_catalog(
        &self,
        side: ModelPickerSide,
        category: Option<MarketCategory>,
    ) -> Result<Vec<ModelCatalogInfo>, StorageError> {
        let family = match side {
            ModelPickerSide::Buy => Column::ModelFamily.ne(ModelFamily::HoldVsExitWeighted),
            ModelPickerSide::Sell => Column::ModelFamily.eq(ModelFamily::HoldVsExitWeighted),
        };
        let category = category.map(|category| QuantModelVersionColumn::CategoryScope.eq(category));

        Entity::find()
            .select_only()
            .column(QuantModelVersionColumn::ModelVersionId)
            .column(QuantModelVersionColumn::ModelSpecId)
            .column_as(Column::Name, "spec_name")
            .column(QuantModelVersionColumn::Version)
            .column(QuantModelVersionColumn::ArtifactHash)
            .column_as(Column::ModelFamily, "model_family")
            .column(QuantModelVersionColumn::CategoryScope)
            .join(JoinType::InnerJoin, Relation::ModelSpec.def())
            .filter(family)
            .filter(Condition::all().add_option(category))
            .order_by_asc(Column::Name)
            .order_by_desc(QuantModelVersionColumn::Version)
            .into_model::<ModelCatalogInfo>()
            .all(&self.db)
            .await
            .map_err(StorageError::from)
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
    pub(crate) async fn verify_parity_permit(
        txn: &DatabaseTransaction,
        run_id: &FeatureParityRunId,
        model: &Model,
    ) -> Result<ContentHash, StorageError> {
        verify_model_version_row(model)?;
        let contract = model.serving_contract.bindings();
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
            && run.feature_contract_hash == Some(contract.schemas.feature_schema_hash)
            && run.transform_hash == Some(contract.transform.input_transform_hash)
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
        if dataset_hash != &contract.transform.training_dataset_hash
            || dataset_hash != &contract.dataset.manifest.semantic_dataset_hash
            || manifest_hash != &contract.dataset.manifest_hash
            || artifact_bytes_hash != &contract.dataset.artifact_bytes_hash
        {
            return Err(StorageError::state_conflict(
                QUANT_FEATURE_PARITY_RUN,
                Some(run_id),
                "full model parity run dataset does not match the exact serving contract",
            ));
        }
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
        Ok(subject.evidence_hash)
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

    #[test]
    fn serving_hash_requires_blake3() {
        let expected = ContentHash::from_bytes([0xa5; 32]);
        assert_eq!(
            serving_hash_from_bytes(expected.as_bytes()).expect("32-byte digest"),
            expected
        );
        let error = serving_hash_from_bytes(&[0; 31]).expect_err("short digest must fail closed");
        assert!(error.to_string().contains("exactly 32 bytes"));
    }

    #[tokio::test]
    async fn model_catalog_projection() -> Result<(), StorageError> {
        let db = MockDatabase::new(DbBackend::Postgres)
            .append_query_results([Vec::<BTreeMap<String, Value>>::new()])
            .into_connection();
        let repository = PgModelRegistryRepository::new(db.clone());

        let catalog = repository
            .list_model_catalog(ModelPickerSide::Buy, Some(MarketCategory::Crypto))
            .await?;

        assert!(catalog.is_empty());
        assert_eq!(db.into_transaction_log().len(), 1);
        Ok(())
    }
}
