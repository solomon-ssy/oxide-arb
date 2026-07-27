//! Shared fail-closed verification for immutable model research ledgers.

use quant_pivot_error::storage::{
    StorageError,
    entity::{
        DECISION_POLICY_SNAPSHOT, QUANT_MODEL_SPEC, QUANT_MODEL_VERSION, QUANT_SOURCE_SLICE,
        QUANT_TRAINING_DATASET,
    },
};
use quant_pivot_models::{
    domain::quant::{
        SourceSliceIdentity, SourceSliceIdentityInput, TrainingDatasetInfo,
        TrainingDatasetMaterialization,
    },
    entities::{
        decision_policy_snapshot::{
            Entity as DecisionPolicySnapshotEntity, Model as DecisionPolicySnapshotModel,
        },
        quant_model_spec::{Entity as ModelSpecEntity, Model as ModelSpecModel},
        quant_model_version::{Entity as ModelVersionEntity, Model as ModelVersionModel},
        quant_source_slice::Entity as SourceSliceEntity,
        quant_training_dataset::Entity as TrainingDatasetEntity,
    },
    enums::{
        quant::{DatasetPurpose, SourceSliceStatus, TrainingDatasetStatus},
        runtime_config::ConfigResourceKind,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, TrainingDatasetId,
        model_lineage::ModelVersionDerivation, model_spec::ModelSpecDefinition,
    },
};
use sea_orm::{ConnectionTrait, EntityTrait, QuerySelect};

/// Fully reverified immutable source-model graph required by report and
/// calibration write boundaries.
pub(super) struct VerifiedModelLineage {
    pub version: ModelVersionModel,
    pub spec: ModelSpecModel,
    pub training_dataset: TrainingDatasetInfo,
    pub policy: DecisionPolicySnapshotModel,
}

impl VerifiedModelLineage {
    pub fn training_materialization(
        &self,
    ) -> Result<TrainingDatasetMaterialization<'_>, StorageError> {
        self.training_dataset.materialization().ok_or_else(|| {
            invariant(
                QUANT_TRAINING_DATASET,
                "verified lineage lost its Training Dataset materialization",
            )
        })
    }
}

pub(super) async fn load_model_lineage<C>(
    db: &C,
    model_version_id: ModelVersionId,
) -> Result<VerifiedModelLineage, StorageError>
where
    C: ConnectionTrait,
{
    let version = ModelVersionEntity::find_by_id(model_version_id)
        .lock_shared()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::not_found(QUANT_MODEL_VERSION, model_version_id))?;
    verify_version_contract(&version)?;
    let spec = ModelSpecEntity::find_by_id(version.model_spec_id)
        .lock_shared()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::not_found(QUANT_MODEL_SPEC, version.model_spec_id))?;
    verify_spec(&spec)?;

    let bindings = version.serving_contract.bindings();
    if bindings.model.model_spec_id != spec.model_spec_id
        || bindings.model.model_spec_definition_hash != spec.definition_hash
        || bindings.model.model_family != spec.model_family
        || bindings.model.prediction_horizon_secs
            != u64::try_from(spec.prediction_horizon_secs)
                .map_err(|error| invariant(QUANT_MODEL_SPEC, error.to_string()))?
    {
        return Err(invariant(
            QUANT_MODEL_VERSION,
            "model serving contract differs from its canonical ModelSpec",
        ));
    }

    let policy = DecisionPolicySnapshotEntity::find_by_id(
        bindings.policy_snapshot.decision_policy_snapshot_id,
    )
    .lock_shared()
    .one(db)
    .await
    .map_err(StorageError::from)?
    .ok_or_else(|| {
        StorageError::not_found(
            DECISION_POLICY_SNAPSHOT,
            bindings.policy_snapshot.decision_policy_snapshot_id,
        )
    })?;
    verify_policy(&policy)?;
    if policy.snapshot_hash != bindings.policy_snapshot.snapshot_hash
        || policy.snapshot.profile_artifact_refs != bindings.policy_snapshot.profile_artifacts
    {
        return Err(invariant(
            QUANT_MODEL_VERSION,
            "model serving contract differs from its exact policy snapshot",
        ));
    }

    let training_dataset_id = bindings.dataset.manifest.training_dataset_id;
    let training_dataset = load_dataset(db, training_dataset_id).await?;
    let training_materialization = verify_dataset(
        db,
        &training_dataset,
        DatasetPurpose::Training,
        &version,
        &spec,
        &policy,
    )
    .await?;
    if training_materialization.manifest != &bindings.dataset.manifest
        || training_materialization.manifest_hash != &bindings.dataset.manifest_hash
        || training_materialization.artifact_bytes_hash != &bindings.dataset.artifact_bytes_hash
        || training_materialization.dataset_hash != &bindings.dataset.manifest.semantic_dataset_hash
    {
        return Err(invariant(
            QUANT_MODEL_VERSION,
            "source Training Dataset differs from the sealed serving contract",
        ));
    }

    Ok(VerifiedModelLineage {
        version,
        spec,
        training_dataset,
        policy,
    })
}

pub(super) async fn load_dataset<C>(
    db: &C,
    dataset_id: TrainingDatasetId,
) -> Result<TrainingDatasetInfo, StorageError>
where
    C: ConnectionTrait,
{
    TrainingDatasetEntity::find_by_id(dataset_id)
        .lock_shared()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .map(Into::into)
        .ok_or_else(|| StorageError::not_found(QUANT_TRAINING_DATASET, dataset_id))
}

pub(super) async fn verify_replay_dataset<'a, C>(
    db: &C,
    dataset: &'a TrainingDatasetInfo,
    purpose: DatasetPurpose,
    model: &VerifiedModelLineage,
) -> Result<TrainingDatasetMaterialization<'a>, StorageError>
where
    C: ConnectionTrait,
{
    let materialization = verify_dataset(
        db,
        dataset,
        purpose,
        &model.version,
        &model.spec,
        &model.policy,
    )
    .await?;
    let bindings = model.version.serving_contract.bindings();
    let training = model.training_materialization()?;
    let lineage = &materialization.manifest.source_lineage;
    let training_lineage = &training.manifest.source_lineage;
    let feature_version_matches =
        materialization.manifest.feature_schema_version == training.manifest.feature_schema_version;
    let feature_hash_matches =
        materialization.manifest.feature_schema_hash == bindings.schemas.feature_schema_hash;
    let factor_plane_matches =
        materialization.manifest.factor_serving_plane == bindings.factors.plane;
    let label_hash_matches =
        materialization.manifest.label_schema_hash == bindings.schemas.label_schema_hash;
    let policy_id_matches = materialization.manifest.trade_policy_artifact_id
        == training.manifest.trade_policy_artifact_id;
    let policy_hash_matches =
        materialization.manifest.trade_policy_hash == training.manifest.trade_policy_hash;
    let reader_contract_matches =
        lineage.reader_contract_version == training_lineage.reader_contract_version;
    let schema_contract_matches =
        lineage.schema_contract_version == training_lineage.schema_contract_version;
    let source_schema_matches = lineage.source_schema_hash == training_lineage.source_schema_hash;
    let capabilities_match =
        lineage.capability_registry_hashes == bindings.capability_registry_hashes;
    let horizon_matches = materialization
        .manifest
        .horizons_secs
        .contains(&bindings.model.prediction_horizon_secs);
    let all_match = feature_version_matches
        && feature_hash_matches
        && factor_plane_matches
        && label_hash_matches
        && policy_id_matches
        && policy_hash_matches
        && reader_contract_matches
        && schema_contract_matches
        && source_schema_matches
        && capabilities_match
        && horizon_matches;
    if !all_match {
        return Err(invariant(
            QUANT_TRAINING_DATASET,
            "replay Dataset schema/source lineage differs from the exact source model",
        ));
    }
    Ok(materialization)
}

fn verify_version_contract(version: &ModelVersionModel) -> Result<(), StorageError> {
    let digest: [u8; 32] = version
        .serving_contract_hash
        .as_slice()
        .try_into()
        .map_err(|_| {
            invariant(
                QUANT_MODEL_VERSION,
                "serving_contract_hash must contain exactly 32 bytes",
            )
        })?;
    version
        .serving_contract
        .verify_persisted_hash(ContentHash::from_bytes(digest))
        .map_err(|error| invariant(QUANT_MODEL_VERSION, error.to_string()))?;
    ModelVersionDerivation::from_persistence(
        version.derivation_kind,
        version.parent_model_version_id,
        version.calibration_artifact_id,
        version.derivation_evidence_hash,
    )
    .map_err(|error| invariant(QUANT_MODEL_VERSION, error.to_string()))?;
    let bindings = version.serving_contract.bindings();
    let trade_policy = bindings
        .trade_policy
        .as_ref()
        .map(|binding| (binding.artifact_id, binding.content_hash));
    if bindings.model.model_version_id != version.model_version_id
        || bindings.model.model_spec_id != version.model_spec_id
        || bindings.model.category_scope != version.category_scope
        || bindings.model.profile_ref.artifact_id() != version.research_profile_artifact_id
        || Some(bindings.dataset.manifest.training_dataset_id) != version.training_dataset_id
        || trade_policy
            != version
                .trade_policy_artifact_id
                .zip(version.trade_policy_hash)
    {
        return Err(invariant(
            QUANT_MODEL_VERSION,
            "model version scalar projections differ from its serving contract",
        ));
    }
    Ok(())
}

fn verify_spec(spec: &ModelSpecModel) -> Result<(), StorageError> {
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
        .map_err(|detail| invariant(QUANT_MODEL_SPEC, detail))?;
    let expected = definition
        .content_hash()
        .map_err(|error| invariant(QUANT_MODEL_SPEC, error.to_string()))?;
    if expected != spec.definition_hash {
        return Err(invariant(
            QUANT_MODEL_SPEC,
            "ModelSpec definition hash mismatch",
        ));
    }
    Ok(())
}

fn verify_policy(policy: &DecisionPolicySnapshotModel) -> Result<(), StorageError> {
    let expected = CanonicalDigest::content_hash_json(&policy.snapshot)
        .map_err(|error| invariant(DECISION_POLICY_SNAPSHOT, error.to_string()))?;
    let revision_projections = [
        (
            ConfigResourceKind::RecommendationPolicy,
            policy.recommendation_policy_revision_id,
        ),
        (
            ConfigResourceKind::ExecutionRiskPolicy,
            policy.execution_risk_policy_revision_id,
        ),
        (
            ConfigResourceKind::ModelRouting,
            policy.model_routing_revision_id,
        ),
        (
            ConfigResourceKind::ReportSchedule,
            policy.report_schedule_revision_id,
        ),
        (
            ConfigResourceKind::OperationalControl,
            policy.operational_control_revision_id,
        ),
        (
            ConfigResourceKind::ExecutionAuthorization,
            policy.execution_authorization_revision_id,
        ),
    ];
    if expected != policy.snapshot_hash
        || DecisionPolicySnapshotId::from_content_hash(&expected)
            != policy.decision_policy_snapshot_id
        || revision_projections
            .iter()
            .any(|(kind, revision)| policy.snapshot.resource_revision_id(*kind) != Some(revision))
    {
        return Err(invariant(
            DECISION_POLICY_SNAPSHOT,
            "policy snapshot content address or revision projections are inconsistent",
        ));
    }
    Ok(())
}

async fn verify_dataset<'a, C>(
    db: &C,
    dataset: &'a TrainingDatasetInfo,
    purpose: DatasetPurpose,
    version: &ModelVersionModel,
    spec: &ModelSpecModel,
    policy: &DecisionPolicySnapshotModel,
) -> Result<TrainingDatasetMaterialization<'a>, StorageError>
where
    C: ConnectionTrait,
{
    if dataset.status != TrainingDatasetStatus::Ready || dataset.purpose != purpose {
        return Err(invariant(
            QUANT_TRAINING_DATASET,
            format!(
                "Dataset {} must remain Ready/{purpose}",
                dataset.training_dataset_id
            ),
        ));
    }
    let materialization = dataset.materialization().ok_or_else(|| {
        invariant(
            QUANT_TRAINING_DATASET,
            "Ready Dataset has an incomplete or inconsistent materialization",
        )
    })?;
    let bindings = version.serving_contract.bindings();
    let manifest = materialization.manifest;
    if manifest.training_dataset_id != dataset.training_dataset_id
        || materialization.dataset_hash != &manifest.semantic_dataset_hash
        || dataset.model_spec_id != spec.model_spec_id
        || dataset.model_spec_definition_hash != spec.definition_hash
        || dataset.model_family != spec.model_family
        || manifest.model_spec_id != spec.model_spec_id
        || manifest.model_spec_definition_hash != spec.definition_hash
        || manifest.model_family != spec.model_family
        || dataset.research_profile_artifact_id != bindings.model.profile_ref.artifact_id()
        || dataset.source_lineage.research_profile_artifact_id
            != bindings.model.profile_ref.artifact_id()
        || dataset.decision_policy_snapshot_id != policy.decision_policy_snapshot_id
        || dataset.source_lineage.decision_policy_snapshot_id != policy.decision_policy_snapshot_id
        || dataset.source_lineage.runtime_config_hash != policy.snapshot_hash
        || manifest.source_lineage != dataset.source_lineage
    {
        return Err(invariant(
            QUANT_TRAINING_DATASET,
            "Dataset relational, manifest, model, profile, or policy projections differ",
        ));
    }
    verify_source_slice(db, dataset).await?;
    Ok(materialization)
}

async fn verify_source_slice<C>(db: &C, dataset: &TrainingDatasetInfo) -> Result<(), StorageError>
where
    C: ConnectionTrait,
{
    let lineage = &dataset.source_lineage;
    let source = SourceSliceEntity::find_by_id(lineage.source_slice_id)
        .lock_shared()
        .one(db)
        .await
        .map_err(StorageError::from)?
        .ok_or_else(|| StorageError::not_found(QUANT_SOURCE_SLICE, lineage.source_slice_id))?;
    let identity = SourceSliceIdentity::derive(SourceSliceIdentityInput {
        profile_ref: source.profile_ref.clone(),
        evaluation_track: source.evaluation_track,
        research_program_hash: source.research_program_hash,
        decision_policy_snapshot_id: source.decision_policy_snapshot_id,
        runtime_config_hash: source.runtime_config_hash,
        window_start: source.window_start,
        window_end: source.window_end,
        pit_cutoff: source.pit_cutoff,
    })
    .map_err(|error| invariant(QUANT_SOURCE_SLICE, error.to_string()))?;
    let manifest = source.manifest.as_ref().ok_or_else(|| {
        invariant(
            QUANT_SOURCE_SLICE,
            "Ready Source Slice has no typed manifest",
        )
    })?;
    lineage
        .verify_manifest(manifest)
        .map_err(|error| invariant(QUANT_SOURCE_SLICE, error.to_string()))?;
    if source.status != SourceSliceStatus::Ready
        || identity.identity_hash != source.identity_hash
        || source.identity_hash != lineage.source_slice_identity_hash
        || source.profile_ref != lineage.research_profile_artifact_id.profile_ref()
        || source.research_program_hash != lineage.research_program_hash
        || source.decision_policy_snapshot_id != lineage.decision_policy_snapshot_id
        || source.runtime_config_hash != lineage.runtime_config_hash
        || source.window_start != lineage.source_window_start
        || source.window_end != lineage.source_window_end
        || source.pit_cutoff != lineage.pit_cutoff
        || source.reader_contract_version != lineage.reader_contract_version
        || source.schema_contract_version != lineage.schema_contract_version
        || source.manifest_uri.as_ref() != Some(&lineage.source_slice.manifest_uri)
        || source.manifest_hash != Some(lineage.source_slice.manifest_hash)
    {
        return Err(invariant(
            QUANT_SOURCE_SLICE,
            "Source Slice ledger differs from the Dataset frozen lineage",
        ));
    }
    Ok(())
}

fn invariant(entity: &'static str, detail: impl Into<String>) -> StorageError {
    StorageError::invariant_violation(Some(entity), detail.into())
}
