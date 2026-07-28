//! Model registry persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveValue, ActiveValue::Set, DeriveIntoActiveModel, DerivePartialModel, FromQueryResult,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    entities::quant_model_version::ActiveModel,
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            ModelRunErrorCode, ModelRunKind, ModelRunStatus, ModelVersionDerivationKind,
            PublicationStatus,
        },
    },
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        MarketSelectionId, ModelInputContract, ModelRunId, ModelSpecId, ModelTrainingContract,
        ModelVersionId, ResearchProfileRef, RoleCode, SchemaVersion, TradePolicyArtifactId,
        TrainingDatasetId, UserId,
        model_lineage::{ModelVersionDerivation, ModelVersionDerivationError},
        model_metrics::ModelVersionMetrics,
        model_quality::QualityGateReport,
        model_serving::{ModelServingContract, ModelServingContractError},
        model_spec::{ModelSpecDefinition, ModelSpecThesis},
        model_training::ModelTrainingObjective,
    },
};

/// Governed model specification row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_model_spec::Entity")]
pub struct ModelSpecInfo {
    pub model_spec_id: ModelSpecId,
    pub name: String,
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub thesis: ModelSpecThesis,
    /// Ordered, governed raw-input contract. Transform-generated columns never
    /// enter this source-level contract.
    pub input_contract: ModelInputContract,
    pub training_contract: ModelTrainingContract,
    pub definition_hash: ContentHash,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub created_by_role: Option<RoleCode>,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(ModelSpecInfo, crate::entities::quant_model_spec::Model, {
    model_spec_id, name, model_family, prediction_horizon_secs, feature_schema_version,
    label_schema_version, thesis, input_contract, training_contract, definition_hash,
    created_by_user_id, created_by_label, created_by_role, reason, created_at,
});

impl ModelSpecInfo {
    #[must_use]
    pub fn definition(&self) -> ModelSpecDefinition<'_> {
        ModelSpecDefinition {
            name: &self.name,
            model_family: self.model_family,
            prediction_horizon_secs: self.prediction_horizon_secs,
            feature_schema_version: self.feature_schema_version,
            label_schema_version: self.label_schema_version,
            thesis: &self.thesis,
            input_contract: &self.input_contract,
            training_contract: &self.training_contract,
        }
    }
}

/// Insert payload for `quant_model_spec`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_spec::ActiveModel")]
pub struct NewModelSpec {
    pub model_spec_id: ModelSpecId,
    pub name: String,
    pub model_family: ModelFamily,
    pub prediction_horizon_secs: i64,
    pub feature_schema_version: SchemaVersion,
    pub label_schema_version: SchemaVersion,
    pub thesis: ModelSpecThesis,
    pub input_contract: ModelInputContract,
    pub training_contract: ModelTrainingContract,
    pub definition_hash: ContentHash,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub created_by_role: Option<RoleCode>,
    pub reason: String,
}

/// Published or candidate model version row, enriched with the owning spec's
/// immutable N:1 identity and research-definition fields from the owning spec.
///
/// `model_family` is **not** a `quant_model_version` column — repository reads
/// always `INNER JOIN quant_model_spec` (or reload via that join after writes).
/// It is not present on [`NewModelVersion`] / patches.
#[derive(Debug, Clone, Serialize, Deserialize, FromQueryResult)]
pub struct ModelVersionInfo {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub model_spec_name: String,
    /// Immutable family from the owning `quant_model_spec` (JOIN projection).
    pub model_family: ModelFamily,
    pub model_spec_thesis: ModelSpecThesis,
    pub model_spec_definition_hash: ContentHash,
    pub model_spec_prediction_horizon_secs: i64,
    pub version: i32,
    pub artifact_hash: ContentHash,
    /// Complete, sealed dependency graph used to load this exact model.
    pub serving_contract: ModelServingContract,
    /// Normalized raw 32-byte database digest projected back into the canonical
    /// in-memory hash type.
    pub serving_contract_hash: ContentHash,
    pub category_scope: Option<MarketCategory>,
    #[sea_orm(nested(prefix = "profile_ref_"))]
    pub profile_ref: ResearchProfileRef,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub publish_path_set_id: Option<BacktestPathSetId>,
    pub derivation_kind: ModelVersionDerivationKind,
    pub parent_model_version_id: Option<ModelVersionId>,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub derivation_evidence_hash: Option<ContentHash>,
    pub metrics: ModelVersionMetrics,
    pub training_objective: ModelTrainingObjective,
    pub quality_gate_report: Option<QualityGateReport>,
    pub publication_status: PublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl ModelVersionInfo {
    /// Canonical derivation kind for root training fixtures and projections.
    #[must_use]
    pub const fn training_derivation_kind() -> ModelVersionDerivationKind {
        ModelVersionDerivationKind::Training
    }

    /// Reconstruct and hash-verify the immutable persisted lineage.
    pub fn verified_derivation(
        &self,
    ) -> Result<ModelVersionDerivation, ModelVersionDerivationError> {
        ModelVersionDerivation::from_persistence(
            self.derivation_kind,
            self.parent_model_version_id,
            self.calibration_artifact_id,
            self.derivation_evidence_hash,
        )
    }

    /// Revalidate the sealed contract, its normalized database hash, and every
    /// model-version/spec projection carried by this joined row.
    pub fn verified_serving_contract(
        &self,
    ) -> Result<&ModelServingContract, ModelVersionPersistenceError> {
        self.serving_contract
            .verify_persisted_hash(self.serving_contract_hash)?;
        let bindings = self.serving_contract.bindings();
        let model = &bindings.model;
        if model.model_version_id != self.model_version_id {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "model_version_id",
            });
        }
        if model.model_spec_id != self.model_spec_id {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "model_spec_id",
            });
        }
        if model.model_spec_definition_hash != self.model_spec_definition_hash {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "model_spec_definition_hash",
            });
        }
        if model.model_family != self.model_family {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "model_family",
            });
        }
        if model.category_scope != self.category_scope {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "category_scope",
            });
        }
        if model.profile_ref != self.profile_ref {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "research_profile",
            });
        }
        if i64::try_from(model.prediction_horizon_secs)
            .ok()
            .as_ref()
            .is_none_or(|horizon| *horizon != self.model_spec_prediction_horizon_secs)
        {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "prediction_horizon_secs",
            });
        }
        if self.training_dataset_id != Some(bindings.dataset.manifest.training_dataset_id) {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "training_dataset_id",
            });
        }
        let trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        if trade_policy != self.trade_policy_artifact_id.zip(self.trade_policy_hash) {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "trade_policy",
            });
        }
        Ok(&self.serving_contract)
    }
}

/// Typed failures while projecting a sealed serving contract into persistence.
#[derive(Debug, Error)]
pub enum ModelVersionPersistenceError {
    #[error(transparent)]
    Derivation(#[from] ModelVersionDerivationError),
    #[error(transparent)]
    ServingContract(#[from] ModelServingContractError),
    #[error("model-serving contract does not match persisted `{binding}`")]
    ServingProjectionMismatch { binding: &'static str },
}

/// Insert payload for `quant_model_version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelVersion {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub serving_contract: ModelServingContract,
    pub category_scope: Option<MarketCategory>,
    pub profile_ref: ResearchProfileRef,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub publish_path_set_id: Option<BacktestPathSetId>,
    pub derivation: ModelVersionDerivation,
    pub metrics: ModelVersionMetrics,
    pub training_objective: ModelTrainingObjective,
    pub quality_gate_report: Option<QualityGateReport>,
    pub publication_status: PublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
}

impl NewModelVersion {
    /// Canonical root lineage for a newly trained model artifact.
    #[must_use]
    pub const fn training_derivation() -> ModelVersionDerivation {
        ModelVersionDerivation::Training
    }

    /// Validate the sealed contract and every scalar projection available on
    /// the insert payload. The database hash is always derived here; callers
    /// cannot supply a second, potentially drifting value.
    pub fn serving_contract_hash(&self) -> Result<ContentHash, ModelVersionPersistenceError> {
        self.serving_contract.validate()?;
        let bindings = self.serving_contract.bindings();
        let model = &bindings.model;
        if model.model_version_id != self.model_version_id {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "model_version_id",
            });
        }
        if model.model_spec_id != self.model_spec_id {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "model_spec_id",
            });
        }
        if model.category_scope != self.category_scope {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "category_scope",
            });
        }
        if model.profile_ref != self.profile_ref {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "research_profile",
            });
        }
        if self.training_dataset_id != Some(bindings.dataset.manifest.training_dataset_id) {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "training_dataset_id",
            });
        }
        let trade_policy = bindings
            .trade_policy
            .as_ref()
            .map(|binding| (binding.artifact_id, binding.content_hash));
        if trade_policy != self.trade_policy_artifact_id.zip(self.trade_policy_hash) {
            return Err(ModelVersionPersistenceError::ServingProjectionMismatch {
                binding: "trade_policy",
            });
        }
        Ok(self.serving_contract.contract_hash())
    }
}

impl TryFrom<NewModelVersion> for ActiveModel {
    type Error = ModelVersionPersistenceError;

    /// Decompose validated serving and derivation contracts into FK-backed
    /// persistence columns.
    fn try_from(version: NewModelVersion) -> Result<Self, Self::Error> {
        let serving_contract_hash = version.serving_contract_hash()?;
        let derivation_evidence_hash = version.derivation.evidence_hash()?;
        let derivation_kind = version.derivation.kind();
        let parent_model_version_id = version.derivation.parent_model_version_id().copied();
        let calibration_artifact_id = version.derivation.calibration_artifact_id().copied();

        Ok(Self {
            model_version_id: Set(version.model_version_id),
            model_spec_id: Set(version.model_spec_id),
            version: Set(version.version),
            artifact_hash: Set(version.artifact_hash),
            serving_contract: Set(version.serving_contract),
            serving_contract_hash: Set(serving_contract_hash.as_bytes().to_vec()),
            category_scope: Set(version.category_scope),
            research_profile_artifact_id: Set(version.profile_ref.artifact_id()),
            training_dataset_id: Set(version.training_dataset_id),
            trade_policy_artifact_id: Set(version.trade_policy_artifact_id),
            trade_policy_hash: Set(version.trade_policy_hash),
            publish_path_set_id: Set(version.publish_path_set_id),
            derivation_kind: Set(derivation_kind),
            parent_model_version_id: Set(parent_model_version_id),
            calibration_artifact_id: Set(calibration_artifact_id),
            derivation_evidence_hash: Set(derivation_evidence_hash),
            metrics: Set(version.metrics),
            training_objective: Set(version.training_objective),
            quality_gate_report: Set(version.quality_gate_report),
            publication_status: Set(version.publication_status),
            published_at: Set(version.published_at),
            retired_at: Set(version.retired_at),
            created_at: ActiveValue::NotSet,
        })
    }
}

/// Minimal typed projection for the governed model picker.
///
/// This is intentionally produced by one joined `SeaORM` query: the picker does
/// not page model versions and then issue one model-spec lookup per row.
#[derive(Debug, Clone, FromQueryResult)]
pub struct PublishedModelCatalogInfo {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub spec_name: String,
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub model_family: ModelFamily,
    pub category_scope: Option<MarketCategory>,
    pub published_at: Option<DateTime<Utc>>,
}

/// Training, backtest, shadow, or inference run row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_model_run::Entity")]
pub struct ModelRunInfo {
    pub model_run_id: ModelRunId,
    pub run_kind: ModelRunKind,
    pub model_version_id: Option<ModelVersionId>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: Option<MarketSelectionId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: ModelRunStatus,
    pub input_hash: ContentHash,
    pub output_hash: Option<ContentHash>,
    pub error_code: Option<ModelRunErrorCode>,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

info_from_model!(ModelRunInfo, crate::entities::quant_model_run::Model, {
    model_run_id, run_kind, model_version_id, decision_policy_snapshot_id,
    market_selection_id, window_start, window_end, status, input_hash, output_hash,
    error_code, error_message, started_at, finished_at,
});

impl ModelRunInfo {
    /// Compare the immutable run subject used by exact durable-job recovery.
    ///
    /// A Training run begins before its model-version row exists, so its
    /// subject is `None` while Running and is atomically enriched with the
    /// preassigned version only when the training commit succeeds. Every
    /// other run kind has an already-existing model subject and must match it
    /// exactly at start and terminal read-back.
    #[must_use]
    pub fn matches_new(&self, run: &NewModelRun) -> bool {
        self.model_run_id == run.model_run_id
            && self.run_kind == run.run_kind
            && self.matches_model_subject(run)
            && self.decision_policy_snapshot_id == run.decision_policy_snapshot_id
            && self.market_selection_id == run.market_selection_id
            && self.window_start == run.window_start
            && self.window_end == run.window_end
            && self.input_hash == run.input_hash
    }

    fn matches_model_subject(&self, run: &NewModelRun) -> bool {
        self.model_version_id == run.model_version_id
            || (self.run_kind == ModelRunKind::Training
                && self.status == ModelRunStatus::Succeeded
                && run.model_version_id.is_none()
                && self.model_version_id.is_some())
    }
}

/// Insert payload for `quant_model_run`.
///
/// Contains immutable run lineage only. `PostgreSQL` owns the initial `Running`
/// state and `started_at`; terminal state is written exclusively through the
/// repository's guarded lifecycle operations.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_model_run::ActiveModel")]
pub struct NewModelRun {
    pub model_run_id: ModelRunId,
    pub run_kind: ModelRunKind,
    pub model_version_id: Option<ModelVersionId>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub market_selection_id: Option<MarketSelectionId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub input_hash: ContentHash,
}

/// Runtime model-run aggregate before persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantModelRunModel {
    pub run: NewModelRun,
}
