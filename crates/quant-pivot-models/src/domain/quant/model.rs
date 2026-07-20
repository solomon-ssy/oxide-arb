//! Model registry persistence DTOs.

use crate::{
    entities::quant_model_version,
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            ModelRunErrorCode, ModelRunKind, ModelRunStatus, ModelVersionDerivationKind,
            PublicationStatus,
        },
    },
    types::{
        BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash,
        DecisionPolicySnapshotId, MarketSelectionId, ModelInputContract, ModelRunId, ModelSpecId,
        ModelTrainingContract, ModelVersionId, ResearchProfileRef, RoleCode, SchemaVersion,
        TradePolicyArtifactId, TrainingDatasetId, UserId,
        calibration::ScoreMultiplierCalibrationReport,
        model_lineage::{ModelVersionDerivation, ModelVersionDerivationError},
        model_metrics::ModelVersionMetrics,
        model_quality::QualityGateReport,
        model_spec::{ModelSpecDefinition, ModelSpecThesis},
        model_training::ModelTrainingObjective,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{ActiveValue::Set, DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

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
    pub version: i32,
    pub artifact_hash: ContentHash,
    pub category_scope: Option<MarketCategory>,
    #[sea_orm(nested(prefix = "profile_ref_"))]
    pub profile_ref: ResearchProfileRef,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub publish_path_set_id: Option<BacktestPathSetId>,
    pub derivation_kind: ModelVersionDerivationKind,
    pub parent_model_version_id: Option<ModelVersionId>,
    pub source_backtest_report_id: Option<BacktestReportId>,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    pub score_multiplier_calibration_report: Option<ScoreMultiplierCalibrationReport>,
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
            self.parent_model_version_id.clone(),
            self.source_backtest_report_id.clone(),
            self.calibration_artifact_id.clone(),
            self.score_multiplier_calibration_report.clone(),
            self.derivation_evidence_hash.clone(),
        )
    }
}

/// Insert payload for `quant_model_version`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewModelVersion {
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
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

    /// Decompose typed derivation evidence into FK-backed persistence columns.
    pub fn try_into_active_model(
        self,
    ) -> Result<quant_model_version::ActiveModel, ModelVersionDerivationError> {
        let derivation_evidence_hash = self.derivation.evidence_hash()?;
        let derivation_kind = self.derivation.kind();
        let parent_model_version_id = self.derivation.parent_model_version_id().cloned();
        let source_backtest_report_id = self.derivation.source_backtest_report_id().cloned();
        let calibration_artifact_id = self.derivation.calibration_artifact_id().cloned();
        let score_multiplier_calibration_report =
            self.derivation.score_multiplier_report().cloned();

        Ok(quant_model_version::ActiveModel {
            model_version_id: Set(self.model_version_id),
            model_spec_id: Set(self.model_spec_id),
            version: Set(self.version),
            artifact_hash: Set(self.artifact_hash),
            category_scope: Set(self.category_scope),
            research_profile_artifact_id: Set(self.profile_ref.artifact_id()),
            training_dataset_id: Set(self.training_dataset_id),
            trade_policy_artifact_id: Set(self.trade_policy_artifact_id),
            trade_policy_hash: Set(self.trade_policy_hash),
            publish_path_set_id: Set(self.publish_path_set_id),
            derivation_kind: Set(derivation_kind),
            parent_model_version_id: Set(parent_model_version_id),
            source_backtest_report_id: Set(source_backtest_report_id),
            calibration_artifact_id: Set(calibration_artifact_id),
            score_multiplier_calibration_report: Set(score_multiplier_calibration_report),
            derivation_evidence_hash: Set(derivation_evidence_hash),
            metrics: Set(self.metrics),
            training_objective: Set(self.training_objective),
            quality_gate_report: Set(self.quality_gate_report),
            publication_status: Set(self.publication_status),
            published_at: Set(self.published_at),
            retired_at: Set(self.retired_at),
            created_at: sea_orm::ActiveValue::NotSet,
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

/// Insert payload for `quant_model_run`.
///
/// Covers every `ActiveModel` column (no DB-managed timestamps); `SeaORM`'s derive
/// emits a redundant `..Default::default()` that triggers `needless_update`.
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
    pub status: ModelRunStatus,
    pub input_hash: ContentHash,
    pub output_hash: Option<ContentHash>,
    pub error_code: Option<ModelRunErrorCode>,
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Runtime model-run aggregate before persistence.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantModelRunModel {
    pub run: NewModelRun,
}
