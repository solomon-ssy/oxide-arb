//! `quant_model_version` table entity.

use crate::{
    enums::{
        common::MarketCategory,
        quant::{ModelVersionDerivationKind, PublicationStatus},
    },
    types::{
        BacktestPathSetId, BacktestReportId, CalibrationArtifactId, ContentHash, ModelSpecId,
        ModelVersionId, ResearchProfileArtifactId, TradePolicyArtifactId, TrainingDatasetId,
        calibration::ScoreMultiplierCalibrationReport, model_metrics::ModelVersionMetrics,
        model_quality::QualityGateReport, model_training::ModelTrainingObjective,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_version")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_version_id: ModelVersionId,
    pub model_spec_id: ModelSpecId,
    pub version: i32,
    pub artifact_hash: ContentHash,
    /// Queryable copy of the immutable artifact scope. Runtime loading still
    /// verifies the artifact bytes; catalog reads never deserialize N objects.
    #[sea_orm(column_type = r#"custom("qp_market_category")"#)]
    pub category_scope: Option<MarketCategory>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub trade_policy_artifact_id: Option<TradePolicyArtifactId>,
    pub trade_policy_hash: Option<ContentHash>,
    pub publish_path_set_id: Option<BacktestPathSetId>,
    pub derivation_kind: ModelVersionDerivationKind,
    pub parent_model_version_id: Option<ModelVersionId>,
    pub source_backtest_report_id: Option<BacktestReportId>,
    pub calibration_artifact_id: Option<CalibrationArtifactId>,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub score_multiplier_calibration_report: Option<ScoreMultiplierCalibrationReport>,
    pub derivation_evidence_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub metrics: ModelVersionMetrics,
    #[sea_orm(column_type = "JsonBinary")]
    pub training_objective: ModelTrainingObjective,
    #[sea_orm(column_type = "JsonBinary", nullable)]
    pub quality_gate_report: Option<QualityGateReport>,
    pub publication_status: PublicationStatus,
    pub published_at: Option<DateTime<Utc>>,
    pub retired_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<super::research_profile_artifact::Entity>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelSpec",
        from = "model_spec_id",
        to = "model_spec_id"
    )]
    pub model_spec: BelongsTo<super::quant_model_spec::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TrainingDataset",
        from = "training_dataset_id",
        to = "training_dataset_id"
    )]
    pub training_dataset: BelongsTo<Option<super::quant_training_dataset::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TradePolicyArtifact",
        from = "trade_policy_artifact_id",
        to = "artifact_id"
    )]
    pub trade_policy_artifact: BelongsTo<Option<super::quant_trade_policy_artifact::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ParentModelVersion",
        from = "parent_model_version_id",
        to = "model_version_id"
    )]
    pub parent_model_version: BelongsTo<Option<super::quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "SourceBacktestReport",
        from = "source_backtest_report_id",
        to = "backtest_report_id"
    )]
    pub source_backtest_report: BelongsTo<Option<super::quant_backtest_report::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CalibrationArtifact",
        from = "calibration_artifact_id",
        to = "artifact_id"
    )]
    pub calibration_artifact: BelongsTo<Option<super::quant_calibration_artifact::Entity>>,
    #[sea_orm(has_many, relation_enum = "ModelRun")]
    pub model_run: HasMany<super::quant_model_run::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
