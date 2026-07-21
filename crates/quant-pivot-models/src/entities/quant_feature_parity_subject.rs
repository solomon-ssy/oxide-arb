//! Frozen deterministic feature-parity subjects.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    quant_feature_parity_candidate, quant_feature_parity_run, quant_market_selection,
    quant_model_run, quant_model_version, quant_recommendation_report, quant_training_dataset,
};
use crate::{
    enums::quant::ParitySubjectKind,
    types::{
        ContentHash, FeatureParityRunId, FeatureParitySubjectId, MarketSelectionId, ModelRunId,
        ModelVersionId, RecommendationReportId, TrainingDatasetId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feature_parity_subject")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub parity_subject_id: FeatureParitySubjectId,
    pub run_id: FeatureParityRunId,
    pub subject_kind: ParitySubjectKind,
    pub model_run_id: Option<ModelRunId>,
    pub recommendation_report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub market_selection_id: Option<MarketSelectionId>,
    pub subject_generation: ContentHash,
    pub decision_at: Option<DateTime<Utc>>,
    pub selection_hash: Option<ContentHash>,
    pub evidence_hash: ContentHash,
    pub created_at: DateTime<Utc>,

    #[sea_orm(belongs_to, relation_enum = "Run", from = "run_id", to = "run_id")]
    pub run: BelongsTo<quant_feature_parity_run::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelRun",
        from = "model_run_id",
        to = "model_run_id"
    )]
    pub model_run: BelongsTo<Option<quant_model_run::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RecommendationReport",
        from = "recommendation_report_id",
        to = "recommendation_report_id"
    )]
    pub recommendation_report: BelongsTo<Option<quant_recommendation_report::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<Option<quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TrainingDataset",
        from = "training_dataset_id",
        to = "training_dataset_id"
    )]
    pub training_dataset: BelongsTo<Option<quant_training_dataset::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "MarketSelection",
        from = "market_selection_id",
        to = "market_selection_id"
    )]
    pub market_selection: BelongsTo<Option<quant_market_selection::Entity>>,
    #[sea_orm(has_many, relation_enum = "Candidate")]
    pub candidate: HasMany<quant_feature_parity_candidate::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
