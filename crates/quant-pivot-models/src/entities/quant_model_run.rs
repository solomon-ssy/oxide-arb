//! `quant_model_run` table entity.

use crate::{
    enums::quant::{ModelRunErrorCode, ModelRunKind, ModelRunStatus},
    types::{ContentHash, MarketSelectionId, ModelRunId, ModelVersionId, RuntimeConfigVersionId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_run_id: ModelRunId,
    pub run_kind: ModelRunKind,
    pub model_version_id: Option<ModelVersionId>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub market_selection_id: Option<MarketSelectionId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: ModelRunStatus,
    pub input_hash: ContentHash,
    pub output_hash: Option<ContentHash>,
    #[sea_orm(column_type = "JsonBinary")]
    pub metrics_json: Json,
    pub error_code: Option<ModelRunErrorCode>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ModelVersion",
        from = "model_version_id",
        to = "model_version_id"
    )]
    pub model_version: BelongsTo<Option<super::quant_model_version::Entity>>,
    #[sea_orm(
        belongs_to,
        relation_enum = "MarketSelection",
        from = "market_selection_id",
        to = "market_selection_id"
    )]
    pub market_selection: BelongsTo<Option<super::quant_market_selection::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
