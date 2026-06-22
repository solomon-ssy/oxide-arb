//! `quant_model_run` table entity.

use crate::{
    enums::quant::{ModelRunKind, ModelRunStatus},
    types::{ModelRunId, ModelVersionId, RuntimeConfigVersionId, UniverseSnapshotId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub model_run_id: ModelRunId,
    pub run_kind: ModelRunKind,
    pub model_version_id: Option<ModelVersionId>,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub universe_snapshot_id: Option<UniverseSnapshotId>,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub status: ModelRunStatus,
    #[sea_orm(column_type = "Text")]
    pub input_hash: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub output_hash: Option<String>,
    #[sea_orm(column_type = "JsonBinary")]
    pub metrics_json: Json,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_code: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub error_message: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::quant_model_version::Entity",
        from = "Column::ModelVersionId",
        to = "super::quant_model_version::Column::ModelVersionId"
    )]
    ModelVersion,
    #[sea_orm(
        belongs_to = "super::quant_universe_snapshot::Entity",
        from = "Column::UniverseSnapshotId",
        to = "super::quant_universe_snapshot::Column::UniverseSnapshotId"
    )]
    UniverseSnapshot,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_universe_snapshot::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::UniverseSnapshot.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
