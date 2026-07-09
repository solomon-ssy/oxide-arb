//! `quant_backtest_path_set` table entity (Phase 11.5 CPCV result).

use crate::types::{
    BacktestPathSetId, ContentHash, ModelRunId, ModelVersionId, RuntimeConfigVersionId,
    TrainingDatasetId,
};
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_backtest_path_set")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub path_set_id: BacktestPathSetId,
    pub model_version_id: ModelVersionId,
    pub model_run_id: ModelRunId,
    pub training_dataset_id: TrainingDatasetId,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub path_count: i64,
    pub combination_count: i64,
    pub median_rank_ic: Decimal,
    #[sea_orm(column_type = "JsonBinary")]
    pub sharpe_distribution: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub paths: Json,
    pub deflated_sharpe: Decimal,
    pub dsr_benchmark_sharpe: Decimal,
    pub pbo: Decimal,
    pub min_track_record_length_secs: Option<i64>,
    /// DSR multiple-testing N (= `trial_grid_count`). Same population as the
    /// trial-grid Sharpe variance V used in the Deflated Sharpe Ratio.
    pub trial_count: i64,
    /// Governed trial-grid configurations evaluated for CSCV/PBO + DSR N/V.
    pub trial_grid_count: i64,
    /// Audit-only: production `coordinate_search` effective trials (not in DSR N).
    pub coord_search_effective_n: i64,
    pub path_set_hash: ContentHash,
    pub created_at: DateTime<Utc>,
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
        belongs_to = "super::quant_model_run::Entity",
        from = "Column::ModelRunId",
        to = "super::quant_model_run::Column::ModelRunId"
    )]
    ModelRun,
    #[sea_orm(
        belongs_to = "super::quant_training_dataset::Entity",
        from = "Column::TrainingDatasetId",
        to = "super::quant_training_dataset::Column::TrainingDatasetId"
    )]
    TrainingDataset,
}

impl Related<super::quant_model_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelVersion.def()
    }
}

impl Related<super::quant_model_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::ModelRun.def()
    }
}

impl Related<super::quant_training_dataset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::TrainingDataset.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
