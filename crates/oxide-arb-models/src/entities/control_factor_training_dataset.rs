//! `control_factor_training_dataset` table entity.

use crate::{
    enums::control_factor::ControlFactorType,
    types::{MaterializationRunId, TrainingDatasetId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_training_dataset")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub dataset_id: TrainingDatasetId,
    pub materialization_run_id: MaterializationRunId,
    pub factor_type: ControlFactorType,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub entity_count: i32,
    pub example_count: i32,
    pub label_count: i32,
    #[sea_orm(column_type = "Text")]
    pub dataset_hash: String,
    #[sea_orm(column_type = "Text")]
    pub feature_schema_hash: String,
    #[sea_orm(column_type = "Text")]
    pub label_schema_hash: String,
    #[sea_orm(column_type = "Text", nullable)]
    pub storage_uri: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
