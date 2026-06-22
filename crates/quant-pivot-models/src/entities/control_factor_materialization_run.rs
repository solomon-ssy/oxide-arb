//! `control_factor_materialization_run` table entity.

use crate::{
    enums::control_factor::{
        MaterializationOutputPolicy, MaterializationRunKind, MaterializationRunStatus,
        RunTriggerType,
    },
    types::MaterializationRunId,
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_materialization_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub materialization_run_id: MaterializationRunId,
    #[sea_orm(column_type = "Text", nullable)]
    pub run_dedupe_key: Option<String>,
    pub run_kind: MaterializationRunKind,
    pub trigger_type: RunTriggerType,
    #[sea_orm(column_type = "Text", nullable)]
    pub trigger_ref: Option<String>,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub market_filter: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub requested_factor_types: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub data_requirements: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub runtime_config_ref: Json,
    #[sea_orm(column_type = "Text")]
    pub simulation_config_hash: String,
    #[sea_orm(column_type = "Text")]
    pub quality_gate_policy_hash: String,
    pub output_policy: MaterializationOutputPolicy,
    #[sea_orm(column_type = "JsonBinary")]
    pub manifest: Json,
    #[sea_orm(column_type = "Text")]
    pub manifest_hash: String,
    #[sea_orm(column_type = "JsonBinary")]
    pub report: Json,
    #[sea_orm(column_type = "Text")]
    pub code_git_sha: String,
    #[sea_orm(column_type = "Text")]
    pub created_by: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub failure_code: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub failure_detail: Option<String>,
    #[sea_orm(column_type = "Text", nullable)]
    pub report_uri: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
