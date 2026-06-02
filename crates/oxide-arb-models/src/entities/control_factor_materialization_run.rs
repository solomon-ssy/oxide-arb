//! `control_factor_materialization_run` table entity.

use crate::{enums::control_factor::MaterializationRunStatus, types::MaterializationRunId};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_materialization_run")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub materialization_run_id: MaterializationRunId,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    #[sea_orm(column_type = "JsonBinary")]
    pub manifest: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub report: Json,
    #[sea_orm(column_type = "Text")]
    pub code_git_sha: String,
    #[sea_orm(column_type = "Text")]
    pub query_fingerprint: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
