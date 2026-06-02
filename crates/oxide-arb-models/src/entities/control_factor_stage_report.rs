//! `control_factor_stage_report` table entity.

use crate::{
    enums::control_factor::EvidenceStageStatus,
    types::{MaterializationRunId, StageReportId},
};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "control_factor_stage_report")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub stage_report_id: StageReportId,
    pub materialization_run_id: MaterializationRunId,
    #[sea_orm(column_type = "Text")]
    pub stage_name: String,
    pub status: EvidenceStageStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    pub coverage: Json,
    #[sea_orm(column_type = "JsonBinary")]
    pub warnings: Json,
    pub created_at: DateTime<Utc>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::control_factor_materialization_run::Entity",
        from = "Column::MaterializationRunId",
        to = "super::control_factor_materialization_run::Column::MaterializationRunId"
    )]
    MaterializationRun,
}

impl Related<super::control_factor_materialization_run::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::MaterializationRun.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
