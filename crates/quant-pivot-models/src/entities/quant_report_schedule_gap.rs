//! Append-only aggregate ledger for missed report schedule occurrences.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::ReportScheduleGapReason,
    types::{ReportScheduleGapId, RuntimeConfigVersionId},
};

#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_schedule_gap")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub gap_id: ReportScheduleGapId,
    pub schedule_id: String,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub reason: ReportScheduleGapReason,
    pub first_scheduled_for: DateTime<Utc>,
    pub last_scheduled_for: DateTime<Utc>,
    pub missed_count: i64,
    pub detected_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text", nullable)]
    pub detail: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[sea_orm(
        belongs_to = "super::runtime_config_version::Entity",
        from = "Column::RuntimeConfigVersionId",
        to = "super::runtime_config_version::Column::RuntimeConfigVersionId"
    )]
    RuntimeConfigVersion,
}

impl Related<super::runtime_config_version::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::RuntimeConfigVersion.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
