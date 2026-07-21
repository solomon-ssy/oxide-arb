//! Append-only aggregate ledger for missed report schedule occurrences.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::decision_policy_snapshot;
use crate::{
    enums::quant::ReportScheduleGapReason,
    types::{DecisionPolicySnapshotId, ReportScheduleGapId, ReportScheduleId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_schedule_gap")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub gap_id: ReportScheduleGapId,
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub reason: ReportScheduleGapReason,
    pub first_scheduled_for: DateTime<Utc>,
    pub last_scheduled_for: DateTime<Utc>,
    pub missed_count: i64,
    pub detected_at: DateTime<Utc>,
    #[sea_orm(column_type = "Text", nullable)]
    pub detail: Option<String>,

    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
