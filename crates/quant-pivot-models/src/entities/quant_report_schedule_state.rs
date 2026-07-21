//! Durable derived state for one configured report schedule.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::decision_policy_snapshot;
use crate::types::{ContentHash, DecisionPolicySnapshotId, ReportScheduleId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_report_schedule_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub schedule_id: ReportScheduleId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub spec_hash: ContentHash,
    pub next_scheduled_for: DateTime<Utc>,
    pub last_materialized_for: Option<DateTime<Utc>>,
    pub enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "DecisionPolicySnapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub decision_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
