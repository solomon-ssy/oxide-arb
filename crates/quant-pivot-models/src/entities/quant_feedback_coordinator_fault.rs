//! `quant_feedback_coordinator_fault` append-only quarantine evidence.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_feedback_cycle;
use crate::{
    enums::quant::FeedbackStage,
    types::{
        ContentHash, FeedbackCoordinatorFaultId, FeedbackCycleId, FeedbackStageEventId, WorkerId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_coordinator_fault")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feedback_coordinator_fault_id: FeedbackCoordinatorFaultId,
    #[sea_orm(unique)]
    pub feedback_cycle_id: FeedbackCycleId,
    pub lease_generation: i64,
    pub worker_id: WorkerId,
    pub active_stage: Option<FeedbackStage>,
    pub last_event_sequence: Option<i64>,
    pub last_stage_event_id: Option<FeedbackStageEventId>,
    pub last_stage_event_hash: Option<ContentHash>,
    pub fault_code: String,
    pub detail: String,
    pub detail_hash: ContentHash,
    pub fault_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FeedbackCycle",
        from = "feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
