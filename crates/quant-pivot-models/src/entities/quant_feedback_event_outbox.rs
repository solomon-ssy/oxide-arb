//! Durable control-plane revision outbox for feedback stage events.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_feedback_stage_event;
use crate::types::{FeedbackStageEventId, WorkerId};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_event_outbox")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub revision: i64,
    #[sea_orm(unique)]
    pub feedback_stage_event_id: FeedbackStageEventId,
    pub published_at: Option<DateTime<Utc>>,
    pub publish_attempts: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "StageEvent",
        from = "feedback_stage_event_id",
        to = "feedback_stage_event_id"
    )]
    pub stage_event: BelongsTo<quant_feedback_stage_event::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
