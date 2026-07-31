//! `quant_feedback_trigger_event` append-only provenance ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_feedback_cycle, user};
use crate::{
    enums::quant::FeedbackTriggerFamily,
    types::{ContentHash, FeedbackCycleId, FeedbackTriggerEventId, RoleCode, UserId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_trigger_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feedback_trigger_event_id: FeedbackTriggerEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub trigger_family: FeedbackTriggerFamily,
    pub actor_user_id: Option<UserId>,
    pub actor_label: String,
    pub actor_role: Option<RoleCode>,
    pub reason_code: String,
    pub event_hash: ContentHash,
    pub occurred_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FeedbackCycle",
        from = "feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ActorUser",
        from = "actor_user_id",
        to = "id"
    )]
    pub actor_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
