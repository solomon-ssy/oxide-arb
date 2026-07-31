//! Database-authoritative feedback scheduler state.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_feedback_cycle, research_profile_artifact};
use crate::types::{
    ContentHash, FeedbackCycleId, ResearchProfileArtifactId, ResearchProfileId, WorkerId,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_scheduler_state")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub research_profile_id: ResearchProfileId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub feedback_policy_hash: ContentHash,
    pub cadence_secs: i64,
    pub cooldown_secs: i64,
    pub next_due_at: DateTime<Utc>,
    pub last_cycle_id: Option<FeedbackCycleId>,
    pub last_cutoff: Option<DateTime<Utc>>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub retry_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub paused: bool,
    pub pause_revision: i64,
    pub pause_reason_code: Option<String>,
    pub pause_note: Option<String>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,

    #[sea_orm(
        belongs_to,
        relation_enum = "LastCycle",
        from = "last_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub last_cycle: BelongsTo<Option<quant_feedback_cycle::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
