//! `quant_feedback_stage_event` append-only timeline.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::{FeedbackStage, FeedbackStageEventKind},
    types::{ArtifactUri, ContentHash, FeedbackCycleId, FeedbackStageEventId, ResearchJobId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_stage_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feedback_stage_event_id: FeedbackStageEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub event_sequence: i64,
    pub stage: FeedbackStage,
    pub event_kind: FeedbackStageEventKind,
    pub research_job_id: Option<ResearchJobId>,
    pub actor: Option<String>,
    pub reason_code: Option<String>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub occurred_at: DateTime<Utc>,
    pub event_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
