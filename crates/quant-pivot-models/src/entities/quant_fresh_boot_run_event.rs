//! `quant_fresh_boot_run_event` append-only orchestration timeline.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::{FreshBootEventKind, FreshBootStage, FreshBootStatus},
    types::{ContentHash, FreshBootRunEventId, FreshBootRunId, ResearchJobId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_fresh_boot_run_event")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub event_id: FreshBootRunEventId,
    pub run_id: FreshBootRunId,
    pub event_sequence: i64,
    pub from_stage: FreshBootStage,
    pub to_stage: FreshBootStage,
    pub from_status: FreshBootStatus,
    pub to_status: FreshBootStatus,
    pub event_kind: FreshBootEventKind,
    pub research_job_id: Option<ResearchJobId>,
    pub result_ref: Option<Uuid>,
    pub evidence_hash: Option<ContentHash>,
    pub attempt: i32,
    pub actor: String,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub event_hash: ContentHash,
}

impl ActiveModelBehavior for ActiveModel {}
