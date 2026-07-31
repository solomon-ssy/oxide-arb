//! Mutable delivery state for resolution inbox projection into canonical facts.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::quant_resolution_observation_inbox;
use crate::{
    enums::quant::ResolutionProjectionStatus,
    types::{ContentHash, ResolutionObservationId, WorkerId},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_resolution_observation_projection")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub resolution_observation_id: ResolutionObservationId,
    pub source_checkpoint_hash: ContentHash,
    pub status: ResolutionProjectionStatus,
    pub attempt_count: i32,
    pub claim_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub next_attempt_at: Option<DateTime<Utc>>,
    #[sea_orm(column_type = "Text", nullable)]
    pub last_error: Option<String>,
    pub canonical_fact_hash: Option<ContentHash>,
    pub verified_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Observation",
        from = "resolution_observation_id",
        to = "resolution_observation_id"
    )]
    pub observation: BelongsTo<quant_resolution_observation_inbox::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
