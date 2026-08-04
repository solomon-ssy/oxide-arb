//! Append-only governed remediation evidence for resolution projections.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{quant_resolution_observation_projection, user};
use crate::{
    enums::quant::{
        ResolutionProjectionErrorCode, ResolutionProjectionStatus, ResolutionRemediationAction,
    },
    types::{
        ContentHash, PolicyIdempotencyKey, ResolutionObservationId, ResolutionRemediationId,
        RoleCode, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_resolution_projection_remediation")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub remediation_id: ResolutionRemediationId,
    pub resolution_observation_id: ResolutionObservationId,
    pub expected_revision: i64,
    pub committed_revision: i64,
    pub action: ResolutionRemediationAction,
    pub prior_status: ResolutionProjectionStatus,
    pub prior_error_code: ResolutionProjectionErrorCode,
    #[sea_orm(column_type = "Text")]
    pub prior_error: String,
    pub resulting_status: ResolutionProjectionStatus,
    #[sea_orm(unique)]
    pub idempotency_key: PolicyIdempotencyKey,
    #[sea_orm(unique)]
    pub request_hash: ContentHash,
    pub reason_code: String,
    #[sea_orm(column_type = "Text")]
    pub operator_note: String,
    pub actor_user_id: UserId,
    pub actor_username: String,
    pub actor_role: RoleCode,
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Projection",
        from = "resolution_observation_id",
        to = "resolution_observation_id"
    )]
    pub projection: BelongsTo<quant_resolution_observation_projection::Entity>,
    #[sea_orm(belongs_to, relation_enum = "Actor", from = "actor_user_id", to = "id")]
    pub actor: BelongsTo<user::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
