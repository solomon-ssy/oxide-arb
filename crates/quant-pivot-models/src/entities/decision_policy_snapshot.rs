//! `decision_policy_snapshot` table entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{policy_activation, user};
use crate::{
    enums::runtime_config::{DecisionPolicySnapshotSource, PolicyActorKind},
    runtime_config::DecisionPolicySnapshotDocument,
    types::{
        ContentHash, DecisionPolicySnapshotId, PolicyBundleGeneration, PolicyRevisionId, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "decision_policy_snapshot")]
pub struct Model {
    #[sea_orm(unique)]
    pub bundle_generation: PolicyBundleGeneration,
    #[sea_orm(primary_key, auto_increment = false)]
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub snapshot: DecisionPolicySnapshotDocument,
    pub recommendation_policy_revision_id: PolicyRevisionId,
    pub execution_risk_policy_revision_id: PolicyRevisionId,
    pub model_routing_revision_id: PolicyRevisionId,
    pub report_schedule_revision_id: PolicyRevisionId,
    pub operational_control_revision_id: PolicyRevisionId,
    pub execution_authorization_revision_id: PolicyRevisionId,
    pub source: DecisionPolicySnapshotSource,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub created_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(has_many, relation_enum = "Activation")]
    pub activation: HasMany<policy_activation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CreatedByUser",
        from = "created_by_user_id",
        to = "id"
    )]
    pub created_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
