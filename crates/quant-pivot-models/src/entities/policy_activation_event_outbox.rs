//! Durable append-only event identity for committed policy bundles.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{decision_policy_snapshot, policy_activation, policy_activation_audit};
use crate::types::{
    AuditEventId, ContentHash, DecisionPolicySnapshotId, PolicyActivationId, PolicyBundleGeneration,
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_activation_event_outbox")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub audit_event_id: AuditEventId,
    #[sea_orm(unique)]
    pub policy_activation_id: PolicyActivationId,
    pub bundle_generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Audit",
        from = "audit_event_id",
        to = "audit_event_id"
    )]
    pub audit: BelongsTo<policy_activation_audit::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Activation",
        from = "policy_activation_id",
        to = "policy_activation_id"
    )]
    pub activation: BelongsTo<policy_activation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "Snapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub snapshot: BelongsTo<decision_policy_snapshot::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
