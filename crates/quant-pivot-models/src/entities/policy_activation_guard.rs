//! Singleton row used to serialize policy activations with a typed row lock.

use crate::types::{ContentHash, DecisionPolicySnapshotId, PolicyBundleGeneration};
use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "policy_activation_guard")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub id: i16,
    pub generation: PolicyBundleGeneration,
    pub current_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub current_snapshot_hash: Option<ContentHash>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "CurrentSnapshot",
        from = "current_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub current_snapshot: BelongsTo<Option<super::decision_policy_snapshot::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
