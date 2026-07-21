//! Immutable production-seal evidence bound to one exact deploy state.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{decision_policy_snapshot, user};
use crate::{
    enums::runtime_config::{PolicyActorKind, ProductionEvidenceKind},
    types::{
        ArtifactUri, BuildCommitHash, ContentHash, DecisionPolicySnapshotId,
        PolicyBundleGeneration, ProductionEvidenceId, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "system_production_evidence")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub production_evidence_id: ProductionEvidenceId,
    pub kind: ProductionEvidenceKind,
    pub artifact_uri: ArtifactUri,
    pub evidence_hash: ContentHash,
    #[sea_orm(column_type = "Text")]
    pub build_commit: BuildCommitHash,
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_bundle_hash: ContentHash,
    pub recorded_by_kind: PolicyActorKind,
    pub recorded_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub recorded_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub reason: String,
    pub observed_at: DateTime<Utc>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "Snapshot",
        from = "decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RecordedByUser",
        from = "recorded_by_user_id",
        to = "id"
    )]
    pub recorded_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
