//! Irreversible production lifecycle seal evidence.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::user;
use crate::{
    enums::runtime_config::PolicyActorKind,
    runtime_config::ProductionSealEvidence,
    types::{
        BuildCommitHash, ContentHash, DecisionPolicySnapshotId, DeploymentEnvironment,
        PolicyBundleGeneration, ProductionBaselineId, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "system_production_baseline")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub production_baseline_id: ProductionBaselineId,
    #[sea_orm(column_type = "Text")]
    pub environment: DeploymentEnvironment,
    pub sealed_at: DateTime<Utc>,
    pub sealed_by_kind: PolicyActorKind,
    pub sealed_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text")]
    pub sealed_by_label: String,
    #[sea_orm(column_type = "Text")]
    pub build_commit: BuildCommitHash,
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_bundle_hash: ContentHash,
    pub lifecycle_policy_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub evidence: ProductionSealEvidence,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "SealedByUser",
        from = "sealed_by_user_id",
        to = "id"
    )]
    pub sealed_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
