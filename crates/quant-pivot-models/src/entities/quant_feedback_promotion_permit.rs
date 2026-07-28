//! `quant_feedback_promotion_permit` governed lifecycle entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{decision_policy_snapshot, quant_model_version, research_profile_artifact, user};
use crate::{
    enums::{common::MarketCategory, quant::QuantRuntimeMode},
    types::{
        ContentHash, DecisionPolicySnapshotId, ModelVersionId, PolicyBundleGeneration,
        PolicyIdempotencyKey, PromotionPermitId, ResearchProfileArtifactId, ResearchProfileRef,
        RoleCode, UserId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_promotion_permit")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub promotion_permit_id: PromotionPermitId,
    pub idempotency_key: PolicyIdempotencyKey,
    pub scope_hash: ContentHash,
    pub issuance_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    #[sea_orm(column_type = r#"custom("qp_market_category")"#)]
    pub category: MarketCategory,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    #[sea_orm(column_type = r#"custom("qp_quant_runtime_mode[]")"#)]
    pub allowed_runtime_modes: Vec<QuantRuntimeMode>,
    pub non_route_policy_hash: ContentHash,
    pub serving_constraints_hash: ContentHash,
    pub preflight_hash: ContentHash,
    pub issued_by_user_id: UserId,
    #[sea_orm(column_type = "Text")]
    pub issued_by_username: String,
    pub issued_by_role: RoleCode,
    #[sea_orm(column_type = "Text")]
    pub issuance_reason: String,
    pub expires_at: DateTime<Utc>,
    pub revoked_by_user_id: Option<UserId>,
    #[sea_orm(column_type = "Text", nullable)]
    pub revoked_by_username: Option<String>,
    pub revoked_by_role: Option<RoleCode>,
    #[sea_orm(column_type = "Text", nullable)]
    pub revocation_reason: Option<String>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub issued_at: DateTime<Utc>,
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
        relation_enum = "ExpectedPolicySnapshot",
        from = "expected_decision_policy_snapshot_id",
        to = "decision_policy_snapshot_id"
    )]
    pub expected_policy_snapshot: BelongsTo<decision_policy_snapshot::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "ChampionModelVersion",
        from = "champion_model_version_id",
        to = "model_version_id"
    )]
    pub champion_model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "IssuedByUser",
        from = "issued_by_user_id",
        to = "id"
    )]
    pub issued_by_user: BelongsTo<user::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "RevokedByUser",
        from = "revoked_by_user_id",
        to = "id"
    )]
    pub revoked_by_user: BelongsTo<Option<user::Entity>>,
}

impl ActiveModelBehavior for ActiveModel {}
