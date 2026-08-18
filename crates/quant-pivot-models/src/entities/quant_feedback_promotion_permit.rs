//! `quant_feedback_promotion_permit` governed lifecycle entity.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    decision_policy_snapshot, quant_feedback_cycle, quant_model_candidate_manifest,
    quant_model_version, research_profile_artifact, user,
};
use crate::{
    enums::{common::MarketCategory, quant::ExecutionAuthorityCeiling},
    types::{
        ContentHash, DecisionPolicySnapshotId, FeedbackCycleId, ModelCandidateManifestId,
        ModelVersionId, PolicyBundleGeneration, PolicyIdempotencyKey, PromotionPermitId,
        ResearchProfileArtifactId, ResearchProfileRef, RoleCode, UserId,
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
    pub feedback_cycle_id: FeedbackCycleId,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    #[sea_orm(column_type = r#"custom("qp_market_category")"#)]
    pub category: MarketCategory,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_runtime_control_revision: i64,
    pub expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub expected_route_generation: i64,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub promotion_gate_hash: ContentHash,
    pub maximum_execution_authority: ExecutionAuthorityCeiling,
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
        relation_enum = "FeedbackCycle",
        from = "feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
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
        relation_enum = "CandidateModelVersion",
        from = "candidate_model_version_id",
        to = "model_version_id"
    )]
    pub candidate_model_version: BelongsTo<quant_model_version::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "CandidateManifest",
        from = "candidate_manifest_id",
        to = "manifest_id"
    )]
    pub candidate_manifest: BelongsTo<quant_model_candidate_manifest::Entity>,
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
