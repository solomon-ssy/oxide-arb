//! `quant_model_route_shadow_binding` route-owned slot ledger.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::{
    policy_activation, quant_feedback_cycle, quant_model_candidate_manifest, quant_model_version,
};
use crate::{
    domain::ports::ShadowBindingReceipt,
    enums::quant::ShadowBindingStatus,
    runtime_config::BuyModelRoute,
    types::{
        AuditEventId, ContentHash, FeedbackCycleId, ModelCandidateManifestId, ModelVersionId,
        PolicyActivationId, PolicyBundleGeneration, RoleCode, ShadowBindingArtifactId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_model_route_shadow_binding")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub binding_id: ShadowBindingArtifactId,
    #[sea_orm(unique)]
    pub feedback_cycle_id: FeedbackCycleId,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    pub status: ShadowBindingStatus,
    pub lifecycle_generation: i64,
    pub binding_generation: i64,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_model_version_id: ModelVersionId,
    pub candidate_serving_contract_hash: ContentHash,
    pub candidate_recipe_hash: ContentHash,
    pub candidate_manifest_id: ModelCandidateManifestId,
    pub candidate_manifest_hash: ContentHash,
    pub reserved_model_bytes: i64,
    pub committed_policy_generation: PolicyBundleGeneration,
    #[sea_orm(unique)]
    pub policy_activation_id: PolicyActivationId,
    #[sea_orm(unique)]
    pub audit_event_id: AuditEventId,
    pub receipt_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub receipt: ShadowBindingReceipt,
    pub bound_at: DateTime<Utc>,
    pub terminated_at: Option<DateTime<Utc>>,
    pub termination_policy_activation_id: Option<PolicyActivationId>,
    pub termination_request_hash: Option<ContentHash>,
    pub termination_reason_code: Option<String>,
    pub termination_note: Option<String>,
    pub termination_actor_role: Option<RoleCode>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub created_at: DateTime<Utc>,
    #[sea_orm(default_expr = "Expr::current_timestamp()")]
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "FeedbackCycle",
        from = "feedback_cycle_id",
        to = "feedback_cycle_id"
    )]
    pub feedback_cycle: BelongsTo<quant_feedback_cycle::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "PolicyActivation",
        from = "policy_activation_id",
        to = "policy_activation_id"
    )]
    pub policy_activation: BelongsTo<policy_activation::Entity>,
    #[sea_orm(
        belongs_to,
        relation_enum = "TerminationPolicyActivation",
        from = "termination_policy_activation_id",
        to = "policy_activation_id"
    )]
    pub termination_policy_activation: BelongsTo<Option<policy_activation::Entity>>,
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
}

impl ActiveModelBehavior for ActiveModel {}
