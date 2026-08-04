//! `quant_feedback_cycle` durable orchestration FSM.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::research_profile_artifact;
use crate::{
    domain::quant::FeedbackCycleKey,
    enums::{
        model::ModelFamily,
        quant::{FeedbackCycleStatus, FeedbackDecision, FeedbackEvaluationMode},
    },
    runtime_config::BuyModelRoute,
    types::{
        ContentHash, DecisionPolicySnapshotId, FeedbackCycleId, ModelSpecId, ModelVersionId,
        PolicyBundleGeneration, PolicyIdempotencyKey, ResearchProfileArtifactId,
        ResearchProfileRef, WorkerId,
    },
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_feedback_cycle")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub feedback_cycle_id: FeedbackCycleId,
    pub idempotency_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub idempotency_key: FeedbackCycleKey,
    #[sea_orm(column_type = "JsonBinary")]
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub feedback_policy_hash: ContentHash,
    pub label_cutoff: DateTime<Utc>,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub champion_model_spec_id: ModelSpecId,
    pub champion_model_spec_definition_hash: ContentHash,
    pub champion_model_family: ModelFamily,
    #[sea_orm(column_type = "JsonBinary")]
    pub route: BuyModelRoute,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub route_generation: i64,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub parent_cycle_id: Option<FeedbackCycleId>,
    pub forced_idempotency_key: Option<PolicyIdempotencyKey>,
    pub status: FeedbackCycleStatus,
    pub decision: Option<FeedbackDecision>,
    pub terminal_reason_code: Option<String>,
    pub generation: i64,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub stage_resume_after: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,

    #[sea_orm(
        belongs_to,
        relation_enum = "ResearchProfileArtifact",
        from = "research_profile_artifact_id",
        to = "research_profile_artifact_id"
    )]
    pub research_profile_artifact: BelongsTo<research_profile_artifact::Entity>,
}

impl ActiveModelBehavior for ActiveModel {}
