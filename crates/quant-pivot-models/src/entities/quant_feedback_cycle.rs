//! `quant_feedback_cycle` durable orchestration FSM.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use super::research_profile_artifact;
use crate::{
    domain::{ports::FeedbackCandidateFamily, quant::FeedbackCycleKey},
    enums::quant::{FeedbackCycleStatus, FeedbackDecision},
    types::{
        CapabilityRegistryHashes, ContentHash, FeedbackCycleId, ModelVersionId,
        ResearchProfileArtifactId, ResearchProfileRef, WorkerId,
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
    #[sea_orm(column_type = "JsonBinary")]
    pub capability_registry_hashes: CapabilityRegistryHashes,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    pub candidate_family: FeedbackCandidateFamily,
    pub candidate_family_hash: ContentHash,
    pub status: FeedbackCycleStatus,
    pub decision: Option<FeedbackDecision>,
    pub terminal_reason_code: Option<String>,
    pub generation: i64,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
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
