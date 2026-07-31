//! Durable feedback-cycle and immutable evidence persistence contracts.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use rust_decimal::Decimal;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};

use super::feedback_trigger::FeedbackTriggerEventInfo;
use crate::{
    domain::ports::FeedbackCandidateFamily,
    entities::{
        quant_drift_report, quant_feedback_cycle, quant_feedback_evaluation_use,
        quant_feedback_stage_event,
    },
    enums::quant::{
        DatasetPurpose, FeedbackCycleStatus, FeedbackDecision, FeedbackDriftAssessment,
        FeedbackDriftKind, FeedbackDriftMetric, FeedbackEvaluationPurpose, FeedbackStage,
        FeedbackStageEventKind, FeedbackTriggerFamily,
    },
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, CapabilityRegistryHashes, ContentHash, DriftReportId, FeedbackCycleId,
        FeedbackEvaluationUseId, FeedbackStageEventId, ModelVersionId, ResearchJobId,
        ResearchProfileArtifactId, ResearchProfileId, ResearchProfileRef, RoleCode,
        TrainingDatasetId, UserId, WorkerId,
    },
};

const FEEDBACK_CYCLE_KEY_VERSION: u32 = 1;
const FEEDBACK_CYCLE_KEY_DOMAIN: &str = "quant-pivot/feedback-cycle-idempotency";
const FEEDBACK_STAGE_EVENT_VERSION: u32 = 2;
const FEEDBACK_STAGE_EVENT_DOMAIN: &str = "quant-pivot/feedback-stage-event";
const DRIFT_REPORT_VERSION: u32 = 1;
const DRIFT_REPORT_DOMAIN: &str = "quant-pivot/drift-report";
const EVALUATION_USE_VERSION: u32 = 1;
const EVALUATION_SEMANTIC_DOMAIN: &str = "quant-pivot/feedback-evaluation-semantic-use";
const EVALUATION_USE_DOMAIN: &str = "quant-pivot/feedback-evaluation-use";
const MAX_ARTIFACT_URI_BYTES: usize = 4_096;
const MAX_ACTOR_BYTES: usize = 256;
const MAX_REASON_BYTES: usize = 128;
const MAX_ROLE_BYTES: usize = 64;

/// Complete immutable preimage of one cycle's typed idempotency hash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields, try_from = "FeedbackCycleKeyDocument")]
pub struct FeedbackCycleKey {
    format_version: u32,
    profile_ref: ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    label_cutoff: DateTime<Utc>,
    capability_registry_hashes: CapabilityRegistryHashes,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_family: FeedbackCandidateFamily,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FeedbackCycleKeyDocument {
    format_version: u32,
    profile_ref: ResearchProfileRef,
    feedback_policy_hash: ContentHash,
    label_cutoff: DateTime<Utc>,
    capability_registry_hashes: CapabilityRegistryHashes,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_family: FeedbackCandidateFamily,
}

/// Inputs frozen by the server before a cycle row is claimed.
#[derive(Debug, Clone)]
pub struct FeedbackCycleKeyInput {
    pub profile_ref: ResearchProfileRef,
    pub feedback_policy_hash: ContentHash,
    pub label_cutoff: DateTime<Utc>,
    pub capability_registry_hashes: CapabilityRegistryHashes,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family: FeedbackCandidateFamily,
}

impl FeedbackCycleKey {
    /// Validate and freeze every semantic input used for cycle deduplication.
    pub fn try_new(input: FeedbackCycleKeyInput) -> Result<Self, FeedbackError> {
        let key = Self {
            format_version: FEEDBACK_CYCLE_KEY_VERSION,
            profile_ref: input.profile_ref,
            feedback_policy_hash: input.feedback_policy_hash,
            label_cutoff: input.label_cutoff,
            capability_registry_hashes: input.capability_registry_hashes,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_family: input.candidate_family,
        };
        key.validate()?;
        Ok(key)
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.format_version != FEEDBACK_CYCLE_KEY_VERSION {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: format!(
                    "unsupported idempotency-key version {}; expected {}",
                    self.format_version, FEEDBACK_CYCLE_KEY_VERSION
                ),
            });
        }
        self.profile_ref
            .validate()
            .map_err(|error| FeedbackError::InvalidCycleIdentity {
                detail: format!("research profile reference is invalid: {error}"),
            })?;
        CapabilityRegistryHashes::try_new(self.capability_registry_hashes.as_slice().to_vec())
            .map_err(|error| FeedbackError::InvalidCycleIdentity {
                detail: format!("capability registry hashes are not canonical: {error}"),
            })?;
        self.candidate_family
            .validate()
            .map_err(|error| FeedbackError::InvalidCycleIdentity {
                detail: format!("candidate family is invalid: {error}"),
            })?;
        let evaluation = self.candidate_family.shared_evaluation();
        if evaluation.window.profile_ref() != &self.profile_ref
            || evaluation.source_lineage.pit_cutoff != self.label_cutoff
            || evaluation.source_lineage.capability_registry_hashes
                != self.capability_registry_hashes
        {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: "candidate family profile, label cutoff, or capability set differs from the cycle"
                    .to_owned(),
            });
        }
        Ok(())
    }

    pub fn idempotency_hash(&self) -> Result<ContentHash, FeedbackError> {
        self.validate()?;
        CanonicalDigest::content_hash_typed(
            FEEDBACK_CYCLE_KEY_DOMAIN,
            FEEDBACK_CYCLE_KEY_VERSION,
            self,
        )
        .map_err(Into::into)
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn feedback_policy_hash(&self) -> ContentHash {
        self.feedback_policy_hash
    }

    #[must_use]
    pub const fn label_cutoff(&self) -> DateTime<Utc> {
        self.label_cutoff
    }

    #[must_use]
    pub const fn capability_registry_hashes(&self) -> &CapabilityRegistryHashes {
        &self.capability_registry_hashes
    }

    #[must_use]
    pub const fn champion_model_version_id(&self) -> ModelVersionId {
        self.champion_model_version_id
    }

    #[must_use]
    pub const fn champion_serving_contract_hash(&self) -> ContentHash {
        self.champion_serving_contract_hash
    }

    #[must_use]
    pub const fn candidate_family_hash(&self) -> ContentHash {
        self.candidate_family.candidate_family_hash()
    }

    #[must_use]
    pub const fn candidate_family(&self) -> &FeedbackCandidateFamily {
        &self.candidate_family
    }
}

impl TryFrom<FeedbackCycleKeyDocument> for FeedbackCycleKey {
    type Error = FeedbackError;

    fn try_from(document: FeedbackCycleKeyDocument) -> Result<Self, Self::Error> {
        let key = Self {
            format_version: document.format_version,
            profile_ref: document.profile_ref,
            feedback_policy_hash: document.feedback_policy_hash,
            label_cutoff: document.label_cutoff,
            capability_registry_hashes: document.capability_registry_hashes,
            champion_model_version_id: document.champion_model_version_id,
            champion_serving_contract_hash: document.champion_serving_contract_hash,
            candidate_family: document.candidate_family,
        };
        key.validate()?;
        Ok(key)
    }
}

/// Insert payload for one deduplicated feedback-cycle claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feedback_cycle::ActiveModel")]
pub struct NewFeedbackCycle {
    feedback_cycle_id: FeedbackCycleId,
    idempotency_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    idempotency_key: FeedbackCycleKey,
    #[sea_orm(column_type = "JsonBinary")]
    profile_ref: ResearchProfileRef,
    research_profile_artifact_id: ResearchProfileArtifactId,
    profile_hash: ContentHash,
    feedback_policy_hash: ContentHash,
    label_cutoff: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    capability_registry_hashes: CapabilityRegistryHashes,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    #[sea_orm(column_type = "JsonBinary")]
    candidate_family: FeedbackCandidateFamily,
    candidate_family_hash: ContentHash,
}

impl NewFeedbackCycle {
    pub fn try_seal(idempotency_key: FeedbackCycleKey) -> Result<Self, FeedbackError> {
        let idempotency_hash = idempotency_key.idempotency_hash()?;
        let profile_ref = idempotency_key.profile_ref().clone();
        let feedback_policy_hash = idempotency_key.feedback_policy_hash();
        let label_cutoff = idempotency_key.label_cutoff();
        let capability_registry_hashes = idempotency_key.capability_registry_hashes().clone();
        let champion_model_version_id = idempotency_key.champion_model_version_id();
        let champion_serving_contract_hash = idempotency_key.champion_serving_contract_hash();
        let candidate_family = idempotency_key.candidate_family().clone();
        let candidate_family_hash = idempotency_key.candidate_family_hash();
        Ok(Self {
            feedback_cycle_id: FeedbackCycleId::from_idempotency_hash(&idempotency_hash),
            idempotency_hash,
            idempotency_key,
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(&profile_ref),
            profile_hash: profile_ref.content_hash,
            profile_ref,
            feedback_policy_hash,
            label_cutoff,
            capability_registry_hashes,
            champion_model_version_id,
            champion_serving_contract_hash,
            candidate_family,
            candidate_family_hash,
        })
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn idempotency_hash(&self) -> ContentHash {
        self.idempotency_hash
    }

    #[must_use]
    pub const fn label_cutoff(&self) -> DateTime<Utc> {
        self.label_cutoff
    }

    #[must_use]
    pub const fn profile_ref(&self) -> &ResearchProfileRef {
        &self.profile_ref
    }

    #[must_use]
    pub const fn research_profile_artifact_id(&self) -> &ResearchProfileArtifactId {
        &self.research_profile_artifact_id
    }

    #[must_use]
    pub const fn feedback_policy_hash(&self) -> ContentHash {
        self.feedback_policy_hash
    }
}

/// Authenticated principal and explicit role for a governed cycle mutation.
///
/// Username is deliberately absent. The transaction-owning repository resolves
/// the active database username after locking the authorization preimage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackCycleActor {
    pub user_id: UserId,
    pub acting_role: RoleCode,
}

impl FeedbackCycleActor {
    fn validate(&self) -> Result<(), FeedbackError> {
        let role = self.acting_role.as_str();
        if role.is_empty()
            || role.len() > MAX_ROLE_BYTES
            || !role
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(FeedbackError::InvalidStageEvent {
                detail: "acting role violates the governed feedback contract".to_owned(),
            });
        }
        Ok(())
    }
}

/// Server-frozen cycle plus the operator intent recorded as trigger evidence.
#[derive(Debug, Clone)]
pub struct GovernedFeedbackTrigger {
    pub actor: FeedbackCycleActor,
    pub cycle: NewFeedbackCycle,
    pub reason_code: String,
}

impl GovernedFeedbackTrigger {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.actor.validate()?;
        if !valid_reason(&self.reason_code) {
            return Err(FeedbackError::InvalidStageEvent {
                detail: "trigger reason code violates the governed feedback contract".to_owned(),
            });
        }
        Ok(())
    }
}

/// Exact timeline precondition for one governed cancellation request.
#[derive(Debug, Clone)]
pub struct GovernedFeedbackCancellation {
    pub actor: FeedbackCycleActor,
    pub feedback_cycle_id: FeedbackCycleId,
    pub expected_generation: i64,
    pub expected_event_sequence: i64,
    pub stage: FeedbackStage,
    pub reason_code: String,
}

impl GovernedFeedbackCancellation {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.actor.validate()?;
        if self.expected_generation < 0
            || self.expected_event_sequence <= 1
            || self.stage == FeedbackStage::Trigger
            || !valid_reason(&self.reason_code)
        {
            return Err(FeedbackError::InvalidStageEvent {
                detail:
                    "cancellation generation, sequence, stage, or reason violates the governed feedback contract"
                        .to_owned(),
            });
        }
        Ok(())
    }
}

/// Valid terminal projection for a feedback-cycle CAS.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FeedbackCycleTerminal {
    Succeeded {
        decision: FeedbackDecision,
        reason_code: String,
    },
    Failed {
        reason_code: String,
    },
    Cancelled {
        reason_code: String,
    },
}

impl FeedbackCycleTerminal {
    pub fn try_succeeded(
        decision: FeedbackDecision,
        reason_code: String,
    ) -> Result<Self, FeedbackError> {
        Self::validate_reason(&reason_code)?;
        Ok(Self::Succeeded {
            decision,
            reason_code,
        })
    }

    pub fn try_failed(reason_code: String) -> Result<Self, FeedbackError> {
        Self::validate_reason(&reason_code)?;
        Ok(Self::Failed { reason_code })
    }

    pub fn try_cancelled(reason_code: String) -> Result<Self, FeedbackError> {
        Self::validate_reason(&reason_code)?;
        Ok(Self::Cancelled { reason_code })
    }

    #[must_use]
    pub const fn status(&self) -> FeedbackCycleStatus {
        match self {
            Self::Succeeded { .. } => FeedbackCycleStatus::Succeeded,
            Self::Failed { .. } => FeedbackCycleStatus::Failed,
            Self::Cancelled { .. } => FeedbackCycleStatus::Cancelled,
        }
    }

    #[must_use]
    pub const fn decision(&self) -> Option<FeedbackDecision> {
        match self {
            Self::Succeeded { decision, .. } => Some(*decision),
            Self::Failed { .. } | Self::Cancelled { .. } => None,
        }
    }

    #[must_use]
    pub fn reason_code(&self) -> &str {
        match self {
            Self::Succeeded { reason_code, .. }
            | Self::Failed { reason_code }
            | Self::Cancelled { reason_code } => reason_code,
        }
    }

    fn validate_reason(reason_code: &str) -> Result<(), FeedbackError> {
        if valid_reason(reason_code) {
            Ok(())
        } else {
            Err(FeedbackError::InvalidCycleState {
                detail: "terminal reason code is empty or invalid".to_owned(),
            })
        }
    }
}

/// Full durable projection of a feedback-cycle FSM row.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feedback_cycle::Entity")]
pub struct FeedbackCycleInfo {
    pub feedback_cycle_id: FeedbackCycleId,
    pub idempotency_hash: ContentHash,
    pub idempotency_key: FeedbackCycleKey,
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub feedback_policy_hash: ContentHash,
    pub label_cutoff: DateTime<Utc>,
    pub capability_registry_hashes: CapabilityRegistryHashes,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
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
}

info_from_model!(
    FeedbackCycleInfo,
    quant_feedback_cycle::Model,
    {
        feedback_cycle_id,
        idempotency_hash,
        idempotency_key,
        profile_ref,
        research_profile_artifact_id,
        profile_hash,
        feedback_policy_hash,
        label_cutoff,
        capability_registry_hashes,
        champion_model_version_id,
        champion_serving_contract_hash,
        candidate_family,
        candidate_family_hash,
        status,
        decision,
        terminal_reason_code,
        generation,
        lease_owner,
        lease_expires_at,
        cancel_requested_at,
        started_at,
        completed_at,
        created_at,
        updated_at,
    }
);

impl FeedbackCycleInfo {
    /// Whether an idempotent retry carries the exact frozen cycle identity.
    #[must_use]
    pub fn has_same_identity(&self, candidate: &NewFeedbackCycle) -> bool {
        self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.idempotency_hash == candidate.idempotency_hash
            && self.idempotency_key == candidate.idempotency_key
            && self.profile_ref == candidate.profile_ref
            && self.research_profile_artifact_id == candidate.research_profile_artifact_id
            && self.profile_hash == candidate.profile_hash
            && self.feedback_policy_hash == candidate.feedback_policy_hash
            && self.label_cutoff == candidate.label_cutoff
            && self.capability_registry_hashes == candidate.capability_registry_hashes
            && self.champion_model_version_id == candidate.champion_model_version_id
            && self.champion_serving_contract_hash == candidate.champion_serving_contract_hash
            && self.candidate_family == candidate.candidate_family
            && self.candidate_family_hash == candidate.candidate_family_hash
    }

    #[must_use]
    pub fn accepts_drift(&self, candidate: &NewDriftReport) -> bool {
        self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.label_cutoff == candidate.label_cutoff
    }

    #[must_use]
    pub fn accepts_evaluation(&self, candidate: &NewFeedbackEvaluationUse) -> bool {
        self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.profile_ref == candidate.profile_ref
            && self.research_profile_artifact_id == candidate.research_profile_artifact_id
            && self.label_cutoff == candidate.label_cutoff
            && self.champion_model_version_id == candidate.champion_model_version_id
            && self.champion_serving_contract_hash == candidate.champion_serving_contract_hash
            && self.candidate_family_hash == candidate.candidate_family_hash
    }

    /// Revalidate every immutable projection plus the lifecycle/decision shape.
    pub fn validate(&self) -> Result<(), FeedbackError> {
        self.idempotency_key.validate()?;
        let expected_hash = self.idempotency_key.idempotency_hash()?;
        let expected_profile_id =
            ResearchProfileArtifactId::from_profile_ref(self.idempotency_key.profile_ref());
        if self.idempotency_hash != expected_hash
            || self.feedback_cycle_id != FeedbackCycleId::from_idempotency_hash(&expected_hash)
            || self.profile_ref != *self.idempotency_key.profile_ref()
            || self.research_profile_artifact_id != expected_profile_id
            || self.profile_hash != self.profile_ref.content_hash
            || self.feedback_policy_hash != self.idempotency_key.feedback_policy_hash()
            || self.label_cutoff != self.idempotency_key.label_cutoff()
            || self.capability_registry_hashes != *self.idempotency_key.capability_registry_hashes()
            || self.champion_model_version_id != self.idempotency_key.champion_model_version_id()
            || self.champion_serving_contract_hash
                != self.idempotency_key.champion_serving_contract_hash()
            || self.candidate_family != *self.idempotency_key.candidate_family()
            || self.candidate_family_hash != self.idempotency_key.candidate_family_hash()
        {
            return Err(FeedbackError::InvalidCycleIdentity {
                detail: "persisted frozen projections do not match the typed key".to_owned(),
            });
        }
        self.validate_state()
    }

    fn validate_state(&self) -> Result<(), FeedbackError> {
        if self.generation < 0
            || self.label_cutoff > self.created_at
            || self.updated_at < self.created_at
            || self
                .cancel_requested_at
                .is_some_and(|requested_at| requested_at < self.created_at)
            || self
                .completed_at
                .is_some_and(|completed_at| completed_at < self.created_at)
            || self
                .started_at
                .zip(self.completed_at)
                .is_some_and(|(started_at, completed_at)| completed_at < started_at)
            || self
                .lease_owner
                .is_some()
                .ne(&self.lease_expires_at.is_some())
            || self
                .terminal_reason_code
                .as_deref()
                .is_some_and(|reason| !valid_reason(reason))
        {
            return Err(FeedbackError::InvalidCycleState {
                detail: "invalid generation, timestamp, lease, or reason projection".to_owned(),
            });
        }
        let valid = match self.status {
            FeedbackCycleStatus::Queued => {
                self.decision.is_none()
                    && self.terminal_reason_code.is_none()
                    && self.started_at.is_none()
                    && self.completed_at.is_none()
                    && self.lease_owner.is_none()
            }
            FeedbackCycleStatus::Running => {
                self.decision.is_none()
                    && self.terminal_reason_code.is_none()
                    && self.started_at.is_some()
                    && self.completed_at.is_none()
                    && self.lease_owner.is_some()
            }
            FeedbackCycleStatus::Succeeded => {
                self.decision.is_some()
                    && self.terminal_reason_code.is_some()
                    && self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self.lease_owner.is_none()
            }
            FeedbackCycleStatus::Failed => {
                self.decision.is_none()
                    && self.terminal_reason_code.is_some()
                    && self.started_at.is_some()
                    && self.completed_at.is_some()
                    && self.lease_owner.is_none()
            }
            FeedbackCycleStatus::Cancelled => {
                self.decision.is_none()
                    && self.terminal_reason_code.is_some()
                    && self.completed_at.is_some()
                    && self.lease_owner.is_none()
            }
        };
        if valid {
            Ok(())
        } else {
            Err(FeedbackError::InvalidCycleState {
                detail: format!(
                    "status {} is inconsistent with decision, lease, or timestamps",
                    self.status
                ),
            })
        }
    }
}

/// Immutable input for one append-only stage timeline event.
#[derive(Debug, Clone)]
pub struct FeedbackStageEventInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub event_sequence: i64,
    pub stage: FeedbackStage,
    pub event_kind: FeedbackStageEventKind,
    pub trigger_family: Option<FeedbackTriggerFamily>,
    pub research_job_id: Option<ResearchJobId>,
    pub actor: Option<String>,
    pub reason_code: Option<String>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub occurred_at: DateTime<Utc>,
}

/// Append-only feedback stage event sealed by a complete content hash.
#[derive(Debug, Clone, Serialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feedback_stage_event::ActiveModel")]
pub struct NewFeedbackStageEvent {
    feedback_stage_event_id: FeedbackStageEventId,
    feedback_cycle_id: FeedbackCycleId,
    event_sequence: i64,
    stage: FeedbackStage,
    event_kind: FeedbackStageEventKind,
    trigger_family: Option<FeedbackTriggerFamily>,
    research_job_id: Option<ResearchJobId>,
    actor: Option<String>,
    reason_code: Option<String>,
    evidence_uri: Option<ArtifactUri>,
    evidence_hash: Option<ContentHash>,
    occurred_at: DateTime<Utc>,
    event_hash: ContentHash,
}

impl NewFeedbackStageEvent {
    pub fn try_seal(input: FeedbackStageEventInput) -> Result<Self, FeedbackError> {
        input.validate()?;
        let event_hash = CanonicalDigest::content_hash_typed(
            FEEDBACK_STAGE_EVENT_DOMAIN,
            FEEDBACK_STAGE_EVENT_VERSION,
            &StageEventDocument::from(&input),
        )?;
        Ok(Self {
            feedback_stage_event_id: FeedbackStageEventId::from_event_hash(&event_hash),
            feedback_cycle_id: input.feedback_cycle_id,
            event_sequence: input.event_sequence,
            stage: input.stage,
            event_kind: input.event_kind,
            trigger_family: input.trigger_family,
            research_job_id: input.research_job_id,
            actor: input.actor,
            reason_code: input.reason_code,
            evidence_uri: input.evidence_uri,
            evidence_hash: input.evidence_hash,
            occurred_at: input.occurred_at,
            event_hash,
        })
    }

    #[must_use]
    pub const fn feedback_stage_event_id(&self) -> FeedbackStageEventId {
        self.feedback_stage_event_id
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn event_sequence(&self) -> i64 {
        self.event_sequence
    }

    #[must_use]
    pub const fn event_kind(&self) -> FeedbackStageEventKind {
        self.event_kind
    }

    #[must_use]
    pub const fn trigger_family(&self) -> Option<FeedbackTriggerFamily> {
        self.trigger_family
    }

    #[must_use]
    pub fn actor(&self) -> Option<&str> {
        self.actor.as_deref()
    }

    #[must_use]
    pub fn reason_code(&self) -> Option<&str> {
        self.reason_code.as_deref()
    }

    pub fn cancellation_reason(&self) -> Result<&str, FeedbackError> {
        if self.event_kind != FeedbackStageEventKind::CancellationRequested {
            return Err(FeedbackError::InvalidStageEvent {
                detail: "event is not a cancellation request".to_owned(),
            });
        }
        self.reason_code
            .as_deref()
            .ok_or_else(|| FeedbackError::InvalidStageEvent {
                detail: "cancellation event has no reason code".to_owned(),
            })
    }

    #[must_use]
    pub const fn occurred_at(&self) -> DateTime<Utc> {
        self.occurred_at
    }
}

#[derive(Debug, Serialize)]
struct StageEventDocument<'a> {
    feedback_cycle_id: FeedbackCycleId,
    event_sequence: i64,
    stage: FeedbackStage,
    event_kind: FeedbackStageEventKind,
    trigger_family: Option<FeedbackTriggerFamily>,
    research_job_id: Option<ResearchJobId>,
    actor: &'a Option<String>,
    reason_code: &'a Option<String>,
    evidence_uri: &'a Option<ArtifactUri>,
    evidence_hash: Option<ContentHash>,
    occurred_at: DateTime<Utc>,
}

impl<'a> From<&'a FeedbackStageEventInput> for StageEventDocument<'a> {
    fn from(input: &'a FeedbackStageEventInput) -> Self {
        Self {
            feedback_cycle_id: input.feedback_cycle_id,
            event_sequence: input.event_sequence,
            stage: input.stage,
            event_kind: input.event_kind,
            trigger_family: input.trigger_family,
            research_job_id: input.research_job_id,
            actor: &input.actor,
            reason_code: &input.reason_code,
            evidence_uri: &input.evidence_uri,
            evidence_hash: input.evidence_hash,
            occurred_at: input.occurred_at,
        }
    }
}

/// Full read projection for an append-only stage event.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feedback_stage_event::Entity")]
pub struct FeedbackStageEventInfo {
    pub feedback_stage_event_id: FeedbackStageEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub event_sequence: i64,
    pub stage: FeedbackStage,
    pub event_kind: FeedbackStageEventKind,
    pub trigger_family: Option<FeedbackTriggerFamily>,
    pub research_job_id: Option<ResearchJobId>,
    pub actor: Option<String>,
    pub reason_code: Option<String>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub occurred_at: DateTime<Utc>,
    pub event_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

/// Immutable source of one globally ordered feedback invalidation.
#[derive(Debug, Clone)]
pub enum FeedbackOutboxSource {
    Stage(FeedbackStageEventInfo),
    Trigger(FeedbackTriggerEventInfo),
}

impl FeedbackOutboxSource {
    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        match self {
            Self::Stage(event) => event.feedback_cycle_id,
            Self::Trigger(event) => event.feedback_cycle_id,
        }
    }

    #[must_use]
    pub const fn occurred_at(&self) -> DateTime<Utc> {
        match self {
            Self::Stage(event) => event.occurred_at,
            Self::Trigger(event) => event.occurred_at,
        }
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        match self {
            Self::Stage(event) => event.validate(),
            Self::Trigger(event) => event.validate(),
        }
    }
}

/// One globally ordered feedback event claimed for durable publication.
#[derive(Debug, Clone)]
pub struct FeedbackOutboxEntry {
    pub revision: i64,
    pub publish_attempts: i32,
    pub profile_id: ResearchProfileId,
    pub source: FeedbackOutboxSource,
}

impl FeedbackOutboxEntry {
    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.revision <= 0 || self.publish_attempts < 0 {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback outbox revision and publish attempts must be non-negative"
                    .to_owned(),
            });
        }
        if self.profile_id.as_str().trim().is_empty() {
            return Err(FeedbackError::InvalidCoordinatorState {
                detail: "feedback outbox profile id must not be empty".to_owned(),
            });
        }
        self.source.validate()
    }
}

/// Bounded scheduler backlog snapshot from the authoritative database clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedbackQueueSnapshot {
    pub queued: u64,
    pub running: u64,
    pub pending_outbox: u64,
    pub oldest_queued_at: Option<DateTime<Utc>>,
    pub oldest_running_at: Option<DateTime<Utc>>,
}

info_from_model!(
    FeedbackStageEventInfo,
    quant_feedback_stage_event::Model,
    {
        feedback_stage_event_id,
        feedback_cycle_id,
        event_sequence,
        stage,
        event_kind,
        trigger_family,
        research_job_id,
        actor,
        reason_code,
        evidence_uri,
        evidence_hash,
        occurred_at,
        event_hash,
        created_at,
    }
);

impl FeedbackStageEventInfo {
    /// Whether an idempotent append carries the exact immutable event.
    #[must_use]
    pub fn has_same_content(&self, candidate: &NewFeedbackStageEvent) -> bool {
        self.feedback_stage_event_id == candidate.feedback_stage_event_id
            && self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.event_sequence == candidate.event_sequence
            && self.stage == candidate.stage
            && self.event_kind == candidate.event_kind
            && self.trigger_family == candidate.trigger_family
            && self.research_job_id == candidate.research_job_id
            && self.actor == candidate.actor
            && self.reason_code == candidate.reason_code
            && self.evidence_uri == candidate.evidence_uri
            && self.evidence_hash == candidate.evidence_hash
            && self.occurred_at == candidate.occurred_at
            && self.event_hash == candidate.event_hash
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let input = FeedbackStageEventInput {
            feedback_cycle_id: self.feedback_cycle_id,
            event_sequence: self.event_sequence,
            stage: self.stage,
            event_kind: self.event_kind,
            trigger_family: self.trigger_family,
            research_job_id: self.research_job_id,
            actor: self.actor.clone(),
            reason_code: self.reason_code.clone(),
            evidence_uri: self.evidence_uri.clone(),
            evidence_hash: self.evidence_hash,
            occurred_at: self.occurred_at,
        };
        input.validate()?;
        let expected_hash = CanonicalDigest::content_hash_typed(
            FEEDBACK_STAGE_EVENT_DOMAIN,
            FEEDBACK_STAGE_EVENT_VERSION,
            &StageEventDocument::from(&input),
        )?;
        if self.created_at < self.occurred_at
            || self.event_hash != expected_hash
            || self.feedback_stage_event_id != FeedbackStageEventId::from_event_hash(&expected_hash)
        {
            return Err(FeedbackError::InvalidStageEvent {
                detail: "event timeline, hash, or content-addressed id mismatch".to_owned(),
            });
        }
        Ok(())
    }
}

/// Complete immutable input for one typed drift summary.
#[derive(Debug, Clone)]
pub struct DriftReportInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub kind: FeedbackDriftKind,
    pub metric: FeedbackDriftMetric,
    pub assessment: FeedbackDriftAssessment,
    pub baseline_window_start: DateTime<Utc>,
    pub baseline_window_end: DateTime<Utc>,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub observed_value: Option<Decimal>,
    pub threshold: Decimal,
    pub sample_count: i64,
    pub detail_uri: ArtifactUri,
    pub detail_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
}

/// Content-addressed append-only drift report header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_drift_report::ActiveModel")]
pub struct NewDriftReport {
    drift_report_id: DriftReportId,
    feedback_cycle_id: FeedbackCycleId,
    kind: FeedbackDriftKind,
    metric: FeedbackDriftMetric,
    assessment: FeedbackDriftAssessment,
    baseline_window_start: DateTime<Utc>,
    baseline_window_end: DateTime<Utc>,
    evaluation_window_start: DateTime<Utc>,
    evaluation_window_end: DateTime<Utc>,
    label_cutoff: DateTime<Utc>,
    observed_value: Option<Decimal>,
    threshold: Decimal,
    sample_count: i64,
    detail_uri: ArtifactUri,
    detail_hash: ContentHash,
    observed_at: DateTime<Utc>,
    report_hash: ContentHash,
}

impl NewDriftReport {
    pub fn try_seal(input: DriftReportInput) -> Result<Self, FeedbackError> {
        input.validate()?;
        let report_hash = CanonicalDigest::content_hash_typed(
            DRIFT_REPORT_DOMAIN,
            DRIFT_REPORT_VERSION,
            &DriftReportDocument::from(&input),
        )?;
        Ok(Self {
            drift_report_id: DriftReportId::from_report_hash(&report_hash),
            feedback_cycle_id: input.feedback_cycle_id,
            kind: input.kind,
            metric: input.metric,
            assessment: input.assessment,
            baseline_window_start: input.baseline_window_start,
            baseline_window_end: input.baseline_window_end,
            evaluation_window_start: input.evaluation_window_start,
            evaluation_window_end: input.evaluation_window_end,
            label_cutoff: input.label_cutoff,
            observed_value: input.observed_value.map(|value| value.normalize()),
            threshold: input.threshold.normalize(),
            sample_count: input.sample_count,
            detail_uri: input.detail_uri,
            detail_hash: input.detail_hash,
            observed_at: input.observed_at,
            report_hash,
        })
    }

    #[must_use]
    pub const fn drift_report_id(&self) -> DriftReportId {
        self.drift_report_id
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn metric(&self) -> FeedbackDriftMetric {
        self.metric
    }

    #[must_use]
    pub const fn observed_at(&self) -> DateTime<Utc> {
        self.observed_at
    }
}

#[derive(Debug, Serialize)]
struct DriftReportDocument<'a> {
    feedback_cycle_id: FeedbackCycleId,
    kind: FeedbackDriftKind,
    metric: FeedbackDriftMetric,
    assessment: FeedbackDriftAssessment,
    baseline_window_start: DateTime<Utc>,
    baseline_window_end: DateTime<Utc>,
    evaluation_window_start: DateTime<Utc>,
    evaluation_window_end: DateTime<Utc>,
    label_cutoff: DateTime<Utc>,
    observed_value: Option<Decimal>,
    threshold: Decimal,
    sample_count: i64,
    detail_uri: &'a ArtifactUri,
    detail_hash: ContentHash,
    observed_at: DateTime<Utc>,
}

impl<'a> From<&'a DriftReportInput> for DriftReportDocument<'a> {
    fn from(input: &'a DriftReportInput) -> Self {
        Self {
            feedback_cycle_id: input.feedback_cycle_id,
            kind: input.kind,
            metric: input.metric,
            assessment: input.assessment,
            baseline_window_start: input.baseline_window_start,
            baseline_window_end: input.baseline_window_end,
            evaluation_window_start: input.evaluation_window_start,
            evaluation_window_end: input.evaluation_window_end,
            label_cutoff: input.label_cutoff,
            observed_value: input.observed_value.map(|value| value.normalize()),
            threshold: input.threshold.normalize(),
            sample_count: input.sample_count,
            detail_uri: &input.detail_uri,
            detail_hash: input.detail_hash,
            observed_at: input.observed_at,
        }
    }
}

/// Full read projection of one immutable drift header.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_drift_report::Entity")]
pub struct DriftReportInfo {
    pub drift_report_id: DriftReportId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub kind: FeedbackDriftKind,
    pub metric: FeedbackDriftMetric,
    pub assessment: FeedbackDriftAssessment,
    pub baseline_window_start: DateTime<Utc>,
    pub baseline_window_end: DateTime<Utc>,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub observed_value: Option<Decimal>,
    pub threshold: Decimal,
    pub sample_count: i64,
    pub detail_uri: ArtifactUri,
    pub detail_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    DriftReportInfo,
    quant_drift_report::Model,
    {
        drift_report_id,
        feedback_cycle_id,
        kind,
        metric,
        assessment,
        baseline_window_start,
        baseline_window_end,
        evaluation_window_start,
        evaluation_window_end,
        label_cutoff,
        observed_value,
        threshold,
        sample_count,
        detail_uri,
        detail_hash,
        observed_at,
        report_hash,
        created_at,
    }
);

impl DriftReportInfo {
    /// Whether an idempotent append carries the exact immutable report.
    #[must_use]
    pub fn has_same_content(&self, candidate: &NewDriftReport) -> bool {
        self.drift_report_id == candidate.drift_report_id
            && self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.kind == candidate.kind
            && self.metric == candidate.metric
            && self.assessment == candidate.assessment
            && self.baseline_window_start == candidate.baseline_window_start
            && self.baseline_window_end == candidate.baseline_window_end
            && self.evaluation_window_start == candidate.evaluation_window_start
            && self.evaluation_window_end == candidate.evaluation_window_end
            && self.label_cutoff == candidate.label_cutoff
            && self.observed_value == candidate.observed_value
            && self.threshold == candidate.threshold
            && self.sample_count == candidate.sample_count
            && self.detail_uri == candidate.detail_uri
            && self.detail_hash == candidate.detail_hash
            && self.observed_at == candidate.observed_at
            && self.report_hash == candidate.report_hash
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        let input = DriftReportInput {
            feedback_cycle_id: self.feedback_cycle_id,
            kind: self.kind,
            metric: self.metric,
            assessment: self.assessment,
            baseline_window_start: self.baseline_window_start,
            baseline_window_end: self.baseline_window_end,
            evaluation_window_start: self.evaluation_window_start,
            evaluation_window_end: self.evaluation_window_end,
            label_cutoff: self.label_cutoff,
            observed_value: self.observed_value,
            threshold: self.threshold,
            sample_count: self.sample_count,
            detail_uri: self.detail_uri.clone(),
            detail_hash: self.detail_hash,
            observed_at: self.observed_at,
        };
        input.validate()?;
        let expected_hash = CanonicalDigest::content_hash_typed(
            DRIFT_REPORT_DOMAIN,
            DRIFT_REPORT_VERSION,
            &DriftReportDocument::from(&input),
        )?;
        if self.created_at < self.observed_at
            || self.report_hash != expected_hash
            || self.drift_report_id != DriftReportId::from_report_hash(&expected_hash)
        {
            return Err(FeedbackError::InvalidDriftReport {
                detail: "report timeline, hash, or content-addressed id mismatch".to_owned(),
            });
        }
        Ok(())
    }
}

/// Semantic identity of an unseen evaluation holdout use.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackEvaluationUseKey {
    format_version: u32,
    purpose: FeedbackEvaluationPurpose,
    profile_ref: ResearchProfileRef,
    evaluation_dataset_id: TrainingDatasetId,
    evaluation_dataset_hash: ContentHash,
    evaluation_artifact_bytes_hash: ContentHash,
    cohort_manifest_hash: ContentHash,
    evaluation_window_start: DateTime<Utc>,
    evaluation_window_end: DateTime<Utc>,
    label_cutoff: DateTime<Utc>,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_family_hash: ContentHash,
    comparison_contract_hash: ContentHash,
}

/// Inputs that irreversibly consume one unseen promotion holdout.
#[derive(Debug, Clone)]
pub struct FeedbackEvaluationUseInput {
    pub feedback_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub evaluation_dataset_id: TrainingDatasetId,
    pub evaluation_dataset_hash: ContentHash,
    pub evaluation_artifact_bytes_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub comparison_contract_hash: ContentHash,
    pub cpcv_artifact_uri: ArtifactUri,
    pub cpcv_artifact_hash: ContentHash,
}

/// Content-addressed append-only proof that an evaluation holdout was consumed.
#[derive(Debug, Clone, Serialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feedback_evaluation_use::ActiveModel")]
pub struct NewFeedbackEvaluationUse {
    feedback_evaluation_use_id: FeedbackEvaluationUseId,
    feedback_cycle_id: FeedbackCycleId,
    purpose: FeedbackEvaluationPurpose,
    dataset_purpose: DatasetPurpose,
    #[sea_orm(column_type = "JsonBinary")]
    profile_ref: ResearchProfileRef,
    research_profile_artifact_id: ResearchProfileArtifactId,
    evaluation_dataset_id: TrainingDatasetId,
    evaluation_dataset_hash: ContentHash,
    evaluation_artifact_bytes_hash: ContentHash,
    cohort_manifest_hash: ContentHash,
    evaluation_window_start: DateTime<Utc>,
    evaluation_window_end: DateTime<Utc>,
    label_cutoff: DateTime<Utc>,
    champion_model_version_id: ModelVersionId,
    champion_serving_contract_hash: ContentHash,
    candidate_family_hash: ContentHash,
    comparison_contract_hash: ContentHash,
    semantic_use_hash: ContentHash,
    cpcv_artifact_uri: ArtifactUri,
    cpcv_artifact_hash: ContentHash,
    evaluation_use_hash: ContentHash,
}

impl NewFeedbackEvaluationUse {
    pub fn try_seal(input: FeedbackEvaluationUseInput) -> Result<Self, FeedbackError> {
        input.validate()?;
        let key = FeedbackEvaluationUseKey::from(&input);
        let semantic_use_hash = CanonicalDigest::content_hash_typed(
            EVALUATION_SEMANTIC_DOMAIN,
            EVALUATION_USE_VERSION,
            &key,
        )?;
        let evaluation_use_hash = CanonicalDigest::content_hash_typed(
            EVALUATION_USE_DOMAIN,
            EVALUATION_USE_VERSION,
            &EvaluationUseDocument {
                feedback_cycle_id: input.feedback_cycle_id,
                semantic_use_hash,
                key: &key,
                cpcv_artifact_uri: &input.cpcv_artifact_uri,
                cpcv_artifact_hash: input.cpcv_artifact_hash,
            },
        )?;
        Ok(Self {
            feedback_evaluation_use_id: FeedbackEvaluationUseId::from_semantic_hash(
                &semantic_use_hash,
            ),
            feedback_cycle_id: input.feedback_cycle_id,
            purpose: FeedbackEvaluationPurpose::PromotionComparison,
            dataset_purpose: DatasetPurpose::Evaluation,
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                &input.profile_ref,
            ),
            profile_ref: input.profile_ref,
            evaluation_dataset_id: input.evaluation_dataset_id,
            evaluation_dataset_hash: input.evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: input.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: input.cohort_manifest_hash,
            evaluation_window_start: input.evaluation_window_start,
            evaluation_window_end: input.evaluation_window_end,
            label_cutoff: input.label_cutoff,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_family_hash: input.candidate_family_hash,
            comparison_contract_hash: input.comparison_contract_hash,
            semantic_use_hash,
            cpcv_artifact_uri: input.cpcv_artifact_uri,
            cpcv_artifact_hash: input.cpcv_artifact_hash,
            evaluation_use_hash,
        })
    }

    #[must_use]
    pub const fn feedback_evaluation_use_id(&self) -> FeedbackEvaluationUseId {
        self.feedback_evaluation_use_id
    }

    #[must_use]
    pub const fn feedback_cycle_id(&self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn evaluation_dataset_id(&self) -> TrainingDatasetId {
        self.evaluation_dataset_id
    }

    #[must_use]
    pub const fn evaluation_dataset_hash(&self) -> ContentHash {
        self.evaluation_dataset_hash
    }

    #[must_use]
    pub const fn evaluation_artifact_bytes_hash(&self) -> ContentHash {
        self.evaluation_artifact_bytes_hash
    }

    #[must_use]
    pub const fn cohort_manifest_hash(&self) -> ContentHash {
        self.cohort_manifest_hash
    }

    #[must_use]
    pub const fn semantic_use_hash(&self) -> ContentHash {
        self.semantic_use_hash
    }
}

impl From<&FeedbackEvaluationUseInput> for FeedbackEvaluationUseKey {
    fn from(input: &FeedbackEvaluationUseInput) -> Self {
        Self {
            format_version: EVALUATION_USE_VERSION,
            purpose: FeedbackEvaluationPurpose::PromotionComparison,
            profile_ref: input.profile_ref.clone(),
            evaluation_dataset_id: input.evaluation_dataset_id,
            evaluation_dataset_hash: input.evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: input.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: input.cohort_manifest_hash,
            evaluation_window_start: input.evaluation_window_start,
            evaluation_window_end: input.evaluation_window_end,
            label_cutoff: input.label_cutoff,
            champion_model_version_id: input.champion_model_version_id,
            champion_serving_contract_hash: input.champion_serving_contract_hash,
            candidate_family_hash: input.candidate_family_hash,
            comparison_contract_hash: input.comparison_contract_hash,
        }
    }
}

#[derive(Debug, Serialize)]
struct EvaluationUseDocument<'a> {
    feedback_cycle_id: FeedbackCycleId,
    semantic_use_hash: ContentHash,
    key: &'a FeedbackEvaluationUseKey,
    cpcv_artifact_uri: &'a ArtifactUri,
    cpcv_artifact_hash: ContentHash,
}

/// Full read projection for one consumed evaluation holdout.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feedback_evaluation_use::Entity")]
pub struct FeedbackEvaluationUseInfo {
    pub feedback_evaluation_use_id: FeedbackEvaluationUseId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub purpose: FeedbackEvaluationPurpose,
    pub dataset_purpose: DatasetPurpose,
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub evaluation_dataset_id: TrainingDatasetId,
    pub evaluation_dataset_hash: ContentHash,
    pub evaluation_artifact_bytes_hash: ContentHash,
    pub cohort_manifest_hash: ContentHash,
    pub evaluation_window_start: DateTime<Utc>,
    pub evaluation_window_end: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub candidate_family_hash: ContentHash,
    pub comparison_contract_hash: ContentHash,
    pub semantic_use_hash: ContentHash,
    pub cpcv_artifact_uri: ArtifactUri,
    pub cpcv_artifact_hash: ContentHash,
    pub evaluation_use_hash: ContentHash,
    pub reserved_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    FeedbackEvaluationUseInfo,
    quant_feedback_evaluation_use::Model,
    {
        feedback_evaluation_use_id,
        feedback_cycle_id,
        purpose,
        dataset_purpose,
        profile_ref,
        research_profile_artifact_id,
        evaluation_dataset_id,
        evaluation_dataset_hash,
        evaluation_artifact_bytes_hash,
        cohort_manifest_hash,
        evaluation_window_start,
        evaluation_window_end,
        label_cutoff,
        champion_model_version_id,
        champion_serving_contract_hash,
        candidate_family_hash,
        comparison_contract_hash,
        semantic_use_hash,
        cpcv_artifact_uri,
        cpcv_artifact_hash,
        evaluation_use_hash,
        reserved_at,
        created_at,
    }
);

impl FeedbackEvaluationUseInfo {
    /// Whether an idempotent append carries the exact immutable holdout use.
    #[must_use]
    pub fn has_same_content(&self, candidate: &NewFeedbackEvaluationUse) -> bool {
        self.feedback_evaluation_use_id == candidate.feedback_evaluation_use_id
            && self.feedback_cycle_id == candidate.feedback_cycle_id
            && self.purpose == candidate.purpose
            && self.dataset_purpose == candidate.dataset_purpose
            && self.profile_ref == candidate.profile_ref
            && self.research_profile_artifact_id == candidate.research_profile_artifact_id
            && self.evaluation_dataset_id == candidate.evaluation_dataset_id
            && self.evaluation_dataset_hash == candidate.evaluation_dataset_hash
            && self.evaluation_artifact_bytes_hash == candidate.evaluation_artifact_bytes_hash
            && self.cohort_manifest_hash == candidate.cohort_manifest_hash
            && self.evaluation_window_start == candidate.evaluation_window_start
            && self.evaluation_window_end == candidate.evaluation_window_end
            && self.label_cutoff == candidate.label_cutoff
            && self.champion_model_version_id == candidate.champion_model_version_id
            && self.champion_serving_contract_hash == candidate.champion_serving_contract_hash
            && self.candidate_family_hash == candidate.candidate_family_hash
            && self.comparison_contract_hash == candidate.comparison_contract_hash
            && self.semantic_use_hash == candidate.semantic_use_hash
            && self.cpcv_artifact_uri == candidate.cpcv_artifact_uri
            && self.cpcv_artifact_hash == candidate.cpcv_artifact_hash
            && self.evaluation_use_hash == candidate.evaluation_use_hash
    }

    pub fn validate(&self) -> Result<(), FeedbackError> {
        if self.purpose != FeedbackEvaluationPurpose::PromotionComparison
            || self.dataset_purpose != DatasetPurpose::Evaluation
            || self.research_profile_artifact_id
                != ResearchProfileArtifactId::from_profile_ref(&self.profile_ref)
        {
            return Err(FeedbackError::InvalidEvaluationUse {
                detail: "purpose or profile projection mismatch".to_owned(),
            });
        }
        let input = FeedbackEvaluationUseInput {
            feedback_cycle_id: self.feedback_cycle_id,
            profile_ref: self.profile_ref.clone(),
            evaluation_dataset_id: self.evaluation_dataset_id,
            evaluation_dataset_hash: self.evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: self.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: self.cohort_manifest_hash,
            evaluation_window_start: self.evaluation_window_start,
            evaluation_window_end: self.evaluation_window_end,
            label_cutoff: self.label_cutoff,
            champion_model_version_id: self.champion_model_version_id,
            champion_serving_contract_hash: self.champion_serving_contract_hash,
            candidate_family_hash: self.candidate_family_hash,
            comparison_contract_hash: self.comparison_contract_hash,
            cpcv_artifact_uri: self.cpcv_artifact_uri.clone(),
            cpcv_artifact_hash: self.cpcv_artifact_hash,
        };
        input.validate()?;
        let key = FeedbackEvaluationUseKey::from(&input);
        let expected_semantic_hash = CanonicalDigest::content_hash_typed(
            EVALUATION_SEMANTIC_DOMAIN,
            EVALUATION_USE_VERSION,
            &key,
        )?;
        let expected_use_hash = CanonicalDigest::content_hash_typed(
            EVALUATION_USE_DOMAIN,
            EVALUATION_USE_VERSION,
            &EvaluationUseDocument {
                feedback_cycle_id: self.feedback_cycle_id,
                semantic_use_hash: expected_semantic_hash,
                key: &key,
                cpcv_artifact_uri: &self.cpcv_artifact_uri,
                cpcv_artifact_hash: self.cpcv_artifact_hash,
            },
        )?;
        if self.reserved_at != self.created_at
            || self.semantic_use_hash != expected_semantic_hash
            || self.evaluation_use_hash != expected_use_hash
            || self.feedback_evaluation_use_id
                != FeedbackEvaluationUseId::from_semantic_hash(&expected_semantic_hash)
        {
            return Err(FeedbackError::InvalidEvaluationUse {
                detail: "timeline, semantic hash, evidence hash, or id mismatch".to_owned(),
            });
        }
        Ok(())
    }
}

impl FeedbackStageEventInput {
    fn validate(&self) -> Result<(), FeedbackError> {
        if self.event_sequence <= 0
            || self
                .actor
                .as_deref()
                .is_some_and(|actor| !valid_actor(actor))
            || self
                .reason_code
                .as_deref()
                .is_some_and(|reason| !valid_reason(reason))
        {
            return Err(FeedbackError::InvalidStageEvent {
                detail: "invalid sequence, actor, or reason code".to_owned(),
            });
        }
        validate_artifact_ref(
            self.evidence_uri.as_ref(),
            self.evidence_hash,
            "stage evidence",
        )
        .map_err(|detail| FeedbackError::InvalidStageEvent { detail })?;
        let trigger_stage = self.stage == FeedbackStage::Trigger;
        let valid = match self.event_kind {
            FeedbackStageEventKind::Triggered => {
                trigger_stage
                    && self.trigger_family.is_some()
                    && self.research_job_id.is_none()
                    && self.actor.is_some()
                    && self.reason_code.is_some()
            }
            FeedbackStageEventKind::CancellationRequested => {
                !trigger_stage
                    && self.trigger_family.is_none()
                    && self.actor.is_some()
                    && self.reason_code.is_some()
            }
            FeedbackStageEventKind::JobLinked | FeedbackStageEventKind::Started => {
                !trigger_stage
                    && self.trigger_family.is_none()
                    && self.research_job_id.is_some()
                    && self.actor.is_none()
                    && self.reason_code.is_none()
            }
            FeedbackStageEventKind::Succeeded => {
                !trigger_stage
                    && self.trigger_family.is_none()
                    && self.research_job_id.is_some()
                    && self.actor.is_none()
                    && self.reason_code.is_none()
                    && self.evidence_uri.is_some()
            }
            FeedbackStageEventKind::Failed
            | FeedbackStageEventKind::Cancelled
            | FeedbackStageEventKind::LeaseRecovered => {
                !trigger_stage
                    && self.trigger_family.is_none()
                    && self.research_job_id.is_some()
                    && self.actor.is_none()
                    && self.reason_code.is_some()
            }
        };
        if valid {
            Ok(())
        } else {
            Err(FeedbackError::InvalidStageEvent {
                detail: format!(
                    "stage {} and event kind {} have inconsistent job/actor/reason/evidence fields",
                    self.stage, self.event_kind
                ),
            })
        }
    }
}

impl DriftReportInput {
    fn validate(&self) -> Result<(), FeedbackError> {
        validate_artifact_ref(
            Some(&self.detail_uri),
            Some(self.detail_hash),
            "drift detail",
        )
        .map_err(|detail| FeedbackError::InvalidDriftReport { detail })?;
        if self.metric.kind() != self.kind
            || self.baseline_window_start >= self.baseline_window_end
            || self.baseline_window_end > self.evaluation_window_start
            || self.evaluation_window_start >= self.evaluation_window_end
            || self.evaluation_window_end > self.label_cutoff
            || self.label_cutoff > self.observed_at
            || self.sample_count < 0
            || self.threshold <= Decimal::ZERO
            || self.metric.is_unit_interval() && self.threshold > Decimal::ONE
        {
            return Err(FeedbackError::InvalidDriftReport {
                detail: "metric family, windows, cutoff, count, or threshold is invalid".to_owned(),
            });
        }
        match (self.assessment, self.observed_value) {
            (FeedbackDriftAssessment::InsufficientEvidence, None) => Ok(()),
            (FeedbackDriftAssessment::InsufficientEvidence, Some(_)) | (_, None) => {
                Err(FeedbackError::InvalidDriftReport {
                    detail: "insufficient evidence must be the only assessment without a value"
                        .to_owned(),
                })
            }
            (assessment, Some(value)) => {
                if self.sample_count == 0
                    || value < Decimal::ZERO
                    || self.metric.is_unit_interval() && value > Decimal::ONE
                    || assessment != expected_assessment(self.metric, value, self.threshold)
                {
                    return Err(FeedbackError::InvalidDriftReport {
                        detail: "observed value does not match metric range or assessment"
                            .to_owned(),
                    });
                }
                Ok(())
            }
        }
    }
}

fn expected_assessment(
    metric: FeedbackDriftMetric,
    value: Decimal,
    threshold: Decimal,
) -> FeedbackDriftAssessment {
    let exceeded = match metric {
        FeedbackDriftMetric::KolmogorovSmirnovPValue => value <= threshold,
        FeedbackDriftMetric::PopulationStabilityIndex
        | FeedbackDriftMetric::RankIcDrop
        | FeedbackDriftMetric::JensenShannonDivergence => value >= threshold,
    };
    if exceeded {
        FeedbackDriftAssessment::ThresholdExceeded
    } else {
        FeedbackDriftAssessment::WithinThreshold
    }
}

impl FeedbackEvaluationUseInput {
    fn validate(&self) -> Result<(), FeedbackError> {
        self.profile_ref
            .validate()
            .map_err(|error| FeedbackError::InvalidEvaluationUse {
                detail: error.to_string(),
            })?;
        validate_artifact_ref(
            Some(&self.cpcv_artifact_uri),
            Some(self.cpcv_artifact_hash),
            "CPCV predecessor",
        )
        .map_err(|detail| FeedbackError::InvalidEvaluationUse { detail })?;
        if self.evaluation_window_start >= self.evaluation_window_end
            || self.evaluation_window_end > self.label_cutoff
        {
            return Err(FeedbackError::InvalidEvaluationUse {
                detail: "evaluation window must end no later than the frozen label cutoff"
                    .to_owned(),
            });
        }
        Ok(())
    }
}

fn validate_artifact_ref(
    uri: Option<&ArtifactUri>,
    hash: Option<ContentHash>,
    owner: &'static str,
) -> Result<(), String> {
    if uri.is_some() != hash.is_some() {
        return Err(format!("{owner} URI and hash must be present together"));
    }
    if let Some(uri) = uri
        && (uri.as_str().len() > MAX_ARTIFACT_URI_BYTES
            || uri.scheme().is_empty()
            || !uri.scheme().bytes().enumerate().all(|(index, byte)| {
                byte.is_ascii_lowercase()
                    || index > 0 && (byte.is_ascii_digit() || matches!(byte, b'+' | b'-' | b'.'))
            }))
    {
        return Err(format!(
            "{owner} URI has an invalid or oversized scheme/path"
        ));
    }
    Ok(())
}

fn valid_actor(actor: &str) -> bool {
    !actor.is_empty()
        && actor.len() <= MAX_ACTOR_BYTES
        && actor == actor.trim()
        && actor.bytes().all(|byte| !byte.is_ascii_control())
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= MAX_REASON_BYTES
        && reason.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
        })
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Duration, TimeZone};
    use rust_decimal_macros::dec;
    use serde_json::{Value, from_value, to_value};

    use super::{
        ArtifactUri, CapabilityRegistryHashes, ContentHash, DriftReportInput, FeedbackCycleId,
        FeedbackCycleKey, FeedbackCycleKeyInput, FeedbackDriftAssessment, FeedbackDriftKind,
        FeedbackDriftMetric, FeedbackEvaluationUseInput, FeedbackStage, FeedbackStageEventInput,
        FeedbackStageEventKind, FeedbackTriggerFamily, ModelVersionId, NewDriftReport,
        NewFeedbackCycle, NewFeedbackEvaluationUse, NewFeedbackStageEvent, ResearchJobId,
        ResearchProfileRef, TrainingDatasetId, Utc,
    };
    use crate::{
        domain::{
            ports::{
                FeedbackCandidateFamily, FeedbackCandidateFamilyInput, FeedbackCandidateRecipe,
                FeedbackComparisonContract, FeedbackDatasetBuildRequest,
            },
            quant::FeedbackCohortWindow,
        },
        enums::quant::{CalibrationMethod, DatasetPurpose, DownsideSource},
        types::{
            DATASET_SOURCE_LINEAGE_FORMAT_VERSION, DatasetSourceLineage, DecisionPolicySnapshotId,
            ModelSpecId, ReaderContractVersion, ResearchProfileArtifactId, ResearchProfileId,
            SchemaContractVersion, SourceSliceId, SourceSliceManifestRef,
            builtin_research_profiles,
        },
    };

    fn hash(seed: u8) -> ContentHash {
        ContentHash::from_bytes([seed; 32])
    }

    impl ResearchProfileRef {
        fn fixture() -> Self {
            Self {
                id: ResearchProfileId::new("crypto_price_15m"),
                version: 3,
                content_hash: hash(1),
            }
        }
    }

    fn cutoff() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 25, 0, 0, 0)
            .single()
            .expect("valid cutoff")
    }

    fn source_lineage(
        profile: &ResearchProfileRef,
        capabilities: &CapabilityRegistryHashes,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> DatasetSourceLineage {
        DatasetSourceLineage {
            format_version: DATASET_SOURCE_LINEAGE_FORMAT_VERSION,
            source_slice_id: SourceSliceId::from_v7(),
            source_slice_identity_hash: hash(20),
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(profile),
            research_program_hash: hash(21),
            source_slice: SourceSliceManifestRef {
                manifest_uri: ArtifactUri::parse("s3://worm/cycle/source.json")
                    .expect("valid source URI"),
                manifest_hash: hash(22),
            },
            source_window_start: window_start,
            source_window_end: window_end,
            pit_cutoff: window_end,
            decision_policy_snapshot_id,
            runtime_config_hash: hash(23),
            reader_contract_version: ReaderContractVersion::v1(),
            schema_contract_version: SchemaContractVersion::v1(),
            source_schema_hash: hash(24),
            capability_registry_hashes: capabilities.clone(),
        }
    }

    fn dataset_request(
        profile: &ResearchProfileRef,
        capabilities: &CapabilityRegistryHashes,
        decision_policy_snapshot_id: DecisionPolicySnapshotId,
        model_spec_id: ModelSpecId,
        purpose: DatasetPurpose,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
    ) -> FeedbackDatasetBuildRequest {
        FeedbackDatasetBuildRequest {
            training_dataset_id: TrainingDatasetId::from_v7(),
            model_spec_id,
            model_spec_definition_hash: hash(25),
            source_lineage: source_lineage(
                profile,
                capabilities,
                decision_policy_snapshot_id,
                window_start,
                window_end,
            ),
            window: FeedbackCohortWindow::try_new(profile.clone(), window_start, window_end)
                .expect("valid cycle Dataset window"),
            purpose,
        }
    }

    fn candidate_family(
        profile: &ResearchProfileRef,
        capabilities: &CapabilityRegistryHashes,
    ) -> FeedbackCandidateFamily {
        let decision_policy_snapshot_id = DecisionPolicySnapshotId::from_v7();
        let model_spec_id = ModelSpecId::from_v7();
        let comparison_contract = FeedbackComparisonContract::try_from_policy(
            &builtin_research_profiles()
                .expect("built-in research profiles")
                .into_iter()
                .next()
                .expect("built-in pooled profile")
                .spec
                .feedback_policy,
        )
        .expect("valid comparison contract");
        let recipe = FeedbackCandidateRecipe::try_seal(
            dataset_request(
                profile,
                capabilities,
                decision_policy_snapshot_id,
                model_spec_id,
                DatasetPurpose::Training,
                cutoff() - Duration::days(9),
                cutoff() - Duration::days(7),
            ),
            dataset_request(
                profile,
                capabilities,
                decision_policy_snapshot_id,
                model_spec_id,
                DatasetPurpose::Calibration,
                cutoff() - Duration::days(6),
                cutoff() - Duration::days(4),
            ),
            CalibrationMethod::Platt,
            DownsideSource::MfeMae,
            decision_policy_snapshot_id,
        )
        .expect("valid cycle candidate recipe");
        FeedbackCandidateFamily::try_seal(FeedbackCandidateFamilyInput {
            shared_evaluation: dataset_request(
                profile,
                capabilities,
                decision_policy_snapshot_id,
                model_spec_id,
                DatasetPurpose::Evaluation,
                cutoff() - Duration::days(3),
                cutoff(),
            ),
            comparison_contract,
            candidates: vec![recipe],
        })
        .expect("valid cycle candidate family")
    }

    impl FeedbackCycleKey {
        fn fixture() -> Self {
            let profile_ref = ResearchProfileRef::fixture();
            let capability_registry_hashes =
                CapabilityRegistryHashes::try_new(vec![hash(3), hash(4)])
                    .expect("canonical capabilities");
            let candidate_family = candidate_family(&profile_ref, &capability_registry_hashes);
            Self::try_new(FeedbackCycleKeyInput {
                profile_ref,
                feedback_policy_hash: hash(2),
                label_cutoff: cutoff(),
                capability_registry_hashes,
                champion_model_version_id: ModelVersionId::from_v7(),
                champion_serving_contract_hash: hash(5),
                candidate_family,
            })
            .expect("valid cycle key")
        }
    }

    #[test]
    fn cycle_identity_is_deterministic() {
        let key = FeedbackCycleKey::fixture();
        let first = NewFeedbackCycle::try_seal(key.clone()).expect("seal first");
        let second = NewFeedbackCycle::try_seal(key.clone()).expect("seal second");
        assert_eq!(first.idempotency_hash, second.idempotency_hash);
        assert_eq!(first.feedback_cycle_id, second.feedback_cycle_id);
        assert_eq!(
            first.feedback_cycle_id,
            FeedbackCycleId::from_idempotency_hash(
                &key.idempotency_hash().expect("idempotency hash")
            )
        );

        let roundtrip =
            from_value::<FeedbackCycleKey>(to_value(&key).expect("serialize feedback-cycle key"))
                .expect("deserialize feedback-cycle key");
        assert_eq!(roundtrip, key);
        let mut unknown = to_value(&key).expect("serialize feedback-cycle key");
        unknown
            .as_object_mut()
            .expect("cycle key object")
            .insert("legacy_run_id".to_owned(), Value::Bool(true));
        assert!(from_value::<FeedbackCycleKey>(unknown).is_err());

        let mut family_tamper = to_value(&key).expect("serialize cycle family");
        family_tamper["candidate_family"]["comparison_contract"]["comparison_contract_hash"] =
            Value::String(hash(99).to_string());
        assert!(from_value::<FeedbackCycleKey>(family_tamper).is_err());

        let mut invalid_profile = key;
        invalid_profile.profile_ref.version = 0;
        assert!(invalid_profile.validate().is_err());
    }

    #[test]
    fn stage_shape_fails_closed() {
        let cycle_id = FeedbackCycleId::from_v7();
        let triggered = NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
            feedback_cycle_id: cycle_id,
            event_sequence: 1,
            stage: FeedbackStage::Trigger,
            event_kind: FeedbackStageEventKind::Triggered,
            trigger_family: Some(FeedbackTriggerFamily::Scheduled),
            research_job_id: None,
            actor: Some("scheduler".to_owned()),
            reason_code: Some("scheduled_cadence".to_owned()),
            evidence_uri: None,
            evidence_hash: None,
            occurred_at: cutoff(),
        })
        .expect("seal trigger event");
        assert_eq!(
            triggered.feedback_stage_event_id,
            super::FeedbackStageEventId::from_event_hash(&triggered.event_hash)
        );

        assert!(
            NewFeedbackStageEvent::try_seal(FeedbackStageEventInput {
                feedback_cycle_id: cycle_id,
                event_sequence: 2,
                stage: FeedbackStage::Training,
                event_kind: FeedbackStageEventKind::Succeeded,
                trigger_family: None,
                research_job_id: Some(ResearchJobId::from_v7()),
                actor: None,
                reason_code: None,
                evidence_uri: None,
                evidence_hash: None,
                occurred_at: cutoff(),
            })
            .is_err()
        );
    }

    #[test]
    fn drift_direction_is_typed() {
        let input = DriftReportInput {
            feedback_cycle_id: FeedbackCycleId::from_v7(),
            kind: FeedbackDriftKind::Data,
            metric: FeedbackDriftMetric::PopulationStabilityIndex,
            assessment: FeedbackDriftAssessment::ThresholdExceeded,
            baseline_window_start: cutoff() - Duration::days(30),
            baseline_window_end: cutoff() - Duration::days(15),
            evaluation_window_start: cutoff() - Duration::days(14),
            evaluation_window_end: cutoff() - Duration::days(1),
            label_cutoff: cutoff(),
            observed_value: Some(dec!(0.2)),
            threshold: dec!(0.2),
            sample_count: 500,
            detail_uri: ArtifactUri::parse("file:///drift/data.json").expect("valid artifact URI"),
            detail_hash: hash(7),
            observed_at: cutoff() + Duration::minutes(1),
        };
        let sealed = NewDriftReport::try_seal(input.clone()).expect("seal drift");
        assert_eq!(
            sealed.drift_report_id,
            super::DriftReportId::from_report_hash(&sealed.report_hash)
        );
        let mut database_scale = input.clone();
        database_scale.observed_value = Some(dec!(0.200000000000));
        database_scale.threshold = dec!(0.200000000000);
        let roundtrip =
            NewDriftReport::try_seal(database_scale).expect("seal database-scale drift");
        assert_eq!(roundtrip.report_hash, sealed.report_hash);
        assert_eq!(roundtrip.drift_report_id, sealed.drift_report_id);

        let mut wrong = input;
        wrong.assessment = FeedbackDriftAssessment::WithinThreshold;
        assert!(NewDriftReport::try_seal(wrong).is_err());
    }

    #[test]
    fn evaluation_use_is_semantic() {
        let input = FeedbackEvaluationUseInput {
            feedback_cycle_id: FeedbackCycleId::from_v7(),
            profile_ref: ResearchProfileRef::fixture(),
            evaluation_dataset_id: TrainingDatasetId::from_v7(),
            evaluation_dataset_hash: hash(8),
            evaluation_artifact_bytes_hash: hash(9),
            cohort_manifest_hash: hash(10),
            evaluation_window_start: cutoff() - Duration::days(7),
            evaluation_window_end: cutoff() - Duration::days(1),
            label_cutoff: cutoff(),
            champion_model_version_id: ModelVersionId::from_v7(),
            champion_serving_contract_hash: hash(11),
            candidate_family_hash: hash(12),
            comparison_contract_hash: hash(13),
            cpcv_artifact_uri: ArtifactUri::parse("file:///feedback/cpcv.json")
                .expect("valid artifact URI"),
            cpcv_artifact_hash: hash(14),
        };
        let first = NewFeedbackEvaluationUse::try_seal(input.clone()).expect("seal evaluation use");
        let second = NewFeedbackEvaluationUse::try_seal(input.clone()).expect("seal exact retry");
        assert_eq!(first.semantic_use_hash, second.semantic_use_hash);
        assert_eq!(
            first.feedback_evaluation_use_id,
            second.feedback_evaluation_use_id
        );

        let mut other_family = input;
        other_family.candidate_family_hash = hash(15);
        let other =
            NewFeedbackEvaluationUse::try_seal(other_family).expect("seal other candidate family");
        assert_ne!(first.semantic_use_hash, other.semantic_use_hash);
        assert_ne!(
            first.feedback_evaluation_use_id,
            other.feedback_evaluation_use_id
        );
    }
}
