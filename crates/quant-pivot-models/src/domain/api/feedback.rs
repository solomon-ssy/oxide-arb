//! Feedback-cycle read contracts for the operator workbench.
//!
//! These views deliberately expose immutable evidence and lifecycle facts
//! without exposing lease ownership. Decimal values cross the JSON boundary as
//! strings so the SPA never loses precision.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::{Validate, ValidationError};

use super::quality_gate::QualityGateReportView;
use crate::{
    domain::{
        pagination::PageRequest,
        ports::ShadowBindingRejectionReceipt,
        quant::{
            DriftReportInfo, FeedbackCycleInfo, FeedbackEvaluationUseInfo, FeedbackQueueSnapshot,
            FeedbackSchedulerStateInfo, FeedbackStageEventInfo, FeedbackTriggerEventInfo,
            PromotionPermitInfo, PromotionPermitStatus, ResolutionObservationProjectionInfo,
            ResolutionProjectionAttentionItem, ResolutionProjectionRemediationInfo,
        },
    },
    enums::{
        common::MarketCategory,
        model::ModelFamily,
        quant::{
            DatasetPurpose, ExecutionAuthorityCeiling, FeedbackCycleStatus, FeedbackDecision,
            FeedbackDriftAssessment, FeedbackDriftKind, FeedbackDriftMetric,
            FeedbackEvaluationMode, FeedbackEvaluationPurpose, FeedbackSchedulerFailureKind,
            FeedbackStage, FeedbackStageEventKind, FeedbackTriggerFamily,
            ResolutionRemediationAction, ShadowBindingStatus,
        },
    },
    runtime_config::BuyModelRoute,
    types::{
        ArtifactUri, AuditEventId, CohortCensorCount, CohortExclusionCount, ContentHash,
        DatasetCohortCounts, DecisionPolicySnapshotId, DriftReportId, FeedbackCoverageArtifactId,
        FeedbackCycleId, FeedbackEvaluationUseId, FeedbackStageEventId, FeedbackTriggerEventId,
        ModelCandidateManifestId, ModelGovernanceAuditId, ModelSpecId, ModelVersionId,
        PolicyActivationId, PolicyBundleGeneration, PolicyIdempotencyKey, PolicyRevisionId,
        PromotionPermitId, ResearchEvaluationTrack, ResearchJobId, ResearchProfileArtifactId,
        ResearchProfileId, ResearchProfileRef, RoleCode, ShadowBindingArtifactId,
        TrainingDatasetId, UserId, WorkerId,
    },
};

fn validate_feedback_reason(reason: &str) -> Result<(), ValidationError> {
    if !reason.is_empty()
        && reason.len() <= 128
        && reason.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'.')
        })
    {
        Ok(())
    } else {
        Err(ValidationError::new("feedback_reason"))
    }
}

fn validate_governed_reason(reason: &str) -> Result<(), ValidationError> {
    if !reason.is_empty()
        && reason.len() <= 2_048
        && reason == reason.trim()
        && !reason.chars().any(char::is_control)
    {
        Ok(())
    } else {
        Err(ValidationError::new("governed_reason"))
    }
}

fn validate_scheduler_reason(reason: &str) -> Result<(), ValidationError> {
    if !reason.is_empty()
        && reason.len() <= 128
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        Ok(())
    } else {
        Err(ValidationError::new("scheduler_reason"))
    }
}

fn validate_scheduler_note(note: &str) -> Result<(), ValidationError> {
    if !note.is_empty()
        && note.len() <= 1_024
        && note == note.trim()
        && !note.chars().any(char::is_control)
    {
        Ok(())
    } else {
        Err(ValidationError::new("scheduler_note"))
    }
}

const fn default_permit_ttl_secs() -> u32 {
    1_800
}

/// Filters for `GET /research/feedback-cycles`.
#[derive(Debug, Clone, Deserialize, NormalizePageQuery)]
pub struct FeedbackCycleListQuery {
    /// Match every immutable version of one profile identity.
    pub profile_id: Option<ResearchProfileId>,
    pub status: Option<FeedbackCycleStatus>,
    pub trigger_family: Option<FeedbackTriggerFamily>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Filters for `GET /research/drift-reports`.
#[derive(Debug, Clone, Deserialize, NormalizePageQuery)]
pub struct DriftReportListQuery {
    pub feedback_cycle_id: Option<FeedbackCycleId>,
    /// Match every immutable version of one profile identity.
    pub profile_id: Option<ResearchProfileId>,
    pub kind: Option<FeedbackDriftKind>,
    pub metric: Option<FeedbackDriftMetric>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Governed operator intent for a conditional occurrence or an explicit
/// forced child attempt.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct FeedbackCycleTriggerRequest {
    pub profile_id: ResearchProfileId,
    pub evaluation_mode: FeedbackEvaluationMode,
    pub idempotency_key: PolicyIdempotencyKey,
    pub parent_cycle_id: Option<FeedbackCycleId>,
    #[validate(custom(function = "validate_feedback_reason"))]
    pub reason: String,
}

/// Governed cancellation intent. Stage, sequence, generation, and database
/// time are resolved by the server.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct CancelFeedbackCycleRequest {
    #[validate(custom(function = "validate_feedback_reason"))]
    pub reason: String,
}

/// Pause/resume scheduler CAS request with mandatory operator context.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct FeedbackSchedulerControlRequest {
    #[validate(range(min = 0))]
    pub expected_pause_revision: i64,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_scheduler_note"))]
    pub note: String,
}

/// Filters for `GET /research/feedback-promotion-permits`.
#[derive(Debug, Clone, Deserialize, NormalizePageQuery)]
pub struct PromotionPermitListQuery {
    pub profile_id: Option<ResearchProfileId>,
    pub category: Option<MarketCategory>,
    pub status: Option<PromotionPermitStatus>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Operator-selected limits for one server-derived promotion permit.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct IssuePromotionPermitRequest {
    pub feedback_cycle_id: FeedbackCycleId,
    #[serde(default = "default_permit_ttl_secs")]
    #[validate(range(min = 300, max = 3600))]
    pub ttl_secs: u32,
    pub idempotency_key: PolicyIdempotencyKey,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_governed_reason"))]
    pub note: String,
}

/// Base-revision CAS intent for the sole permit lifecycle mutation.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct RevokePromotionPermitRequest {
    #[validate(range(min = 0, max = 0))]
    pub expected_revision: i64,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_governed_reason"))]
    pub note: String,
}

/// Authenticated intent to consume one exact permit and atomically change one
/// model route.
///
/// Candidate, route, entry-authorization policy, and gate identities are
/// intentionally absent and remain server-derived.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct ActivateModelRouteRequest {
    pub promotion_permit_id: PromotionPermitId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub expected_policy_generation: PolicyBundleGeneration,
    #[validate(range(min = 0))]
    pub expected_runtime_control_revision: i64,
    pub idempotency_key: PolicyIdempotencyKey,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_governed_reason"))]
    pub note: String,
}

/// Exact-CAS intent to reject one `CandidateReady` shadow and release its route
/// slot through a governed policy activation.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct RejectShadowBindingRequest {
    #[validate(range(min = 1))]
    pub expected_binding_generation: u64,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub idempotency_key: PolicyIdempotencyKey,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_governed_reason"))]
    pub note: String,
}

/// Exact-CAS governed disposition for a blocked resolution projection.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct RemediateResolutionProjectionRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    pub action: ResolutionRemediationAction,
    pub idempotency_key: PolicyIdempotencyKey,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_governed_reason"))]
    pub operator_note: String,
}

/// Authenticated intent to establish the first champion for one server-derived
/// Pooled, Crypto, or Weather Buy route. Route, profile, model family, and all
/// evidence remain server-authoritative.
#[derive(Debug, Clone, Deserialize, Validate)]
#[serde(deny_unknown_fields)]
pub struct BootstrapModelRouteRequest {
    pub model_version_id: ModelVersionId,
    pub expected_policy_generation: PolicyBundleGeneration,
    #[validate(range(min = 0))]
    pub expected_runtime_control_revision: i64,
    pub idempotency_key: PolicyIdempotencyKey,
    #[validate(custom(function = "validate_scheduler_reason"))]
    pub reason_code: String,
    #[validate(custom(function = "validate_governed_reason"))]
    pub note: String,
}

/// One feedback-cycle row without worker lease ownership.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCycleView {
    pub feedback_cycle_id: FeedbackCycleId,
    pub idempotency_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub feedback_policy_hash: ContentHash,
    pub label_cutoff: DateTime<Utc>,
    pub champion_model_version_id: ModelVersionId,
    pub champion_serving_contract_hash: ContentHash,
    pub champion_model_spec_id: ModelSpecId,
    pub champion_model_spec_definition_hash: ContentHash,
    pub champion_model_family: ModelFamily,
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
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub cancel_requested_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<FeedbackCycleInfo> for FeedbackCycleView {
    fn from(info: FeedbackCycleInfo) -> Self {
        Self {
            feedback_cycle_id: info.feedback_cycle_id,
            idempotency_hash: info.idempotency_hash,
            profile_ref: info.profile_ref,
            research_profile_artifact_id: info.research_profile_artifact_id,
            feedback_policy_hash: info.feedback_policy_hash,
            label_cutoff: info.label_cutoff,
            champion_model_version_id: info.champion_model_version_id,
            champion_serving_contract_hash: info.champion_serving_contract_hash,
            champion_model_spec_id: info.champion_model_spec_id,
            champion_model_spec_definition_hash: info.champion_model_spec_definition_hash,
            champion_model_family: info.champion_model_family,
            route: info.route,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            decision_policy_snapshot_hash: info.decision_policy_snapshot_hash,
            policy_bundle_generation: info.policy_bundle_generation,
            route_generation: info.route_generation,
            evaluation_mode: info.evaluation_mode,
            parent_cycle_id: info.parent_cycle_id,
            forced_idempotency_key: info.forced_idempotency_key,
            status: info.status,
            decision: info.decision,
            terminal_reason_code: info.terminal_reason_code,
            generation: info.generation,
            lease_expires_at: info.lease_expires_at,
            cancel_requested_at: info.cancel_requested_at,
            started_at: info.started_at,
            completed_at: info.completed_at,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// Idempotent governed cycle-mutation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCycleMutationView {
    pub cycle: FeedbackCycleView,
    pub replayed: bool,
}

impl FeedbackCycleMutationView {
    #[must_use]
    pub fn new(cycle: FeedbackCycleInfo, replayed: bool) -> Self {
        Self {
            cycle: cycle.into(),
            replayed,
        }
    }
}

/// Governed trigger result separating cadence convergence from exact replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCycleTriggerView {
    pub cycle: FeedbackCycleView,
    pub cycle_reused: bool,
    pub trigger_replayed: bool,
}

impl FeedbackCycleTriggerView {
    #[must_use]
    pub fn new(cycle: FeedbackCycleInfo, cycle_reused: bool, trigger_replayed: bool) -> Self {
        Self {
            cycle: cycle.into(),
            cycle_reused,
            trigger_replayed,
        }
    }
}

/// Operational projection of one PostgreSQL-authoritative scheduler row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackSchedulerStateView {
    pub research_profile_id: ResearchProfileId,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub feedback_policy_hash: ContentHash,
    pub cadence_secs: i64,
    pub cooldown_secs: i64,
    pub next_due_at: DateTime<Utc>,
    pub pending_cutoff: Option<DateTime<Utc>>,
    pub pending_started_at: Option<DateTime<Utc>>,
    pub last_cycle_id: Option<FeedbackCycleId>,
    pub last_cutoff: Option<DateTime<Utc>>,
    pub cooldown_until: Option<DateTime<Utc>>,
    pub coalesced_gap_count: i64,
    pub last_coalesced_from: Option<DateTime<Utc>>,
    pub last_coalesced_to: Option<DateTime<Utc>>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub attempt: i32,
    pub retry_at: Option<DateTime<Utc>>,
    pub last_failure_kind: Option<FeedbackSchedulerFailureKind>,
    pub last_error: Option<String>,
    pub settlement_failure_count: i64,
    pub last_settlement_failed_at: Option<DateTime<Utc>>,
    pub last_settlement_error: Option<String>,
    pub paused: bool,
    pub pause_revision: i64,
    pub pause_reason_code: Option<String>,
    pub pause_note: Option<String>,
    pub revision: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<FeedbackSchedulerStateInfo> for FeedbackSchedulerStateView {
    fn from(info: FeedbackSchedulerStateInfo) -> Self {
        Self {
            research_profile_id: info.research_profile_id,
            research_profile_artifact_id: info.research_profile_artifact_id,
            profile_hash: info.profile_hash,
            feedback_policy_hash: info.feedback_policy_hash,
            cadence_secs: info.cadence_secs,
            cooldown_secs: info.cooldown_secs,
            next_due_at: info.next_due_at,
            pending_cutoff: info.pending_cutoff,
            pending_started_at: info.pending_started_at,
            last_cycle_id: info.last_cycle_id,
            last_cutoff: info.last_cutoff,
            cooldown_until: info.cooldown_until,
            coalesced_gap_count: info.coalesced_gap_count,
            last_coalesced_from: info.last_coalesced_from,
            last_coalesced_to: info.last_coalesced_to,
            lease_owner: info.lease_owner,
            lease_expires_at: info.lease_expires_at,
            attempt: info.attempt,
            retry_at: info.retry_at,
            last_failure_kind: info.last_failure_kind,
            last_error: info.last_error,
            settlement_failure_count: info.settlement_failure_count,
            last_settlement_failed_at: info.last_settlement_failed_at,
            last_settlement_error: info.last_settlement_error,
            paused: info.paused,
            pause_revision: info.pause_revision,
            pause_reason_code: info.pause_reason_code,
            pause_note: info.pause_note,
            revision: info.revision,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

/// List snapshot timestamped by the authoritative `PostgreSQL` clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackSchedulerListView {
    pub observed_at: DateTime<Utc>,
    pub items: Vec<FeedbackSchedulerStateView>,
}

/// One pause/resume mutation and its authoritative observation time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackSchedulerMutationView {
    pub observed_at: DateTime<Utc>,
    pub state: FeedbackSchedulerStateView,
}

/// One promotion permit with status derived at an authoritative database time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PromotionPermitView {
    pub promotion_permit_id: PromotionPermitId,
    pub idempotency_key: PolicyIdempotencyKey,
    pub scope_hash: ContentHash,
    pub issuance_hash: ContentHash,
    pub feedback_cycle_id: FeedbackCycleId,
    pub profile_ref: ResearchProfileRef,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub category: MarketCategory,
    pub expected_policy_generation: PolicyBundleGeneration,
    pub expected_runtime_control_revision: i64,
    pub expected_decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub expected_snapshot_hash: ContentHash,
    pub expected_route_generation: u64,
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
    pub issued_by_username: String,
    pub issued_by_role: RoleCode,
    pub issuance_reason: String,
    pub expires_at: DateTime<Utc>,
    pub revoked_by_user_id: Option<UserId>,
    pub revoked_by_username: Option<String>,
    pub revoked_by_role: Option<RoleCode>,
    pub revocation_reason: Option<String>,
    pub revoked_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub issued_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub status: PromotionPermitStatus,
    pub observed_at: DateTime<Utc>,
}

impl PromotionPermitView {
    pub fn try_new(
        info: PromotionPermitInfo,
        observed_at: DateTime<Utc>,
    ) -> Result<Self, FeedbackError> {
        let status = info.status_at(observed_at)?;
        let expected_route_generation = info.scope()?.expected_route_generation();
        Ok(Self {
            promotion_permit_id: info.promotion_permit_id,
            idempotency_key: info.idempotency_key,
            scope_hash: info.scope_hash,
            issuance_hash: info.issuance_hash,
            feedback_cycle_id: info.feedback_cycle_id,
            profile_ref: info.profile_ref,
            research_profile_artifact_id: info.research_profile_artifact_id,
            category: info.category,
            expected_policy_generation: info.expected_policy_generation,
            expected_runtime_control_revision: info.expected_runtime_control_revision,
            expected_decision_policy_snapshot_id: info.expected_decision_policy_snapshot_id,
            expected_snapshot_hash: info.expected_snapshot_hash,
            expected_route_generation,
            champion_model_version_id: info.champion_model_version_id,
            champion_serving_contract_hash: info.champion_serving_contract_hash,
            candidate_model_version_id: info.candidate_model_version_id,
            candidate_manifest_id: info.candidate_manifest_id,
            candidate_manifest_hash: info.candidate_manifest_hash,
            promotion_gate_hash: info.promotion_gate_hash,
            maximum_execution_authority: info.maximum_execution_authority,
            non_route_policy_hash: info.non_route_policy_hash,
            serving_constraints_hash: info.serving_constraints_hash,
            preflight_hash: info.preflight_hash,
            issued_by_user_id: info.issued_by_user_id,
            issued_by_username: info.issued_by_username,
            issued_by_role: info.issued_by_role,
            issuance_reason: info.issuance_reason,
            expires_at: info.expires_at,
            revoked_by_user_id: info.revoked_by_user_id,
            revoked_by_username: info.revoked_by_username,
            revoked_by_role: info.revoked_by_role,
            revocation_reason: info.revocation_reason,
            revoked_at: info.revoked_at,
            revision: info.revision,
            issued_at: info.issued_at,
            updated_at: info.updated_at,
            status,
            observed_at,
        })
    }
}

/// Idempotent governed permit-mutation result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PromotionPermitMutationView {
    pub permit: PromotionPermitView,
    pub replayed: bool,
}

impl PromotionPermitMutationView {
    pub fn try_new(
        permit: PromotionPermitInfo,
        observed_at: DateTime<Utc>,
        replayed: bool,
    ) -> Result<Self, FeedbackError> {
        Ok(Self {
            permit: PromotionPermitView::try_new(permit, observed_at)?,
            replayed,
        })
    }
}

/// Immutable receipt for one committed model-route activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelRouteActivationReceiptView {
    pub promotion_permit_id: PromotionPermitId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub route: BuyModelRoute,
    pub previous_route_generation: u64,
    pub activated_route_generation: u64,
    pub previous_model_version_id: ModelVersionId,
    pub activated_model_version_id: ModelVersionId,
    pub policy_activation_id: PolicyActivationId,
    pub model_governance_audit_id: ModelGovernanceAuditId,
    pub audit_event_id: AuditEventId,
    pub outbox_event_id: AuditEventId,
    pub transaction_hash: ContentHash,
    pub activated_model_routing_revision_id: PolicyRevisionId,
    pub rollback_target: ModelRouteRollbackTargetView,
    pub permit_issued_by_user_id: UserId,
    pub permit_issued_by_username: String,
    pub permit_issued_by_role: RoleCode,
    pub activated_by_user_id: UserId,
    pub activated_by_username: String,
    pub activated_by_role: RoleCode,
    pub server_timestamp: DateTime<Utc>,
    pub execution_authority_unchanged: bool,
}

/// Invocation outcome for an idempotent model-route activation command.
///
/// `replayed` describes the current command invocation, not the immutable
/// receipt. Read APIs therefore return the receipt directly and can never
/// mislabel a historical activation as a command replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelRouteActivationMutationView {
    pub receipt: ModelRouteActivationReceiptView,
    pub replayed: bool,
}

/// Exact sanitized rollback delta sealed into a model-route activation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelRouteRollbackTargetView {
    pub route: BuyModelRoute,
    pub rollback_target_revision_id: PolicyRevisionId,
    pub rollback_target_revision_hash: ContentHash,
    pub activated_model_version_id: ModelVersionId,
    pub restored_model_version_id: ModelVersionId,
    pub shadow_cleared: bool,
}

/// Governed shadow-rejection result, including the immutable policy/audit
/// receipt and whether the request was an exact idempotent replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ShadowBindingRejectionReceiptView {
    pub receipt: ShadowBindingRejectionReceipt,
    pub outbox_event_id: AuditEventId,
    pub replayed: bool,
}

/// Current projection plus immutable remediation evidence after commit/replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ResolutionProjectionRemediationView {
    pub projection: ResolutionObservationProjectionInfo,
    pub remediation: ResolutionProjectionRemediationInfo,
    pub replayed: bool,
}

/// Typed receipt for a first-champion route bootstrap or exact replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ModelRouteBootstrapReceiptView {
    pub route: BuyModelRoute,
    pub previous_route_generation: PolicyBundleGeneration,
    pub activated_route_generation: PolicyBundleGeneration,
    pub activated_model_version_id: ModelVersionId,
    pub policy_activation_id: PolicyActivationId,
    pub model_governance_audit_id: ModelGovernanceAuditId,
    pub audit_event_id: AuditEventId,
    pub outbox_event_id: AuditEventId,
    pub transaction_hash: ContentHash,
    pub activated_by_user_id: UserId,
    pub activated_by_username: String,
    pub activated_by_role: RoleCode,
    pub server_timestamp: DateTime<Utc>,
    pub execution_authority_unchanged: bool,
    pub replayed: bool,
}

/// One immutable stage transition in cycle sequence order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackStageEventView {
    pub feedback_stage_event_id: FeedbackStageEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub event_sequence: i64,
    pub stage: FeedbackStage,
    pub event_kind: FeedbackStageEventKind,
    pub research_job_id: Option<ResearchJobId>,
    pub actor: Option<String>,
    pub reason_code: Option<String>,
    pub evidence_uri: Option<ArtifactUri>,
    pub evidence_hash: Option<ContentHash>,
    pub occurred_at: DateTime<Utc>,
    pub event_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<FeedbackStageEventInfo> for FeedbackStageEventView {
    fn from(info: FeedbackStageEventInfo) -> Self {
        Self {
            feedback_stage_event_id: info.feedback_stage_event_id,
            feedback_cycle_id: info.feedback_cycle_id,
            event_sequence: info.event_sequence,
            stage: info.stage,
            event_kind: info.event_kind,
            research_job_id: info.research_job_id,
            actor: info.actor,
            reason_code: info.reason_code,
            evidence_uri: info.evidence_uri,
            evidence_hash: info.evidence_hash,
            occurred_at: info.occurred_at,
            event_hash: info.event_hash,
            created_at: info.created_at,
        }
    }
}

/// One append-only trigger intent; multiple families may converge on one
/// canonical feedback cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackTriggerEventView {
    pub feedback_trigger_event_id: FeedbackTriggerEventId,
    pub feedback_cycle_id: FeedbackCycleId,
    pub trigger_family: FeedbackTriggerFamily,
    pub actor_user_id: Option<UserId>,
    pub actor_label: String,
    pub actor_role: Option<RoleCode>,
    pub reason_code: String,
    pub event_hash: ContentHash,
    pub occurred_at: DateTime<Utc>,
}

impl From<FeedbackTriggerEventInfo> for FeedbackTriggerEventView {
    fn from(info: FeedbackTriggerEventInfo) -> Self {
        Self {
            feedback_trigger_event_id: info.feedback_trigger_event_id,
            feedback_cycle_id: info.feedback_cycle_id,
            trigger_family: info.trigger_family,
            actor_user_id: info.actor_user_id,
            actor_label: info.actor_label,
            actor_role: info.actor_role,
            reason_code: info.reason_code,
            event_hash: info.event_hash,
            occurred_at: info.occurred_at,
        }
    }
}

/// Immutable typed drift header. Decimal fields are canonical strings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DriftReportView {
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
    pub observed_value: Option<String>,
    pub threshold: String,
    pub sample_count: i64,
    pub detail_uri: ArtifactUri,
    pub detail_hash: ContentHash,
    pub observed_at: DateTime<Utc>,
    pub report_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<DriftReportInfo> for DriftReportView {
    fn from(info: DriftReportInfo) -> Self {
        Self {
            drift_report_id: info.drift_report_id,
            feedback_cycle_id: info.feedback_cycle_id,
            kind: info.kind,
            metric: info.metric,
            assessment: info.assessment,
            baseline_window_start: info.baseline_window_start,
            baseline_window_end: info.baseline_window_end,
            evaluation_window_start: info.evaluation_window_start,
            evaluation_window_end: info.evaluation_window_end,
            label_cutoff: info.label_cutoff,
            observed_value: info
                .observed_value
                .map(|value| value.normalize().to_string()),
            threshold: info.threshold.normalize().to_string(),
            sample_count: info.sample_count,
            detail_uri: info.detail_uri,
            detail_hash: info.detail_hash,
            observed_at: info.observed_at,
            report_hash: info.report_hash,
            created_at: info.created_at,
        }
    }
}

/// Immutable one-time evaluation-holdout use lineage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackEvaluationUseView {
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

impl From<FeedbackEvaluationUseInfo> for FeedbackEvaluationUseView {
    fn from(info: FeedbackEvaluationUseInfo) -> Self {
        Self {
            feedback_evaluation_use_id: info.feedback_evaluation_use_id,
            feedback_cycle_id: info.feedback_cycle_id,
            purpose: info.purpose,
            dataset_purpose: info.dataset_purpose,
            profile_ref: info.profile_ref,
            research_profile_artifact_id: info.research_profile_artifact_id,
            evaluation_dataset_id: info.evaluation_dataset_id,
            evaluation_dataset_hash: info.evaluation_dataset_hash,
            evaluation_artifact_bytes_hash: info.evaluation_artifact_bytes_hash,
            cohort_manifest_hash: info.cohort_manifest_hash,
            evaluation_window_start: info.evaluation_window_start,
            evaluation_window_end: info.evaluation_window_end,
            label_cutoff: info.label_cutoff,
            champion_model_version_id: info.champion_model_version_id,
            champion_serving_contract_hash: info.champion_serving_contract_hash,
            candidate_family_hash: info.candidate_family_hash,
            comparison_contract_hash: info.comparison_contract_hash,
            semantic_use_hash: info.semantic_use_hash,
            cpcv_artifact_uri: info.cpcv_artifact_uri,
            cpcv_artifact_hash: info.cpcv_artifact_hash,
            evaluation_use_hash: info.evaluation_use_hash,
            reserved_at: info.reserved_at,
            created_at: info.created_at,
        }
    }
}

/// Reconciled candidate classification for one cohort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCohortCountsView {
    pub candidate_count: u64,
    pub eligible_count: u64,
    pub included_count: u64,
    pub exclusion_counts: Vec<CohortExclusionCount>,
    pub censor_counts: Vec<CohortCensorCount>,
}

impl From<&DatasetCohortCounts> for FeedbackCohortCountsView {
    fn from(counts: &DatasetCohortCounts) -> Self {
        Self {
            candidate_count: counts.candidate_count(),
            eligible_count: counts.eligible_count(),
            included_count: counts.included_count(),
            exclusion_counts: counts.exclusion_counts().to_vec(),
            censor_counts: counts.censor_counts().to_vec(),
        }
    }
}

/// Stable coverage-gate outcome vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FeedbackCoverageDecision {
    Advance,
    NoAction,
}

/// Bounded API projection of the immutable coverage artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCoverageView {
    pub artifact_id: FeedbackCoverageArtifactId,
    pub artifact_uri: ArtifactUri,
    pub artifact_hash: ContentHash,
    pub evaluation_window_start: DateTime<Utc>,
    pub label_cutoff: DateTime<Utc>,
    pub model_learning_candidate_count: u64,
    pub mature_label_count: u64,
    pub new_mature_label_count: u64,
    pub minimum_mature_labels: u64,
    pub minimum_new_mature_labels: u64,
    pub minimum_coverage: String,
    pub coverage: String,
    pub decision: FeedbackCoverageDecision,
    pub reason_code: Option<String>,
    pub model_learning: FeedbackCohortCountsView,
    pub execution_learning: FeedbackCohortCountsView,
    pub policy_evaluation: FeedbackCohortCountsView,
}

/// Same-window challenger evidence selected by the terminal Decision stage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCandidateComparisonView {
    pub observation_count: u64,
    pub effect_bps: String,
    pub simultaneous_lower_bound_bps: String,
    pub adjusted_p_value: String,
    pub confidence: String,
}

/// Production-generation shadow evidence for the selected challenger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCandidateShadowView {
    pub observed: u64,
    pub required: u64,
    pub served_window_secs: u64,
    pub required_window_secs: u64,
    pub mean_topn_decision_overlap: String,
    pub minimum_topn_decision_overlap: String,
    pub any_hard_divergence: bool,
}

/// Attribution plan summary; artifact hashes remain expandable evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackAttributionSummaryView {
    pub prior_cycle_use_count: u64,
    pub prediction_explanation_count: u64,
    pub decision_intervention_replay_count: u64,
    pub resolution_outcome_association_count: u64,
    pub execution_outcome_association_count: u64,
    pub execution_trajectory_count: u64,
    pub policy_counterfactual_count: u64,
    pub use_set_hash: ContentHash,
    pub produced_set_hash: ContentHash,
}

/// Exact serving-only route change proposed by one `CandidateReady` decision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackRouteDiffView {
    pub route: BuyModelRoute,
    pub shadow_binding_id: ShadowBindingArtifactId,
    pub shadow_bound_at: DateTime<Utc>,
    pub shadow_binding_generation: u64,
    pub shadow_binding_status: ShadowBindingStatus,
    pub shadow_lifecycle_generation: u64,
    pub shadow_terminated_at: Option<DateTime<Utc>>,
    pub shadow_termination_policy_activation_id: Option<PolicyActivationId>,
    pub shadow_termination_reason_code: Option<String>,
    pub current_policy_generation: PolicyBundleGeneration,
    pub current_route_generation: u64,
    pub proposed_route_generation: u64,
    pub champion_model_version_id: ModelVersionId,
    pub candidate_model_version_id: ModelVersionId,
    pub execution_authority_unchanged: bool,
}

/// Operator scorecard for a terminal `CandidateReady` cycle.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCandidateReadyView {
    pub quality_gate: QualityGateReportView,
    pub comparison: FeedbackCandidateComparisonView,
    pub shadow: FeedbackCandidateShadowView,
    pub attribution: FeedbackAttributionSummaryView,
    pub route_diff: FeedbackRouteDiffView,
    pub blockers: Vec<String>,
}

/// Current queue state from the authoritative `PostgreSQL` clock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackQueueView {
    pub queued: u64,
    pub running: u64,
    pub pending_outbox: u64,
    pub oldest_queued_at: Option<DateTime<Utc>>,
    pub oldest_running_at: Option<DateTime<Utc>>,
}

impl From<FeedbackQueueSnapshot> for FeedbackQueueView {
    fn from(snapshot: FeedbackQueueSnapshot) -> Self {
        Self {
            queued: snapshot.queued,
            running: snapshot.running,
            pending_outbox: snapshot.pending_outbox,
            oldest_queued_at: snapshot.oldest_queued_at,
            oldest_running_at: snapshot.oldest_running_at,
        }
    }
}

/// PostgreSQL-authoritative truth pipeline coverage at one observation time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackTruthOperationsView {
    pub observed_at: DateTime<Utc>,
    pub resolution_unresolved_count: u64,
    pub resolution_mapping_blocked_count: u64,
    pub resolution_quarantined_count: u64,
    pub resolution_excluded_count: u64,
    pub resolution_oldest_unresolved_at: Option<DateTime<Utc>>,
    pub resolution_terminal_through: DateTime<Utc>,
    pub resolution_attention: Vec<ResolutionProjectionAttentionItem>,
    pub execution_attempt_unsealed_count: u64,
    pub execution_attempt_sealed_through: DateTime<Utc>,
    pub recommendation_rollup_unsealed_count: u64,
    pub recommendation_rollup_sealed_through: DateTime<Utc>,
}

/// Verified research-readiness evidence. Missing measurements remain null.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackReadinessView {
    pub observed_at: DateTime<Utc>,
    pub required_history_days: u32,
    pub observed_history_days: Option<u32>,
    pub retention_ready: bool,
    pub latency_ready: bool,
}

/// Feedback policy and latest cycle for one immutable built-in profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackProfileOverviewView {
    pub profile_ref: ResearchProfileRef,
    pub category: Option<MarketCategory>,
    pub activation_eligibility: ResearchEvaluationTrack,
    pub feedback_policy_hash: ContentHash,
    pub evaluation_window_days: u32,
    pub feedback_cadence_secs: u64,
    pub minimum_mature_labels: u64,
    pub minimum_new_mature_labels: u64,
    pub retraining_cooldown_secs: u64,
    pub minimum_coverage: String,
    pub latest_cycle: Option<FeedbackCycleView>,
    pub latest_coverage: Option<FeedbackCoverageView>,
}

/// Authoritative feedback dashboard snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackOverviewView {
    /// Latest durable outbox revision at snapshot time.
    pub revision: i64,
    pub generated_at: DateTime<Utc>,
    pub queue: FeedbackQueueView,
    pub truth_operations: FeedbackTruthOperationsView,
    pub readiness: Option<FeedbackReadinessView>,
    pub profiles: Vec<FeedbackProfileOverviewView>,
}

/// Cycle master-detail payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FeedbackCycleDetailView {
    pub cycle: FeedbackCycleView,
    pub triggers: Vec<FeedbackTriggerEventView>,
    pub timeline: Vec<FeedbackStageEventView>,
    pub coverage: Option<FeedbackCoverageView>,
    pub candidate_ready: Option<FeedbackCandidateReadyView>,
    pub activation_receipt: Option<ModelRouteActivationReceiptView>,
    pub drift_reports: Vec<DriftReportView>,
    pub evaluation_uses: Vec<FeedbackEvaluationUseView>,
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};
    use rust_decimal::Decimal;
    use serde_json::json;
    use validator::Validate as _;

    use super::{
        BootstrapModelRouteRequest, CancelFeedbackCycleRequest, DriftReportListQuery,
        DriftReportView, FeedbackCycleListQuery, FeedbackCycleTriggerRequest,
        FeedbackReadinessView, IssuePromotionPermitRequest, RevokePromotionPermitRequest,
    };
    use crate::{
        domain::{
            pagination::{NormalizePageQuery as _, PageRequest, Paginated},
            quant::DriftReportInfo,
        },
        enums::quant::{
            FeedbackDriftAssessment, FeedbackDriftKind, FeedbackDriftMetric, FeedbackEvaluationMode,
        },
        types::{
            ArtifactUri, ContentHash, DriftReportId, FeedbackCycleId, ModelVersionId,
            PolicyBundleGeneration, PolicyIdempotencyKey, ResearchProfileId,
        },
    };

    fn at(hour: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 28, hour, 0, 0)
            .single()
            .expect("valid test timestamp")
    }

    #[test]
    fn cycle_query_normalizes() {
        let query = FeedbackCycleListQuery {
            profile_id: None,
            status: None,
            trigger_family: None,
            page: PageRequest::new(0, 1_000),
        }
        .normalized();

        assert_eq!(query.page.page, 1);
        assert_eq!(query.page.size, PageRequest::MAX_SIZE);
    }

    #[test]
    fn drift_query_normalizes() {
        let query = DriftReportListQuery {
            feedback_cycle_id: None,
            profile_id: None,
            kind: Some(FeedbackDriftKind::Data),
            metric: Some(FeedbackDriftMetric::PopulationStabilityIndex),
            page: PageRequest::new(0, 1_000),
        }
        .normalized();

        assert_eq!(query.page.page, 1);
        assert_eq!(query.page.size, PageRequest::MAX_SIZE);
    }

    #[test]
    fn drift_decimal_wire() {
        let info = DriftReportInfo {
            drift_report_id: DriftReportId::from_v7(),
            feedback_cycle_id: FeedbackCycleId::from_v7(),
            kind: FeedbackDriftKind::Data,
            metric: FeedbackDriftMetric::PopulationStabilityIndex,
            assessment: FeedbackDriftAssessment::WithinThreshold,
            baseline_window_start: at(0),
            baseline_window_end: at(1),
            evaluation_window_start: at(1),
            evaluation_window_end: at(2),
            label_cutoff: at(2),
            observed_value: Some(Decimal::new(123_456_789_012_345_678, 18)),
            threshold: Decimal::new(1, 18),
            sample_count: 12,
            detail_uri: ArtifactUri::parse("file://feedback/drift.json")
                .expect("valid artifact URI"),
            detail_hash: ContentHash::from_bytes([3; 32]),
            observed_at: at(3),
            report_hash: ContentHash::from_bytes([4; 32]),
            created_at: at(3),
        };
        let value =
            serde_json::to_value(DriftReportView::from(info)).expect("drift view serializes");

        assert_eq!(value["observed_value"], "0.123456789012345678");
        assert_eq!(value["threshold"], "0.000000000000000001");
        assert!(value["observed_value"].is_string());
        assert!(value["threshold"].is_string());
    }

    #[test]
    fn readiness_null_wire() {
        let value = serde_json::to_value(FeedbackReadinessView {
            observed_at: at(0),
            required_history_days: 30,
            observed_history_days: None,
            retention_ready: false,
            latency_ready: true,
        })
        .expect("readiness view serializes");

        assert!(value["observed_history_days"].is_null());
    }

    #[test]
    fn page_wire_snapshot() {
        let page = Paginated::new(vec!["cycle"], 3, 1, 1);
        let value = serde_json::to_value(page).expect("page serializes");

        assert_eq!(
            value,
            serde_json::json!({
                "items": ["cycle"],
                "total": 3,
                "page": 1,
                "size": 1,
                "has_next": true,
            })
        );
    }

    #[test]
    fn mutation_requests_validate() {
        assert!(
            FeedbackCycleTriggerRequest {
                profile_id: ResearchProfileId::new("crypto_standard"),
                evaluation_mode: FeedbackEvaluationMode::Conditional,
                idempotency_key: "feedback-cycle-0001"
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid cycle idempotency key"),
                parent_cycle_id: None,
                reason: "operator_retrain.1".to_owned(),
            }
            .validate()
            .is_ok()
        );
        assert!(
            FeedbackCycleTriggerRequest {
                profile_id: ResearchProfileId::new("crypto_standard"),
                evaluation_mode: FeedbackEvaluationMode::Conditional,
                idempotency_key: "feedback-cycle-0002"
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid cycle idempotency key"),
                parent_cycle_id: None,
                reason: "Operator retrain".to_owned(),
            }
            .validate()
            .is_err()
        );
        assert!(
            CancelFeedbackCycleRequest {
                reason: "operator_cancelled".to_owned(),
            }
            .validate()
            .is_ok()
        );
        let permit = IssuePromotionPermitRequest {
            feedback_cycle_id: FeedbackCycleId::from_v7(),
            ttl_secs: 1_800,
            idempotency_key: "feedback-permit-0001"
                .parse::<PolicyIdempotencyKey>()
                .expect("valid permit idempotency key"),
            reason_code: "candidate_approved".to_owned(),
            note: "authorize one exact category route".to_owned(),
        };
        assert!(permit.validate().is_ok());
        let mut short_ttl = permit.clone();
        short_ttl.ttl_secs = 299;
        assert!(short_ttl.validate().is_err());
        let mut long_ttl = permit;
        long_ttl.ttl_secs = 3_601;
        assert!(long_ttl.validate().is_err());
        assert!(
            BootstrapModelRouteRequest {
                model_version_id: ModelVersionId::from_v7(),
                expected_policy_generation: PolicyBundleGeneration::FIRST,
                expected_runtime_control_revision: 0,
                idempotency_key: "model-bootstrap-0001"
                    .parse::<PolicyIdempotencyKey>()
                    .expect("valid bootstrap idempotency key"),
                reason_code: "initial_champion".to_owned(),
                note: "establish first analysis-only category route".to_owned(),
            }
            .validate()
            .is_ok()
        );
        assert!(
            RevokePromotionPermitRequest {
                expected_revision: 0,
                reason_code: "operator_revoked".to_owned(),
                note: "withdraw exact authority".to_owned(),
            }
            .validate()
            .is_ok()
        );
        assert!(
            RevokePromotionPermitRequest {
                expected_revision: 1,
                reason_code: "operator_revoked".to_owned(),
                note: "withdraw exact authority".to_owned(),
            }
            .validate()
            .is_err()
        );
    }

    #[test]
    fn mutation_wire_rejects_unknown() {
        let result = serde_json::from_value::<FeedbackCycleTriggerRequest>(json!({
            "profile_id": "crypto_standard",
            "reason": "operator_retrain",
            "label_cutoff": "2026-07-28T00:00:00Z",
        }));

        assert!(result.is_err());
    }
}
