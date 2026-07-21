//! Governed configuration-resource API contracts.

use chrono::{DateTime, Utc};
use schemars::{JsonSchema, Schema};
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::governance::{
        ConfigActivityInfo, DecisionPolicySnapshotOptionInfo, PolicyActivationInfo,
        PolicyActivationOutcome, PolicyApprovalInfo, PolicyRevisionInfo, ProductionBaselineInfo,
    },
    enums::runtime_config::{
        CheckOutcome, ConfigResourceKind, CredentialHealthStatus, CredentialKind,
        DecisionPolicySnapshotSource, DeploymentEndpointKind, LifecycleBaseline,
        LifecycleCheckKind, PolicyActivationKind, PolicyActorKind, PolicyApplyBoundary,
        PolicyApprovalDecision, PolicyConsumer, PolicyRevisionStatus, ProjectLifecycleState,
        ResourceBudgetKind, ResourceBudgetMetric, ResourceBudgetUnit,
    },
    runtime_config::{
        LifecycleCheckDetail, PolicyDocument, PolicyRevisionBundle, PolicyValidationEvidence,
        PolicyValidationSubject, ProductionSealEvidence, ScheduleCadence,
    },
    types::{
        AuditEventId, BuildCommitHash, ContentHash, DecisionPolicySnapshotId,
        DeploymentEnvironment, PolicyActivationId, PolicyApprovalId, PolicyBundleGeneration,
        PolicyIdempotencyKey, PolicyPreflightToken, PolicyRevisionId, ProductionBaselineId,
        ProductionSealConfirmationPhrase, SchemaVersion, UserId,
    },
};

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ConfigResourceSummaryView {
    pub kind: ConfigResourceKind,
    pub schema_version: SchemaVersion,
    pub active_revision_id: Option<PolicyRevisionId>,
    pub active_revision_hash: Option<ContentHash>,
    pub pending_approval_count: u64,
    pub effective_boundary: PolicyApplyBoundary,
    pub restart_required: bool,
    pub last_activated_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ConfigResourcesView {
    pub resources: Vec<ConfigResourceSummaryView>,
    pub active_bundle_generation: PolicyBundleGeneration,
    pub active_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub active_policy_bundle_hash: Option<ContentHash>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyResourceSchemaView {
    pub kind: ConfigResourceKind,
    pub schema_version: SchemaVersion,
    #[schemars(with = "serde_json::Value")]
    pub json_schema: Schema,
    pub effective_boundary: PolicyApplyBoundary,
    pub consumers: Vec<PolicyConsumer>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyRevisionView {
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub schema_version: SchemaVersion,
    pub revision_hash: ContentHash,
    pub document: PolicyDocument,
    pub status: PolicyRevisionStatus,
    pub validation_evidence: Option<PolicyValidationEvidence>,
    pub validated_at: Option<DateTime<Utc>>,
    pub preflight_expires_at: Option<DateTime<Utc>>,
    pub created_by: PolicyActorView,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl From<PolicyRevisionInfo> for PolicyRevisionView {
    fn from(info: PolicyRevisionInfo) -> Self {
        Self {
            policy_revision_id: info.policy_revision_id,
            resource_kind: info.resource_kind,
            schema_version: info.schema_version,
            revision_hash: info.revision_hash,
            document: info.document,
            status: info.status,
            validation_evidence: info.validation_evidence,
            validated_at: info.validated_at,
            preflight_expires_at: info.preflight_expires_at,
            created_by: PolicyActorView {
                kind: info.created_by_kind,
                user_id: info.created_by_user_id,
                label: info.created_by_label,
            },
            reason: info.reason,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyActorView {
    pub kind: PolicyActorKind,
    pub user_id: Option<UserId>,
    pub label: String,
}

/// Immutable approval record exposed by the Config API.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyApprovalView {
    pub policy_approval_id: PolicyApprovalId,
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub revision_hash: ContentHash,
    pub validation_subject: Option<PolicyValidationSubject>,
    pub decision: PolicyApprovalDecision,
    pub decided_by: PolicyActorView,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl From<PolicyApprovalInfo> for PolicyApprovalView {
    fn from(info: PolicyApprovalInfo) -> Self {
        Self {
            policy_approval_id: info.policy_approval_id,
            policy_revision_id: info.policy_revision_id,
            resource_kind: info.resource_kind,
            revision_hash: info.revision_hash,
            validation_subject: info.validation_subject,
            decision: info.decision,
            decided_by: PolicyActorView {
                kind: info.decided_by_kind,
                user_id: info.decided_by_user_id,
                label: info.decided_by_label,
            },
            reason: info.reason,
            decided_at: info.decided_at,
            expires_at: info.expires_at,
            created_at: info.created_at,
        }
    }
}

/// Immutable activation record exposed by the Config API.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyActivationView {
    pub bundle_generation: PolicyBundleGeneration,
    pub expected_bundle_generation: PolicyBundleGeneration,
    pub policy_activation_id: PolicyActivationId,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_approval_id: PolicyApprovalId,
    pub activated_at: DateTime<Utc>,
    pub activated_by: PolicyActorView,
    pub reason: String,
    pub activation_kind: PolicyActivationKind,
    pub expected_active_revision_id: Option<PolicyRevisionId>,
    pub previous_policy_revision_id: Option<PolicyRevisionId>,
    pub rollback_target_revision_id: Option<PolicyRevisionId>,
    pub idempotency_key: PolicyIdempotencyKey,
    pub activation_request_hash: ContentHash,
    pub audit_event_id: AuditEventId,
    pub created_at: DateTime<Utc>,
}

impl From<PolicyActivationInfo> for PolicyActivationView {
    fn from(info: PolicyActivationInfo) -> Self {
        Self {
            bundle_generation: info.bundle_generation,
            expected_bundle_generation: info.expected_bundle_generation,
            policy_activation_id: info.policy_activation_id,
            resource_kind: info.resource_kind,
            policy_revision_id: info.policy_revision_id,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            policy_approval_id: info.policy_approval_id,
            activated_at: info.activated_at,
            activated_by: PolicyActorView {
                kind: info.activated_by_kind,
                user_id: info.activated_by_user_id,
                label: info.activated_by_label,
            },
            reason: info.reason,
            activation_kind: info.activation_kind,
            expected_active_revision_id: info.expected_active_revision_id,
            previous_policy_revision_id: info.previous_policy_revision_id,
            rollback_target_revision_id: info.rollback_target_revision_id,
            idempotency_key: info.idempotency_key,
            activation_request_hash: info.activation_request_hash,
            audit_event_id: info.audit_event_id,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CurrentPolicyResourceView {
    pub resource: ConfigResourceKind,
    pub revision: Option<PolicyRevisionView>,
    pub activation: Option<PolicyActivationView>,
}

#[derive(Debug, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CreatePolicyDraftRequest {
    pub document: PolicyDocument,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ValidatePolicyDraftRequest {
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ApprovePolicyDraftRequest {
    pub decision: PolicyApprovalDecision,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActivatePolicyDraftRequest {
    pub approval_id: PolicyApprovalId,
    pub expected_bundle_generation: PolicyBundleGeneration,
    pub candidate_bundle_hash: ContentHash,
    pub expected_active_revision_id: Option<PolicyRevisionId>,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    pub preflight_token: PolicyPreflightToken,
    pub idempotency_key: PolicyIdempotencyKey,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyValidationView {
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub valid: bool,
    pub validation_evidence: PolicyValidationEvidence,
    pub preflight_token: Option<PolicyPreflightToken>,
    pub preflight_expires_at: Option<DateTime<Utc>>,
    pub effective_boundary: PolicyApplyBoundary,
    pub affected_consumers: Vec<PolicyConsumer>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PolicyRevisionListQuery {
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConfigActivityQuery {
    pub limit: Option<u64>,
}

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConfigSnapshotOptionsQuery {
    pub limit: Option<u64>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[serde(tag = "event_type", content = "event", rename_all = "snake_case")]
pub enum ConfigActivityView {
    Revision(Box<PolicyRevisionView>),
    Approval(PolicyApprovalView),
    Activation(PolicyActivationView),
}

impl From<ConfigActivityInfo> for ConfigActivityView {
    fn from(info: ConfigActivityInfo) -> Self {
        match info {
            ConfigActivityInfo::Revision(revision) => {
                Self::Revision(Box::new(PolicyRevisionView::from(*revision)))
            }
            ConfigActivityInfo::Approval(approval) => Self::Approval(approval.into()),
            ConfigActivityInfo::Activation(activation) => Self::Activation(activation.into()),
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeploymentConfigView {
    pub environment: DeploymentEnvironment,
    pub restart_required: bool,
    pub snapshot: DeploymentConfigSnapshotView,
    pub credential_health: Vec<CredentialHealthView>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CredentialHealthView {
    pub credential: CredentialKind,
    pub status: CredentialHealthStatus,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeploymentConfigSnapshotView {
    pub endpoints: Vec<DeploymentEndpointView>,
    pub identity: DeploymentIdentityView,
    pub resource_budgets: Vec<DeploymentResourceBudgetView>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeploymentEndpointView {
    pub kind: DeploymentEndpointKind,
    /// Redacted or non-secret endpoint suitable for operator display.
    pub address: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeploymentIdentityView {
    pub deployment_id: String,
    pub instance_id: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeploymentResourceBudgetView {
    pub kind: ResourceBudgetKind,
    pub limits: Vec<DeploymentResourceLimitView>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DeploymentResourceLimitView {
    pub metric: ResourceBudgetMetric,
    pub value: u64,
    pub unit: ResourceBudgetUnit,
}

/// Append-only production baseline exposed by the lifecycle endpoint.
#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct ProductionBaselineView {
    pub production_baseline_id: ProductionBaselineId,
    pub environment: DeploymentEnvironment,
    pub sealed_at: DateTime<Utc>,
    pub sealed_by: PolicyActorView,
    pub build_commit: BuildCommitHash,
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
    pub policy_bundle_generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_bundle_hash: ContentHash,
    pub lifecycle_policy_hash: ContentHash,
    pub evidence: ProductionSealEvidence,
    pub created_at: DateTime<Utc>,
}

impl From<ProductionBaselineInfo> for ProductionBaselineView {
    fn from(info: ProductionBaselineInfo) -> Self {
        Self {
            production_baseline_id: info.production_baseline_id,
            environment: info.environment,
            sealed_at: info.sealed_at,
            sealed_by: PolicyActorView {
                kind: info.sealed_by_kind,
                user_id: info.sealed_by_user_id,
                label: info.sealed_by_label,
            },
            build_commit: info.build_commit,
            postgres_schema_fingerprint: info.postgres_schema_fingerprint,
            clickhouse_schema_fingerprint: info.clickhouse_schema_fingerprint,
            policy_bundle_generation: info.policy_bundle_generation,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            policy_bundle_hash: info.policy_bundle_hash,
            lifecycle_policy_hash: info.lifecycle_policy_hash,
            evidence: info.evidence,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct LifecycleView {
    pub state: ProjectLifecycleState,
    pub baseline: LifecycleBaseline,
    pub environment: DeploymentEnvironment,
    pub build_commit: Option<BuildCommitHash>,
    pub postgres_schema_fingerprint: Option<ContentHash>,
    pub clickhouse_schema_fingerprint: Option<ContentHash>,
    pub active_policy_bundle_hash: Option<ContentHash>,
    pub checks: Vec<LifecycleCheckView>,
    pub production_baseline: Option<ProductionBaselineView>,
    pub required_confirmation_phrase: Option<ProductionSealConfirmationPhrase>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct LifecycleCheckView {
    pub kind: LifecycleCheckKind,
    pub outcome: CheckOutcome,
    pub detail: LifecycleCheckDetail,
}

#[derive(Debug, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SealProductionRequest {
    pub environment: DeploymentEnvironment,
    pub confirmation_phrase: ProductionSealConfirmationPhrase,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Deserialize, Validate, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SchedulePreviewRequest {
    pub cadence: ScheduleCadence,
    #[validate(range(min = 1, max = 20))]
    #[serde(default = "default_preview_count")]
    pub count: u8,
}

const fn default_preview_count() -> u8 {
    5
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct SchedulePreviewView {
    pub next_fire_times: Vec<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct PolicyActivationResultView {
    pub activation: PolicyActivationView,
    pub applied_revision: PolicyRevisionView,
    pub activation_kind: PolicyActivationKind,
    pub outcome: PolicyActivationOutcome,
    pub committed_generation: PolicyBundleGeneration,
    pub committed_snapshot_id: DecisionPolicySnapshotId,
    pub committed_snapshot_hash: ContentHash,
    pub committed_revision_vector: PolicyRevisionBundle,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct DecisionPolicySnapshotOptionView {
    pub bundle_generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub revision_vector: PolicyRevisionBundle,
    pub source: DecisionPolicySnapshotSource,
    pub created_at: DateTime<Utc>,
}

impl From<DecisionPolicySnapshotOptionInfo> for DecisionPolicySnapshotOptionView {
    fn from(info: DecisionPolicySnapshotOptionInfo) -> Self {
        Self {
            bundle_generation: info.bundle_generation,
            decision_policy_snapshot_id: info.decision_policy_snapshot_id,
            snapshot_hash: info.snapshot_hash,
            revision_vector: PolicyRevisionBundle {
                recommendation_policy: Some(info.recommendation_policy_revision_id),
                execution_risk_policy: Some(info.execution_risk_policy_revision_id),
                model_routing: Some(info.model_routing_revision_id),
                report_schedule: Some(info.report_schedule_revision_id),
                operational_control: Some(info.operational_control_revision_id),
                execution_authorization: Some(info.execution_authorization_revision_id),
            },
            source: info.source,
            created_at: info.created_at,
        }
    }
}

/// Schema-only envelope used to generate the frontend Config API contract.
///
/// It is never serialized by an HTTP handler. Keeping every request and
/// response DTO reachable from one Rust root makes generated TypeScript drift
/// mechanically detectable without duplicating wire shapes in the SPA.
#[derive(JsonSchema)]
pub struct ConfigApiContractSchema {
    pub resources_response: ConfigResourcesView,
    pub current_response: CurrentPolicyResourceView,
    pub resource_schema_response: PolicyResourceSchemaView,
    pub revisions_response: Vec<PolicyRevisionView>,
    pub create_draft_request: CreatePolicyDraftRequest,
    pub create_draft_response: PolicyRevisionView,
    pub validate_draft_request: ValidatePolicyDraftRequest,
    pub validation_response: PolicyValidationView,
    pub approve_request: ApprovePolicyDraftRequest,
    pub approve_response: PolicyApprovalView,
    pub activate_request: ActivatePolicyDraftRequest,
    pub activate_response: PolicyActivationResultView,
    pub activity_response: Vec<ConfigActivityView>,
    pub snapshot_options_query: ConfigSnapshotOptionsQuery,
    pub snapshot_options_response: Vec<DecisionPolicySnapshotOptionView>,
    pub deployment_response: DeploymentConfigView,
    pub lifecycle_response: LifecycleView,
    pub seal_production_request: SealProductionRequest,
    pub seal_production_response: LifecycleView,
    pub schedule_preview_request: SchedulePreviewRequest,
    pub schedule_preview_response: SchedulePreviewView,
}
