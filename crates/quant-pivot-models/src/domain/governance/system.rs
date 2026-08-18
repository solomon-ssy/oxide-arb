//! System status, lifecycle, config, accounting, and reporting domain models.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

use crate::{
    domain::{
        api::ExecutionRecoverySummary,
        governance::{
            kill_switch::KillSwitchView,
            lifecycle::{MarketDataConnectivity, OperationalPhase, WsShardConnectivity},
        },
        ports::runtime_control::CatalogState,
    },
    entities::{policy_activation, policy_approval, policy_profile_artifact, policy_revision},
    enums::{
        execution::KillSwitchState,
        quant::EntryAuthorizationPolicy,
        runtime_config::{
            ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind,
            PolicyActorKind, PolicyApprovalDecision, PolicyRevisionStatus, ProfileArtifactKind,
        },
        system::ShutdownStage,
    },
    runtime_config::{
        ActivePolicyBundle, DecisionPolicySnapshot, DecisionPolicySnapshotDocument, PolicyDocument,
        PolicyProfileDocument, PolicyValidationEvidence, PolicyValidationSubject,
    },
    types::{
        AuditEventId, ContentHash, DecisionPolicySnapshotId, ModelGovernanceAuditId,
        PolicyActivationId, PolicyApprovalId, PolicyBundleGeneration, PolicyIdempotencyKey,
        PolicyRevisionId, ProfileArtifactId, PromotionPermitId, SchemaVersion, UserId,
    },
};

/// Overall system status reported by the health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub entry_authorization_policy: EntryAuthorizationPolicy,
    pub uptime_secs: u64,
    pub active_markets: u32,
    /// Market-catalog warmup state; report generation is gated until `Ready`.
    pub catalog: CatalogState,
    /// Authoritative operator lifecycle for report and optional execution modes.
    pub operational_phase: OperationalPhase,
    /// CLOB websocket market-data readiness snapshot.
    pub market_data: MarketDataConnectivity,
    /// Kill-switch projection from the atomic runtime-control snapshot.
    pub kill_switch: KillSwitchView,
    /// Lightweight auto-execution recovery playbook summary.
    pub execution_recovery: ExecutionRecoverySummary,
    pub checked_at: DateTime<Utc>,
}

/// Health check results for all subsystems.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthReport {
    pub overall_healthy: bool,
    pub checks: Vec<SubsystemHealth>,
    pub checked_at: DateTime<Utc>,
}

/// Outcome of a single subsystem health probe.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum SubsystemCheckStatus {
    Healthy,
    Unhealthy,
    Skipped { reason: String },
}

/// Health status of a single subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubsystemHealth {
    pub name: String,
    pub status: SubsystemCheckStatus,
    pub latency_ms: Option<u64>,
    pub detail: Option<String>,
}

impl SubsystemHealth {
    /// Whether this probe counts toward `HealthReport::overall_healthy`.
    #[must_use]
    pub const fn counts_toward_overall(&self) -> bool {
        !matches!(self.status, SubsystemCheckStatus::Skipped { .. })
    }

    /// Legacy-style healthy flag for metrics and quick checks.
    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        matches!(self.status, SubsystemCheckStatus::Healthy)
    }

    #[must_use]
    pub fn healthy(name: impl Into<String>, latency_ms: Option<u64>) -> Self {
        Self {
            name: name.into(),
            status: SubsystemCheckStatus::Healthy,
            latency_ms,
            detail: None,
        }
    }

    #[must_use]
    pub fn unhealthy(
        name: impl Into<String>,
        latency_ms: Option<u64>,
        detail: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            status: SubsystemCheckStatus::Unhealthy,
            latency_ms,
            detail: Some(detail.into()),
        }
    }

    #[must_use]
    pub fn skipped(name: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            status: SubsystemCheckStatus::Skipped {
                reason: reason.into(),
            },
            latency_ms: None,
            detail: None,
        }
    }
}

impl SystemStatus {
    /// Initial "warming" snapshot used for the very first WebSocket publish and
    /// in tests. The authoritative live projection (real mode + kill-switch +
    /// catalog + market-data + uptime) is built by
    /// [`RuntimeControlPort::system_status`](crate::domain::ports::RuntimeControlPort::system_status).
    #[must_use]
    pub fn bootstrap(entry_authorization_policy: EntryAuthorizationPolicy) -> Self {
        Self {
            entry_authorization_policy,
            uptime_secs: 0,
            active_markets: 0,
            catalog: CatalogState::Warming,
            operational_phase: OperationalPhase::CatalogWarming,
            market_data: MarketDataConnectivity {
                ready: false,
                last_message_age_ms: None,
                ws_shards: WsShardConnectivity {
                    total: 0,
                    disconnected: 0,
                    oldest_disconnected_secs: None,
                    connected_ratio_bps: 0,
                },
            },
            kill_switch: KillSwitchView {
                state: KillSwitchState::Closed,
                requires_operator_ack: false,
                revision: 0,
                last_reason: "authoritative control state is not loaded".to_owned(),
                changed_by: "system".to_owned(),
                changed_at: Utc::now(),
            },
            execution_recovery: ExecutionRecoverySummary {
                has_unresolvable_reconciliation: false,
                unresolvable_count: 0,
                kill_switch_requires_ack: false,
                kill_switch_state: KillSwitchState::Closed,
                entry_authorization_policy,
                policy_automatic_blocked: false,
                next_steps: Vec::new(),
            },
            checked_at: Utc::now(),
        }
    }
}

impl HealthReport {
    /// Recompute aggregate health from non-skipped subsystem probes.
    #[must_use]
    pub fn from_checks(checks: Vec<SubsystemHealth>, checked_at: DateTime<Utc>) -> Self {
        let overall_healthy = checks
            .iter()
            .filter(|check| check.counts_toward_overall())
            .all(SubsystemHealth::is_healthy);
        Self {
            overall_healthy,
            checks,
            checked_at,
        }
    }
}

/// Shutdown progress tracking.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShutdownProgress {
    pub stage: ShutdownStage,
    pub inflight_trades: u32,
    pub pending_flushes: u32,
    pub started_at: DateTime<Utc>,
}

// ── Governed configuration resources ─────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::policy_revision::Entity")]
pub struct PolicyRevisionInfo {
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub schema_version: SchemaVersion,
    pub revision_hash: ContentHash,
    pub document: PolicyDocument,
    pub status: PolicyRevisionStatus,
    pub validation_evidence: Option<PolicyValidationEvidence>,
    pub validated_at: Option<DateTime<Utc>>,
    pub preflight_token_hash: Option<ContentHash>,
    pub preflight_expires_at: Option<DateTime<Utc>>,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(PolicyRevisionInfo, policy_revision::Model, {
    policy_revision_id, resource_kind, schema_version, revision_hash, document, status,
    validation_evidence, validated_at, preflight_token_hash, preflight_expires_at,
    created_by_kind, created_by_user_id, created_by_label, reason, created_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_revision::ActiveModel")]
pub struct NewPolicyRevision {
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub schema_version: SchemaVersion,
    pub revision_hash: ContentHash,
    pub document: PolicyDocument,
    pub status: PolicyRevisionStatus,
    pub validation_evidence: Option<PolicyValidationEvidence>,
    pub validated_at: Option<DateTime<Utc>>,
    pub preflight_token_hash: Option<ContentHash>,
    pub preflight_expires_at: Option<DateTime<Utc>>,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::policy_approval::Entity")]
pub struct PolicyApprovalInfo {
    pub policy_approval_id: PolicyApprovalId,
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub revision_hash: ContentHash,
    pub validation_subject: Option<PolicyValidationSubject>,
    pub decision: PolicyApprovalDecision,
    pub decided_by_kind: PolicyActorKind,
    pub decided_by_user_id: Option<UserId>,
    pub decided_by_label: String,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// One entry in the globally ordered Config governance activity ledger.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigActivityInfo {
    Revision(Box<PolicyRevisionInfo>),
    Approval(PolicyApprovalInfo),
    Activation(PolicyActivationInfo),
}

info_from_model!(PolicyApprovalInfo, policy_approval::Model, {
    policy_approval_id, policy_revision_id, resource_kind, revision_hash, validation_subject, decision,
    decided_by_kind, decided_by_user_id, decided_by_label, reason, decided_at, expires_at,
    created_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_approval::ActiveModel")]
pub struct NewPolicyApproval {
    pub policy_approval_id: PolicyApprovalId,
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub revision_hash: ContentHash,
    pub validation_subject: Option<PolicyValidationSubject>,
    pub decision: PolicyApprovalDecision,
    pub decided_by_kind: PolicyActorKind,
    pub decided_by_user_id: Option<UserId>,
    pub decided_by_label: String,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

/// Governed approval command. The repository resolves and freezes the exact
/// revision hash in the same transaction instead of trusting a caller copy.
#[derive(Debug, Clone)]
pub struct RecordPolicyApproval {
    pub policy_approval_id: PolicyApprovalId,
    pub policy_revision_id: PolicyRevisionId,
    pub resource_kind: ConfigResourceKind,
    pub decision: PolicyApprovalDecision,
    pub decided_by_kind: PolicyActorKind,
    pub decided_by_user_id: Option<UserId>,
    pub decided_by_label: String,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecisionPolicySnapshotInfo {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub snapshot: DecisionPolicySnapshot,
    pub recommendation_policy_revision_id: PolicyRevisionId,
    pub execution_risk_policy_revision_id: PolicyRevisionId,
    pub model_routing_revision_id: PolicyRevisionId,
    pub report_schedule_revision_id: PolicyRevisionId,
    pub operations_policy_revision_id: PolicyRevisionId,
    pub execution_authorization_policy_revision_id: PolicyRevisionId,
    pub source: DecisionPolicySnapshotSource,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

/// Lightweight, single-row projection for selecting an immutable policy bundle.
///
/// This deliberately excludes the profile documents: option lists need stable
/// identity and lineage only, so loading or decoding the full snapshot would
/// add work without changing the selection contract.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::decision_policy_snapshot::Entity")]
pub struct DecisionPolicySnapshotOptionInfo {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub recommendation_policy_revision_id: PolicyRevisionId,
    pub execution_risk_policy_revision_id: PolicyRevisionId,
    pub model_routing_revision_id: PolicyRevisionId,
    pub report_schedule_revision_id: PolicyRevisionId,
    pub operations_policy_revision_id: PolicyRevisionId,
    pub execution_authorization_policy_revision_id: PolicyRevisionId,
    pub source: DecisionPolicySnapshotSource,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::decision_policy_snapshot::ActiveModel")]
pub struct NewDecisionPolicySnapshot {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub snapshot: DecisionPolicySnapshotDocument,
    pub recommendation_policy_revision_id: PolicyRevisionId,
    pub execution_risk_policy_revision_id: PolicyRevisionId,
    pub model_routing_revision_id: PolicyRevisionId,
    pub report_schedule_revision_id: PolicyRevisionId,
    pub operations_policy_revision_id: PolicyRevisionId,
    pub execution_authorization_policy_revision_id: PolicyRevisionId,
    pub source: DecisionPolicySnapshotSource,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::policy_profile_artifact::Entity")]
pub struct PolicyProfileArtifactInfo {
    pub profile_artifact_id: ProfileArtifactId,
    pub kind: ProfileArtifactKind,
    pub schema_version: SchemaVersion,
    pub document: PolicyProfileDocument,
    pub content_hash: ContentHash,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    PolicyProfileArtifactInfo,
    policy_profile_artifact::Model,
    {
        profile_artifact_id,
        kind,
        schema_version,
        document,
        content_hash,
        created_by_kind,
        created_by_user_id,
        created_by_label,
        reason,
        created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_profile_artifact::ActiveModel")]
pub struct NewPolicyProfileArtifact {
    pub profile_artifact_id: ProfileArtifactId,
    pub kind: ProfileArtifactKind,
    pub schema_version: SchemaVersion,
    pub document: PolicyProfileDocument,
    pub content_hash: ContentHash,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::policy_activation::Entity")]
pub struct PolicyActivationInfo {
    pub bundle_generation: PolicyBundleGeneration,
    pub expected_bundle_generation: PolicyBundleGeneration,
    pub policy_activation_id: PolicyActivationId,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_approval_id: PolicyApprovalId,
    pub activated_at: DateTime<Utc>,
    pub activated_by_kind: PolicyActorKind,
    pub activated_by_user_id: Option<UserId>,
    pub activated_by_label: String,
    pub reason: String,
    pub activation_kind: PolicyActivationKind,
    pub expected_active_revision_id: Option<PolicyRevisionId>,
    pub previous_policy_revision_id: Option<PolicyRevisionId>,
    pub rollback_target_revision_id: Option<PolicyRevisionId>,
    pub preflight_token_hash: ContentHash,
    pub idempotency_key: PolicyIdempotencyKey,
    pub activation_request_hash: ContentHash,
    pub audit_event_id: AuditEventId,
    pub promotion_permit_id: Option<PromotionPermitId>,
    pub promotion_transaction_hash: Option<ContentHash>,
    pub model_governance_audit_id: Option<ModelGovernanceAuditId>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(PolicyActivationInfo, policy_activation::Model, {
    bundle_generation, expected_bundle_generation, policy_activation_id, resource_kind,
    policy_revision_id,
    decision_policy_snapshot_id,
    policy_approval_id, activated_at, activated_by_kind, activated_by_user_id,
    activated_by_label, reason, activation_kind,
    expected_active_revision_id, previous_policy_revision_id, rollback_target_revision_id,
    preflight_token_hash, idempotency_key, activation_request_hash, audit_event_id,
    promotion_permit_id, promotion_transaction_hash, model_governance_audit_id, created_at,
});

/// Latest activation and exact immutable revision for one policy resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivePolicyResourceInfo {
    pub activation: PolicyActivationInfo,
    pub revision: PolicyRevisionInfo,
}

/// One resource row from the DB-authoritative Config inventory statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResourceInventoryRow {
    pub resource_kind: ConfigResourceKind,
    pub active_revision_id: Option<PolicyRevisionId>,
    pub active_revision_hash: Option<ContentHash>,
    pub last_activated_at: Option<DateTime<Utc>>,
    pub pending_approval_count: u64,
}

/// Consistent Config inventory read from one `PostgreSQL` statement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigResourceInventoryInfo {
    pub bundle_generation: PolicyBundleGeneration,
    pub active_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub active_snapshot_hash: Option<ContentHash>,
    pub resources: Vec<ConfigResourceInventoryRow>,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_activation::ActiveModel")]
pub struct NewPolicyActivation {
    pub bundle_generation: PolicyBundleGeneration,
    pub expected_bundle_generation: PolicyBundleGeneration,
    pub policy_activation_id: PolicyActivationId,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_approval_id: PolicyApprovalId,
    pub activated_by_kind: PolicyActorKind,
    pub activated_by_user_id: Option<UserId>,
    pub activated_by_label: String,
    pub reason: String,
    pub activation_kind: PolicyActivationKind,
    pub expected_active_revision_id: Option<PolicyRevisionId>,
    pub previous_policy_revision_id: Option<PolicyRevisionId>,
    pub rollback_target_revision_id: Option<PolicyRevisionId>,
    pub preflight_token_hash: ContentHash,
    pub idempotency_key: PolicyIdempotencyKey,
    pub activation_request_hash: ContentHash,
    pub audit_event_id: AuditEventId,
}

/// Insert payload for one first-champion model-route bootstrap activation.
///
/// Bootstrap links the generic policy ledger to its WORM model-governance
/// audit without manufacturing a promotion permit or promotion hash.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_activation::ActiveModel")]
pub struct NewModelBootstrapActivation {
    pub bundle_generation: PolicyBundleGeneration,
    pub expected_bundle_generation: PolicyBundleGeneration,
    pub policy_activation_id: PolicyActivationId,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_approval_id: PolicyApprovalId,
    pub activated_by_kind: PolicyActorKind,
    pub activated_by_user_id: Option<UserId>,
    pub activated_by_label: String,
    pub reason: String,
    pub activation_kind: PolicyActivationKind,
    pub expected_active_revision_id: PolicyRevisionId,
    pub previous_policy_revision_id: PolicyRevisionId,
    pub rollback_target_revision_id: Option<PolicyRevisionId>,
    pub preflight_token_hash: ContentHash,
    pub idempotency_key: PolicyIdempotencyKey,
    pub activation_request_hash: ContentHash,
    pub audit_event_id: AuditEventId,
    pub model_governance_audit_id: ModelGovernanceAuditId,
}

/// Insert payload for one model-route promotion activation.
///
/// The subtype bindings are required here even though the shared table stores
/// them as nullable columns for non-promotion activation kinds.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_activation::ActiveModel")]
pub struct NewModelPromotionActivation {
    pub bundle_generation: PolicyBundleGeneration,
    pub expected_bundle_generation: PolicyBundleGeneration,
    pub policy_activation_id: PolicyActivationId,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub policy_approval_id: PolicyApprovalId,
    pub activated_at: DateTime<Utc>,
    pub activated_by_kind: PolicyActorKind,
    pub activated_by_user_id: UserId,
    pub activated_by_label: String,
    pub reason: String,
    pub activation_kind: PolicyActivationKind,
    pub expected_active_revision_id: PolicyRevisionId,
    pub previous_policy_revision_id: PolicyRevisionId,
    pub rollback_target_revision_id: Option<PolicyRevisionId>,
    pub preflight_token_hash: ContentHash,
    pub idempotency_key: PolicyIdempotencyKey,
    pub activation_request_hash: ContentHash,
    pub audit_event_id: AuditEventId,
    pub promotion_permit_id: PromotionPermitId,
    pub promotion_transaction_hash: ContentHash,
    pub model_governance_audit_id: ModelGovernanceAuditId,
}

/// Exact audit projection inserted from a committed activation row.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::policy_activation_audit::ActiveModel",
    exhaustive
)]
pub struct NewPolicyActivationAudit {
    pub audit_event_id: AuditEventId,
    pub policy_activation_id: PolicyActivationId,
    pub bundle_generation: PolicyBundleGeneration,
    pub resource_kind: ConfigResourceKind,
    pub policy_revision_id: PolicyRevisionId,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub activation_request_hash: ContentHash,
    pub actor_kind: PolicyActorKind,
    pub actor_user_id: Option<UserId>,
    pub actor_label: String,
    pub reason: String,
    pub occurred_at: DateTime<Utc>,
    pub promotion_permit_id: Option<PromotionPermitId>,
    pub promotion_transaction_hash: Option<ContentHash>,
    pub model_governance_audit_id: Option<ModelGovernanceAuditId>,
    pub created_at: DateTime<Utc>,
}

/// Exact durable-outbox projection inserted from a committed activation row.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::policy_activation_event_outbox::ActiveModel",
    exhaustive
)]
pub struct NewPolicyActivationEventOutbox {
    pub audit_event_id: AuditEventId,
    pub policy_activation_id: PolicyActivationId,
    pub bundle_generation: PolicyBundleGeneration,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub promotion_permit_id: Option<PromotionPermitId>,
    pub promotion_transaction_hash: Option<ContentHash>,
    pub model_governance_audit_id: Option<ModelGovernanceAuditId>,
    pub created_at: DateTime<Utc>,
}

/// Outcome of an atomic policy activation transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PolicyActivationOutcome {
    Committed,
    ExactReplay,
}

/// Exact durable result returned to the applicator after commit or replay.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyActivationCommit {
    pub activation: PolicyActivationInfo,
    pub bundle: ActivePolicyBundle,
    pub outcome: PolicyActivationOutcome,
}
