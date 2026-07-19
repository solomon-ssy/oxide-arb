//! System status, lifecycle, config, accounting, and reporting domain models.

use crate::{
    domain::{
        ExecutionRecoverySummary,
        governance::kill_switch::KillSwitchView,
        lifecycle::{MarketDataConnectivity, OperationalPhase, WsShardConnectivity},
        ports::runtime_control::CatalogState,
    },
    entities::{
        policy_activation, policy_approval, policy_revision, system_production_baseline,
        system_runtime_state,
    },
    enums::{
        execution::KillSwitchState,
        quant::QuantRuntimeMode,
        runtime_config::{
            ConfigResourceKind, DecisionPolicySnapshotSource, PolicyActivationKind,
            PolicyActorKind, PolicyApprovalDecision, PolicyRevisionStatus,
        },
        system::{BootstrapPhase, ShutdownStage},
    },
    runtime_config::{
        DecisionPolicySnapshot, PolicyDocument, PolicyValidationEvidence, ProductionSealEvidence,
    },
    types::{
        AuditEventId, BootstrapTransitionId, BuildCommitHash, ContentHash,
        DecisionPolicySnapshotId, DeploymentEnvironment, PolicyActivationId, PolicyApprovalId,
        PolicyIdempotencyKey, PolicyRevisionId, ProductionBaselineId, SchemaVersion, UserId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// Overall system status reported by the health endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatus {
    pub quant_runtime_mode: QuantRuntimeMode,
    pub uptime_secs: u64,
    pub active_markets: u32,
    /// Market-catalog warmup state; report generation is gated until `Ready`.
    pub catalog: CatalogState,
    /// Authoritative operator lifecycle for report and optional execution modes.
    pub operational_phase: OperationalPhase,
    /// CLOB websocket market-data readiness snapshot.
    pub market_data: MarketDataConnectivity,
    /// Operational kill-switch projection (real `system_kill_switch` state).
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
    pub fn bootstrap(quant_runtime_mode: QuantRuntimeMode) -> Self {
        Self {
            quant_runtime_mode,
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
                state: KillSwitchState::ReportOnlyForced,
                requires_operator_ack: true,
                last_reason: "authoritative control state is not loaded".to_owned(),
                changed_by: "system".to_owned(),
                changed_at: Utc::now(),
            },
            execution_recovery: ExecutionRecoverySummary {
                has_unresolvable_reconciliation: false,
                unresolvable_count: 0,
                kill_switch_requires_ack: true,
                kill_switch_state: KillSwitchState::ReportOnlyForced,
                quant_runtime_mode,
                auto_execution_blocked: false,
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
    pub decision: PolicyApprovalDecision,
    pub decided_by_kind: PolicyActorKind,
    pub decided_by_user_id: Option<UserId>,
    pub decided_by_label: String,
    pub reason: String,
    pub decided_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(PolicyApprovalInfo, policy_approval::Model, {
    policy_approval_id, policy_revision_id, resource_kind, revision_hash, decision,
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

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::decision_policy_snapshot::Entity")]
pub struct DecisionPolicySnapshotInfo {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub snapshot: DecisionPolicySnapshot,
    pub recommendation_policy_revision_id: PolicyRevisionId,
    pub execution_risk_policy_revision_id: PolicyRevisionId,
    pub model_routing_revision_id: PolicyRevisionId,
    pub report_schedule_revision_id: PolicyRevisionId,
    pub operational_control_revision_id: PolicyRevisionId,
    pub execution_authorization_revision_id: PolicyRevisionId,
    pub source: DecisionPolicySnapshotSource,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(DecisionPolicySnapshotInfo, crate::entities::decision_policy_snapshot::Model, {
    decision_policy_snapshot_id, snapshot_hash, snapshot,
    recommendation_policy_revision_id, execution_risk_policy_revision_id,
    model_routing_revision_id, report_schedule_revision_id,
    operational_control_revision_id, execution_authorization_revision_id,
    source, created_by_kind, created_by_user_id, created_by_label, reason, created_at,
});

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::decision_policy_snapshot::ActiveModel")]
pub struct NewDecisionPolicySnapshot {
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub snapshot_hash: ContentHash,
    pub snapshot: DecisionPolicySnapshot,
    pub recommendation_policy_revision_id: PolicyRevisionId,
    pub execution_risk_policy_revision_id: PolicyRevisionId,
    pub model_routing_revision_id: PolicyRevisionId,
    pub report_schedule_revision_id: PolicyRevisionId,
    pub operational_control_revision_id: PolicyRevisionId,
    pub execution_authorization_revision_id: PolicyRevisionId,
    pub source: DecisionPolicySnapshotSource,
    pub created_by_kind: PolicyActorKind,
    pub created_by_user_id: Option<UserId>,
    pub created_by_label: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::policy_activation::Entity")]
pub struct PolicyActivationInfo {
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
    pub audit_event_id: Option<AuditEventId>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(PolicyActivationInfo, policy_activation::Model, {
    policy_activation_id, resource_kind, policy_revision_id, decision_policy_snapshot_id,
    policy_approval_id, activated_at, activated_by_kind, activated_by_user_id,
    activated_by_label, reason, activation_kind,
    expected_active_revision_id, previous_policy_revision_id, rollback_target_revision_id,
    preflight_token_hash, idempotency_key, audit_event_id, created_at,
});

/// Latest activation and exact immutable revision for one policy resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivePolicyResourceInfo {
    pub activation: PolicyActivationInfo,
    pub revision: PolicyRevisionInfo,
}

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::policy_activation::ActiveModel")]
pub struct NewPolicyActivation {
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
    pub audit_event_id: Option<AuditEventId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::system_production_baseline::Entity")]
pub struct ProductionBaselineInfo {
    pub production_baseline_id: ProductionBaselineId,
    pub environment: DeploymentEnvironment,
    pub sealed_at: DateTime<Utc>,
    pub sealed_by_kind: PolicyActorKind,
    pub sealed_by_user_id: Option<UserId>,
    pub sealed_by_label: String,
    pub build_commit: BuildCommitHash,
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
    pub policy_bundle_hash: ContentHash,
    pub lifecycle_policy_hash: ContentHash,
    pub evidence: ProductionSealEvidence,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ProductionBaselineInfo,
    system_production_baseline::Model,
    {
        production_baseline_id,
        environment,
        sealed_at,
        sealed_by_kind,
        sealed_by_user_id,
        sealed_by_label,
        build_commit,
        postgres_schema_fingerprint,
        clickhouse_schema_fingerprint,
        policy_bundle_hash,
        lifecycle_policy_hash,
        evidence,
        created_at,
    }
);

#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::system_production_baseline::ActiveModel")]
pub struct NewProductionBaseline {
    pub production_baseline_id: ProductionBaselineId,
    pub environment: DeploymentEnvironment,
    pub sealed_at: DateTime<Utc>,
    pub sealed_by_kind: PolicyActorKind,
    pub sealed_by_user_id: Option<UserId>,
    pub sealed_by_label: String,
    pub build_commit: BuildCommitHash,
    pub postgres_schema_fingerprint: ContentHash,
    pub clickhouse_schema_fingerprint: ContentHash,
    pub policy_bundle_hash: ContentHash,
    pub lifecycle_policy_hash: ContentHash,
    pub evidence: ProductionSealEvidence,
}

// ── System runtime state (operational control singleton) ─────────────

/// DB row projection for the `system_runtime_state` singleton.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::system_runtime_state::Entity")]
pub struct SystemRuntimeStateInfo {
    pub id: i32,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub bootstrap_phase: BootstrapPhase,
    pub bootstrap_contract_version: i32,
    pub state_revision: i64,
    pub changed_by: String,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    SystemRuntimeStateInfo,
    system_runtime_state::Model,
    {
        id,
        quant_runtime_mode,
        bootstrap_phase,
        bootstrap_contract_version,
        state_revision,
        changed_by,
        reason,
        changed_at,
        updated_at,
    }
);

/// Upsert payload for the runtime-mode singleton (`id` is always the singleton key).
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::system_runtime_state::ActiveModel")]
pub struct UpsertSystemRuntimeState {
    pub id: i32,
    pub quant_runtime_mode: QuantRuntimeMode,
    pub bootstrap_phase: BootstrapPhase,
    pub bootstrap_contract_version: i32,
    pub state_revision: i64,
    pub changed_by: String,
    pub reason: String,
    pub changed_at: DateTime<Utc>,
}

/// Append-only bootstrap transition insert payload.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::system_bootstrap_transition::ActiveModel",
    exhaustive
)]
pub struct NewSystemBootstrapTransition {
    pub bootstrap_transition_id: BootstrapTransitionId,
    pub bootstrap_contract_version: i32,
    pub state_revision: i64,
    pub from_phase: BootstrapPhase,
    pub to_phase: BootstrapPhase,
    pub actor: String,
    pub acting_role: Option<String>,
    pub reason: String,
    pub report_only_forced_ack: bool,
    pub occurred_at: DateTime<Utc>,
}

/// Repository command for the one permitted operator activation transition.
#[derive(Debug, Clone)]
pub struct ActivateBootstrapState {
    pub bootstrap_contract_version: i32,
    pub expected_state_revision: i64,
    pub actor: String,
    pub acting_role: String,
    pub reason: String,
    pub report_only_forced_ack: bool,
}

#[derive(Debug, Clone)]
pub struct BootstrapActivationInfo {
    pub state: SystemRuntimeStateInfo,
}
