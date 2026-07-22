//! Durable research-job persistence DTOs + progress sink.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    enums::quant::{ResearchJobKind, ResearchJobResultKind, ResearchJobStatus},
    types::{
        DatasetCoverage, DecisionPolicySnapshotId, ModelSpecId, ResearchJobError, ResearchJobId,
        ResearchJobParams, ResearchJobProgress, RoleCode, WorkerId,
    },
};

/// Namespace-tagged terminal artifact reference produced by a research job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchJobResultRef {
    pub kind: ResearchJobResultKind,
    pub id: Uuid,
}

/// Durable research-job ledger row (full projection).
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_research_job::Entity")]
pub struct ResearchJobInfo {
    pub job_id: ResearchJobId,
    pub kind: ResearchJobKind,
    pub status: ResearchJobStatus,
    pub model_spec_id: Option<ModelSpecId>,
    pub decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    /// Frozen request body (includes `reason` + pre-assigned result id).
    pub params_json: ResearchJobParams,
    /// Live progress snapshot (phase + processed/total + pct); `None` until first tick.
    pub progress_json: Option<ResearchJobProgress>,
    /// Namespace discriminator for `result_ref`.
    pub result_kind: Option<ResearchJobResultKind>,
    /// Terminal result id; valid only together with `result_kind`.
    pub result_ref: Option<Uuid>,
    /// Structured failure payload (`code` + `message`), on terminal `failed`.
    pub error_json: Option<ResearchJobError>,
    /// Build/backtest coverage diagnostics, mirrored for quick UI access.
    pub coverage_json: Option<DatasetCoverage>,
    pub requested_by: Option<String>,
    pub acting_role: RoleCode,
    /// Parent job this row was retried from (retry lineage).
    pub parent_job_id: Option<ResearchJobId>,
    pub recovery_attempt: i32,
    pub max_recovery_attempts: i32,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub heartbeat_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(ResearchJobInfo, crate::entities::quant_research_job::Model, {
    job_id,
    kind,
    status,
    model_spec_id,
    decision_policy_snapshot_id,
    params_json,
    progress_json,
    result_kind,
    result_ref,
    error_json,
    coverage_json,
    requested_by,
    acting_role,
    parent_job_id,
    recovery_attempt,
    max_recovery_attempts,
    lease_owner,
    lease_expires_at,
    started_at,
    finished_at,
    heartbeat_at,
    created_at,
    updated_at,
});

impl ResearchJobInfo {
    /// Return the terminal reference only when both persisted columns are present.
    #[must_use]
    pub fn result(&self) -> Option<ResearchJobResultRef> {
        self.result_kind
            .zip(self.result_ref)
            .map(|(kind, id)| ResearchJobResultRef { kind, id })
    }
}

/// Insert payload for `quant_research_job` (enqueue).
///
/// DB-managed `created_at` / `updated_at` are omitted. Lease / progress / result
/// columns start unset (DB `NULL` / defaults); the worker fills them via the
/// repository's single-purpose lease / progress / finalize methods.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_research_job::ActiveModel")]
pub struct NewResearchJob {
    pub job_id: ResearchJobId,
    pub kind: ResearchJobKind,
    pub status: ResearchJobStatus,
    pub model_spec_id: Option<ModelSpecId>,
    pub decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub params_json: ResearchJobParams,
    pub requested_by: Option<String>,
    pub acting_role: RoleCode,
    pub parent_job_id: Option<ResearchJobId>,
    pub recovery_attempt: i32,
    pub max_recovery_attempts: i32,
}

/// A progress channel handed to a long-running research task.
///
/// The task reports coarse progress snapshots (`prefetch → materialize → finalize`
/// phases, cross-section counts) and the implementation decides how to surface
/// them. `report` is **synchronous** and non-blocking by contract so it is safe
/// to call from CPU-bound executor code (which cannot `.await`): the
/// durable worker's sink is a lock-free channel push, coalesced + persisted by
/// the async supervisor, while non-job callers use [`NoopProgressSink`].
pub trait JobProgressSink: Send + Sync {
    /// Report a progress snapshot. Implementations MUST be non-blocking (a
    /// bounded channel push or a no-op) — never any I/O or `.await`.
    fn report(&self, progress: ResearchJobProgress);
}

/// A [`JobProgressSink`] that discards every report (non-job / test callers).
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopProgressSink;

impl JobProgressSink for NoopProgressSink {
    fn report(&self, _progress: ResearchJobProgress) {}
}
