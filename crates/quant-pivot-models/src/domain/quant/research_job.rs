//! Durable research-job persistence DTOs + progress sink.

use chrono::{DateTime, Utc};
use quant_pivot_error::feedback::FeedbackError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    enums::quant::{FeedbackStage, ResearchJobKind, ResearchJobResultKind, ResearchJobStatus},
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, DatasetCoverage, DecisionPolicySnapshotId, FeedbackCycleId,
        ModelSpecId, ResearchJobError, ResearchJobId, ResearchJobParams, ResearchJobProgress,
        RoleCode, WorkerId,
    },
};

const FEEDBACK_STAGE_JOB_VERSION: u32 = 1;
const FEEDBACK_STAGE_JOB_DOMAIN: &str = "quant-pivot/feedback-stage-job";

/// Namespace-tagged terminal artifact reference produced by a research job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchJobResultRef {
    pub kind: ResearchJobResultKind,
    pub id: Uuid,
}

/// Content-addressed object identity returned by artifact-producing jobs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResearchJobArtifactRef {
    pub uri: ArtifactUri,
    pub content_hash: ContentHash,
}

/// One valid terminal transition command for a leased research job.
///
/// Private fields and terminal-specific constructors make an active status, a
/// successful error payload, or a failed result payload unrepresentable before
/// the repository applies its lease-owner compare-and-set.
#[derive(Debug)]
pub struct ResearchJobFinalization {
    status: ResearchJobStatus,
    result: Option<ResearchJobResultRef>,
    artifact: Option<ResearchJobArtifactRef>,
    error: Option<ResearchJobError>,
    coverage: Option<DatasetCoverage>,
}

impl ResearchJobFinalization {
    /// Build a successful terminal command with its optional typed outputs.
    #[must_use]
    pub const fn succeeded(
        result: Option<ResearchJobResultRef>,
        artifact: Option<ResearchJobArtifactRef>,
        coverage: Option<DatasetCoverage>,
    ) -> Self {
        Self {
            status: ResearchJobStatus::Succeeded,
            result,
            artifact,
            error: None,
            coverage,
        }
    }

    /// Build a failed terminal command with its required structured error.
    #[must_use]
    pub const fn failed(error: ResearchJobError) -> Self {
        Self {
            status: ResearchJobStatus::Failed,
            result: None,
            artifact: None,
            error: Some(error),
            coverage: None,
        }
    }

    /// Build a cancelled terminal command with its required structured error.
    #[must_use]
    pub const fn cancelled(error: ResearchJobError) -> Self {
        Self {
            status: ResearchJobStatus::Cancelled,
            result: None,
            artifact: None,
            error: Some(error),
            coverage: None,
        }
    }

    /// Return the persisted terminal lifecycle status.
    #[must_use]
    pub const fn status(&self) -> ResearchJobStatus {
        self.status
    }

    /// Decompose the command into the canonical persistence columns.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        ResearchJobStatus,
        Option<ResearchJobResultRef>,
        Option<ResearchJobArtifactRef>,
        Option<ResearchJobError>,
        Option<DatasetCoverage>,
    ) {
        (
            self.status,
            self.result,
            self.artifact,
            self.error,
            self.coverage,
        )
    }
}

/// Canonical identity of one feedback-stage root job or explicit retry.
///
/// The root id is a pure function of `(cycle, stage)`. A retry additionally
/// commits its direct parent, so repeating the same retry request converges on
/// one child while a subsequent retry forms a new, auditable lineage node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeedbackStageJobIdentity {
    feedback_cycle_id: FeedbackCycleId,
    feedback_stage: FeedbackStage,
    parent_job_id: Option<ResearchJobId>,
    job_id: ResearchJobId,
}

#[derive(Serialize)]
struct FeedbackStageJobDocument {
    format_version: u32,
    feedback_cycle_id: FeedbackCycleId,
    feedback_stage: FeedbackStage,
    parent_job_id: Option<ResearchJobId>,
}

impl FeedbackStageJobIdentity {
    /// Freeze the deterministic root job for one executable feedback stage.
    pub fn try_root(
        feedback_cycle_id: FeedbackCycleId,
        feedback_stage: FeedbackStage,
    ) -> Result<Self, FeedbackError> {
        Self::try_new(feedback_cycle_id, feedback_stage, None)
    }

    /// Freeze the deterministic child produced by explicitly retrying `parent_job_id`.
    pub fn try_retry(
        feedback_cycle_id: FeedbackCycleId,
        feedback_stage: FeedbackStage,
        parent_job_id: ResearchJobId,
    ) -> Result<Self, FeedbackError> {
        Self::try_new(feedback_cycle_id, feedback_stage, Some(parent_job_id))
    }

    fn try_new(
        feedback_cycle_id: FeedbackCycleId,
        feedback_stage: FeedbackStage,
        parent_job_id: Option<ResearchJobId>,
    ) -> Result<Self, FeedbackError> {
        if feedback_stage == FeedbackStage::Trigger {
            return Err(FeedbackError::InvalidJobIdentity {
                detail: "trigger is timeline evidence and cannot own a ResearchJob".to_owned(),
            });
        }
        let document = FeedbackStageJobDocument {
            format_version: FEEDBACK_STAGE_JOB_VERSION,
            feedback_cycle_id,
            feedback_stage,
            parent_job_id,
        };
        let identity_hash = CanonicalDigest::content_hash_typed(
            FEEDBACK_STAGE_JOB_DOMAIN,
            FEEDBACK_STAGE_JOB_VERSION,
            &document,
        )?;
        Ok(Self {
            feedback_cycle_id,
            feedback_stage,
            parent_job_id,
            job_id: ResearchJobId::from_feedback_identity_hash(&identity_hash),
        })
    }

    #[must_use]
    pub const fn job_id(self) -> ResearchJobId {
        self.job_id
    }

    #[must_use]
    pub const fn feedback_cycle_id(self) -> FeedbackCycleId {
        self.feedback_cycle_id
    }

    #[must_use]
    pub const fn feedback_stage(self) -> FeedbackStage {
        self.feedback_stage
    }

    #[must_use]
    pub const fn parent_job_id(self) -> Option<ResearchJobId> {
        self.parent_job_id
    }
}

/// Durable research-job ledger row (full projection).
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_research_job::Entity")]
pub struct ResearchJobInfo {
    pub job_id: ResearchJobId,
    pub feedback_cycle_id: Option<FeedbackCycleId>,
    pub feedback_stage: Option<FeedbackStage>,
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
    /// Canonical object location for artifact-producing result kinds.
    pub result_artifact_uri: Option<ArtifactUri>,
    /// Exact bytes hash stored at `result_artifact_uri`.
    pub result_artifact_hash: Option<ContentHash>,
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
    feedback_cycle_id,
    feedback_stage,
    kind,
    status,
    model_spec_id,
    decision_policy_snapshot_id,
    params_json,
    progress_json,
    result_kind,
    result_ref,
    result_artifact_uri,
    result_artifact_hash,
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

    /// Return the terminal object identity only when both columns are present.
    #[must_use]
    pub fn result_artifact(&self) -> Option<ResearchJobArtifactRef> {
        self.result_artifact_uri
            .clone()
            .zip(self.result_artifact_hash)
            .map(|(uri, content_hash)| ResearchJobArtifactRef { uri, content_hash })
    }

    /// Recompute and verify feedback-stage identity while accepting explicit standalone rows.
    pub fn validate_identity(&self) -> Result<(), FeedbackError> {
        validate_identity(
            self.job_id,
            self.feedback_cycle_id,
            self.feedback_stage,
            self.parent_job_id,
        )
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
    pub feedback_cycle_id: Option<FeedbackCycleId>,
    pub feedback_stage: Option<FeedbackStage>,
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

impl NewResearchJob {
    /// Bind a queued job to one canonical feedback-stage root/retry identity.
    pub fn try_bind_feedback(
        mut self,
        identity: FeedbackStageJobIdentity,
    ) -> Result<Self, FeedbackError> {
        if self.feedback_cycle_id.is_some() || self.feedback_stage.is_some() {
            return Err(FeedbackError::InvalidJobIdentity {
                detail: "research job is already feedback-bound".to_owned(),
            });
        }
        if self.parent_job_id.is_some() && self.parent_job_id != identity.parent_job_id() {
            return Err(FeedbackError::InvalidJobIdentity {
                detail: "pre-existing parent does not match feedback retry identity".to_owned(),
            });
        }
        self.job_id = identity.job_id();
        self.feedback_cycle_id = Some(identity.feedback_cycle_id());
        self.feedback_stage = Some(identity.feedback_stage());
        self.parent_job_id = identity.parent_job_id();
        self.validate_identity()?;
        Ok(self)
    }

    /// Verify all caller-owned enqueue fields before any repository mutation.
    pub fn validate_enqueue(&self) -> Result<(), FeedbackError> {
        if self.kind != self.params_json.kind() {
            return Err(FeedbackError::InvalidJobContract {
                detail: format!(
                    "job kind {} does not match params kind {}",
                    self.kind,
                    self.params_json.kind()
                ),
            });
        }
        if self.status != ResearchJobStatus::Queued {
            return Err(FeedbackError::InvalidJobContract {
                detail: format!("new research job must be queued, got {}", self.status),
            });
        }
        if self.recovery_attempt != 0 {
            return Err(FeedbackError::InvalidJobContract {
                detail: format!(
                    "new research job recovery_attempt must be zero, got {}",
                    self.recovery_attempt
                ),
            });
        }
        if self.max_recovery_attempts < 0 {
            return Err(FeedbackError::InvalidJobContract {
                detail: format!(
                    "max_recovery_attempts cannot be negative, got {}",
                    self.max_recovery_attempts
                ),
            });
        }
        if self.parent_job_id == Some(self.job_id) {
            return Err(FeedbackError::InvalidJobContract {
                detail: "research job cannot be its own retry parent".to_owned(),
            });
        }
        self.validate_identity()
    }

    /// Verify the deterministic feedback identity, accepting explicit standalone jobs.
    pub fn validate_identity(&self) -> Result<(), FeedbackError> {
        validate_identity(
            self.job_id,
            self.feedback_cycle_id,
            self.feedback_stage,
            self.parent_job_id,
        )
    }

    /// Verify an existing mutable ledger row still represents this exact enqueue contract.
    #[must_use]
    pub fn accepts(&self, existing: &ResearchJobInfo) -> bool {
        self.job_id == existing.job_id
            && self.feedback_cycle_id == existing.feedback_cycle_id
            && self.feedback_stage == existing.feedback_stage
            && self.kind == existing.kind
            && self.model_spec_id == existing.model_spec_id
            && self.decision_policy_snapshot_id == existing.decision_policy_snapshot_id
            && self.params_json == existing.params_json
            && self.requested_by == existing.requested_by
            && self.acting_role == existing.acting_role
            && self.parent_job_id == existing.parent_job_id
            && self.max_recovery_attempts == existing.max_recovery_attempts
    }
}

fn validate_identity(
    job_id: ResearchJobId,
    feedback_cycle_id: Option<FeedbackCycleId>,
    feedback_stage: Option<FeedbackStage>,
    parent_job_id: Option<ResearchJobId>,
) -> Result<(), FeedbackError> {
    let identity = match (feedback_cycle_id, feedback_stage) {
        (None, None) => return Ok(()),
        (Some(feedback_cycle_id), Some(feedback_stage)) => {
            if let Some(parent_job_id) = parent_job_id {
                FeedbackStageJobIdentity::try_retry(
                    feedback_cycle_id,
                    feedback_stage,
                    parent_job_id,
                )?
            } else {
                FeedbackStageJobIdentity::try_root(feedback_cycle_id, feedback_stage)?
            }
        }
        _ => {
            return Err(FeedbackError::InvalidJobIdentity {
                detail: "feedback_cycle_id and feedback_stage must be both present or both absent"
                    .to_owned(),
            });
        }
    };
    if identity.job_id() != job_id {
        return Err(FeedbackError::InvalidJobIdentity {
            detail: format!(
                "job id {job_id} does not match canonical feedback identity {}",
                identity.job_id()
            ),
        });
    }
    Ok(())
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
