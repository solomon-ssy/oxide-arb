//! Durable fresh-boot orchestration contracts.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use quant_pivot_error::storage::StorageError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromJsonQueryResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    domain::quant::ModelRouteBootstrapPreflight,
    entities::{quant_fresh_boot_run, quant_fresh_boot_run_event},
    enums::quant::{
        FreshBootBlockedReason, FreshBootEventKind, FreshBootRetryReason, FreshBootStage,
        FreshBootStatus,
    },
    hashing::CanonicalDigest,
    runtime_config::BuyModelRoute,
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        FeatureParityRunId, FreshBootRunEventId, FreshBootRunId, ModelSpecId, ModelVersionId,
        PolicyActivationId, PolicyIdempotencyKey, PortfolioScenarioModelArtifactId,
        RecommendationReportId, ReportRunId, ResearchJobId, ResearchProfileArtifactId,
        ResearchProfileRef, ResearchReadinessEvidenceId, ResearchReadinessSource, SourceSliceId,
        TrainingDatasetId, WorkerId,
    },
};

const FRESH_BOOT_EVENT_DOMAIN: &str = "quant-pivot/fresh-boot-run-event";
const FRESH_BOOT_EVENT_VERSION: u32 = 2;
const FRESH_BOOT_IDENTITY_DOMAIN: &str = "quant-pivot/fresh-boot-run";
const FRESH_BOOT_IDENTITY_VERSION: u32 = 1;
pub const FRESH_BOOT_MAX_RETRY_COUNT: i32 = 8;

/// Complete immutable preimage for one fresh-boot run identity.
///
/// The same constructor is used by autonomous seeding and governed
/// supersession so the two control paths cannot mint divergent identities.
#[derive(Debug, Clone, Serialize)]
pub struct FreshBootRunContract {
    pub profile_ref: ResearchProfileRef,
    pub route: BuyModelRoute,
    pub history_plan_id: Uuid,
    pub history_policy_hash: ContentHash,
    pub history_from_block: i64,
    pub history_through_block: i64,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub decision_policy_snapshot_hash: ContentHash,
    pub supersedes_run_id: Option<FreshBootRunId>,
}

impl FreshBootRunContract {
    /// Seal the immutable run projection at one explicit database timeline
    /// instant. No caller may partially initialize an orchestration run.
    pub fn seal(
        self,
        plan_created_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> Result<NewFreshBootRun, StorageError> {
        if self.history_from_block < 0
            || self.history_through_block < self.history_from_block
            || plan_created_at > now
        {
            return Err(StorageError::invariant_violation(
                Some("quant_fresh_boot_run"),
                "fresh-boot contract has an invalid history range or timeline",
            ));
        }
        let identity_hash = CanonicalDigest::content_hash_typed(
            FRESH_BOOT_IDENTITY_DOMAIN,
            FRESH_BOOT_IDENTITY_VERSION,
            &self,
        )
        .map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_fresh_boot_run"),
                format!("fresh-boot identity cannot be sealed: {error}"),
            )
        })?;
        let idempotency_key =
            PolicyIdempotencyKey::from_str(&format!("fresh_boot:{}", identity_hash.hex()))
                .map_err(|error| {
                    StorageError::invariant_violation(
                        Some("quant_fresh_boot_run"),
                        format!("fresh-boot idempotency key is invalid: {error}"),
                    )
                })?;
        Ok(NewFreshBootRun {
            run_id: FreshBootRunId::from_idempotency_hash(&identity_hash),
            supersedes_run_id: self.supersedes_run_id,
            research_profile_artifact_id: ResearchProfileArtifactId::from_profile_ref(
                &self.profile_ref,
            ),
            profile_hash: self.profile_ref.content_hash,
            route: self.route,
            stage: FreshBootStage::AwaitingSourceCoverage,
            status: FreshBootStatus::WaitingEvidence,
            source_coverage_manifest: None,
            source_coverage_hash: None,
            source_slice_id: None,
            source_slice_hash: None,
            decision_policy_snapshot_id: self.decision_policy_snapshot_id,
            model_spec_id: None,
            training_dataset_id: None,
            calibration_dataset_id: None,
            source_model_version_id: None,
            model_version_id: None,
            path_set_id: None,
            calibration_id: None,
            parity_run_id: None,
            scenario_artifact_id: None,
            scenario_artifact_hash: None,
            bootstrap_preflight: None,
            bootstrap_preflight_hash: None,
            active_job_id: None,
            last_job_id: None,
            bootstrap_policy_activation_id: None,
            manual_report_ready_at: None,
            first_report_run_id: None,
            first_report_id: None,
            next_scheduled_report_at: None,
            retry_reason: Some(FreshBootRetryReason::SourceCoverageIncomplete),
            retry_detail: Some("source coverage has not yet been evaluated".to_owned()),
            retry_count: 0,
            next_attempt_at: Some(now),
            blocked_reason: None,
            blocked_detail: None,
            lease_owner: None,
            lease_expires_at: None,
            idempotency_key,
            revision: 0,
            stage_entered_at: now,
            started_at: plan_created_at,
            completed_at: None,
            created_at: now,
            updated_at: now,
        })
    }
}

/// One exact storage binding required by a profile-specific source-coverage seal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FreshBootSourceCoverage {
    pub source: ResearchReadinessSource,
    pub object: String,
    pub earliest_event_time: DateTime<Utc>,
    pub latest_event_time: DateTime<Utc>,
    pub row_count: u64,
}

/// Immutable evidence manifest that authorizes a dataset build for one profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, FromJsonQueryResult)]
#[serde(deny_unknown_fields)]
pub struct FreshBootSourceCoverageManifest {
    pub history_plan_id: Uuid,
    pub history_policy_hash: ContentHash,
    pub availability_policy_hash: ContentHash,
    pub readiness_evidence_id: ResearchReadinessEvidenceId,
    pub source_registry_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub history_from_block: i64,
    pub history_through_block: i64,
    pub requirements: Vec<FreshBootSourceCoverage>,
    pub sealed_at: DateTime<Utc>,
}

impl FreshBootSourceCoverageManifest {
    /// Whether every sealed binding covers the complete model window.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.history_from_block >= 0
            && self.history_through_block >= self.history_from_block
            && self.window_start < self.window_end
            && self.window_end <= self.pit_cutoff
            && self.pit_cutoff <= self.sealed_at
            && !self.requirements.is_empty()
            && self.requirements.windows(2).all(|requirements| {
                (requirements[0].source, requirements[0].object.as_str())
                    < (requirements[1].source, requirements[1].object.as_str())
            })
            && self.requirements.iter().all(|coverage| {
                !coverage.object.trim().is_empty()
                    && coverage.object.trim() == coverage.object
                    && coverage.row_count > 0
                    && coverage.earliest_event_time <= self.window_start
                    && coverage.latest_event_time >= self.window_end
            })
    }
}

impl FreshBootStage {
    /// Closed transition table. Skipping or reversing a gate is always invalid.
    pub const fn advance(self, event: FreshBootEventKind) -> Result<Self, &'static str> {
        match (self, event) {
            (Self::AwaitingSourceCoverage, FreshBootEventKind::SourceCoverageSatisfied) => {
                Ok(Self::DatasetQueued)
            }
            (Self::DatasetQueued, FreshBootEventKind::DatasetStarted) => Ok(Self::DatasetRunning),
            (Self::DatasetRunning, FreshBootEventKind::DatasetCompleted) => Ok(Self::DatasetReady),
            (Self::DatasetReady, FreshBootEventKind::TrainingEnqueued) => Ok(Self::TrainingQueued),
            (Self::TrainingQueued, FreshBootEventKind::TrainingStarted) => {
                Ok(Self::TrainingRunning)
            }
            (Self::TrainingRunning, FreshBootEventKind::TrainingCompleted) => {
                Ok(Self::TrainingReady)
            }
            (Self::TrainingReady, FreshBootEventKind::CalibrationDatasetEnqueued) => {
                Ok(Self::CalibrationDatasetQueued)
            }
            (Self::CalibrationDatasetQueued, FreshBootEventKind::CalibrationDatasetStarted) => {
                Ok(Self::CalibrationDatasetRunning)
            }
            (Self::CalibrationDatasetRunning, FreshBootEventKind::CalibrationDatasetCompleted) => {
                Ok(Self::CalibrationDatasetReady)
            }
            (Self::CalibrationDatasetReady, FreshBootEventKind::CalibrationEnqueued) => {
                Ok(Self::CalibrationQueued)
            }
            (Self::CalibrationQueued, FreshBootEventKind::CalibrationStarted) => {
                Ok(Self::CalibrationRunning)
            }
            (Self::CalibrationRunning, FreshBootEventKind::CalibrationCompleted) => {
                Ok(Self::CalibrationReady)
            }
            (Self::CalibrationReady, FreshBootEventKind::CpcvEnqueued) => Ok(Self::CpcvQueued),
            (Self::CpcvQueued, FreshBootEventKind::CpcvStarted) => Ok(Self::CpcvRunning),
            (Self::CpcvRunning, FreshBootEventKind::CpcvCompleted) => Ok(Self::CpcvReady),
            (Self::CpcvReady, FreshBootEventKind::ParityVerified) => Ok(Self::ParityReady),
            (Self::ParityReady, FreshBootEventKind::ScenarioBound) => Ok(Self::ScenarioReady),
            (Self::ScenarioReady, FreshBootEventKind::BootstrapPrepared)
            | (Self::BootstrapPreflight, FreshBootEventKind::PreflightRefreshed) => {
                Ok(Self::BootstrapPreflight)
            }
            (Self::BootstrapPreflight, FreshBootEventKind::BootstrapCommitted) => {
                Ok(Self::BootstrapCommitted)
            }
            (Self::BootstrapCommitted, FreshBootEventKind::ReportEnabled) => {
                Ok(Self::ReportEligible)
            }
            (Self::ReportEligible, FreshBootEventKind::ReportRetried) => Ok(Self::ReportEligible),
            (Self::ReportEligible, FreshBootEventKind::ReportPublished) => {
                Ok(Self::FirstReportPublished)
            }
            _ => Err("fresh-boot event is not legal at the current stage"),
        }
    }
}

/// Complete durable projection of one profile/route bootstrap run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_fresh_boot_run::Entity")]
pub struct FreshBootRunInfo {
    pub run_id: FreshBootRunId,
    pub supersedes_run_id: Option<FreshBootRunId>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub route: BuyModelRoute,
    pub stage: FreshBootStage,
    pub status: FreshBootStatus,
    pub source_coverage_manifest: Option<FreshBootSourceCoverageManifest>,
    pub source_coverage_hash: Option<ContentHash>,
    pub source_slice_id: Option<SourceSliceId>,
    pub source_slice_hash: Option<ContentHash>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_spec_id: Option<ModelSpecId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub calibration_dataset_id: Option<TrainingDatasetId>,
    pub source_model_version_id: Option<ModelVersionId>,
    pub model_version_id: Option<ModelVersionId>,
    pub path_set_id: Option<BacktestPathSetId>,
    pub calibration_id: Option<CalibrationArtifactId>,
    pub parity_run_id: Option<FeatureParityRunId>,
    pub scenario_artifact_id: Option<PortfolioScenarioModelArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    pub bootstrap_preflight: Option<ModelRouteBootstrapPreflight>,
    pub bootstrap_preflight_hash: Option<ContentHash>,
    pub active_job_id: Option<ResearchJobId>,
    pub last_job_id: Option<ResearchJobId>,
    pub bootstrap_policy_activation_id: Option<PolicyActivationId>,
    pub manual_report_ready_at: Option<DateTime<Utc>>,
    pub first_report_run_id: Option<ReportRunId>,
    pub first_report_id: Option<RecommendationReportId>,
    pub next_scheduled_report_at: Option<DateTime<Utc>>,
    pub retry_reason: Option<FreshBootRetryReason>,
    pub retry_detail: Option<String>,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub blocked_reason: Option<FreshBootBlockedReason>,
    pub blocked_detail: Option<String>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub idempotency_key: PolicyIdempotencyKey,
    pub revision: i64,
    pub stage_entered_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(FreshBootRunInfo, quant_fresh_boot_run::Model, {
    run_id, supersedes_run_id, research_profile_artifact_id, profile_hash, route, stage, status,
    source_coverage_manifest, source_coverage_hash, source_slice_id, source_slice_hash,
    decision_policy_snapshot_id, model_spec_id, training_dataset_id, calibration_dataset_id,
    source_model_version_id, model_version_id, path_set_id, calibration_id, parity_run_id,
    scenario_artifact_id, scenario_artifact_hash, bootstrap_preflight, bootstrap_preflight_hash,
    active_job_id, last_job_id, bootstrap_policy_activation_id, manual_report_ready_at,
    first_report_run_id, first_report_id, next_scheduled_report_at, retry_reason, retry_detail,
    retry_count, next_attempt_at, blocked_reason, blocked_detail, lease_owner, lease_expires_at,
    idempotency_key, revision, stage_entered_at, started_at, completed_at, created_at, updated_at,
});

/// Initial projection written together with the immutable `RunCreated` event.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(
    active_model = "crate::entities::quant_fresh_boot_run::ActiveModel",
    exhaustive
)]
pub struct NewFreshBootRun {
    pub run_id: FreshBootRunId,
    pub supersedes_run_id: Option<FreshBootRunId>,
    pub research_profile_artifact_id: ResearchProfileArtifactId,
    pub profile_hash: ContentHash,
    pub route: BuyModelRoute,
    pub stage: FreshBootStage,
    pub status: FreshBootStatus,
    pub source_coverage_manifest: Option<FreshBootSourceCoverageManifest>,
    pub source_coverage_hash: Option<ContentHash>,
    pub source_slice_id: Option<SourceSliceId>,
    pub source_slice_hash: Option<ContentHash>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub model_spec_id: Option<ModelSpecId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub calibration_dataset_id: Option<TrainingDatasetId>,
    pub source_model_version_id: Option<ModelVersionId>,
    pub model_version_id: Option<ModelVersionId>,
    pub path_set_id: Option<BacktestPathSetId>,
    pub calibration_id: Option<CalibrationArtifactId>,
    pub parity_run_id: Option<FeatureParityRunId>,
    pub scenario_artifact_id: Option<PortfolioScenarioModelArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    pub bootstrap_preflight: Option<ModelRouteBootstrapPreflight>,
    pub bootstrap_preflight_hash: Option<ContentHash>,
    pub active_job_id: Option<ResearchJobId>,
    pub last_job_id: Option<ResearchJobId>,
    pub bootstrap_policy_activation_id: Option<PolicyActivationId>,
    pub manual_report_ready_at: Option<DateTime<Utc>>,
    pub first_report_run_id: Option<ReportRunId>,
    pub first_report_id: Option<RecommendationReportId>,
    pub next_scheduled_report_at: Option<DateTime<Utc>>,
    pub retry_reason: Option<FreshBootRetryReason>,
    pub retry_detail: Option<String>,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub blocked_reason: Option<FreshBootBlockedReason>,
    pub blocked_detail: Option<String>,
    pub lease_owner: Option<WorkerId>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub idempotency_key: PolicyIdempotencyKey,
    pub revision: i64,
    pub stage_entered_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Evidence identities learned at one successful transition edge.
#[derive(Debug, Clone, Default)]
pub struct FreshBootAdvancePatch {
    pub source_coverage_manifest: Option<FreshBootSourceCoverageManifest>,
    pub source_coverage_hash: Option<ContentHash>,
    pub source_slice_id: Option<SourceSliceId>,
    pub source_slice_hash: Option<ContentHash>,
    pub model_spec_id: Option<ModelSpecId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub calibration_dataset_id: Option<TrainingDatasetId>,
    pub source_model_version_id: Option<ModelVersionId>,
    pub model_version_id: Option<ModelVersionId>,
    pub path_set_id: Option<BacktestPathSetId>,
    pub calibration_id: Option<CalibrationArtifactId>,
    pub parity_run_id: Option<FeatureParityRunId>,
    pub scenario_artifact_id: Option<PortfolioScenarioModelArtifactId>,
    pub scenario_artifact_hash: Option<ContentHash>,
    pub bootstrap_preflight: Option<ModelRouteBootstrapPreflight>,
    pub bootstrap_preflight_hash: Option<ContentHash>,
    /// `Some(None)` clears a completed job; `None` preserves the current job.
    pub active_job_id: Option<Option<ResearchJobId>>,
    pub last_job_id: Option<ResearchJobId>,
    pub bootstrap_policy_activation_id: Option<PolicyActivationId>,
    pub manual_report_ready_at: Option<DateTime<Utc>>,
    pub first_report_run_id: Option<ReportRunId>,
    pub first_report_id: Option<RecommendationReportId>,
    pub next_scheduled_report_at: Option<DateTime<Utc>>,
    pub retry_count: Option<i32>,
}

/// One validated optimistic stage transition.
#[derive(Debug, Clone)]
pub struct AdvanceFreshBootRun {
    pub run_id: FreshBootRunId,
    pub expected_revision: i64,
    pub event: FreshBootEventKind,
    pub patch: FreshBootAdvancePatch,
    pub evidence_hash: Option<ContentHash>,
    pub actor: String,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

/// Move a run into a non-terminal evidence wait or retry schedule.
#[derive(Debug, Clone)]
pub struct DelayFreshBootRun {
    pub run_id: FreshBootRunId,
    pub expected_revision: i64,
    pub status: FreshBootStatus,
    pub reason: FreshBootRetryReason,
    pub detail: String,
    pub next_attempt_at: DateTime<Utc>,
    pub consume_retry: bool,
    pub actor: String,
    pub occurred_at: DateTime<Utc>,
}

/// Resume a due delayed run without changing its business stage.
#[derive(Debug, Clone)]
pub struct ResumeFreshBootRun {
    pub run_id: FreshBootRunId,
    pub expected_revision: i64,
    pub worker_id: WorkerId,
    pub lease_expires_at: DateTime<Utc>,
    pub actor: String,
    pub occurred_at: DateTime<Utc>,
}

/// Typed fail-closed terminal transition that never changes the current stage.
#[derive(Debug, Clone)]
pub struct BlockFreshBootRun {
    pub run_id: FreshBootRunId,
    pub expected_revision: i64,
    pub reason: FreshBootBlockedReason,
    pub detail: String,
    pub actor: String,
    pub occurred_at: DateTime<Utc>,
}

/// Governed terminal-run replacement. Historical rows remain immutable afterwards.
#[derive(Debug, Clone)]
pub struct SupersedeFreshBootRun {
    pub run_id: FreshBootRunId,
    pub expected_revision: i64,
    pub replacement_run_id: FreshBootRunId,
    pub reason: String,
    pub actor: String,
    pub occurred_at: DateTime<Utc>,
}

/// One immutable timeline record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_fresh_boot_run_event::Entity")]
pub struct FreshBootRunEventInfo {
    pub event_id: FreshBootRunEventId,
    pub run_id: FreshBootRunId,
    pub event_sequence: i64,
    pub from_stage: FreshBootStage,
    pub to_stage: FreshBootStage,
    pub from_status: FreshBootStatus,
    pub to_status: FreshBootStatus,
    pub event_kind: FreshBootEventKind,
    pub research_job_id: Option<ResearchJobId>,
    pub result_ref: Option<Uuid>,
    pub evidence_hash: Option<ContentHash>,
    pub attempt: i32,
    pub actor: String,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub event_hash: ContentHash,
}

info_from_model!(FreshBootRunEventInfo, quant_fresh_boot_run_event::Model, {
    event_id, run_id, event_sequence, from_stage, to_stage, from_status, to_status, event_kind,
    research_job_id, result_ref, evidence_hash, attempt, actor, detail, occurred_at, event_hash,
});

/// Complete immutable preimage for one fresh-boot event.
#[derive(Debug, Clone, Serialize)]
pub struct FreshBootRunEventInput {
    pub run_id: FreshBootRunId,
    pub event_sequence: i64,
    pub from_stage: FreshBootStage,
    pub to_stage: FreshBootStage,
    pub from_status: FreshBootStatus,
    pub to_status: FreshBootStatus,
    pub event_kind: FreshBootEventKind,
    pub research_job_id: Option<ResearchJobId>,
    pub result_ref: Option<Uuid>,
    pub evidence_hash: Option<ContentHash>,
    pub attempt: i32,
    pub actor: String,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

/// Append-only event sealed by a domain-separated canonical content hash.
#[derive(Debug, Clone, Serialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_fresh_boot_run_event::ActiveModel")]
pub struct NewFreshBootRunEvent {
    event_id: FreshBootRunEventId,
    run_id: FreshBootRunId,
    event_sequence: i64,
    from_stage: FreshBootStage,
    to_stage: FreshBootStage,
    from_status: FreshBootStatus,
    to_status: FreshBootStatus,
    event_kind: FreshBootEventKind,
    research_job_id: Option<ResearchJobId>,
    result_ref: Option<Uuid>,
    evidence_hash: Option<ContentHash>,
    attempt: i32,
    actor: String,
    detail: Option<String>,
    occurred_at: DateTime<Utc>,
    event_hash: ContentHash,
}

impl NewFreshBootRunEvent {
    pub fn try_seal(input: FreshBootRunEventInput) -> Result<Self, StorageError> {
        let actor = input.actor.trim();
        if input.event_sequence < 0
            || input.attempt < 0
            || actor.is_empty()
            || actor.len() > 128
            || actor != input.actor
        {
            return Err(StorageError::invariant_violation(
                Some("quant_fresh_boot_run_event"),
                "event sequence, attempt, and actor are not canonical",
            ));
        }
        if input.detail.as_ref().is_some_and(|detail| {
            detail.is_empty() || detail.len() > 2_048 || detail.trim() != detail
        }) {
            return Err(StorageError::invariant_violation(
                Some("quant_fresh_boot_run_event"),
                "event detail must contain 1..=2048 trimmed bytes when present",
            ));
        }
        let event_hash = CanonicalDigest::content_hash_typed(
            FRESH_BOOT_EVENT_DOMAIN,
            FRESH_BOOT_EVENT_VERSION,
            &input,
        )
        .map_err(|error| {
            StorageError::invariant_violation(
                Some("quant_fresh_boot_run_event"),
                format!("fresh-boot event hashing failed: {error}"),
            )
        })?;
        Ok(Self {
            event_id: FreshBootRunEventId::from_event_hash(&event_hash),
            run_id: input.run_id,
            event_sequence: input.event_sequence,
            from_stage: input.from_stage,
            to_stage: input.to_stage,
            from_status: input.from_status,
            to_status: input.to_status,
            event_kind: input.event_kind,
            research_job_id: input.research_job_id,
            result_ref: input.result_ref,
            evidence_hash: input.evidence_hash,
            attempt: input.attempt,
            actor: input.actor,
            detail: input.detail,
            occurred_at: input.occurred_at,
            event_hash,
        })
    }

    #[must_use]
    pub const fn event_id(&self) -> FreshBootRunEventId {
        self.event_id
    }
}

impl FreshBootRunInfo {
    /// Validate and derive the next state before any repository write.
    pub fn next_stage(&self, event: FreshBootEventKind) -> Result<FreshBootStage, StorageError> {
        if self.status != FreshBootStatus::Running {
            return Err(StorageError::illegal_transition(
                "fresh_boot_run",
                Some(self.run_id),
                self.status.as_str(),
                "running",
            ));
        }
        self.stage.advance(event).map_err(|detail| {
            StorageError::state_conflict("fresh_boot_run", Some(self.run_id), detail)
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::enums::quant::{FreshBootEventKind, FreshBootStage};

    const STAGES: [FreshBootStage; 22] = [
        FreshBootStage::AwaitingSourceCoverage,
        FreshBootStage::DatasetQueued,
        FreshBootStage::DatasetRunning,
        FreshBootStage::DatasetReady,
        FreshBootStage::TrainingQueued,
        FreshBootStage::TrainingRunning,
        FreshBootStage::TrainingReady,
        FreshBootStage::CalibrationDatasetQueued,
        FreshBootStage::CalibrationDatasetRunning,
        FreshBootStage::CalibrationDatasetReady,
        FreshBootStage::CalibrationQueued,
        FreshBootStage::CalibrationRunning,
        FreshBootStage::CalibrationReady,
        FreshBootStage::CpcvQueued,
        FreshBootStage::CpcvRunning,
        FreshBootStage::CpcvReady,
        FreshBootStage::ParityReady,
        FreshBootStage::ScenarioReady,
        FreshBootStage::BootstrapPreflight,
        FreshBootStage::BootstrapCommitted,
        FreshBootStage::ReportEligible,
        FreshBootStage::FirstReportPublished,
    ];

    const EVENTS: [FreshBootEventKind; 30] = [
        FreshBootEventKind::RunCreated,
        FreshBootEventKind::SourceCoverageSatisfied,
        FreshBootEventKind::DatasetStarted,
        FreshBootEventKind::DatasetCompleted,
        FreshBootEventKind::TrainingEnqueued,
        FreshBootEventKind::TrainingStarted,
        FreshBootEventKind::TrainingCompleted,
        FreshBootEventKind::CalibrationDatasetEnqueued,
        FreshBootEventKind::CalibrationDatasetStarted,
        FreshBootEventKind::CalibrationDatasetCompleted,
        FreshBootEventKind::CalibrationEnqueued,
        FreshBootEventKind::CalibrationStarted,
        FreshBootEventKind::CalibrationCompleted,
        FreshBootEventKind::CpcvEnqueued,
        FreshBootEventKind::CpcvStarted,
        FreshBootEventKind::CpcvCompleted,
        FreshBootEventKind::ParityVerified,
        FreshBootEventKind::ScenarioBound,
        FreshBootEventKind::BootstrapPrepared,
        FreshBootEventKind::PreflightRefreshed,
        FreshBootEventKind::BootstrapCommitted,
        FreshBootEventKind::ReportEnabled,
        FreshBootEventKind::ReportRetried,
        FreshBootEventKind::ReportPublished,
        FreshBootEventKind::EvidenceWaitScheduled,
        FreshBootEventKind::RetryScheduled,
        FreshBootEventKind::RetryStarted,
        FreshBootEventKind::TerminalBlocked,
        FreshBootEventKind::RetryAccelerated,
        FreshBootEventKind::Superseded,
    ];

    const EDGES: [(FreshBootStage, FreshBootEventKind, FreshBootStage); 23] = [
        (
            FreshBootStage::AwaitingSourceCoverage,
            FreshBootEventKind::SourceCoverageSatisfied,
            FreshBootStage::DatasetQueued,
        ),
        (
            FreshBootStage::DatasetQueued,
            FreshBootEventKind::DatasetStarted,
            FreshBootStage::DatasetRunning,
        ),
        (
            FreshBootStage::DatasetRunning,
            FreshBootEventKind::DatasetCompleted,
            FreshBootStage::DatasetReady,
        ),
        (
            FreshBootStage::DatasetReady,
            FreshBootEventKind::TrainingEnqueued,
            FreshBootStage::TrainingQueued,
        ),
        (
            FreshBootStage::TrainingQueued,
            FreshBootEventKind::TrainingStarted,
            FreshBootStage::TrainingRunning,
        ),
        (
            FreshBootStage::TrainingRunning,
            FreshBootEventKind::TrainingCompleted,
            FreshBootStage::TrainingReady,
        ),
        (
            FreshBootStage::TrainingReady,
            FreshBootEventKind::CalibrationDatasetEnqueued,
            FreshBootStage::CalibrationDatasetQueued,
        ),
        (
            FreshBootStage::CalibrationDatasetQueued,
            FreshBootEventKind::CalibrationDatasetStarted,
            FreshBootStage::CalibrationDatasetRunning,
        ),
        (
            FreshBootStage::CalibrationDatasetRunning,
            FreshBootEventKind::CalibrationDatasetCompleted,
            FreshBootStage::CalibrationDatasetReady,
        ),
        (
            FreshBootStage::CalibrationDatasetReady,
            FreshBootEventKind::CalibrationEnqueued,
            FreshBootStage::CalibrationQueued,
        ),
        (
            FreshBootStage::CalibrationQueued,
            FreshBootEventKind::CalibrationStarted,
            FreshBootStage::CalibrationRunning,
        ),
        (
            FreshBootStage::CalibrationRunning,
            FreshBootEventKind::CalibrationCompleted,
            FreshBootStage::CalibrationReady,
        ),
        (
            FreshBootStage::CalibrationReady,
            FreshBootEventKind::CpcvEnqueued,
            FreshBootStage::CpcvQueued,
        ),
        (
            FreshBootStage::CpcvQueued,
            FreshBootEventKind::CpcvStarted,
            FreshBootStage::CpcvRunning,
        ),
        (
            FreshBootStage::CpcvRunning,
            FreshBootEventKind::CpcvCompleted,
            FreshBootStage::CpcvReady,
        ),
        (
            FreshBootStage::CpcvReady,
            FreshBootEventKind::ParityVerified,
            FreshBootStage::ParityReady,
        ),
        (
            FreshBootStage::ParityReady,
            FreshBootEventKind::ScenarioBound,
            FreshBootStage::ScenarioReady,
        ),
        (
            FreshBootStage::ScenarioReady,
            FreshBootEventKind::BootstrapPrepared,
            FreshBootStage::BootstrapPreflight,
        ),
        (
            FreshBootStage::BootstrapPreflight,
            FreshBootEventKind::PreflightRefreshed,
            FreshBootStage::BootstrapPreflight,
        ),
        (
            FreshBootStage::BootstrapPreflight,
            FreshBootEventKind::BootstrapCommitted,
            FreshBootStage::BootstrapCommitted,
        ),
        (
            FreshBootStage::BootstrapCommitted,
            FreshBootEventKind::ReportEnabled,
            FreshBootStage::ReportEligible,
        ),
        (
            FreshBootStage::ReportEligible,
            FreshBootEventKind::ReportRetried,
            FreshBootStage::ReportEligible,
        ),
        (
            FreshBootStage::ReportEligible,
            FreshBootEventKind::ReportPublished,
            FreshBootStage::FirstReportPublished,
        ),
    ];

    #[test]
    fn transition_contract_is_closed() {
        for (from, event, to) in EDGES {
            assert_eq!(from.advance(event), Ok(to));
        }
        for stage in STAGES {
            for event in EVENTS {
                let expected = EDGES
                    .iter()
                    .find(|(from, edge, _)| *from == stage && *edge == event)
                    .map(|(_, _, to)| *to);
                assert_eq!(stage.advance(event).ok(), expected);
            }
        }
    }
}
