//! System control-plane API contract.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use validator::Validate;

use crate::{
    domain::{
        data_plane::ExchangeHistoryFrontierProgress,
        governance::SystemStatus,
        quant::{FreshBootRunEventInfo, FreshBootRunInfo, FreshBootSourceCoverageManifest},
    },
    enums::{
        execution::KillSwitchState,
        quant::{
            FreshBootBlockedReason, FreshBootEventKind, FreshBootRetryReason, FreshBootStage,
            FreshBootStatus, QuantRuntimeMode, TrainingDatasetStatus,
        },
        settlement::SettlementWritePolicy,
        system::{CapabilityId, CapabilityReason},
    },
    runtime_config::BuyModelRoute,
    types::{
        BacktestPathSetId, CalibrationArtifactId, ContentHash, DecisionPolicySnapshotId,
        FeatureParityRunId, FreshBootRunEventId, FreshBootRunId, ModelSpecId, ModelVersionId,
        PolicyActivationId, PortfolioScenarioModelArtifactId, RecommendationReportId, ReportRunId,
        ResearchJobId, ResearchProfileArtifactId, SourceSliceId, TrainingDatasetId,
    },
};

/// Typed operator action selected from the current durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshBootRecommendedAction {
    WaitForEvidence,
    InspectRunningJob,
    RetryNow,
    ResolveAndSupersede,
    ViewFirstReport,
}

/// Stable failure domain used to route an operator to the owning subsystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FreshBootBlockerScope {
    SourceCoverage,
    ResearchJob,
    BootstrapGovernance,
    ReportPublication,
}

impl FreshBootBlockerScope {
    const fn for_stage(stage: FreshBootStage) -> Self {
        match stage {
            FreshBootStage::AwaitingSourceCoverage => Self::SourceCoverage,
            FreshBootStage::BootstrapPreflight | FreshBootStage::BootstrapCommitted => {
                Self::BootstrapGovernance
            }
            FreshBootStage::ReportEligible | FreshBootStage::FirstReportPublished => {
                Self::ReportPublication
            }
            _ => Self::ResearchJob,
        }
    }
}

/// Machine-readable blocker code without collapsing retryable and terminal failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "code", rename_all = "snake_case")]
pub enum FreshBootBlockerCode {
    Retryable(FreshBootRetryReason),
    Terminal(FreshBootBlockedReason),
}

/// One actionable blocker or wait reason. Exactly one action is recommended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshBootBlockerView {
    pub code: FreshBootBlockerCode,
    pub scope: FreshBootBlockerScope,
    pub evidence_ref: Option<ContentHash>,
    pub retryable: bool,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub detail: String,
    pub recommended_action: FreshBootRecommendedAction,
}

/// Stable operator projection of one durable fresh-boot run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshBootRunProgressView {
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
    pub bootstrap_preflight_hash: Option<ContentHash>,
    pub active_job_id: Option<ResearchJobId>,
    pub last_job_id: Option<ResearchJobId>,
    pub bootstrap_policy_activation_id: Option<PolicyActivationId>,
    pub manual_report_ready_at: Option<DateTime<Utc>>,
    pub first_report_run_id: Option<ReportRunId>,
    pub first_report_id: Option<RecommendationReportId>,
    pub next_scheduled_report_at: Option<DateTime<Utc>>,
    pub blocker: Option<FreshBootBlockerView>,
    pub retry_count: i32,
    pub next_attempt_at: Option<DateTime<Utc>>,
    pub revision: i64,
    pub stage_entered_at: DateTime<Utc>,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

impl From<FreshBootRunInfo> for FreshBootRunProgressView {
    fn from(run: FreshBootRunInfo) -> Self {
        let blocker = match (
            run.retry_reason,
            run.retry_detail.as_deref(),
            run.blocked_reason,
            run.blocked_detail.as_deref(),
        ) {
            (Some(reason), Some(detail), _, _) => Some(FreshBootBlockerView {
                code: FreshBootBlockerCode::Retryable(reason),
                scope: FreshBootBlockerScope::for_stage(run.stage),
                evidence_ref: run.source_coverage_hash,
                retryable: true,
                next_retry_at: run.next_attempt_at,
                detail: detail.to_owned(),
                recommended_action: if run.status == FreshBootStatus::WaitingEvidence {
                    FreshBootRecommendedAction::WaitForEvidence
                } else {
                    FreshBootRecommendedAction::RetryNow
                },
            }),
            (_, _, Some(reason), Some(detail)) => Some(FreshBootBlockerView {
                code: FreshBootBlockerCode::Terminal(reason),
                scope: FreshBootBlockerScope::for_stage(run.stage),
                evidence_ref: run.source_coverage_hash,
                retryable: false,
                next_retry_at: None,
                detail: detail.to_owned(),
                recommended_action: FreshBootRecommendedAction::ResolveAndSupersede,
            }),
            _ => None,
        };
        Self {
            run_id: run.run_id,
            supersedes_run_id: run.supersedes_run_id,
            research_profile_artifact_id: run.research_profile_artifact_id,
            profile_hash: run.profile_hash,
            route: run.route,
            stage: run.stage,
            status: run.status,
            source_coverage_manifest: run.source_coverage_manifest,
            source_coverage_hash: run.source_coverage_hash,
            source_slice_id: run.source_slice_id,
            source_slice_hash: run.source_slice_hash,
            decision_policy_snapshot_id: run.decision_policy_snapshot_id,
            model_spec_id: run.model_spec_id,
            training_dataset_id: run.training_dataset_id,
            calibration_dataset_id: run.calibration_dataset_id,
            source_model_version_id: run.source_model_version_id,
            model_version_id: run.model_version_id,
            path_set_id: run.path_set_id,
            calibration_id: run.calibration_id,
            parity_run_id: run.parity_run_id,
            scenario_artifact_id: run.scenario_artifact_id,
            scenario_artifact_hash: run.scenario_artifact_hash,
            bootstrap_preflight_hash: run.bootstrap_preflight_hash,
            active_job_id: run.active_job_id,
            last_job_id: run.last_job_id,
            bootstrap_policy_activation_id: run.bootstrap_policy_activation_id,
            manual_report_ready_at: run.manual_report_ready_at,
            first_report_run_id: run.first_report_run_id,
            first_report_id: run.first_report_id,
            next_scheduled_report_at: run.next_scheduled_report_at,
            blocker,
            retry_count: run.retry_count,
            next_attempt_at: run.next_attempt_at,
            revision: run.revision,
            stage_entered_at: run.stage_entered_at,
            started_at: run.started_at,
            completed_at: run.completed_at,
            updated_at: run.updated_at,
        }
    }
}

/// Public event timeline item. Internal bootstrap documents remain hash-addressed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshBootRunEventView {
    pub event_id: FreshBootRunEventId,
    pub sequence: i64,
    pub from_stage: FreshBootStage,
    pub to_stage: FreshBootStage,
    pub from_status: FreshBootStatus,
    pub to_status: FreshBootStatus,
    pub event: FreshBootEventKind,
    pub research_job_id: Option<ResearchJobId>,
    pub result_ref: Option<Uuid>,
    pub evidence_ref: Option<ContentHash>,
    pub attempt: i32,
    pub actor: String,
    pub detail: Option<String>,
    pub occurred_at: DateTime<Utc>,
}

impl From<FreshBootRunEventInfo> for FreshBootRunEventView {
    fn from(event: FreshBootRunEventInfo) -> Self {
        Self {
            event_id: event.event_id,
            sequence: event.event_sequence,
            from_stage: event.from_stage,
            to_stage: event.to_stage,
            from_status: event.from_status,
            to_status: event.to_status,
            event: event.event_kind,
            research_job_id: event.research_job_id,
            result_ref: event.result_ref,
            evidence_ref: event.evidence_hash,
            attempt: event.attempt,
            actor: event.actor,
            detail: event.detail,
            occurred_at: event.occurred_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FreshBootRunDetailView {
    pub run: FreshBootRunProgressView,
    pub events: Vec<FreshBootRunEventView>,
}

/// Complete operator projection of the L2-free cold-start path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshBootProfileProgressView {
    pub run: FreshBootRunProgressView,
    pub last_event: Option<FreshBootRunEventView>,
    pub training_dataset_status: Option<TrainingDatasetStatus>,
    pub training_sample_count: Option<i64>,
    pub calibration_dataset_status: Option<TrainingDatasetStatus>,
    pub calibration_sample_count: Option<i64>,
    pub manual_report_ready: bool,
}

/// Complete operator projection of the L2-free cold-start path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FreshBootProgressView {
    pub observed_at: DateTime<Utc>,
    pub exchange_history: ExchangeHistoryFrontierProgress,
    pub profiles: Vec<FreshBootProfileProgressView>,
}

#[derive(Debug, Deserialize, Validate)]
pub struct RetryFreshBootRunRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Deserialize, Validate)]
pub struct SupersedeFreshBootRunRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Governed quant runtime mode transition request.
#[derive(Debug, Deserialize, Validate)]
pub struct SwitchQuantModeRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    pub mode: QuantRuntimeMode,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

/// Governed operational kill-switch transition request.
#[derive(Debug, Deserialize, Validate)]
pub struct SetKillSwitchRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    pub state: KillSwitchState,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
    /// Operator acknowledgement, required to clear `emergency_halted`.
    #[serde(default)]
    pub ack: bool,
}

/// Governed settlement write-policy transition request.
#[derive(Debug, Deserialize, Validate)]
pub struct SwitchSettlementWritePolicyRequest {
    #[validate(range(min = 0))]
    pub expected_revision: i64,
    pub policy: SettlementWritePolicy,
    #[validate(length(min = 1, max = 1024))]
    pub reason: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityView {
    pub enabled: bool,
    pub reasons: Vec<CapabilityReason>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemCapabilities {
    pub revision: u64,
    pub control_plane_ready: CapabilityView,
    pub catalog_baseline_ready: CapabilityView,
    pub research_capture_enabled: CapabilityView,
    pub report_generation_eligible: CapabilityView,
    pub entry_admission_eligible: CapabilityView,
    pub order_submission_eligible: CapabilityView,
    pub automatic_parity_eligible: CapabilityView,
}

impl SystemCapabilities {
    #[must_use]
    pub fn fail_closed(reason: CapabilityReason) -> Self {
        let disabled = CapabilityView {
            enabled: false,
            reasons: vec![reason],
        };
        Self {
            revision: 0,
            control_plane_ready: disabled.clone(),
            catalog_baseline_ready: disabled.clone(),
            research_capture_enabled: disabled.clone(),
            report_generation_eligible: disabled.clone(),
            entry_admission_eligible: disabled.clone(),
            order_submission_eligible: disabled.clone(),
            automatic_parity_eligible: disabled,
        }
    }

    #[must_use]
    pub const fn get(&self, capability: CapabilityId) -> &CapabilityView {
        match capability {
            CapabilityId::ControlPlaneReady => &self.control_plane_ready,
            CapabilityId::CatalogBaselineReady => &self.catalog_baseline_ready,
            CapabilityId::ResearchCaptureEnabled => &self.research_capture_enabled,
            CapabilityId::ReportGenerationEligible => &self.report_generation_eligible,
            CapabilityId::EntryAdmissionEligible => &self.entry_admission_eligible,
            CapabilityId::OrderSubmissionEligible => &self.order_submission_eligible,
            CapabilityId::AutomaticParityEligible => &self.automatic_parity_eligible,
        }
    }

    #[must_use]
    pub fn decisions_equal(&self, other: &Self) -> bool {
        self.control_plane_ready == other.control_plane_ready
            && self.catalog_baseline_ready == other.catalog_baseline_ready
            && self.research_capture_enabled == other.research_capture_enabled
            && self.report_generation_eligible == other.report_generation_eligible
            && self.entry_admission_eligible == other.entry_admission_eligible
            && self.order_submission_eligible == other.order_submission_eligible
            && self.automatic_parity_eligible == other.automatic_parity_eligible
    }
}

/// Authenticated control-plane status with derived capabilities layered over
/// the live operational projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemStatusView {
    #[serde(flatten)]
    pub runtime: SystemStatus,
    pub capabilities: SystemCapabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionEligibilityDecision {
    pub enabled: bool,
    pub permission_granted: bool,
    pub capability: CapabilityView,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionEligibilityView {
    pub capability_revision: u64,
    pub report_generation: ActionEligibilityDecision,
    pub entry_admission: ActionEligibilityDecision,
    pub order_submission: ActionEligibilityDecision,
}
