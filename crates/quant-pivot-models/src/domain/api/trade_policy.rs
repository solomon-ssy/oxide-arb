//! Trade-policy research and governance HTTP contracts.

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{
        TradePolicyArtifactInfo, TradePolicyGovernanceAuditInfo, TradePolicyTrialAttemptInfo,
        TradePolicyValidationRowInfo, TradePolicyValidationRunInfo, pagination::PageRequest,
    },
    enums::quant::{
        DatasetPurpose, ResearchReadinessEvidenceKind, TradePolicyGovernanceAction,
        TradePolicyStatus, TradePolicyTrialScope, TradePolicyTrialStatus,
        TradePolicyValidationStatus, TrainingDatasetStatus,
    },
    types::{
        ArtifactUri, ContentHash, DecisionPolicySnapshotId, MarketId, ResearchEvaluationTrack,
        ResearchJobId, ResearchPolicyFitter, ResearchProfileArtifact, ResearchProfileRef,
        ResearchReadinessEvidenceId, ShadowLatencyProfileV1, SourceSliceId, SourceSliceManifestRef,
        SourceSliceObjectKind, SourceSliceObjectRef, TokenId, TradePolicyArtifactId,
        TradePolicyArtifactPayload, TradePolicyCandidateSpec, TradePolicyEvidenceObjectKind,
        TradePolicyGovernanceAuditId, TradePolicyPublicationBlocker, TradePolicyTrialAttemptId,
        TradePolicyTrialMetrics, TradePolicyValidationRunId, TrainingDatasetId, TrainingExampleId,
        UserId, resolve_builtin_research_profile,
    },
};

/// Caller-owned fit selection. Window, horizon, cash budget, methodology,
/// latency profile, and publication floors are resolved by the server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradePolicyFitSelection {
    pub profile_ref: ResearchProfileRef,
    pub pit_cutoff: DateTime<Utc>,
}

impl TradePolicyFitSelection {
    pub fn validate(&self) -> Result<(), String> {
        resolve_builtin_research_profile(&self.profile_ref).map(|_| ())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate)]
pub struct FitTradePolicyRequest {
    pub selection: TradePolicyFitSelection,
    pub evaluation_track: ResearchEvaluationTrack,
    #[validate(length(min = 1, max = 32))]
    pub candidates: Vec<TradePolicyCandidateSpec>,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
    #[validate(length(min = 1, max = 128))]
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TradePolicyFitPreflightRequest {
    pub selection: TradePolicyFitSelection,
    pub evaluation_track: ResearchEvaluationTrack,
    #[validate(length(min = 1, max = 32))]
    pub candidates: Vec<TradePolicyCandidateSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct TradePolicyGovernanceRequest {
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyListQuery {
    pub status: Option<TradePolicyStatus>,
    pub source_dataset_id: Option<TrainingDatasetId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyAuditListQuery {
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyValidationListQuery {
    pub status: Option<TradePolicyValidationStatus>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyValidationRowListQuery {
    pub passed: Option<bool>,
    pub evidence_kind: Option<String>,
    pub diagnostic_kind: Option<String>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyTrialListQuery {
    pub candidate_id: Option<String>,
    pub scope: Option<TradePolicyTrialScope>,
    pub status: Option<TradePolicyTrialStatus>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicySourceSliceObjectListQuery {
    pub kind: Option<SourceSliceObjectKind>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct TradePolicyEvidenceRowListQuery {
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyTrialAttemptView {
    pub trial_attempt_id: TradePolicyTrialAttemptId,
    pub fit_job_id: ResearchJobId,
    pub attempt_ordinal: i64,
    pub experiment_family_hash: ContentHash,
    pub research_program_hash: ContentHash,
    pub candidate_id: String,
    pub candidate_hash: ContentHash,
    pub scope: TradePolicyTrialScope,
    pub fold_index: Option<i32>,
    pub path_index: Option<i32>,
    pub status: TradePolicyTrialStatus,
    pub metrics: Option<TradePolicyTrialMetrics>,
    pub evidence_hash: Option<ContentHash>,
    pub evidence_row_count: Option<i64>,
    pub failure_detail: Option<String>,
    pub row_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<TradePolicyTrialAttemptInfo> for TradePolicyTrialAttemptView {
    fn from(info: TradePolicyTrialAttemptInfo) -> Self {
        Self {
            trial_attempt_id: info.trial_attempt_id,
            fit_job_id: info.fit_job_id,
            attempt_ordinal: info.attempt_ordinal,
            experiment_family_hash: info.experiment_family_hash,
            research_program_hash: info.research_program_hash,
            candidate_id: info.candidate_id,
            candidate_hash: info.candidate_hash,
            scope: info.scope,
            fold_index: info.fold_index,
            path_index: info.path_index,
            status: info.status,
            metrics: info.metrics_json,
            evidence_hash: info.evidence_hash,
            evidence_row_count: info.evidence_row_count,
            failure_detail: info.failure_detail,
            row_hash: info.row_hash,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyGovernanceAuditView {
    pub audit_id: TradePolicyGovernanceAuditId,
    pub artifact_id: TradePolicyArtifactId,
    pub action: TradePolicyGovernanceAction,
    pub from_status: TradePolicyStatus,
    pub to_status: TradePolicyStatus,
    pub content_hash: ContentHash,
    pub actor_id: UserId,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

impl From<TradePolicyGovernanceAuditInfo> for TradePolicyGovernanceAuditView {
    fn from(info: TradePolicyGovernanceAuditInfo) -> Self {
        Self {
            audit_id: info.audit_id,
            artifact_id: info.artifact_id,
            action: info.action,
            from_status: info.from_status,
            to_status: info.to_status,
            content_hash: info.content_hash,
            actor_id: info.actor_id,
            reason: info.reason,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyValidationRunView {
    pub validation_run_id: TradePolicyValidationRunId,
    pub artifact_id: TradePolicyArtifactId,
    pub artifact_hash: ContentHash,
    pub source_dataset_id: TrainingDatasetId,
    pub source_dataset_hash: ContentHash,
    pub source_slice_manifest_hash: ContentHash,
    pub evidence_manifest_hash: ContentHash,
    pub status: TradePolicyValidationStatus,
    pub total_rows: i64,
    pub passed_rows: i64,
    pub failed_rows: i64,
    pub validation_hash: Option<ContentHash>,
    pub failure_detail: Option<String>,
    pub actor_id: UserId,
    pub reason: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl From<TradePolicyValidationRunInfo> for TradePolicyValidationRunView {
    fn from(info: TradePolicyValidationRunInfo) -> Self {
        Self {
            validation_run_id: info.validation_run_id,
            artifact_id: info.artifact_id,
            artifact_hash: info.artifact_hash,
            source_dataset_id: info.source_dataset_id,
            source_dataset_hash: info.source_dataset_hash,
            source_slice_manifest_hash: info.source_slice_manifest_hash,
            evidence_manifest_hash: info.evidence_manifest_hash,
            status: info.status,
            total_rows: info.total_rows,
            passed_rows: info.passed_rows,
            failed_rows: info.failed_rows,
            validation_hash: info.validation_hash,
            failure_detail: info.failure_detail,
            actor_id: info.actor_id,
            reason: info.reason,
            started_at: info.started_at,
            completed_at: info.completed_at,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyValidationRowView {
    pub validation_run_id: TradePolicyValidationRunId,
    pub row_ordinal: i64,
    pub evidence_kind: String,
    pub record_key: String,
    pub example_id: Option<TrainingExampleId>,
    pub market_id: Option<MarketId>,
    pub token_id: Option<TokenId>,
    pub decision_at: Option<DateTime<Utc>>,
    pub expected_row_hash: Option<ContentHash>,
    pub actual_row_hash: Option<ContentHash>,
    pub passed: bool,
    pub diagnostic_kind: Option<String>,
    pub detail: Option<String>,
    pub row_hash: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl From<TradePolicyValidationRowInfo> for TradePolicyValidationRowView {
    fn from(info: TradePolicyValidationRowInfo) -> Self {
        Self {
            validation_run_id: info.validation_run_id,
            row_ordinal: info.row_ordinal,
            evidence_kind: info.evidence_kind,
            record_key: info.record_key,
            example_id: info.example_id,
            market_id: info.market_id,
            token_id: info.token_id,
            decision_at: info.decision_at,
            expected_row_hash: info.expected_row_hash,
            actual_row_hash: info.actual_row_hash,
            passed: info.passed,
            diagnostic_kind: info.diagnostic_kind,
            detail: info.detail,
            row_hash: info.row_hash,
            created_at: info.created_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicySummaryView {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub cohort_count: usize,
    pub executable_coverage: Option<rust_decimal::Decimal>,
    pub publishable: bool,
    pub publication_blocker_count: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<TradePolicyArtifactInfo> for TradePolicySummaryView {
    fn from(info: TradePolicyArtifactInfo) -> Self {
        let publication_blockers = info.payload_json.publication_blockers();
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            status: info.status,
            source_dataset_id: info.source_dataset_id,
            cohort_count: info.payload_json.cohorts.len(),
            executable_coverage: info
                .payload_json
                .cohorts
                .iter()
                .map(|cohort| cohort.executable_coverage)
                .min(),
            publishable: publication_blockers.is_empty(),
            publication_blocker_count: publication_blockers.len(),
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyDetailView {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub payload: TradePolicyArtifactPayload,
    pub publication_blockers: Vec<TradePolicyPublicationBlocker>,
    pub allowed_governance_actions: Vec<TradePolicyGovernanceAction>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicySourceSliceView {
    pub artifact_id: TradePolicyArtifactId,
    pub profile_ref: ResearchProfileRef,
    pub source_slice: SourceSliceManifestRef,
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicySourceSliceObjectView {
    pub kind: SourceSliceObjectKind,
    pub uri: ArtifactUri,
    pub object_version: String,
    pub byte_hash: ContentHash,
    pub schema_hash: ContentHash,
    pub row_count: u64,
    pub min_event_at: Option<DateTime<Utc>>,
    pub max_event_at: Option<DateTime<Utc>>,
    pub min_available_at: Option<DateTime<Utc>>,
    pub max_available_at: Option<DateTime<Utc>>,
}

impl From<SourceSliceObjectRef> for TradePolicySourceSliceObjectView {
    fn from(object: SourceSliceObjectRef) -> Self {
        Self {
            kind: object.kind,
            uri: object.uri,
            object_version: object.object_version,
            byte_hash: object.byte_hash,
            schema_hash: object.schema_hash,
            row_count: object.row_count,
            min_event_at: object.min_event_at,
            max_event_at: object.max_event_at,
            min_available_at: object.min_available_at,
            max_available_at: object.max_available_at,
        }
    }
}

/// Short-lived, backend-signed access to one immutable evidence object.
#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyEvidenceDownloadView {
    pub artifact_id: TradePolicyArtifactId,
    pub kind: TradePolicyEvidenceObjectKind,
    pub byte_hash: ContentHash,
    pub row_count: u64,
    pub expires_at: DateTime<Utc>,
    pub url: String,
}

/// One verified row from an immutable typed policy-evidence object.
///
/// `payload` remains JSON at this heterogeneous API boundary, but the service
/// validates every row against the concrete schema selected by `kind` before
/// returning the page.
#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyEvidenceRowView {
    pub kind: TradePolicyEvidenceObjectKind,
    pub record_key: String,
    pub event_at: Option<DateTime<Utc>>,
    pub payload: serde_json::Value,
    pub row_hash: ContentHash,
}

impl From<TradePolicyArtifactInfo> for TradePolicyDetailView {
    fn from(info: TradePolicyArtifactInfo) -> Self {
        let blockers = info.payload_json.publication_blockers();
        let allowed_governance_actions = match info.status {
            TradePolicyStatus::Draft if blockers.is_empty() => {
                vec![TradePolicyGovernanceAction::Validate]
            }
            TradePolicyStatus::Validated if blockers.is_empty() => {
                vec![TradePolicyGovernanceAction::Publish]
            }
            TradePolicyStatus::Published => vec![TradePolicyGovernanceAction::Retire],
            TradePolicyStatus::Draft
            | TradePolicyStatus::Validated
            | TradePolicyStatus::Retired => Vec::new(),
        };
        Self {
            artifact_id: info.artifact_id,
            content_hash: info.content_hash,
            status: info.status,
            source_dataset_id: info.source_dataset_id,
            payload: info.payload_json,
            publication_blockers: blockers,
            allowed_governance_actions,
            created_at: info.created_at,
            updated_at: info.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyFitPreflightView {
    pub readiness: TradePolicyFitReadiness,
    pub reusable_source_dataset_id: Option<TrainingDatasetId>,
    pub profile: Option<ResearchProfileArtifact>,
    pub fit_window_start: Option<DateTime<Utc>>,
    pub fit_window_end: Option<DateTime<Utc>>,
    pub research_program_hash: Option<ContentHash>,
    pub source_slice_id: Option<SourceSliceId>,
    pub source_slice_identity_hash: Option<ContentHash>,
    pub estimated_candidate_trials: u64,
    pub estimated_fold_evaluations: u64,
    pub catalog_completeness_proven: TradePolicyPreflightCheckStatus,
    pub source_completeness_proven: TradePolicyPreflightCheckStatus,
    pub required_raw_retention_days: Option<u32>,
    pub retention_runway_days: Option<u32>,
    pub retention_runway_proven: TradePolicyPreflightCheckStatus,
    pub contract_valid: TradePolicyPreflightCheckStatus,
    pub profile_fitter_available: TradePolicyPreflightCheckStatus,
    pub source_dataset_ready: TradePolicyPreflightCheckStatus,
    pub source_dataset_policy_fit: TradePolicyPreflightCheckStatus,
    pub raw_trajectory_labels_present: TradePolicyPreflightCheckStatus,
    pub profile_lineage_valid: TradePolicyPreflightCheckStatus,
    pub source_slice_verified: TradePolicyPreflightCheckStatus,
    pub fit_window_contained: TradePolicyPreflightCheckStatus,
    pub profile_quality_gate_available: TradePolicyPreflightCheckStatus,
    pub decision_policy_snapshot_id: Option<DecisionPolicySnapshotId>,
    pub methodology_hash: Option<ContentHash>,
    pub latency_profile_present: TradePolicyPreflightCheckStatus,
    pub latency_evidence: Option<TradePolicyOperationalEvidenceView>,
    pub pit_cutoff_valid: TradePolicyPreflightCheckStatus,
    pub labels_matured_by_cutoff: u64,
    pub labels_excluded_after_cutoff: u64,
    pub full_l2_trajectory_present: TradePolicyPreflightCheckStatus,
    pub fee_model_present: TradePolicyPreflightCheckStatus,
    pub retention_evidence: Option<TradePolicyOperationalEvidenceView>,
    pub publishable_input: TradePolicyPreflightCheckStatus,
    pub canonical_candidates: Option<Vec<TradePolicyCandidateSpec>>,
    pub candidate_set_hash: Option<ContentHash>,
    pub blockers: Vec<TradePolicyPreflightBlockerView>,
}

/// Verified append-only operational evidence reference. Canonical object URIs
/// remain server-side; operators use the stable hash/version identity.
#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyOperationalEvidenceView {
    pub evidence_id: ResearchReadinessEvidenceId,
    pub kind: ResearchReadinessEvidenceKind,
    pub payload_hash: ContentHash,
    pub artifact_version: String,
    pub attestation_key_id: String,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}

/// Server-derived readiness of an immutable trade-policy fit plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyFitReadiness {
    Blocked,
    BlockedInsufficientHistory,
    ReadyToMaterialize,
    Reusable,
}

/// Closed, server-owned facts for one actionable policy-fit denial.
///
/// Static requirements are represented by the discriminator and localized UI
/// copy; only observed or request-dependent facts travel on the wire.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TradePolicyPreflightBlockerDetail {
    ContractInvalid {
        diagnostics: Vec<String>,
    },
    ProfileFitterUnavailable {
        configured_fitter: Option<ResearchPolicyFitter>,
    },
    DatasetNotReady {
        actual_status: Option<TrainingDatasetStatus>,
    },
    DatasetPurposeMismatch {
        actual_purpose: Option<DatasetPurpose>,
    },
    RawTrajectoryLabelsMissing {
        labels_matured_by_cutoff: u64,
        labels_excluded_after_cutoff: u64,
    },
    ProfileLineageMismatch {
        actual_profile_ref: Option<ResearchProfileRef>,
        required_profile_ref: ResearchProfileRef,
    },
    SourceSliceUnverified {
        diagnostics: Vec<String>,
    },
    FitWindowNotContained {
        dataset_window_start: Option<DateTime<Utc>>,
        dataset_window_end: Option<DateTime<Utc>>,
        required_window_start: Option<DateTime<Utc>>,
        required_window_end: Option<DateTime<Utc>>,
    },
    QualityGateUnavailable,
    PitCutoffInvalid {
        pit_cutoff: DateTime<Utc>,
        fit_window_end: Option<DateTime<Utc>>,
        not_future: bool,
    },
    FullL2TrajectoryMissing,
    PitFeeFactsMissing,
    ProductionLatencyProfileMissing {
        observed_profile: Option<ShadowLatencyProfileV1>,
    },
    RetentionRunwayUnproven {
        actual_runway_days: Option<u32>,
        required_minimum_days: Option<u32>,
    },
}

/// Actionable, server-derived denial with a closed discriminated fact shape.
#[derive(Debug, Clone, Serialize)]
pub struct TradePolicyPreflightBlockerView {
    #[serde(flatten)]
    pub detail: TradePolicyPreflightBlockerDetail,
    pub remediation: String,
    pub evidence_link: Option<String>,
}

/// Binary outcome of one deterministic trade-policy fit preflight check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TradePolicyPreflightCheckStatus {
    Pass,
    Fail,
}

impl From<bool> for TradePolicyPreflightCheckStatus {
    fn from(value: bool) -> Self {
        if value { Self::Pass } else { Self::Fail }
    }
}
