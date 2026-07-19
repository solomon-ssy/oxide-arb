//! Feature-parity run and governed latch persistence contracts.

use crate::{
    entities::{quant_feature_parity_run, quant_feature_parity_state},
    enums::quant::{
        FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus,
        FeatureParityStateTransition,
    },
    hashing::CanonicalDigest,
    types::{
        ContentHash, DecisionPolicySnapshotId, FeatureParityRunId, FeatureParityStateId, MarketId,
        MarketSelectionId, ModelRunId, ModelSpecId, ModelVersionId, PortfolioPlanId,
        RecommendationReportId, ReportDataQualitySnapshotId, TrainingDatasetId,
    },
};
use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// One persisted deterministic parity replay.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feature_parity_run::Entity")]
pub struct FeatureParityRunInfo {
    pub run_id: FeatureParityRunId,
    pub kind: FeatureParityRunKind,
    pub status: FeatureParityRunStatus,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub triggered_by: String,
    pub requested_by: Option<String>,
    pub acting_role: String,
    pub reason: String,
    pub total_count: i64,
    pub compared_count: i64,
    pub matched_count: i64,
    pub mismatched_count: i64,
    pub pending_materialization_count: i64,
    pub feature_contract_hash: Option<ContentHash>,
    pub transform_hash: Option<ContentHash>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub pending_since: Option<DateTime<Utc>>,
    pub containment_completed_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    FeatureParityRunInfo,
    quant_feature_parity_run::Model,
    {
        run_id,
        kind,
        status,
        window_start,
        window_end,
        report_id,
        model_version_id,
        training_dataset_id,
        triggered_by,
        requested_by,
        acting_role,
        reason,
        total_count,
        compared_count,
        matched_count,
        mismatched_count,
        pending_materialization_count,
        feature_contract_hash,
        transform_hash,
        failure_code,
        failure_detail,
        started_at,
        pending_since,
        containment_completed_at,
        finished_at,
        created_at,
        updated_at,
    }
);

/// Insert payload for a queued parity replay.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feature_parity_run::ActiveModel")]
pub struct NewFeatureParityRun {
    pub run_id: FeatureParityRunId,
    pub kind: FeatureParityRunKind,
    pub status: FeatureParityRunStatus,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub triggered_by: String,
    pub requested_by: Option<String>,
    pub acting_role: String,
    pub reason: String,
    pub total_count: i64,
    pub compared_count: i64,
    pub matched_count: i64,
    pub mismatched_count: i64,
    pub pending_materialization_count: i64,
    pub feature_contract_hash: Option<ContentHash>,
    pub transform_hash: Option<ContentHash>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub pending_since: Option<DateTime<Utc>>,
    pub containment_completed_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// Validated terminal/pending result written by a parity executor.
#[derive(Debug, Clone)]
pub struct CompleteFeatureParityRun {
    pub status: FeatureParityRunStatus,
    pub total_count: i64,
    pub compared_count: i64,
    pub matched_count: i64,
    pub mismatched_count: i64,
    pub pending_materialization_count: i64,
    pub feature_contract_hash: Option<ContentHash>,
    pub transform_hash: Option<ContentHash>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
}

/// Frozen market membership loaded from the WORM parity ledger.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenFeatureParityCandidate {
    pub market_id: MarketId,
    pub ordinal: i32,
    pub membership_hash: ContentHash,
}

/// Typed identity of one frozen serving decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrozenFeatureParitySubjectId {
    ModelRun(ModelRunId),
    RecommendationReport(RecommendationReportId),
    ModelVersion {
        model_version_id: ModelVersionId,
        training_dataset_id: TrainingDatasetId,
    },
}

/// Exact serving subject and membership frozen before a parity job is visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrozenFeatureParitySubject {
    pub subject_id: FrozenFeatureParitySubjectId,
    pub market_selection_id: Option<MarketSelectionId>,
    pub subject_generation: ContentHash,
    pub decision_at: Option<DateTime<Utc>>,
    pub selection_hash: Option<ContentHash>,
    pub evidence_hash: ContentHash,
    pub candidates: Vec<FrozenFeatureParityCandidate>,
}

/// WORM subject written atomically with an offline pre-publication full proof.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFrozenModelParitySubject {
    pub model_version_id: ModelVersionId,
    pub training_dataset_id: TrainingDatasetId,
    pub subject_generation: ContentHash,
    pub evidence_hash: ContentHash,
}

/// Exact model artifact and dataset materialization covered by a publication proof.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct ModelVersionParityEvidence<'a> {
    pub model_version_id: &'a ModelVersionId,
    pub model_spec_id: &'a ModelSpecId,
    pub artifact_hash: &'a ContentHash,
    pub training_dataset_id: &'a TrainingDatasetId,
    pub dataset_hash: &'a ContentHash,
    pub manifest_hash: &'a ContentHash,
    pub artifact_bytes_hash: &'a ContentHash,
}

pub fn model_version_parity_evidence_hash(
    evidence: &ModelVersionParityEvidence<'_>,
) -> Result<ContentHash, CanonicalDigestError> {
    CanonicalDigest::content_hash_typed(
        "quant-pivot/feature-parity/model-version-evidence",
        1,
        evidence,
    )
}

/// Exact immutable model-run fields covered by a frozen parity subject.
pub fn model_run_parity_evidence_hash(
    model_run_id: &ModelRunId,
    input_hash: &ContentHash,
    output_hash: &ContentHash,
    model_version_id: &Option<ModelVersionId>,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
) -> Result<ContentHash, CanonicalDigestError> {
    #[derive(Serialize)]
    struct Evidence<'a> {
        model_run_id: &'a ModelRunId,
        input_hash: &'a ContentHash,
        output_hash: &'a ContentHash,
        model_version_id: &'a Option<ModelVersionId>,
        decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
    }

    CanonicalDigest::content_hash_typed(
        "quant-pivot/feature-parity/model-run-evidence",
        1,
        &Evidence {
            model_run_id,
            input_hash,
            output_hash,
            model_version_id,
            decision_policy_snapshot_id,
        },
    )
}

/// Stable generation identifier for an immutable report artifact.
pub fn report_parity_generation_hash(
    report_id: &RecommendationReportId,
    decision_at: DateTime<Utc>,
    created_at: DateTime<Utc>,
) -> Result<ContentHash, CanonicalDigestError> {
    #[derive(Serialize)]
    struct Generation<'a> {
        report_id: &'a RecommendationReportId,
        decision_at: DateTime<Utc>,
        created_at: DateTime<Utc>,
    }

    CanonicalDigest::content_hash_typed(
        "quant-pivot/feature-parity/report-generation",
        1,
        &Generation {
            report_id,
            decision_at,
            created_at,
        },
    )
}

/// Immutable report evidence bound to its generation and selection.
pub fn report_parity_evidence_hash(
    generation: &ContentHash,
    model_version_id: &ModelVersionId,
    decision_policy_snapshot_id: &DecisionPolicySnapshotId,
    market_selection_id: &MarketSelectionId,
    data_quality_snapshot_id: &ReportDataQualitySnapshotId,
    portfolio_plan_id: &PortfolioPlanId,
) -> Result<ContentHash, CanonicalDigestError> {
    #[derive(Serialize)]
    struct Evidence<'a> {
        generation: &'a ContentHash,
        model_version_id: &'a ModelVersionId,
        decision_policy_snapshot_id: &'a DecisionPolicySnapshotId,
        market_selection_id: &'a MarketSelectionId,
        data_quality_snapshot_id: &'a ReportDataQualitySnapshotId,
        portfolio_plan_id: &'a PortfolioPlanId,
    }

    CanonicalDigest::content_hash_typed(
        "quant-pivot/feature-parity/report-evidence",
        1,
        &Evidence {
            generation,
            model_version_id,
            decision_policy_snapshot_id,
            market_selection_id,
            data_quality_snapshot_id,
            portfolio_plan_id,
        },
    )
}

/// Canonical set identity for a selector result. Market ids are sorted here so
/// callers cannot accidentally make the digest depend on query row order.
pub fn parity_selection_hash(
    selection_id: &MarketSelectionId,
    selector_hash: &ContentHash,
    market_ids: &[MarketId],
) -> Result<ContentHash, CanonicalDigestError> {
    #[derive(Serialize)]
    struct ParityMembership<'a> {
        selection_id: &'a MarketSelectionId,
        selector_hash: &'a ContentHash,
        market_ids: Vec<&'a MarketId>,
    }

    let mut market_ids = market_ids.iter().collect::<Vec<_>>();
    market_ids.sort();
    CanonicalDigest::content_hash_typed(
        "quant-pivot/feature-parity/selection",
        1,
        &ParityMembership {
            selection_id,
            selector_hash,
            market_ids,
        },
    )
}

/// Exact ordinal membership proof within a frozen selection.
pub fn parity_candidate_membership_hash(
    selection_hash: &ContentHash,
    market_id: &MarketId,
    ordinal: i32,
) -> Result<ContentHash, CanonicalDigestError> {
    #[derive(Serialize)]
    struct Membership<'a> {
        selection_hash: &'a ContentHash,
        market_id: &'a MarketId,
        ordinal: i32,
    }

    CanonicalDigest::content_hash_typed(
        "quant-pivot/feature-parity/candidate-membership",
        1,
        &Membership {
            selection_hash,
            market_id,
            ordinal,
        },
    )
}

/// One immutable transition of the admission latch.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_feature_parity_state::Entity")]
pub struct FeatureParityStateInfo {
    pub state_id: FeatureParityStateId,
    pub state: FeatureParityLatchState,
    pub transition: FeatureParityStateTransition,
    pub cause_run_id: Option<FeatureParityRunId>,
    pub recovery_run_id: Option<FeatureParityRunId>,
    pub previous_state_id: Option<FeatureParityStateId>,
    pub actor: Option<String>,
    pub acting_role: Option<String>,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    FeatureParityStateInfo,
    quant_feature_parity_state::Model,
    {
        state_id,
        state,
        transition,
        cause_run_id,
        recovery_run_id,
        previous_state_id,
        actor,
        acting_role,
        reason,
        created_at,
    }
);

/// Insert payload for the append-only latch ledger.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_feature_parity_state::ActiveModel")]
pub struct NewFeatureParityState {
    pub state_id: FeatureParityStateId,
    pub state: FeatureParityLatchState,
    pub transition: FeatureParityStateTransition,
    pub cause_run_id: Option<FeatureParityRunId>,
    pub recovery_run_id: Option<FeatureParityRunId>,
    pub previous_state_id: Option<FeatureParityStateId>,
    pub actor: Option<String>,
    pub acting_role: Option<String>,
    pub reason: String,
}
