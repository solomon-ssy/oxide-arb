//! Feature-integrity HTTP contracts frozen against the Admin SPA wire types.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use quant_pivot_macros::NormalizePageQuery;
use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    domain::{
        pagination::PageRequest,
        quant::{FeatureParityRunInfo, FeatureParityStateInfo},
    },
    enums::quant::{
        FeatureCellState, FeatureParityEventStatus, FeatureParityLatchState, FeatureParityRunKind,
        FeatureParityRunStatus, FeatureParityStage,
    },
    types::{
        ContentHash, DiagnosticCode, FeatureParityDetail, FeatureParityEventId, FeatureParityRunId,
        MarketId, ModelRunId, ModelVersionId, RecommendationReportId, RoleCode, TrainingDatasetId,
    },
};

/// Filters the durable parity-run ledger.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct FeatureParityRunListQuery {
    pub kind: Option<FeatureParityRunKind>,
    pub status: Option<FeatureParityRunStatus>,
    pub report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Filters stage-level parity evidence in `ClickHouse`.
#[derive(Debug, Clone, Default, Deserialize, NormalizePageQuery)]
pub struct FeatureParityEventListQuery {
    pub parity_run_id: Option<FeatureParityRunId>,
    pub status: Option<FeatureParityEventStatus>,
    pub stage: Option<FeatureParityStage>,
    pub feature_name: Option<String>,
    pub reason: Option<String>,
    pub report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[normalize_page]
    #[serde(flatten)]
    pub page: PageRequest,
}

/// Governed request for a full replay. Missing bounds mean the latest 24 hours.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Validate)]
pub struct RunFullFeatureParityRequest {
    pub window_start: Option<DateTime<Utc>>,
    pub window_end: Option<DateTime<Utc>>,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Governed latch-clear request. The referenced full run is verified server-side.
#[derive(Debug, Clone, Deserialize, Validate)]
pub struct AcknowledgeFeatureParityLatchRequest {
    pub parity_run_id: FeatureParityRunId,
    #[validate(length(min = 1, max = 512))]
    pub reason: String,
}

/// Public projection of one parity run.
#[derive(Debug, Clone, Serialize)]
pub struct FeatureParityRunView {
    pub parity_run_id: FeatureParityRunId,
    pub kind: FeatureParityRunKind,
    pub status: FeatureParityRunStatus,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub report_id: Option<RecommendationReportId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub total_count: i64,
    pub compared_count: i64,
    pub matched_count: i64,
    pub mismatched_count: i64,
    pub pending_materialization_count: i64,
    pub feature_contract_hash: ContentHash,
    pub transform_hash: Option<ContentHash>,
    pub triggered_by: String,
    pub requested_by: Option<String>,
    pub acting_role: RoleCode,
    pub reason: String,
    pub failure_code: Option<DiagnosticCode>,
    pub failure_detail: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub pending_since: Option<DateTime<Utc>>,
    pub containment_completed_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

impl FeatureParityRunView {
    /// Build the frozen wire view, rejecting legacy/incomplete rows that do not
    /// carry the contract hash required to interpret their evidence.
    pub fn try_from_info(info: FeatureParityRunInfo) -> Result<Self, &'static str> {
        let feature_contract_hash = info
            .feature_contract_hash
            .ok_or("parity run is missing feature_contract_hash")?;
        Ok(Self {
            parity_run_id: info.run_id,
            kind: info.kind,
            status: info.status,
            window_start: info.window_start,
            window_end: info.window_end,
            report_id: info.report_id,
            model_version_id: info.model_version_id,
            training_dataset_id: info.training_dataset_id,
            total_count: info.total_count,
            compared_count: info.compared_count,
            matched_count: info.matched_count,
            mismatched_count: info.mismatched_count,
            pending_materialization_count: info.pending_materialization_count,
            feature_contract_hash,
            transform_hash: info.transform_hash,
            triggered_by: info.triggered_by,
            requested_by: info.requested_by,
            acting_role: info.acting_role,
            reason: info.reason,
            failure_code: info.failure_code,
            failure_detail: info.failure_detail,
            started_at: info.started_at,
            pending_since: info.pending_since,
            containment_completed_at: info.containment_completed_at,
            finished_at: info.finished_at,
            created_at: info.created_at,
        })
    }
}

/// Fail-closed runtime latch projection.
#[derive(Debug, Clone, Serialize)]
pub struct FeatureIntegrityLatchView {
    pub open: bool,
    pub blocking_run_id: Option<FeatureParityRunId>,
    pub opened_at: Option<DateTime<Utc>>,
    pub last_acknowledged_at: Option<DateTime<Utc>>,
    pub reason: Option<String>,
}

impl FeatureIntegrityLatchView {
    /// No persisted state is a guarded bootstrap condition, never a clear latch.
    #[must_use]
    pub const fn uninitialized() -> Self {
        Self {
            open: true,
            blocking_run_id: None,
            opened_at: None,
            last_acknowledged_at: None,
            reason: None,
        }
    }
}

impl From<FeatureParityStateInfo> for FeatureIntegrityLatchView {
    fn from(info: FeatureParityStateInfo) -> Self {
        let open = info.state == FeatureParityLatchState::Open;
        Self {
            open,
            blocking_run_id: open.then_some(info.cause_run_id).flatten(),
            opened_at: open.then_some(info.created_at),
            last_acknowledged_at: (!open).then_some(info.created_at),
            reason: Some(info.reason),
        }
    }
}

/// Feature-integrity overview for the operator page.
#[derive(Debug, Clone, Serialize)]
pub struct FeatureIntegritySummaryView {
    pub catalog_coverage_start: Option<DateTime<Utc>>,
    pub catalog_watermark: Option<DateTime<Utc>>,
    pub feature_state_counts: BTreeMap<FeatureCellState, u64>,
    pub rejection_reason_counts: BTreeMap<String, u64>,
    pub last_full_run: Option<FeatureParityRunView>,
    pub last_sampled_run: Option<FeatureParityRunView>,
    pub latch: FeatureIntegrityLatchView,
    pub parity_age_secs: Option<u64>,
}

/// One side of an exact online/replay comparison.
#[derive(Debug, Clone, Serialize)]
pub struct FeatureParityEvidenceView {
    pub state: Option<FeatureCellState>,
    pub value: Option<String>,
    pub effective_at: Option<DateTime<Utc>>,
    pub available_at: Option<DateTime<Utc>>,
    pub cutoff: Option<DateTime<Utc>>,
    pub fingerprint: String,
}

/// One stage-level evidence comparison within a parity run.
#[derive(Debug, Clone, Serialize)]
pub struct FeatureParityEventView {
    pub parity_event_id: FeatureParityEventId,
    pub parity_run_id: FeatureParityRunId,
    pub status: FeatureParityEventStatus,
    pub stage: FeatureParityStage,
    pub decision_at: DateTime<Utc>,
    pub report_id: Option<RecommendationReportId>,
    pub model_run_id: Option<ModelRunId>,
    pub model_version_id: Option<ModelVersionId>,
    pub training_dataset_id: Option<TrainingDatasetId>,
    pub market_id: Option<MarketId>,
    pub feature_name: Option<String>,
    pub reason: Option<String>,
    pub feature_contract_hash: ContentHash,
    pub transform_hash: Option<ContentHash>,
    pub online: FeatureParityEvidenceView,
    pub replay: FeatureParityEvidenceView,
    pub detail: FeatureParityDetail,
    pub created_at: DateTime<Utc>,
}

/// Low-cardinality aggregate counts returned alongside summary metadata.
#[derive(Debug, Clone, Default)]
pub struct FeatureIntegrityCounts {
    pub feature_state_counts: BTreeMap<FeatureCellState, u64>,
    pub rejection_reason_counts: BTreeMap<String, u64>,
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        domain::{api::FeatureParityRunView, quant::FeatureParityRunInfo},
        enums::quant::{FeatureParityRunKind, FeatureParityRunStatus},
        types::{
            ContentHash, DiagnosticCode, FeatureParityRunId, ModelVersionId,
            RecommendationReportId, RoleCode, TrainingDatasetId,
        },
    };

    const HASH: &str = "blake3:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn run_preserves_failure_fields() {
        let now = Utc::now();
        let report_id = RecommendationReportId::from_v7();
        let model_version_id = ModelVersionId::from_v7();
        let training_dataset_id = TrainingDatasetId::from_v7();
        let info = FeatureParityRunInfo {
            run_id: FeatureParityRunId::from_v7(),
            kind: FeatureParityRunKind::Full,
            status: FeatureParityRunStatus::Failed,
            window_start: now,
            window_end: now,
            report_id: Some(report_id),
            model_version_id: Some(model_version_id),
            training_dataset_id: Some(training_dataset_id),
            triggered_by: "manual".to_owned(),
            requested_by: Some("risk-owner".to_owned()),
            acting_role: RoleCode::new("risk_owner"),
            reason: "recovery proof".to_owned(),
            total_count: 1,
            compared_count: 0,
            matched_count: 0,
            mismatched_count: 0,
            pending_materialization_count: 0,
            feature_contract_hash: Some(ContentHash::parse(HASH).expect("canonical hash")),
            transform_hash: None,
            failure_code: Some(DiagnosticCode::new("replay_failed")),
            failure_detail: Some("durable evidence unavailable".to_owned()),
            started_at: Some(now),
            pending_since: Some(now),
            containment_completed_at: Some(now),
            finished_at: Some(now),
            created_at: now,
            updated_at: now,
        };

        let view = FeatureParityRunView::try_from_info(info).expect("complete run view");
        assert_eq!(view.report_id.as_ref(), Some(&report_id));
        assert_eq!(view.model_version_id.as_ref(), Some(&model_version_id));
        assert_eq!(
            view.training_dataset_id.as_ref(),
            Some(&training_dataset_id)
        );
        assert_eq!(view.requested_by.as_deref(), Some("risk-owner"));
        assert_eq!(view.acting_role.as_str(), "risk_owner");
        assert_eq!(view.reason, "recovery proof");
        assert_eq!(
            view.failure_code.as_ref().map(DiagnosticCode::as_str),
            Some("replay_failed")
        );
        assert_eq!(
            view.failure_detail.as_deref(),
            Some("durable evidence unavailable")
        );
        assert_eq!(view.pending_since, Some(now));
        assert_eq!(view.containment_completed_at, Some(now));
    }
}
