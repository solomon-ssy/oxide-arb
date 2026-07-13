//! Feature-parity run and governed latch persistence contracts.

use crate::{
    entities::{quant_feature_parity_run, quant_feature_parity_state},
    enums::quant::{
        FeatureParityLatchState, FeatureParityRunKind, FeatureParityRunStatus,
        FeatureParityStateTransition,
    },
    types::{
        ContentHash, FeatureParityRunId, FeatureParityStateId, ModelVersionId,
        RecommendationReportId, TrainingDatasetId,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// One persisted deterministic parity replay.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
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

/// One immutable transition of the admission latch.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
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
