//! Report-level data-quality snapshot persistence DTOs.

use crate::{
    entities::quant_report_data_quality_snapshot,
    types::{DecisionPolicySnapshotId, ReportDataQualitySnapshotId, ReportDataQualityTokens},
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

/// Persisted per-fire data-quality snapshot (accepted + rejected markets).
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_report_data_quality_snapshot::Entity")]
pub struct ReportDataQualitySnapshotInfo {
    pub report_data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub decision_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub tokens_json: ReportDataQualityTokens,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ReportDataQualitySnapshotInfo,
    quant_report_data_quality_snapshot::Model,
    {
        report_data_quality_snapshot_id,
        decision_at,
        decision_policy_snapshot_id,
        tokens_json,
        created_at,
    }
);

/// Insert payload for `quant_report_data_quality_snapshot`.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_report_data_quality_snapshot::ActiveModel")]
pub struct NewReportDataQualitySnapshot {
    pub report_data_quality_snapshot_id: ReportDataQualitySnapshotId,
    pub decision_at: DateTime<Utc>,
    pub decision_policy_snapshot_id: DecisionPolicySnapshotId,
    pub tokens_json: ReportDataQualityTokens,
}
