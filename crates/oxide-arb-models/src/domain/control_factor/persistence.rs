//! Persistence projections and write DTOs for the control-factor registry.

use super::{publication::ControlFactorPublication, value::ControlFactorValue};
use crate::{
    enums::control_factor::{
        ControlAuditEventType, ControlFactorType, EvidenceStageStatus, FactorStatus,
        MaterializationRunStatus, PublicationMode, PublicationStatus,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::control::FactorValueError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// DB row projection for `control_factor_value`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_value::Entity")]
pub struct ControlFactorValueInfo {
    pub factor_id: ControlFactorId,
    pub factor_type: ControlFactorType,
    pub dimensions: serde_json::Value,
    pub payload: serde_json::Value,
    pub evidence: serde_json::Value,
    pub status: FactorStatus,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub owner: String,
    pub schema_version: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorValueInfo,
    crate::entities::control_factor_value::Model,
    {
        factor_id,
        factor_type,
        dimensions,
        payload,
        evidence,
        status,
        generated_at,
        expires_at,
        owner,
        schema_version,
        created_at,
        updated_at,
    }
);

impl TryFrom<ControlFactorValueInfo> for ControlFactorValue {
    type Error = FactorValueError;

    fn try_from(info: ControlFactorValueInfo) -> Result<Self, Self::Error> {
        Self::from_info(&info)
    }
}

impl ControlFactorValueInfo {
    pub fn to_typed(&self) -> Result<ControlFactorValue, FactorValueError> {
        ControlFactorValue::from_info(self)
    }
}

impl ControlFactorValue {
    pub fn from_info(info: &ControlFactorValueInfo) -> Result<Self, FactorValueError> {
        let dimensions = decode_json_field("dimensions", &info.dimensions)?;
        let payload = decode_json_field("payload", &info.payload)?;
        let evidence = decode_json_field("evidence", &info.evidence)?;
        let schema_version = u32::try_from(info.schema_version).map_err(|error| {
            FactorValueError::TypedRowDecode {
                field: "schema_version",
                message: error.to_string(),
            }
        })?;
        Ok(Self {
            factor_id: info.factor_id.clone(),
            factor_type: info.factor_type,
            dimensions,
            payload,
            evidence,
            status: info.status,
            generated_at: info.generated_at,
            expires_at: info.expires_at,
            owner: info.owner.clone(),
            schema_version,
        })
    }
}

fn decode_json_field<T: serde::de::DeserializeOwned>(
    field: &'static str,
    value: &serde_json::Value,
) -> Result<T, FactorValueError> {
    serde_json::from_value(value.clone()).map_err(|error| FactorValueError::TypedRowDecode {
        field,
        message: error.to_string(),
    })
}

/// Insert payload for `control_factor_value`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_value::ActiveModel")]
pub struct NewControlFactorValue {
    pub factor_id: ControlFactorId,
    pub factor_type: ControlFactorType,
    pub dimensions: serde_json::Value,
    pub payload: serde_json::Value,
    pub evidence: serde_json::Value,
    pub status: FactorStatus,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub owner: String,
    pub schema_version: i32,
}

impl NewControlFactorValue {
    pub fn from_typed(value: &ControlFactorValue) -> Result<Self, serde_json::Error> {
        Ok(Self {
            factor_id: value.factor_id.clone(),
            factor_type: value.factor_type,
            dimensions: serde_json::to_value(&value.dimensions)?,
            payload: serde_json::to_value(&value.payload)?,
            evidence: serde_json::to_value(&value.evidence)?,
            status: value.status,
            generated_at: value.generated_at,
            expires_at: value.expires_at,
            owner: value.owner.clone(),
            schema_version: i32::try_from(value.schema_version).unwrap_or(i32::MAX),
        })
    }
}

/// DB row projection for `control_factor_publication`, enriched with factor IDs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlFactorPublicationInfo {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub factor_ids: Vec<ControlFactorId>,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ControlFactorPublicationInfo {
    #[must_use]
    pub fn to_publication(&self) -> ControlFactorPublication {
        ControlFactorPublication {
            publication_id: self.publication_id.clone(),
            mode: self.mode,
            factor_ids: self.factor_ids.clone(),
            previous_publication_id: self.previous_publication_id.clone(),
            status: self.status,
            effective_from: self.effective_from,
            expires_at: self.expires_at,
            approved_by: self.approved_by.clone(),
            approval_reason: self.approval_reason.clone(),
            publication_hash: self.publication_hash.clone(),
        }
    }
}

/// Raw DB row projection for `control_factor_publication`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_publication::Entity")]
pub struct ControlFactorPublicationRowInfo {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorPublicationRowInfo,
    crate::entities::control_factor_publication::Model,
    {
        publication_id,
        mode,
        previous_publication_id,
        status,
        effective_from,
        expires_at,
        approved_by,
        approval_reason,
        publication_hash,
        created_at,
        updated_at,
    }
);

impl ControlFactorPublicationRowInfo {
    #[must_use]
    pub fn with_factor_ids(self, factor_ids: Vec<ControlFactorId>) -> ControlFactorPublicationInfo {
        ControlFactorPublicationInfo {
            publication_id: self.publication_id,
            mode: self.mode,
            factor_ids,
            previous_publication_id: self.previous_publication_id,
            status: self.status,
            effective_from: self.effective_from,
            expires_at: self.expires_at,
            approved_by: self.approved_by,
            approval_reason: self.approval_reason,
            publication_hash: self.publication_hash,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

/// Insert payload for `control_factor_publication` plus its factor membership.
#[derive(Debug, Clone)]
pub struct NewControlFactorPublication {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub factor_ids: Vec<ControlFactorId>,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
}

/// Insert payload for the publication row only.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_publication::ActiveModel")]
pub struct NewControlFactorPublicationRow {
    pub publication_id: FactorPublicationId,
    pub mode: PublicationMode,
    pub previous_publication_id: Option<FactorPublicationId>,
    pub status: PublicationStatus,
    pub effective_from: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub approved_by: Option<String>,
    pub approval_reason: String,
    pub publication_hash: String,
}

impl From<&NewControlFactorPublication> for NewControlFactorPublicationRow {
    fn from(value: &NewControlFactorPublication) -> Self {
        Self {
            publication_id: value.publication_id.clone(),
            mode: value.mode,
            previous_publication_id: value.previous_publication_id.clone(),
            status: value.status,
            effective_from: value.effective_from,
            expires_at: value.expires_at,
            approved_by: value.approved_by.clone(),
            approval_reason: value.approval_reason.clone(),
            publication_hash: value.publication_hash.clone(),
        }
    }
}

/// Insert payload for `control_factor_publication_factor`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_publication_factor::ActiveModel")]
pub struct NewControlFactorPublicationFactor {
    pub publication_id: FactorPublicationId,
    pub factor_id: ControlFactorId,
}

/// DB row projection for `control_factor_audit_event`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_audit_event::Entity")]
pub struct ControlFactorAuditEventInfo {
    pub id: i64,
    pub event_type: ControlAuditEventType,
    pub factor_id: Option<ControlFactorId>,
    pub publication_id: Option<FactorPublicationId>,
    pub actor: String,
    pub reason: String,
    pub payload: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorAuditEventInfo,
    crate::entities::control_factor_audit_event::Model,
    {
        id,
        event_type,
        factor_id,
        publication_id,
        actor,
        reason,
        payload,
        created_at,
    }
);

/// Insert payload for `control_factor_audit_event`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_audit_event::ActiveModel")]
pub struct NewControlFactorAuditEvent {
    pub event_type: ControlAuditEventType,
    pub factor_id: Option<ControlFactorId>,
    pub publication_id: Option<FactorPublicationId>,
    pub actor: String,
    pub reason: String,
    pub payload: serde_json::Value,
}

/// DB row projection for `control_factor_materialization_run`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_materialization_run::Entity")]
pub struct ControlFactorMaterializationRunInfo {
    pub materialization_run_id: MaterializationRunId,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub manifest: serde_json::Value,
    pub report: serde_json::Value,
    pub code_git_sha: String,
    pub query_fingerprint: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorMaterializationRunInfo,
    crate::entities::control_factor_materialization_run::Model,
    {
        materialization_run_id,
        status,
        window_from,
        window_to,
        source_delay_secs,
        manifest,
        report,
        code_git_sha,
        query_fingerprint,
        started_at,
        finished_at,
        created_at,
        updated_at,
    }
);

/// Insert payload for `control_factor_materialization_run`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_materialization_run::ActiveModel")]
pub struct NewControlFactorMaterializationRun {
    pub materialization_run_id: MaterializationRunId,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub manifest: serde_json::Value,
    pub report: serde_json::Value,
    pub code_git_sha: String,
    pub query_fingerprint: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
}

/// DB row projection for `control_factor_stage_report`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_stage_report::Entity")]
pub struct ControlFactorStageReportInfo {
    pub stage_report_id: StageReportId,
    pub materialization_run_id: MaterializationRunId,
    pub stage_name: String,
    pub status: EvidenceStageStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub coverage: serde_json::Value,
    pub warnings: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorStageReportInfo,
    crate::entities::control_factor_stage_report::Model,
    {
        stage_report_id,
        materialization_run_id,
        stage_name,
        status,
        window_from,
        window_to,
        coverage,
        warnings,
        created_at,
    }
);

/// Insert payload for `control_factor_stage_report`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_stage_report::ActiveModel")]
pub struct NewControlFactorStageReport {
    pub stage_report_id: StageReportId,
    pub materialization_run_id: MaterializationRunId,
    pub stage_name: String,
    pub status: EvidenceStageStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub coverage: serde_json::Value,
    pub warnings: serde_json::Value,
}
