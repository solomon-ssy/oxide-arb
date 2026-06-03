//! Persistence projections and write DTOs for the control-factor registry.

use super::{
    materialization::StageReportBody, publication::ControlFactorPublication,
    value::ControlFactorValue,
};
use crate::{
    enums::control_factor::{
        ControlAuditEventType, ControlFactorType, EvidenceStageStatus, FactorStatus,
        MaterializationOutputPolicy, MaterializationRunKind, MaterializationRunStatus,
        MaterializationStageName, PublicationMode, PublicationStatus, RunTriggerType,
    },
    types::{ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId},
};
use chrono::{DateTime, Utc};
use oxide_arb_error::control::{ControlPersistenceError, FactorValueError};
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
    pub run_dedupe_key: Option<String>,
    pub run_kind: MaterializationRunKind,
    pub trigger_type: RunTriggerType,
    pub trigger_ref: Option<String>,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub market_filter: serde_json::Value,
    pub requested_factor_types: serde_json::Value,
    pub data_requirements: serde_json::Value,
    pub runtime_config_ref: serde_json::Value,
    pub simulation_config_hash: String,
    pub quality_gate_policy_hash: String,
    pub output_policy: MaterializationOutputPolicy,
    pub manifest: serde_json::Value,
    pub manifest_hash: String,
    pub report: serde_json::Value,
    pub code_git_sha: String,
    pub created_by: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub report_uri: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorMaterializationRunInfo,
    crate::entities::control_factor_materialization_run::Model,
    {
        materialization_run_id,
        run_dedupe_key,
        run_kind,
        trigger_type,
        trigger_ref,
        status,
        window_from,
        window_to,
        source_delay_secs,
        market_filter,
        requested_factor_types,
        data_requirements,
        runtime_config_ref,
        simulation_config_hash,
        quality_gate_policy_hash,
        output_policy,
        manifest,
        manifest_hash,
        report,
        code_git_sha,
        created_by,
        started_at,
        finished_at,
        failure_code,
        failure_detail,
        report_uri,
        created_at,
        updated_at,
    }
);

/// Insert payload for `control_factor_materialization_run`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_materialization_run::ActiveModel")]
pub struct NewControlFactorMaterializationRun {
    pub materialization_run_id: MaterializationRunId,
    pub run_dedupe_key: Option<String>,
    pub run_kind: MaterializationRunKind,
    pub trigger_type: RunTriggerType,
    pub trigger_ref: Option<String>,
    pub status: MaterializationRunStatus,
    pub window_from: DateTime<Utc>,
    pub window_to: DateTime<Utc>,
    pub source_delay_secs: i64,
    pub market_filter: serde_json::Value,
    pub requested_factor_types: serde_json::Value,
    pub data_requirements: serde_json::Value,
    pub runtime_config_ref: serde_json::Value,
    pub simulation_config_hash: String,
    pub quality_gate_policy_hash: String,
    pub output_policy: MaterializationOutputPolicy,
    pub manifest: serde_json::Value,
    pub manifest_hash: String,
    pub report: serde_json::Value,
    pub code_git_sha: String,
    pub created_by: String,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub report_uri: Option<String>,
}

/// DB row projection for `control_factor_stage_report`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_stage_report::Entity")]
pub struct ControlFactorStageReportInfo {
    pub stage_report_id: StageReportId,
    pub materialization_run_id: MaterializationRunId,
    pub stage_name: MaterializationStageName,
    pub status: EvidenceStageStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub input_artifact_hashes: serde_json::Value,
    pub output_artifact_hash: Option<String>,
    pub coverage: serde_json::Value,
    pub metrics: serde_json::Value,
    pub records_read: i64,
    pub records_written: i64,
    pub warnings: serde_json::Value,
    pub errors: serde_json::Value,
    pub query_fingerprints: serde_json::Value,
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
        started_at,
        finished_at,
        input_artifact_hashes,
        output_artifact_hash,
        coverage,
        metrics,
        records_read,
        records_written,
        warnings,
        errors,
        query_fingerprints,
        created_at,
    }
);

/// Insert payload for `control_factor_stage_report`.
#[derive(Debug, Clone, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::control_factor_stage_report::ActiveModel")]
pub struct NewControlFactorStageReport {
    pub stage_report_id: StageReportId,
    pub materialization_run_id: MaterializationRunId,
    pub stage_name: MaterializationStageName,
    pub status: EvidenceStageStatus,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub input_artifact_hashes: serde_json::Value,
    pub output_artifact_hash: Option<String>,
    pub coverage: serde_json::Value,
    pub metrics: serde_json::Value,
    pub records_read: i64,
    pub records_written: i64,
    pub warnings: serde_json::Value,
    pub errors: serde_json::Value,
    pub query_fingerprints: serde_json::Value,
}

impl TryFrom<&StageReportBody> for NewControlFactorStageReport {
    type Error = ControlPersistenceError;

    fn try_from(report: &StageReportBody) -> Result<Self, Self::Error> {
        Ok(Self {
            stage_report_id: report.stage_report_id.clone(),
            materialization_run_id: report.run_id.clone(),
            stage_name: report.stage_name,
            status: report.status,
            started_at: report.started_at,
            finished_at: report.finished_at,
            input_artifact_hashes: encode_stage_json_field(
                "input_artifact_hashes",
                &report.input_artifact_hashes,
            )?,
            output_artifact_hash: report
                .output_artifact_hash
                .as_ref()
                .map(|hash| hash.0.clone()),
            coverage: encode_stage_json_field("coverage", &report.coverage)?,
            metrics: report.metrics.clone(),
            records_read: checked_u64_to_i64("records_read", report.records_read)?,
            records_written: checked_u64_to_i64("records_written", report.records_written)?,
            warnings: encode_stage_json_field("warnings", &report.warnings)?,
            errors: encode_stage_json_field("errors", &report.errors)?,
            query_fingerprints: encode_stage_json_field(
                "query_fingerprints",
                &report.query_fingerprints,
            )?,
        })
    }
}

#[inline]
fn encode_stage_json_field<T: Serialize>(
    field: &'static str,
    value: &T,
) -> Result<serde_json::Value, ControlPersistenceError> {
    serde_json::to_value(value).map_err(|error| ControlPersistenceError::Encode {
        field,
        message: error.to_string(),
    })
}

#[inline]
fn checked_u64_to_i64(field: &'static str, value: u64) -> Result<i64, ControlPersistenceError> {
    i64::try_from(value).map_err(|_| ControlPersistenceError::IntegerOverflow { field, value })
}

#[derive(Debug, Clone)]
pub struct EnqueueMaterializationRunOptions {
    pub force_new_run: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub enum EnqueueMaterializationRunOutcome {
    Created(ControlFactorMaterializationRunInfo),
    DuplicateActive(ControlFactorMaterializationRunInfo),
    DuplicateCompleted(ControlFactorMaterializationRunInfo),
}

#[derive(Debug, Clone)]
pub enum AcquireMaterializationRunOutcome {
    Acquired(ControlFactorMaterializationRunInfo),
    NotQueued(ControlFactorMaterializationRunInfo),
    NotFound,
}

#[derive(Debug, Clone)]
pub struct MaterializationRunStatusPatch {
    pub finished_at: Option<DateTime<Utc>>,
    pub failure_code: Option<String>,
    pub failure_detail: Option<String>,
    pub report: Option<serde_json::Value>,
    pub report_uri: Option<String>,
}

#[derive(Debug, Clone)]
pub enum RunTransitionOutcome {
    Transitioned(Box<ControlFactorMaterializationRunInfo>),
    InvalidTransition {
        current_status: MaterializationRunStatus,
    },
    NotFound,
}

#[derive(Debug, Clone)]
pub enum CancelMaterializationRunOutcome {
    Cancelled(ControlFactorMaterializationRunInfo),
    AlreadyTerminal(ControlFactorMaterializationRunInfo),
    NotFound,
}
