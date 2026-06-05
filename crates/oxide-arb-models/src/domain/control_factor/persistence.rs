//! Persistence projections and write DTOs for the control-factor registry.

use super::{
    materialization::StageReportBody, publication::ControlFactorPublication,
    value::ControlFactorValue,
};
use crate::{
    enums::control_factor::{
        AuditResourceType, ControlAuditEventType, ControlFactorType, EvidenceStageStatus,
        FactorStatus, MaterializationOutputPolicy, MaterializationRunKind,
        MaterializationRunStatus, MaterializationStageName, OperatorRole, PublicationMode,
        PublicationStatus, RunTriggerType,
    },
    hashing::CanonicalDigest,
    types::{
        AuditEventId, ControlFactorId, FactorPublicationId, MaterializationRunId, StageReportId,
    },
};
use chrono::{DateTime, Utc};
use oxide_arb_error::control::{
    CanonicalDigestError, ControlPersistenceError, FactorValueError, GovernanceError,
    SnapshotBuildError,
};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

/// DB row projection for `control_factor_value`.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_value::Entity")]
pub struct ControlFactorValueInfo {
    pub factor_id: ControlFactorId,
    pub run_id: MaterializationRunId,
    pub factor_type: ControlFactorType,
    pub dimensions: serde_json::Value,
    pub dimensions_hash: String,
    pub payload: serde_json::Value,
    pub payload_hash: String,
    pub evidence: serde_json::Value,
    pub status: FactorStatus,
    pub status_reason: Option<String>,
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
        run_id,
        factor_type,
        dimensions,
        dimensions_hash,
        payload,
        payload_hash,
        evidence,
        status,
        status_reason,
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

    /// Recomputes canonical dimension/payload digests and compares them to the
    /// persisted hashes. Tampered rows are rejected at snapshot compile time.
    pub fn verify_stored_hashes(&self) -> Result<(), SnapshotBuildError> {
        let factor_id = self.factor_id.as_str().to_owned();
        let typed =
            self.to_typed()
                .map_err(|error| SnapshotBuildError::DimensionPayloadMismatch {
                    factor_id: format!("{factor_id}: {error}"),
                })?;
        let expected_payload = CanonicalDigest::blake3_json(&typed.payload).map_err(|source| {
            SnapshotBuildError::PayloadHashMismatch {
                factor_id: factor_id.clone(),
                expected: String::new(),
                actual: format!("digest failed: {source}"),
            }
        })?;
        if self.payload_hash != expected_payload {
            return Err(SnapshotBuildError::PayloadHashMismatch {
                factor_id,
                expected: expected_payload,
                actual: self.payload_hash.clone(),
            });
        }
        let expected_dims = CanonicalDigest::blake3_json(&typed.dimensions).map_err(|source| {
            SnapshotBuildError::DimensionsHashMismatch {
                factor_id: self.factor_id.as_str().to_owned(),
                expected: String::new(),
                actual: format!("digest failed: {source}"),
            }
        })?;
        if self.dimensions_hash != expected_dims {
            return Err(SnapshotBuildError::DimensionsHashMismatch {
                factor_id,
                expected: expected_dims,
                actual: self.dimensions_hash.clone(),
            });
        }
        Ok(())
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
    pub run_id: MaterializationRunId,
    pub factor_type: ControlFactorType,
    pub dimensions: serde_json::Value,
    pub dimensions_hash: String,
    pub payload: serde_json::Value,
    pub payload_hash: String,
    pub evidence: serde_json::Value,
    pub status: FactorStatus,
    pub status_reason: Option<String>,
    pub generated_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub owner: String,
    pub schema_version: i32,
}

impl NewControlFactorValue {
    /// Builds an insert row from a typed factor, deriving `run_id` from evidence
    /// and computing canonical `dimensions_hash` / `payload_hash` for dedupe.
    pub fn from_typed(
        value: &ControlFactorValue,
        status_reason: Option<String>,
    ) -> Result<Self, CanonicalDigestError> {
        let dimensions = serde_json::to_value(&value.dimensions)
            .map_err(|error| CanonicalDigestError::Serialize(error.to_string()))?;
        let payload = serde_json::to_value(&value.payload)
            .map_err(|error| CanonicalDigestError::Serialize(error.to_string()))?;
        let evidence = serde_json::to_value(&value.evidence)
            .map_err(|error| CanonicalDigestError::Serialize(error.to_string()))?;
        Ok(Self {
            factor_id: value.factor_id.clone(),
            run_id: value.evidence.materialization_run_id.clone(),
            factor_type: value.factor_type,
            dimensions_hash: CanonicalDigest::blake3_json(&value.dimensions)?,
            dimensions,
            payload_hash: CanonicalDigest::blake3_json(&value.payload)?,
            payload,
            evidence,
            status: value.status,
            status_reason,
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
    pub idempotency_key: String,
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
    pub idempotency_key: String,
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
            idempotency_key: value.idempotency_key.clone(),
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

/// DB row projection for `control_factor_audit_event` (full chain row).
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::control_factor_audit_event::Entity")]
pub struct ControlFactorAuditEventInfo {
    pub event_id: AuditEventId,
    pub sequence: i64,
    pub event_type: ControlAuditEventType,
    pub actor: String,
    pub actor_role: OperatorRole,
    pub resource_type: AuditResourceType,
    pub resource_id: String,
    pub request_id: String,
    pub reason: String,
    pub before_hash: Option<String>,
    pub after_hash: Option<String>,
    pub diff: serde_json::Value,
    pub prev_event_hash: Option<String>,
    pub event_hash: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ControlFactorAuditEventInfo,
    crate::entities::control_factor_audit_event::Model,
    {
        event_id,
        sequence,
        event_type,
        actor,
        actor_role,
        resource_type,
        resource_id,
        request_id,
        reason,
        before_hash,
        after_hash,
        diff,
        prev_event_hash,
        event_hash,
        created_at,
    }
);

/// Semantic content of a governance audit event.
///
/// Chain fields (`event_id`, `sequence`, `prev_event_hash`, `event_hash`,
/// `created_at`) are assigned atomically by the repository under the audit-chain
/// advisory lock, so this DTO intentionally omits them and is **not** an
/// `IntoActiveModel`.
#[derive(Debug, Clone)]
pub struct NewControlFactorAuditEvent {
    pub event_type: ControlAuditEventType,
    pub actor: String,
    pub actor_role: OperatorRole,
    pub resource_type: AuditResourceType,
    pub resource_id: String,
    pub request_id: String,
    pub reason: String,
    pub before_hash: Option<String>,
    pub after_hash: Option<String>,
    pub diff: serde_json::Value,
}

/// Mutation envelope for governance operations.
///
/// Carries who acted, at what role, the request id, and the human reason. Role
/// *authorization* (which role may invoke which operation) is enforced at the
/// transport boundary (oxide-arb-web RBAC); this envelope only carries the
/// attribution recorded into the audit chain and is reused for multi-resource
/// sweeps (e.g. TTL expiry).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditActor {
    pub actor: String,
    pub actor_role: OperatorRole,
    pub request_id: String,
    pub reason: String,
}

impl AuditActor {
    /// Validates envelope integrity (non-empty actor / `request_id` / reason).
    pub fn validate(&self) -> Result<(), GovernanceError> {
        if self.actor.trim().is_empty() {
            return Err(GovernanceError::MissingField { field: "actor" });
        }
        if self.request_id.trim().is_empty() {
            return Err(GovernanceError::MissingField {
                field: "request_id",
            });
        }
        if self.reason.trim().is_empty() {
            return Err(GovernanceError::MissingReason);
        }
        Ok(())
    }
}

/// Outcome of an idempotent publish.
#[derive(Debug, Clone)]
pub enum PublishPublicationOutcome {
    /// Created and activated a new publication.
    Published(ControlFactorPublicationInfo),
    /// Returned an existing publication matching the idempotency key.
    AlreadyApplied(ControlFactorPublicationInfo),
}

impl PublishPublicationOutcome {
    #[must_use]
    pub const fn publication(&self) -> &ControlFactorPublicationInfo {
        match self {
            Self::Published(info) | Self::AlreadyApplied(info) => info,
        }
    }
}

/// Outcome of a governed TTL expiry sweep.
#[derive(Debug, Clone, Default)]
pub struct ExpireFactorsOutcome {
    pub expired: Vec<ControlFactorId>,
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

#[cfg(test)]
mod tests {
    use super::AuditActor;
    use crate::enums::control_factor::OperatorRole;
    use oxide_arb_error::control::GovernanceError;

    fn envelope() -> AuditActor {
        AuditActor {
            actor: "op".into(),
            actor_role: OperatorRole::Operator,
            request_id: "req-1".into(),
            reason: "because".into(),
        }
    }

    #[test]
    fn audit_actor_validates_complete_envelope() {
        assert!(envelope().validate().is_ok());
    }

    #[test]
    fn audit_actor_requires_reason() {
        let envelope = AuditActor {
            reason: "   ".into(),
            ..envelope()
        };
        assert!(matches!(
            envelope.validate(),
            Err(GovernanceError::MissingReason)
        ));
    }

    #[test]
    fn audit_actor_requires_identity_fields() {
        let missing_actor = AuditActor {
            actor: String::new(),
            ..envelope()
        };
        assert!(matches!(
            missing_actor.validate(),
            Err(GovernanceError::MissingField { field: "actor" })
        ));
        let missing_request = AuditActor {
            request_id: String::new(),
            ..envelope()
        };
        assert!(matches!(
            missing_request.validate(),
            Err(GovernanceError::MissingField {
                field: "request_id"
            })
        ));
    }
}
