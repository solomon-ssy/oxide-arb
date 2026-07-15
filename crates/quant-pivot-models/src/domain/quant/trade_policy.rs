//! Trade-policy artifact persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    entities::{
        quant_trade_policy_artifact, quant_trade_policy_governance_audit,
        quant_trade_policy_validation, quant_trade_policy_validation_row,
    },
    enums::quant::{TradePolicyGovernanceAction, TradePolicyStatus, TradePolicyValidationStatus},
    types::{
        ContentHash, MarketId, TokenId, TradePolicyArtifactId, TradePolicyArtifactPayload,
        TradePolicyGovernanceAuditId, TradePolicyValidationRunId, TrainingDatasetId,
        TrainingExampleId,
    },
};

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_trade_policy_artifact::Entity")]
pub struct TradePolicyArtifactInfo {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub payload_json: TradePolicyArtifactPayload,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

info_from_model!(
    TradePolicyArtifactInfo,
    quant_trade_policy_artifact::Model,
    {
        artifact_id,
        content_hash,
        status,
        source_dataset_id,
        payload_json,
        created_at,
        updated_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_trade_policy_artifact::ActiveModel")]
pub struct NewTradePolicyArtifact {
    pub artifact_id: TradePolicyArtifactId,
    pub content_hash: ContentHash,
    pub status: TradePolicyStatus,
    pub source_dataset_id: TrainingDatasetId,
    pub payload_json: TradePolicyArtifactPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_trade_policy_governance_audit::ActiveModel")]
pub struct NewTradePolicyGovernanceAudit {
    pub audit_id: TradePolicyGovernanceAuditId,
    pub artifact_id: TradePolicyArtifactId,
    pub action: TradePolicyGovernanceAction,
    pub from_status: TradePolicyStatus,
    pub to_status: TradePolicyStatus,
    pub content_hash: ContentHash,
    pub actor_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_trade_policy_validation::ActiveModel")]
pub struct NewTradePolicyValidationRun {
    pub validation_run_id: TradePolicyValidationRunId,
    pub artifact_id: TradePolicyArtifactId,
    pub artifact_hash: ContentHash,
    pub source_dataset_id: TrainingDatasetId,
    pub source_dataset_hash: ContentHash,
    pub source_slice_manifest_hash: ContentHash,
    pub evidence_manifest_hash: ContentHash,
    pub status: TradePolicyValidationStatus,
    pub actor_id: Uuid,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_trade_policy_validation_row::ActiveModel")]
pub struct NewTradePolicyValidationRow {
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
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_trade_policy_validation::Entity")]
pub struct TradePolicyValidationRunInfo {
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
    pub actor_id: Uuid,
    pub reason: String,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    TradePolicyValidationRunInfo,
    quant_trade_policy_validation::Model,
    {
        validation_run_id,
        artifact_id,
        artifact_hash,
        source_dataset_id,
        source_dataset_hash,
        source_slice_manifest_hash,
        evidence_manifest_hash,
        status,
        total_rows,
        passed_rows,
        failed_rows,
        validation_hash,
        failure_detail,
        actor_id,
        reason,
        started_at,
        completed_at,
        created_at,
    }
);

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_trade_policy_validation_row::Entity")]
pub struct TradePolicyValidationRowInfo {
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

info_from_model!(
    TradePolicyValidationRowInfo,
    quant_trade_policy_validation_row::Model,
    {
        validation_run_id,
        row_ordinal,
        evidence_kind,
        record_key,
        example_id,
        market_id,
        token_id,
        decision_at,
        expected_row_hash,
        actual_row_hash,
        passed,
        diagnostic_kind,
        detail,
        row_hash,
        created_at,
    }
);

#[derive(Debug, Clone)]
pub struct CompleteTradePolicyValidation {
    pub total_rows: i64,
    pub passed_rows: i64,
    pub validation_hash: ContentHash,
    pub audit: NewTradePolicyGovernanceAudit,
}

#[derive(Debug, Clone)]
pub struct FailTradePolicyValidation {
    pub status: TradePolicyValidationStatus,
    pub validation_hash: ContentHash,
    pub failure_detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_trade_policy_governance_audit::Entity")]
pub struct TradePolicyGovernanceAuditInfo {
    pub audit_id: TradePolicyGovernanceAuditId,
    pub artifact_id: TradePolicyArtifactId,
    pub action: TradePolicyGovernanceAction,
    pub from_status: TradePolicyStatus,
    pub to_status: TradePolicyStatus,
    pub content_hash: ContentHash,
    pub actor_id: Uuid,
    pub reason: String,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    TradePolicyGovernanceAuditInfo,
    quant_trade_policy_governance_audit::Model,
    {
        audit_id,
        artifact_id,
        action,
        from_status,
        to_status,
        content_hash,
        actor_id,
        reason,
        created_at,
    }
);
