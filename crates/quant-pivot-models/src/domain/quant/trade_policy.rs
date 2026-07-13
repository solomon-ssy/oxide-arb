//! Trade-policy artifact persistence DTOs.

use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    enums::quant::{TradePolicyGovernanceAction, TradePolicyStatus},
    types::{
        ContentHash, TradePolicyArtifactId, TradePolicyArtifactPayload,
        TradePolicyGovernanceAuditId, TrainingDatasetId,
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
    crate::entities::quant_trade_policy_artifact::Model,
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
    crate::entities::quant_trade_policy_governance_audit::Model,
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
