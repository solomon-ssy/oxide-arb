//! Persistence DTOs for append-only operational readiness evidence.

use crate::{
    entities::quant_research_readiness_evidence,
    enums::quant::ResearchReadinessEvidenceKind,
    types::{
        ArtifactUri, ArtifactVersion, AttestationKeyId, ContentHash, ResearchReadinessEvidenceId,
        ResearchReadinessEvidencePayload,
    },
};
use chrono::{DateTime, Utc};
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_research_readiness_evidence::ActiveModel")]
pub struct NewResearchReadinessEvidence {
    pub evidence_id: ResearchReadinessEvidenceId,
    pub kind: ResearchReadinessEvidenceKind,
    pub scope_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub payload_json: ResearchReadinessEvidencePayload,
    pub payload_hash: ContentHash,
    pub artifact_uri: ArtifactUri,
    pub artifact_version: ArtifactVersion,
    pub attestation_key_id: AttestationKeyId,
    pub attestation_mac: ContentHash,
}

#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel)]
#[sea_orm(entity = "crate::entities::quant_research_readiness_evidence::Entity")]
pub struct ResearchReadinessEvidenceInfo {
    pub evidence_id: ResearchReadinessEvidenceId,
    pub kind: ResearchReadinessEvidenceKind,
    pub scope_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub payload_json: ResearchReadinessEvidencePayload,
    pub payload_hash: ContentHash,
    pub artifact_uri: ArtifactUri,
    pub artifact_version: ArtifactVersion,
    pub attestation_key_id: AttestationKeyId,
    pub attestation_mac: ContentHash,
    pub created_at: DateTime<Utc>,
}

info_from_model!(
    ResearchReadinessEvidenceInfo,
    quant_research_readiness_evidence::Model,
    {
        evidence_id,
        kind,
        scope_hash,
        window_start,
        window_end,
        observed_at,
        expires_at,
        payload_json,
        payload_hash,
        artifact_uri,
        artifact_version,
        attestation_key_id,
        attestation_mac,
        created_at,
    }
);
