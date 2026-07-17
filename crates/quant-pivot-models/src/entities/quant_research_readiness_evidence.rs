//! Append-only operational evidence consumed by research preflight.

use chrono::{DateTime, Utc};
use sea_orm::entity::prelude::*;

use crate::{
    enums::quant::ResearchReadinessEvidenceKind,
    types::{ArtifactUri, ContentHash, ResearchReadinessEvidencePayload},
};

#[sea_orm::model]
#[derive(Clone, Debug, PartialEq, Eq, DeriveEntityModel)]
#[sea_orm(table_name = "quant_research_readiness_evidence")]
pub struct Model {
    #[sea_orm(primary_key, auto_increment = false)]
    pub evidence_id: Uuid,
    pub kind: ResearchReadinessEvidenceKind,
    pub scope_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    #[sea_orm(column_type = "JsonBinary")]
    pub payload_json: ResearchReadinessEvidencePayload,
    pub payload_hash: ContentHash,
    pub artifact_uri: ArtifactUri,
    pub artifact_version: String,
    pub attestation_key_id: String,
    pub attestation_mac: ContentHash,
    pub created_at: DateTime<Utc>,
}

impl ActiveModelBehavior for ActiveModel {}
