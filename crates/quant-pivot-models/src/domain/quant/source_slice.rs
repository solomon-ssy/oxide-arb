//! Source-slice materialization persistence DTOs.

use chrono::{DateTime, Utc};
use quant_pivot_error::hashing::CanonicalDigestError;
use sea_orm::{DeriveIntoActiveModel, DerivePartialModel, FromQueryResult};
use serde::{Deserialize, Serialize};

use crate::{
    entities::quant_source_slice,
    enums::quant::SourceSliceStatus,
    hashing::CanonicalDigest,
    types::{
        ArtifactUri, ContentHash, ResearchEvaluationTrack, ResearchProfileRef,
        RuntimeConfigVersionId, SourceSliceId, SourceSliceManifestRef, SourceSliceManifestV2,
    },
};

pub const SOURCE_SLICE_READER_CONTRACT_V2: &str = "source_slice_reader_v2";
pub const SOURCE_SLICE_SCHEMA_CONTRACT_V2: &str = "source_slice_schema_v2";

/// Server-derived semantic fields that determine one Source Slice identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSliceIdentityInput {
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: ContentHash,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
}

/// Canonical identity frozen before any source object is read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSliceIdentity {
    pub identity_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: ResearchEvaluationTrack,
    pub research_program_hash: ContentHash,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub reader_contract_version: String,
    pub schema_contract_version: String,
}

impl SourceSliceIdentity {
    pub fn derive(input: SourceSliceIdentityInput) -> Result<Self, CanonicalDigestError> {
        let SourceSliceIdentityInput {
            profile_ref,
            evaluation_track,
            research_program_hash,
            runtime_config_version_id,
            runtime_config_hash,
            window_start,
            window_end,
            pit_cutoff,
        } = input;
        let reader_contract_version = SOURCE_SLICE_READER_CONTRACT_V2.to_owned();
        let schema_contract_version = SOURCE_SLICE_SCHEMA_CONTRACT_V2.to_owned();
        let identity_hash = CanonicalDigest::content_hash_json(&(
            &profile_ref,
            evaluation_track,
            &research_program_hash,
            &runtime_config_version_id,
            &runtime_config_hash,
            window_start,
            window_end,
            pit_cutoff,
            &reader_contract_version,
            &schema_contract_version,
        ))?;
        Ok(Self {
            identity_hash,
            profile_ref,
            evaluation_track,
            research_program_hash,
            runtime_config_version_id,
            runtime_config_hash,
            window_start,
            window_end,
            pit_cutoff,
            reader_contract_version,
            schema_contract_version,
        })
    }
}

/// Insert payload for the deduplicated materialization claim.
#[derive(Debug, Clone, Serialize, Deserialize, DeriveIntoActiveModel)]
#[sea_orm(active_model = "crate::entities::quant_source_slice::ActiveModel")]
pub struct NewSourceSlice {
    pub source_slice_id: SourceSliceId,
    pub identity_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: String,
    pub research_program_hash: ContentHash,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub reader_contract_version: String,
    pub schema_contract_version: String,
}

impl NewSourceSlice {
    #[must_use]
    pub fn from_identity(source_slice_id: SourceSliceId, identity: SourceSliceIdentity) -> Self {
        Self {
            source_slice_id,
            identity_hash: identity.identity_hash,
            profile_ref: identity.profile_ref,
            evaluation_track: evaluation_track_name(identity.evaluation_track).to_owned(),
            research_program_hash: identity.research_program_hash,
            runtime_config_version_id: identity.runtime_config_version_id,
            runtime_config_hash: identity.runtime_config_hash,
            window_start: identity.window_start,
            window_end: identity.window_end,
            pit_cutoff: identity.pit_cutoff,
            reader_contract_version: identity.reader_contract_version,
            schema_contract_version: identity.schema_contract_version,
        }
    }
}

/// Immutable successful materialization bindings.
#[derive(Debug, Clone)]
pub struct CompleteSourceSlice {
    pub manifest_ref: SourceSliceManifestRef,
    pub manifest: SourceSliceManifestV2,
}

/// Operator/read-model projection for one source slice.
#[derive(Debug, Clone, Serialize, Deserialize, DerivePartialModel, FromQueryResult)]
#[sea_orm(entity = "crate::entities::quant_source_slice::Entity")]
pub struct SourceSliceInfo {
    pub source_slice_id: SourceSliceId,
    pub identity_hash: ContentHash,
    pub profile_ref: ResearchProfileRef,
    pub evaluation_track: String,
    pub research_program_hash: ContentHash,
    pub runtime_config_version_id: RuntimeConfigVersionId,
    pub runtime_config_hash: ContentHash,
    pub window_start: DateTime<Utc>,
    pub window_end: DateTime<Utc>,
    pub pit_cutoff: DateTime<Utc>,
    pub reader_contract_version: String,
    pub schema_contract_version: String,
    pub status: SourceSliceStatus,
    pub manifest_uri: Option<ArtifactUri>,
    pub manifest_hash: Option<ContentHash>,
    pub manifest_json: Option<SourceSliceManifestV2>,
    pub failure_detail: Option<String>,
    pub completed_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
}

/// Result of the unique-identity materialization claim.
#[derive(Debug, Clone)]
pub struct BeginSourceSliceOutcome {
    pub source_slice: SourceSliceInfo,
    /// True only for the transaction that inserted the canonical identity.
    pub acquired: bool,
}

info_from_model!(
    SourceSliceInfo,
    quant_source_slice::Model,
    {
        source_slice_id,
        identity_hash,
        profile_ref,
        evaluation_track,
        research_program_hash,
        runtime_config_version_id,
        runtime_config_hash,
        window_start,
        window_end,
        pit_cutoff,
        reader_contract_version,
        schema_contract_version,
        status,
        manifest_uri,
        manifest_hash,
        manifest_json,
        failure_detail,
        completed_at,
        created_at,
    }
);

#[must_use]
pub const fn evaluation_track_name(track: ResearchEvaluationTrack) -> &'static str {
    match track {
        ResearchEvaluationTrack::ResearchOnly => "research_only",
        ResearchEvaluationTrack::SemiAutoCandidate => "semi_auto_candidate",
    }
}
