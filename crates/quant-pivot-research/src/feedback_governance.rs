//! Canonical codecs for feedback truth, attribution-plan, and validation artifacts.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{
    domain::ports::{
        FeedbackAttributionPlanArtifact, FeedbackTruthFreezeArtifact, FeedbackValidationArtifact,
    },
    hashing::CanonicalDigest,
    types::ContentHash,
};
use serde::{Serialize, de::DeserializeOwned};

/// Canonical JSON codec for the three feedback governance stages.
pub struct FeedbackGovernanceCodec;

impl FeedbackGovernanceCodec {
    pub fn encode_truth(artifact: &FeedbackTruthFreezeArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        Self::encode(artifact)
    }

    pub fn decode_truth(bytes: &[u8]) -> QuantResult<FeedbackTruthFreezeArtifact> {
        let artifact: FeedbackTruthFreezeArtifact = Self::decode(bytes, "truth-freeze")?;
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn encode_attribution(artifact: &FeedbackAttributionPlanArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        Self::encode(artifact)
    }

    pub fn decode_attribution(bytes: &[u8]) -> QuantResult<FeedbackAttributionPlanArtifact> {
        let artifact: FeedbackAttributionPlanArtifact = Self::decode(bytes, "attribution-plan")?;
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn encode_validation(artifact: &FeedbackValidationArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        Self::encode(artifact)
    }

    pub fn decode_validation(bytes: &[u8]) -> QuantResult<FeedbackValidationArtifact> {
        let artifact: FeedbackValidationArtifact = Self::decode(bytes, "validation")?;
        artifact.validate()?;
        Ok(artifact)
    }

    #[must_use]
    pub fn bytes_hash(bytes: &[u8]) -> ContentHash {
        CanonicalDigest::content_hash_bytes(bytes)
    }

    fn encode<T: Serialize>(artifact: &T) -> QuantResult<Vec<u8>> {
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    fn decode<T: DeserializeOwned + Serialize>(bytes: &[u8], kind: &'static str) -> QuantResult<T> {
        let artifact =
            serde_json::from_slice::<T>(bytes).map_err(|error| ResearchError::Serialization {
                detail: format!("decode feedback {kind} artifact: {error}"),
            })?;
        if Self::encode(&artifact)? != bytes {
            return Err(QuantError::from(ResearchError::Serialization {
                detail: format!("feedback {kind} artifact is not canonical JSON"),
            }));
        }
        Ok(artifact)
    }
}
