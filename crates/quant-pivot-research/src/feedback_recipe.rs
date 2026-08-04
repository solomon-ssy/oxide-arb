//! Canonical codec for governed feedback recipe-plan artifacts.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{domain::ports::CandidateRecipePlanArtifact, hashing::CanonicalDigest};

/// Canonical JSON codec for recipe-plan evidence.
pub struct CandidateRecipePlanCodec;

impl CandidateRecipePlanCodec {
    pub fn encode(artifact: &CandidateRecipePlanArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<CandidateRecipePlanArtifact> {
        let artifact =
            serde_json::from_slice::<CandidateRecipePlanArtifact>(bytes).map_err(|error| {
                ResearchError::Serialization {
                    detail: format!("decode candidate recipe-plan artifact: {error}"),
                }
            })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(QuantError::from(ResearchError::Serialization {
                detail: "candidate recipe-plan artifact is not canonical JSON".to_owned(),
            }));
        }
        Ok(artifact)
    }
}
