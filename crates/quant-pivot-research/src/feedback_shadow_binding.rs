//! Canonical codec for route-owned shadow-binding artifacts.

use quant_pivot_error::{QuantError, QuantResult, research::ResearchError};
use quant_pivot_models::{domain::ports::ShadowBindingArtifact, hashing::CanonicalDigest};

/// Canonical JSON codec for committed and runtime-converged shadow bindings.
pub struct ShadowBindingCodec;

impl ShadowBindingCodec {
    pub fn encode(artifact: &ShadowBindingArtifact) -> QuantResult<Vec<u8>> {
        artifact.validate()?;
        CanonicalDigest::canonical_json_bytes(artifact).map_err(Into::into)
    }

    pub fn decode(bytes: &[u8]) -> QuantResult<ShadowBindingArtifact> {
        let artifact = serde_json::from_slice::<ShadowBindingArtifact>(bytes).map_err(|error| {
            ResearchError::Serialization {
                detail: format!("decode shadow-binding artifact: {error}"),
            }
        })?;
        artifact.validate()?;
        if Self::encode(&artifact)? != bytes {
            return Err(QuantError::from(ResearchError::Serialization {
                detail: "shadow-binding artifact is not canonical JSON".to_owned(),
            }));
        }
        Ok(artifact)
    }
}
