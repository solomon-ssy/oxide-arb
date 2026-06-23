//! Serialized model artifacts: the [`ModelArtifact`] enum and its common header.
//!
//! Artifact **bytes** live in the [`ArtifactStore`](crate::artifact::ArtifactStore);
//! Postgres stores only metadata + [`ContentHash`]. 3.4 fills the
//! weighted-factor body (normalization / multipliers / objective report); 3.6
//! fills the classical body. 3.0 fixes the enum shell + common header so the
//! factory and registry have a stable contract.

use quant_pivot_models::types::{ContentHash, ModelArtifactId, ModelVersionId};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::{
    factors::FactorName,
    model::runtime::{ClassicalKind, ModelFamily},
};

/// Provenance header shared by every model artifact: which version, family, and
/// the schema hashes it is bound to. Loading must reject a mismatch against the
/// active feature/factor schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelArtifactHeader {
    /// The published model version this artifact realizes.
    pub model_version_id: ModelVersionId,
    /// Model family.
    pub model_family: ModelFamily,
    /// Feature-schema hash the artifact was trained/built against.
    pub feature_schema_hash: ContentHash,
    /// Factor-schema hash the artifact was trained/built against.
    pub factor_schema_hash: ContentHash,
}

/// A single factor weight in a weighted-factor artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FactorWeight {
    /// The weighted factor.
    pub factor: FactorName,
    /// The (frozen) weight.
    pub weight: Decimal,
}

/// Weighted-factor scorer artifact (first-class, fully explainable).
///
/// 3.4 extends this with normalization, score multipliers, and the trainer's
/// objective report; 3.0 fixes the header + weights contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WeightedFactorModelArtifact {
    /// Common provenance header.
    pub header: ModelArtifactHeader,
    /// Frozen per-factor weights.
    pub weights: Vec<FactorWeight>,
}

/// Classical-ML artifact (smartcore-backed). 3.6 fills the serialized-model URI,
/// preprocessing, and metrics; 3.0 fixes the header + identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClassicalModelArtifact {
    /// Common provenance header.
    pub header: ModelArtifactHeader,
    /// Stored-artifact id (bytes live in the artifact store).
    pub artifact_id: ModelArtifactId,
    /// Concrete classical kind.
    pub kind: ClassicalKind,
}

/// A versioned, content-addressable model artifact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelArtifact {
    /// Weighted-factor scorer.
    WeightedFactor(WeightedFactorModelArtifact),
    /// Classical ML model.
    Classical(ClassicalModelArtifact),
    // Onnx(OnnxArtifactRef) reserved — Phase 06.
}

impl ModelArtifact {
    /// The common provenance header, regardless of family.
    #[must_use]
    pub const fn header(&self) -> &ModelArtifactHeader {
        match self {
            Self::WeightedFactor(artifact) => &artifact.header,
            Self::Classical(artifact) => &artifact.header,
        }
    }
}
