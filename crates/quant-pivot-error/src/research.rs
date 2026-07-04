//! Research-plane errors (feature/factor/model materialization, artifact store).
//!
//! The `quant-pivot-research` crate's computation traits and the artifact store
//! surface their failures through [`ResearchError`], which folds into
//! [`crate::QuantError`] via `#[from]` so `?` propagation works uniformly across
//! the workspace. Keeping the enum here (next to every other domain sub-error)
//! avoids a re-export shim and keeps `quant-pivot-research` free of a bespoke
//! error root.

use thiserror::Error;

use crate::{QuantError, storage::StorageError};

/// Failure modes of the research plane.
///
/// Covers artifact IO, schema/hash mismatches, and determinism violations.
/// Money-adjacent invariants (hash mismatch, schema mismatch) are dedicated
/// variants so callers can reject loads rather than silently degrade.
#[derive(Debug, Error)]
pub enum ResearchError {
    /// An artifact-store IO operation failed.
    #[error("artifact store IO failed for `{uri}`: {detail}")]
    ArtifactIo {
        /// The artifact location involved.
        uri: String,
        /// Underlying IO failure detail.
        detail: String,
    },

    /// The requested artifact does not exist in the store.
    #[error("artifact not found: `{uri}`")]
    ArtifactNotFound {
        /// The missing artifact location.
        uri: String,
    },

    /// An artifact key could not be turned into a valid location.
    #[error("invalid artifact key: {detail}")]
    InvalidArtifactKey {
        /// Why the key was rejected.
        detail: String,
    },

    /// A model runtime's feature-schema hash did not match the active schema.
    #[error("feature schema hash mismatch: expected `{expected}`, got `{actual}`")]
    FeatureSchemaMismatch {
        /// The hash the runtime/artifact was built against.
        expected: String,
        /// The currently active schema hash.
        actual: String,
    },

    /// A stored artifact's content hash did not match its recorded hash.
    #[error("artifact hash mismatch: expected `{expected}`, got `{actual}`")]
    ArtifactHashMismatch {
        /// The recorded canonical hash.
        expected: String,
        /// The recomputed canonical hash.
        actual: String,
    },

    /// A factor/feature schema hash binding check failed.
    #[error("schema hash mismatch: {detail}")]
    SchemaHashMismatch {
        /// Context describing which binding failed.
        detail: String,
    },

    /// A canonical-hash input violated determinism rules (e.g. unsorted set).
    #[error("determinism violation: {detail}")]
    Determinism {
        /// Context describing the violated invariant.
        detail: String,
    },

    /// A factor's raw computation failed irrecoverably (not a missing input,
    /// which is modeled as `RawFactor { raw_value: None }`).
    #[error("factor computation failed: {detail}")]
    FactorComputation {
        /// Context describing the failure.
        detail: String,
    },

    /// Artifact (de)serialization failed.
    #[error("research artifact serialization failed: {detail}")]
    Serialization {
        /// Underlying serialization failure detail.
        detail: String,
    },

    /// A model artifact violated a structural invariant (e.g. unnormalized
    /// weights, an empty weight set, a non-monotone calibration curve).
    #[error("invalid model artifact: {detail}")]
    InvalidModelArtifact {
        /// Context describing the violated invariant.
        detail: String,
    },

    /// A loaded model runtime could not score the supplied input (e.g. a
    /// factor-table runtime handed a feature-matrix input, or a market lacked
    /// the executable price needed to reference an entry).
    #[error("model inference failed: {detail}")]
    Inference {
        /// Context describing the failure.
        detail: String,
    },

    /// A concrete model family is recognized but its runtime is not linked in
    /// this build/phase (classical → 3.6, ONNX → Phase 06+).
    #[error("model runtime not available for family `{family}`: {detail}")]
    RuntimeUnavailable {
        /// The model family whose runtime is not linked.
        family: String,
        /// Context (which phase introduces it).
        detail: String,
    },

    /// A historical point-in-time resolution failed (e.g. an undecodable book
    /// snapshot payload, or an inconsistent historical fact).
    #[error("point-in-time resolution failed: {detail}")]
    PitResolution {
        /// Context describing the failure.
        detail: String,
    },

    /// A dataset plan could not be produced (e.g. an empty or inverted window,
    /// a zero sampling interval).
    #[error("dataset plan failed: {detail}")]
    DatasetPlan {
        /// Context describing the failure.
        detail: String,
    },

    /// A dataset build step failed irrecoverably (not a per-sample skip, which is
    /// accounted in coverage).
    #[error("dataset build failed: {detail}")]
    DatasetBuild {
        /// Context describing the failure.
        detail: String,
    },

    /// A forward label could not be resolved due to inconsistent forward data.
    #[error("label resolution failed: {detail}")]
    LabelResolution {
        /// Context describing the failure.
        detail: String,
    },

    /// A future-leakage invariant was violated: a feature read state newer than
    /// `as_of - source_delay`, or a label read state at or before `as_of`. This
    /// is a hard, money-critical failure — the dataset must never be persisted.
    #[error("future leakage detected: {detail}")]
    LeakageDetected {
        /// Context describing which sample and bound was violated.
        detail: String,
    },

    /// The training matrix could not be built (NaN/inf, a critical missing
    /// feature, or an out-of-spec column).
    #[error("training matrix build failed: {detail}")]
    MatrixBuild {
        /// Context describing the failure.
        detail: String,
    },

    /// Parquet (de)serialization of a dataset failed.
    #[error("parquet codec failed: {detail}")]
    ParquetCodec {
        /// Context describing the failure.
        detail: String,
    },

    /// A long-running research job was cooperatively cancelled at a
    /// section/phase boundary (operator cancel, lease loss, or graceful
    /// shutdown). Terminal but distinct from a failure — the durable worker
    /// records it as `Cancelled`, not `Failed`, and never persists a partial
    /// artifact.
    #[error("research job cancelled: {detail}")]
    Cancelled {
        /// Context describing where the cancellation was observed.
        detail: String,
    },
}

/// A storage failure surfaced during point-in-time resolution.
///
/// Wraps [`crate::storage::StorageError`] so PIT call sites can propagate via
/// `?` into [`ResearchError::PitResolution`] without a bespoke mapper.
#[derive(Debug)]
pub struct PitResolutionStorageError(pub StorageError);

impl From<StorageError> for PitResolutionStorageError {
    fn from(error: StorageError) -> Self {
        Self(error)
    }
}

impl From<PitResolutionStorageError> for ResearchError {
    fn from(error: PitResolutionStorageError) -> Self {
        Self::PitResolution {
            detail: error.0.to_string(),
        }
    }
}

impl From<PitResolutionStorageError> for QuantError {
    fn from(error: PitResolutionStorageError) -> Self {
        ResearchError::from(error).into()
    }
}
