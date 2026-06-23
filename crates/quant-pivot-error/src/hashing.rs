//! Canonical content-hashing errors.
//!
//! The platform derives every content hash (manifest, dedupe key, publication,
//! audit linkage, query fingerprint) from a single canonical BLAKE3 digest. The
//! digest can fail on a serialization fault, or a typed content-addressing
//! newtype can reject a malformed value (a hash without the `blake3:` prefix, a
//! URI without a scheme, a non-positive schema version), all captured here.

use thiserror::Error;

/// Failure while computing a canonical digest or validating a content-addressing
/// newtype (`ContentHash` / `ArtifactUri` / `SchemaVersion`).
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CanonicalDigestError {
    /// The value could not be serialized to its canonical byte form.
    #[error("failed to serialize value for canonical digest: {0}")]
    Serialize(String),

    /// A `ContentHash` was constructed from a string lacking the canonical
    /// `blake3:<64-hex>` shape.
    #[error("invalid content hash (expected `blake3:<64 lowercase hex>`): {value}")]
    InvalidContentHash {
        /// The rejected raw value.
        value: String,
    },

    /// An `ArtifactUri` was constructed from a string lacking a `<scheme>://`
    /// prefix.
    #[error("invalid artifact uri (expected `<scheme>://<path>`): {value}")]
    InvalidArtifactUri {
        /// The rejected raw value.
        value: String,
    },

    /// A `SchemaVersion` was constructed from a non-positive integer.
    #[error("invalid schema version (must be >= 1): {value}")]
    InvalidSchemaVersion {
        /// The rejected raw value.
        value: i32,
    },
}
