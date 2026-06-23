//! Canonical content-hashing errors.
//!
//! The platform derives every content hash (manifest, dedupe key, publication,
//! audit linkage, query fingerprint) from a single canonical BLAKE3 digest. The
//! only way that digest can fail is a serialization fault on the value being
//! hashed, captured here.

use thiserror::Error;

/// Failure while computing a canonical BLAKE3 digest over a serializable value.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CanonicalDigestError {
    /// The value could not be serialized to its canonical byte form.
    #[error("failed to serialize value for canonical digest: {0}")]
    Serialize(String),
}
