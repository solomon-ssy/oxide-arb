//! Canonical BLAKE3 digest primitive.
//!
//! Single source of truth for the platform's content hashes. Every typed hasher
//! (materialization manifest, dedupe key, stage artifact, publication, audit
//! event, factor dimensions/payload, query fingerprint) delegates here so the
//! serialization + BLAKE3 + encoding contract lives in exactly one place.

use quant_pivot_error::hashing::CanonicalDigestError;
use serde::Serialize;

use crate::types::ContentHash;

/// Prefix applied to canonical digests so the algorithm is self-describing.
pub const BLAKE3_PREFIX: &str = "blake3:";

/// Canonical BLAKE3 digest helper over serde-serialized values and raw bytes.
pub struct CanonicalDigest;

impl CanonicalDigest {
    /// Lowercase hex BLAKE3 digest of `bytes` (no algorithm prefix).
    #[must_use]
    pub fn raw_hex(bytes: &[u8]) -> String {
        hex::encode(blake3::hash(bytes).as_bytes())
    }

    /// Prefixed `blake3:<hex>` digest of `bytes`.
    #[must_use]
    pub fn prefixed_bytes(bytes: &[u8]) -> String {
        let mut digest = String::with_capacity(BLAKE3_PREFIX.len() + 64);
        digest.push_str(BLAKE3_PREFIX);
        digest.push_str(&Self::raw_hex(bytes));
        digest
    }

    /// Prefixed `blake3:<hex>` digest over the canonical JSON of `value`.
    ///
    /// Determinism note: callers that need order-independent digests (e.g. sets
    /// of IDs) must sort the relevant fields before serialization; this helper
    /// hashes the serialized bytes verbatim.
    pub fn blake3_json<T>(value: &T) -> Result<String, CanonicalDigestError>
    where
        T: Serialize + ?Sized,
    {
        let bytes = serde_json::to_vec(value)
            .map_err(|error| CanonicalDigestError::Serialize(error.to_string()))?;
        Ok(Self::prefixed_bytes(&bytes))
    }

    /// Typed `blake3:<hex>` digest over the canonical JSON of `value`.
    ///
    /// Equivalent to [`Self::blake3_json`] but returns a validated
    /// [`ContentHash`], the only sanctioned way to mint a content hash from a
    /// serializable value. Determinism note: callers needing order-independent
    /// digests must sort the relevant fields before serialization.
    pub fn content_hash_json<T>(value: &T) -> Result<ContentHash, CanonicalDigestError>
    where
        T: Serialize + ?Sized,
    {
        ContentHash::parse(Self::blake3_json(value)?)
    }
}

/// Canonical governance state hash (prefixed BLAKE3 JSON digest).
pub fn canonical_state_hash<T: Serialize>(value: &T) -> Result<String, CanonicalDigestError> {
    CanonicalDigest::blake3_json(value)
}

#[cfg(test)]
mod tests {
    use super::{BLAKE3_PREFIX, CanonicalDigest};
    use serde::Serialize;

    #[derive(Serialize)]
    struct Sample {
        a: u32,
        b: &'static str,
    }

    #[test]
    fn digest_is_prefixed_and_stable() {
        let value = Sample { a: 1, b: "x" };
        let digest = CanonicalDigest::blake3_json(&value).expect("digest");
        assert!(digest.starts_with(BLAKE3_PREFIX));
        assert_eq!(
            digest,
            CanonicalDigest::blake3_json(&value).expect("digest repeat")
        );
    }

    #[test]
    fn distinct_values_differ() {
        let left = CanonicalDigest::blake3_json(&Sample { a: 1, b: "x" }).expect("left");
        let right = CanonicalDigest::blake3_json(&Sample { a: 2, b: "x" }).expect("right");
        assert_ne!(left, right);
    }

    #[test]
    fn canonical_state_hash_matches_blake3_json() {
        let value = Sample { a: 1, b: "x" };
        assert_eq!(
            super::canonical_state_hash(&value).expect("hash"),
            CanonicalDigest::blake3_json(&value).expect("digest")
        );
    }

    #[test]
    fn raw_hex_has_no_prefix() {
        let hex = CanonicalDigest::raw_hex(b"abc");
        assert!(!hex.starts_with(BLAKE3_PREFIX));
        assert_eq!(hex.len(), 64);
    }
}
