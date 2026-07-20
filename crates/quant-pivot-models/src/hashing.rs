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

    /// Prefixed `blake3:<hex>` digest over RFC 8785 JCS bytes.
    ///
    /// Arrays retain their business ordering. Callers hashing mathematical sets
    /// must still sort their elements before invoking this primitive.
    pub fn blake3_json<T>(value: &T) -> Result<String, CanonicalDigestError>
    where
        T: Serialize + ?Sized,
    {
        let bytes = Self::canonical_json_bytes(value)?;
        Ok(Self::prefixed_bytes(&bytes))
    }

    /// Serialize `value` using RFC 8785 JSON Canonicalization Scheme semantics.
    pub fn canonical_json_bytes<T>(value: &T) -> Result<Vec<u8>, CanonicalDigestError>
    where
        T: Serialize + ?Sized,
    {
        serde_json_canonicalizer::to_vec(&value)
            .map_err(|error| CanonicalDigestError::Serialize(error.to_string()))
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

    /// RFC 8785 canonical, domain-separated content identifier.
    ///
    /// The NUL delimiter makes the domain prefix unambiguous. `schema_version`
    /// is part of the hashed typed envelope, so incompatible payload schemas
    /// cannot accidentally share an identifier.
    pub fn content_hash_typed<T>(
        domain_separator: &str,
        schema_version: u32,
        value: &T,
    ) -> Result<ContentHash, CanonicalDigestError>
    where
        T: Serialize + ?Sized,
    {
        #[derive(Serialize)]
        struct TypedEnvelope<'a, T: ?Sized> {
            schema_version: u32,
            payload: &'a T,
        }

        let canonical = Self::canonical_json_bytes(&TypedEnvelope {
            schema_version,
            payload: value,
        })?;
        let mut input = Vec::with_capacity(domain_separator.len() + 1 + canonical.len());
        input.extend_from_slice(domain_separator.as_bytes());
        input.push(0);
        input.extend_from_slice(&canonical);
        ContentHash::parse(Self::prefixed_bytes(&input))
    }
}

/// Canonical governance state hash (validated, prefixed BLAKE3 JSON digest).
pub fn canonical_state_hash<T: Serialize>(value: &T) -> Result<ContentHash, CanonicalDigestError> {
    CanonicalDigest::content_hash_json(value)
}

#[cfg(test)]
mod tests {
    use crate::hashing::canonical_state_hash;

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
            canonical_state_hash(&value).expect("hash"),
            CanonicalDigest::content_hash_json(&value).expect("digest")
        );
    }

    #[test]
    fn raw_hex_has_no_prefix() {
        let hex = CanonicalDigest::raw_hex(b"abc");
        assert!(!hex.starts_with(BLAKE3_PREFIX));
        assert_eq!(hex.len(), 64);
    }

    #[test]
    fn typed_digest_is_key_order_independent_and_domain_separated() {
        let left = serde_json::json!({"z": 1, "a": 2});
        let right = serde_json::json!({"a": 2, "z": 1});
        let left_hash =
            CanonicalDigest::content_hash_typed("catalog.event", 1, &left).expect("left digest");
        let right_hash =
            CanonicalDigest::content_hash_typed("catalog.event", 1, &right).expect("right digest");
        assert_eq!(left_hash, right_hash);
        assert_ne!(
            left_hash,
            CanonicalDigest::content_hash_typed("catalog.market", 1, &right).expect("other domain")
        );
    }

    #[test]
    fn untyped_digest_is_key_order_independent() {
        let left = serde_json::json!({"z": 1, "a": 2});
        let right = serde_json::json!({"a": 2, "z": 1});
        assert_eq!(
            CanonicalDigest::content_hash_json(&left).expect("left digest"),
            CanonicalDigest::content_hash_json(&right).expect("right digest")
        );
    }
}
