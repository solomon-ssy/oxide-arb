//! Content-addressing newtypes: [`ContentHash`], [`ArtifactUri`], and
//! [`SchemaVersion`].
//!
//! These three types make the research plane's provenance invariants
//! unforgeable at the type level:
//!
//! - [`ContentHash`] can only hold a canonical `blake3:<64-hex>` digest. It is
//!   constructed exclusively through validation ([`ContentHash::parse`]) or
//!   through the canonical hasher
//!   ([`crate::hashing::CanonicalDigest::content_hash_json`]). A bare string can
//!   never masquerade as a content hash.
//! - [`ArtifactUri`] holds a validated `<scheme>://<path>` location (e.g.
//!   `file://<root>/datasets/<id>.parquet`). Postgres stores the URI string;
//!   the bytes live in the artifact store.
//! - [`SchemaVersion`] is a monotonic feature/factor/label/config schema version
//!   that cannot be mixed with arbitrary integers.
//!
//! # `SeaORM` persistence
//!
//! [`SchemaVersion`] uses read-time validation via custom [`TryGetable`] /
//! [`ValueType`] (not [`DeriveValueType`], which would accept corrupt integers).
//! [`ContentHash`] and [`ArtifactUri`] bind as `text` with **read-time validation**
//! via [`validated_text_seaorm!`] — `DeriveValueType` on a `String` tuple struct
//! would skip validation when loading corrupt rows from Postgres.

use std::{borrow::Cow, fmt, str::FromStr};

use quant_pivot_error::hashing::CanonicalDigestError;
use schemars::JsonSchema;
use sea_orm::{
    ActiveValue, ColIdx, IntoActiveValue, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::hashing::BLAKE3_PREFIX;

/// Length, in hexadecimal characters, of a BLAKE3-256 digest.
const BLAKE3_HEX_LEN: usize = 64;

/// `SeaORM` bindings for validated `text` newtypes (read-time `parse`).
///
/// Not [`DeriveValueType`]: struct derive wraps `String` losslessly and would
/// accept malformed DB values without calling `parse`.
macro_rules! validated_text_seaorm {
    ($name:ident) => {
        impl From<$name> for Value {
            #[inline]
            fn from(v: $name) -> Self {
                Self::String(Some(v.0))
            }
        }

        impl From<&$name> for Value {
            #[inline]
            fn from(v: &$name) -> Self {
                Self::String(Some(v.0.clone()))
            }
        }

        impl TryGetable for $name {
            fn try_get_by<I: ColIdx>(
                res: &sea_orm::QueryResult,
                index: I,
            ) -> Result<Self, TryGetError> {
                let raw: String = <String as TryGetable>::try_get_by(res, index)?;
                Self::parse(raw)
                    .map_err(|e| TryGetError::DbErr(sea_orm::DbErr::Type(e.to_string())))
            }
        }

        impl ValueType for $name {
            fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
                match v {
                    Value::String(Some(s)) => Self::parse(s).map_err(|_| ValueTypeErr),
                    _ => Err(ValueTypeErr),
                }
            }

            fn type_name() -> String {
                stringify!($name).to_owned()
            }

            fn array_type() -> ArrayType {
                ArrayType::String
            }

            fn column_type() -> ColumnType {
                ColumnType::Text
            }
        }

        impl Nullable for $name {
            fn null() -> Value {
                Value::String(None)
            }
        }

        impl IntoActiveValue<$name> for $name {
            #[inline]
            fn into_active_value(self) -> ActiveValue<$name> {
                ActiveValue::Set(self)
            }
        }
    };
}

// ── ContentHash ─────────────────────────────────────────────────────────────

/// A canonical `blake3:<64 lowercase hex>` content hash.
///
/// The inner string is private and validated on every construction path, so an
/// instance is a proof that the value is a well-formed canonical digest. Use
/// [`ContentHash::parse`] for untrusted input or
/// [`crate::hashing::CanonicalDigest::content_hash_json`] to hash a value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash(String);

impl JsonSchema for ContentHash {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("ContentHash")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "pattern": "^blake3:[0-9a-f]{64}$"
        })
    }
}

impl ContentHash {
    /// Validate and wrap a canonical `blake3:<64-hex>` string.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalDigestError::InvalidContentHash`] when the value does
    /// not start with the `blake3:` prefix followed by exactly 64 lowercase hex
    /// characters.
    pub fn parse(value: impl Into<String>) -> Result<Self, CanonicalDigestError> {
        let value = value.into();
        if Self::is_canonical(&value) {
            Ok(Self(value))
        } else {
            Err(CanonicalDigestError::InvalidContentHash { value })
        }
    }

    /// The canonical `blake3:<hex>` string.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The hex digest without the `blake3:` algorithm prefix.
    #[must_use]
    #[inline]
    pub fn hex(&self) -> &str {
        &self.0[BLAKE3_PREFIX.len()..]
    }

    /// Whether `value` has the canonical `blake3:<64 lowercase hex>` shape.
    fn is_canonical(value: &str) -> bool {
        let Some(hex) = value.strip_prefix(BLAKE3_PREFIX) else {
            return false;
        };
        hex.len() == BLAKE3_HEX_LEN && hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ContentHash {
    type Err = CanonicalDigestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ContentHash {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

validated_text_seaorm!(ContentHash);

// ── ArtifactUri ───────────────────────────────────────────────────────────────

/// A validated artifact location URI, e.g. `file://<root>/datasets/<id>.parquet`.
///
/// The inner string is private and validated to contain a `<scheme>://` prefix,
/// so an instance always denotes a resolvable location for some artifact-store
/// backend (local `file://` today; `s3://` later without changing the type).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArtifactUri(String);

impl ArtifactUri {
    /// Validate and wrap a `<scheme>://<path>` URI string.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalDigestError::InvalidArtifactUri`] when the value has
    /// no `<scheme>://` prefix with a non-empty scheme.
    pub fn parse(value: impl Into<String>) -> Result<Self, CanonicalDigestError> {
        let value = value.into();
        if Self::has_scheme(&value) {
            Ok(Self(value))
        } else {
            Err(CanonicalDigestError::InvalidArtifactUri { value })
        }
    }

    /// The full URI string.
    #[must_use]
    #[inline]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The URI scheme (the substring before `://`).
    #[must_use]
    pub fn scheme(&self) -> &str {
        self.0.split_once("://").map_or("", |(scheme, _)| scheme)
    }

    /// Whether `value` has a non-empty `<scheme>://` prefix.
    fn has_scheme(value: &str) -> bool {
        value
            .split_once("://")
            .is_some_and(|(scheme, rest)| !scheme.is_empty() && !rest.is_empty())
    }
}

impl fmt::Display for ArtifactUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for ArtifactUri {
    type Err = CanonicalDigestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for ArtifactUri {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ArtifactUri {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

validated_text_seaorm!(ArtifactUri);

// ── SchemaVersion ─────────────────────────────────────────────────────────────

/// A monotonic schema version for feature / factor / label / config schemas.
///
/// Wrapping the version prevents accidentally mixing it with unrelated integers
/// (counts, ids, ordinals) and makes "which schema generated this row" explicit
/// in every signature. Versions are `>= 1` by convention; untrusted wire and DB
/// values are validated through [`SchemaVersion::try_new`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, JsonSchema)]
#[serde(transparent)]
#[schemars(transparent)]
pub struct SchemaVersion(i32);

impl SchemaVersion {
    /// The first valid schema version.
    pub const FIRST: Self = Self(1);

    /// Wrap a raw schema version without validation.
    ///
    /// Intended for compile-time constants and trusted internal callers; use
    /// [`SchemaVersion::try_new`] for untrusted input.
    #[must_use]
    #[inline]
    pub const fn new(version: i32) -> Self {
        Self(version)
    }

    /// Validate (`>= 1`) and wrap a raw schema version.
    ///
    /// # Errors
    ///
    /// Returns [`CanonicalDigestError::InvalidSchemaVersion`] when `version < 1`.
    pub const fn try_new(version: i32) -> Result<Self, CanonicalDigestError> {
        if version >= 1 {
            Ok(Self(version))
        } else {
            Err(CanonicalDigestError::InvalidSchemaVersion { value: version })
        }
    }

    /// The raw integer version.
    #[must_use]
    #[inline]
    pub const fn get(self) -> i32 {
        self.0
    }
}

impl fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl IntoActiveValue<Self> for SchemaVersion {
    #[inline]
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = i32::deserialize(deserializer)?;
        Self::try_new(raw).map_err(serde::de::Error::custom)
    }
}

impl From<SchemaVersion> for Value {
    #[inline]
    fn from(v: SchemaVersion) -> Self {
        Self::Int(Some(v.get()))
    }
}

impl From<&SchemaVersion> for Value {
    #[inline]
    fn from(v: &SchemaVersion) -> Self {
        Self::Int(Some(v.get()))
    }
}

impl TryGetable for SchemaVersion {
    fn try_get_by<I: ColIdx>(res: &sea_orm::QueryResult, index: I) -> Result<Self, TryGetError> {
        let raw: i32 = <i32 as TryGetable>::try_get_by(res, index)?;
        Self::try_new(raw).map_err(|e| TryGetError::DbErr(sea_orm::DbErr::Type(e.to_string())))
    }
}

impl ValueType for SchemaVersion {
    fn try_from(v: Value) -> Result<Self, ValueTypeErr> {
        match v {
            Value::Int(Some(raw)) => Self::try_new(raw).map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(SchemaVersion).to_owned()
    }

    fn array_type() -> ArrayType {
        ArrayType::Int
    }

    fn column_type() -> ColumnType {
        ColumnType::Integer
    }
}

impl Nullable for SchemaVersion {
    fn null() -> Value {
        Value::Int(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_orm::sea_query::{Value, ValueType};

    const VALID_HASH: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn content_hash_accepts_canonical_blake3() {
        let h = ContentHash::parse(VALID_HASH).expect("valid");
        assert_eq!(h.as_str(), VALID_HASH);
        assert_eq!(h.hex().len(), BLAKE3_HEX_LEN);
    }

    #[test]
    fn content_hash_rejects_non_blake3_prefix() {
        let sha = format!("sha256:{}", "a".repeat(BLAKE3_HEX_LEN));
        assert!(ContentHash::parse(sha).is_err());
        assert!(ContentHash::parse("blake3:short").is_err());
        assert!(ContentHash::parse(format!("blake3:{}", "A".repeat(BLAKE3_HEX_LEN))).is_err());
    }

    #[test]
    fn content_hash_serde_validates() {
        let json = format!("\"{VALID_HASH}\"");
        let back: ContentHash = serde_json::from_str(&json).expect("valid");
        assert_eq!(back, ContentHash::parse(VALID_HASH).unwrap());
        assert!(serde_json::from_str::<ContentHash>("\"not-a-hash\"").is_err());
    }

    #[test]
    fn content_hash_seaorm_value_type_validates() {
        let valid = Value::String(Some(VALID_HASH.to_owned()));
        let parsed = <ContentHash as ValueType>::try_from(valid).expect("valid db value");
        assert_eq!(parsed.as_str(), VALID_HASH);

        let sha = format!("sha256:{}", "a".repeat(BLAKE3_HEX_LEN));
        let invalid = Value::String(Some(sha));
        assert!(<ContentHash as ValueType>::try_from(invalid).is_err());

        assert!(<ContentHash as ValueType>::try_from(Value::String(None)).is_err());
    }

    #[test]
    fn content_hash_seaorm_roundtrip_value() {
        let hash = ContentHash::parse(VALID_HASH).expect("valid");
        let value: Value = hash.clone().into();
        let back = <ContentHash as ValueType>::try_from(value).expect("roundtrip");
        assert_eq!(back, hash);
    }

    #[test]
    fn artifact_uri_requires_scheme() {
        let uri = ArtifactUri::parse("file:///var/artifacts/x.parquet").expect("valid");
        assert_eq!(uri.scheme(), "file");
        assert!(ArtifactUri::parse("/var/artifacts/x").is_err());
        assert!(ArtifactUri::parse("://nope").is_err());
    }

    #[test]
    fn schema_version_try_new_rejects_non_positive() {
        assert!(SchemaVersion::try_new(0).is_err());
        assert_eq!(SchemaVersion::try_new(3).unwrap().get(), 3);
        assert_eq!(SchemaVersion::FIRST.get(), 1);
    }

    #[test]
    fn schema_version_serde_is_transparent_integer() {
        let v = SchemaVersion::new(3);
        assert_eq!(serde_json::to_string(&v).unwrap(), "3");
        let back: SchemaVersion = serde_json::from_str("3").unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn schema_version_serde_rejects_non_positive() {
        assert!(serde_json::from_str::<SchemaVersion>("0").is_err());
        assert!(serde_json::from_str::<SchemaVersion>("-1").is_err());
    }

    #[test]
    fn schema_version_seaorm_value_type_validates() {
        let valid = Value::Int(Some(3));
        assert_eq!(
            <SchemaVersion as ValueType>::try_from(valid)
                .expect("valid")
                .get(),
            3
        );

        assert!(<SchemaVersion as ValueType>::try_from(Value::Int(Some(0))).is_err());
        assert!(<SchemaVersion as ValueType>::try_from(Value::Int(Some(-1))).is_err());
        assert!(<SchemaVersion as ValueType>::try_from(Value::Int(None)).is_err());
    }

    #[test]
    fn schema_version_seaorm_roundtrip_value() {
        let version = SchemaVersion::new(7);
        let value: Value = version.into();
        let back = <SchemaVersion as ValueType>::try_from(value).expect("roundtrip");
        assert_eq!(back, version);
    }
}
