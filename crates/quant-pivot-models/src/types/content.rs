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
//! [`ValueType`] (not `DeriveValueType`, which would accept corrupt integers).
//! [`ContentHash`] and [`ArtifactUri`] bind as `text` with **read-time validation**.
//! `ContentHash` stores only the 32-byte digest in memory and formats the
//! canonical text at persistence/wire boundaries; `ArtifactUri` uses
//! `validated_text_seaorm!`. `DeriveValueType` on a `String` tuple struct would
//! skip validation when loading corrupt rows from Postgres.

use std::{
    borrow::Cow,
    fmt::{Debug, Display, Formatter, Result as FmtResult},
    str::FromStr,
};

use quant_pivot_error::hashing::CanonicalDigestError;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use sea_orm::{
    ActiveValue, ColIdx, DbErr, IntoActiveValue, QueryResult, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{Error, Visitor},
};
use url::Url;

use crate::hashing::BLAKE3_PREFIX;

/// Length, in hexadecimal characters, of a BLAKE3-256 digest.
const BLAKE3_HEX_LEN: usize = 64;

/// Length of the canonical `blake3:<64 lowercase hex>` representation.
const CONTENT_HASH_TEXT_LEN: usize = BLAKE3_PREFIX.len() + BLAKE3_HEX_LEN;

/// Lowercase hexadecimal alphabet used by the allocation-free formatter.
const LOWER_HEX: &[u8; 16] = b"0123456789abcdef";

/// `SeaORM` bindings for validated `text` newtypes (read-time `parse`).
///
/// Not `DeriveValueType`: struct derive wraps `String` losslessly and would
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

/// A BLAKE3-256 content hash with canonical text formatting at boundaries.
///
/// The digest is stored inline as 32 bytes. Use [`ContentHash::parse`] for
/// untrusted canonical text or
/// [`crate::hashing::CanonicalDigest::content_hash_json`] to hash a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHash([u8; 32]);

/// Stack-backed canonical `blake3:<64 lowercase hex>` text.
///
/// This is primarily the stable UUID-v5 name representation for existing
/// content-addressed identifiers. It also avoids allocating when an API accepts
/// bytes directly.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ContentHashText([u8; CONTENT_HASH_TEXT_LEN]);

impl ContentHashText {
    /// Canonical text bytes, including the `blake3:` prefix.
    #[must_use]
    #[inline]
    pub const fn as_bytes(&self) -> &[u8; CONTENT_HASH_TEXT_LEN] {
        &self.0
    }
}

impl Display for ContentHashText {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        for byte in self.0 {
            f.write_str(char::from(byte).encode_utf8(&mut [0; 4]))?;
        }
        Ok(())
    }
}

impl Debug for ContentHashText {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(self, f)
    }
}

impl JsonSchema for ContentHash {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("ContentHash")
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
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
    pub fn parse(value: &str) -> Result<Self, CanonicalDigestError> {
        let Some(hex) = value.strip_prefix(BLAKE3_PREFIX) else {
            return Err(Self::invalid(value));
        };
        if hex.len() != BLAKE3_HEX_LEN
            || !hex
                .bytes()
                .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(Self::invalid(value));
        }

        let mut digest = [0_u8; 32];
        hex::decode_to_slice(hex, &mut digest).map_err(|_| Self::invalid(value))?;
        Ok(Self(digest))
    }

    /// Construct from an already computed BLAKE3-256 digest.
    #[must_use]
    #[inline]
    pub const fn from_bytes(digest: [u8; 32]) -> Self {
        Self(digest)
    }

    /// Raw 32-byte digest.
    #[must_use]
    #[inline]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase digest hex without the algorithm prefix.
    ///
    /// This allocates only for boundaries that require an owned path/id string.
    #[must_use]
    pub fn hex(&self) -> String {
        hex::encode(self.0)
    }

    /// Stack-backed canonical `blake3:<64 lowercase hex>` representation.
    #[must_use]
    pub fn canonical_text(&self) -> ContentHashText {
        let mut text = [0_u8; CONTENT_HASH_TEXT_LEN];
        text[..BLAKE3_PREFIX.len()].copy_from_slice(BLAKE3_PREFIX.as_bytes());
        for (index, byte) in self.0.iter().copied().enumerate() {
            let offset = BLAKE3_PREFIX.len() + index * 2;
            text[offset] = LOWER_HEX[usize::from(byte >> 4)];
            text[offset + 1] = LOWER_HEX[usize::from(byte & 0x0f)];
        }
        ContentHashText(text)
    }

    fn invalid(value: &str) -> CanonicalDigestError {
        CanonicalDigestError::InvalidContentHash {
            value: value.to_owned(),
        }
    }
}

impl Display for ContentHash {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        Display::fmt(&self.canonical_text(), f)
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
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for ContentHash {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ContentHashVisitor;

        impl Visitor<'_> for ContentHashVisitor {
            type Value = ContentHash;

            fn expecting(&self, formatter: &mut Formatter<'_>) -> FmtResult {
                formatter.write_str("a canonical blake3:<64 lowercase hex> digest")
            }

            fn visit_str<E: Error>(self, value: &str) -> Result<Self::Value, E> {
                ContentHash::parse(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(ContentHashVisitor)
    }
}

impl From<ContentHash> for Value {
    #[inline]
    fn from(value: ContentHash) -> Self {
        Self::String(Some(value.to_string()))
    }
}

impl From<&ContentHash> for Value {
    #[inline]
    fn from(value: &ContentHash) -> Self {
        Self::String(Some(value.to_string()))
    }
}

impl TryGetable for ContentHash {
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let raw: String = <String as TryGetable>::try_get_by(res, index)?;
        Self::parse(&raw).map_err(|error| TryGetError::DbErr(DbErr::Type(error.to_string())))
    }
}

impl ValueType for ContentHash {
    fn try_from(value: Value) -> Result<Self, ValueTypeErr> {
        match value {
            Value::String(Some(raw)) => Self::parse(&raw).map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(ContentHash).to_owned()
    }

    fn array_type() -> ArrayType {
        ArrayType::String
    }

    fn column_type() -> ColumnType {
        ColumnType::Text
    }
}

impl Nullable for ContentHash {
    fn null() -> Value {
        Value::String(None)
    }
}

impl IntoActiveValue<Self> for ContentHash {
    #[inline]
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

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

    /// Whether the URI path has the exact final extension.
    ///
    /// Query parameters such as an immutable S3 `versionId` are intentionally
    /// excluded from the path comparison.
    #[must_use]
    pub fn has_path_extension(&self, extension: &str) -> bool {
        !extension.is_empty()
            && !extension.contains('.')
            && Url::parse(&self.0).is_ok_and(|url| {
                url.path_segments()
                    .and_then(Iterator::last)
                    .and_then(|segment| segment.rsplit_once('.'))
                    .is_some_and(|(_, actual)| actual == extension)
            })
    }

    /// Whether `value` has a non-empty `<scheme>://` prefix.
    fn has_scheme(value: &str) -> bool {
        value
            .split_once("://")
            .is_some_and(|(scheme, rest)| !scheme.is_empty() && !rest.is_empty())
    }
}

impl Display for ArtifactUri {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
        Self::parse(raw).map_err(Error::custom)
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

impl Default for SchemaVersion {
    fn default() -> Self {
        Self::FIRST
    }
}

impl Display for SchemaVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
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
        Self::try_new(raw).map_err(Error::custom)
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
    fn try_get_by<I: ColIdx>(res: &QueryResult, index: I) -> Result<Self, TryGetError> {
        let raw: i32 = <i32 as TryGetable>::try_get_by(res, index)?;
        Self::try_new(raw).map_err(|e| TryGetError::DbErr(DbErr::Type(e.to_string())))
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
    use std::mem::{needs_drop, size_of};

    use bincode::{deserialize as bincode_deserialize, serialize as bincode_serialize};
    use bitcode::{deserialize as bitcode_deserialize, serialize as bitcode_serialize};
    use sea_orm::sea_query::{Value, ValueType};

    use super::*;
    use crate::hashing::CanonicalDigest;

    const VALID_HASH: &str =
        "blake3:0000000000000000000000000000000000000000000000000000000000000000";

    #[test]
    fn content_hash_accepts_blake3() {
        let h = ContentHash::parse(VALID_HASH).expect("valid");
        assert_eq!(h.to_string(), VALID_HASH);
        assert_eq!(h.as_bytes(), &[0; 32]);
        assert_eq!(h.canonical_text().as_bytes().len(), CONTENT_HASH_TEXT_LEN);
    }

    #[test]
    fn content_hash_inline_value() {
        fn assert_copy<T: Copy>() {}

        assert_copy::<ContentHash>();
        assert_eq!(size_of::<ContentHash>(), 32);
        assert!(!needs_drop::<ContentHash>());
    }

    #[test]
    fn content_hash_raw_stable() {
        let hash = CanonicalDigest::content_hash_bytes(b"abc");
        assert_eq!(
            hash.to_string(),
            "blake3:6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
        assert_eq!(ContentHash::from_bytes(*hash.as_bytes()), hash);
    }

    #[test]
    fn content_rejects_non_prefix() {
        let sha = format!("sha256:{}", "a".repeat(BLAKE3_HEX_LEN));
        assert!(ContentHash::parse(&sha).is_err());
        assert!(ContentHash::parse("blake3:short").is_err());
        assert!(ContentHash::parse(&format!("blake3:{}", "A".repeat(BLAKE3_HEX_LEN))).is_err());
    }

    #[test]
    fn content_hash_serde_validates() {
        let json = format!("\"{VALID_HASH}\"");
        let back: ContentHash = serde_json::from_str(&json).expect("valid");
        assert_eq!(back, ContentHash::parse(VALID_HASH).unwrap());
        assert!(serde_json::from_str::<ContentHash>("\"not-a-hash\"").is_err());
    }

    #[test]
    fn content_hash_matches_string() {
        let hash = ContentHash::parse(VALID_HASH).expect("valid");
        let canonical = VALID_HASH.to_owned();

        {
            let actual_wire = bincode_serialize(&hash).expect("serialize bincode hash");
            assert_eq!(
                actual_wire,
                bincode_serialize(&canonical).expect("serialize bincode string")
            );
            assert_eq!(
                bincode_deserialize::<ContentHash>(&actual_wire).expect("deserialize bincode hash"),
                hash
            );
        }

        {
            let actual_wire = bitcode_serialize(&hash).expect("serialize bitcode hash");
            assert_eq!(
                actual_wire,
                bitcode_serialize(&canonical).expect("serialize bitcode string")
            );
            assert_eq!(
                bitcode_deserialize::<ContentHash>(&actual_wire).expect("deserialize bitcode hash"),
                hash
            );
        }
    }

    #[test]
    fn content_hash_seaorm_validates() {
        let valid = Value::String(Some(VALID_HASH.to_owned()));
        let parsed = <ContentHash as ValueType>::try_from(valid).expect("valid db value");
        assert_eq!(parsed.to_string(), VALID_HASH);

        let sha = format!("sha256:{}", "a".repeat(BLAKE3_HEX_LEN));
        let invalid = Value::String(Some(sha));
        assert!(<ContentHash as ValueType>::try_from(invalid).is_err());

        assert!(<ContentHash as ValueType>::try_from(Value::String(None)).is_err());
    }

    #[test]
    fn content_hash_seaorm_value() {
        let hash = ContentHash::parse(VALID_HASH).expect("valid");
        let value: Value = hash.into();
        let back = <ContentHash as ValueType>::try_from(value).expect("roundtrip");
        assert_eq!(back, hash);
    }

    #[test]
    fn artifact_uri_requires_scheme() {
        let uri = ArtifactUri::parse("file:///var/artifacts/x.parquet").expect("valid");
        assert_eq!(uri.scheme(), "file");
        assert!(uri.has_path_extension("parquet"));
        let versioned = ArtifactUri::parse("s3://bucket/evidence/x.parquet?versionId=immutable")
            .expect("valid");
        assert!(versioned.has_path_extension("parquet"));
        assert!(!versioned.has_path_extension("json"));
        assert!(!versioned.has_path_extension(".parquet"));
        assert!(ArtifactUri::parse("/var/artifacts/x").is_err());
        assert!(ArtifactUri::parse("://nope").is_err());
    }

    #[test]
    fn schema_version_rejects_non() {
        assert!(SchemaVersion::try_new(0).is_err());
        assert_eq!(SchemaVersion::try_new(3).unwrap().get(), 3);
        assert_eq!(SchemaVersion::FIRST.get(), 1);
    }

    #[test]
    fn schema_version_serde_integer() {
        let v = SchemaVersion::new(3);
        assert_eq!(serde_json::to_string(&v).unwrap(), "3");
        let back: SchemaVersion = serde_json::from_str("3").unwrap();
        assert_eq!(back, v);
    }

    #[test]
    fn schema_serde_rejects_non() {
        assert!(serde_json::from_str::<SchemaVersion>("0").is_err());
        assert!(serde_json::from_str::<SchemaVersion>("-1").is_err());
    }

    #[test]
    fn schema_version_seaorm_validates() {
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
    fn schema_version_seaorm_value() {
        let version = SchemaVersion::new(7);
        let value: Value = version.into();
        let back = <SchemaVersion as ValueType>::try_from(value).expect("roundtrip");
        assert_eq!(back, version);
    }
}
