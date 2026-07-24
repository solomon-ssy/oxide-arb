//! Validated semantic text values persisted as `PostgreSQL` `text`.

use std::{borrow::Cow, fmt, str::FromStr};

use schemars::JsonSchema;
use sea_orm::{
    ActiveValue, ColIdx, IntoActiveValue, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Validation failure for a project-owned semantic text value.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid {kind}: {detail}")]
pub struct SemanticTextError {
    kind: &'static str,
    detail: &'static str,
}

fn semantic_key(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 256
        && value
            .bytes()
            .all(|byte| byte.is_ascii_graphic() && byte != b'\\' && byte != b'\"')
}

fn contract_version(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'@'))
}

fn trade_policy_candidate_key(value: &str) -> bool {
    value.len() <= 128 && semantic_key(value)
}

fn evm_hex(value: &str, hex_len: usize) -> bool {
    value.len() == hex_len + 2
        && value.starts_with("0x")
        && value[2..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn evm_uint256(value: &str) -> bool {
    const MAX_U256: &str =
        "115792089237316195423570985008687907853269984665640564039457584007913129639935";
    let canonical_digits = value == "0"
        || (!value.starts_with('0') && value.bytes().all(|byte| byte.is_ascii_digit()));
    canonical_digits
        && (value.len() < MAX_U256.len() || (value.len() == MAX_U256.len() && value <= MAX_U256))
}

macro_rules! validated_semantic_text {
    (
        $(#[$meta:meta])*
        $name:ident,
        kind = $kind:literal,
        validate = $validate:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            /// Validate an untrusted wire or persistence value.
            pub fn parse(value: impl Into<String>) -> Result<Self, SemanticTextError> {
                let value = value.into();
                if ($validate)(&value) {
                    Ok(Self(value))
                } else {
                    Err(SemanticTextError {
                        kind: $kind,
                        detail: "value does not satisfy the canonical format",
                    })
                }
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl FromStr for $name {
            type Err = SemanticTextError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = SemanticTextError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::parse(value)
            }
        }

        impl From<$name> for String {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(serde::de::Error::custom)
            }
        }

        impl JsonSchema for $name {
            fn inline_schema() -> bool {
                true
            }

            fn schema_name() -> Cow<'static, str> {
                Cow::Borrowed(stringify!($name))
            }

            fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                <String as JsonSchema>::json_schema(generator)
            }
        }

        impl From<$name> for Value {
            fn from(value: $name) -> Self {
                Self::String(Some(value.0))
            }
        }

        impl From<&$name> for Value {
            fn from(value: &$name) -> Self {
                Self::String(Some(value.0.clone()))
            }
        }

        impl TryGetable for $name {
            fn try_get_by<I: ColIdx>(
                result: &sea_orm::QueryResult,
                index: I,
            ) -> Result<Self, TryGetError> {
                let value = <String as TryGetable>::try_get_by(result, index)?;
                Self::parse(value)
                    .map_err(|error| TryGetError::DbErr(sea_orm::DbErr::Type(error.to_string())))
            }
        }

        impl ValueType for $name {
            fn try_from(value: Value) -> Result<Self, ValueTypeErr> {
                match value {
                    Value::String(Some(value)) => Self::parse(value).map_err(|_| ValueTypeErr),
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

        impl sea_orm::sea_query::postgres_array::NotU8 for $name {}

        impl sea_orm::TryGetableArray for $name {
            fn try_get_by<I: ColIdx>(
                result: &sea_orm::QueryResult,
                index: I,
            ) -> Result<Vec<Self>, TryGetError> {
                <Vec<String> as TryGetable>::try_get_by(result, index)?
                    .into_iter()
                    .map(Self::parse)
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|error| TryGetError::DbErr(sea_orm::DbErr::Type(error.to_string())))
            }
        }

        impl sea_orm::TryFromU64 for $name {
            fn try_from_u64(_value: u64) -> Result<Self, sea_orm::DbErr> {
                Err(sea_orm::DbErr::ConvertFromU64(stringify!($name)))
            }
        }

        impl IntoActiveValue<$name> for $name {
            fn into_active_value(self) -> ActiveValue<$name> {
                ActiveValue::Set(self)
            }
        }
    };
}

validated_semantic_text! {
    /// Version of the application-owned source-slice reader contract.
    ReaderContractVersion,
    kind = "reader contract version",
    validate = contract_version
}

impl ReaderContractVersion {
    /// Fresh-boot v1 source-slice reader contract.
    #[must_use]
    pub fn v1() -> Self {
        Self("source_slice_reader_v1".to_owned())
    }
}

validated_semantic_text! {
    /// Version of the application-owned source-slice schema contract.
    SchemaContractVersion,
    kind = "schema contract version",
    validate = contract_version
}

validated_semantic_text! {
    /// Version of the authoritative settlement deployment evidence catalog.
    SettlementEvidenceVersion,
    kind = "settlement evidence version",
    validate = contract_version
}

validated_semantic_text! {
    /// Caller-supplied idempotency key for one governed settlement action.
    SettlementActionIdempotencyKey,
    kind = "settlement action idempotency key",
    validate = semantic_key
}

impl SchemaContractVersion {
    /// Fresh-boot v1 source-slice schema contract.
    #[must_use]
    pub fn v1() -> Self {
        Self("source_slice_schema_v1".to_owned())
    }
}

validated_semantic_text! {
    /// Deterministic identity of a scheduled or ad-hoc report trigger.
    ReportTriggerKey,
    kind = "report trigger key",
    validate = semantic_key
}

validated_semantic_text! {
    /// Candidate identity within one governed trade-policy experiment family.
    TradePolicyCandidateId,
    kind = "trade-policy candidate id",
    validate = trade_policy_candidate_key
}

validated_semantic_text! {
    /// Version label sealed into research-readiness artifact evidence.
    ArtifactVersion,
    kind = "artifact version",
    validate = semantic_key
}

validated_semantic_text! {
    /// Identifier of the key used to attest research-readiness evidence.
    AttestationKeyId,
    kind = "attestation key id",
    validate = semantic_key
}

validated_semantic_text! {
    /// Canonical lower-case `0x`-prefixed EVM address.
    EvmAddress,
    kind = "EVM address",
    validate = |value: &str| evm_hex(value, 40)
}

validated_semantic_text! {
    /// Canonical lower-case `0x`-prefixed EVM transaction hash.
    EvmTransactionHash,
    kind = "EVM transaction hash",
    validate = |value: &str| evm_hex(value, 64)
}

validated_semantic_text! {
    /// Canonical lower-case `0x`-prefixed Polygon block hash.
    EvmBlockHash,
    kind = "EVM block hash",
    validate = |value: &str| evm_hex(value, 64)
}

validated_semantic_text! {
    /// Canonical lower-case `0x`-prefixed Polymarket CTF condition identifier.
    EvmConditionId,
    kind = "EVM condition ID",
    validate = |value: &str| evm_hex(value, 64)
}

validated_semantic_text! {
    /// Canonical lower-case `0x`-prefixed Keccak-256 hash of prepared calldata.
    EvmCalldataHash,
    kind = "EVM calldata hash",
    validate = |value: &str| evm_hex(value, 64)
}

validated_semantic_text! {
    /// Opaque Polymarket relayer resource identity; never an EVM hash.
    RelayerTransactionId,
    kind = "relayer transaction ID",
    validate = semantic_key
}

validated_semantic_text! {
    /// Canonical base-10 unsigned 256-bit EVM integer.
    EvmUint256,
    kind = "EVM uint256",
    validate = evm_uint256
}

impl EvmUint256 {
    /// Canonical zero without a fallible parse on an internal constant.
    #[must_use]
    pub fn zero() -> Self {
        Self("0".to_owned())
    }
}

validated_semantic_text! {
    /// Canonical lower-case `0x`-prefixed Keccak-256 hash of deployed EVM bytecode.
    EvmCodeHash,
    kind = "EVM code hash",
    validate = |value: &str| evm_hex(value, 64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_values_non_input() {
        assert!(ReportTriggerKey::parse("").is_err());
        assert!(ReportTriggerKey::parse("ad_hoc:request 1").is_err());
        assert!(ReportTriggerKey::parse("ad_hoc:\\request").is_err());
        assert!(ReportTriggerKey::parse("ad_hoc:\"request\"").is_err());
        assert!(ReaderContractVersion::parse("reader version 1").is_err());
        assert!(SettlementEvidenceVersion::parse("polymarket-v2-2026-07-22.1").is_ok());
        assert!(SettlementEvidenceVersion::parse("settlement evidence v1").is_err());
        assert!(TradePolicyCandidateId::parse("a".repeat(128)).is_ok());
        assert!(TradePolicyCandidateId::parse("a".repeat(129)).is_err());
        assert!(EvmAddress::parse(format!("0x{}", "a".repeat(40))).is_ok());
        assert!(EvmAddress::parse(format!("0x{}", "A".repeat(40))).is_err());
        assert!(EvmTransactionHash::parse(format!("0x{}", "f".repeat(64))).is_ok());
        assert!(EvmTransactionHash::parse("0xdeadbeef").is_err());
        assert!(EvmCodeHash::parse(format!("0x{}", "0".repeat(64))).is_ok());
        assert!(EvmBlockHash::parse(format!("0x{}", "1".repeat(64))).is_ok());
        assert!(EvmConditionId::parse(format!("0x{}", "3".repeat(64))).is_ok());
        assert!(EvmCalldataHash::parse(format!("0x{}", "2".repeat(64))).is_ok());
        assert!(RelayerTransactionId::parse("relay_01jz7c").is_ok());
        assert!(RelayerTransactionId::parse("relay id").is_err());
        assert!(EvmUint256::parse("0").is_ok());
        assert!(EvmUint256::parse("01").is_err());
        assert!(
            EvmUint256::parse(
                "115792089237316195423570985008687907853269984665640564039457584007913129639935"
            )
            .is_ok()
        );
        assert!(
            EvmUint256::parse(
                "115792089237316195423570985008687907853269984665640564039457584007913129639936"
            )
            .is_err()
        );
    }

    #[test]
    fn serde_seaorm_decode_values() {
        let address = EvmAddress::parse(format!("0x{}", "a".repeat(40))).expect("EVM address");
        let encoded = serde_json::to_string(&address).expect("serialize EVM address");
        assert_eq!(
            serde_json::from_str::<EvmAddress>(&encoded).expect("deserialize EVM address"),
            address
        );
        assert!(serde_json::from_str::<EvmAddress>(r#""0xABC""#).is_err());
        assert!(
            <EvmAddress as ValueType>::try_from(Value::String(Some(format!(
                "0x{}",
                "A".repeat(40)
            ))))
            .is_err()
        );
    }
}
