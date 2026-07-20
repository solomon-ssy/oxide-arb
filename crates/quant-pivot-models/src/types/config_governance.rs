//! Validated text contracts used by governed configuration workflows.

use std::{borrow::Cow, fmt, str::FromStr};

use sea_orm::{
    ActiveValue, ColIdx, IntoActiveValue, TryGetError, TryGetable,
    sea_query::{ArrayType, ColumnType, Nullable, Value, ValueType, ValueTypeErr},
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

/// Process-independent `PostgreSQL` advisory lock shared by schema mutation,
/// preproduction reset and irreversible production sealing.
pub const LIFECYCLE_ADVISORY_LOCK_KEY: i64 = 7_293_770_821_154_641_591;

/// Monotonic generation of the complete active policy bundle.
///
/// The value is stored as `PostgreSQL` `bigint`. Construction and database reads
/// reject zero and negative values so a corrupt guard row cannot silently enter
/// optimistic-concurrency decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct PolicyBundleGeneration(i64);

impl PolicyBundleGeneration {
    pub const FIRST: Self = Self(1);

    pub const fn try_new(value: i64) -> Result<Self, PolicyBundleGenerationError> {
        if value >= Self::FIRST.0 {
            Ok(Self(value))
        } else {
            Err(PolicyBundleGenerationError::NonPositive(value))
        }
    }

    #[must_use]
    pub const fn get(self) -> i64 {
        self.0
    }

    pub const fn checked_next(self) -> Result<Self, PolicyBundleGenerationError> {
        match self.0.checked_add(1) {
            Some(value) => Ok(Self(value)),
            None => Err(PolicyBundleGenerationError::Overflow),
        }
    }
}

impl Default for PolicyBundleGeneration {
    fn default() -> Self {
        Self::FIRST
    }
}

impl fmt::Display for PolicyBundleGeneration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl schemars::JsonSchema for PolicyBundleGeneration {
    fn inline_schema() -> bool {
        true
    }

    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("PolicyBundleGeneration")
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({ "type": "integer", "format": "int64", "minimum": 1 })
    }
}

impl<'de> Deserialize<'de> for PolicyBundleGeneration {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = i64::deserialize(deserializer)?;
        Self::try_new(value).map_err(serde::de::Error::custom)
    }
}

impl From<PolicyBundleGeneration> for Value {
    fn from(value: PolicyBundleGeneration) -> Self {
        Self::BigInt(Some(value.get()))
    }
}

impl From<&PolicyBundleGeneration> for Value {
    fn from(value: &PolicyBundleGeneration) -> Self {
        Self::BigInt(Some(value.get()))
    }
}

impl TryGetable for PolicyBundleGeneration {
    fn try_get_by<I: ColIdx>(result: &sea_orm::QueryResult, index: I) -> Result<Self, TryGetError> {
        let value = <i64 as TryGetable>::try_get_by(result, index)?;
        Self::try_new(value)
            .map_err(|error| TryGetError::DbErr(sea_orm::DbErr::Type(error.to_string())))
    }
}

impl ValueType for PolicyBundleGeneration {
    fn try_from(value: Value) -> Result<Self, ValueTypeErr> {
        match value {
            Value::BigInt(Some(value)) => Self::try_new(value).map_err(|_| ValueTypeErr),
            _ => Err(ValueTypeErr),
        }
    }

    fn type_name() -> String {
        stringify!(PolicyBundleGeneration).to_owned()
    }

    fn array_type() -> ArrayType {
        ArrayType::BigInt
    }

    fn column_type() -> ColumnType {
        ColumnType::BigInteger
    }
}

impl Nullable for PolicyBundleGeneration {
    fn null() -> Value {
        Value::BigInt(None)
    }
}

impl IntoActiveValue<Self> for PolicyBundleGeneration {
    fn into_active_value(self) -> ActiveValue<Self> {
        ActiveValue::Set(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum PolicyBundleGenerationError {
    #[error("policy bundle generation must be positive, got {0}")]
    NonPositive(i64),
    #[error("policy bundle generation overflow")]
    Overflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid {field}: {detail}")]
pub struct ConfigGovernanceTextError {
    field: &'static str,
    detail: &'static str,
}

macro_rules! validated_text_type {
    (
        $(#[$meta:meta])*
        $name:ident,
        field = $field:literal,
        detail = $detail:literal,
        validate = $validate:expr
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);

        impl $name {
            pub fn parse(value: impl Into<String>) -> Result<Self, ConfigGovernanceTextError> {
                let value = value.into();
                if ($validate)(&value) {
                    Ok(Self(value))
                } else {
                    Err(ConfigGovernanceTextError {
                        field: $field,
                        detail: $detail,
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

        impl FromStr for $name {
            type Err = ConfigGovernanceTextError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::parse(value)
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.0)
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                Self::parse(value).map_err(serde::de::Error::custom)
            }
        }

        impl schemars::JsonSchema for $name {
            fn inline_schema() -> bool {
                true
            }

            fn schema_name() -> std::borrow::Cow<'static, str> {
                std::borrow::Cow::Borrowed(stringify!($name))
            }

            fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
                <String as schemars::JsonSchema>::json_schema(generator)
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

        impl IntoActiveValue<$name> for $name {
            fn into_active_value(self) -> ActiveValue<$name> {
                ActiveValue::Set(self)
            }
        }
    };
}

validated_text_type! {
    /// Deployment environment name captured by the lifecycle seal.
    DeploymentEnvironment,
    field = "environment",
    detail = "must contain 1..=64 ASCII letters, digits, '.', '_' or '-'",
    validate = |value: &str| !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

impl DeploymentEnvironment {
    /// Local development environment used by the bootable default deploy config.
    #[must_use]
    pub fn local_development() -> Self {
        Self("local-development".to_owned())
    }
}

validated_text_type! {
    /// Canonical Git object id captured by an irreversible production seal.
    BuildCommitHash,
    field = "build_commit",
    detail = "must be a 40- or 64-character lowercase hexadecimal Git object id",
    validate = |value: &str| matches!(value.len(), 40 | 64)
        && value.bytes().all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

validated_text_type! {
    /// Client-generated idempotency key for one activation command.
    PolicyIdempotencyKey,
    field = "idempotency_key",
    detail = "must contain 8..=128 visible ASCII characters without whitespace",
    validate = |value: &str| (8..=128).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

validated_text_type! {
    /// Opaque short-lived proof returned by policy preflight.
    PolicyPreflightToken,
    field = "preflight_token",
    detail = "must contain 16..=512 visible ASCII characters",
    validate = |value: &str| (16..=512).contains(&value.len())
        && value.bytes().all(|byte| byte.is_ascii_graphic())
}

validated_text_type! {
    /// Exact operator confirmation used by the irreversible production seal.
    ProductionSealConfirmationPhrase,
    field = "confirmation_phrase",
    detail = "must contain 16..=128 printable ASCII characters without leading or trailing whitespace",
    validate = |value: &str| (16..=128).contains(&value.len())
        && value.trim() == value
        && value.bytes().all(|byte| byte == b' ' || byte.is_ascii_graphic())
}
