//! Feature-plane enums shared by research builders and `ClickHouse` fact rows.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::domain::data_plane::DecisionSource;

/// The data source a feature value was derived from, for audit / replay.
///
/// Single taxonomy of "where a feature value came from" — attached to evidence
/// in feature builders and persisted on `quant_feature_event.source_kind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceSourceKind {
    /// Live or historical CLOB order book.
    Book,
    /// Gamma market metadata.
    GammaMetadata,
    /// A persisted `ClickHouse` fact (microstructure / tick window).
    ClickHouseFact,
    /// Dual-provider-attested finalized economic executions.
    FinalizedExecution,
    /// Derived/computed from other in-memory inputs.
    Derived,
    /// A crypto-domain observation (`quant_domain_observation`: Binance
    /// klines / Chainlink oracle quotes).
    DomainCrypto,
    /// A weather-domain observation (`quant_domain_observation`: ensemble
    /// forecasts / station observations / NOAA resolution data).
    DomainWeather,
    /// A version from the append-only market-linkage ledger.
    Linkage,
}

impl EvidenceSourceKind {
    /// The stable wire label persisted to the `quant_feature_event.source_kind`
    /// column (append-only contract).
    #[must_use]
    pub const fn as_wire(self) -> &'static str {
        match self {
            Self::Book => "book",
            Self::GammaMetadata => "gamma_metadata",
            Self::ClickHouseFact => "clickhouse_fact",
            Self::FinalizedExecution => "finalized_execution",
            Self::Derived => "derived",
            Self::DomainCrypto => "domain_crypto",
            Self::DomainWeather => "domain_weather",
            Self::Linkage => "linkage",
        }
    }

    /// The governed point-in-time clock that bounds this evidence source.
    ///
    /// `Derived` evidence inherits the enclosing decision boundary and
    /// therefore has no independent source clock.
    #[must_use]
    pub const fn decision_source(self) -> Option<DecisionSource> {
        match self {
            Self::Book => Some(DecisionSource::Book),
            Self::GammaMetadata => Some(DecisionSource::Catalog),
            Self::ClickHouseFact => Some(DecisionSource::Microstructure),
            Self::FinalizedExecution => Some(DecisionSource::FinalizedExecution),
            Self::Derived => None,
            Self::DomainCrypto => Some(DecisionSource::DomainCrypto),
            Self::DomainWeather => Some(DecisionSource::DomainWeather),
            Self::Linkage => Some(DecisionSource::Linkage),
        }
    }
}

/// The dimensional kind of a present feature value.
///
/// Carries a stable `i8` code persisted to `quant_feature_event.value_kind`.
/// Append-only contract: never renumber an existing variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FeatureValueKind {
    /// A dimensionless decimal.
    Decimal,
    /// A probability / confidence in `[0, 1]`.
    Probability,
    /// A basis-point quantity.
    Bps,
    /// A USD-denominated amount.
    Usd,
    /// A non-negative count.
    Count,
    /// A boolean flag.
    Bool,
    /// A categorical market class (faithful enum; encoding is a downstream
    /// normalization concern — never consumed as an ordinal number).
    Category,
}

impl FeatureValueKind {
    /// The stable `ClickHouse` `value_kind` code (append-only contract).
    #[must_use]
    pub const fn as_i8(self) -> i8 {
        match self {
            Self::Decimal => 0,
            Self::Probability => 1,
            Self::Bps => 2,
            Self::Usd => 3,
            Self::Count => 4,
            Self::Bool => 5,
            Self::Category => 6,
        }
    }

    /// Decode a persisted `value_kind` code, rejecting unknown values.
    #[must_use]
    pub const fn from_i8(code: i8) -> Option<Self> {
        match code {
            0 => Some(Self::Decimal),
            1 => Some(Self::Probability),
            2 => Some(Self::Bps),
            3 => Some(Self::Usd),
            4 => Some(Self::Count),
            5 => Some(Self::Bool),
            6 => Some(Self::Category),
            _ => None,
        }
    }
}
