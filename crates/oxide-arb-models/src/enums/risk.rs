//! Risk engine enums — circuit breakers, blacklists, exposure reservations.

use oxide_arb_macros::IntoActiveValue;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};

/// Persisted top-level state of the multi-level circuit breaker.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum BreakerStateName {
    /// Execution permitted.
    #[sea_orm(string_value = "active")]
    Active,
    /// Auto-recovering cooldown (Level 2 session throttle).
    #[sea_orm(string_value = "cooling")]
    Cooling,
    /// Hard halt — manual operator acknowledgement required.
    #[sea_orm(string_value = "halted")]
    Halted,
}

impl std::fmt::Display for BreakerStateName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Cooling => f.write_str("cooling"),
            Self::Halted => f.write_str("halted"),
        }
    }
}

/// Severity level of a circuit-breaker trip (1–4).
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum CircuitBreakerLevel {
    #[sea_orm(string_value = "trade")]
    Trade = 1,
    #[sea_orm(string_value = "session")]
    Session = 2,
    #[sea_orm(string_value = "daily")]
    Daily = 3,
    #[sea_orm(string_value = "system")]
    System = 4,
}

impl CircuitBreakerLevel {
    #[must_use]
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::Trade => 1,
            Self::Session => 2,
            Self::Daily => 3,
            Self::System => 4,
        }
    }
}

impl std::fmt::Display for CircuitBreakerLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trade => f.write_str("trade"),
            Self::Session => f.write_str("session"),
            Self::Daily => f.write_str("daily"),
            Self::System => f.write_str("system"),
        }
    }
}

/// Severity scope for a blacklist entry.
///
/// Ordered so that higher scopes include all lower ones via `>=` comparison.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
)]
#[serde(rename_all = "snake_case")]
pub enum BlacklistScope {
    DataPath = 0,
    TradingPath = 1,
    Full = 2,
}

impl std::fmt::Display for BlacklistScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataPath => f.write_str("data_path"),
            Self::TradingPath => f.write_str("trading_path"),
            Self::Full => f.write_str("full"),
        }
    }
}

/// Why a market or token was added to the blacklist.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    bitcode::Encode,
    bitcode::Decode,
)]
#[serde(rename_all = "snake_case")]
pub enum BlacklistReason {
    ConsecutiveFokFailures,
    TradeFailedAfterMatched,
    DepthDrop,
    TickChange,
    Manual,
    DataNotFound,
}

impl std::fmt::Display for BlacklistReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConsecutiveFokFailures => f.write_str("consecutive_fok_failures"),
            Self::TradeFailedAfterMatched => f.write_str("trade_failed_after_matched"),
            Self::DepthDrop => f.write_str("depth_drop"),
            Self::TickChange => f.write_str("tick_change"),
            Self::Manual => f.write_str("manual"),
            Self::DataNotFound => f.write_str("data_not_found"),
        }
    }
}

/// Lifecycle state of an exposure reservation.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum ReservationStatus {
    #[sea_orm(string_value = "pending")]
    Pending,
    #[sea_orm(string_value = "confirmed")]
    Confirmed,
    #[sea_orm(string_value = "released")]
    Released,
}

impl std::fmt::Display for ReservationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Confirmed => f.write_str("confirmed"),
            Self::Released => f.write_str("released"),
        }
    }
}

/// Granularity of a time-windowed risk accumulator.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    EnumIter,
    DeriveActiveEnum,
    IntoActiveValue,
)]
#[sea_orm(rs_type = "String", db_type = "Text")]
#[serde(rename_all = "snake_case")]
pub enum WindowType {
    #[sea_orm(string_value = "hourly")]
    Hourly,
    #[sea_orm(string_value = "daily")]
    Daily,
    #[sea_orm(string_value = "weekly")]
    Weekly,
}

impl WindowType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Hourly => "hourly",
            Self::Daily => "daily",
            Self::Weekly => "weekly",
        }
    }
}

impl std::fmt::Display for WindowType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
