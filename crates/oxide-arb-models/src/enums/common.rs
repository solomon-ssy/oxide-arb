//! Common enums used across the oxide-arb platform.
//!
//! Enums that appear as `SeaORM` entity columns derive `DeriveActiveEnum` +
//! `IntoActiveValue` so they can be stored directly in the database without
//! JSON serialization.

use oxide_arb_macros::IntoActiveValue;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use sea_orm::{DeriveActiveEnum, EnumIter};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Trade direction.
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
pub enum Side {
    #[sea_orm(string_value = "BUY")]
    Buy,
    #[sea_orm(string_value = "SELL")]
    Sell,
}

impl Side {
    #[must_use]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

impl std::fmt::Display for Side {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Buy => write!(f, "BUY"),
            Self::Sell => write!(f, "SELL"),
        }
    }
}

/// Polymarket CLOB order time-in-force types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    /// Fill-or-Kill: must fill entirely and immediately, or cancel. Never retried.
    Fok,
    /// Good-Till-Cancelled: rests on the book until filled or manually cancelled.
    Gtc,
    /// Good-Till-Date: rests on the book until `expiration` (unix timestamp).
    Gtd { expiration: u64 },
}

/// Trade execution mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    #[default]
    DryRun,
    Paper,
    Live,
}

impl std::fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DryRun => write!(f, "dry_run"),
            Self::Paper => write!(f, "paper"),
            Self::Live => write!(f, "live"),
        }
    }
}

/// Staleness classification for market-data snapshots.
///
/// Variants are ordered from freshest to most stale. The derived `Ord`
/// follows this ordering so `Fresh < Acceptable < Stale < Expired`.
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
pub enum StalenessLevel {
    #[sea_orm(string_value = "fresh")]
    Fresh,
    #[sea_orm(string_value = "acceptable")]
    Acceptable,
    #[sea_orm(string_value = "stale")]
    Stale,
    #[sea_orm(string_value = "expired")]
    Expired,
}

impl StalenessLevel {
    #[must_use]
    #[inline]
    pub const fn worse(self, other: Self) -> Self {
        if (self as u8) > (other as u8) {
            self
        } else {
            other
        }
    }

    #[must_use]
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Acceptable => "acceptable",
            Self::Stale => "stale",
            Self::Expired => "expired",
        }
    }
}

impl std::fmt::Display for StalenessLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Final outcome of a trade attempt.
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
pub enum TradeOutcome {
    /// Order filled, position opened, awaiting settlement.
    #[sea_orm(string_value = "success")]
    Success,
    /// FOK not filled (book moved or insufficient depth).
    #[sea_orm(string_value = "miss")]
    Miss,
    /// Data was too old, trade rejected at validation.
    #[sea_orm(string_value = "stale")]
    Stale,
    /// Order submitted but venue returned error.
    #[sea_orm(string_value = "trade_failed")]
    TradeFailed,
    /// Internal error in our pipeline.
    #[sea_orm(string_value = "system_error")]
    SystemError,
}

/// Origin of a market-data update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    WebSocket,
    RestPoll,
}

/// Severity level for operational alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
}

/// Polymarket event category for fee-rate lookup and opportunity scoring.
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
pub enum MarketCategory {
    #[sea_orm(string_value = "geopolitics")]
    Geopolitics,
    #[sea_orm(string_value = "sports")]
    Sports,
    #[sea_orm(string_value = "politics")]
    Politics,
    #[sea_orm(string_value = "finance")]
    Finance,
    #[sea_orm(string_value = "tech")]
    Tech,
    #[sea_orm(string_value = "culture")]
    Culture,
    #[sea_orm(string_value = "weather")]
    Weather,
    #[sea_orm(string_value = "economics")]
    Economics,
    #[sea_orm(string_value = "crypto")]
    Crypto,
    #[sea_orm(string_value = "other")]
    Other,
}

impl std::fmt::Display for MarketCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl MarketCategory {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Geopolitics => "geopolitics",
            Self::Sports => "sports",
            Self::Politics => "politics",
            Self::Finance => "finance",
            Self::Tech => "tech",
            Self::Culture => "culture",
            Self::Weather => "weather",
            Self::Economics => "economics",
            Self::Crypto => "crypto",
            Self::Other => "other",
        }
    }

    /// Parse Gamma API category labels; unknown labels map to [`Self::Other`].
    #[must_use]
    pub fn from_gamma_label(label: Option<&str>) -> Self {
        label.and_then(|s| s.parse().ok()).unwrap_or(Self::Other)
    }
}

impl From<Option<&str>> for MarketCategory {
    fn from(label: Option<&str>) -> Self {
        Self::from_gamma_label(label)
    }
}

/// Error returned when a category string cannot be parsed (config / DB keys).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketCategoryParseError(pub String);

impl std::fmt::Display for MarketCategoryParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "unknown market category: {}", self.0)
    }
}

impl std::error::Error for MarketCategoryParseError {}

impl std::str::FromStr for MarketCategory {
    type Err = MarketCategoryParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "geopolitics" | "Geopolitics" => Ok(Self::Geopolitics),
            "sports" | "Sports" => Ok(Self::Sports),
            "politics" | "Politics" => Ok(Self::Politics),
            "finance" | "Finance" => Ok(Self::Finance),
            "tech" | "Tech" => Ok(Self::Tech),
            "culture" | "Culture" | "Pop Culture" => Ok(Self::Culture),
            "weather" | "Weather" | "Climate" => Ok(Self::Weather),
            "economics" | "Economics" => Ok(Self::Economics),
            "crypto" | "Crypto" => Ok(Self::Crypto),
            "other" | "Other" => Ok(Self::Other),
            other => Err(MarketCategoryParseError(other.to_owned())),
        }
    }
}

/// Minimum price increment supported by a Polymarket CLOB market.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TickSize {
    Tenth,
    Hundredth,
    Thousandth,
    TenThousandth,
}

/// Invalid tick size string from Gamma / CLOB wire format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickSizeParseError(pub String);

impl std::fmt::Display for TickSizeParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid tick size: {}", self.0)
    }
}

impl std::error::Error for TickSizeParseError {}

impl TickSize {
    #[must_use]
    #[inline]
    pub const fn as_decimal(&self) -> Decimal {
        match self {
            Self::Tenth => dec!(0.1),
            Self::Hundredth => dec!(0.01),
            Self::Thousandth => dec!(0.001),
            Self::TenThousandth => dec!(0.0001),
        }
    }
}

impl std::str::FromStr for TickSize {
    type Err = TickSizeParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim() {
            "0.1" => Ok(Self::Tenth),
            "0.01" => Ok(Self::Hundredth),
            "0.001" => Ok(Self::Thousandth),
            "0.0001" => Ok(Self::TenThousandth),
            other => Err(TickSizeParseError(other.to_owned())),
        }
    }
}

impl TryFrom<Decimal> for TickSize {
    type Error = TickSizeParseError;

    fn try_from(d: Decimal) -> Result<Self, TickSizeParseError> {
        Self::from_str(&d.normalize().to_string())
    }
}

/// Type of report snapshot.
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
pub enum ReportType {
    #[sea_orm(string_value = "daily")]
    Daily,
    #[sea_orm(string_value = "weekly")]
    Weekly,
}

impl std::fmt::Display for ReportType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Daily => write!(f, "daily"),
            Self::Weekly => write!(f, "weekly"),
        }
    }
}
