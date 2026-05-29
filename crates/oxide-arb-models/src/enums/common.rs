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
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};
use thiserror::Error;

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
    #[inline]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

impl Display for Side {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
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
pub enum ExecutionMode {
    #[sea_orm(string_value = "dry_run")]
    #[default]
    DryRun,
    #[sea_orm(string_value = "paper")]
    Paper,
    #[sea_orm(string_value = "live")]
    Live,
}

impl Display for ExecutionMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
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

impl Display for StalenessLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Success/miss/failed bucket derived from [`TradeState`].
///
/// This is not the trade's stored state: the `trade` row has exactly one state
/// field, while this bucket is surfaced as a read-only PG generated column for
/// risk accounting, reporting, and audit.
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
pub enum TradeBusinessOutcome {
    /// Order filled, position opened.
    #[sea_orm(string_value = "success")]
    Success,
    /// FOK not filled (book moved or insufficient depth).
    #[sea_orm(string_value = "miss")]
    Miss,
    /// Order failed/errored, or submitted-but-unconfirmed (orphaned).
    #[sea_orm(string_value = "failed")]
    Failed,
}

impl Display for TradeBusinessOutcome {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Success => write!(f, "success"),
            Self::Miss => write!(f, "miss"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Durable trade lifecycle state machine — single source of truth on the `trade` row.
///
/// Replaces the former `phase × outcome` two-column model: one field makes illegal
/// states unrepresentable (e.g. "processed but no outcome" cannot occur). The
/// business-outcome classification (success/miss/failed) is derived via
/// [`TradeState::business_outcome`] in Rust and a PG generated column for reporting.
///
/// Transitions:
/// `Intent` → `Submitted` → (`FillObserved` | `MissObserved` | `FailObserved`)
///   → (`FillProcessing` | `MissProcessing` | `FailProcessing`)
///   → (`Settled` | `Missed` | `Failed`); stale `Submitted` → `Orphaned`.
/// The `*Observed` states are unclaimed durable work. The `*Processing` states
/// are lease-backed claims and may be reclaimed after lease expiry.
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
pub enum TradeState {
    /// Row inserted, order not yet submitted to the venue.
    #[sea_orm(string_value = "intent")]
    Intent,
    /// Order signed and sent, venue outcome not yet observed (crash-orphan window).
    #[sea_orm(string_value = "submitted")]
    Submitted,
    /// Fill observed; position + risk Fill accounting not yet applied (relay queue).
    #[sea_orm(string_value = "fill_observed")]
    FillObserved,
    /// FOK miss observed; finalization not yet applied (relay queue).
    #[sea_orm(string_value = "miss_observed")]
    MissObserved,
    /// Venue error/timeout observed; finalization not yet applied (relay queue).
    #[sea_orm(string_value = "fail_observed")]
    FailObserved,
    /// Fill relay side-effects are currently claimed by a worker.
    #[sea_orm(string_value = "fill_processing")]
    FillProcessing,
    /// Miss relay finalization is currently claimed by a worker.
    #[sea_orm(string_value = "miss_processing")]
    MissProcessing,
    /// Failure relay finalization is currently claimed by a worker.
    #[sea_orm(string_value = "fail_processing")]
    FailProcessing,
    /// Fill fully processed (position created, risk Fill accounted). Terminal.
    #[sea_orm(string_value = "settled")]
    Settled,
    /// Miss fully processed. Terminal.
    #[sea_orm(string_value = "missed")]
    Missed,
    /// Failure fully processed. Terminal.
    #[sea_orm(string_value = "failed")]
    Failed,
    /// Submitted but never confirmed past the timeout — needs reconciliation. Terminal.
    #[sea_orm(string_value = "orphaned")]
    Orphaned,
}

impl TradeState {
    /// States awaiting relay processing (outcome observed, side-effects not applied).
    #[must_use]
    pub const fn is_unprocessed(self) -> bool {
        matches!(
            self,
            Self::FillObserved | Self::MissObserved | Self::FailObserved
        )
    }

    /// States held by a relay worker lease.
    #[must_use]
    pub const fn is_processing(self) -> bool {
        matches!(
            self,
            Self::FillProcessing | Self::MissProcessing | Self::FailProcessing
        )
    }

    /// Terminal state reached once the relay processes the matching observed state.
    #[must_use]
    pub const fn processed_terminal(self) -> Option<Self> {
        match self {
            Self::FillObserved | Self::FillProcessing => Some(Self::Settled),
            Self::MissObserved | Self::MissProcessing => Some(Self::Missed),
            Self::FailObserved | Self::FailProcessing => Some(Self::Failed),
            _ => None,
        }
    }

    /// Business-outcome classification, or `None` while still in-flight.
    #[must_use]
    pub const fn business_outcome(self) -> Option<TradeBusinessOutcome> {
        match self {
            Self::FillObserved | Self::FillProcessing | Self::Settled => {
                Some(TradeBusinessOutcome::Success)
            }
            Self::MissObserved | Self::MissProcessing | Self::Missed => {
                Some(TradeBusinessOutcome::Miss)
            }
            Self::FailObserved | Self::FailProcessing | Self::Failed | Self::Orphaned => {
                Some(TradeBusinessOutcome::Failed)
            }
            Self::Intent | Self::Submitted => None,
        }
    }

    /// True once a fill has been observed (whether or not post-trade is applied).
    #[must_use]
    pub const fn is_success(self) -> bool {
        matches!(
            self,
            Self::FillObserved | Self::FillProcessing | Self::Settled
        )
    }
}

impl Display for TradeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Intent => write!(f, "intent"),
            Self::Submitted => write!(f, "submitted"),
            Self::FillObserved => write!(f, "fill_observed"),
            Self::MissObserved => write!(f, "miss_observed"),
            Self::FailObserved => write!(f, "fail_observed"),
            Self::FillProcessing => write!(f, "fill_processing"),
            Self::MissProcessing => write!(f, "miss_processing"),
            Self::FailProcessing => write!(f, "fail_processing"),
            Self::Settled => write!(f, "settled"),
            Self::Missed => write!(f, "missed"),
            Self::Failed => write!(f, "failed"),
            Self::Orphaned => write!(f, "orphaned"),
        }
    }
}

/// Origin of a market-data update.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    WebSocket,
    RestPoll,
}

/// Severity level for operational alerts.
///
/// Ordered from lowest to highest: `Info < Warning < Critical < Emergency`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
    /// System-level fault: API fully down, DB corrupted, circuit breaker L4.
    Emergency,
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

impl Display for MarketCategory {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl MarketCategory {
    /// Every persisted category variant — used for bulk cache invalidation.
    pub const ALL_VARIANTS: [Self; 10] = [
        Self::Geopolitics,
        Self::Sports,
        Self::Politics,
        Self::Finance,
        Self::Tech,
        Self::Culture,
        Self::Weather,
        Self::Economics,
        Self::Crypto,
        Self::Other,
    ];

    /// Index into fixed-size category weight tables (0..9).
    #[must_use]
    #[inline]
    pub const fn table_index(self) -> usize {
        match self {
            Self::Geopolitics => 0,
            Self::Sports => 1,
            Self::Politics => 2,
            Self::Finance => 3,
            Self::Tech => 4,
            Self::Culture => 5,
            Self::Weather => 6,
            Self::Economics => 7,
            Self::Crypto => 8,
            Self::Other => 9,
        }
    }

    #[must_use]
    #[inline]
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
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("unknown market category: {0}")]
pub struct MarketCategoryParseError(pub String);

impl FromStr for MarketCategory {
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
pub enum TickSize {
    #[sea_orm(string_value = "0.1")]
    Tenth,
    #[sea_orm(string_value = "0.01")]
    Hundredth,
    #[sea_orm(string_value = "0.001")]
    Thousandth,
    #[sea_orm(string_value = "0.0001")]
    TenThousandth,
}

/// Invalid tick size string from Gamma / CLOB wire format.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("invalid tick size: {0}")]
pub struct TickSizeParseError(pub String);

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

impl FromStr for TickSize {
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

/// Lifecycle status of a position.
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
pub enum PositionStatus {
    #[sea_orm(string_value = "open")]
    Open,
    #[sea_orm(string_value = "closed")]
    Closed,
    #[sea_orm(string_value = "settled")]
    Settled,
}

impl Display for PositionStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Open => write!(f, "open"),
            Self::Closed => write!(f, "closed"),
            Self::Settled => write!(f, "settled"),
        }
    }
}

/// Lifecycle status of on-chain CTF redemption for a position.
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
pub enum RedeemStatus {
    #[sea_orm(string_value = "not_required")]
    NotRequired,
    #[sea_orm(string_value = "pending")]
    Pending,
    #[sea_orm(string_value = "completed")]
    Completed,
    #[sea_orm(string_value = "failed")]
    Failed,
}

impl RedeemStatus {
    #[must_use]
    pub const fn initial_for_mode(mode: ExecutionMode) -> Self {
        match mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => Self::NotRequired,
            ExecutionMode::Live => Self::Pending,
        }
    }

    #[must_use]
    pub const fn settled_for_mode(mode: ExecutionMode) -> Self {
        match mode {
            ExecutionMode::DryRun | ExecutionMode::Paper => Self::NotRequired,
            ExecutionMode::Live => Self::Completed,
        }
    }
}

impl Display for RedeemStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotRequired => write!(f, "not_required"),
            Self::Pending => write!(f, "pending"),
            Self::Completed => write!(f, "completed"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

/// Configured on-chain redemption route.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedeemRoute {
    #[default]
    Disabled,
    StandardCtf,
    NegRiskLegacyAdapter,
    CtfCollateralAdapter,
    NegRiskCollateralAdapter,
    ProxySafe,
}

impl RedeemRoute {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::StandardCtf => "standard_ctf",
            Self::NegRiskLegacyAdapter => "neg_risk_legacy_adapter",
            Self::CtfCollateralAdapter => "ctf_collateral_adapter",
            Self::NegRiskCollateralAdapter => "neg_risk_collateral_adapter",
            Self::ProxySafe => "proxy_safe",
        }
    }
}

impl Display for RedeemRoute {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Settlement redeem output asset for adapter routes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RedeemOutputAsset {
    #[default]
    UsdcE,
    Pusd,
}

/// Lifecycle status for post-redeem accounting persistence.
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
pub enum SettlementAccountingStatus {
    #[sea_orm(string_value = "pending")]
    Pending,
    #[sea_orm(string_value = "redeemed")]
    Redeemed,
    #[sea_orm(string_value = "accounted")]
    Accounted,
    #[sea_orm(string_value = "failed")]
    Failed,
}

/// Source that triggered market settlement processing.
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
pub enum SettlementTrigger {
    #[sea_orm(string_value = "ws")]
    Ws,
    #[sea_orm(string_value = "periodic_retry")]
    PeriodicRetry,
    #[sea_orm(string_value = "manual")]
    Manual,
}

impl SettlementTrigger {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ws => "ws",
            Self::PeriodicRetry => "periodic_retry",
            Self::Manual => "manual",
        }
    }
}

impl Display for SettlementTrigger {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Display for SettlementAccountingStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => f.write_str("pending"),
            Self::Redeemed => f.write_str("redeemed"),
            Self::Accounted => f.write_str("accounted"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

/// Lifecycle status of a potential-loss ledger entry.
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
pub enum LedgerStatus {
    #[sea_orm(string_value = "active")]
    Active,
    #[sea_orm(string_value = "resolved")]
    Resolved,
    #[sea_orm(string_value = "expired")]
    Expired,
}

impl Display for LedgerStatus {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => write!(f, "active"),
            Self::Resolved => write!(f, "resolved"),
            Self::Expired => write!(f, "expired"),
        }
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

impl Display for ReportType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Daily => write!(f, "daily"),
            Self::Weekly => write!(f, "weekly"),
        }
    }
}
